"""
notify-slack — two-way Slack bridge CLI.

Used to notify the user during long-running /loop sessions and background jobs (job
completion, phase transitions, final results) without requiring them to watch the
terminal, and to wait for the user's reply when a decision is needed. Port of
notify-zulip (zulip_bridge) to Slack.

Config — two layers, merged (project values override machine values; blank values in
the project file are ignored, so the token only needs to live in one place):
  ~/.config/notify_slack/config.json   Machine-level, set up once:
                                          {"bot_token": "xoxb-...",
                                           "recipient": "U0123ABCD" or "you@example.com"}
  ./slack_bridge.json                  Per-project, read from the CURRENT WORKING
                                        DIRECTORY. Create with `notify-slack init`:
                                          {"channel": "#my-project" or "C0123ABCD"}
                                        Optional keys: recipient, bot_token,
                                        origin_tag (bool), mention_recipient (bool),
                                        format ("markdown" | "mrkdwn").
  $SLACK_BOT_TOKEN, if set, overrides bot_token from both files.

Zulip "stream+topic" maps to Slack "channel+thread": a topic is a thread whose root
message is posted by the bot as "🧵 *<topic>*" (and tagged with message metadata so it
can be found again). The first `send --topic X` creates the thread; later sends with the
same topic reply in it. topic -> thread_ts is cached in ~/.cache/notify_slack/cache.json
and re-discovered from channel history if the cache is missing.

Works in public and private channels. The bot must be a member of the channel: for a
public channel it tries to join automatically (needs channels:join); a private channel
must be joined by inviting the bot (`/invite @<bot>` in the channel).

Two delivery modes:
  - Channel+thread (default):
      notify-slack send --topic "<topic>" "<message>"     (message "-" = read stdin)
      notify-slack listen --topic "<topic>"
      notify-slack list-topics
  - Private DM to `recipient` (opt-in via --dm):
      notify-slack send --dm "<message>"
      notify-slack listen --dm

`listen` (either mode) polls Slack every --interval seconds (default 5) and prints one
JSON line per new message from `recipient`:
    {"sender": "U...", "content": "...", "timestamp": 1730000000, "ts": "1730000000.000100"}
It only reports messages newer than the bot's own latest message in that thread/DM, so a
reply that lands between `send` and arming `listen` is not lost. Runs forever (or until
the first reply with --once). Intended to be run under the Monitor tool
(persistent: true); stop it with TaskStop once a reply is handled.

Polling (not Socket Mode) is deliberate: Socket Mode delivers each event to only one of
the app's open connections, so several concurrent listeners (different projects/topics)
would steal each other's events.

Messages are sent as a Slack "markdown" block, so standard Markdown (**bold**,
[label](url), lists, code fences) renders; falls back to plain mrkdwn text if the
workspace rejects the block. Every outgoing message gets a small footer with the origin
host/IP/tmux session (disable with "origin_tag": false) and, in channels, an @-mention
of the recipient so thread replies actually notify them (disable with
"mention_recipient": false).

IMPORTANT: if invoking via `conda run`, `listen` must use `--no-capture-output`, or its
stdout is buffered until exit and silently lost.
"""
import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.http_retry.builtin_handlers import RateLimitErrorRetryHandler

MACHINE_CONFIG_PATH = os.path.expanduser("~/.config/notify_slack/config.json")
PROJECT_CONFIG_PATH = os.path.join(os.getcwd(), "slack_bridge.json")
CACHE_PATH = os.path.expanduser("~/.cache/notify_slack/cache.json")
STATE_PATH = os.path.expanduser("~/.local/state/notify_slack/read_state.json")  # inbox read cursors

TOPIC_EVENT_TYPE = "slack_bridge_topic"
TOPIC_TEXT_RE = re.compile(r"^(?:🧵|:thread:) \*(.+?)\*")  # Slack stores the emoji as :thread:
HISTORY_SCAN_LIMIT = 3000      # max channel messages scanned when looking up a topic
MARKDOWN_BLOCK_LIMIT = 12000   # Slack's cap on markdown-block text per message
THREAD_SCAN_WINDOW = 200       # inbox/watch look for new thread replies under the last N top-level posts
MEMBERS_TTL = 300.0             # seconds to cache channel membership (wake_from: "channel")
CLOCK_SKEW = 10.0              # seconds of overlap kept between polls / reads (local vs Slack clock)

# Errors that won't fix themselves by retrying — listen exits on these.
FATAL_ERRORS = {
    "invalid_auth", "not_authed", "account_inactive", "token_revoked", "token_expired",
    "missing_scope", "channel_not_found", "not_in_channel", "thread_not_found",
    "is_archived", "user_not_found", "users_not_found",
}

# Message subtypes that represent a real human message worth reporting.
REPORTABLE_SUBTYPES = {None, "file_share", "thread_broadcast", "me_message"}

RECOMMENDED_SCOPES = {
    "chat:write": "send messages",
    "channels:history": "read public-channel threads (listen, topic lookup)",
    "channels:read": "resolve public channel names",
    "groups:history": "read private-channel threads",
    "groups:read": "resolve private channel names",
    "im:write": "open the DM with recipient (--dm)",
    "im:history": "read DM replies (listen --dm)",
    "files:write": "send_file",
    "users:read": "check / user lookups",
}
OPTIONAL_SCOPES = {
    "channels:join": "auto-join public channels (else /invite the bot)",
    "users:read.email": "allow recipient to be given as an email address",
    "reactions:write": "serve: 👀/✅ marks on handled messages, ✅/❌ buttons on approval prompts",
    "reactions:read": "serve: approve/decline tasks by reacting to the approval prompt",
}


# --------------------------------------------------------------------------- utils

def _die(msg, stream=sys.stderr):
    print(msg, file=stream, flush=True)
    sys.exit(1)


def _load_json(path):
    with open(path) as f:
        return json.load(f)


def _write_json(path, data, mode=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp{os.getpid()}"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    if mode is not None:
        os.chmod(tmp, mode)
    os.replace(tmp, path)


def _load_config():
    if not os.path.isfile(PROJECT_CONFIG_PATH):
        _die(f"{PROJECT_CONFIG_PATH} not found. Run `notify-slack init` in this "
             "project's root first.")
    machine = _load_json(MACHINE_CONFIG_PATH) if os.path.isfile(MACHINE_CONFIG_PATH) else {}
    project = _load_json(PROJECT_CONFIG_PATH)
    config = dict(machine)
    config.update({k: v for k, v in project.items() if v not in ("", None)})
    if os.environ.get("SLACK_BOT_TOKEN"):
        config["bot_token"] = os.environ["SLACK_BOT_TOKEN"]
    token = config.get("bot_token", "")
    if not token:
        _die(f'No "bot_token" found. Put your bot token (xoxb-...) in {MACHINE_CONFIG_PATH} '
             f"(or {PROJECT_CONFIG_PATH}, or $SLACK_BOT_TOKEN). See README.md Setup.")
    if not token.startswith("xoxb-"):
        _die('"bot_token" should be a Bot User OAuth Token starting with "xoxb-" '
             "(Slack app → OAuth & Permissions).")
    return config


def _origin_tag():
    """'gh-login03 (141.142.248.9) · tmux sampling-0' — where this process is running."""
    host = socket.gethostname().split(".")[0]
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # UDP connect sends nothing; just picks the outbound interface
        ip = s.getsockname()[0]
        s.close()
    except OSError:
        ip = None
    tag = f"{host} ({ip})" if ip else host
    # Only when actually inside tmux: without $TMUX, `tmux display` falls back to the
    # server's most recent session, which would mislabel non-tmux callers.
    if os.environ.get("TMUX"):
        try:
            sess = subprocess.run(
                ["tmux", "display-message", "-p", "#S"],
                capture_output=True, text=True, timeout=2,
            ).stdout.strip()
            if sess:
                tag += f" · tmux {sess}"
        except (OSError, subprocess.TimeoutExpired):
            pass
    return tag


def _escape(text):
    """Escape the three characters Slack's mrkdwn treats as control characters."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _decode(text, name=None):
    """Turn Slack's wire format (<@U123>, <url|label>, &amp;, ...) back into plain text.
    name(uid) -> display name, if given, is used to resolve user mentions."""
    text = re.sub(r"<@([UW][A-Z0-9]+)(?:\|([^>]*))?>",
                  lambda m: "@" + (m.group(2) or (name(m.group(1)) if name else m.group(1))), text)
    text = re.sub(r"<#([CG][A-Z0-9]+)(?:\|([^>]*))?>",
                  lambda m: "#" + (m.group(2) or m.group(1)), text)
    text = re.sub(r"<!subteam\^[A-Z0-9]+(?:\|([^>]*))?>", lambda m: m.group(1) or "@group", text)
    text = re.sub(r"<!(here|channel|everyone)(?:\|[^>]*)?>", r"@\1", text)

    def link(m):
        url, label = m.group(1), m.group(2)
        bare = re.sub(r"^(mailto:|https?://)", "", url)
        return url if not label or label in (url, bare) else f"{label} ({url})"

    text = re.sub(r"<([^<>|]+)(?:\|([^<>]*))?>", link, text)
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def _tsf(ts):
    """Sort key for Slack ts strings ("1730000000.000100"), compared exactly (no float)."""
    if not ts:
        return (0, 0)
    sec, _, frac = str(ts).partition(".")
    return (int(sec), int((frac + "000000")[:6]))


class _Cache:
    """Small on-disk cache: channel name -> id, topic -> thread_ts, email -> user id."""

    def __init__(self, path=CACHE_PATH):
        self.path = path
        self.data = self._read()

    def _read(self):
        try:
            return _load_json(self.path)
        except (OSError, ValueError):
            return {}

    def get(self, section, key, fresh=False):
        if fresh:
            self.data = self._read()
        return self.data.get(section, {}).get(key)

    def put(self, section, key, value):
        # Re-read before writing so concurrent processes don't clobber each other much.
        fresh = self._read()
        fresh.setdefault(section, {})[key] = value
        self.data = fresh
        try:
            _write_json(self.path, fresh)
        except OSError:
            pass  # cache is best-effort


# -------------------------------------------------------------------------- bridge

class Bridge:
    def __init__(self, config):
        self.config = config
        self.client = WebClient(token=config["bot_token"], timeout=30)
        self.client.retry_handlers.append(RateLimitErrorRetryHandler(max_retry_count=5))
        self.cache = _Cache()
        self.state = _Cache(STATE_PATH)
        self._labels = {}
        self._members = {}
        self._served = None
        self._no_reactions = False
        self._me = None
        self._dm = None
        self._recipient = None
        self._blocks_ok = config.get("format", "markdown") == "markdown"

    # ---- identity / resolution

    @property
    def me(self):
        if self._me is None:
            r = self.client.auth_test()
            self._me = {"user_id": r["user_id"], "bot_id": r.get("bot_id"),
                        "team_id": r["team_id"], "team": r.get("team"), "url": r.get("url"),
                        "user": r.get("user"), "scopes": _header(r, "x-oauth-scopes")}
        return self._me

    def _is_mine(self, msg):
        return (msg.get("user") == self.me["user_id"]
                or (msg.get("bot_id") and msg.get("bot_id") == self.me["bot_id"]))

    def channel_id(self, channel):
        if not channel or channel == "CHANGE_ME":
            _die(f'No channel configured. Set "channel" in {PROJECT_CONFIG_PATH}, or pass --channel.')
        name = channel.lstrip("#").strip()
        if re.fullmatch(r"[CG][A-Z0-9]{6,}", name):
            return name
        name = name.lower()
        key = f"{self.me['team_id']}:{name}"
        cached = self.cache.get("channels", key)
        if cached:
            return cached
        for types in ("public_channel,private_channel", "public_channel"):
            try:
                for page in self.client.conversations_list(
                        types=types, exclude_archived=True, limit=999):
                    for c in page["channels"]:
                        if c.get("name") == name:
                            self.cache.put("channels", key, c["id"])
                            return c["id"]
                break
            except SlackApiError as e:
                if e.response.get("error") == "missing_scope" and "private" in types:
                    continue  # no groups:read — retry looking at public channels only
                raise
        _die(f"Channel #{name} not found (or not visible to the bot). A private channel is "
             "only visible after the bot is invited: run `/invite @<bot name>` in it. "
             "You can also put the channel ID (C0..., from channel details) in "
             f"{PROJECT_CONFIG_PATH} instead of its name.")

    def recipient_id(self):
        if self._recipient:
            return self._recipient
        r = (self.config.get("recipient") or "").strip()
        if not r:
            _die(f'"recipient" not set in {PROJECT_CONFIG_PATH} or {MACHINE_CONFIG_PATH}. '
                 "Set it to your Slack member ID (Profile → ⋮ → Copy member ID).")
        if "@" in r:
            key = f"{self.me['team_id']}:{r.lower()}"
            uid = self.cache.get("users", key)
            if not uid:
                uid = self.client.users_lookupByEmail(email=r)["user"]["id"]
                self.cache.put("users", key, uid)
            r = uid
        self._recipient = r
        return r

    def dm_channel(self):
        if self._dm is None:
            self._dm = self.client.conversations_open(users=self.recipient_id())["channel"]["id"]
        return self._dm

    # ---- low-level calls

    def _with_membership(self, cid, fn, **kwargs):
        """Call fn; if the bot isn't in the channel, try joining once (public channels)."""
        try:
            return fn(**kwargs)
        except SlackApiError as e:
            if e.response.get("error") != "not_in_channel":
                raise
            try:
                self.client.conversations_join(channel=cid)
            except SlackApiError as je:
                _die(f"The bot is not a member of channel {cid} and couldn't join it "
                     f"({je.response.get('error')}). Invite it from Slack: `/invite @<bot name>` "
                     "in the channel (required for private channels).")
            return fn(**kwargs)

    def _footer(self, mention):
        parts = []
        if self.config.get("origin_tag", True):
            parts.append(f"_— {_escape(_origin_tag())}_")
        if mention and self.config.get("mention_recipient", True) and self.config.get("recipient"):
            parts.append(f"<@{self.recipient_id()}>")
        return " · ".join(parts)

    def post(self, cid, body, thread_ts=None, header=None, metadata=None, mention=True):
        footer = self._footer(mention)
        md = (f"🧵 **{header}**\n\n{body}" if body else f"🧵 **{header}**") if header else body
        text = _escape(body)
        if header:
            text = f"🧵 *{_escape(header)}*" + (f"\n\n{text}" if body else "")
        if footer:
            text = f"{text}\n\n{footer}"
        kwargs = dict(channel=cid, text=text, thread_ts=thread_ts, unfurl_links=False)
        if metadata:
            kwargs["metadata"] = metadata
        if self._blocks_ok and md.strip() and len(md) <= MARKDOWN_BLOCK_LIMIT:
            blocks = [{"type": "markdown", "text": md}]
            if footer:
                blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": footer}]})
            try:
                return self._with_membership(cid, self.client.chat_postMessage, blocks=blocks, **kwargs)
            except SlackApiError as e:
                if e.response.get("error") not in ("invalid_blocks", "invalid_blocks_format"):
                    raise
                self._blocks_ok = False  # workspace doesn't take markdown blocks; plain text
        return self._with_membership(cid, self.client.chat_postMessage, **kwargs)

    def _history(self, cid, oldest=None, limit=None, include_all_metadata=False):
        n = 0
        kwargs = dict(channel=cid, limit=200, include_all_metadata=include_all_metadata)
        if oldest:
            kwargs["oldest"] = oldest
        for page in self._with_membership(cid, self.client.conversations_history, **kwargs):
            for m in page["messages"]:
                yield m
                n += 1
                if limit and n >= limit:
                    return

    def _replies(self, cid, thread_ts, oldest=None):
        kwargs = dict(channel=cid, ts=thread_ts, limit=200)
        if oldest:
            kwargs["oldest"] = oldest
        out = []
        for page in self._with_membership(cid, self.client.conversations_replies, **kwargs):
            out.extend(page["messages"])
        return out

    # ---- topics

    def _root_topic(self, msg):
        if not self._is_mine(msg):
            return None
        md = msg.get("metadata") or {}
        if md.get("event_type") == TOPIC_EVENT_TYPE:
            return (md.get("event_payload") or {}).get("topic")
        m = TOPIC_TEXT_RE.match(msg.get("text", ""))
        return _decode(m.group(1)) if m else None

    def find_thread(self, cid, topic):
        key = f"{cid}:{topic}"
        ts = self.cache.get("threads", key)
        if ts:
            return ts
        for msg in self._history(cid, limit=HISTORY_SCAN_LIMIT, include_all_metadata=True):
            if self._root_topic(msg) == topic:
                self.cache.put("threads", key, msg["ts"])
                return msg["ts"]
        return None

    def list_topics(self, cid):
        seen = []
        for msg in self._history(cid, limit=HISTORY_SCAN_LIMIT, include_all_metadata=True):
            t = self._root_topic(msg)
            if t and t not in seen:
                seen.append(t)
                self.cache.put("threads", f"{cid}:{t}", msg["ts"])
        return seen

    def _create_thread(self, cid, topic, body=""):
        r = self.post(cid, body, header=topic,
                      metadata={"event_type": TOPIC_EVENT_TYPE, "event_payload": {"topic": topic}})
        self.cache.put("threads", f"{cid}:{topic}", r["ts"])
        return r

    # ---- commands

    def send_topic(self, cid, topic, body):
        thread_ts = self.find_thread(cid, topic)
        if thread_ts is None:
            r = self._create_thread(cid, topic, body)
            return r["ts"], r["ts"], True
        r = self.post(cid, body, thread_ts=thread_ts)
        return r["ts"], thread_ts, False

    def send_thread(self, cid, thread_ts, body):
        return self.post(cid, body, thread_ts=thread_ts)["ts"]

    def send_dm(self, body):
        r = self.post(self.dm_channel(), body, mention=False)
        return r["ts"]

    def send_file(self, path, caption=None, cid=None, topic=None, thread_ts=None):
        if not os.path.isfile(path):
            _die(f"No such file: {path}")
        if thread_ts:
            mention = True
        elif topic:
            thread_ts = self.find_thread(cid, topic)
            if thread_ts is None:
                thread_ts = self._create_thread(cid, topic)["ts"]
            mention = True
        else:
            cid, thread_ts, mention = self.dm_channel(), None, False
        footer = self._footer(mention)
        comment = "\n\n".join(p for p in (_escape(caption) if caption else "", footer) if p)
        kwargs = dict(channel=cid, file=path, filename=os.path.basename(path),
                      title=os.path.basename(path), thread_ts=thread_ts)
        if comment:
            kwargs["initial_comment"] = comment
        r = self._with_membership(cid, self.client.files_upload_v2, **kwargs)
        f = r.get("file") or (r.get("files") or [{}])[0]
        return f.get("id"), thread_ts

    def _emit(self, m):
        out = {
            "sender": m.get("user"),
            "content": _decode(m.get("text", "")),
            "timestamp": _tsf(m["ts"])[0],
            "ts": m["ts"],
        }
        if m.get("files"):
            out["files"] = [{"name": f.get("name"), "url": f.get("url_private")} for f in m["files"]]
        print(json.dumps(out, ensure_ascii=False), flush=True)

    def _wanted(self, m, rid):
        return (not self._is_mine(m) and m.get("user") == rid
                and m.get("subtype") in REPORTABLE_SUBTYPES)

    def listen(self, fetch, initial, interval, once):
        """Generic poll loop. fetch(cursor) -> list of messages newer than cursor."""
        rid = self.recipient_id()
        mine = [m["ts"] for m in initial if self._is_mine(m)]
        cursor = max(mine, key=_tsf) if mine else f"{time.time():.6f}"
        seen = set()
        backoff = interval
        while True:
            msgs, ok = self._poll(lambda: fetch(cursor))
            backoff = interval if ok else min(backoff * 2, 120)
            msgs = msgs or []
            for m in sorted(msgs, key=lambda m: _tsf(m.get("ts"))):
                ts = m.get("ts")
                if not ts or _tsf(ts) <= _tsf(cursor) or ts in seen:
                    continue
                seen.add(ts)
                if self._wanted(m, rid):
                    self._emit(m)
                    if once:
                        return
            if msgs:
                cursor = max([cursor] + [m["ts"] for m in msgs if m.get("ts")], key=_tsf)
            time.sleep(backoff)

    @staticmethod
    def _poll(fetch):
        """Run fetch() -> (result, ok). Exits on fatal Slack errors; transient ones
        (network hiccups on a login node, exhausted rate-limit retries) just warn."""
        try:
            return fetch(), True
        except SlackApiError as e:
            err = e.response.get("error")
            if err in FATAL_ERRORS:
                _die(f"FAILED: {err} ({e.response.get('needed', '')})".rstrip(" ()"), sys.stdout)
            print(f"notify-slack: transient error {err!r}, retrying", file=sys.stderr, flush=True)
        except Exception as e:
            print(f"notify-slack: {type(e).__name__}: {e}, retrying", file=sys.stderr, flush=True)
        return None, False

    def listen_topic(self, cid, topic, interval, once, top_level):
        thread_ts = self.find_thread(cid, topic)
        if thread_ts is None:
            _die(f"FAILED: no thread for topic {topic!r} in this channel yet — "
                 "`send --topic` to it first.", sys.stdout)
        self.listen_thread(cid, thread_ts, interval, once, top_level)

    def listen_thread(self, cid, thread_ts, interval, once, top_level):
        def fetch(cursor):
            msgs = [m for m in self._replies(cid, thread_ts, oldest=cursor) if m["ts"] != thread_ts]
            if top_level:
                msgs += [m for m in self._history(cid, oldest=cursor)
                         if m.get("thread_ts") in (None, m["ts"]) and not self._is_mine(m)]
            return msgs

        self.listen(fetch, self._replies(cid, thread_ts), interval, once)

    def listen_dm(self, interval, once):
        cid = self.dm_channel()
        self.listen(lambda cursor: list(self._history(cid, oldest=cursor)),
                    list(self._history(cid, limit=50)), interval, once)

    # ---- inbox / watch
    #
    # "Wake" triggers (from an allowed sender, see wake_from):
    #   mention — the bot is @-mentioned anywhere in the channel (top level or any thread)
    #   reply   — a reply in a thread the bot started or has posted in
    #   dm      — any DM to the bot
    # `watch` prints one line per trigger; `inbox` prints EVERYTHING unread (all senders,
    # all threads, triggers or not) since the last `inbox`, then marks it read.

    def _name(self, uid):
        if not uid:
            return "?"
        if uid == self.me["user_id"]:
            return self.me["user"]
        key = f"{self.me['team_id']}:{uid}"
        n = self.cache.get("names", key)
        if n is None:
            try:
                u = self.client.users_info(user=uid)["user"]
                n = ((u.get("profile") or {}).get("display_name") or u.get("real_name")
                     or u.get("name") or uid)
            except SlackApiError:
                n = uid
            self.cache.put("names", key, n)
        return n

    def _chan_label(self, cid):
        if cid not in self._labels:
            try:
                self._labels[cid] = "#" + self.client.conversations_info(channel=cid)["channel"]["name"]
            except SlackApiError:
                self._labels[cid] = cid
        return self._labels[cid]

    def _bot_thread(self, root):
        return root is not None and (self._is_mine(root)
                                     or self.me["user_id"] in (root.get("reply_users") or []))

    def _trigger(self, m, root, dm):
        if self._is_mine(m) or m.get("bot_id") or m.get("subtype") not in REPORTABLE_SUBTYPES:
            return None
        if dm:
            return "dm"
        if re.search(rf"<@{self.me['user_id']}(?:\|[^>]*)?>", m.get("text", "")):
            return "mention"
        if self._bot_thread(root):
            return "reply"
        return None

    def channel_members(self, cid):
        """Member IDs of a channel, cached for MEMBERS_TTL seconds."""
        hit = self._members.get(cid)
        if hit and time.time() - hit[0] < MEMBERS_TTL:
            return hit[1]
        members = set()
        for page in self.client.conversations_members(channel=cid, limit=1000):
            members.update(page["members"])
        self._members[cid] = (time.time(), members)
        return members

    def _may_wake(self, item):
        """wake_from: "recipient" (default) | "channel" (current members of the served
        channel) | "anyone" | a list mixing those keywords and member IDs."""
        allow = self.config.get("wake_from", "recipient")
        allow = allow if isinstance(allow, list) else [allow]
        sender = item["sender"]
        for a in allow:
            if a == "anyone":
                return True
            if a == "recipient" and self.config.get("recipient") and sender == self.recipient_id():
                return True
            if a == "channel" and self._served and sender in self.channel_members(self._served):
                return True
            if a == sender:
                return True
        return False

    def add_reaction(self, cid, ts, name, remove=False):
        """Best effort (needs reactions:write); silently skipped without it."""
        if self._no_reactions:
            return
        try:
            fn = self.client.reactions_remove if remove else self.client.reactions_add
            fn(channel=cid, timestamp=ts, name=name)
        except SlackApiError as e:
            if e.response.get("error") in ("missing_scope", "not_allowed_token_type"):
                self._no_reactions = True

    def transcript(self, cid, thread_ts=None, limit=20):
        """Human-readable lines: a whole thread, or the last `limit` top-level posts."""
        if thread_ts:
            msgs = self._replies(cid, thread_ts)
        else:
            msgs = list(reversed(list(self._history(cid, limit=limit))))
        lines = []
        for m in msgs:
            if m.get("subtype") not in REPORTABLE_SUBTYPES and m.get("subtype") != "bot_message":
                continue
            who = (f"{self.me['user']} (you, the bot)" if self._is_mine(m)
                   else self._name(m.get("user")) if m.get("user") else (m.get("username") or "bot"))
            when = time.strftime("%m-%d %H:%M", time.localtime(_tsf(m["ts"])[0]))
            text = _decode(m.get("text", ""), self._name).strip()
            if m.get("files"):
                text += " [files: " + ", ".join(f.get("name", "?") for f in m["files"]) + "]"
            extra = ""
            if not thread_ts and m.get("reply_count"):
                extra = f"  ({m['reply_count']} replies; thread ts {m['ts']})"
            lines.append(f"[{when}] {who}: {text}{extra}")
        return lines

    def _annotate(self, m, cid, root=None, dm=False):
        if dm:
            where, reply_args, thread_ts = "DM", "--dm", None
        else:
            thread_ts = root["ts"] if root else None
            label = self._chan_label(cid)
            if root is None:
                where = label
            else:
                topic = self._root_topic(root)
                preview = _decode(root.get("text", ""), self._name).replace("\n", " ")
                where = (f"{label} › 🧵 {topic}" if topic
                         else f"{label} › thread “{preview[:60]}{'…' if len(preview) > 60 else ''}”")
            reply_args = f"--channel {cid} --thread {thread_ts or m['ts']}"
        trig = self._trigger(m, root, dm)
        item = {
            "ts": m["ts"],
            "time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(_tsf(m["ts"])[0])),
            "where": where,
            "sender": m.get("user") or m.get("bot_id"),
            "sender_name": self._name(m.get("user")) if m.get("user") else (m.get("username") or "bot"),
            "content": _decode(m.get("text", ""), self._name),
            "trigger": trig,
            "reply_args": reply_args,
            "channel": cid,
            "thread_ts": thread_ts,
        }
        item["wakes"] = bool(trig) and self._may_wake(item)
        if m.get("files"):
            item["files"] = [{"name": f.get("name"), "url": f.get("url_private")} for f in m["files"]]
        return item

    def _gather(self, cid, oldest, dm=True):
        """All human/other-bot messages newer than `oldest` (excluding the bot's own): top
        level of `cid`, replies in threads under its last THREAD_SCAN_WINDOW top-level
        posts, and (if dm) the DM with recipient. Sorted oldest first."""
        self._served = cid
        out = {}
        later = lambda ts: _tsf(ts) > _tsf(oldest)
        recent = list(self._history(cid, limit=THREAD_SCAN_WINDOW, include_all_metadata=True))
        if len(recent) >= THREAD_SCAN_WINDOW and later(recent[-1]["ts"]):  # big backlog
            known = {m["ts"] for m in recent}
            recent += [m for m in self._history(cid, oldest=oldest, include_all_metadata=True)
                       if m["ts"] not in known]
        for root in recent:
            if root.get("reply_count") and later(root.get("latest_reply")):
                for r in self._replies(cid, root["ts"], oldest=oldest):
                    if r["ts"] != root["ts"] and later(r["ts"]) and not self._is_mine(r):
                        out[(cid, r["ts"])] = (r, cid, root, False)
        for m in recent:
            if later(m["ts"]) and (cid, m["ts"]) not in out and not self._is_mine(m):
                out[(cid, m["ts"])] = (m, cid, None, False)
        if dm:
            dcid = self.dm_channel()
            for m in self._history(dcid, oldest=oldest):
                if not self._is_mine(m):
                    out[(dcid, m["ts"])] = (m, dcid, None, True)
        items = [self._annotate(m, c, root=r, dm=d) for (m, c, r, d) in out.values()
                 if m.get("subtype") in REPORTABLE_SUBTYPES]
        return sorted(items, key=lambda a: _tsf(a["ts"]))

    def _read_key(self, cid, dm, namespace=""):
        return f"{namespace}{self.me['team_id']}:{cid}" + (":dm" if dm else "")

    def _read_state(self, key, fresh=False):
        st = self.state.get("read", key, fresh=fresh)
        if st is None:  # first use: start from now, don't dump the whole history
            st = {"cursor": f"{time.time():.6f}", "delivered": []}
            self.state.put("read", key, st)
        return st

    def inbox(self, cid, dm, since=None, peek=False, namespace=""):
        key = self._read_key(cid, dm, namespace)
        st = self._read_state(key, fresh=True)
        start = time.time()
        cursor = since or st["cursor"]
        delivered = set() if since else set(st["delivered"])
        items = [a for a in self._gather(cid, cursor, dm)
                 if f"{a['channel']}:{a['ts']}" not in delivered]
        if not peek:
            new_cursor = max(st["cursor"], f"{start - CLOCK_SKEW:.6f}", key=_tsf)
            keep = set(st["delivered"]) | {f"{a['channel']}:{a['ts']}" for a in items}
            keep = sorted(k for k in keep if _tsf(k.rsplit(":", 1)[1]) > _tsf(new_cursor))
            self.state.put("read", key, {"cursor": new_cursor, "delivered": keep})
        return items

    def iter_wakes(self, cid, dm, interval, namespace="", on_idle=None):
        """Poll forever; yield a list of new wake items (not yet consumed by `inbox` in the
        same namespace) whenever a poll finds some. on_idle() is called every poll."""
        key = self._read_key(cid, dm, namespace)
        st = self._read_state(key, fresh=True)
        cursor, emitted = st["cursor"], set(st["delivered"])
        backoff = interval
        while True:
            if on_idle:
                on_idle()
            start = time.time()
            items, ok = self._poll(lambda: self._gather(cid, cursor, dm))
            backoff = interval if ok else min(backoff * 2, 120)
            if ok:
                read = self._read_state(key, fresh=True)  # skip what `inbox` already consumed
                read_set = set(read["delivered"])
                wakes = []
                for a in items:
                    k = f"{a['channel']}:{a['ts']}"
                    if k in emitted:
                        continue
                    emitted.add(k)
                    if (a["wakes"] and _tsf(a["ts"]) > _tsf(read["cursor"])
                            and k not in read_set):
                        wakes.append(a)
                cursor = max(cursor, f"{start - CLOCK_SKEW:.6f}", key=_tsf)
                emitted = {k for k in emitted if _tsf(k.rsplit(":", 1)[1]) > _tsf(cursor)}
                if wakes:
                    yield wakes
            time.sleep(backoff)

    def watch(self, cid, dm, interval):
        for wakes in self.iter_wakes(cid, dm, interval):
            for a in wakes:
                ev = {"event": "wake", "trigger": a["trigger"], "where": a["where"],
                      "sender_name": a["sender_name"], "content": a["content"],
                      "ts": a["ts"], "reply_args": a["reply_args"]}
                print(json.dumps(ev, ensure_ascii=False), flush=True)

    def check(self, channel, dm):
        ok = True
        me = self.me
        print(f"workspace : {me['team']} ({me['url']})")
        print(f"bot user  : @{me['user']} ({me['user_id']}, bot_id {me['bot_id']})")
        scopes = set(filter(None, (me["scopes"] or "").split(",")))
        if scopes:
            missing = [s for s in RECOMMENDED_SCOPES if s not in scopes]
            for s in missing:
                print(f"  missing scope {s:18s} — {RECOMMENDED_SCOPES[s]}")
            for s in OPTIONAL_SCOPES:
                if s not in scopes:
                    print(f"  optional scope {s:17s} — {OPTIONAL_SCOPES[s]}")
            if not missing:
                print("scopes    : all recommended scopes present")
            ok &= not [s for s in missing if not s.startswith("groups:")]
        if self.config.get("recipient"):
            try:
                rid = self.recipient_id()
                u = self.client.users_info(user=rid)["user"]
                print(f"recipient : {u.get('real_name') or u.get('name')} ({rid})")
            except SlackApiError as e:
                ok = False
                print(f"recipient : FAILED to resolve {self.config['recipient']!r}: {e.response.get('error')}")
        else:
            ok = False
            print("recipient : NOT SET (needed for listen and --dm)")
        if channel and channel != "CHANGE_ME":
            try:
                cid = self.channel_id(channel)
                c = self.client.conversations_info(channel=cid)["channel"]
                kind = "private" if c.get("is_private") else "public"
                member = c.get("is_member")
                print(f"channel   : #{c.get('name')} ({cid}, {kind}, bot is "
                      f"{'a member' if member else 'NOT a member'})")
                if not member:
                    print("            → " + ("will auto-join on first send if channels:join is granted, "
                                             "else /invite the bot" if kind == "public"
                                             else "run `/invite @<bot name>` in the channel"))
            except SlackApiError as e:
                ok = False
                print(f"channel   : FAILED to look up {channel!r}: {e.response.get('error')}")
        else:
            print("channel   : not configured")
        if dm:
            try:
                print(f"dm        : {self.dm_channel()}")
            except SlackApiError as e:
                ok = False
                print(f"dm        : FAILED to open: {e.response.get('error')}")
        print("OK" if ok else "Problems found (see above).")
        return ok


def _header(resp, name):
    for k, v in (getattr(resp, "headers", None) or {}).items():
        if k.lower() == name:
            return v[0] if isinstance(v, list) else v
    return None


# ----------------------------------------------------------------------------- cli

def cmd_init():
    if os.path.isfile(PROJECT_CONFIG_PATH):
        _die(f"{PROJECT_CONFIG_PATH} already exists, not overwriting.")
    _write_json(PROJECT_CONFIG_PATH, {"channel": "CHANGE_ME"})
    print(f'Wrote {PROJECT_CONFIG_PATH} — set "channel" to the channel name (e.g. "#my-project") '
          "or ID.")
    if not os.path.isfile(MACHINE_CONFIG_PATH):
        _write_json(MACHINE_CONFIG_PATH, {"bot_token": "", "recipient": ""}, mode=0o600)
        print(f'Also created {MACHINE_CONFIG_PATH} (mode 600) — fill in "bot_token" (xoxb-...) '
              'and "recipient" (your Slack member ID) once for this machine.')
    print("Then run `notify-slack check` to verify.")


def cmd_task(args):
    from .tasks import Tasks, FINAL
    state_dir = os.path.join(os.path.dirname(PROJECT_CONFIG_PATH), ".slack_assistant")
    if not os.path.isdir(state_dir):
        _die(f"No {state_dir} — `notify-slack serve` hasn't run in this project.")
    reg = Tasks(state_dir)
    if args.action == "list":
        lines = reg.summary_lines(limit=50)
        print("\n".join(lines) if lines else "(no tasks)")
        return
    if not args.id:
        _die(f"`task {args.action}` needs a task id (see `notify-slack task list`).")
    t = reg.get(args.id.lower())
    if not t:
        _die(f"No task {args.id}.")
    if args.action == "show":
        print(json.dumps(t, indent=2, ensure_ascii=False))
        res = os.path.join(reg.dir(t["id"]), "result.md")
        if os.path.exists(res):
            print(f"\nreport: {res}")
        return
    if t["status"] in FINAL:
        _die(f"{t['id']} is already {t['status']}.")
    t["control"] = args.action
    reg.save(t)
    print(f"Requested {args.action} of {t['id']}; the running dispatcher applies it within ~15 s.")


def _parse_since(v):
    if not v:
        return None
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", v.strip())
    if m:
        secs = float(m.group(1)) * {"s": 1, "m": 60, "h": 3600, "d": 86400}[m.group(2)]
        return f"{time.time() - secs:.6f}"
    if re.fullmatch(r"\d{9,}(?:\.\d+)?", v.strip()):
        return v.strip()
    _die(f"--since: expected e.g. 30m, 2h, 1d or a Slack ts, got {v!r}")


def _build_parser():
    parser = argparse.ArgumentParser(prog="notify-slack",
                                     description="Two-way Slack bridge (channel+thread or DM).")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init", help="Scaffold ./slack_bridge.json (and the machine config if missing).")

    p_check = sub.add_parser("check", help="Verify token, scopes, recipient and channel access.")
    p_check.add_argument("--channel", default=None)
    p_check.add_argument("--dm", action="store_true", help="Also check that the DM can be opened.")

    p_send = sub.add_parser("send", help="Send a message (channel thread by default, or --dm).")
    p_send.add_argument("message", help='Message text (Markdown). "-" reads it from stdin.')
    p_send.add_argument("--channel", default=None)
    p_send.add_argument("--topic", default=None)
    p_send.add_argument("--thread", default=None, metavar="TS",
                        help="Use the thread rooted at this message ts (e.g. from inbox's reply_args).")
    p_send.add_argument("--dm", action="store_true", help="Send as a DM to recipient instead.")

    p_file = sub.add_parser("send_file", help="Upload+send a file (channel thread by default, or --dm).")
    p_file.add_argument("path")
    p_file.add_argument("caption", nargs="?", default=None)
    p_file.add_argument("--channel", default=None)
    p_file.add_argument("--topic", default=None)
    p_file.add_argument("--thread", default=None, metavar="TS",
                        help="Use the thread rooted at this message ts (e.g. from inbox's reply_args).")
    p_file.add_argument("--dm", action="store_true", help="Send as a DM to recipient instead.")

    p_listen = sub.add_parser("listen", help="Block and print replies (channel thread by default, or --dm).")
    p_listen.add_argument("--channel", default=None)
    p_listen.add_argument("--topic", default=None)
    p_listen.add_argument("--thread", default=None, metavar="TS",
                        help="Use the thread rooted at this message ts (e.g. from inbox's reply_args).")
    p_listen.add_argument("--dm", action="store_true", help="Listen for DMs instead.")
    p_listen.add_argument("--interval", type=float, default=5.0, help="Poll interval, seconds (default 5).")
    p_listen.add_argument("--once", action="store_true", help="Exit after the first reply.")
    p_listen.add_argument("--include-top-level", action="store_true",
                          help="Also accept recipient's top-level channel messages, not just thread replies.")

    p_watch = sub.add_parser("watch", help="Block; print one line per wake trigger (mention / reply in a "
                                           "bot thread / DM). Then run `inbox`.")
    p_watch.add_argument("--channel", default=None)
    p_watch.add_argument("--no-dm", action="store_true", help="Don't watch the DM with recipient.")
    p_watch.add_argument("--interval", type=float, default=10.0, help="Poll interval, seconds (default 10).")

    p_inbox = sub.add_parser("inbox", help="Print every unread message (channel, its threads, DM) as JSON "
                                           "lines and mark them read.")
    p_inbox.add_argument("--channel", default=None)
    p_inbox.add_argument("--no-dm", action="store_true", help="Leave the DM out.")
    p_inbox.add_argument("--peek", action="store_true", help="Show without marking read.")
    p_inbox.add_argument("--since", default=None,
                         help="Ignore the read cursor; show messages from this far back (30m, 2h, 1d, or a ts).")

    p_read = sub.add_parser("read", help="Print the last N top-level posts of the channel, or a whole "
                                         "thread with --thread (read-only, human-readable).")
    p_read.add_argument("--channel", default=None)
    p_read.add_argument("--thread", default=None, metavar="TS")
    p_read.add_argument("--limit", type=int, default=20)

    p_serve = sub.add_parser("serve", help="Run the always-on Slack assistant for this project "
                                           "(dispatcher + headless central Claude agent).")
    p_serve.add_argument("--once", action="store_true", help="Handle one batch of wake messages, then exit.")
    p_serve.add_argument("--dry-run", action="store_true",
                         help="With --once: print the prompt instead of running Claude or posting.")
    p_serve.add_argument("--status", action="store_true", help="Show whether serve is running, then exit.")
    p_serve.add_argument("--new-session", action="store_true",
                         help="Start a fresh central-agent session on the next event.")

    p_task = sub.add_parser("task", help="List/inspect serve's worker tasks, or approve/reject/cancel one.")
    p_task.add_argument("action", choices=["list", "show", "approve", "reject", "cancel"])
    p_task.add_argument("id", nargs="?")

    p_topics = sub.add_parser("list-topics", help="List topics (bot-created threads) in the channel.")
    p_topics.add_argument("--channel", default=None)

    return parser


def main():
    args = _build_parser().parse_args()
    if args.cmd == "init":
        cmd_init()
        return

    if args.cmd == "task":
        cmd_task(args)
        return
    config = _load_config()
    if args.cmd == "serve":
        from . import serve
        serve.main(args, config)
        return
    bridge = Bridge(config)
    channel = getattr(args, "channel", None) or config.get("channel")
    dm = getattr(args, "dm", False)

    if args.cmd in ("send", "send_file", "listen"):
        given = [x for x in ("topic", "thread", "dm") if getattr(args, x)]
        if len(given) != 1:
            _die("Give exactly one of --topic, --thread or --dm.")

    try:
        if args.cmd == "check":
            sys.exit(0 if bridge.check(channel, dm) else 1)

        if args.cmd == "send":
            body = sys.stdin.read() if args.message == "-" else args.message
            if not body.strip():
                _die("Refusing to send an empty message.")
            if dm:
                print(f"Sent DM (ts={bridge.send_dm(body)})")
            elif args.thread:
                ts = bridge.send_thread(bridge.channel_id(channel), args.thread, body)
                print(f"Sent (ts={ts}, thread={args.thread})")
            else:
                ts, thread_ts, created = bridge.send_topic(bridge.channel_id(channel), args.topic, body)
                print(f"Sent (ts={ts}, thread={thread_ts}{', new topic' if created else ''})")
        elif args.cmd == "send_file":
            cid = None if dm else bridge.channel_id(channel)
            fid, thread_ts = bridge.send_file(args.path, args.caption, cid=cid,
                                              topic=None if dm else args.topic,
                                              thread_ts=None if dm else args.thread)
            print(f"Sent file (id={fid}" + (f", thread={thread_ts})" if thread_ts else ")"))
        elif args.cmd == "listen":
            if dm:
                bridge.listen_dm(args.interval, args.once)
            elif args.thread:
                bridge.listen_thread(bridge.channel_id(channel), args.thread,
                                     args.interval, args.once, args.include_top_level)
            else:
                bridge.listen_topic(bridge.channel_id(channel), args.topic,
                                    args.interval, args.once, args.include_top_level)
        elif args.cmd == "watch":
            bridge.watch(bridge.channel_id(channel), not args.no_dm, args.interval)
        elif args.cmd == "inbox":
            items = bridge.inbox(bridge.channel_id(channel), not args.no_dm,
                                 since=_parse_since(args.since), peek=args.peek)
            for a in items:
                print(json.dumps(a, ensure_ascii=False))
            n_wake = sum(a["wakes"] for a in items)
            print(f"{len(items)} unread ({n_wake} addressed to the bot)"
                  + ("" if args.peek else "; marked read") + ".", file=sys.stderr)
        elif args.cmd == "read":
            cid = bridge.channel_id(channel)
            locked = os.environ.get("NOTIFY_SLACK_LOCK_CHANNEL")
            if locked and cid != locked:
                _die(f"FAILED: this session may only read channel {locked}.", sys.stdout)
            for line in bridge.transcript(cid, args.thread, args.limit):
                print(line)
        elif args.cmd == "list-topics":
            for t in bridge.list_topics(bridge.channel_id(channel)):
                print(t)
    except SlackApiError as e:
        err = e.response.get("error")
        hint = ""
        if err == "missing_scope":
            hint = (f" — needs scope {e.response.get('needed')!r}; add it under OAuth & Permissions "
                    "and reinstall the app.")
        elif err in ("invalid_auth", "not_authed", "token_revoked"):
            hint = " — check bot_token."
        _die(f"FAILED: {err}{hint}", sys.stdout)
    except KeyboardInterrupt:
        sys.exit(130)


if __name__ == "__main__":
    main()
