# slack_bridge

A two-way Slack bridge for Claude Code (or any script) to post progress updates and
wait for replies during long-running work, without you having to stay in the terminal.
Send a message when a step finishes, arm a listener when a decision is needed, and pick
up the reply from Slack whenever it comes. It is a port of `zulip_bridge`, with the same
commands and workflow.

Installs as a `notify-slack` CLI command. One Slack app (bot) serves every project on a
machine, and each project points at its own channel through a small per-project config
file. Public and private channels both work.

## How Zulip concepts map to Slack

| Zulip            | Slack                                                                 |
|------------------|-----------------------------------------------------------------------|
| stream           | channel (public or private)                                           |
| topic            | thread. The first `send --topic X` posts a root message `🧵 *X*` and later sends with the same topic reply in that thread |
| DM               | DM from the bot to `recipient`                                        |

## Install

```bash
pip install -e ./slack_bridge        # into whichever env the calling project uses
```

## Setup

### 1. Slack app (once per workspace)

In <https://api.slack.com/apps>, open your app and go to **OAuth & Permissions → Bot Token Scopes**.
Add these scopes:

| scope | used for |
|---|---|
| `chat:write` | sending messages |
| `channels:history`, `channels:read` | public channels: reading threads, resolving `#name` |
| `groups:history`, `groups:read` | private channels: the same |
| `im:write`, `im:history` | `--dm` |
| `files:write` | `send_file` |
| `users:read` | `check`, user lookups |
| `channels:join` *(optional)* | lets the bot join a public channel by itself |
| `users:read.email` *(optional)* | lets `recipient` be an email address |

Then click **Install/Reinstall to Workspace**. You have to reinstall after any scope change.
Copy the **Bot User OAuth Token** (`xoxb-…`).

Invite the bot to the channel by typing `/invite @<bot name>` in the channel. A private
channel needs this. For a public channel, `channels:join` lets the bot join without it.

### 2. Machine config (once per machine)

`~/.config/notify_slack/config.json` (mode 600; `notify-slack init` creates a blank one):

```json
{
  "bot_token": "xoxb-...",
  "recipient": "U0123ABCD"
}
```

`recipient` is your Slack member ID (Profile → ⋮ → **Copy member ID**), or your email if
the app has `users:read.email`. It is the only person whose replies are accepted, and
the person DMs go to. `$SLACK_BOT_TOKEN`, if set, overrides `bot_token`.

### 3. Project config (once per project)

From the project root:

```bash
notify-slack init     # writes ./slack_bridge.json = {"channel": "CHANGE_ME"}
```

Set `"channel"` to the channel name (`"#my-project"`) or its ID (`"C0…"`, found under
channel details → About). Then run:

```bash
notify-slack check    # token, scopes, recipient, channel visibility & membership
```

Settings from the machine config are inherited, and any key can be overridden per
project: `recipient`, `bot_token`, `origin_tag`, `mention_recipient`, `format`. Keep
`slack_bridge.json` out of git if you ever put a token in it.

## Usage

```bash
# Send to a topic (thread) in the configured channel; standard Markdown renders
notify-slack send --topic "my-task" "Training run finished, results in **results/run3**."
echo "long message" | notify-slack send --topic "my-task" -

# List existing topics (bot-created threads) in the channel
notify-slack list-topics

# Block and print replies in that thread, one JSON line per message:
#   {"sender": "U...", "content": "...", "timestamp": 1730000000, "ts": "1730000000.000100"}
notify-slack listen --topic "my-task"
notify-slack listen --topic "my-task" --once                # exit after the first reply
notify-slack listen --topic "my-task" --include-top-level   # also accept top-level channel posts

# Upload a file into the thread (images render inline)
notify-slack send_file results/plot.png "Latest fit" --topic "my-task"

# DM instead of channel+thread
notify-slack send --dm "quick heads up"
notify-slack listen --dm
```

### Watch / inbox mode (wake on mention, read everything unread)

`listen` waits for a reply in one known thread. To leave the bot "on call" instead, use
`watch` and `inbox`:

```bash
notify-slack watch            # blocks; one JSON line per wake trigger
notify-slack inbox            # print everything unread, then mark it read
notify-slack inbox --peek     # print without marking read
notify-slack inbox --since 2h # ignore the read position; show the last 2 hours
```

**Wake triggers.** Only these produce a `watch` event:

- `mention`: `@<bot>` anywhere in the channel, top level or in any thread.
- `reply`: a reply in a thread the bot started, or one it has posted in.
- `dm`: any DM to the bot.

Other channel traffic doesn't wake it. By default only `recipient` can wake it, because
waking it is effectively giving it an instruction. Change this with `"wake_from"` in
`slack_bridge.json`: `"recipient"` (default), `"anyone"`, or a list of member IDs (which
may include `"recipient"`).

**`inbox`** prints *every* message the bot hasn't read since the last `inbox`. That
covers top-level posts, replies in any thread (under the last 200 top-level posts) and
the DM, from all senders, whether they are triggers or not. Each JSON line has:

- `where`: e.g. `#proj › 🧵 my-task` or `DM`
- `sender_name`, `content`, `time`
- `trigger`, and `wakes` (true when it's addressed to the bot by an allowed sender)
- `reply_args`: the arguments to pass to `send`/`listen` to answer it in place,
  e.g. `--channel C0… --thread 1793….` or `--dm`

A top-level mention is answered in a new thread under that message.

The read position is stored per channel in `~/.local/state/notify_slack/read_state.json`.
The first use starts from "now", so old history isn't dumped. `--no-dm` leaves the DM out
of both commands. Run only one `watch` per channel at a time. DMs are shared by every
project using the same bot, so each project's `watch` will see them unless it uses
`--no-dm`. Users can only DM the bot if the app's **App Home → Messages Tab** allows it.

`send`, `send_file` and `listen` also accept `--thread <ts>`, to work in any thread, not
just the bot's own topics.

### Always-on project assistant (`serve`)

`notify-slack serve` runs a Slack assistant for the project, so you and your
collaborators can ask about progress, results and job status at any time:

```
dispatcher (`notify-slack serve`, in tmux, restarted by cron)  ← the only thing always running
  polls the channel + owner DM for wake triggers (mention / reply in a bot thread / DM)
  └─ on a wake: all unread messages + recent channel posts + the shared log
       └─ CENTRAL AGENT: one Claude Code session, resumed with `claude -p --resume`
          read-only tools (Read/Grep/Glob + allowlisted Bash such as squeue/sacct/ls)
          returns JSON {"replies": [{target, text, files}], "notes": [...]}
  └─ dispatcher posts the replies (👀 while working, ✅ when done) and appends notes to the log
```

- **Who can wake it:** set `"wake_from": "channel"` in `slack_bridge.json`, so any
  current member of the served channel can. Anyone in the workspace can join a public
  channel, so use a **private** channel for real projects.
- **What it can do:** read the workspace (plus symlinked data dirs and this project's
  Claude memory) and run allowlisted read-only commands. It can't edit files or run
  jobs. It can only reply to conversations it was given, plus a DM to you for
  escalations. Reads under `~/.ssh`, `~/.config`, `~/.claude` and similar are denied.
- **Memory:** the central session keeps the recent chat. Anything durable goes to
  `.slack_assistant/log.md`, which is shown every turn and survives session resets. A
  session that grows past `rotate_tokens` is replaced by a fresh one.
- **Your own instructions:** put them in `.slack_assistant/instructions.md`, e.g. what
  the project is, which files hold status, and what must not be shared.
- **Cost:** every handled batch is a headless Claude Code run billed to your account,
  capped per run by `max_budget_usd`. `.slack_assistant/events.jsonl` logs cost,
  duration and errors.

Running it:

```bash
notify-slack serve --status                     # is it alive? which session?
tmux new -d -s slack-assistant 'cd /path/to/project && notify-slack serve >> .slack_assistant/serve.log 2>&1'
# watchdog: restart it if it dies (install on ONE login node)
crontab -e   →   */5 * * * * /path/to/slack_bridge/scripts/serve-watchdog.sh /path/to/project /path/to/env/bin/notify-slack
notify-slack serve --once --dry-run             # show the prompt for the next batch, post nothing
notify-slack serve --new-session                # start the central agent fresh
```

Settings (all optional) go under `"serve"` in `slack_bridge.json`:

| key | default | what it does |
|---|---|---|
| `interval` | 10 | seconds between polls |
| `coalesce` | 4 | seconds to wait after a wake so quick follow-ups join the same batch |
| `timeout` | 900 | seconds per run |
| `max_budget_usd` | 1.0 | spend cap per run |
| `context_posts`, `log_entries` | 20, 40 | how much recent context each turn gets |
| `rotate_tokens` | 150000 | context size that triggers a fresh session |
| `model` | your default | model for the central agent |
| `dm` | true | also serve the owner's DM |
| `extra_allowed_bash` | [] | more Bash allow patterns, e.g. `"git log:*"` |
| `add_dirs`, `deny_read` | [] | extra readable dirs and extra Read deny patterns |

#### Long tasks: worker sessions (Phase 2)

The central agent stays read-only. When a request needs real work (editing files, running
analyses, submitting Slurm jobs, anything longer than a few minutes), it hands the work to a
**worker**: a separate `claude -p` run in Claude Code's `auto` permission mode, where a safety
check approves routine actions and blocks risky ones. The worker gets the same Read deny rules
for secrets.

```
request in Slack ─► central agent: "tasks": [{target, title, instructions}]
   └─► dispatcher creates tasks/<id>/task.json
         requester = owner        → queued → worker starts
         requester = collaborator → pending_approval → owner DM: "approve t003" / "reject t003"
   worker: writes tasks/<id>/result.md, final JSON {status, summary, files, jobs}
     status waiting_on_jobs → dispatcher watches squeue → resumes the worker when jobs leave the queue
   dispatcher → <task_event> → central agent reports the result in the original thread
```

- **Requester and approval.** The dispatcher decides who the requester is from the Slack
  message that woke the agent, not from anything the agent says. Your own tasks start
  immediately. A collaborator's task needs `approve t003` from **you, in the bot DM**, or the
  terminal command below. The dispatcher parses that itself, so no message can talk it into an
  approval.
- **Control from the terminal:**
  ```bash
  notify-slack task list
  notify-slack task show t003
  notify-slack task approve t003      # also: reject / cancel
  ```
  In Slack, `cancel t003` in the bot DM works, and so does asking the agent in the task's thread
  (only you or the task's requester can cancel).
- **Robustness.** Workers are detached processes, so they survive a dispatcher restart, which
  picks them up again from `task.json`. Each attempt is capped by `worker_timeout` and
  `worker_max_budget_usd`. At most `max_workers` run at once; the rest wait in the queue.
- **Model and thinking level per task.** Ask in Slack, e.g. "@bot … use sonnet, think hard". The
  central agent sets the task's `model`/`effort`. The dispatcher checks them against
  `worker_allowed_models` (fable, opus, sonnet, haiku) and `worker_allowed_efforts` (low, medium,
  high, xhigh, max), and passes them to the worker as `--model` / `--effort`. The choice is shown in
  the "On it" line, the approval prompt and `notify-slack task list`, and it is kept when a worker
  resumes after Slurm jobs. Without a request, `worker_model` / `worker_effort` apply; if those are
  unset, Claude Code's own defaults do.
- **Settings** (under `"serve"`): `max_workers` (2), `worker_timeout` (14400 s),
  `worker_max_budget_usd` (10), `worker_model`, `worker_effort`, `worker_permission_mode` (`"auto"`),
  `job_check_interval` (120 s), `job_wait_max_hours` (72).

**What `listen` reports.** It reports messages from `recipient` that are newer than the
bot's own most recent message in that thread or DM. A reply that arrives between `send`
and arming `listen` is still caught. Something already acknowledged (the bot posted after
it) is not reported again. Replies have to be posted *in the thread*. Top-level channel
messages are ignored unless you pass `--include-top-level`.

**Why polling.** `listen` polls every `--interval` seconds (default 5) instead of using
Socket Mode. Socket Mode delivers each event to only *one* of an app's open connections.
Several concurrent listeners (different projects, topics, or tmux sessions) would then
silently take each other's replies. Polling needs only the bot token, and transient
network or rate-limit errors are retried.

**Footer and notifications.** Every outgoing message gets a small footer with the host,
IP and tmux session it was sent from, e.g. `— gh-login03 (141.142.248.9) · tmux sampling-0`.
Set `"origin_tag": false` to drop it. In channels, the footer also @-mentions
`recipient`. Slack only notifies you about thread replies in threads you follow, so
without the mention you'd miss them. Set `"mention_recipient": false` to turn the
mention off.

**Formatting.** Messages go out as a Slack `markdown` block, so ordinary Markdown works:
`**bold**`, `[label](url)`, lists, and code fences. If a workspace rejects that block,
the bridge automatically falls back to plain mrkdwn text. Set `"format": "mrkdwn"` to
always use plain text.

**Caches.** Channel-name→ID, topic→thread and email→user-ID lookups are cached in
`~/.cache/notify_slack/cache.json`. The cache is safe to delete: topics are found again
from the channel history (the last 3000 messages).

**`conda run` gotcha**: if you invoke `notify-slack` via `conda run -n <env> ...`, always
add `--no-capture-output` for `listen`. Without it, stdout is buffered until the process
exits, and since `listen` runs forever, its output is lost.

## Using this with Claude

[`skills/notify-slack/`](skills/notify-slack/SKILL.md) is a Claude Code skill. It covers
when to notify and when to wait for a reply, topic naming, and ack-before-acting.
Install it globally once per machine:

```bash
mkdir -p ~/.claude/skills
ln -s "$(pwd)/skills/notify-slack" ~/.claude/skills/notify-slack
```
