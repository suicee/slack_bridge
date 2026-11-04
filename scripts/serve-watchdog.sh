#!/bin/bash
# Keep `notify-slack serve` running for one project. Safe to run as often as you like:
# it does nothing if the dispatcher is alive. Install on ONE login node with `crontab -e`:
#
#   */5 * * * * /path/to/slack_bridge/scripts/serve-watchdog.sh /path/to/project /path/to/env/bin/notify-slack
#
set -u
PROJECT="${1:?usage: serve-watchdog.sh PROJECT_DIR [NOTIFY_SLACK_BIN]}"
BIN="${2:-notify-slack}"
cd "$PROJECT" || exit 1
mkdir -p .slack_assistant
LOG=.slack_assistant/serve.log
if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 10485760 ]; then mv -f "$LOG" "$LOG.1"; fi
if "$BIN" serve --status >/dev/null 2>&1; then
  echo "$(date '+%F %T') $(hostname -s) alive" >> .slack_assistant/watchdog.log
  exit 0
fi
echo "$(date '+%F %T') $(hostname -s) dispatcher down -> starting it" >> .slack_assistant/watchdog.log
nohup "$BIN" serve >> "$LOG" 2>&1 < /dev/null &
