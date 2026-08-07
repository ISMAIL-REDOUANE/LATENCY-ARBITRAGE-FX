#!/usr/bin/env bash
# =============================================================================
# monitor_oanda.sh — light-weight health monitor for the Lead-Lag + OANDA bot.
#
# Designed to run from cron every minute:
#   */1 * * * * root /opt/lead_lag/monitor_oanda.sh
# (see "Automate via cron" at the bottom of this file for the full snippet.)
#
# Checks, in order:
#   1. Service down ..... systemctl is-active lead_lag_oanda
#   2. Trades stale .... no new line in trades_telemetry.csv for X hours
#   3. Log errors ....... scan today's log for CRITICAL/FAILED/OANDA…FAIL
#
# Failure handling:
#   * ENABLE_AUTORESTART=1 -> try one `systemctl restart` before alerting.
#   * Telegram alert via curl (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID).
#   * Alert throttling to avoid spam:
#       - CRITICAL (service down / restart failed): once per 15 min
#       - WARNING  (stale trades / log errors):       once per 60 min
#
# All state is stored in STATE_DIR (default /var/lib/lead_lag_monitor).
# Exit code 0 = healthy or nothing new; non-zero only for convenience.
# =============================================================================
set -uo pipefail

# ---- config (override via env/cron) -----------------------------------------
SERVICE_NAME="${SERVICE_NAME:-lead_lag_oanda}"
LOG_DIR="${LOG_DIR:-/var/log/lead_lag}"
TELEMETRY_CSV="${TELEMETRY_CSV:-$LOG_DIR/trades_telemetry.csv}"
DAILY_LOG="${DAILY_LOG:-$LOG_DIR/lead_lag.log}"          # engine text log
STATE_DIR="${STATE_DIR:-/var/log/lead_lag_monitor}"
BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"
ENABLE_AUTORESTART="${ENABLE_AUTORESTART:-1}"

# thresholds ---------------------------------------------------------------
STALE_HOURS="${STALE_HOURS:-2}"          # warn if no trade older than this
CRITICAL_THROTTLE_S="${CRITICAL_THROTTLE_S:-900}"   # 15 min
WARN_THROTTLE_S="${WARN_THROTTLE_S:-3600}"         # 60 min

CRIT_MARKER="${CRIT_MARKER:-$STATE_DIR/last_critical}"

mkdir -p "$STATE_DIR"

log()  { printf '[monitor %(%F %T)T] %s\n' -1 "$*"; }
send() { # message
  [[ -z "$BOT_TOKEN" || -z "$CHAT_ID" ]] && { log "telegram creds missing; message: $1"; return 1; }
  curl -sS --max-time 10 \
    --data-urlencode "chat_id=$CHAT_ID" \
    --data-urlencode "text=$1" \
    "https://api.telegram.org/bot$BOT_TOKEN/sendMessage" >/dev/null 2>&1 || true
}

# returns 0 (trigger) if enough time elapsed since $markfile was touched
_throttled() { # markfile interval_seconds
  local mark="$1" interval="$2" now cur
  now=$(date +%s)
  cur=$([[ -f "$mark" ]] && cat "$mark" || echo 0)
  if (( now - cur >= interval )); then echo "$now" > "$mark"; return 0; fi
  return 1
}

# ---- optional single-flight lock (cron overlap protection) -----------------
exec 9>"$STATE_DIR/.lock"
flock -n 9 || { echo "another monitor run in progress; exiting"; exit 0; }

# ---- 1) service liveness -----------------------------------------------------
if ! systemctl is-active --quiet "$SERVICE_NAME"; then
  if [[ "$ENABLE_AUTORESTART" == "1" ]]; then
    log "service down — attempting one restart"
    systemctl restart "$SERVICE_NAME"
    sleep 5
    if systemctl is-active --quiet "$SERVICE_NAME"; then
      send "✅ $SERVICE_NAME auto-restarted at $(date '+%F %T')"
    else
      if _throttled "$CRIT_MARKER" "$CRIT_THROTTLE_S"; then
        critical=1
        send "🚨 CRITICAL: $SERVICE_NAME is DOWN. Auto-restart FAILED."
      fi
    fi
  else
    if _throttled "$CRIT_MARKER" "$CRIT_THROTTLE_S"; then
      critical=1
      send "🚨 CRITICAL: $SERVICE_NAME is DOWN."
    fi
  fi
fi

# ---- 2) trades heartbeat -----------------------------------------------------
if [[ -f "$TELEMETRY_CSV" ]]; then
  last=$(date +%s)
  # shellcheck disable=SC2012
  lastm=$(ls -l --time-style=+%s "$TELEMETRY_CSV" 2>/dev/null | awk '{print $6}')
  age=$(( last - lastm ))
  if (( age > STALE_HOURS*3600 )); then
    if _throttled "$STATE_DIR/last_stale" "$WARN_THROTTLE_S"; then
      warn=1
      send "⚠️ WARNING: no trades recorded in $STALE_HOURS h (last $((age/3600))h ago). File: $TELEMETRY_CSV"
    fi
  fi
else
  # no file yet: engine may be in its first run; only warn if running >5min
  log "$TELEMETRY_CSV missing (ok if service just started)"
fi

# ---- 3) scan log for hard errors ----------------------------------------------
[[ -f "$DAILY_LOG" ]] && errors=$(grep -iEc 'CRITICAL|FAILED|OANDA.*[Ff]ail' "$DAILY_LOG" || true)
if [[ "${errors:-0}" -gt 0 ]]; then
  if _throttled "$STATE_DIR/last_errscan" "$WARN_THROTTLE_S"; then
    warn=1
    sample=$(grep -iE 'CRITICAL|FAILED|OANDA.*[Ff]ail' "$DAILY_LOG" | tail -n 5)
    send "⚠️ WARNING: $errors error(s) in $DAILY_LOG (24h). Sample:
$sample
(Check: journalctl -u $SERVICE_NAME -e)"
  fi
fi

exit 0

# =============================================================================
# INSTALL AS CRON (run as root, every minute):
#   sudo cp scripts/monitor_oanda.sh /opt/lead_lag/monitor.sh
#   sudo chmod 700 /opt/lead_lag/monitor.sh
#   sudo crontab -e   # add:
#
#     OANDA_HOST=api-fxpractice.oanda.com
#     TELEGRAM_BOT_TOKEN=123456:ABC-DEF YOUR_TG_TOKEN
#     TELEGRAM_CHAT_ID=123456789
#     */1 * * * * /opt/lead_lag/monitor.sh
#
# Reduce noise with per-command env:
#     */1 * * * * TELEGRAM_BOT_TOKEN=x TELEGRAM_CHAT_ID=y /opt/lead_lag/monitor.sh
#
# Disable auto-restart (unattended restart is risky):
#   sudo systemctl stop lead_lag_monitor 2>/dev/null # or
#   set ENABLE_AUTORESTART=0 in the cron line above.
# =============================================================================