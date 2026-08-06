#!/usr/bin/env bash
# ============================================================================
# monitor_lead_lag.sh — health monitor + Telegram alerts for the Lead-Lag bot
# ----------------------------------------------------------------------------
# Run every 60 seconds from cron. Alerts via Telegram only when something is
# wrong (nothing is emailed/paged on normal operation).
#
# Checks, in order:
#   1. Service down  ->  send "🚨 CRITICAL: Lead-Lag Bot is DOWN!"
#   2. Heartbeat (trades_telemetry.csv mtime) ->  send a warning if no trade
#      in the last NO_TRADE_HOURS.
#   3. Log scan      ->  send the exact "CRITICAL" / "FAILED" / "FXCM POST
#      error" lines newly appended to the daily log since the previous run.
#
# Rate limiting: an alert category is not re-sent until its throttle window
# (minutes) has elapsed, so a long outage does NOT flood Telegram.
#
# Ownership:
#    defaults fine as root (cron). Writes only to STATE_DIR.
#
# Usage / env:
#   TELEGRAM_BOT_TOKEN  (required) your bot token from @BotFather
#   TELEGRAM_CHAT_ID    (required) target chat id
#   LOG_DIR            default /var/log/lead_lag
#   STATE_DIR          default /var/log/lead_lag-monitor  (alert throttling)
#   NO_TRADE_HOURS     default 3     hours without a trade before warning
#   CRIT_RETRY_MIN     default 15    min between re-alerts while service DOWN
#   WARN_RETRY_MIN      default 60    min between re-alerts of same category
#   ENABLE_AUTORESTART default 0     set to 1 to attempt one `systemctl
#                                    restart` before the CRITICAL alert
#   NOTIFY_ALWAYS       default 0     set to 1 to bypass throttling (testing)
# ============================================================================
set -uo pipefail

# --- configuration -----------------------------------------------------------
SERVICE=lead_lag.service
LOG_DIR="${LOG_DIR:-/var/log/lead_lag}"
STATE_DIR="${STATE_DIR:-/var/log/lead_lag-monitor}"
NO_TRADE_HOURS="${NO_TRADE_HOURS:-3}"
CRIT_RETRY_MIN="${CRIT_RETRY_MIN:-15}"
WARN_RETRY_MIN="${WARN_RETRY_MIN:-60}"      # also log-error throttle
ENABLE_AUTORESTART="${ENABLE_AUTORESTART:-0}"
NOTIFY_ALWAYS="${NOTIFY_ALWAYS:-0}"
BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"

mkdir -p "$STATE_DIR"
LOCK_FILE="$STATE_DIR/lock"

# --- single-instance guard (resume-run cron safety) ------------------------------
exec 9>"$LOCK_FILE"
flock -n 9 2>/dev/null || exit 0        # another instance is still running

now=$(date +%s)

# --- helpers ---------------------------------------------------------------------
errout() { printf '%s\n' "$*" >&2; }

send_telegram() {
    local msg="$1"
    if [[ -z $BOT_TOKEN || -z $CHAT_ID ]]; then
        errout "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set; cannot send alert."
        return 1
    fi
    curl -s -o /dev/null --max-time 10 \
        --data-urlencode "chat_id=$CHAT_ID" \
        --data-urlencode "text=$msg" \
        "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        && return 0
    errout "telegram send failed for: ${msg%% *}"
    return 1
}

# send_once <category> <min_interval> <message>
send_once() {
    local category="$1" min_interval="$2" msg="$3"
    if [[ $NOTIFY_ALWAYS == "1" ]]; then
        send_telegram "$msg"
        return
    fi
    local stamp="$STATE_DIR/notify_${category}"
    local last=0
    [[ -f $stamp ]] && last=$(cat "$stamp" 2>/dev/null || echo 0)
    if (( now - last >= min_interval * 60 )); then
        if send_telegram "$msg"; then
            echo "$now" > "$stamp"
        fi
    fi
}

# ===========================================================================
# 1) SERVICE STATUS
# ===========================================================================
if [[ $(systemctl is-active "$SERVICE" 2>/dev/null) != "active" ]]; then
    if [[ $ENABLE_AUTORESTART == "1" ]]; then
        errout "$SERVICE down — attempting restart"
        systemctl restart "$SERVICE" >/dev/null 2>&1 || true
        sleep 5
        if [[ $(systemctl is-active "$SERVICE" 2>/dev/null) == "active" ]]; then
            send_once restart_ok 60 \
                "🔄 Lead-Lag bot auto-restarted after going DOWN."
            exit 0
        fi
    fi
    send_once down "$CRIT_RETRY_MIN" \
        "🚨 CRITICAL: Lead-Lag Bot is DOWN!"$'\n'\
        "$(date '+%Y-%m-%d %H:%M:%S') — service $SERVICE is not active."$'\n'\
        "Fix it, then:  sudo systemctl start $SERVICE"
    exit 0
fi

errout "service active"

# ===========================================================================
# 2) TRADING ACTIVITY (heartbeat on trades_telemetry.csv mtime)
# ===========================================================================
CSV="$LOG_DIR/trades_telemetry.csv"
if [[ -f $CSV ]]; then
    csv_mtime=$(stat -c %Y "$CSV" 2>/dev/null || echo 0)
    age_min=$(( (now - csv_mtime) / 60 ))
    if [[ $csv_mtime -gt 0 && $age_min -gt $(( NO_TRADE_HOURS * 60 )) ]]; then
        send_once "no_trade" "$WARN_RETRY_MIN" \
            "⚠️ WARNING: No trades executed in the last ${NO_TRADE_HOURS} hours. Check market conditions or connection."$'\n'\
            "$(date '+%Y-%m-%d %H:%M:%S') — last trade ${age_min} min ago."
    fi
else
    errout "telemetry file not present yet: $CSV"
fi

# ===========================================================================
# 3) LOG ERROR SCAN — only lines NEW since the previous run (watermark)
# ===========================================================================
logfile=$(ls -1t "$LOG_DIR"/log_*.txt 2>/dev/null | head -n 1)
if [[ -n $logfile ]]; then
    base=$(basename "$logfile")
    wm="$STATE_DIR/watermark_$base"
    prev=0
    [[ -f $wm ]] && prev=$(cat "$wm" 2>/dev/null || echo 0)
    size=$(stat -c %s "$logfile" 2>/dev/null || echo 0)
    if [[ $size -gt $prev ]]; then
        new=$(tail -c +$((prev + 1)) "$logfile" 2>/dev/null \
              | grep -aE "CRITICAL|FAILED|FXCM POST error" | tail -n 5)
        if [[ -n $new ]]; then
            send_once "log_$base" "$WARN_RETRY_MIN" \
                "⚠️ Lead-Lag log error(s) detected:"$'\n'"$(printf '%s\n' "$new")"
        fi
    fi
    echo "$size" > "$wm"
fi

exit 0