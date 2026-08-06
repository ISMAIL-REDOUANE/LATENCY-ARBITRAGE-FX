#!/usr/bin/env bash
# ============================================================================
# deploy.sh — build-free installer for the Lead-Lag systemd service
# ----------------------------------------------------------------------------
# USAGE:
#     sudo ./deploy.sh [/path/to/lead_lag]        # install + enable + start
#     sudo ./deploy.sh --remove                    # stop + uninstall service
#
# Environment (used to generate /etc/lead_lag/lead_lag.env, mode 0600):
#     FXCM_TOKEN        required for live mode (DRY_RUN != 1)
#     FXCM_ACCOUNT_ID   required for live mode (DRY_RUN != 1)
#     FXCM_SYMBOL       default: BTC/USD          (also set in the unit)
#     BINANCE_SYMBOL    default: btcusdt
#     LOG_DIR           default: /var/log/lead_lag
#     LOG_TICKS         default: 0   (set to 1 to capture ticks.csv)
#     DRY_RUN           default: 0   (set to 1 to install in paper mode)
#
# Example (live):
#     sudo FXCM_TOKEN=abc123... FXCM_ACCOUNT_ID=1027808 ./deploy.sh ./lead_lag
# Example (paper):
#     sudo DRY_RUN=1 ./deploy.sh ./lead_lag
#
# The secret values are written with restrictive permissions; the unit file
# itself contains only non-secret tuning, so world-readable config is safe.
# ============================================================================
set -euo pipefail

INSTALL_DIR=/opt/lead_lag
LOG_DIR="${LOG_DIR:-/var/log/lead_lag}"
ENV_DIR=/etc/lead_lag
ENV_FILE="$ENV_DIR/lead_lag.env"
SERVICE=lead_lag.service
SERVICE_SRC="${SERVICE_SRC:-$(dirname "$0")/$SERVICE}"

# ---------------------------------------------------------------------------
# uninstall path
# ---------------------------------------------------------------------------
if [[ ${1:-} == "--remove" ]]; then
    systemctl disable --now "$SERVICE" 2>/dev/null || true
    rm -f "/etc/systemd/system/$SERVICE"
    systemctl daemon-reload
    echo "Service $SERVICE removed. (Binary/logs left in place.)"
    exit 0
fi

BIN_SRC="${1:-./lead_lag}"
DRY_RUN="${DRY_RUN:-0}"
FXCM_TOKEN="${FXCM_TOKEN:-}"
FXCM_ACCOUNT_ID="${FXCM_ACCOUNT_ID:-}"

# ---------------------------------------------------------------------------
# preflight checks
# ---------------------------------------------------------------------------
if [[ $EUID -ne 0 ]]; then
    echo "error: run as root, e.g.  sudo ./deploy.sh" >&2
    exit 1
fi
command -v systemctl >/dev/null 2>&1 || {
    echo "error: systemd (systemctl) not found — is this Ubuntu with systemd?" >&2
    exit 1
}
[[ -x $BIN_SRC ]] || {
    echo "error: binary not found/executable at: $BIN_SRC" >&2
    echo "       compile it first:  g++ ... -o lead_lag  (see lead_lag.cpp header)" >&2
    exit 1
}
[[ -f $SERVICE_SRC ]] || {
    echo "error: unit file not found: $SERVICE_SRC" >&2
    exit 1
}
if [[ $DRY_RUN != "1" ]]; then
    [[ -n $FXCM_TOKEN ]]      || { echo "error: FXCM_TOKEN is required for live mode" >&2; exit 1; }
    [[ -n $FXCM_ACCOUNT_ID ]] || { echo "error: FXCM_ACCOUNT_ID is required for live mode" >&2; exit 1; }
fi

# ---------------------------------------------------------------------------
# dedicated service user + directories
# ---------------------------------------------------------------------------
if ! id -u leadlag >/dev/null 2>&1; then
    useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin leadlag
    echo "created system user: leadlag"
fi

mkdir -p "$INSTALL_DIR" "$LOG_DIR" "$ENV_DIR"
install -m 0755 "$BIN_SRC" "$INSTALL_DIR/lead_lag"
chown -R leadlag:leadlag "$INSTALL_DIR" "$LOG_DIR"
chmod 750 "$LOG_DIR"

# ---------------------------------------------------------------------------
# secrets file (0600 so only root may read it; service user reads at start)
# ---------------------------------------------------------------------------
umask 077
: > "$ENV_FILE"
echo "FXCM_TOKEN=$FXCM_TOKEN"        >> "$ENV_FILE"
echo "FXCM_ACCOUNT_ID=$FXCM_ACCOUNT_ID" >> "$ENV_FILE"
chown root:leadlag "$ENV_FILE"
chmod 0640 "$ENV_FILE"

# ---------------------------------------------------------------------------
# install unit, enable on boot, start now
# ---------------------------------------------------------------------------
install -m 0644 "$SERVICE_SRC" "/etc/systemd/system/$SERVICE"
systemctl daemon-reload
systemctl enable --now "$SERVICE"

sleep 3
if systemctl is-active --quiet "$SERVICE"; then
    echo "OK: $SERVICE is active (auto-start enabled)."
else
    echo "WARN: $SERVICE did not become active. Recent log output:" >&2
    journalctl -u "$SERVICE" -n 20 --no-pager >&2 || true
fi

# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
cat <<EOF

Deployment complete.
  binary      : $INSTALL_DIR/lead_lag
  secrets     : $ENV_FILE  (mode 0600)
  text logs   : $LOG_DIR/log_YYYYMMDD.txt
  telemetry   : $LOG_DIR/trades_telemetry.csv${LOG_TICKS:+, ticks.csv}

  status      : sudo systemctl status $SERVICE
  follow logs : sudo journalctl -u $SERVICE -f
  recent logs : sudo journalctl -u $SERVICE -n 200
  today's logs: sudo journalctl -u $SERVICE --since today
  restart     : sudo systemctl restart $SERVICE
  stop        : sudo systemctl stop $SERVICE
  disable     : sudo systemctl disable --now $SERVICE

  If the unit entered a failed state after repeated instant crashes:
      sudo systemctl reset-failed $SERVICE && sudo systemctl start $SERVICE
EOF
