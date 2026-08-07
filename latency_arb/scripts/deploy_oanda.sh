#!/usr/bin/env bash
# =============================================================================
# deploy_oanda.sh — one-shot automated deployment of the Lead-Lag HFT bot with
# OANDA v2 execution on a fresh Ubuntu/Debian VPS.
#
# Run ONCE on a fresh server:
#   git clone https://github.com/ISMAIL-REDOUANE/LATENCY-ARBITRAGE-FX.git
#   cd LATENCY-ARBITRAGE-FX/latency_arb
#   sudo bash scripts/deploy_oanda.sh
#
# Pipeline (idempotent — safe to re-run):
#   1. Detect OS (Ubuntu/Debian) and install ALL build/runtime deps.
#   2. Locate/clone the repo (default target dir: /opt/latency-arb).
#   3. Build Release (-O3 -march=native -DNDEBUG): lead_lag + oanda_probe.
#   4. Create the dedicated 'leadlag' system user.
#   5. Create /opt/lead_lag (binaries) + /var/log/lead_lag (logs).
#   6. Generate a secured /etc/lead_lag/lead_lag.env (0600) interactively.
#   7. Install + enable the hardened systemd unit.
#   8. Run an oanda_probe smoke test against OANDA (demo) first.
#   9. Start the service and verify it is active (warn if it is not).
#  10. Print a summary (status, logs, monitoring, stop/restart).
#
# Options:
#   --no-prompt      read pre-seeded env from $DEPLOY_ENV_FILE or the OANDA_*
#                    environment variables of this shell (never prompts).
#   --env-file=FILE  install this pre-written env file (chmod 600) instead of prompting.
#   --no-probe       skip the oanda_probe smoke test.
#   --no-build       skip the build step (useful if binaries already in INSTALL_DIR).
#   --no-start       install everything but do not start/enable the service.
#   --dry-run        print what would run; change nothing.
#
# SAFETY: DRY_RUN=0 flips the bot to LIVE OANDA execution. Only do that on a
# fully-tested demo account with a paper/limited balance you can afford to lose.
# =============================================================================
set -uo pipefail

# ---- configuration ----------------------------------------------------------
REPO_URL="https://github.com/ISMAIL-REDOUANE/LATENCY-ARBITRAGE-FX.git"
LATENCY_ARB_HOME="${LATENCY_ARB_HOME:-/opt/latency-arb}"
INSTALL_DIR="${INSTALL_DIR:-/opt/lead_lag}"
LOG_DIR="${LOG_DIR:-/var/log/lead_lag}"
ENV_DIR="/etc/lead_lag"
ENV_FILE="${ENV_FILE:-/etc/lead_lag/lead_lag.env}"
SERVICE_SRC="systemd/lead_lag_oanda.service"
SERVICE_NAME="lead_lag_oanda.service"
RUN_USER="leadlag"

PROBE=1      # 1 = run oanda_probe, 0 = skip
DO_BUILD=1
DO_START=1
DRY=0
NO_PROMPT=0

# parse args (order-independent)
while (( $# )); do
  case "$1" in
    --env-file=*) ENV_FILE="${1#*=}" ;;
    --env-file) shift; ENV_FILE="$1" ;;
    --no-probe)  PROBE=0 ;;
    --no-build)  DO_BUILD=0 ;;
    --no-start)  DO_START=0 ;;
    --no-prompt) NO_PROMPT=1 ;;
    --dry-run)   DRY=1 ;;
    -h|--help)   sed -n '1,40p' "$0"; exit 0 ;;
    *) warn "ignoring unknown argument: $1" ;;
  esac
  shift
done

# ---- helpers ----------------------------------------------------------------
log() { printf '\033[1;34m[deployo]\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m[  ok  ]\033[0m %s\n' "$*"; }
warn(){ printf '\033[1;33m[ warn ]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[error ]\033[0m %s\n' "$*" >&2; exit 1; }

# run(): prefix a real command with [dry] but DON'T execute under --dry-run.
# Pipe-friendly replacement for the special-case in pipelines.
run() {
  if [[ "$DRY" == "1" ]]; then printf '   [dry] %s\n' "$*"; return 0; fi
  "$@"
}

[[ "$(id -u)" -eq 0 ]] || err 'run as root (or `sudo bash scripts/deploy_oanda.sh`); use --dry-run to preview first'

# ----------------------------------------------------------------------------
# 1) dependencies
# ----------------------------------------------------------------------------
install_deps() {
  if [[ "$DRY" == "1" ]]; then
    log "would install deps: (apt-get update; apt-get install build-essential cmake ninja-build libboost-all-dev libssl-dev nlohmann-json3-dev libzmq3-dev libsodium-dev git python3)"
    return
  fi
  if command -v apt-get >/dev/null 2>&1; then
    log "dedect package manager: apt (Ubuntu/Debian)"
    export DEBIAN_FRONTEND=noninteractive
    log "apt-get update"
    apt-get update -y
    log "installing dependencies"
    apt-get install -y --no-install-recommends \
      build-essential g++ gcc cmake ninja-build pkg-config git curl jq python3 ca-certificates \
      libboost-all-dev libssl-dev nlohmann-json3-dev \
      libzmq3-dev libsodium-dev
  elif command -v dnf >/dev/null 2>&1; then
    log "detech package manager: dnf (RHEL-family; Ubuntu/Debian recommended)"
    dnf install -y gcc gcc-c++ cmake ninja-build pkgconfig git curl jq python3 \
      openssl-devel boost-devel nlohmann-json3-devel zeromq-devel libsodium-devel
  else
    err "no apt-get or dnf found — supported OS is Ubuntu/Debian 22.04/24.04"
  fi
}

# -----------------------------------------------------------------------------
# 2) repo
# -----------------------------------------------------------------------------
seed_repo() {
  local src="$LATENCY_ARB_HOME/latency_arb"
  # (a) already cloned into the default location
  if [[ -d "$src/.git" ]]; then log "repo found: $src"; return 0; fi
  # (b) we are running from inside a clone (repo root has latency_arb/)
  if [[ -f "CMakeLists.txt" ]] && [[ -d "systemd" ]]; then
    LATENCY_ARB_HOME="$(pwd)"
    return 0
  fi
  log "cloning $REPO_URL -> $LATENCY_ARB_HOME"
  run mkdir -p "$LATENCY_ARB_HOME"
  run git clone --depth 1 "$REPO_URL" "$LATENCY_ARB_HOME"
  [[ -d "$LATENCY_ARB_HOME/latency_arb" ]] || err "repo clone failed"
}

# -----------------------------------------------------------------------------
# 3) build
# -----------------------------------------------------------------------------
do_build() {
  [[ "$DO_BUILD" == "0" ]] && { log "skipping build (--no-build)"; return 0; }
  local src="$LATENCY_ARB_HOME/latency_arb"
  log "cmake configure (Release) at $src/build"
  run cmake -S "$src" -B "$src/build" -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER=g++ -DCMAKE_CXX_FLAGS="-O3 -DNDEBUG"
  # -march=native at configure is risky on cloud VMs; we hardcode the flags above.
  run cmake --build "$src/build" --target lead_lag oanda_probe -j"$(nproc)"
  [[ -x "$src/build/lead_lag" && -x "$src/build/oanda_probe" ]] \
    || err "build did not produce lead_lag / oanda_probe (see CMake output above)"
}

# -----------------------------------------------------------------------------
# 4) user + dirs
# -----------------------------------------------------------------------------
prep_user_dirs() {
  if ! id -u "$RUN_USER" >/dev/null 2>&1; then
    log "creating system user: $RUN_USER"
    run useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin "$RUN_USER"
  fi
  log "creating runtime dirs (INSTALL_DIR, LOG_DIR, ENV_DIR)"
  run mkdir -p "$INSTALL_DIR" "$LOG_DIR" "$ENV_DIR"
  run chown "$RUN_USER:$RUN_USER" "$INSTALL_DIR" "$LOG_DIR"
  run chmod 0750 "$LOG_DIR"
}

# -----------------------------------------------------------------------------
# 5) env file
# -----------------------------------------------------------------------------
prompt_val() { # name prompt default
  local name="$1" prompt="$2" dflt="$3"
  if [[ "$DRY" == "1" ]]; then printf ' [dry] prompted %s\n' "$name"; echo "$dflt"; return 0; fi
  if [[ -n "${!name:-}" ]]; then echo "${!name}"; return 0; fi   # env prese
  if [[ "$NO_PROMPT" == "1" ]]; then
    warn "no value for $name and --no-prompt is set; leaving empty"
    echo ""; return 0
  fi
  printf '  %s [%s]: ' "$prompt" "$dflt"
  read -r v
  echo "${v:-$dflt}"
}

gen_env_file() {
  if [[ -f "$ENV_FILE" && "$ENV_FILE" != /etc/lead_lag/lead_lag.env ]]; then
    # user supplied --env-file=FILE
    run install -o root -g root -m 0600 "$ENV_FILE" /etc/lead_lag/lead_lag.env
    ENV_FILE=/etc/lead_lag/lead_lag.env
    ok "installed env file from: $1 (0600)"
    return
  fi
  log "prompting for OANDA account details (seeded from your current shell if set)"
  token=$(prompt_val OANDA_TOKEN         "OANDA API token" "")
  account=$(prompt_val OANDA_ACCOUNT_ID  "OANDA account ID" "")
  host=$(prompt_val OANDA_HOST           "OANDA host (demo/practice vs live)" "api-fxpractice.oanda.com")
  instr=$(prompt_val OANDA_INSTRUMENT    "OANDA instrument" "XAU_USD")
  dry=$(prompt_val DRY_RUN               "DRY_RUN 1=dry 0=LIVE" "0")
  [[ -z "$token" ]]   && warn "OANDA_TOKEN empty -> service will fail auth until fixed"
  [[ -z "$account" ]] && warn "OANDA_ACCOUNT_ID empty -> service will fail auth until fixed"

  run mkdir -p "$ENV_DIR"
  run chmod 600 "$ENV_FILE" 2>/dev/null || true

  if [[ "$DRY" == "1" ]]; then
    printf '   [dry] would write %s with OANDA_* / DRY_RUN values\n' "$ENV_FILE"
    return
  fi

  cat > "$ENV_FILE" <<EOF
# generated by deploy_oanda.sh $(date -u +%F_%T)
OANDA_TOKEN=$token
OANDA_ACCOUNT_ID=$account
OANDA_HOST=$host
OANDA_INSTRUMENT=$instr
OANDA_ENABLED=1
DRY_RUN=$dry
# ------- engine tunables (edit freely) ------
BINANCE_SYMBOL=btcusdt
BINANCE_HOST=stream.binance.com
BINANCE_PORT=9443
THRESHOLD_PIPS=0.5
LATENCY_BUFFER_PIPS=0.2
MIN_PROFIT_MARGIN_PIPS=0.3
MIN_LOT_PIPS=0.0
MAX_OPEN_LOTS=1
MAX_DAILY_LOSS=10.0
MAX_ORDERS_PER_INTERVAL=5
INTERVAL_SECONDS=60
TRADE_AMOUNT=1.0
LOG_DIR=$LOG_DIR
EOFMISSING
  run chmod 0600 "$ENV_FILE"
  run chown root:root "$ENV_FILE"
  ENV_FILE=/etc/lead_lag/lead_lag.env
  ok "env file written: $ENV_FILE  (0600, root-owned)"
}

# -----------------------------------------------------------------------------
# 6) systemd unit  + sysctl
# -----------------------------------------------------------------------------
install_unit() {
  local src="$LATENCY_ARB_HOME/latency_arb/$SERVICE_SRC"
  [[ -f "$src" ]] || src="$LATENCY_ARB_HOME/$SERVICE_SRC"
  [[ -f "$src" ]] || err "systemd unit not found: $SERVICE_SRC"
  run install -m 0644 "$src" "/etc/systemd/system/$SERVICE_NAME"
  run systemctl daemon-reload
}

apply_tuning() {
  log "tuning sysctl for low-latency TX (best-effort, root)"
  if [[ "$DRY" == "1" ]]; then printf '   [dry] write /etc/sysctl.d/99-leadlag-oanda.conf\n'; return 0; fi
  cat > /etc/sysctl.d/99-leadlag-oanda.conf <<'EOF'
net.core.rmem_max=67108864
net.core.wmem_max=67108864
net.ipv4.tcp_fastopen=3
EOF
  sysctl --system >/dev/null 2>&1 || true
}

# -----------------------------------------------------------------------------
# 7) install binaries
# -----------------------------------------------------------------------------
install_binaries() {
  local src="$LATENCY_ARB_HOME/latency_arb/build"
  log "installing binaries to $INSTALL_DIR"
  run install -m 0755 -o "$RUN_USER" -g "$RUN_USER" "$src/lead_lag"   "$INSTALL_DIR/lead_lag"
  run install -m 0755 -o "$RUN_USER" -g "$RUN_USER" "$src/oanda_probe" "$INSTALL_DIR/oanda_probe"
}

install_monitor() {
  local m="$LATENCY_ARB_HOME/latency_arb/scripts/monitor_oanda.sh"
  [[ -f "$m" ]] || m="$LATENCY_ARB_HOME/scripts/monitor_oanda.sh"
  [[ -f "$m" ]] || { warn "monitor_oanda.sh not found; skipping monitor install"; return 0; }
  log "installing monitor to $INSTALL_DIR/monitor_oanda.sh"
  run install -m 0700 -o root -g root "$m" "$INSTALL_DIR/monitor_oanda.sh"
}

# -----------------------------------------------------------------------------
# 8) smoke test
# -----------------------------------------------------------------------------
smoke_test() {
  [[ "$PROBE" == "0" ]] && { log "skipping oanda_probe (--no-probe)"; return 0; }
  log "smoke-testing OANDA execution via oanda_probe buy 1 0.1 0.2 (demo only)"
  if [[ "$DRY" == "1" ]]; then printf '   [dry] would run oanda_probe\n'; return 0; fi
  # shellcheck disable=SC1090
  set -a; . "$ENV_FILE" 2>/dev/null || true; set +a
  if [[ -z "${OANDA_TOKEN:-}" || -z "${OANDA_ACCOUNT_ID:-}" ]]; then
    warn "no OANDA credentials loaded; skipping probe (fix env file then re-run)"
    return 0
  fi
  "$INSTALL_DIR/oanda_probe" buy 1 0.1 0.2 && ok "probe ok (order accepted)" || warn "probe failed (see above)"
}

# -----------------------------------------------------------------------------
# 9) start + verify
# -----------------------------------------------------------------------------
start_service() {
  [[ "$DO_START" == "0" ]] && { log "skipping start (--no-start)"; return 0; }
  log "enabling + starting $SERVICE_NAME"
  run systemctl enable --now "$SERVICE_NAME"
  sleep 3
  if systemctl is-active --quiet "$SERVICE_NAME"; then
    ok "service is active"
  else
    warn "service not active — printing recent logs:"
    run journalctl -u "$SERVICE_NAME" -n 25 --no-pager
  fi
}

# -----------------------------------------------------------------------------
# 10) run
# -----------------------------------------------------------------------------
install_deps
seed_repo
[[ "$DO_BUILD" == "1" ]] && do_build
prep_user_dirs
gen_env_file
install_unit
apply_tuning
install_binaries
install_monitor
smoke_test
start_service

cat <<'SUMM'

        ============  Lead-Lag  +  OANDA deployment complete  ============

  Service   :  sudo systemctl status lead_lag_oanda
  Live logs :  journalctl -u lead_lag_oanda -f
  Trades    :  tail -f /var/log/lead_lag/trades_telemetry.csv
  Env file  :  /etc/lead_lag/lead_lag.env   (0600, root)
  Binaries  :  /opt/lead_lag/{lead_lag,oanda_probe,monitor_oanda.sh}
  Monitor   :  add to cron every 1 min  (see DEPLOYMENT.md)
                 */1 * * * * /opt/lead_lag/monitor_oanda.sh

  Control  :  sudo systemctl restart / stop / enable --now lead_lag_oanda
  Live!!    :  OANDA_HOST=api-fxtrade.oanda.com + DRY_RUN=0
  Test next:  sudo -u leadlag /opt/lead_lag/oanda_probe buy 1 0.1 0.2

  SAFETY   :  DRY_RUN="$dry"   (1 = simulated, 0 = real orders on OANDA)
        --------------------------------------------------------
        You are responsible for every order this engine places when
        DRY_RUN=0. Verify demo behavior for >= 1h before considering
        a live account. Traders can lose money; trade only what you
        can afford to lose.
        =========================================================
SUMM
exit 0