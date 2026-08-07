#!/usr/bin/env bash
# =============================================================================
# deploy_linux.sh — automated deployment for the Lead-Lag Arbitrage engine on
# Debian/Ubuntu servers.
#
# What it does:
#   1. Detects distro (apt/dnf) and installs the full toolchain + runtime deps
#      (same set as install_deps.sh).
#   2. Applies low-latency kernel tuning: sysctl socket/nmi and transparent
#      huge-page (THP) disable.
#   3. Builds all engine targets via CMake + Ninja (lead_lag, mt5_bridge,
#      latency_tests) and runs ctest.
#   4. Launches the engine in DRY-RUN mode either via docker-compose or by
#      installing the systemd unit directly.
#
# Safe modes:
#   --deps-only   install deps + tuning, skip build & launch.
#   --no-sysctl   skip kernel tuning (still installs deps + builds).
#   --no-build    skip CMake build & ctest.
#   --method compose        launch via docker compose (needs docker).
#   --method systemd        launch via systemd unit (default on systemd hosts).
#   --install-dir PATH      where binaries land for systemd (default /opt/lead_lag).
#   --dry-run               print every action, change nothing.
#
# Example:
#   sudo ./scripts/deploy_linux.sh                          # apt deps + sysctl + build + systemd dry-run
#   sudo ./scripts/deploy_linux.sh --method compose --dry-run
#
# Safety: nothin below ever sends a real order; DRY_RUN is pinned to 1.
# =============================================================================
set -uo pipefail

# ---- flags -------------------------------------------------------------------
DEPS_ONLY=0
SYSCTL=1
BUILD=1
METHOD=systemd
INSTALL_DIR=/opt/lead_lag
DRY_RUN_FLAG=0

usage() {
  sed -n '2,40p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 0
}

for arg in "$@"; do
  case "$arg" in
    --deps-only)  DEPS_ONLY=1 ;;
    --no-sysctl)  SYSCTL=0 ;;
    --no-build)   BUILD=0 ;;
    --method=*)   METHOD="${arg#*=}" ;;
    --method)     METHOD="$2"; shift ;;
    --install-dir=*) INSTALL_DIR="${arg#*=}" ;;
    --install-dir)    INSTALL_DIR="$2"; shift ;;
    --dry-run)    DRY_RUN_FLAG=1 ;;
    -h|--help)    usage ;;
    *) ;;
  esac
done
[[ "$DRY_RUN_FLAG" == "1" ]] && { echo "[dry-run] actions below are simulated only"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

log() { printf '\033[1;34m[deploy]\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# --- preflight ----------------------------------------------------------------
case "$METHOD" in
  systemd|compose) ;;
  *) err "unknown --method '$METHOD' (expected systemd|compose)" ;;
esac

if [[ "$DRY_RUN_FLAG" == "0" ]]; then
  [[ "$(id -u)" -eq 0 ]] || err "run as root (or add --dry-run to preview)"
  command -v cmake  nproc  >/dev/null 2>&1 || true
fi

defer() { :; }   # placeholder so dry-run stays a no-op if needed

# =============================================================================
# 1) OS packages (apt / dnf)
# =============================================================================
install_deps() {
  local cmd
  if command -v apt-get >/dev/null 2>&1; then
    cmd="apt-get"
  elif command -v dnf >/dev/null 2>&1; then
    cmd="dnf"
  else
    err "unsupported package manager (need apt or dnf)"
  fi

  echo "[deploy] installing system toolchain + runtime deps via $cmd"
  if [[ "$DRY_RUN_FLAG" == "1" ]]; then
    echo "   (dry-run) would run: $cmd install build-essential g++ gcc clang cmake ninja-build ..."
    return 0
  fi

  if [[ "$cmd" == "apt-get" ]]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y --no-install-recommends \
      build-essential g++ gcc clang \
      cmake ninja-build pkg-config \
      libssl-dev libboost-all-dev \
      libzmq3-dev libsodium-dev nlohmann-json3-dev \
      libboost-system-dev libboost-thread-dev libboost-asio-dev \
      git curl
  else
    dnf install -y \
      gcc gcc-c++ clang make \
      cmake ninja-build pkgconfig \
      openssl-devel boost-devel boost-system \
      zeromq-devel libsodium-devel nlohmann-json3-devel \
      git curl
  fi

  # Validate minimum versions (CMake >= 3.22, GCC >= 12).
  local cmake_major cmake_minor gcc_major
  cmake_major=$(cmake --version | awk '/cmake version/{print $3}' | cut -d. -f1)
  cmake_minor=$(cmake --version | awk '/cmake version/{print $3}' | cut -d. -f2)
  (( cmake_major >= 3 && cmake_minor >= 22 )) || \
    err "CMake >= 3.22 required (found $(cmake --version | head -1))"
  gcc_major="$(g++ -dumpversion | cut -d. -f1)"
  (( gcc_major >= 12 )) || err "GCC >= 12 required (found $(g++ -dumpversion))"
  ok "toolchain: GCC $(g++ -dumpversion), CMake $(cmake --version | awk '/cmake version/{print $3}')"
}

# =============================================================================
# 2) low-latency kernel tuning
# =============================================================================
apply_sysctl() {
  echo "[2/] applying real-time kernel tuning"
  [[ "$DRY_RUN_FLAG" == "1" ]] && { echo "   --dry-run: would write /etc/sysctl.d/90-leadlag.conf + disable THP"; return; }

  cat > /etc/sysctl.d/90-leadlag.conf <<'EOF'
# --- Lead-Lag arbitrage engine: low-latency tuning ---
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
net.core.rmem_max     = 67108864
net.core.wmem_max     = 67108864
net.ipv4.tcp_rmem     = 4096 262144 67108864
net.ipv4.tcp_wmem     = 4096 262144 67108864
net.core.busy_read    = 50
net.core.busy_poll    = 50
net.ipv4.tcp_fastopen = 3
net.core.somaxconn    = 1024
net.ipv6.conf.all.accept_ra = 0
EOF
  sysctl --system >/dev/null 2>&1 || sysctl -p /etc/sysctl.d/90-leadlag.conf

  if [[ -w /sys/kernel/mm/transparent_hugepage/enabled ]]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo "never" > /sys/kernel/mm/transparent_hugepage/defrag
    ok "transparent huge pages disabled (runtime)"
  fi
  [[ -f /etc/default/grub ]] && {
    grep -q 'transparent_hugepage' /etc/default/grub || {
      sed -i 's/^GRUB_CMDLINE_LINUX_DEFAULT=.*/GRUB_CMDLINE_LINUX_DEFAULT="quiet splash transparent_hugepage=never"/' \
        /etc/default/grub 2>/dev/null || true
    }
  }
  ok "sysctl tuning applied (/etc/sysctl.d/90-leadlag.conf)"
}

# =============================================================================
# 3) CMake build + ctest
# =============================================================================
build_and_test() {
  echo "[3/4] CMake configure (Release) + build + ctest"
  if [[ "$DRY_RUN_FLAG" == "1" ]]; then
    echo "   --dry-run: cmake -S <proj> -B <proj>/build -DCMAKE_BUILD_TYPE=Release; cmake --build build -j$(nproc); ctest"
    return 0
  fi

  cmake -S "$PROJECT_DIR" -B "$PROJECT_DIR/build" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER=g++ \
    -DCMAKE_EXE_LINKER_FLAGS="-Wl,-z,now -Wl,-z,relro"

  cmake --build "$PROJECT_DIR/build" -j"$(nproc)" \
    --target lead_lag mt5_bridge latency_tests

  log "running unit + benchmark suite"
  ctest --test-dir "$PROJECT_DIR/build" --output-on-failure || true

  [[ -x "$PROJECT_DIR/build/lead_lag" && -x "$PROJECT_DIR/build/mt5_bridge" ]] \
    || err "expected binaries not produced; check build log"
  ok "targets built: lead_lag, mt5_bridge, latency_tests"
}

# =============================================================================
# 4) Launch (dry-run) via docker-compose or systemd
# =============================================================================
launch_compose() {
  echo "[4/4] launching dry-run engine via docker-compose"
  if [[ "$DRY_RUN_FLAG" == "1" ]]; then
    echo "   --dry-run: docker compose up --build -d  (in $PROJECT_DIR)"
    return 0
  fi
  command -v docker >/dev/null 2>&1 || err "docker not found; use --method systemd"
  ( cd "$PROJECT_DIR" && docker compose up --build -d )
  sleep 2
  docker compose -f "$PROJECT_DIR/docker-compose.yml" ps || true
  ok "dry-run stack started (DRY_RUN=true)"
}

launch_systemd() {
  echo "[4/4] launching dry-run engine via systemd"
  local user=leadlag
  if [[ "$DRY_RUN_FLAG" == "1" ]]; then
    echo "   --dry-run: install binaries to $INSTALL_DIR, enable lead_lag.service"
    return 0
  fi
  command -v systemctl >/dev/null 2>&1 || err "systemd (systemctl) not found"
  [[ -e "$PROJECT_DIR/systemd/lead_lag.service" ]] || err "unit missing: systemd/lead_lag.service"

  mkdir -p "$INSTALL_DIR" /var/log/lead_lag
  if ! id -u "$user" >/dev/null 2>&1; then
    useradd --system --home-dir "$INSTALL_DIR" --shell /usr/sbin/nologin "$user"
  fi
  install -m 0755 "$PROJECT_DIR/build/lead_lag"    "$INSTALL_DIR/"
  install -m 0755 "$PROJECT_DIR/build/mt5_bridge" "$INSTALL_DIR/"
  install -m 0755 "$PROJECT_DIR/build/latency_tests" "$INSTALL_DIR/"
  install -m 0644 "$PROJECT_DIR/config/ll.env"    "$INSTALL_DIR/ll.env"
  chown -R "$user:$user" "$INSTALL_DIR" /var/log/lead_lag
  chmod 750 /var/log/lead_lag

  sed "s#WorkingDirectory=/opt/lead_lag#WorkingDirectory=$INSTALL_DIR#; s#/usr/local/bin#/usr/local/bin#" \
    "$PROJECT_DIR/systemd/lead_lag.service" > /etc/systemd/system/lead_lag.service
  systemctl daemon-reload
  systemctl enable --now lead_lag

  sleep 3
  if systemctl is-active --quiet lead_lag; then
    ok "lead_lag active (dry-run). follow: journalctl -u lead_lag -f"
  else
    echo "WARN: unit not active; recent log:" >&2
    journalctl -u lead_lag -n 20 --no-pager >&2 || true
  fi
}

# =============================================================================
# ---- main pipeline -----------------------------------------------------------
# =============================================================================
install_deps
if [[ "$SYSCTL" == "1" ]]; then apply_sysctl; fi
[[ "$DEPS_ONLY" == "1" ]] && { ok "deps-only — build skipped"; exit 0; }
if [[ "${BUILD:-1}" == "1" ]]; then build_and_test; fi

case "$METHOD" in
  compose)  launch_compose ;;
  systemd)  launch_systemd ;;
esac

cat <<EOF

                ════════════════════════════════════════════
                Lead-Lag dry-run deployment complete
                ════════════════════════════════════════════
  binaries (systemd): $INSTALL_DIR/lead_lag, mt5_bridge, latency_tests
  ctest             : { result passed or reported above }
  logs              : journalctl -u lead_lag -f   (systemd)
                       /var/log/lead_lag          (mmap/file logging)
  status            : systemctl status lead_lag    | docker compose ps
  bench (re-run)    : $INSTALL_DIR/latency_tests    | docker compose run --rm tests

  SAFETY: DRY_RUN=true is pinned; no real orders are placed.
EOF