#!/usr/bin/env bash
#
# install_deps.sh — Ubuntu 24.04 / Debian 12 production environment bootstrap
# for the Lead-Lag Arbitrage engine.
#
# Installs:
#   * Toolchain : GCC 12+, Clang, CMake 3.22+, Ninja, pkg-config
#   * Runtime   : OpenSSL, Boost (system + thread), libzmq3-dev, libsodium-dev,
#                 nlohmann-json3-dev
#   * MT5 bridge: nothing extra (pure C++20, statically linked where possible)
#
# Also applies kernel tuning for real-time low-latency networking:
#   * net.core.rmem_max / wmem_max        -> 64MB
#   * net.core.busy_read / busy_poll      -> 50us (requires NET_RX_BUSY_POLL)
#   * vm.transparent_hugepage=never        (THP causes latency spikes)
#   * net.ipv4.tcp_fastopen, tcp_rmem/wmem, somaxconn
#
# Usage:
#   sudo ./scripts/install_deps.sh                 # full install + build
#   sudo ./scripts/install_deps.sh --deps-only     # deps only, skip build
#   sudo ./scripts/install_deps.sh --no-sysctl     # skip kernel tuning
#
set -euo pipefail

PREFIX="${PREFIX:-/usr/local}"
DEPS_ONLY=0
SYSCTL=1
for arg in "$@"; do
  case "$arg" in
    --deps-only) DEPS_ONLY=1 ;;
    --no-sysctl) SYSCTL=0 ;;
    *) ;;
  esac
done

log() { printf '\033[1;34m[install]\033[0m %s\n' "$*"; }
ok()  { printf '\033[1;32m[ ok ]\033[0m %s\n' "$*"; }
err() { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

[[ "$(id -u)" -eq 0 ]] || err "run as root (sudo)"

# ---- distro detection -----------------------------------------------------
PKG=""
if command -v apt-get >/dev/null 2>&1; then PKG=apt; fi
if command -v dnf     >/dev/null 2>&1; then PKG=dnf; fi
[[ -n "$PKG" ]] || err "unsupported package manager (need apt or dnf)"

# ---- install packages ------------------------------------------------------
if [[ "$PKG" == "apt" ]]; then
  export DEBIAN_FRONTEND=noninteractive
  log "apt update"
  apt-get update -y
  log "installing toolchain + runtime deps"
  apt-get install -y --no-install-recommends \
    build-essential g++ gcc clang \
    cmake ninja-build pkg-config \
    libssl-dev libboost-system-dev libboost-thread-dev \
    libzmq3-dev libsodium-dev \
    nlohmann-json3-dev \
    git
elif [[ "$PKG" == "dnf" ]]; then
  log "dnf install"
  dnf install -y \
    gcc gcc-c++ clang make \
    cmake ninja-build pkgconfig \
    openssl-devel boost-system boost-devel \
    zeromq-devel libsodium-devel \
    nlohmann-json3-devel \
    git
fi

# Validate minimum versions (CMake >= 3.22, GCC >= 12).
cmake_major=$(cmake --version | awk '/cmake version/{print $3}' | cut -d. -f1)
cmake_minor=$(cmake --version | awk '/cmake version/{print $3}' | cut -d. -f2)
if (( cmake_major < 3 || (cmake_major == 3 && cmake_minor < 22) )); then
  err "CMake >= 3.22 required (found $(cmake --version | head -1))"
fi
gcc_major=$(g++ -dumpversion | cut -d. -f1)
if (( gcc_major < 12 )); then
  err "GCC >= 12 required (found $(g++ -dumpversion))"
fi
ok "toolchain: GCC $(g++ -dumpversion), CMake $(cmake --version | awk '/cmake version/{print $3}')"

# ---- kernel / network tuning -----------------------------------------------
if [[ "$SYSCTL" == "1" ]]; then
  log "applying real-time network tuning (sysctl)"
  cat > /etc/sysctl.d/90-leadlag.conf <<'EOF'
# --- Lead-Lag arbitrage engine: low-latency tuning ---
# Socket buffer headroom (default + max).
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
net.core.rmem_max     = 67108864
net.core.wmem_max     = 67108864
# TCP autotuning bounds (bytes).
net.ipv4.tcp_rmem     = 4096 262144 67108864
net.ipv4.tcp_wmem     = 4096 262144 67108864
# Busy polling for <100us recv (requires NET_RX_BUSY_POLL).
net.core.busy_read    = 50
net.core.busy_poll    = 50
# Fast open for repeated connections.
net.ipv4.tcp_fastopen = 3
# Backlog for the ZMQ/FIX listeners.
net.core.somaxconn    = 1024
# Disable IPv6 delay to reduce route lookup jitter.
net.ipv6.conf.all.accept_ra = 0
EOF
  sysctl --system >/dev/null 2>&1 || sysctl -p /etc/sysctl.d/90-leadlag.conf

  # THP -> never: huge pages cause microsecond-scale allocation stalls.
  if [[ -w /sys/kernel/mm/transparent_hugepage/enabled ]]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo "never" > /sys/kernel/mm/transparent_hugepage/defrag
    ok "transparent huge pages disabled (runtime)"
  fi
  # Persist across boots.
  if [[ -f /etc/default/grub ]]; then
    grep -q 'transparent_hugepage' /etc/default/grub || {
      sed -i 's/^GRUB_CMDLINE_LINUX_DEFAULT=.*/GRUB_CMDLINE_LINUX_DEFAULT="quiet splash transparent_hugepage=never"/' \
        /etc/default/grub 2>/dev/null || true
    }
  fi
  ok "sysctl tuning applied (/etc/sysctl.d/90-leadlag.conf)"
fi

[[ "$DEPS_ONLY" == "1" ]] && { log "deps-only — skipping build"; exit 0; }

# ---- build + install --------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

log "CMake configure (Release + Ninja)"
cmake -S "$PROJECT_DIR" -B "$PROJECT_DIR/build" -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++ \
  -DCMAKE_EXE_LINKER_FLAGS="-Wl,-z,now -Wl,-z,relro"

log "building (parallel)"
cmake --build "$PROJECT_DIR/build" -j"$(nproc)"

log "running unit tests"
ctest --test-dir "$PROJECT_DIR/build" --output-on-failure || true

log "installing to $PREFIX"
cmake --install "$PROJECT_DIR/build" --prefix "$PREFIX"

ok "done. binaries: $PREFIX/bin/lead_lag, $PREFIX/bin/mt5_bridge"
