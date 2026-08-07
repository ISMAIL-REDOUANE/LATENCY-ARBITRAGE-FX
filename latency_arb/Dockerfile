# =============================================================================
# lead_lag — Multi-stage Docker build for the Lead-Lag Arbitrage engine.
#
#   Stage 1 (builder):  Ubuntu 24.04 + full toolchain & runtime dev deps,
#                       compiles the three release targets with production
#                       low-latency flags: lead_lag, mt5_bridge, latency_tests.
#   Stage 2 (runtime):  lean Ubuntu 24.04 with only the shared libraries the
#                       binaries link against, plus binaries + config + unit.
#
# Build:
#   docker build -t lead_lag:latest -f latency_arb/Dockerfile latency_arb
#
# Run (single-binary, foreground):
#   docker run --rm --network host \
#       --env-file latency_arb/config/ll.env \
#       -v /var/log/lead_lag:/var/log/lead_lag \
#       lead_lag:latest /opt/lead_lag/lead_lag
#
# Or run the whole dry-run stack with compose:
#   docker compose up --build -d
#
# NOTE: the build uses -march=native (matches CMakeLists release flags), so the
# image must run on the same (or compatible) CPU family it was built on.
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1 — build
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive

# Toolchain + runtime development libraries.
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      g++ \
      gcc \
      clang \
      cmake \
      ninja-build \
      pkg-config \
      git \
      ca-certificates \
      \
      libzmq3-dev \
      libssl-dev \
      libboost-all-dev \
      libsodium-dev \
      nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy the whole project (engine + tests + configs + systemd unit).
COPY . /src

# Configure with Ninja in Release mode, then build all three targets with the
# production flag set enforced in CMakeLists (-O3 -march=native -flto
# -funroll-loops -ffast-math for binaries; tests stay -O2 for FP correctness).
RUN cmake -S /src -B /src/build -G Ninja \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_CXX_COMPILER=g++ \
      -DCMAKE_EXE_LINKER_FLAGS="-Wl,-z,now -Wl,-z,relro" \
 && cmake --build /src/build --config Release \
      --target lead_lag mt5_bridge latency_tests -j"$(nproc)"

# Run the self-contained benchmark suite now, while the toolchain image is
# still warm, so a regression fails the build instead of the deployment.
RUN ctest --test-dir /src/build --output-on-failure

# ---------------------------------------------------------------------------
# Stage 2 — minimal runtime image (binaries + configs only).
# ---------------------------------------------------------------------------
FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

# Only the shared libraries the binaries link against (no dev headers, no
# toolchain). Lead-Lag is a leaf service; ubuntu user keeps image least-priv.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libzmq5 \
      libssl3 \
      libsodium23 \
      libboost-system1.83.0 \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user that owns logs + the service data dir.
RUN useradd --system --uid 1001 --home-dir /opt/lead_lag --shell /usr/sbin/nologin \
      leadlag \
 && mkdir -p /opt/lead_lag /var/log/lead_lag \
 && chown -R leadlag:leadlag /opt/lead_lag /var/log/lead_lag

WORKDIR /opt/lead_lag

# Production binaries.
COPY --from=build --chown=leadlag:leadlag /src/build/lead_lag     /opt/lead_lag/
COPY --from=build --chown=leadlag:leadlag /src/build/mt5_bridge  /opt/lead_lag/
COPY --from=build --chown=leadlag:leadlag /src/build/latency_tests \
                                                 /opt/lead_lag/

# Configuration template + systemd unit (kept in-image for reference / manual
# operation; env is injected at runtime via --env-file or compose).
COPY --chown=leadlag:leadlag config/ll.env              /opt/lead_lag/ll.env
COPY --chown=leadlag:leadlag systemd/lead_lag.service  /opt/lead_lag/lead_lag.service

# Telemetry log + mmap region.
VOLUME ["/var/log/lead_lag"]

USER leadlag

# Default dry-run entrypoint (overridden by compose with a test step).
ENTRYPOINT ["/opt/lead_lag/lead_lag"]