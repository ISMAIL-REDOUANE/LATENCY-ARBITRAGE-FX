# latency_arb — Lead-Lag Multi-Venue Arbitrage Engine

A C++20, low-latency cross-venue arbitrage engine. It ingests live market data
from Binance and Deribit over secure WebSockets, fuses them into a single
composite "lead" price, decides a fair-value gap against a broker retailed
quote, and emits execution signals over a zero-allocation wire codec — with
sub-microsecond Tick-to-Trade telemetry and a drop-in C++ MT5 execution bridge.

Goals, in order:

1. **Deterministic low latency** — lock-free, allocation-free hot paths, mmap
   logging, rdtsc timestamping.
2. **Predictable risk** — hard caps on lots, daily loss, per-interval orders,
   and slippage.
3. **Safe by default** — `DRY_RUN=true` until quotes and execution are
   production-validated.

---

## Table of Contents

- [System Architecture](#system-architecture)
- [Micro-Performance & Telemetry Specs](#micro-performance--telemetry-specs)
- [Build & Testing Guide](#build--testing-guide)
- [Deployment Options](#deployment-options)
- [Environment Configuration](#environment-configuration)

---

## System Architecture

```
                ┌──────────────────────────────────────────────────────────┐
                │                          ENGINE                         │
  Binance       │   ┌──────────┐   ┌──────────────────┐   ┌─────────────┐  │
  WSS stream ───┼──▶│ Binance  │──▶│                  │   │             │  │
  (b-"stream")  │   │ Reader   │   │                  │   │   Strategy  │  │
                │   └──────────┘   │    Tick          │   │   Engine    │  │
  Deribit       │   ┌──────────┐   │    Aggregator    │   │  (lead-lag  │  │
  WSS     ──────┼──▶│ Deribit  │──▶│  (composite      │──▶│  decision)  │──┼─► signal
  (wss://)      │   │ Reader   │   │   lead price)    │   │             │  │
                │   └──────────┘   │                  │   └─────────────┘  │
                │                  └──────────────────┘                     │
                │                          │ lead + edge                    │
                │                          ▼                                │
                │                   ┌─────────────────┐   ┌───────────────┐ │
                │   broker ZMQ ──▶  │  Execution      │──▶│   ZMQ PUB     │──┼─►  MT5 / FIX
                │   quote (5556)    │  Dispatcher     │   │  (latency_arb)│ │
                │                   └─────────────────┘   └───────────────┘ │
                └──────────────────────────────────────────────────────────┘
```

### 1. WebSocket Ingestion

- **`binance_ws` / `deribit_ws`** — custom WSS clients built on Boost.Asio +
  Boost.Beast with OpenSSL. Each runs its own `io_context` thread.
- **Reconnect with backoff**: base `WS_RECONNECT_BASE_S`, capped by
  `WS_RECONNECT_MAX_S`; keepalive ping every `WS_PING_INTERVAL_S`.
- Each reader pushes `Tick` frames into an **SPSC lock-free ring** — the WS
  read loop only deposits and never blocks on disk or the consumer.

### 2. Tick Aggregator (`aggregator`)

- Drains both venue rings and maintains a rolling per-venue window, dropping
  ticks older than a max-age (stale data never pollutes the signal).
- Fuses the freshest Binance/Deribit mid into a **composite lead price**.
- **Session-aware weighting** — session via `US_OPEN/CLOSE_HOUR_UTC`:
  - US hours   : Deribit 60 / Binance 40
  - Asian hours: Deribit 40 / Binance 60
- Graceful **venue fallback** to the single available feed. Lead is
  (`round4`) rounded to four decimals when enabled.

### 3. Lead-Lag Strategy Engine (`strategy`)

- Compares the composite lead against the broker's retailed `bid/ask`:
  - `lead > ask + threshold` → **Long**
  - `bid > lead + threshold` → **Short**
  - else → **Hold**
- **Dynamic threshold** (no double-counting):
  `threshold = broker_spread + latency_buffer_pips + min_profit_margin_pips`,
  floored at `threshold_pips` (derived spread when not precomputed).
- **Risk gates**: `max_open_lots`, `max_daily_loss`, per-interval order cap
  set by `max_orders_per_interval`, and a cooldown so at most one signal fires
  per `interval_seconds` window.
- Emits a `Signal` carry the side, lead, quote, threshold, edge, ts.

### 4. Low-Latency ZMQ/FIX Execution Pipeline

- **`execution`** dispatcher thread pulls signals, applies the **slippage cap**
  (`within_slippage_cap`), and publishes.
- **`zmq_pub`** publishes the signal frame over the negotiated socket
  (`ZMQ_PUB_BIND`).
- **`mt5_bridge`** — a pure C++20 native MT5 execution client subscribing on
  the same ZMQ pipe (no Python). Feet at `MT5_BRIDGE_HOST:MT5_BRIDGE_PORT`.
- **`fix_client`** — FIX 4.4 session for direct execution when
  `FIX_ENABLED=true`.

---

## Micro-Performance & Telemetry Specs

The hot path is engineered so that **no allocation, no lock, and no syscall**
ever appears between the network read and the signal publish.

### Zero-allocation Lock-Free SPSC Ring Buffer (`ring`, `tick`)

- `SpscRing<T, Capacity=4096>` — **single producer / single consumer**,
  power-of-two capacity so the wrap is a mask.
- `push()` **overwrites the oldest slot when full (drop-oldest) and never
  blocks** — the WS read-loop deposit path is off the disk/network clock.
- `release/acquire` ordering makes a slot's payload visible to the consumer
  exactly when it becomes pop-able; `T` must be trivially copyable.

### 2. x86 RDTSC Nanosecond Timestamping (`telemetry`)

- `rdtsc()` — x86 instruction-inline (`_rdtscp`/`__rdtscp`), sub-nanosecond
  resolution; `steady_clock` for calibration and non-x86 builds.
- `bench_mark(stage)` stamps a fixed 32-byte `LatencySample`
  on the Tick-to-Trade path at five gates —
  `kStageTickRx → kStageAggregate → kStageStrategy → kStageSignalTx →
  kStageExecSend`.
- The stamp is a **relaxed fetch-add + release-store into a pre-allocated
  power-of-two ring** (`VirtualAlloc`/`mmap`) — **zero allocation** and ~
  measured to a **few hundred ns per stamp**.
- `bench_stats()` derives min/avg/max + **p50/p99** over a fixed window, no
  heap allocation; TSC frequency is calibrated once.
  - **Tip-to-Trade: ~150 ns** on a typical server core (`latency_tests`).

### 3. Zero-Allocation Wire Codec (`wire`)

- `encode_signal` / `decode_signal` — a fixed-capacity, **zero-alloc**, exact
  wire framing of a `Signal` (side, reason, lead, bid, ask, threshold, edge,
  timestamp). No `std::stringstream`, no heap.
- `encode` returns bytes written (or `-1` when the buffer is too small) and
  **never truncates** a signal — the publisher drops rather than corrupts.
- Malformed frames are rejected on decode. Verified under a global
  `operator new` counter: **1,000,000 encodes = 0 heap allocations**.

---

## Build & Testing Guide

> Requires CMake ≥ 3.22 and GCC ≥ 12 (or Clang ≥ 14). The test suite itself
> only compiles the pure sources, so it works even before libzmq/Boost/OpenSSL
> are installed.

### 1. Install dependencies

```bash
# Debian / Ubuntu 24.04
sudo apt-get update -y
sudo apt-get install -y \
  build-essential cmake ninja-build pkg-config \
  libzmq3-dev libssl-dev libboost-all-dev libsodium-dev nlohmann-json3-dev
```

or use the provided helper:

```bash
sudo ./scripts/install_deps.sh          # installs deps + builds + runs ctest
sudo ./scripts/install_deps.sh --deps-only
sudo ./scripts/install_deps.sh --no-sysctl
```

### 2. Configure & build (Release, Ninja)

```bash
cmake -S latency_arb -B latency_arb/build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER=g++
```

Production binaries get the low-latency flag set via CMakeLists:
`-O3 -march=native -flto -funroll-loops -ffast-math` (the *test* target stays
`-O2` so its FP/rounding assertions are deterministic).

```bash
cmake --build latency_arb/build -j"$(nproc)" \
  --target lead_lag mt5_bridge latency_tests
```

### 3. Run tests

```bash
ctest --test-dir latency_arb/build --output-on-failure
```

Or launch the self-contained suite directly (it needs no third-party libs):

```bash
./latency_arb/build/latency_tests
```

Reports (from latest run): **33 test cases, 24,722 checks, 0 failures**.

### 4. Micro-benchmarks (printed by `latency_tests`)

| Metric | Value (host-measured) |
|-------|-----------------------|
| SPSC ring `push+pop` | ~2.8 ns/op |
| Wire codec `encode` | ~3.7 µs/op (0 heap allocs) |
| Tick-to-Trade `bench_mark` | ~85–150 ns/stamp |

> Numbers are sensitive to core clock, `-march=native`, and THP state; they are
> sanity bounds, not guarantees.

---

## Deployment Options

### Option A — Docker Compose stack

```bash
cd latency_arb
docker compose up --build -d       # dry-run engine (DRY_RUN=true pinned)
docker compose run --rm tests      # run the benchmark/unit suite once
docker compose ps
```

- Multi-stage `Dockerfile` (Ubuntu 24.04): a **builder** compiles all three
  targets, the **runtime** stage keeps only the linked `.so` libraries and the
  binaries + config.
- `network_mode: host` — the engine binds a ZMQ PUB on `ipc:` and subscribes on
  `tcp://127.0.0.1:5556`, so it needs the host namespace.
- Container runs as non-root `leadlag`; logs under a named `lead_lag_logs`
  volume (< `/var/log/lead_lag`).
- ⚠️ The build uses `-march=native`, so the image must run on a
  compatible CPU.

### Option B — systemd service

```bash
sudo ./scripts/deploy_linux.sh                        # apt deps + sysctl + build + systemd dry-run
sudo ./scripts/deploy_linux.sh --method compose      # …or via docker
sudo ./scripts/deploy_linux.sh --method systemd --install-dir /opt/lead_lag
```

Alternatively install the committed unit directly:

```bash
sudo cp latency_arb/systemd/lead_lag.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now lead_lag
sudo journalctl -u lead_lag -f
```

The unit runs `SCHED_RR` (RR priority 99) for the arbitration threads, with
`LimitMEMLOCK=infinity`, `Restart=always`, and a hardened sandbox
(`ProtectSystem=strict`, `PrivateTmp`, `NoNewPrivileges`).

### Option C — Kernel network tuning

`deploy_linux.sh` (and `install_deps.sh --no-sysctl` skip) writes
`/etc/sysctl.d/90-leadlag.conf`:

```bash
net.core.rmem_max / wmem_max     = 67 MB      # socket buffer headroom
net.core.busy_read / busy_poll  = 50 µs       # busy-poll (NET_RX_BUSY_POLL)
net.ipv4.tcp_fastopen           = 3           # fast open on repeated conns
net.core.somaxconn              = 1024        # ZMQ / FIX listener backlog
net.ipv6.conf.all.accept_ra      = 0
```

and **disables transparent huge pages** (THP → `never`, plus a GRUB cmdline
persist) because THP causes microsecond-scale allocation stalls. Realtime
scheduling can additionally benefit from the `SCHED_RR` policy in the unit.

---

## Environment Configuration

All settings are read from environment variables by `Config::from_env()`. The
canonical template is [`config/ll.env`](config/ll.env). Every variable below
appears in the systemd unit and the compose stack.

| Variable | Default | Purpose |
|---------|--------|---------|
| `BINANCE_SYMBOL` | `btcusdt` | Binance market symbol |
| `BINANCE_HOST` | `stream.binance.com` | Binance WSS host |
| `BINANCE_PORT` | `9443` | Binance WSS port |
| `DERIBIT_SYMBOL` | `BTC` | Deribit instrument |
| `DERIBIT_WS` | `wss://www.deribit.com/ws/api/v2` | Deribit WSS endpoint |
| `US_OPEN_HOUR_UTC` / `US_CLOSE_HOUR_UTC` | `13` / `20` | USD session window (session) |
| `DERIBIT_WEIGHT_ASIA` / `BINANCE_WEIGHT_ASIA` | `0.40` / `0.60` | Venue weight in Asia |
| `DERIBIT_WEIGHT_US` / `BINANCE_WEIGHT_US` | `0.60` / `0.40` | Venue weight in US |
| `USE_DST` | `true` | Daylight-saving-time handling for session |
| `THRESHOLD_PIPS` | `0.5` | Absolute floor on the dynamic threshold |
| `LATENCY_BUFFER_PIPS` | `0.2` | Latency-penalty buffer (added to spread) |
| `MIN_PROFIT_MARGIN_PIPS` | `0.3` | Minimum-profit margin (added to spread) |
| `MIN_LOT_PIPS` | `0.0` | Minimum lot size (pips) |
| `MAX_OPEN_LOTS` | `1` | Max concurrent open positions |
| `MAX_DAILY_LOSS` | `500.0` | Max daily loss allowed (close when hit) |
| `MAX_ORDERS_PER_INTERVAL` | `5` | Order cap per interval window |
| `INTERVAL_SECONDS` | `60` | Signal cooldown interval |
| `MAX_TRADE_PER_INTERVAL` | `1.0` | Max trade per interval |
| `FOUR_DECIMAL_ROUNDING` | `1` | Round doubles to 4 decimals at scope start |
| `WS_PING_INTERVAL_S` | `20` | WSS keep-alive ping interval |
| `WS_RECONNECT_BASE_S` / `WS_RECONNECT_MAX_S` | `2` / `60` | Reconnect backoff bounds |
| `BROKER_ZMQ_BIND` | `tcp://127.0.0.1:5556` | Broker ZMQ subscriber bind |
| `BROKER_TOPIC` | `quote` | Broker subscription topic |
| `ZMQ_PUB_BIND` | `ipc:///tmp/latency_arb.ipc` | Signal publisher endpoint (MT5 bridge) |
| `FIX_ENABLED` | `false` | Enable FIX 4.4 direct execution |
| `FIX_HOST` / `FIX_PORT` | `127.0.0.1` / `5200` | FIX gateway endpoint |
| `FIX_SENDER_COMP_ID` | `LEADLAG` | FIX sender-comp-id |
| `FIX_TARGET_COMP_ID` | `BROKER` | FIX target-comp-id |
| `DRY_RUN` | `true` | Dry-run: publish signals, place no real orders |
| `DRY_RUN_SYMBOL` | `BTC/USD` | Symbol label in dry run |
| `TRADE_AMOUNT` | `0.001` | Trade quantity |
| `MT5_BRIDGE_HOST` / `MT5_BRIDGE_PORT` | `127.0.0.1` / `6161` | C++ MT5 bridge socket |
| `MT5_BRIDGE_AUTH` | *(set)* | MT5 bridge shared secret |
| `EA_ORDER_MODE` | `IOC` | MT5 order mode |
| `MT5_SYMBOL` | `BTCUSD` | MT5 symbol for the bridge |
| `MT5_VOLUME` | `0.01` | MT5 volume |
| `MMAP_LOG_DIR` | `1` | Write telemetry log via mmap |
| `MMAP_PREALLOC_MB` | `1` | Initial mmap region (MB) |
| `MMAP_PREALLOC_BIG_MB` | `10` | Big mmap region before threads (MB) |
| `LOG_DIR` | `/var/log/lead_lag` | Log / telemetry directory |
| `CPU_STRATEGY` | `2` | CPU core pin for the strategy worker |
| `CPU_EXEC` | `3` | CPU core pin for the execution worker |

Run with this file as your environment:

```bash
set -a; source latency_arb/config/ll.env; set +a
./latency_arb/build/lead_lag
```

> **Safety**: `DRY_RUN=true` is pinned in `config/ll.env`, the compose stack,
> and the systemd unit. Flip it only after quotes and execution are validated
> against paper/target credentials.