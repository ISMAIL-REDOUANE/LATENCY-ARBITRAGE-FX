# BACKTESTING — Tick Replay & Backtest Guide

The Lead-Lag engine ships a self-contained backtest subsystem that replays
recorded tick data through the **production** strategy and execution code,
without any network or broker connectivity. It lives in:

```
include/backtest/   tick_binary.hpp, execution_simulator.hpp, backtest_engine.hpp
src/backtest/       tick_binary.cpp,  execution_simulator.cpp, backtest_engine.cpp
src/bin/            tick_replay_main.cpp          (the CLI)
tests/              test_backtest.cpp             (CTest unit tests)
```

The replay binary is built as `tick_replay` by CMake and links against the
dependency-free `llm_backtest` library, so it compiles even on a fresh box with
no libzmq/Boost/OpenSSL installed.

---

## 1. Preparing Tick Data

The engine replays two streams: the **lead** feed (fast venue / composite) and
the **lag** feed (retail broker book). Both are stored as compact 24-byte
`BinaryTick` files produced from CSV with the converter.

### Expected CSV format

One header line, then `timestamp,bid,ask,volume` rows. `timestamp` is epoch
milliseconds (an epoch-seconds value `< 1e12` is auto-scaled to ms).

```csv
timestamp,bid,ask,volume
1723050000000,64321.5,64322.1,1.25
1723050000010,64000.0,64001.0,0.5
1723050000020,64200.0,64201.5,2.0
```

Rows with non-numeric fields or non-positive prices are skipped — a single bad
row never aborts the conversion.

### Converting CSV → binary

```bash
./build/tick_replay --convert-csv lead.csv lead.bin
./build/tick_replay --convert-csv lag.csv  lag.bin
```

Output is a flat array of 24-byte `BinaryTick` records (no header). The reader
uses `mmap` (POSIX) / `MapViewOfFile` (Windows) for zero-copy parsing.

---

## 2. Running a Backtest

```bash
./build/tick_replay \
  --lead lead.bin \
  --lag  lag.bin \
  --delay-ms 5 \
  --report build/backtest_report.json \
  --equity build/equity.csv
```

### CLI options

| Flag              | Default                     | Meaning |
|-------------------|-----------------------------|---------|
| `--lead <file>`   | —                           | Lead stream (fast venue) `.bin` |
| `--lag <file>`    | —                           | Lag stream (retail book) `.bin` |
| `--delay-ms <ms>` | `5`                         | Execution latency, `T_signal → fill` |
| `--report <path>` | `build/backtest_report.json`| JSON results file |
| `--equity <path>` | *(none)*                    | Optional equity-curve CSV |
| `--threshold <pts>` | `0.5`                     | Strategy `threshold_pips` override |
| `--latency-buffer <pts>` | `0.2`               | `latency_buffer_pips` override |
| `--margin <pts>`  | `0.3`                       | `min_profit_margin_pips` override |
| `--max-loss <usd>`| `500.0`                     | `max_daily_loss` override |
| `--volume <lots>` | `1.0`                       | Order quantity (notional lots) |
| `--convert-csv <in.csv> <out.bin>` | —    | One-shot CSV→binary helper |
| `--help`          | —                           | Print usage |

The primary run uses `--delay-ms`. Independently, the engine also sweeps a
**Latency Decay Matrix** over `0/5/15/30/50/100` ms to show how the edge decays
as execution speed falls.

> All options are positional-style flags; `--lead` and `--lag` are required for
> replay. Strategy cooldown and the per-interval order cap are disabled in
> replay so every valid signal is simulated.

---

## 3. Reading `backtest_report.json`

```json
{
  "engine": "lead_lag_tick_replay",
  "lead_file": "lead.bin",
  "lag_file": "lag.bin",
  "run_delay_ms": 5,
  "ticks": { "lead": 200, "lag": 200 },
  "signals": 200,
  "fills": 200,
  "rejections": 0,
  "pnl": { "total": -200.000000, "gross_profit": 0.0, "gross_loss": 200.0, "profit_factor": 0.0000 },
  "sharpe": { "per_trade": 0.0, "annualized": 0.0 },
  "drawdown": { "max": 200.0, "max_pct": 20.00 },
  "equity": { "initial": 1000.00, "final": 800.00 },
  "latency_decay": [
    { "delay_ms": 0,   "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 },
    { "delay_ms": 5,   "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 },
    { "delay_ms": 15,  "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 },
    { "delay_ms": 30,  "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 },
    { "delay_ms": 50,  "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 },
    { "delay_ms": 100, "pnl": -200.0, "sharpe": 0.0, "fills": 200, "rejections": 0 }
  ]
}
```

### Field meanings

| Field | Meaning |
|-------|---------|
| `ticks.lead` / `ticks.lag` | Ticks consumed from each stream during the chronological merge. |
| `signals` | Signals emitted by the strategy (risk-gated, cooldown disabled). |
| `fills` / `rejections` | Order outcomes: fills crossed the post-delay book; rejections hit the spread / slippage / invalid-price caps. |
| `pnl.total` | Net round-trip PnL for the whole run (negative when the retail spread exceeds the captured edge). |
| `pnl.gross_profit` / `gross_loss` | Sum of winning / losing trades. |
| `pnl.profit_factor` | `gross_profit / |gross_loss|` (infinity is reported as `1e9` when there are no losses). |
| `sharpe.per_trade` | `mean(pnl) / std(pnl)` over filled trades. |
| `sharpe.annualized` | `per_trade × sqrt(252)` — a very rough annualization, valid for comparing runs with similar trade counts. |
| `drawdown.max` / `max_pct` | Peak-to-trough equity drawdown, in currency and as % of running peak. |
| `equity.initial` / `final` | Start / end equity. Initial = `volume × 1000` nominal per-lot capital. |

### Latency Decay Matrix

Each `latency_decay` row re-runs the same replay with that `delay_ms`. Read it
as a curve: as the delay grows, the lag feed re-prices toward the lead, so the
edge captured by the strategy shrinks. A healthy setup shows **PnL flattening
or turning negative as delay increases** — that is the latency budget the
production execution path must stay inside.

In the example above every delay yields `-1.0` per trade because the synthetic
lead is a constant 100 pts above the lag; the entire run loses exactly the
1-pt retail spread on each fill (buy at `ask`, exit at `bid`).

---

## 4. Running the Backtest CTest Suite

```bash
# Configure + build the backtest targets (dependency-free).
cmake -S latency_arb -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --target backtest_tests tick_replay

# Run only the backtest tests.
ctest --test-dir build -R backtest_tests --output-on-failure

# Or the full suite (latency + backtest).
ctest --test-dir build --output-on-failure
```

`test_backtest.cpp` covers: the 24-byte `BinaryTick` layout, CSV→binary
conversion round-trip, mmap binary search, execution-simulator fill mechanics
(delay, slippage, spread markup, commissions, spread/slippage rejection,
single-slot ordering), and end-to-end engine metrics (PnL, Sharpe, profit
factor, drawdown, latency-decay rows).

---

## 5. End-to-End Quick Start

```bash
# 1) Build.
cmake -S latency_arb -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build --target tick_replay backtest_tests

# 2) Convert two CSVs.
./build/tick_replay --convert-csv data/lead.csv data/lead.bin
./build/tick_replay --convert-csv data/lag.csv  data/lag.bin

# 3) Replay with 5 ms execution delay.
./build/tick_replay --lead data/lead.bin --lag data/lag.bin \
    --delay-ms 5 --report build/backtest_report.json --equity build/equity.csv

# 4) Inspect the results.
cat build/backtest_report.json      # PnL, Sharpe, PF, MDD, decay matrix
head build/equity.csv               # per-trade equity curve
```
