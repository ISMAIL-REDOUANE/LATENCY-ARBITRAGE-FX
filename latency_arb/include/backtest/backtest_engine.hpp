#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "backtest/execution_simulator.hpp"
#include "backtest/tick_binary.hpp"
#include "llm/config.h"
#include "llm/strategy.h"

namespace llm {

// ===========================================================================
// Backtest engine — chronologically merges two binary tick streams (the
// "lead" fast venue and the "lag" retail feed) and replays them through the
// production Lead-Lag Strategy with an ExecutionSimulator in front of the
// book.
//
// Pipeline per merged tick:
//   1. Advance whichever stream has the earlier timestamp.
//   2. A lead tick updates the current composite lead mid.
//   3. A lag tick updates the retail bid/ask and, when the lead is fresh,
//      evaluates the strategy (long/short/hold via Strategy::evaluate and
//      risk-gated via maybe_emit).
//   4. Emitted signals are submitted to the simulator at T_signal.
//   5. When the replay clock reaches T_signal + delay, the fill is evaluated
//      against the post-delay lag quote.
//
// Metrics: PnL, profit factor, per-trade + annualized Sharpe, max drawdown,
// and a latency-decay matrix (net PnL for a sweep of execution delays).
// ===========================================================================

struct BacktestResult {
    // Trade attribution.
    int64_t       start_ts_ms = 0;
    int64_t       end_ts_ms   = 0;
    size_t        lead_ticks_consumed  = 0;
    size_t        lag_ticks_consumed   = 0;
    size_t        signals_emitted = 0;
    size_t        fills           = 0;
    size_t        rejections      = 0;

    // PnL.
    double        initial_capital = 0.0;
    double        final_equity    = 0.0;
    double        total_pnl       = 0.0;
    double        gross_profit    = 0.0;
    double        gross_loss      = 0.0;
    double        profit_factor   = 0.0;   // gross_profit / |gross_loss|
    double        max_drawdown    = 0.0;   // peak-to-trough equity (currency)
    double        max_drawdown_pct= 0.0;   // same, as % of running peak
    double        sharpe_per_trade= 0.0;   // mean(pnl)/std(pnl)
    double        sharpe_annual  = 0.0;    // per-trade Sharpe * sqrt(252)

    // Per-trade detail (for the equity curve / report).
    struct Trade {
        int64_t signal_ts_ms = 0;
        int64_t fill_ts_ms   = 0;
        Side    side         = Side::None;
        double  signal_mark  = 0.0;   // lead at signal time
        double  fill_price   = 0.0;
        double  pnl          = 0.0;
        double  equity_after = 0.0;
        bool    rejected     = false;
        const char* reason   = nullptr;
    };
    std::vector<Trade> trades;

    int delay_ms = 0;   // execution delay used for this run (for the matrix)
};

class BacktestEngine {
public:
    // Takes ownership semantics-free references: readers stay owned by the
    // caller (tick_replay / tests) and must outlive the engine.
    BacktestEngine(const MmapTickReader& lead, const MmapTickReader& lag,
                   const Config& cfg);

    // Run one full replay with the given execution parameters.
    BacktestResult run(const ExecutionParams& params);

    // Latency decay matrix: net PnL (and fills) for a sweep of delays.
    // Each entry re-runs the replay with that delay value.
    struct DecayRow {
        int     delay_ms   = 0;
        double  net_pnl    = 0.0;
        double  sharpe     = 0.0;
        size_t  fills      = 0;
        size_t  rejections = 0;
    };
    std::vector<DecayRow> latency_decay_matrix(
        const std::vector<int>& delays,
        const ExecutionParams&  base_params);

private:
    struct State;
    BacktestResult run_impl(const ExecutionParams& params, int delay_ms);

    const MmapTickReader& lead_;
    const MmapTickReader& lag_;
    Config cfg_;
};

}  // namespace llm