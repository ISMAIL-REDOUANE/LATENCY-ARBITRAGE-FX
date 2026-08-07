#pragma once

#include <cstdint>
#include <string>

#include "llm/strategy.h"   // for llm::Side

namespace llm {

// PnL attribution model for the backtest engine.
//
//  * Instant — the historical behaviour: a buy fills at the fill-time ask and
//    is immediately marked to the fill-time bid (and vice-versa for a sell).
//    Because both legs are against the SAME fill-time book, this always pays
//    the full spread on every trade and can never realise a positive edge.
//
//  * HoldConvergence — the "fixed" latency-arbitrage model: the fill opens a
//    held position, which is closed at the first later lag tick where the lag
//    mid has converged to the lead mid (or after a max holding period / at end
//    of stream). The long exits at the lag bid, the short at the lag ask, so
//    the strategy can actually capture the lead-to-lag catch-up it bet on.
enum class PnlModel : int {
    Instant        = 0,
    HoldConvergence = 1,
};

// ===========================================================================
// Execution simulator — models a market fills against the lag/retail feed at
// time (T_signal + T_exec).
//
// The simulator is deliberately decoupled from the replay loop:
//   * submit() records a latent order with the quote captured at signal time
//     and the chosen execution delay.
//   * at_exec() is called when the replay clock reaches (T_signal + T_exec)
//     with the *post-degree* lag quote; it applies the fill model and returns
//     a Fill. Only one pending order is tracked at a time.
//
// Fill model:
//   * Buy  : fill = lag_ask_at_exec + spread_markup + slippage
//   * Sell : fill = lag_bid_at_exec - spread_markup - slippage
//   * commissions are per-lot (params.commission_per_lot * lots).
//   * rejection: if the executable spread is wider than spread_cap_pts OR the
//     realized adverse slippage is beyond slippage_cap_pts, the order is
//     rejected (no partial fill — a conservative no-fill model).
// ===========================================================================

struct ExecutionParams {
    int      delay_ms          = 5;     // execution latency (T_signal -> fill)
    double   slippage_pts       = 0.0;  // fixed adverse slippage on the fill
    double   spread_markup_pts  = 0.0;  // fixed spread markup added to the fill
    double   commission_per_lot = 0.0;  // per-lot commission (currency)
    double   spread_cap_pts     = 100.0; // reject if post-delay spread > cap
    double   slippage_cap_pts   = 100.0; // reject if modeled slippage > cap
    double   quantity           = 1.0;  // notional lots on the order

    // PnL attribution (inputs to the engine's fill book-keepers).
    PnlModel pnl_model          = PnlModel::HoldConvergence;
    // HoldConvergence only: exit when |lag_mid - lead_mid| <= this (points).
    double   hold_tol_pts       = 0.05;
    // HoldConvergence only: max hold in ms (0 = no time cap; closes at stream
    // end only if the convergence test never fires).
    int      hold_max_ms        = 0;
    // HoldConvergence only: forward the signal-time bid/ask into the closed
    // trade record (for attribution) — the fill itself is at fill-time quotes.
    double   hold_exit_markup_pts = 0.0;  // exit-side markup (like fill-side)
    double   hold_exit_slip_pts   = 0.0;  // exit-side slippage
};

struct Fill {
    bool      filled      = false;
    bool      rejected    = false;
    const char* reject_reason = nullptr;
    Side      side        = Side::None;
    int64_t   signal_ts_ms = 0;
    int64_t   fill_ts_ms   = 0;   // = signal_ts_ms + delay_ms
    int64_t   target_fill_ts_ms = 0;  // computed at submit
    double    signal_mark  = 0.0;   // lead at signal time (for pnl attribution)
    double    signal_bid   = 0.0;   // lag bid at signal time
    double    signal_ask   = 0.0;   // lag ask at signal time
    double    exec_bid     = 0.0;   // lag bid at fill time
    double    exec_ask     = 0.0;   // lag ask at fill time
    double    fill_price   = 0.0;   // modeled price actually traded at
    double    commission   = 0.0;
    double    slippage_pts = 0.0;   // actual modeled adverse slippage
};

class ExecutionSimulator {
public:
    explicit ExecutionSimulator(ExecutionParams p);

    // Begin tracking an order. Records the signal-time quote + target fill
    // timestamp. Returns false if a prior order is still pending (single-slot).
    bool submit(Side side, int64_t signal_ts_ms, double lane_mark,
                double lag_bid, double lag_ask);

    // If it is now past the target fill time and an order is pending, evaluate
    // against `exec_lag_bid/ask` (the post-delay quote) and populate `out`.
    // Returns true when a Fill was produced (filled or rejected) and clears the
    // pending order; false when still waiting or nothing pending.
    bool at_fill(int64_t now_ms, double exec_lag_bid, double exec_lag_ask,
                 Fill& out);

    bool   pending() const { return pending_; }
    int64_t target_fill_ts() const { return target_fill_ts_ms_; }
    void   cancel() { pending_ = false; }

    const ExecutionParams& params() const { return params_; }

private:
    ExecutionParams params_;
    bool    pending_          = false;
    Side    side_             = Side::None;
    int64_t signal_ts_ms_     = 0;
    int64_t target_fill_ts_ms_= 0;
    double  signal_lane_mid_  = 0.0;
    double  signal_lag_bid_   = 0.0;
    double  signal_lag_ask_   = 0.0;
};

}  // namespace llm