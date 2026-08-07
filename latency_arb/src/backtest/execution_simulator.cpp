#include "backtest/execution_simulator.hpp"

#include <cmath>
#include <utility>

namespace llm {

ExecutionSimulator::ExecutionSimulator(ExecutionParams p) : params_(std::move(p)) {}

bool ExecutionSimulator::submit(Side side, int64_t signal_ts_ms, double signal_mid,
                                double lag_bid, double lag_ask) {
    if (pending_) return false;  // single-slot tracker: one in flight at a time
    if (side != Side::Buy && side != Side::Sell) return false;
    if (lag_bid <= 0.0 || lag_ask <= 0.0 || lag_ask < lag_bid) return false;

    pending_           = true;
    side_              = side;
    signal_ts_ms_      = signal_ts_ms;
    target_fill_ts_ms_ = signal_ts_ms + static_cast<int64_t>(params_.delay_ms);
    signal_lane_mid_   = signal_mid;
    signal_lag_bid_    = lag_bid;
    signal_lag_ask_    = lag_ask;
    return true;
}

bool ExecutionSimulator::at_fill(int64_t now_ms, double exec_lag_bid,
                                 double exec_lag_ask, Fill& out) {
    out = Fill{};
    if (!pending_) return false;
    if (now_ms < target_fill_ts_ms_) return false;  // not yet due

    const double spread = exec_lag_ask - exec_lag_bid;
    out.side            = side_;
    out.signal_ts_ms    = signal_ts_ms_;
    out.fill_ts_ms      = now_ms;
    out.target_fill_ts_ms = target_fill_ts_ms_;
    out.signal_mark     = signal_lane_mid_;
    out.signal_bid      = signal_lag_bid_;
    out.signal_ask      = signal_lag_ask_;
    out.exec_bid        = exec_lag_bid;
    out.exec_ask        = exec_lag_ask;

    // ---- rejection: spread cap ------------------------------------------ //
    if (spread > params_.spread_cap_pts) {
        out.rejected      = true;
        out.reject_reason = "spread_cap";
        pending_ = false;
        return true;
    }

    // ---- fill price (cross the book + markup + slippage) ------------------ //
    double fill;
    if (side_ == Side::Buy) {
        fill = exec_lag_ask + params_.spread_markup_pts + params_.slippage_pts;
    } else {
        fill = exec_lag_bid - params_.spread_markup_pts - params_.slippage_pts;
    }
    out.fill_price   = fill;
    out.slippage_pts = params_.slippage_pts;
    out.commission   = params_.commission_per_lot * params_.quantity;

    // ---- rejection: modeled slippage cap --------------------------------- //
    if (params_.slippage_pts > params_.slippage_cap_pts) {
        out.rejected      = true;
        out.reject_reason = "slippage_cap";
        pending_ = false;
        return true;
    }

    // Conservative no-fill sanity: a fill that trades outside a plausible band
    // (e.g. negative/zero price from a corrupt post-delay quote) is rejected.
    if (!(fill > 0.0) || !std::isfinite(fill)) {
        out.rejected      = true;
        out.reject_reason = "invalid_price";
        pending_ = false;
        return true;
    }

    out.filled = true;
    pending_   = false;
    return true;
}

}  // namespace llm