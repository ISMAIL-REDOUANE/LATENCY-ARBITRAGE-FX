#include "backtest/backtest_engine.hpp"

#include <algorithm>
#include <cmath>
#include <utility>

namespace llm {

namespace {

// per-trade Sharpe; mean/std of trade pnl, 0 when <2 trades or std==0.
double sharpe_of(const std::vector<BacktestResult::Trade>& trades,
                 size_t filled) {
    if (filled < 2) return 0.0;
    double sum = 0.0;
    for (const auto& t : trades) {
        if (t.rejected) continue;
        sum += t.pnl;
    }
    const double mean = sum / static_cast<double>(filled);
    double var = 0.0;
    for (const auto& t : trades) {
        if (t.rejected) continue;
        const double d = t.pnl - mean;
        var += d * d;
    }
    var /= static_cast<double>(filled);
    const double sd = std::sqrt(var);
    if (sd <= 1e-12) return 0.0;
    return mean / sd;
}

}  // namespace

// ---------------------------------------------------------------------------
// BacktestEngine
// ---------------------------------------------------------------------------
BacktestEngine::BacktestEngine(const MmapTickReader& lead,
                               const MmapTickReader& lag,
                               const Config& cfg)
    : lead_(lead), lag_(lag), cfg_(cfg) {}

BacktestResult BacktestEngine::run(const ExecutionParams& params) {
    return run_impl(params, params.delay_ms);
}

BacktestResult BacktestEngine::run_impl(const ExecutionParams& params,
                                        int delay_ms) {
    BacktestResult out;
    out.delay_ms        = delay_ms;
    out.initial_capital = params.quantity * 1000.0;  // nominal per-lot capital

    if (!lead_.is_open() || !lag_.is_open()) return out;
    if (lead_.count() == 0 || lag_.count() == 0) return out;

    Strategy strategy(cfg_);
    ExecutionSimulator sim([&] {
        ExecutionParams p = params;
        p.delay_ms = delay_ms;
        return p;
    }());

    const BinaryTick* lead = lead_.data();
    const BinaryTick* lag  = lag_.data();
    const size_t n_lead = lead_.count();
    const size_t n_lag  = lag_.count();

    size_t i_lead = 0, i_lag = 0;
    double lead_mid = 0.0;
    double lag_bid = 0.0, lag_ask = 0.0;
    double last_bid = 0.0, last_ask = 0.0, last_lead = 0.0;

    double equity = out.initial_capital;
    double peak   = equity;
    double gross_p = 0.0, gross_l = 0.0;

    // -- held position (PnlModel::HoldConvergence) ------------------------ --
    bool   has_open  = false;
    Side   open_side = Side::None;
    int64_t open_sig_ts = 0, open_fill_ts = 0;
    double open_price = 0.0, open_comm = 0.0, open_mark = 0.0;

    auto record_trade = [&](BacktestResult::Trade& tr) {
        tr.equity_after = equity;
        out.trades.push_back(tr);
        peak = std::max(peak, equity);
        const double dd = peak - equity;
        if (dd > out.max_drawdown) {
            out.max_drawdown     = dd;
            out.max_drawdown_pct = peak > 0.0 ? dd / peak * 100.0 : 0.0;
        }
    };

    // Place an executed fill into the open (held) position.
    auto open_position = [&](const Fill& f) {
        has_open     = true;
        open_side    = f.side;
        open_sig_ts  = f.signal_ts_ms;
        open_fill_ts = f.fill_ts_ms;
        open_price   = f.fill_price;
        open_comm    = f.commission;
        open_mark    = f.signal_mark;
    };

    // Close the held position against a later tick, realising the pnl.
    //   * force == true : force-close (max-hold elapsed or end of stream).
    //   * otherwise     : close when the lag mid has converged to the lead
    //                     mid within `hold_pts_`, or the max hold expired.
    auto close_open = [&](int64_t now_ms, double cur_lead, double cur_bid,
                          double cur_ask, bool force) {
        if (!has_open) return;

        bool do_close = force;
        if (!do_close && params.hold_max_ms > 0 &&
            (now_ms - open_fill_ts) >= (int64_t)params.hold_max_ms) {
            do_close = true;
        }
        if (!do_close) {
            const double lag_mid = 0.5 * (cur_bid + cur_ask);
            do_close = std::fabs(cur_lead - lag_mid) <= params.hold_tol_pts;
        }
        if (!do_close) return;

        // exit at the far side of the spread (buy closes at bid, sell at ask)
        const double exit = (open_side == Side::Buy)
                    ? (cur_bid - params.hold_exit_slip_pts - params.hold_exit_markup_pts)
                    : (cur_ask + params.hold_exit_slip_pts + params.hold_exit_markup_pts);
        const double raw = (open_side == Side::Buy) ? (exit - open_price)
                                                     : (open_price - exit);
        const double pnl = raw - open_comm;

        BacktestResult::Trade tr;
        tr.signal_ts_ms = open_sig_ts;
        tr.fill_ts_ms   = now_ms;
        tr.side         = open_side;
        tr.signal_mark  = open_mark;
        tr.fill_price   = open_price;
        tr.rejected     = false;
        tr.reason       = nullptr;
        tr.pnl          = pnl;

        equity += pnl;
        if (pnl >= 0.0) gross_p += pnl; else gross_l += -pnl;
        out.fills++;
        record_trade(tr);

        // Return the lot so the strategy can re-open.
        strategy.on_order_closed(pnl, open_comm, "bt_close");
        has_open = false;
    };

    // Pending order reached its due time -> produce the entry fill.
    auto handle_fill = [&](int64_t now_ms) {
        if (!sim.pending()) return;
        if (now_ms < sim.target_fill_ts()) return;
        Fill f;
        if (!sim.at_fill(now_ms, lag_bid, lag_ask, f)) return;

        if (f.filled) {
            if (params.pnl_model == PnlModel::HoldConvergence) {
                open_position(f);
            } else {
                // Instant model: mark the round trip at the fill-time book,
                // which by construction always pays the spread.
                BacktestResult::Trade tr;
                tr.signal_ts_ms = f.signal_ts_ms;
                tr.fill_ts_ms   = f.fill_ts_ms;
                tr.side         = f.side;
                tr.signal_mark  = f.signal_mark;
                tr.fill_price   = f.fill_price;
                tr.rejected     = false;
                tr.reason       = nullptr;
                const double exit = (f.side == Side::Buy) ? f.exec_bid : f.exec_ask;
                const double raw  = (f.side == Side::Buy)
                                        ? (exit - f.fill_price)
                                        : (f.fill_price - exit);
                tr.pnl = raw - f.commission;
                out.fills++;
                equity += tr.pnl;
                if (tr.pnl >= 0.0) gross_p += tr.pnl; else gross_l += -tr.pnl;
                strategy.on_order_closed(tr.pnl, f.commission, "bt_close");
                record_trade(tr);
            }
        } else {
            out.rejections++;
        }
    };

    out.start_ts_ms = std::min(lead[0].ts_ms, lag[0].ts_ms);
    out.end_ts_ms   = std::max(lead[n_lead - 1].ts_ms, lag[n_lag - 1].ts_ms);

    while (i_lead < n_lead || i_lag < n_lag) {
        const bool lead_next = i_lead < n_lead &&
            (i_lag >= n_lag || lead[i_lead].ts_ms <= lag[i_lag].ts_ms);

        if (lead_next) {
            const BinaryTick& t = lead[i_lead++];
            if (!t.is_valid()) continue;
            lead_mid = t.mid();
            out.lead_ticks_consumed++;
        } else {
            const BinaryTick& t = lag[i_lag++];
            if (!t.is_valid()) continue;
            lag_bid = static_cast<double>(t.bid);
            lag_ask = static_cast<double>(t.ask);
            out.lag_ticks_consumed++;

            const int64_t now = t.ts_ms;
            handle_fill(now);

            // Let any held position ride the lag as it catches up to the lead.
            if (has_open || params.pnl_model != PnlModel::HoldConvergence)
                close_open(now, lead_mid, lag_bid, lag_ask, false);

            // Strategy evaluation on a fresh retail quote + a live lead.
            if (lead_mid > 0.0 && lag_bid > 0.0 && lag_ask > 0.0) {
                BrokerQuote q;
                q.ts_ms   = now;
                q.bid     = lag_bid;
                q.ask     = lag_ask;
                q.spread  = lag_ask - lag_bid;
                q.valid   = 1;

                double th = 0.0;
                const Decision d = strategy.evaluate(lead_mid, q, now, th);
                auto sig = strategy.maybe_emit(d, lead_mid, q, now);
                if (sig) {
                    const bool submitted = sim.submit(sig->side, now, lead_mid,
                                                      lag_bid, lag_ask);
                    if (submitted) {
                        out.signals_emitted++;
                        strategy.on_order_placed(sig->side, params.quantity);
                    }
                }
            }
            last_bid  = lag_bid;
            last_ask  = lag_ask;
            last_lead = lead_mid;
        }
    }
    // Drain any order still pending at end-of-stream.
    handle_fill(out.end_ts_ms + static_cast<int64_t>(delay_ms) + 1);
    // Force-close any held position at the last observed quote so the result
    // always accounts for the full position (max-hold / end-of-stream path).
    if (has_open) {
        if (last_bid <= 0.0 || last_ask <= 0.0) { last_bid = open_price; last_ask = open_price; }
        close_open(out.end_ts_ms + static_cast<int64_t>(delay_ms) + 2,
                   last_lead, last_bid, last_ask, true);
    }

    out.final_equity  = equity;
    out.total_pnl     = equity - out.initial_capital;
    out.gross_profit  = gross_p;
    out.gross_loss    = gross_l;
    out.profit_factor = gross_l > 0.0 ? gross_p / gross_l : (gross_p > 0.0 ? 1e9 : 0.0);
    out.sharpe_per_trade = sharpe_of(out.trades, out.fills);
    out.sharpe_annual    = out.sharpe_per_trade * std::sqrt(252.0);
    return out;
}

std::vector<BacktestEngine::DecayRow> BacktestEngine::latency_decay_matrix(
    const std::vector<int>& delays, const ExecutionParams& base_params) {
    std::vector<DecayRow> rows;
    rows.reserve(delays.size());
    for (int d : delays) {
        ExecutionParams p = base_params;
        p.delay_ms = d;
        const BacktestResult r = run_impl(p, d);
        DecayRow row;
        row.delay_ms   = d;
        row.net_pnl    = r.total_pnl;
        row.sharpe     = r.sharpe_per_trade;
        row.fills      = r.fills;
        row.rejections = r.rejections;
        rows.push_back(row);
    }
    return rows;
}

}  // namespace llm