#include "llm/strategy.h"

#include <algorithm>
#include <cmath>

#include "llm/config.h"
#include "llm/telemetry.h"

namespace llm {

Strategy::Strategy(const Config& cfg) : cfg_(cfg) {}

double Strategy::round4(double v) const {
    if (!cfg_.round_four_decimals) return v;
    return std::round(v * 10000.0) / 10000.0;
}

// Dynamic_Threshold = Broker_Spread + Latency_Penalty_Buffer
//                     + Minimum_Profit_Margin
double Strategy::dynamic_threshold(const BrokerQuote& q) const {
    double t = q.spread;
    if (t <= 0.0) t = std::fabs(q.ask - q.bid);  // derive if not precomputed
    t += cfg_.latency_buffer_pips;
    t += cfg_.min_profit_margin_pips;
    // A configured absolute floor keeps tiny spreads from over-trading.
    t = std::max(t, cfg_.threshold_pips);
    return t;
}

Decision Strategy::evaluate(double lead, const BrokerQuote& q, int64_t /*now_ms*/,
                            double& out_threshold) {
    out_threshold = dynamic_threshold(q);

    lead = round4(lead);
    const double bid = round4(q.bid);
    const double ask = round4(q.ask);

    if (lead > ask + out_threshold) {
        Telemetry::instance().bench_mark(kStageStrategy);
        return Decision::Long;
    }
    if (bid > lead + out_threshold) {
        Telemetry::instance().bench_mark(kStageStrategy);
        return Decision::Short;
    }
    return Decision::Hold;
}

bool Strategy::interval_spent(int64_t now_ms) const {
    if (interval_start_ms_ == 0) return false;
    return (now_ms - interval_start_ms_) / 1000 >= cfg_.interval_seconds;
}

bool Strategy::can_trade_now(int64_t now_ms) const {
    if (open_lots_ >= cfg_.max_open_lots) return false;
    if (day_pnl_ <= -cfg_.max_daily_loss) return false;
    if (interval_spent(now_ms)) return true;  // fresh window below
    return orders_this_interval_ < cfg_.max_orders_per_interval;
}

int Strategy::mrpc_open_lots() const { return open_lots_; }

void Strategy::on_order_placed(Side, double) {
    ++open_lots_;
    if (interval_start_ms_ == 0) interval_start_ms_ = 0;
    ++orders_this_interval_;
}

void Strategy::on_order_closed(double realized_pnl, double commissions,
                               const std::string&) {
    day_pnl_ += realized_pnl - commissions;
    if (open_lots_ > 0) --open_lots_;
}

std::optional<Signal> Strategy::maybe_emit(Decision d, double lead,
                                           const BrokerQuote& q,
                                           int64_t now_ms) {
    if (d == Decision::Hold) return std::nullopt;
    if (!can_trade_now(now_ms)) return std::nullopt;

    // One signal per interval window to avoid spamming orders.
    if (last_signal_ms_ != 0 &&
        (now_ms - last_signal_ms_) < cfg_.interval_seconds * 1000LL) {
        return std::nullopt;
    }

    double th = 0.0;
    (void)evaluate(lead, q, now_ms, th);

    Signal s;
    s.side      = (d == Decision::Long) ? Side::Buy : Side::Sell;
    s.lead      = lead;
    s.broker_bid= q.bid;
    s.broker_ask= q.ask;
    s.threshold = th;
    s.edge      = (d == Decision::Long)
                      ? (round4(lead) - round4(q.ask) - th)
                      : (round4(q.bid) - round4(lead) - th);
    s.ts_ms     = now_ms;
    s.symbol    = cfg_.dry_run_symbol;
    s.reason    = (d == Decision::Long) ? "lead_gt_ask" : "bid_gt_lead";

    last_signal_ms_ = now_ms;
    return s;
}

}  // namespace llm