#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "llm/config.h"
#include "llm/tick.h"

namespace llm {

// Side a lead-lag signal can take.
enum class Side : uint8_t { Buy, Sell, None };

// A tradeable recommendation, produced by the strategy thread.
struct Signal {
    Side       side;
    double     lead;             // composite lead price at decision time
    double     broker_bid;       // broker quote side that triggered
    double     broker_ask;
    double     threshold;        // actual dynamic threshold used
    double     edge;             // |lead vs broker| minus threshold (pts)
    int64_t    ts_ms;
    std::string symbol;
    std::string reason;
};

// Decision mode returned by Strategy::evaluate.
enum class Decision : uint8_t {
    Long,      // Lead - Broker_Ask > Threshold   -> buy
    Short,     // Broker_Bid - Lead > Threshold   -> sell
    Hold       // neither; below threshold
};

// Latency-arbitrage strategy.
//
// Rule (from spec):
//   long  if  Lead - Broker_Ask > Dynamic_Threshold
//   short if  Broker_Bid - Lead   > Dynamic_Threshold
//   Dynamic_Threshold = Broker_Spread + Latency_Penalty_Buffer
//                       + Minimum_Profit_Margin
//
// Rounding: doubles rounded to 4 decimals at scope start (FOUR_DECIMAL_ROUNDING)
// before any scope-base differences.
//
// Single-threaded; owned by the strategy/math worker.
class Strategy {
public:
    explicit Strategy(const Config& cfg);

    // Evaluate one broker quote against the current lead price.
    Decision evaluate(double lead, const BrokerQuote& q, int64_t now_ms,
                      double& out_threshold);

    // Dynamic_Threshold = Broker_Spread + Latency_Penalty_Buffer
    //                     + Minimum_Profit_Margin, floored at threshold_pips.
    // Public so unit tests can assert the exact offset math.
    double dynamic_threshold(const BrokerQuote& q) const;

    // Returns signals only when a side change passes risk gates + cooldown.
    std::optional<Signal> maybe_emit(Decision d, double lead,
                                     const BrokerQuote& q,
                                     int64_t now_ms);

    // ---- risk state ----------------------------------------------------- //
    void on_order_placed(Side side, double notional);
    void on_order_closed(double realized_pnl, double commissions,
                         const std::string& outcome);
    bool can_trade_now(int64_t now_ms) const;
    int  mrpc_open_lots() const;

private:
    double round4(double v) const;
    bool   interval_spent(int64_t now_ms) const;

    const Config& cfg_;

    // Risk state (strategy thread only).
    int        open_lots_ = 0;
    double     day_pnl_   = 0.0;
    int        orders_this_interval_ = 0;
    int64_t    interval_start_ms_    = 0;
    int64_t    last_signal_ms_       = 0;
};

}  // namespace llm