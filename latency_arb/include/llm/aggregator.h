#pragma once

#include <cstdint>
#include <deque>
#include <optional>

#include "llm/config.h"
#include "llm/ring.h"
#include "llm/tick.h"

namespace llm {

// Composite Lead Price aggregator.
//
// Maintains a recent window of Deribit + Binance index ticks, drops ticks
// older than kMaxTickAgeMs, and produces a single weighted mid price.
//
// Weighting switches on US vs Asian trading hours:
//   US hours   : Deribit 60% / Binance 40%
//   Asian hours: Deribit 40% / Binance 60%
//
// This object is owned by the strategy thread (single consumer). Readers push
// into shared rings; the aggregator drains those rings on each poll.
class Aggregator {
public:
    explicit Aggregator(const Config& cfg);

    // Drain each input ring, ingest fresh ticks, return the weighted lead.
    std::optional<double> update(TickRing& binance, TickRing& deribit,
                                 int64_t now_ms);

private:
    // A single venue's recent valid ticks, ordered oldest->newest.
    struct VenueWindow {
        std::deque<Tick> ticks;
    };

    void ingest(TickRing& ring, VenueWindow& win, int64_t now_ms);
    void prune_old(VenueWindow& win, int64_t now_ms);

    // Latest valid mid for a venue within the window.
    double latest_mid(const VenueWindow& win) const;
    bool   in_us_hours(int64_t now_ms) const;
    // Weights at a point in time; sums to 1.0.
    void   weights(bool us, double& w_deribit, double& w_binance) const;
    double round4(double v) const;

    const Config& cfg_;
    VenueWindow binance_;
    VenueWindow deribit_;
};

}  // namespace llm