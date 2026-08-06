#pragma once

#include <cstdint>
#include <cstring>

namespace llm {

// Source venues.
enum class Venue : uint8_t { Unknown = 0, Binance, Deribit, Broker };

// One aggregated market event. Fixed-layout, trivially copyable so it can
// travel through lock-free ring buffers and ZeroMQ without serialization
// overhead on the hot path.
struct Tick {
    // Monotonic-ish timestamp (epoch ms from steady origin), used for
    // staleness checks. Wall-clock arrival ns appended by the reader when the
    // OS clock is available.
    int64_t  ts_ms       = 0;
    int64_t  ts_ns       = 0;
    double   bid         = 0.0;   // best bid  (index feeds: last price)
    double   ask         = 0.0;   // best ask  (index feeds: last price)
    double   last        = 0.0;   // last trade price
    Venue    venue       = Venue::Unknown;
    uint32_t seq         = 0;     // reader-local monotonic sequence
    uint8_t  valid       = 0;

    bool is_valid() const { return valid != 0; }
    void invalidate() { valid = 0; }

    // Mid price for index-like sources.
    double mid() const { return (bid > 0.0 && ask > 0.0) ? (bid + ask) * 0.5 : last; }
};

// Broker quote (OmsBroker-style) parsed from the ZMQ stream.
struct BrokerQuote {
    int64_t  ts_ms   = 0;
    double   bid     = 0.0;
    double   ask     = 0.0;
    double   spread  = 0.0;   // precomputed by OmsBroker (pips or points)
    int64_t  latency_us = 0; // broker-side latency hint, if published
    uint8_t  valid   = 0;
    uint8_t  session = 0;    // 0 = closed, 1 = open (from session field)

    bool is_valid() const { return valid != 0 && bid > 0.0 && ask > 0.0; }
};

}  // namespace llm
