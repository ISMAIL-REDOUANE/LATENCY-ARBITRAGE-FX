// ---------------------------------------------------------------------------
// latency_tests — self-contained C++20 unit + benchmark suite.
//
// Covers, per the engine spec:
//   1. SpscRing lock-free correctness (FIFO, drop-oldest, no lost frames when
//      the backlog fits, SPSC concurrent producer/consumer integrity).
//   2. PriceAggregator composite lead (US vs Asia session weighting).
//   3. StrategyEngine Long / Short / Hold triggers.
//   4. Dynamic spread threshold offsets (spread + buffer + margin, floor).
//   5. Max-slippage bounds on the execution gate.
//   6. Zero-allocation wire signal serialization (encode/decode round-trip,
//      malformed-frame rejection, and a malloc-countered hot loop).
//
// No external test framework and no third-party library: this file compiles
// against the header-only core plus the pure sources (config/strategy/
// aggregator/telemetry/wire), so `ctest` works even before libzmq/Boost/
// OpenSSL are installed.
// ---------------------------------------------------------------------------

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <optional>
#include <thread>
#include <vector>

#if defined(_WIN32)
#include <malloc.h>
#endif

#include "llm/aggregator.h"
#include "llm/config.h"
#include "llm/mt5_bridge.h"
#include "llm/ring.h"
#include "llm/strategy.h"
#include "llm/telemetry.h"
#include "llm/tick.h"
#include "llm/wire.h"

using namespace llm;

// ===========================================================================
// Minimal test framework
// ===========================================================================
namespace testfw {

struct TestCase {
    const char* name;
    void (*fn)();
};

std::vector<TestCase>& registry() {
    static std::vector<TestCase> r;
    return r;
}

struct Registrar {
    Registrar(const char* name, void (*fn)()) { registry().push_back({name, fn}); }
};

int failures = 0;
int checks   = 0;

void require(bool cond, const char* expr, const char* file, int line) {
    ++checks;
    if (!cond) {
        ++failures;
        std::printf("  [FAIL] %s:%d  %s\n", file, line, expr);
    }
}

void require_near(double a, double b, double tol, const char* expr,
                  const char* file, int line) {
    ++checks;
    if (!(std::fabs(a - b) <= tol)) {
        ++failures;
        std::printf("  [FAIL] %s:%d  %s  (%.10f vs %.10f)\n", file, line,
                    expr, a, b);
    }
}

}  // namespace testfw

#define TEST_CASE(name)                                                  \
    static void test_##name();                                           \
    static testfw::Registrar testfw_reg_##name(#name, &test_##name);     \
    static void test_##name()

#define REQUIRE(cond) testfw::require(!!(cond), #cond, __FILE__, __LINE__)
#define REQUIRE_NEAR(a, b, tol) \
    testfw::require_near((a), (b), (tol), #a " ~= " #b, __FILE__, __LINE__)

// ===========================================================================
// Global allocation counter — used only inside the zero-alloc wire test.
// ===========================================================================
namespace {
std::atomic<long long> g_alloc_count{0};
std::atomic<bool>      g_track{false};
}  // namespace

void* operator new(std::size_t n) {
    if (g_track.load(std::memory_order_relaxed))
        g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    if (void* p = std::malloc(n ? n : 1)) return p;
    throw std::bad_alloc();
}
void operator delete(void* p) noexcept { std::free(p); }
void* operator new[](std::size_t n) { return ::operator new(n); }
void operator delete[](void* p) noexcept { ::operator delete(p); }
void operator delete(void* p, std::size_t) noexcept { ::operator delete(p); }
void operator delete[](void* p, std::size_t) noexcept { ::operator delete[](p); }

void* operator new(std::size_t n, std::align_val_t al) {
    if (g_track.load(std::memory_order_relaxed))
        g_alloc_count.fetch_add(1, std::memory_order_relaxed);
    void* p = nullptr;
#if defined(_WIN32)
    p = _aligned_malloc(n ? n : 1, static_cast<std::size_t>(al));
    if (!p) throw std::bad_alloc();
#else
    if (posix_memalign(&p, static_cast<std::size_t>(al), n ? n : 1) != 0)
        throw std::bad_alloc();
#endif
    return p;
}
void operator delete(void* p, std::align_val_t) noexcept {
#if defined(_WIN32)
    _aligned_free(p);
#else
    std::free(p);
#endif
}
void* operator new[](std::size_t n, std::align_val_t al) {
    return ::operator new(n, al);
}
void operator delete[](void* p, std::align_val_t al) noexcept {
    ::operator delete(p, al);
}

// ===========================================================================
// Helpers
// ===========================================================================
namespace {

Tick make_tick(double last, int64_t ts_ms, Venue venue) {
    Tick t;
    t.last = last;
    t.ts_ms = ts_ms;
    t.venue = venue;
    t.valid = 1;
    return t;
}

Config test_cfg() {
    Config cfg;
    cfg.round_four_decimals = true;
    cfg.threshold_pips = 0.5;
    cfg.latency_buffer_pips = 0.2;
    cfg.min_profit_margin_pips = 0.3;
    return cfg;
}

}  // namespace

// ===========================================================================
// 1. SpscRing
// ===========================================================================
TEST_CASE(ring_basic_fifo) {
    TickRing ring;
    REQUIRE(ring.empty());

    Tick t = make_tick(100.25, 1234, Venue::Binance);
    t.bid = 100.0;
    t.ask = 100.5;

    REQUIRE(ring.push(t));
    REQUIRE(!ring.empty());
    Tick out;
    REQUIRE(ring.pop(out));
    REQUIRE(out.bid == 100.0 && out.venue == Venue::Binance);
    REQUIRE(ring.empty());
    REQUIRE(!ring.pop(out));
}

TEST_CASE(ring_fifo_order_no_drops) {
    // Backlog below capacity => every push must succeed and order preserved.
    const std::size_t n = TickRing::kCapacity - 4;
    TickRing ring;
    for (std::size_t i = 0; i < n; ++i)
        REQUIRE(ring.push(make_tick(static_cast<double>(i),
                                    static_cast<int64_t>(i), Venue::Deribit)));
    REQUIRE(ring.size() == n);
    Tick out;
    for (std::size_t i = 0; i < n; ++i) {
        REQUIRE(ring.pop(out));
        REQUIRE(out.last == static_cast<double>(i));
    }
    REQUIRE(ring.empty());
}

TEST_CASE(ring_drop_oldest_matches_reference) {
    // Drop-oldest ring semantics: the writer never blocks; once the read
    // pointer is lapped, only the newest `pushed % capacity` frames survive,
    // and they come out in strict FIFO order with no gaps. Assert the exact
    // survivor stream against that invariant.
    const std::size_t over = TickRing::kCapacity + 37;
    const std::size_t expect = over % TickRing::kCapacity;  // survivors
    REQUIRE(expect > 0);

    TickRing ring;
    bool any_drop = false;
    for (std::size_t i = 0; i < over; ++i) {
        if (!ring.push(make_tick(static_cast<double>(i),
                                 static_cast<int64_t>(i), Venue::Binance)))
            any_drop = true;
    }
    REQUIRE(any_drop);   // overflow actually happened

    std::vector<double> got;
    Tick out;
    while (ring.pop(out)) got.push_back(out.last);

    REQUIRE(got.size() == expect);
    for (std::size_t i = 0; i < expect; ++i)
        REQUIRE(got[i] == static_cast<double>(over - expect + i));
    REQUIRE(ring.empty());
}

TEST_CASE(ring_spsc_concurrent_no_loss) {
    // Concurrent SPSC producer/consumer; backlog fits capacity so no frame is
    // dropped — count and running sum must both be exact.
    const long TOT = static_cast<long>(TickRing::kCapacity) - 4;
    TickRing shared;
    std::atomic<long> count{0}, sum{0};

    std::thread producer([&] {
        for (long i = 0; i < TOT; ++i)
            shared.push(make_tick(static_cast<double>(i), i, Venue::Deribit));
    });
    std::thread consumer([&] {
        Tick x;
        while (count.load() < TOT)
            if (shared.pop(x)) {
                sum.fetch_add(static_cast<long>(x.last));
                count.fetch_add(1);
            }
    });
    producer.join();
    consumer.join();

    const long long expected = static_cast<long long>(TOT) * (TOT - 1) / 2;
    REQUIRE(count.load() == TOT);
    REQUIRE(static_cast<long long>(sum.load()) == expected);
}

TEST_CASE(ring_capacity_is_power_of_two) {
    static_assert((TickRing::kCapacity & (TickRing::kCapacity - 1)) == 0,
                  "TickRing capacity must be a power of two");
    static_assert(TickRing::kCapacity == 8192,
                  "TickRing capacity must be 8192");
    REQUIRE((TickRing::kCapacity & (TickRing::kCapacity - 1)) == 0);
    REQUIRE(TickRing::kCapacity == 8192);
}

// ===========================================================================
// 2. PriceAggregator composite
// ===========================================================================
TEST_CASE(aggregator_us_session_weighting) {
    // Force "US hours" (open 00:00, close 24:00) => always in session.
    Config cfg = test_cfg();
    cfg.us_open_hour_utc = 0;
    cfg.us_close_hour_utc = 24;
    Aggregator agg(cfg);

    const int64_t now = 1'700'000'000'000;
    TickRing bin, der;
    bin.push(make_tick(63800.0, now - 1, Venue::Binance));
    der.push(make_tick(64000.0, now - 1, Venue::Deribit));

    // US: 0.6 * deribit + 0.4 * binance = 0.6*64000 + 0.4*63800 = 63920
    auto lead = agg.update(bin, der, now);
    REQUIRE(lead.has_value());
    REQUIRE_NEAR(*lead, 63920.0, 1e-6);
}

TEST_CASE(aggregator_asia_session_weighting) {
    // Force "Asia hours" (open 24:00, close 00:00) => never in session.
    Config cfg = test_cfg();
    cfg.us_open_hour_utc = 24;
    cfg.us_close_hour_utc = 0;
    Aggregator agg(cfg);

    const int64_t now = 1'700'000'000'000;
    TickRing bin, der;
    bin.push(make_tick(63800.0, now - 1, Venue::Binance));
    der.push(make_tick(64000.0, now - 1, Venue::Deribit));

    // Asia: 0.4 * deribit + 0.6 * binance = 0.4*64000 + 0.6*63800 = 63880
    auto lead = agg.update(bin, der, now);
    REQUIRE(lead.has_value());
    REQUIRE_NEAR(*lead, 63880.0, 1e-6);
}

TEST_CASE(aggregator_stale_ticks_dropped) {
    Config cfg = test_cfg();
    Aggregator agg(cfg);

    const int64_t now = 1'700'000'000'000;
    TickRing bin, der;
    // Older than kMaxTickAgeMs (50ms) => must be pruned on ingest.
    bin.push(make_tick(63800.0, now - 200, Venue::Binance));
    der.push(make_tick(64000.0, now - 200, Venue::Deribit));

    auto lead = agg.update(bin, der, now);
    REQUIRE(!lead.has_value());
}

TEST_CASE(aggregator_venue_fallback) {
    // One venue silent => fall back to the other's latest mid.
    const int64_t now = 1'700'000'000'000;

    // Deribit only.
    {
        Config cfg = test_cfg();
        Aggregator agg(cfg);
        TickRing bin, der;
        der.push(make_tick(64000.0, now - 1, Venue::Deribit));
        auto lead = agg.update(bin, der, now);
        REQUIRE(lead.has_value());
        REQUIRE_NEAR(*lead, 64000.0, 1e-9);
    }
    // Binance only.
    {
        Config cfg = test_cfg();
        Aggregator agg(cfg);
        TickRing bin, der;
        bin.push(make_tick(63800.0, now - 1, Venue::Binance));
        auto lead = agg.update(bin, der, now);
        REQUIRE(lead.has_value());
        REQUIRE_NEAR(*lead, 63800.0, 1e-9);
    }
    // Neither has data.
    {
        Config cfg = test_cfg();
        Aggregator agg(cfg);
        TickRing bin, der;
        auto lead = agg.update(bin, der, now);
        REQUIRE(!lead.has_value());
    }
}

// ===========================================================================
// 3. StrategyEngine triggers
// ===========================================================================
static BrokerQuote quote(double bid, double ask, double spread) {
    BrokerQuote q;
    q.bid = bid;
    q.ask = ask;
    q.spread = spread;
    q.valid = 1;
    return q;
}

TEST_CASE(strategy_long_trigger) {
    Strategy st(test_cfg());
    BrokerQuote q = quote(99.9, 100.0, 0.1);  // threshold = 0.1+0.2+0.3 = 0.6
    double th = 0.0;

    REQUIRE(st.evaluate(100.7, q, 0, th) == Decision::Long);   // +0.7 > 0.6
    REQUIRE_NEAR(th, 0.6, 1e-9);
    REQUIRE(st.evaluate(100.6, q, 0, th) == Decision::Hold);   // +0.6 not >
    REQUIRE(st.evaluate(100.61, q, 0, th) == Decision::Long);  // +0.61 > 0.6
}

TEST_CASE(strategy_short_trigger) {
    Strategy st(test_cfg());
    BrokerQuote q = quote(100.0, 100.1, 0.1);  // threshold = 0.6
    double th = 0.0;

    REQUIRE(st.evaluate(99.3, q, 0, th) == Decision::Short);   // 100.0-99.3=0.7 > 0.6
    REQUIRE_NEAR(th, 0.6, 1e-9);
    REQUIRE(st.evaluate(99.4, q, 0, th) == Decision::Hold);    // 0.6 not >
    REQUIRE(st.evaluate(99.39, q, 0, th) == Decision::Short);
}

TEST_CASE(strategy_hold_no_signal) {
    Strategy st(test_cfg());
    BrokerQuote q = quote(99.9, 100.0, 0.1);
    double th = 0.0;
    REQUIRE(st.evaluate(100.0, q, 0, th) == Decision::Hold);
    REQUIRE(st.maybe_emit(Decision::Hold, 100.0, q, 0) == std::nullopt);
}

TEST_CASE(strategy_rounding_flips_decision) {
    // lead just above ask+threshold, but 4-decimal rounding pulls it back
    // under the line. threshold=0.6, ask=100.0 => line=100.6.
    Config cfg = test_cfg();
    cfg.round_four_decimals = true;
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.1);
    double th = 0.0;

    // 100.600001 rounds to 100.6 (not strictly > 100.6) => Hold.
    REQUIRE(st.evaluate(100.600001, q, 0, th) == Decision::Hold);
    // 100.700001 rounds to 100.7 => Long.
    REQUIRE(st.evaluate(100.700001, q, 0, th) == Decision::Long);

    // With rounding disabled the same price stays above the line => Long.
    Config cfg2 = test_cfg();
    cfg2.round_four_decimals = false;
    Strategy st2(cfg2);
    REQUIRE(st2.evaluate(100.600001, q, 0, th) == Decision::Long);
}

// ===========================================================================
// 4. Dynamic spread threshold offsets
// ===========================================================================
TEST_CASE(threshold_is_spread_plus_buffers) {
    Strategy st(test_cfg());  // buffer=0.2, margin=0.3
    BrokerQuote q;
    double th = 0.0;

    q = quote(99.95, 100.05, 0.10);
    (void)st.evaluate(100.0, q, 0, th);
    REQUIRE_NEAR(th, 0.10 + 0.2 + 0.3, 1e-9);   // 0.6

    q = quote(99.90, 100.10, 0.20);
    (void)st.evaluate(100.0, q, 0, th);
    REQUIRE_NEAR(th, 0.20 + 0.2 + 0.3, 1e-9);   // 0.7

    q = quote(99.70, 100.30, 0.60);
    (void)st.evaluate(100.0, q, 0, th);
    REQUIRE_NEAR(th, 0.60 + 0.2 + 0.3, 1e-9);   // 1.1
}

TEST_CASE(threshold_floor_threshold_pips) {
    // Tiny spread => raw sum below THRESHOLD_PIPS floor => floor wins.
    Config cfg = test_cfg();
    cfg.threshold_pips = 2.0;
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.001);  // 0.001+0.2+0.3 = 0.501 < 2.0
    double th = 0.0;
    (void)st.evaluate(100.0, q, 0, th);
    REQUIRE_NEAR(th, 2.0, 1e-9);
}

TEST_CASE(threshold_derived_spread_no_double_count) {
    // spread field missing (<=0) => derive from |ask-bid| ONCE. Regression
    // guard: an earlier implementation added it a second time.
    Strategy st(test_cfg());
    BrokerQuote q = quote(100.0, 100.1, 0.0);   // derived spread = 0.1
    double th = 0.0;

    REQUIRE_NEAR(st.dynamic_threshold(q), 0.6, 1e-9);
    // 100.75 - (100.1 + 0.6) = +0.05 > 0 => Long. With a double count the
    // threshold would be 0.7 and this would wrongly hold.
    REQUIRE(st.evaluate(100.75, q, 0, th) == Decision::Long);
    REQUIRE_NEAR(th, 0.6, 1e-9);
}

// ===========================================================================
// Risk gates + signal emission
// ===========================================================================
TEST_CASE(strategy_max_open_lots_blocks) {
    Config cfg = test_cfg();
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.1);

    REQUIRE(st.can_trade_now(1000));
    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 1000).has_value());

    st.on_order_placed(Side::Buy, 0.1);
    REQUIRE(!st.can_trade_now(2000));                        // open_lots == max
    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 2000) == std::nullopt);

    st.on_order_closed(50.0, 0.5, "tp");
    REQUIRE(st.can_trade_now(3000));                         // freed
}

TEST_CASE(strategy_daily_loss_halt) {
    Config cfg = test_cfg();
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.1);

    st.on_order_placed(Side::Sell, 0.1);
    st.on_order_closed(-600.0, 0.0, "sl");                   // -600 < -500 cap
    REQUIRE(!st.can_trade_now(1000));
    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 1000) == std::nullopt);
}

TEST_CASE(strategy_cooldown_gate) {
    Config cfg = test_cfg();
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.1);

    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 1000).has_value());
    // Same interval => suppressed.
    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 1001) == std::nullopt);
    // After interval_seconds (default 60s) => allowed again.
    REQUIRE(st.maybe_emit(Decision::Long, 100.7, q, 1000 + 60'000).has_value());
}

TEST_CASE(signal_fields_populated) {
    Config cfg = test_cfg();
    Strategy st(cfg);
    BrokerQuote q = quote(99.9, 100.0, 0.1);

    auto sig = st.maybe_emit(Decision::Long, 100.7, q, 42);
    REQUIRE(sig.has_value());
    REQUIRE(sig->side == Side::Buy);
    REQUIRE(sig->lead == 100.7);
    REQUIRE(sig->broker_ask == 100.0);
    REQUIRE_NEAR(sig->threshold, 0.6, 1e-9);
    REQUIRE_NEAR(sig->edge, 100.7 - 100.0 - 0.6, 1e-9);  // +0.1
    REQUIRE(sig->ts_ms == 42);
    REQUIRE(sig->reason == "lead_gt_ask");
}

// ===========================================================================
// 5. Max slippage bounds
// ===========================================================================
TEST_CASE(slippage_positive_edge_always_passes) {
    Signal s;
    s.edge = 3.0;      // far above 2-pip cap but profitable => pass
    REQUIRE(within_slippage_cap(s));
    s.edge = 0.5;
    REQUIRE(within_slippage_cap(s));
    s.edge = 0.0;
    REQUIRE(within_slippage_cap(s));
}

TEST_CASE(slippage_negative_edge_capped) {
    Signal s;
    s.edge = -1.0;     // within 2-pip cap
    REQUIRE(within_slippage_cap(s));
    s.edge = -2.0;     // at the cap boundary (|edge| <= cap) => pass
    REQUIRE(within_slippage_cap(s));
    s.edge = -2.5;     // worse than cap and negative => reject
    REQUIRE(!within_slippage_cap(s));
    s.edge = -10.0;
    REQUIRE(!within_slippage_cap(s));
}

// ===========================================================================
// 6. Zero-allocation wire signal serialization
// ===========================================================================
TEST_CASE(wire_encode_exact_bytes) {
    Signal s;
    s.side = Side::Buy;
    s.reason = "lead_gt_ask";
    s.lead = 64000.123456;
    s.broker_bid = 63999.0;
    s.broker_ask = 64000.0;
    s.threshold = 0.6;
    s.edge = 0.123456;
    s.ts_ms = 1'700'000'000'123;

    char buf[256];
    const int n = encode_signal(s, buf, sizeof(buf));
    REQUIRE(n > 0);
    REQUIRE(std::strlen(buf) == static_cast<std::size_t>(n));

    char expected[256];
    const int en = std::snprintf(
        expected, sizeof(expected),
        "signal|BUY,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%lld",
        s.reason.c_str(), s.lead, s.broker_bid, s.broker_ask, s.threshold,
        s.edge, static_cast<long long>(s.ts_ms));
    REQUIRE(n == en);
    REQUIRE(std::strcmp(buf, expected) == 0);
}

TEST_CASE(wire_encode_rejects_tiny_buffer) {
    Signal s;
    s.side = Side::Sell;
    s.reason = "bid_gt_lead";
    s.lead = 64000.0;
    s.broker_bid = 64000.5;
    s.broker_ask = 64001.0;
    s.threshold = 0.6;
    s.edge = 0.1;
    s.ts_ms = 1;

    char small[4];
    REQUIRE(encode_signal(s, small, sizeof(small)) == -1);
    char one[1];
    REQUIRE(encode_signal(s, one, sizeof(one)) == -1);
    REQUIRE(encode_signal(s, nullptr, 0) == -1);
}

TEST_CASE(wire_decode_roundtrip) {
    Signal s;
    s.side = Side::Sell;
    s.reason = "bid_gt_lead";
    s.lead = 63990.5;
    s.broker_bid = 64000.0;
    s.broker_ask = 64001.0;
    s.threshold = 0.6;
    s.edge = 0.123456;
    s.ts_ms = 1'700'000'000'999;

    char buf[256];
    const int n = encode_signal(s, buf, sizeof(buf));
    REQUIRE(n > 0);

    ExecSignal ex;
    REQUIRE(decode_signal(buf, static_cast<std::size_t>(n), ex));
    REQUIRE(ex.valid == 1);
    REQUIRE(ex.side == 2);                                    // SELL
    REQUIRE(std::strcmp(ex.reason, "bid_gt_lead") == 0);
    REQUIRE_NEAR(ex.lead, 63990.5, 1e-6);
    REQUIRE_NEAR(ex.broker_bid, 64000.0, 1e-6);
    REQUIRE_NEAR(ex.broker_ask, 64001.0, 1e-6);
    REQUIRE_NEAR(ex.threshold, 0.6, 1e-6);
    REQUIRE_NEAR(ex.edge, 0.123456, 1e-6);
    REQUIRE(ex.ts_ms == 1'700'000'000'999);
    REQUIRE(std::strcmp(ex.symbol, "BTC/USD") == 0);
}

TEST_CASE(wire_decode_rejects_malformed) {
    ExecSignal out;
    REQUIRE(!decode_signal("", 0, out));                       // empty
    REQUIRE(!decode_signal("signal|HOLD,a,1,2,3,4,5,6", 24, out));  // bad side
    REQUIRE(!decode_signal("signal|BUY", 9, out));             // truncated
    REQUIRE(!decode_signal("signal|BUY,reason,1,2,3,4,5", 25, out));  // no ts
    REQUIRE(!decode_signal("signal|SELL,,0,0,0,0,0,0", 25, out));     // lead 0
    REQUIRE(!decode_signal("garbage,no,commas", 17, out));
    REQUIRE(!decode_signal("signal|BUY,reason,1,2,3,4,5,6", 25, out));
}

TEST_CASE(wire_encode_zero_allocation) {
    Signal s;
    s.side = Side::Buy;
    s.reason = "lead_gt_ask";
    s.lead = 64000.123456;
    s.broker_bid = 63999.0;
    s.broker_ask = 64000.0;
    s.threshold = 0.6;
    s.edge = 0.123456;
    s.ts_ms = 1'700'000'000'123;

    char buf[256];
    REQUIRE(encode_signal(s, buf, sizeof(buf)) > 0);  // warm-up

    g_alloc_count.store(0);
    g_track.store(true);
    volatile int sink = 0;
    for (int i = 0; i < 1'000'000; ++i)
        sink += encode_signal(s, buf, sizeof(buf));
    g_track.store(false);
    (void)sink;

    REQUIRE(g_alloc_count.load() == 0);   // no heap activity on the hot path
}

// ===========================================================================
// Benchmarks (informational; failures only if wildly wrong)
// ===========================================================================
TEST_CASE(bench_ring_throughput) {
    constexpr long N = 4'000'000;
    TickRing ring;
    Tick t = make_tick(1.0, 0, Venue::Binance);
    Tick out;

    auto t0 = std::chrono::steady_clock::now();
    for (long i = 0; i < N; ++i) { ring.push(t); ring.pop(out); }
    auto t1 = std::chrono::steady_clock::now();

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / N;
    std::printf("  ring push+pop: %8.2f ns/op\n", ns);
    REQUIRE(ns > 0.0 && ns < 1000.0);   // sanity bound only
}

TEST_CASE(bench_wire_encode) {
    Signal s;
    s.side = Side::Buy;
    s.reason = "lead_gt_ask";
    s.lead = 64000.123456;
    s.broker_bid = 63999.0;
    s.broker_ask = 64000.0;
    s.threshold = 0.6;
    s.edge = 0.123456;
    s.ts_ms = 1;

    char buf[256];
    REQUIRE(encode_signal(s, buf, sizeof(buf)) > 0);  // warm-up
    constexpr long N = 2'000'000;
    auto t0 = std::chrono::steady_clock::now();
    volatile int sink = 0;
    for (long i = 0; i < N; ++i) sink += encode_signal(s, buf, sizeof(buf));
    auto t1 = std::chrono::steady_clock::now();
    (void)sink;

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count() / N;
    std::printf("  wire encode:  %8.2f ns/op\n", ns);
    // Sanity bound only — NOT a strict performance gate. A loaded CPU (or
    // frequency scaling) can legitimately push encode above 10us; the limit is
    // widened to 50us so the suite stays deterministic under normal load
    // variation while still catching a real order-of-magnitude regression.
    REQUIRE(ns > 0.0 && ns < 50000.0);
}

// ===========================================================================
// Nanosecond benchmark telemetry (lock-free latency ring)
// ===========================================================================
TEST_CASE(bench_telemetry_ring) {
    Telemetry& tel = Telemetry::instance();
    REQUIRE(tel.enable_benchmark_ring(1u << 16));  // 64 KiB ring
    REQUIRE(tel.bench_enabled());

    // Stamp a mix of stages so we know the pipeline ids are recorded.
    constexpr long N = 100'000;
    auto t0 = std::chrono::steady_clock::now();
    for (long i = 0; i < N; ++i) {
        tel.bench_mark(kStageTickRx);
        tel.bench_mark(kStageAggregate);
        tel.bench_mark(kStageStrategy);
        tel.bench_mark(kStageSignalTx);
        tel.bench_mark(kStageExecSend);
    }
    auto t1 = std::chrono::steady_clock::now();

    const double ns = std::chrono::duration<double, std::nano>(t1 - t0).count()
                      / (5.0 * N);
    std::printf("  bench_mark:   %8.2f ns/op\n", ns);

    const auto st = tel.bench_stats();
    REQUIRE(st.samples > 0);
    REQUIRE(st.tsc_hz > 0.0);            // calibration ran
    REQUIRE(st.last_seq > st.first_seq); // sequence advanced
    // Inter-sample deltas are wall-clock nanoseconds: must be sane.
    REQUIRE(st.avg_dt_ns >= 0.0);
    REQUIRE(st.min_dt_ns >= 0.0);
    REQUIRE(st.max_dt_ns >= 0.0);
    REQUIRE(st.avg_dt_ns <= st.max_dt_ns);
    REQUIRE(st.min_dt_ns <= st.p50_dt_ns);
    REQUIRE(st.p50_dt_ns <= st.p99_dt_ns);
    REQUIRE(st.p99_dt_ns <= st.max_dt_ns);
    REQUIRE(ns > 0.0 && ns < 2000.0);   // ~sub-µs per stamp on the hot path
}

TEST_CASE(bench_telemetry_zero_alloc) {
    Telemetry& tel = Telemetry::instance();

    // Clear counters, then hammer bench_mark under the allocation tracker.
    g_track.store(true);
    g_alloc_count.store(0);
    constexpr long N = 1'000'000;
    for (long i = 0; i < N; ++i) tel.bench_mark(kStageTickRx);
    g_track.store(false);

    REQUIRE(g_alloc_count.load() == 0);  // no heap activity on the hot path
}

TEST_CASE(bench_telemetry_rdtsc_monotonic) {
    // rdtsc must be monotonic within a single thread.
    uint64_t prev = 0;
    long bad = 0;
    for (long i = 0; i < 1'000'000; ++i) {
        const uint64_t now = rdtsc();
        if (now < prev) ++bad;
        prev = now;
    }
    REQUIRE(bad == 0);

    // steady_ns also monotonic and ns-resolution.
    int64_t p = 0;
    long bad2 = 0;
    for (long i = 0; i < 1'000'000; ++i) {
        const int64_t now = steady_ns();
        if (now < p) ++bad2;
        p = now;
    }
    REQUIRE(bad2 == 0);
}

TEST_CASE(bench_telemetry_multithread_spmc) {
    // Concurrent producers must never corrupt the ring or lose monotonicity
    // of the per-writer sequence; stats remain consistent afterward.
    Telemetry& tel = Telemetry::instance();
    constexpr int kThreads = 4;
    constexpr long kPerThread = 100'000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&tel, t] {
            for (long i = 0; i < kPerThread; ++i)
                tel.bench_mark(kStageExecSend + static_cast<uint32_t>(t % 3));
        });
    }
    for (auto& th : threads) th.join();

    const auto st = tel.bench_stats();
    REQUIRE(st.samples > 0);
    REQUIRE(st.last_seq >= st.first_seq);
}

// ===========================================================================
int main() {
    std::printf("latency_tests: %zu test cases\n", testfw::registry().size());
    for (auto& tc : testfw::registry()) {
        const int before = testfw::failures;
        std::printf("[ RUN  ] %s\n", tc.name);
        tc.fn();
        const bool ok = (testfw::failures == before);
        std::printf("[ %s ] %s\n", ok ? "  OK " : "FAIL ", tc.name);
    }
    std::printf("\n%zu tests, %d checks, %d failures\n",
                testfw::registry().size(), testfw::checks, testfw::failures);
    return testfw::failures == 0 ? 0 : 1;
}
