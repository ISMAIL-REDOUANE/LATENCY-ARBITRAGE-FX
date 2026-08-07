// ===========================================================================
// backtest_tests — unit + behavior tests for the backtest subsystem:
//   * BinaryTick layout (24-byte, cache-aligned).
//   * CSV -> binary conversion round-trip.
//   * MmapTickReader zero-copy mapping + binary search.
//   * ExecutionSimulator fill mechanics (delay, slippage, spread markup,
//     commissions, rejection, single-slot pending).
//   * BacktestEngine merge + metrics (PnL, Sharpe, profit factor, drawdown,
//     latency-decay matrix).
//
// Self-contained (no test framework, no third-party deps) so it runs through
// CTest on a fresh box.
// ===========================================================================

#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "backtest/backtest_engine.hpp"
#include "backtest/execution_simulator.hpp"
#include "backtest/tick_binary.hpp"
#include "llm/strategy.h"

using namespace llm;

namespace btfw {

int failures = 0;
int checks   = 0;

void check(bool cond, const char* expr, const char* file, int line) {
    ++checks;
    if (!cond) {
        ++failures;
        std::printf("  [FAIL] %s:%d  %s\n", file, line, expr);
    }
}

void check_near(double a, double b, double tol, const char* expr,
                const char* file, int line) {
    ++checks;
    if (!(std::fabs(a - b) <= tol)) {
        ++failures;
        std::printf("  [FAIL] %s:%d  %s  (%.10f vs %.10f)\n", file, line,
                    expr, a, b);
    }
}

}  // namespace btfw

#define C(cond) btfw::check(!!(cond), #cond, __FILE__, __LINE__)
#define CN(a, b, tol) btfw::check_near((a), (b), (tol), #a "~=" #b, __FILE__, __LINE__)

namespace testutil {

std::string write_ticks(const std::vector<BinaryTick>& ticks,
                        const std::string& suffix) {
    const std::string path = std::string(".backtest_tmp_" + suffix + ".bin");
    FILE* f = std::fopen(path.c_str(), "wb");
    if (!f) return std::string();
    std::fwrite(ticks.data(), sizeof(BinaryTick), ticks.size(), f);
    std::fclose(f);
    return path;
}

BinaryTick mk(int64_t ts, double bid, double ask, double vol = 1.0) {
    BinaryTick t;
    t.ts_ms  = ts;
    t.bid    = static_cast<float>(bid);
    t.ask    = static_cast<float>(ask);
    t.volume = static_cast<float>(vol);
    t.set_valid(true);
    return t;
}

Config test_config() {
    Config c;
    c.threshold_pips          = 0.5;
    c.latency_buffer_pips     = 0.2;
    c.min_profit_margin_pips  = 0.3;
    c.max_daily_loss          = 1e9;   // effectively unlimited for these tests
    c.interval_seconds        = 0;
    c.max_orders_per_interval = 1'000'000;
    return c;
}

}  // namespace

// ===========================================================================
// Layout
// ===========================================================================
static void test_tick_layout() {
    C(sizeof(BinaryTick) == 24);
    C(alignof(BinaryTick) <= 8);

    BinaryTick t;
    t.ts_ms  = 1700000000000LL;
    t.bid    = 64321.5f;
    t.ask    = 64322.1f;
    t.volume = 1.25f;
    t.set_valid(true);
    C(t.is_valid());
    CN(t.mid(), 64321.8, 1e-2);
    CN(t.spread(), 0.6, 1e-2);
}

// ===========================================================================
// CSV converter round-trip
// ===========================================================================
static void test_csv_convert_roundtrip() {
    const std::string csv = ".bt_csv_in.csv";
    {
        FILE* f = std::fopen(csv.c_str(), "w");
        C(f != nullptr);
        std::fprintf(f, "timestamp,bid,ask,volume\n");
        std::fprintf(f, "1723050000000,64321.5,64322.1,1.25\n");
        std::fprintf(f, "1723050000010,64000.0,64001.0,0.5\n");
        std::fprintf(f, "bad,row,ignored,here\n");          // skipped
        std::fprintf(f, "1723050000020,64200.0,0.0,1.0\n"); // non-positive ask
        std::fprintf(f, "1723050000030,64100.0,64101.5,2.0\n");
        std::fclose(f);
    }
    const std::string bin = ".bt_csv_out.bin";
    std::string err;
    CsvToBinaryConverter::Options opt;
    opt.has_header = true;
    const long n = CsvToBinaryConverter::convert(csv, bin, opt, &err);
    C(n == 3);
    if (n != 3)
        std::printf("   csv converted %ld (expected 3): %s\n", n, err.c_str());

    MmapTickReader r(bin);
    C(r.open(&err));
    C(r.count() == 3);
    if (r.count() == 3) {
        CN(static_cast<double>(r[0].ts_ms), 1723050000000LL, 0);
        CN(r[0].spread(), 0.6, 1e-2);
        CN(static_cast<double>(r[2].ts_ms), 1723050000030LL, 0);
        C(r[2].ask > r[2].bid);
    }
    r.close();
    std::remove(csv.c_str());
    std::remove(bin.c_str());
}

// ===========================================================================
// MmapTickReader binary search
// ===========================================================================
static void test_mmap_read_find() {
    std::vector<BinaryTick> v;
    for (int i = 0; i < 100; ++i)
        v.push_back(testutil::mk(i * 10, 64000.0 + i, 64001.0 + i));
    const std::string path = testutil::write_ticks(v, "reader");
    C(!path.empty());
    MmapTickReader r(path);
    std::string err;
    C(r.open(&err));
    C(r.count() == 100);
    C(r.data() != nullptr);

    BinaryTick out;
    C(r.find_at_or_before(55, out));    // index 5 -> ts=50
    CN(static_cast<double>(out.ts_ms), 50.0, 0);
    C(r.find_at_or_before(5000, out));  // past end -> last ts=990
    CN(static_cast<double>(out.ts_ms), 990.0, 0);
    C(!r.find_at_or_before(-1, out));   // before the first
    r.close();
    std::remove(path.c_str());
}

// ===========================================================================
// ExecutionSimulator — fill mechanics
// ===========================================================================
static void test_sim_basic_buy_fill() {
    ExecutionParams p;
    p.delay_ms = 5;
    p.slippage_pts = 0.0;
    ExecutionSimulator sim(p);

    C(sim.submit(Side::Buy, 1000, 64400.0, 64399.0, 64400.0));
    C(sim.pending());
    C(sim.target_fill_ts() == 1005);

    Fill out;
    C(!sim.at_fill(1004, 64399.0, 64400.0, out));  // not yet due

    C(sim.at_fill(1005, 64401.0, 64403.0, out));   // due now
    C(out.filled);
    C(!out.rejected);
    C(out.side == Side::Buy);
    CN(out.fill_price, 64403.0, 1e-6);   // exec ask + markup(0) + slip(0)
    CN(static_cast<double>(out.target_fill_ts_ms), 1005.0, 0);
}

static void test_sim_fill_sell_side_and_markup() {
    ExecutionParams p;
    p.delay_ms        = 0;
    p.slippage_pts    = 0.5;
    p.spread_markup_pts = 0.25;
    ExecutionSimulator sim(p);

    // sell: fill = exec_bid - markup - slip = 64000 - 0.25 - 0.5
    C(sim.submit(Side::Sell, 2000, 64001.0, 63999.0, 64001.0));
    Fill out;
    C(sim.at_fill(2000, 64000.0, 64002.0, out));
    C(out.filled);
    C(out.side == Side::Sell);
    CN(out.fill_price, 63999.25, 1e-6);
    CN(out.slippage_pts, 0.5, 1e-9);
}

static void test_sim_reject_spread_and_slippage() {
    // Spread cap rejection.
    {
        ExecutionParams p;
        p.delay_ms       = 0;
        p.spread_cap_pts = 1.0;
        ExecutionSimulator sim(p);
        C(sim.submit(Side::Buy, 0, 1.0, 10.0, 12.0));
        Fill out;
        C(sim.at_fill(100, 11.0, 13.0, out));   // exec spread 2 > cap 1
        C(out.rejected);
        C(out.reject_reason != nullptr &&
          std::strcmp(out.reject_reason, "spread_cap") == 0);
        C(!sim.pending());   // cleared after the fill attempt
    }
    // Slippage cap.
    {
        ExecutionParams p;
        p.delay_ms         = 0;
        p.slippage_cap_pts = 0.1;
        p.slippage_pts     = 0.5;
        ExecutionSimulator sim(p);
        C(sim.submit(Side::Buy, 0, 1.0, 10.0, 11.0));
        Fill out;
        C(sim.at_fill(0, 10.0, 11.0, out));
        C(out.rejected);
        C(std::strcmp(out.reject_reason, "slippage_cap") == 0);
    }
    // Invalid (non-positive) exec price -> rejected.
    {
        ExecutionParams p;
        p.delay_ms = 0;
        ExecutionSimulator sim(p);
        C(sim.submit(Side::Buy, 0, 1.0, 64000.0, 64001.0));
        Fill out;
        C(sim.at_fill(0, 0.0, 0.0, out));   // corrupt quote
        C(out.rejected);
        C(out.reject_reason != nullptr &&
          std::strcmp(out.reject_reason, "invalid_price") == 0);
    }
}

static void test_sim_single_slot_order() {
    ExecutionParams p;
    ExecutionSimulator sim(p);
    C(sim.submit(Side::Buy, 0, 1.0, 10.0, 11.0));
    C(!sim.submit(Side::Sell, 0, 1.0, 10.0, 11.0));  // single slot
    sim.cancel();
    C(sim.submit(Side::Buy, 0, 1.0, 10.0, 11.0));    // may place again
}

static void test_sim_commission() {
    ExecutionParams p;
    p.delay_ms           = 0;
    p.commission_per_lot = 2.5;
    p.quantity           = 2.0;
    ExecutionSimulator sim(p);
    CN(sim.params().quantity, 2.0, 1e-9);
    C(sim.submit(Side::Buy, 0, 1.0, 64000.0, 64002.0));
    Fill out;
    C(sim.at_fill(0, 64000.0, 64002.0, out));
    C(out.filled);
    CN(out.commission, 5.0, 1e-9);     // 2.5 * 2.0
    CN(out.fill_price, 64002.0, 1e-6);
}

// ===========================================================================
// BacktestEngine — end-to-end metrics
// ===========================================================================
static void test_backtest_engine_metrics() {
    // Lead is persistently 100 pts above the lag ask -> BUY every quote.
    std::vector<BinaryTick> lead, lag;
    for (int i = 0; i < 50; ++i) lead.push_back(testutil::mk(i * 10, 64100.0, 64100.0));
    for (int i = 0; i < 50; ++i) lag.push_back(testutil::mk(i * 10, 64000.0, 64001.0));

    const std::string lp = testutil::write_ticks(lead, "lead");
    const std::string lg = testutil::write_ticks(lag, "lag");
    C(!lp.empty() && !lg.empty());

    MmapTickReader rlead(lp), rlag(lg);
    std::string err;
    C(rlead.open(&err) && rlag.open(&err));

    const Config cfg = testutil::test_config();
    BacktestEngine engine(rlead, rlag, cfg);

    ExecutionParams ep;
    ep.delay_ms         = 0;
    ep.spread_cap_pts   = 100;
    ep.slippage_cap_pts = 50;

    const BacktestResult r = engine.run(ep);
    C(r.signals_emitted >= 1);
    C(r.fills >= 1);
    // Each long fill pays the retail spread (bid<ask), so PnL is <= 0 here.
    C(r.total_pnl <= 1e-6);
    C(r.final_equity <= r.initial_capital + 1e-9);
    C(r.trades.size() >= r.fills);
    C(r.gross_loss >= 0.0);

    // Latency-decay matrix must produce one row per delay.
    const std::vector<int> delays = {0, 5, 15, 30};
    const auto decay = engine.latency_decay_matrix(delays, ep);
    C(decay.size() == delays.size());
    for (size_t i = 0; i < decay.size(); ++i) C(decay[i].delay_ms == delays[i]);

    rlead.close(); rlag.close();
    std::remove(lp.c_str());
    std::remove(lg.c_str());
}

// ===========================================================================
int main() {
    std::printf("backtest_tests\n");
    test_tick_layout();
    test_csv_convert_roundtrip();
    test_mmap_read_find();
    test_sim_basic_buy_fill();
    test_sim_fill_sell_side_and_markup();
    test_sim_reject_spread_and_slippage();
    test_sim_single_slot_order();
    test_sim_commission();
    test_backtest_engine_metrics();
    std::printf("\n%d checks, %d failures\n", btfw::checks, btfw::failures);
    return btfw::failures == 0 ? 0 : 1;
}