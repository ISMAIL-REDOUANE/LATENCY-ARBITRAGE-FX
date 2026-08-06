#include "llm/aggregator.h"

#include <cmath>
#include <cstdio>
#include <ctime>

#include "llm/telemetry.h"

namespace llm {

Aggregator::Aggregator(const Config& cfg) : cfg_(cfg) {}

void Aggregator::ingest(TickRing& ring, VenueWindow& win, int64_t now_ms) {
    Tick t;
    while (ring.pop(t)) {
        if (!t.is_valid()) continue;
        Telemetry::instance().bench_mark(kStageTickRx);
        if (t.ts_ms > 0 && now_ms - t.ts_ms > CompileTime::kMaxTickAgeMs) {
            continue;  // stale — drop per spec
        }
        win.ticks.push_back(t);
    }
    prune_old(win, now_ms);
}

void Aggregator::prune_old(VenueWindow& win, int64_t now_ms) {
    while (!win.ticks.empty() &&
           win.ticks.front().ts_ms > 0 &&
           now_ms - win.ticks.front().ts_ms > CompileTime::kMaxTickAgeMs) {
        win.ticks.pop_front();
    }
}

double Aggregator::latest_mid(const VenueWindow& win) const {
    if (win.ticks.empty()) return 0.0;
    return win.ticks.back().mid();
}

std::optional<double> Aggregator::update(TickRing& binance, TickRing& deribit,
                                         int64_t now_ms) {
    ingest(binance, binance_, now_ms);
    ingest(deribit, deribit_, now_ms);

    const double b = latest_mid(binance_);
    const double d = latest_mid(deribit_);
    if (b <= 0.0 && d <= 0.0) return std::nullopt;

    // If one venue has no fresh data, fall back to the other (weight 1.0).
    if (b <= 0.0) return d;
    if (d <= 0.0) return b;

    const bool us = in_us_hours(now_ms);
    double wd = 0.0, wb = 0.0;
    weights(us, wd, wb);

    double lead = d * wd + b * wb;
    if (cfg_.round_four_decimals) lead = round4(lead);

    static long last_log = 0;
    if (now_ms - last_log > 5000) {
        last_log = now_ms;
        char buf[200];
        std::snprintf(buf, sizeof(buf),
                      "\"lead\":{\"binance\":%.2f,\"deribit\":%.2f,"
                      "\"lead\":%.2f,\"session\":\"%s\","
                      "\"w_deribit\":%.2f,\"w_binance\":%.2f}",
                      b, d, lead, us ? "us" : "asia", wd, wb);
        Telemetry::instance().log(buf);
    }
    Telemetry::instance().bench_mark(kStageAggregate);
    return lead;
}

double Aggregator::round4(double v) const {
    return std::round(v * 10000.0) / 10000.0;
}

bool Aggregator::in_us_hours(int64_t now_ms) const {
    const std::time_t t = static_cast<std::time_t>(now_ms / 1000);
    std::tm tmv{};
#ifdef _WIN32
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    int hour = tmv.tm_hour;
    int minute = tmv.tm_min;
    const int us_open  = cfg_.us_open_hour_utc;
    const int us_close = cfg_.us_close_hour_utc;
    const int now_min  = hour * 60 + minute;
    return now_min >= us_open * 60 && now_min < us_close * 60;
}

void Aggregator::weights(bool us, double& w_deribit, double& w_binance) const {
    if (us) {
        w_deribit = cfg_.deribit_weight_us;
        w_binance = cfg_.binance_weight_us;
    } else {
        w_deribit = cfg_.deribit_weight_asia;
        w_binance = cfg_.binance_weight_asia;
    }
    // Normalize in case the operator configured non-summing weights.
    const double s = w_deribit + w_binance;
    if (s > 0.0) { w_deribit /= s; w_binance /= s; }
}

}  // namespace llm