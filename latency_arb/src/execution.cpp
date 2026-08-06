#include "llm/execution.h"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <utility>

#include "llm/config.h"
#include "llm/telemetry.h"

namespace llm {

ExecutionDispatcher::ExecutionDispatcher(const Config& cfg, ZmqPub& pub,
                                         FixClient& fix)
    : cfg_(cfg), pub_(pub), fix_(fix) {}
ExecutionDispatcher::~ExecutionDispatcher() { stop(); }

void ExecutionDispatcher::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&ExecutionDispatcher::worker_loop, this);
}

void ExecutionDispatcher::stop() {
    stopped_.store(true);
    { std::lock_guard<std::mutex> lk(mtx_); /* wake */ }
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

void ExecutionDispatcher::submit(const Signal& s) {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        queue_.push_back(s);
    }
}

void ExecutionDispatcher::worker_loop() {
    Telemetry::instance().log("\"exec\":{\"thread_started\":true}");
    while (running_.load() && !stopped_.load()) {
        Signal s;
        bool have = false;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            if (!queue_.empty()) { s = std::move(queue_.front()); queue_.pop_front(); have = true; }
        }
        if (!have) {
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            continue;
        }
        if (!within_slippage_cap(s)) {
            Telemetry::instance().log_warn(
                std::string("\"exec\":{\"rejected\":\"slippage_cap\",\"edge\":") +
                std::to_string(s.edge) + "}");
            continue;
        }
        publish_signal(s);
        if (cfg_.fix_enabled) fix_.send_order(s);
    }
    Telemetry::instance().log("\"exec\":{\"thread_stopped\":true}");
}

bool ExecutionDispatcher::within_slippage_cap(const Signal& s) const {
    // Broker-venue cap in pips; BTC-index cap in USD. The edge was computed in
    // strategy terms of the same unit as the threshold (broker pips). We cap
    // the deviation of the broker quote against lead accordingly.
    const double cap = CompileTime::kMaxSlippageBroker;
    return std::fabs(s.edge) <= cap || s.edge > 0.0;
}

void ExecutionDispatcher::publish_signal(const Signal& s) {
    char buf[320];
    std::snprintf(buf, sizeof(buf),
                  "signal|%s,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%lld",
                  s.side == Side::Buy ? "BUY" : "SELL",
                  s.reason.c_str(), s.lead, s.broker_bid, s.broker_ask,
                  s.threshold, s.edge,
                  static_cast<long long>(s.ts_ms));
    pub_.publish_signal(buf);
    Telemetry::instance().log(
        std::string("\"signal\":{\"side\":\"") +
        (s.side == Side::Buy ? "BUY" : "SELL") + "\",\"lead\":" +
        std::to_string(s.lead) + ",\"edge\":" + std::to_string(s.edge) + "}");
}

}  // namespace llm