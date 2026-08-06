#include "llm/execution.h"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <utility>

#include "llm/config.h"
#include "llm/telemetry.h"
#include "llm/wire.h"

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
    return llm::within_slippage_cap(s);
}

void ExecutionDispatcher::publish_signal(const Signal& s) {
    char buf[320];
    const int n = encode_signal(s, buf, sizeof(buf));
    if (n < 0) return;  // buffer too small — drop, never truncate a signal
    pub_.publish_signal(buf);
    Telemetry::instance().bench_mark(kStageSignalTx);
    Telemetry::instance().log(
        std::string("\"signal\":{\"side\":\"") +
        (s.side == Side::Buy ? "BUY" : "SELL") + "\",\"lead\":" +
        std::to_string(s.lead) + ",\"edge\":" + std::to_string(s.edge) + "}");
}

}  // namespace llm