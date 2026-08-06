#pragma once

#include <atomic>
#include <deque>
#include <mutex>
#include <thread>

#include "llm/config.h"
#include "llm/fix_client.h"
#include "llm/strategy.h"
#include "llm/zmq_pub.h"

namespace llm {

// Execution dispatcher.
//
// Consumes signals produced by the strategy thread, applies final risk checks,
// then routes to:
//   1. The ZeroMQ pub socket (consumed by the Python MT5 bridge / EA).
//   2. The FIX 4.4 client socket (direct broker execution) when enabled.
//
// Non-blocking for callers: signals are enqueued and a worker thread performs
// the actual publication / FIX send. Max slippage caps are enforced here as a
// final gate (pips for broker venue, USD for BTC index).
class ExecutionDispatcher {
public:
    ExecutionDispatcher(const Config& cfg, ZmqPub& pub, FixClient& fix);
    ~ExecutionDispatcher();

    void start();
    void stop();

    // Non-blocking enqueue of a validated signal.
    void submit(const Signal& s);

private:
    void worker_loop();
    bool within_slippage_cap(const Signal& s) const;
    void publish_signal(const Signal& s);

    const Config& cfg_;
    ZmqPub&       pub_;
    FixClient&    fix_;

    std::thread   thread_;
    std::mutex    mtx_;
    std::deque<Signal> queue_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
};

}  // namespace llm