#pragma once

#include <atomic>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "llm/tick.h"

namespace llm {

// Multi-threaded ZeroMQ PUB publisher.
//
// Design:
//   * Producers NEVER touch ZeroMQ. They enqueue into a mutex-guarded mailbox
//     and return immediately (non-blocking on the hot path).
//   * A single dedicated pub thread owns the zmq context + PUB socket and
//     drains the mailbox in batches, amortizing send() syscalls.
//   * TCP_NODELAY + send/recv high-water-marks are applied on the socket.
//   * Exactly one context I/O thread to keep scheduler/CPU pressure minimal.
class ZmqPub {
public:
    enum class Payload : uint8_t { Tick, Broker, Signal };

    explicit ZmqPub(std::string bind);
    ~ZmqPub();

    ZmqPub(const ZmqPub&) = delete;
    ZmqPub& operator=(const ZmqPub&) = delete;

    // Non-blocking producers — safe from any thread, never block on I/O.
    void publish(const Tick& tick);
    void publish(const BrokerQuote& q);
    void publish_signal(const std::string& json);

    // Start the pub thread. Idempotent.
    void start();
    // Signal stop + join. Safe from any thread.
    void stop();

private:
    struct Msg {
        Payload      kind;
        Tick         tick;
        BrokerQuote  quote;
        std::string  signal;
    };

    void thread_loop();
    void drain(std::vector<Msg>& out);

    std::string bind_;
    std::thread thread_;

    std::mutex            mtx_;
    std::deque<Msg>       queue_;
    std::atomic<bool>     running_{false};
    std::atomic<bool>     shutdown_{false};
};

}  // namespace llm