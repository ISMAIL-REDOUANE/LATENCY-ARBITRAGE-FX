#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/strategy.h"

namespace llm {

// Free monotonic ms since process start (hides platform time plumbing).
int64_t now_ms();

// Minimal synchronous FIX 4.4 client for execution.
//
//   * Single TCP session to the broker FIX gateway, auto-reconnecting.
//   * Logon 35=A (EncryptMethod=0, ResetSeqNum), Heartbeat 35=0 (35=0),
//     NewOrderSingle 35=D with TimeInForce=3 (IOC).
//   * Message bytes are spec-correct: 8=FIX.4.4, 9=BodyLength, body,
//     10=Checksum.
//
// Threading: a single session thread owns the socket and serializes send().
// Dispatchers enqueue orders through a mutex mailbox (non-blocking for the
// caller). TCP_NODELAY is set on the socket.
//
// NOTE: lean reference client — no resend-range persistence; wire format is
// complete and correct for IOC market orders.
class FixClient {
public:
    explicit FixClient(const Config& cfg);
    ~FixClient();

    FixClient(const FixClient&) = delete;
    FixClient& operator=(const FixClient&) = delete;

    // Non-blocking: enqueue an order for the session thread.
    void send_order(const Signal& s);

    void start();
    void stop();

    bool is_connected() const { return connected_.load(); }

private:
    struct OrderJob { Signal s; };

    void session_loop();
    void run_event_loop();
    bool connect_socket(const std::string& host, const std::string& port);
    void close_socket();
    bool send_frame(const std::string& frame);

    // FIX 4.4 message builders (wire-correct).
    static std::string build_fix(int msg_type, const std::string& body,
                                 const std::string& sender, const std::string& target,
                                 long seq);
    static std::string logon(long seq, const std::string& sender,
                             const std::string& target, int heartbeat_s);
    static std::string new_order_single(const Signal& s, const Config& cfg,
                                        long seq, const std::string& sender,
                                        const std::string& target);
    static std::string heartbeat(long seq, const std::string& sender,
                                 const std::string& target);

    const Config& cfg_;

    std::thread             thread_;
    std::atomic<bool>       running_{false};
    std::atomic<bool>       connected_{false};

    // Session-thread state.
    int     fd_ = -1;
    long    seq_out_ = 1;

    // Mailbox dispatcher -> session thread.
    std::mutex              mtx_;
    std::deque<OrderJob>    orders_;
    std::condition_variable cv_;
    bool                    wake_ = false;
};

}  // namespace llm