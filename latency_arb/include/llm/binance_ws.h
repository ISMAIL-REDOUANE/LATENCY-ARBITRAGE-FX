#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/ring.h"
#include "llm/tick.h"

namespace llm {

// Binance bookTicker WebSocket feed.
//
//   Stream: wss://stream.binance.com:9443/ws/<sym>@bookTicker
//   Payload: {"e":"bookTicker","s":"BTCUSDT","b":"best bid","a":"best ask",...}
//
// Runs its own io_context + ssl::context on a dedicated thread. Every parsed
// bookTicker is pushed (non-blocking, drop-oldest) into the SPSC ring handed
// to it. Auto-reconnects with exponential backoff on a timer on the same io.
class BinanceWs {
public:
    explicit BinanceWs(const Config& cfg, TickRing& out);
    ~BinanceWs();

    void start();
    void stop();

private:
    class Session;

    void run();
    void handle_message(const std::string& msg);
    void schedule_reconnect(double delay_s);
    void spawn_session();

    const Config& cfg_;
    TickRing&     out_;

    std::thread   thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    double        reconnect_delay_s_ = 2.0;

    // Owned by the reader thread while running; used to respawn sessions.
    void* io_ = nullptr;        // net::io_context* (opaque; avoids Beast in header)
    void* ssl_ctx_ = nullptr;   // ssl::context*
    std::shared_ptr<struct Session> session_;
};

}  // namespace llm