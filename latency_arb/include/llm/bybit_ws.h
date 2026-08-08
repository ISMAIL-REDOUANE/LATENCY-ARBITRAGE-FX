#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/ring.h"
#include "llm/tick.h"

namespace llm {

// Bybit v5 public spot WebSocket feed (best-bid/ask orderbook).
//
//   Stream: wss://stream.bybit.com/v5/public/spot
//   Subscribe: {"op":"subscribe","args":["orderbook.1.<sym>"]}
//   Payload (snapshot): data.b[0]=[bid,size], data.a[0]=[ask,size]
//
// Runs its own io_context + ssl::context on a dedicated thread. Every parsed
// best bid/ask is pushed (non-blocking, drop-oldest) into the SPSC ring handed
// to it. Auto-reconnects with exponential backoff on a timer on the same io.
class BybitWs {
public:
    explicit BybitWs(const Config& cfg, TickRing& out);
    ~BybitWs();

    void start();
    void stop();

private:
    class Session;

    void run();
    // Parse one frame; returns a non-empty string when the server expects a
    // reply (e.g. keep-alive pong).
    std::string handle_message(const std::string& msg);
    void schedule_reconnect(double delay_s);
    void spawn_session();

    const Config& cfg_;
    TickRing&     out_;

    std::thread   thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    double        reconnect_delay_ms_ = 2000.0;

    // Owned by the reader thread while running; used to respawn sessions.
    void* io_ = nullptr;        // net::io_context* (opaque; avoids Beast in header)
    void* ssl_ctx_ = nullptr;   // ssl::context*
    std::shared_ptr<struct Session> session_;
};

}  // namespace llm