#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/ring.h"
#include "llm/tick.h"

namespace llm {

// Deribit price-index WebSocket feed.
//
//   Stream: wss://www.deribit.com/ws/api/v2
//   Request (JSON-RPC):
//     {"jsonrpc":"2.0","method":"public/subscribe","id":<n>,
//      "params":{"channels":["deribit_price_index.btc_usd"]}}
//   Push:  {"jsonrpc":"2.0","method":"subscription",
//           "params":{"channel":"deribit_price_index.btc_usd","data":{"price":...}}}
//
// Runs its own io_context + ssl::context on a dedicated thread, pushing each
// parsed index into the SPSC ring (non-blocking, drop-oldest). The subscribe
// id is generated once per connection (user does not supply stream ids).
class DeribitWs {
public:
    explicit DeribitWs(const Config& cfg, TickRing& out);
    ~DeribitWs();

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

    void* io_ = nullptr;        // net::io_context*
    void* ssl_ctx_ = nullptr;   // ssl::context*
    std::shared_ptr<struct Session> session_;
};

}  // namespace llm