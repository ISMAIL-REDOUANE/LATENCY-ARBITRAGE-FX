#pragma once

// =============================================================================
// CTraderExecutor — asynchronous market-order execution against the cTrader
// Open API (IC Markets) over Protobuf + TLS.
//
// Replaces the OANDA REST executor (and avoids MT5/Wine entirely): the cTrader
// Open API is a protobuf-over-TCP protocol which is lighter-faster than REST
// and can run headless on plain Linux.
//
//   * Endpoint (demo): openapi.ctrader.com:5030   (TLS)
//   * Endpoint (live): openapi.ctrader.com:5025   (TLS)
//   * Wire            : [ 4-byte big-endian length ][ serialized ProtoMessage ]
//   * Auth            : ProtoOAApplicationAuthReq (clientId + clientSecret)
//                       then ProtoOAAccountAuthReq (ctidTraderAccountId + token)
//   * Orders          : ProtoOANewOrderReq         (MARKET w/ relative SL/TP)
//   * Realtime price  : ProtoOASubscribeSpotsReq subscription caches last bid/ask.
//
// The protobuf schemas are the official cTrader Open API messages vendored in
// third_party/openapi/ and compiled at build time by protoc (see CMakeLists).
//
// Non-blocking contract: `execute()` only *enqueues* an order and returns
// immediately; the protobuf exchange happens on a dedicated worker thread, so
// the Binance WebSocket read loop is NEVER blocked by broker I/O.
//
// Threading: a mutex + FIFO queue serializes orders; the worker thread is the
// SOLE owner of the persistent TLS stream (all access inside worker_loop).
//
// Persistent connection: the TLS stream + auth are established once in
// connect() and REUSED across orders (no per-trade handshake/auth). On a
// transport failure the connection is re-established (including re-auth) and
// the order is retried once.
// ===========================================================================

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>

#include "llm/config.h"

namespace llm {

// Result of a single order attempt, produced on the worker thread and surfaced
// through the result callback.
struct CTraderExecutionResult {
    bool          ok         = false;   // order accepted/executed
    std::string   status;               // "ACCEPTED" / "FILLED" / "REJECTED"
    int64_t       order_id   = 0;       // cTrader order id
    int64_t       position_id = 0;
    double        latency_ms = 0.0;     // RTT request -> EXECUTION_EVENT
    std::string   error;                // human-readable failure text
};

class CTraderExecutor {
public:
    explicit CTraderExecutor(const Config& cfg);
    ~CTraderExecutor();

    CTraderExecutor(const CTraderExecutor&) = delete;
    CTraderExecutor& operator=(const CTraderExecutor&) = delete;

    void start();
    void stop();

    // Non-blocking: enqueue a market order and return immediately.
    //   * symbol     — cTrader symbol name (e.g. "XAU/USD" or "BTC/USD").
    //   * side       — "buy" or "sell".
    //   * volume     — volume expressed in LOTS (multiplied by the symbol's
    //                  real lot size before serialization).
    //   * stoploss_pips / takeprofit_pips — pip distance for the relative
    //                  Stop Loss / Take Profit (converted to broker points).
    void execute(const std::string& symbol, const std::string& side,
                 double volume, double stoploss_pips, double takeprofit_pips);

    // Max queue depth before execute() drops an order (back-pressure guard).
    void set_queue_limit(std::size_t limit) { queue_limit_ = limit; }

    // Optional callback (invoked on the worker thread) to receive results.
    void set_result_callback(void (*cb)(const CTraderExecutionResult&, void*),
                             void* userdata);

private:
    struct OrderJob {
        std::string symbol;
        std::string side;
        double      volume;         // lots
        double      stoploss_pips;
        double      takeprofit_pips;
    };
    struct SymbolInfo {
        int64_t symbol_id = 0;
        int32_t digits = 0;
        int32_t pip_position = 1;   // digits to the right of a pip
        int64_t lot_size = 1000000; // 1 lot, in proto volume units (0.01 unit)
        bool    valid = false;
    };

    void worker_loop();
    CTraderExecutionResult send_market_order(const OrderJob& job);

    // Persistent connection management (worker thread only).
    bool connect();          // TLS + app/account auth + subscribe spots
    void disconnect();       // close TLS + clear state
    bool connected() const;

    // --- protobuf / wire plumbing -------------------------------------------//
    // Raw TLS framed write of a serialized ProtoMessage.
    bool   send_frame(const std::string& payload);
    // Read one frame (4-byte length + payload), returns the raw ProtoMessage bytes or "".
    std::string read_frame(bool& ok);
    // Round-trip: deliver one typed request, then read frames until the payload
    // type matches `expect` (dispatching spot/heartbeat/error events on the way).
    std::string exchange(int32_t payload_type, const std::string& body,
                         int32_t expect_type, std::string* error);

    // True if a ProtoOAxErrorRes / ProtoOAExecutionEvent error was seen.
    bool dispatch_other(const std::string& raw, int32_t type, std::string* error);

    // Returns true after application+account auth succeeded.
    bool authenticate(std::string* error);
    bool resolve_symbol(const std::string& symbol, SymbolInfo* out, std::string* error);
    bool subscribe_spot(std::string* error);

    // Encode a market order + await its EXECUTION_EVENT.
    CTraderExecutionResult place_order(const OrderJob& job, bool* transport_failed);

    const Config& cfg_;

    boost::asio::io_context                           io_;
    boost::asio::ssl::context                          ssl_{boost::asio::ssl::context::tlsv12_client};
    std::unique_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> stream_;

    std::string host_ = "openapi.ctrader.com";
    std::string port_ = "5030";
    std::atomic<bool> connected_{false};

    int64_t account_id_ = 0;      // parsed from cfg (ctidTraderAccountId)
    SymbolInfo syminfo_;          // resolved once per (re)connect

    std::atomic<double> spot_bid_{0.0};
    std::atomic<double> spot_ask_{0.0};

    std::thread             thread_;
    std::atomic<bool>       running_{false};
    std::atomic<bool>       stopped_{false};

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::deque<OrderJob>    jobs_;
    bool                    wake_ = false;
    std::size_t             queue_limit_ = 256;

    void (*cb_)(const CTraderExecutionResult&, void*) = nullptr;
    void* cb_data_ = nullptr;
    int   next_id_ = 0;
};

}  // namespace llm