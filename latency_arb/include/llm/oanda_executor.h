#pragma once

// ===========================================================================
// OandaExecutor — asynchronous market-order execution against the OANDA v20
// REST API using Boost.Beast + Boost.Asio.
//
// Replaces the broker/FIX execution path: OANDA requires no heavy SDK / MT5
// terminal, just a plain HTTPS POST per order.
//
//   * Endpoint (demo): https://api-fxpractice.oanda.com/v3/accounts/
//                      {OANDA_ACCOUNT_ID}/orders
//   * Endpoint (live): https://api-fxtrade.oanda.com/...  (same path, host
//                      switch via OANDA_HOST)
//   * Auth           : Authorization: Bearer {OANDA_TOKEN}
//   * Payload        : OANDA v20 MarketOrderRequest with stopLoss / takeProfit
//                      declared as a *distance* (OrderDistance) in pips so the
//                      broker converts then, not us.
//
// Non-blocking contract: `execute()` only *enqueues* an order and returns
// immediately; the HTTPS POST is performed on a dedicated worker std::thread,
// so the Binance WebSocket read loop is NEVER blocked by network I/O. Latency is
// measured with std::chrono::high_resolution_clock from just before the request
// body is issued until the HTTP response headers arrive, and logged in ms.
//
// Threading: a mutex + FIFO queue serializes sends; the worker thread is the
// SOLE owner of the persistence boost::beast::ssl::stream, so there is no data
// race on it (all access happens inside worker_loop -> send_market_order).
//
// Persistent connection: the TLS stream is opened once in connect() and
// REUSED across orders, eliminating the per-trade TCP connect + TLS handshake
// cost. If a request fails (network error / server close), the stream is torn
// down, the connection is re-established, and the order is retried once.
// ===========================================================================

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>

#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include "llm/config.h"

namespace llm {

// Result of a single order attempt, produced on the worker thread and surfaced
// through a logging sink (see on_result / the .cpp default).
struct OandaExecutionResult {
    bool          ok          = false;   // HTTP 2xx
    int           http_status = 0;
    int           order_id    = 0;       // caller-visible serial
    double        latency_ms  = 0.0;     // RTT to HTTP response headers
    std::string   transaction_id;        // OANDA order 'id' on acceptance
    std::string   error;                 // human-readable failure text
};

class OandaExecutor {
public:
    explicit OandaExecutor(const Config& cfg);
    ~OandaExecutor();

    OandaExecutor(const OandaExecutor&) = delete;
    OandaExecutor& operator=(const OandaExecutor&) = delete;

    void start();
    void stop();

    // Non-blocking: enqueue a market order and return immediately.
    //   * instrument    — OANDA instrument name (e.g. "XAU_USD").
    //                       BTC/USD is NOT natively traded on OANDA; map venue
    //                       symbols to the OANDA instrument before calling.
    //   * side          — "buy" or "sell".
    //   * units         — signed units: +ve = long, -ve = short. If you pass
    //                       lots, multiply by instrument size yourself.
    //   * stoploss_pips — distance (pips) for the stop-loss OrderDistance.
    //   * takeprofit_pips — distance (pips) for the take-profit OrderDistance.
    void execute(const std::string& instrument, const std::string& side,
                 double units, double stoploss_pips, double takeprofit_pips);

    // Max queue depth before execute() drops an order (back-pressure guard).
    void set_queue_limit(size_t limit) { queue_limit_ = limit; }

    // Optional callback (invoked on the worker thread) to receive results.
    void set_result_callback(void (*cb)(const OandaExecutionResult&, void*),
                             void* userdata);

private:
    struct OrderJob {
        std::string instrument;
        std::string side;
        double      units;
        double      stoploss_pips;
        double      takeprofit_pips;
    };

    void worker_loop();
    OandaExecutionResult send_market_order(const OrderJob& job);

    // Persistent connection management (worker thread only).
    bool connect();                     // open TCP + TLS (TCP_NODELAY, no-delay)
    void disconnect();                  // close + clear the broken stream

    // Returns true if the underlying socket is currently open.
    bool connected() const;

    // Asio I/O. The io_context and ssl::context outlive the stream; the
    // stream member is owned exclusively by the worker thread.
    boost::asio::io_context                          io_;
    boost::asio::ssl::context  ssl_{boost::asio::ssl::context::tlsv12_client};
    std::unique_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> stream_;

    const Config& cfg_;

    std::string host_ = "api-fxpractice.oanda.com";  // resolved once
    std::string port_ = "443";
    std::atomic<bool> connected_{false};

    std::thread             thread_;
    std::atomic<bool>       running_{false};
    std::atomic<bool>       stopped_{false};

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::deque<OrderJob>    jobs_;
    bool                    wake_ = false;
    size_t                  queue_limit_ = 256;

    void (*cb_)(const OandaExecutionResult&, void*) = nullptr;
    void* cb_data_ = nullptr;
    int  next_id_  = 0;
};

}  // namespace llm