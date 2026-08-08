#pragma once

// ===========================================================================
// FixExecutor — asynchronous execution against the IC Markets cTrader FIX 4.4
// gateway over a plain TCP + TLS socket (Boost.Asio SSL). No MT5, no REST,
// no protobuf: a stock FIX 4.4 session is all the wire needs.
//
//   * Endpoint : demo-uk-eqx-01.p.c-trader.com:5211  (SSL, TLSv1.2)
//   * Logon    : 35=A  Tag49 SenderCompID = demo.icmarkets.10092442 (or live)
//                       Tag56 TargetCompID = cServer
//                       Tag554 (Password), Tag108 (HeartBtInt) = 30
//   * Heartbeat: 35=0 every HeartBtInt seconds; TestRequest 35=1 is acked with
//                a heartbeat carrying Tag112=TestReqID. Inbound sequence gaps
//                trigger 35=3 (ResendRequest) per FIX 4.4.
//   * Order    : 35=D NewOrderSingle, OrdType Tag40=1 (MARKET), TimeInForce
//                Tag59=3 (IOC). Stop-loss / take-profit levels are carried via
//                Tag99 (StopPx = SL price) and Tag100 (TP price) — the cTrader
//                FIX inline SL/TP extension used by IC Markets.
//   * Reports  : the async read loop classifies inbound 35=8 (ExecutionReport),
//                35=0/35=1/35=5 and records the enqueue->fill latency.
//   * Logout   : 35=5 on stop(), then TLS shutdown.
//
// Non-blocking contract: `execute()` ONLY enqueues an order and returns. All
// FIX serialization, socket writes and inbound parsing run on a dedicated
// session thread, so the Binance WebSocket read loop never sees broker I/O.
// A mutex + FIFO queue (with back-pressure cap) guards orders.
//
// Threading: the session thread is the SOLE owner of the Boost.Asio io_context
// and the ssl::stream. Writes are serialized through an asynchronous write
// queue (never two overlapping async_writes). Dispatchers wake the session via
// a posted handler on the io_context, so no lock guards the socket itself.
//
// Resilience: on any transport failure the session tears the TLS stream down,
// sleeps a back-off, and reconnects (fresh Logon, ResetSeqNum on reconnect).
// ===========================================================================

#include <atomic>
#include <array>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <boost/asio/connect.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/asio/steady_timer.hpp>

#include "llm/config.h"

namespace llm {

// Result of a single order attempt (issuer ClOrdID match), surfaced on the
// session thread through the result callback.
struct FixExecutionResult {
    bool        ok          = false;    // order accepted / executed
    std::string status;                 // "NEW" / "FILLED" / "REJECTED" etc.
    long        order_id    = 0;        // caller-visible serial (1-based)
    std::string cl_ord_id;              // the ClOrdID echoed on tag 11
    std::string broker_order_id;        // broker OrderID from tag 37, if present
    double      latency_ms  = 0.0;      // enqueue -> execution report
    std::string error;                  // human-readable failure text
};

class FixExecutor {
public:
    explicit FixExecutor(const Config& cfg);
    ~FixExecutor();

    FixExecutor(const FixExecutor&) = delete;
    FixExecutor& operator=(const FixExecutor&) = delete;

    void start();
    void stop();

    bool is_connected() const { return connected_.load(); }

    // Non-blocking: enqueue a market order and return immediately.
    //   * instrument   — FIX Symbol tag 55 (e.g. "XAU/USD", "BTC/USD").
    //   * side         — "buy" or "sell" (Tag 54: 1 = buy / 2 = sell).
    //   * volume       — order quantity in lots (Tag 38, fractional ok).
    //   * stop_price   — absolute SL price (Tag 99 StopPx).
    //   * take_price   — absolute TP price (Tag 100).
    void execute(const std::string& instrument, const std::string& side,
                 double volume, double stop_price, double take_price);

    // Max queue depth before execute() drops an order (back-pressure guard).
    void set_queue_limit(std::size_t limit) { queue_limit_ = limit; }

    // Optional callback (invoked on the session thread) to receive results.
    void set_result_callback(void (*cb)(const FixExecutionResult&, void*),
                             void* userdata);

private:
    struct OrderJob {
        std::string instrument;
        std::string side;
        double      volume;
        double      stop_price;
        double      take_price;
        long        order_id;           // caller-visible serial
        std::string cl_ord_id;          // unique ClOrdID built by execute()
        int64_t     t0_ms = 0;          // enqueue stamp for RTT measurement
    };

    // ---- session lifecycle ------------------------------------------------ //
    void session_loop();                // reconnect + fault-tolerant outer loop
    bool connect();                    // TCP + TLS handshake (session thread)
    void disconnect();                 // close the TLS stream (best effort)
    void run_read_loop();              // io_.run() until break / error
    void post_logout_and_stop_io();    // stop(): send 35=5 then halt io_

    // ---- io plumbing ------------------------------------------------------ //
    void io_drain_orders();            // posted from execute(); sends the 35=D
    void io_write(std::shared_ptr<std::string> frame);   // serialised write queue
    void pump_write();                 // issue the next queued async_write
    void start_read();                 // async_read_some on the TLS stream
    void on_read(const boost::system::error_code& ec, std::size_t bytes);
    void process_inbound();            // frame splitter (SOH-delimited FIX 4.4)
    void start_hb_timer();              // async heartbeat / peer-timeout timer

    // ---- FIX 4.4 handlers ------------------------------------------------- //
    void handle_frame(const std::string& frame);   // dispatch by MsgType 35
    void on_execution_report(const std::string& frame);   // 35=8
    void on_test_request(const std::string& frame);       // 35=1
    void on_logon_response(const std::string& frame);     // 35=A / 35=2 (login)
    void on_logout(const std::string& frame);             // 35=5
    void on_sequence_reset(const std::string& frame);     // 35=4

    // Compact tag scanner for a single frame.
    std::string scan_tag(const std::string& frame, int tag) const;

    // ---- FIX 4.4 builders (wire-correct) ---------------------------------- //
    std::string build_msg(char msg_type, const std::string& body);
    std::string build_logon();
    std::string build_logout();
    std::string build_heartbeat(const std::string& test_req_id);
    std::string build_order(const OrderJob& job);
    std::string build_resend_request(long begin_seq);

    const Config& cfg_;

    // ---- session state (session thread only) ------------------------------ //
    boost::asio::io_context io_;
    boost::asio::ssl::context ssl_{boost::asio::ssl::context::tlsv12_client};
    std::unique_ptr<boost::asio::ssl::stream<boost::asio::ip::tcp::socket>> stream_;
    std::string host_ = "demo-uk-eqx-01.p.c-trader.com";
    std::string port_ = "5211";
    long        seq_out_ = 1;       // outbound msg seq
    long        seq_in_  = 0;       // last inbound msg seq
    int64_t     last_recv_ms_ = 0;  // steady-clock stamp of last inbound msg
    std::atomic<bool> connected_{false};

    // ---- read machinery (session thread) ---------------------------------- //
    std::string       inbound_;
    std::array<char, 65536> rbuf_{};

    // ---- write queue (serialised via strand_) ----------------------------- //
    std::deque<std::shared_ptr<std::string>> wqueue_;
    bool writing_ = false;

    // ---- heartbeat timer (session thread) --------------------------------- //
    boost::asio::steady_timer hb_timer_;
    int64_t last_send_ms_ = 0;

    // ---- mailbox: execution dispatcher -> session thread -------------------- //
    std::mutex              mtx_;
    std::condition_variable cv_;
    bool                    wake_ = false;
    std::deque<OrderJob>    orders_;
    std::size_t             queue_limit_ = 256;

    // ---- ClOrdID -> job table (session thread only); T0 = enqueue tick. ---- //
    struct PendingOrder {
        OrderJob job;
        int64_t  t0_ms = 0;
    };
    std::unordered_map<std::string, PendingOrder> pending_orders_;

    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};

    void (*cb_)(const FixExecutionResult&, void*) = nullptr;
    void* cb_data_            = nullptr;
    long  next_order_id_      = 1;
};

}  // namespace llm