// ===========================================================================
// FixExecutor — IC Markets cTrader FIX 4.4 over Boost.Asio SSL.
// See fix_executor.h for the full design; this file is the session engine.
//
// Lifecycle:
//   start()   -> spawns the session thread -> connect() (TCP + TLS) -> Logon
//                (35=A) -> io_context.run()
//   execute() enqueues through a mutex mailbox; the session thread is the SOLE
//                owner of io_context / ssl::stream / buffers.
//   stop()    -> posts a Logout (35=5) + io stop onto the io_context, then
//                joins the session thread.
// ===========================================================================

#include "llm/fix_executor.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/write.hpp>

namespace asio = boost::asio;
namespace ssl  = boost::asio::ssl;
using   tcp    = boost::asio::ip::tcp;

namespace llm {

namespace {

constexpr char  SOH      = '\x01';
constexpr char  FIX_VER[] = "FIX.4.4";
constexpr long  kMaxMsgLen  = 1 << 20;   // 1 MiB sanity cap on inbound frames
constexpr long  kDefaultHbMs = 30'000;   // default HeartBtInt (30 s)

// Steady-clock ms since process start (heartbeat/disconnect math).
int64_t now_ms() {
    static auto const origin = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - origin).count();
}

// UTC "YYYYMMDD-HH:MM:SS.mmm" for tag 52.
std::string utc_now() {
    using std::chrono::system_clock;
    const auto now = system_clock::now();
    const auto tt  = system_clock::to_time_t(now);
    const auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                         now.time_since_epoch()).count() % 1000;
    std::tm tmv{};
#ifdef _WIN32
    gmtime_s(&tmv, &tt);
#else
    gmtime_r(&tt, &tmv);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y%m%d-%H:%M:%S", &tmv);
    std::snprintf(buf + std::strlen(buf), sizeof(buf) - std::strlen(buf),
                  ".%03d", static_cast<int>(ms));
    return buf;
}

void fix_field(std::string& b, int tag, const std::string& v) {
    b += std::to_string(tag); b += '='; b += v; b += SOH;
}
void fix_field(std::string& b, int tag, long v) { fix_field(b, tag, std::to_string(v)); }
void fix_field(std::string& b, int tag, double v) {
    char buf[48]; std::snprintf(buf, sizeof(buf), "%.5f", v);
    fix_field(b, tag, buf);
}

// Price with trailing-zero trimming (gold 2-dp, FX 5-dp, oh whatever the venue).
std::string fmt_px(double px) {
    char buf[48];
    std::snprintf(buf, sizeof(buf), "%.5f", px);
    std::string s(buf);
    if (s.find('.') != std::string::npos) {
        while (!s.empty() && s.back() == '0') s.pop_back();
        if (!s.empty() && s.back() == '.') s.pop_back();
    }
    return s.empty() ? std::string("0") : s;
}

// Quantity formatting (fractional lots -> "0.5", "1.25").
std::string fmt_qty(double q) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%g", q);
    return buf;
}

bool is_digits(const std::string& s) {
    return !s.empty() && std::all_of(s.begin(), s.end(),
                                     [](char c) { return c >= '0' && c <= '9'; });
}

}  // namespace

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------
FixExecutor::FixExecutor(const Config& cfg)
    : cfg_(cfg), hb_timer_(io_) {
    if (!cfg_.fix_host.empty())        host_ = cfg_.fix_host;
    if (!cfg_.fix_port.empty())        port_ = cfg_.fix_port;
    ssl_.set_verify_mode(asio::ssl::verify_none);   // broker cert; pin in prod
}

FixExecutor::~FixExecutor() { stop(); }

void FixExecutor::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&FixExecutor::session_loop, this);
}

void FixExecutor::stop() {
    if (stopped_.exchange(true)) return;
    running_.store(false);
    { std::lock_guard<std::mutex> lk(mtx_); wake_ = true; }
    cv_.notify_all();

    // Graceful Logout before the socket dies: posted onto the io_ so it runs on
    // the session thread. If we are mid-reconnect it is a harmless no-op.
    asio::post(io_, [this] {
        if (stream_ && connected_.load()) {
            boost::system::error_code ec;
            const std::string lg = build_logout();
            asio::write(*stream_, asio::buffer(lg), ec);   // sync, 1 final 35=5
            stream_->shutdown(ec);
        }
        io_.stop();
    });
    if (thread_.joinable()) thread_.join();
    hb_timer_.cancel();
}

void FixExecutor::set_result_callback(
    void (*cb)(const FixExecutionResult&, void*), void* userdata) {
    cb_ = cb; cb_data_ = userdata;
}

// ---------------------------------------------------------------------------
// Non-blocking enqueue (called from the strategy/execution thread).
// ---------------------------------------------------------------------------
void FixExecutor::execute(const std::string& instrument,
                          const std::string& side, double volume,
                          double stop_price, double take_price) {
    if (instrument.empty() || (side != "buy" && side != "sell") || volume <= 0)
        return;
    OrderJob j;
    j.instrument = instrument;
    j.side       = side;
    j.volume     = volume;
    j.stop_price = stop_price;
    j.take_price = take_price;
    j.order_id   = next_order_id_++;
    j.cl_ord_id  = "LL" + std::to_string(j.order_id);
    j.t0_ms      = now_ms();

    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (orders_.size() >= queue_limit_) return;   // back-pressure drop
        orders_.push_back(std::move(j));
    }
    cv_.notify_all();
    asio::post(io_, [this] { io_drain_orders(); });
}

// ---------------------------------------------------------------------------
// Session loop — reconnect forever until stop().
// ---------------------------------------------------------------------------
void FixExecutor::session_loop() {
    std::printf("[FIX] session thread started (%s:%s)\n", host_.c_str(), port_.c_str());
    while (running_.load() && !stopped_.load()) {
        if (!connect()) {
            std::printf("[FIX] connect failed to %s:%s; retrying...\n",
                        host_.c_str(), port_.c_str());
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait_for(lk, std::chrono::seconds(
                cfg_.ws_reconnect_base_s > 0 ? cfg_.ws_reconnect_base_s : 2),
                [&] { return !running_.load() || wake_; });
            wake_ = false;
            continue;
        }
        connected_.store(true);
        seq_out_ = 1;                     // fresh session, fresh msg-seq window
        // Send the FIX 4.4 Logon (35=A) before anything else; the session
        // thread owns the stream, so a direct io_write is safe here.
        io_write(std::make_shared<std::string>(build_logon()));
        run_read_loop();                  // returns only when io_ stops
        connected_.store(false);
        if (!stopped_.load())
            std::printf("[FIX] session ended, reconnecting...\n");
        if (stream_) { disconnect(); }
    }
    std::printf("[FIX] session thread stopped\n");
}

bool FixExecutor::connect() {
    try {
        stream_ = std::make_unique<ssl::stream<tcp::socket>>(io_, ssl_);
        boost::system::error_code ec;
        stream_->next_layer().set_option(tcp::no_delay(true), ec);

        tcp::resolver resolver(io_);
        auto eps = resolver.resolve(host_, port_, ec);
        if (ec) return false;
        asio::connect(stream_->next_layer(), eps, ec);
        if (ec) { std::printf("[FIX] tcp connect: %s\n", ec.message().c_str()); return false; }

        stream_->handshake(ssl::stream_base::client, ec);
        if (ec) { std::printf("[FIX] tls handshake: %s\n", ec.message().c_str()); return false; }

        std::printf("[FIX] TLS up %s:%s, sending Logon\n", host_.c_str(), port_.c_str());
        return true;
    } catch (const std::exception& e) {
        std::printf("[FIX] connect threw: %s\n", e.what());
        disconnect();
        return false;
    } catch (...) {
        disconnect(); return false;
    }
}

void FixExecutor::disconnect() {
    if (!stream_) return;
    boost::system::error_code ec;
    stream_->shutdown(ec);       // best-effort TLS close
    stream_->next_layer().close(ec);
    stream_.reset();
}

// ---------------------------------------------------------------------------
// Main loop: drains queued orders (Logon already queued in connect), arms the
// async read + heartbeat timer, then blocks in io_.run().
// ---------------------------------------------------------------------------
void FixExecutor::run_read_loop() {
    io_.restart();
    io_drain_orders();                     // offline-queued 35=D's, if any
    last_recv_ms_ = now_ms();
    start_read();
    start_hb_timer();
    io_.run();                             // session lifetime
}

// ---------------------------------------------------------------------------
// Outbound write queue — one async write at a time on the shared stream.
// ---------------------------------------------------------------------------
void FixExecutor::io_write(std::shared_ptr<std::string> frame) {
    last_send_ms_ = now_ms();
    wqueue_.push_back(std::move(frame));
    pump_write();
}

void FixExecutor::pump_write() {
    if (writing_ || !stream_ || wqueue_.empty()) return;
    auto frame = wqueue_.front();
    writing_ = true;
    asio::async_write(
        *stream_, asio::buffer(*frame),
        [this](const boost::system::error_code& ec, std::size_t /*bytes*/) {
            writing_ = false;
            if (!wqueue_.empty()) wqueue_.pop_front();
            if (ec) {
                std::printf("[FIX] write error: %s\n", ec.message().c_str());
                connected_.store(false);
                io_.stop();                        // → session_loop reconnect
                return;
            }
            pump_write();
        });
}

void FixExecutor::io_drain_orders() {
    std::deque<OrderJob> local;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        local.swap(orders_);
    }
    if (local.empty()) return;
    if (!connected_.load()) {
        // Offline — park them back in front (FIFO preserved) for reconnect.
        std::lock_guard<std::mutex> lk(mtx_);
        while (!local.empty()) {
            orders_.push_front(std::move(local.back()));
            local.pop_back();
        }
        return;
    }
    while (!local.empty()) {
        OrderJob job = std::move(local.front());
        local.pop_front();
        pending_orders_[job.cl_ord_id] = PendingOrder{job, job.t0_ms};
        std::printf("[FIX] order=%ld %s %s vol=%.2f sl=%s tp=%s cl='%s'\n",
                    job.order_id, job.instrument.c_str(), job.side.c_str(),
                    job.volume, fmt_px(job.stop_price).c_str(),
                    fmt_px(job.take_price).c_str(), job.cl_ord_id.c_str());
        io_write(std::make_shared<std::string>(build_order(job)));
    }
}

// ---------------------------------------------------------------------------
// Inbound — async read_some into a growing buffer, frame-split on SOH.
// ---------------------------------------------------------------------------
void FixExecutor::start_read() {
    if (stopped_ || !stream_ || !connected_.load()) return;
    stream_->async_read_some(
        asio::buffer(rbuf_),
        [this](const boost::system::error_code& ec, std::size_t n) {
            on_read(ec, n);
        });
}

void FixExecutor::on_read(const boost::system::error_code& ec, std::size_t n) {
    if (ec) {
        if (ec == asio::error::operation_aborted) return;
        std::printf("[FIX] read error: %s — closing session\n", ec.message().c_str());
        io_.stop();
        return;
    }
    last_recv_ms_ = now_ms();
    inbound_.append(rbuf_.data(), n);
    process_inbound();
    start_read();                 // re-arm next read
}

// ---------------------------------------------------------------------------
// Inbound splitter — pulls complete FIX 4.4 frames out of the byte stream.
// Each frame is framed by its BodyLength (tag 9) so partial TLS records never
// stick a half message into handle_frame().
// ---------------------------------------------------------------------------

void FixExecutor::process_inbound() {
    while (true) {
        // Locate a frame header "8=FIX.4.4<SOH>"; trim any partial preamble.
        const size_t header = inbound_.find(std::string("8=") + FIX_VER + SOH);
        if (header == std::string::npos) {
            // No complete header yet; keep the possible trailing tail bytes.
            if (inbound_.size() > 16) inbound_.erase(0, inbound_.size() - 16);
            return;
        }
        if (header > 0) inbound_.erase(0, header);

        // BodyLength tag (9=).
        const size_t p9  = inbound_.find("9=");
        if (p9 == std::string::npos) return;                     // incomplete
        const size_t p9e = inbound_.find(SOH, p9 + 2);
        if (p9e == std::string::npos) return;                    // incomplete
        const std::string blen_s = inbound_.substr(p9 + 2, p9e - p9 - 2);
        if (!is_digits(blen_s)) { inbound_.erase(0, 1); continue; } // resync
        const long blen = std::atol(blen_s.c_str());
        if (blen < 0 || blen > kMaxMsgLen) { inbound_.erase(0, 1); continue; }

        // Frame length = body-start + BodyLength + tag10="000<SOH> (7 bytes).
        const size_t total = (p9e + 1) + static_cast<size_t>(blen) + 7;
        if (total > inbound_.size()) return;                     // await remainder
        const std::string frame = inbound_.substr(0, total);
        inbound_.erase(0, total);

        handle_frame(frame);
        if (stopped_ || !running_.load()) return;
    }
}

// ---------------------------------------------------------------------------
// Heartbeat timer — send 35=0 when quiet for one HeartBtInt; drop session on
// inbound silence beyond 1.5x to let the reconnect loop recover.
// ---------------------------------------------------------------------------
void FixExecutor::start_hb_timer() {
    if (stopped_.load() || !connected_.load()) return;
    const long hb = cfg_.fix_heartbeat_s > 0
                        ? cfg_.fix_heartbeat_s * 1000L
                        : kDefaultHbMs;
    hb_timer_.expires_after(std::chrono::milliseconds(hb));
    hb_timer_.async_wait([this, hb](const boost::system::error_code& ec) {
        if (ec) return;
        if (!connected_.load() || !running_.load() || stopped_.load()) return;
        const int64_t now = now_ms();
        if (now - last_recv_ms_ > hb * 3 / 2) {
            std::printf("[FIX] peer timeout (%ld ms silent), reconnecting\n",
                        static_cast<long>(now - last_recv_ms_));
            io_.stop();
            return;
        }
        if (now - last_send_ms_ >= hb) io_write(std::make_shared<std::string>(build_heartbeat("")));
        start_hb_timer();
    });
}

// ---------------------------------------------------------------------------
// Frame dispatch
// ---------------------------------------------------------------------------
void FixExecutor::handle_frame(const std::string& f) {
    last_recv_ms_ = now_ms();
    // Sequence bookkeeping (FIX 4.4: 34). A gap triggers a resend request.
    const std::string sns = scan_tag(f, 34);
    if (is_digits(sns)) {
        const long sn = std::atol(sns.c_str());
        if (sn > 0) {
            if (seq_in_ == 0) seq_in_ = sn;
            else if (sn > seq_in_ + 1) {
                seq_in_ = sn;
                io_write(std::make_shared<std::string>(build_resend_request(seq_in_)));
            } else {
                seq_in_ = std::max(seq_in_, sn);
            }
        }
    }

    const std::string mt = scan_tag(f, 35);
    std::printf("[FIX] <- 35=%s (seq=%s)\n", mt.c_str(), scan_tag(f, 34).c_str());
    if (mt == "A")           on_logon_response(f);
    else if (mt == "0")      { /* heartbeat: nothing to do */ }
    else if (mt == "8")      on_execution_report(f);
    else if (mt == "1")      on_test_request(f);
    else if (mt == "4")      on_sequence_reset(f);
    else if (mt == "5")      on_logout(f);
    else if (mt == "2")      { /* ResendRequest; lean client has no log of sent
                                  messages to replay, so just ignore */ }
    // 35=3, 35=j, 35=p etc. are intentionally ignored in the lean client.
}

// ---------------------------------------------------------------------------
// Execution report 35=8 — match by ClOrdID (tag 11), terminal outcome,
// latency = now - enqueue_stamp.
// ---------------------------------------------------------------------------
void FixExecutor::on_execution_report(const std::string& frame) {
    const std::string cl = scan_tag(frame, 11);
    if (cl.empty()) return;
    auto it = pending_orders_.find(cl);
    if (it == pending_orders_.end()) return;          // restatement / unknown order
    const OrderJob& job = it->second.job;

    const std::string exectype = scan_tag(frame, 150);   // 0 New 1 Partial 2 Fill 8 Reject
    const std::string ordstat  = scan_tag(frame, 39);
    const std::string text     = scan_tag(frame, 58);

    FixExecutionResult r;
    r.order_id       = job.order_id;
    r.cl_ord_id      = cl;
    r.broker_order_id= scan_tag(frame, 37);
    r.latency_ms     = static_cast<double>(now_ms() - it->second.t0_ms);

    bool terminal = false;
    if (exectype == "2") { r.status = "FILLED"; r.ok = true;  terminal = true; }
    else if (exectype == "1") { r.status = "PARTIAL"; r.ok = true; terminal = true; }
    else if (exectype == "8" || exectype == "3" || ordstat == "8") {
        r.status = "REJECTED"; r.ok = false; r.error = text; terminal = true;
    } else if (exectype == "4") { r.status = "CANCELED"; r.ok = false; terminal = true; }
    else if (exectype == "0") {
        r.status = "NEW"; r.ok = true;                       // placement ack
        terminal = true;                                     // IOC market: fill follows
    }
    if (terminal) {
        std::printf("[FIX] exec: cl=%s execType=%s ordStat=%s lat=%.2fms\n",
                    cl.c_str(), exectype.c_str(), ordstat.c_str(), r.latency_ms);
        pending_orders_.erase(it);
        if (cb_) cb_(r, cb_data_);
    }
}

void FixExecutor::on_test_request(const std::string& frame) {
    const std::string id = scan_tag(frame, 112);
    io_write(std::make_shared<std::string>(build_heartbeat(id)));
}

void FixExecutor::on_logon_response(const std::string& /*frame*/) {
    std::printf("[FIX] logon ACK received\n");
}

void FixExecutor::on_logout(const std::string& frame) {
    const std::string text = scan_tag(frame, 58);
    std::printf("[FIX] logout received: %.60s%s\n", text.c_str(),
                text.size() > 60 ? "..." : "");
}

void FixExecutor::on_sequence_reset(const std::string& /*frame*/) {
    seq_in_ = 0;   // fresh after ResetSeqNum (GapFill): re-baseline
}

// ---------------------------------------------------------------------------
// Tag scanner: single pass over the SOH-delimited fields.
// ---------------------------------------------------------------------------
std::string FixExecutor::scan_tag(const std::string& f, int tag) const {
    size_t i = 0;
    while (i < f.size()) {
        size_t eq = f.find('=', i);
        if (eq == std::string::npos) break;
        size_t soh = f.find(SOH, eq);
        std::string tagn = f.substr(i, eq - i);
        if (is_digits(tagn) && std::atoi(tagn.c_str()) == tag) {
            if (soh == std::string::npos) return f.substr(eq + 1);
            return f.substr(eq + 1, soh - eq - 1);
        }
        if (soh == std::string::npos) break;
        i = soh + 1;
    }
    return "";
}

// ---------------------------------------------------------------------------
// FIX 4.4 message builders
// ---------------------------------------------------------------------------
std::string FixExecutor::build_msg(char msg_type, const std::string& body) {
    std::string hdr;
    fix_field(hdr, 35, std::string(1, msg_type));
    fix_field(hdr, 49, cfg_.fix_sender_comp_id);
    fix_field(hdr, 56, cfg_.fix_target_comp_id);
    fix_field(hdr, 34, seq_out_++);
    fix_field(hdr, 52, utc_now());
    const long blen = static_cast<long>(hdr.size() + body.size());
    std::string msg = std::string("8=") + FIX_VER + SOH + "9=" +
                      std::to_string(blen) + SOH + hdr + body;
    unsigned sum = 0;
    for (unsigned char c : msg) sum += c;
    char ck[8];
    std::snprintf(ck, sizeof(ck), "%03u", sum % 256);
    msg += std::string("10=") + ck + SOH;
    return msg;
}

std::string FixExecutor::build_logon() {
    std::string b;
    fix_field(b, 98, "0");                               // EncryptMethod = none
    fix_field(b, 108, static_cast<long>(cfg_.fix_heartbeat_s > 0
                                           ? cfg_.fix_heartbeat_s : 30));  // HeartBtInt
    fix_field(b, 141, "1");                               // ResetSeqNum
    fix_field(b, 553, cfg_.fix_sender_comp_id);           // Username (SenderCompID)
    fix_field(b, 554, cfg_.fix_password);                 // Password (raw)
    fix_field(b, 1, cfg_.fix_account_id);                 // Account id
    return build_msg('A', b);                            // 35=A
}

std::string FixExecutor::build_logout() {
    std::string b;
    // tag 58 free text; spec requires Text on Logout for the reason.
    fix_field(b, 58, "client shutting down, bye");
    return build_msg('5', b);                            // 35=5
}

std::string FixExecutor::build_heartbeat(const std::string& test_req_id) {
    std::string b;
    if (!test_req_id.empty()) fix_field(b, 112, test_req_id);
    return build_msg('0', b);                            // 35=0
}

// The order message, cTrader FIX 4.4 dialect:
//   Tag 55 Symbol, 54 Side, 38 OrderQty(lots), 40=1 MARKET, 59=3 IOC,
//   Tag 99 StopPx (-> SL), Tag 100 (-> TP).
std::string FixExecutor::build_order(const OrderJob& job) {
    std::string b;
    fix_field(b, 11, job.cl_ord_id);                 // ClOrdID
    fix_field(b, 1, cfg_.fix_account_id);            // Account
    fix_field(b, 55, job.instrument);                // SymPx
    fix_field(b, 54, job.side == "buy" ? "1" : "2");   // Side
    // OrderQty (lots, fractional allowed — e.g. "0.5").
    fix_field(b, 38, fmt_qty(job.volume));
    fix_field(b, 40, "1");                           // OrdType = MARKET
    fix_field(b, 59, "3");                           // TimeInForce = IOC
    fix_field(b, 60, utc_now());                     // TransactTime
    if (job.stop_price > 0) fix_field(b, 99, job.stop_price);   // SL level
    if (job.take_price > 0) fix_field(b, 100, job.take_price);  // TP level (cTrader ext)
    return build_msg('D', b);                        // 35=D
}

std::string FixExecutor::build_resend_request(long begin_seq) {
    std::string b;
    fix_field(b, 7, begin_seq);     // BeginSeqNo
    fix_field(b, 8, 0L);            // EndSeqNo 0 => unbounded
    return build_msg('3', b);       // 35=3 ResendRequest
}

}  // namespace llm