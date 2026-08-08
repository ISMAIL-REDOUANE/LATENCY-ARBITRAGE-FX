#include "llm/bybit_ws.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/core/ignore_unused.hpp>
#include <nlohmann/json.hpp>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include "llm/telemetry.h"

namespace beast     = boost::beast;
namespace net       = boost::asio;
namespace ssl       = boost::asio::ssl;
namespace websocket = beast::websocket;
using tcp = net::ip::tcp;

namespace llm {

namespace {
// Server keeps the connection alive by sending {"op":"ping"}; we must reply.
constexpr const char* kPong = "{\"op\":\"pong\"}";
}  // namespace

// ---------------------------------------------------------------------------
// Async session (recreated on every reconnect)
// ---------------------------------------------------------------------------
class BybitWs::Session : public std::enable_shared_from_this<Session> {
public:
    Session(BybitWs& owner, net::io_context& io, ssl::context& ctx)
        : owner_(owner),
          ws_(net::make_strand(io), ctx),
          resolver_(net::make_strand(io)),
          subscribe_frame_("{\"op\":\"subscribe\",\"args\":[\"" +
                           owner_.cfg_.bybit_channel + "." +
                           owner_.cfg_.bybit_symbol + "\"]}") {}

    void run() {
        resolver_.async_resolve(
            owner_.cfg_.bybit_host, owner_.cfg_.bybit_port,
            beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
    }

    // Abort any pending async op (shutdown path).
    void cancel() { ws_.next_layer().next_layer().cancel(); }

private:
    void on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
        if (ec) return fail(ec, "resolve");
        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
        beast::get_lowest_layer(ws_).async_connect(
            results,
            beast::bind_front_handler(&Session::on_connect, shared_from_this()));
    }

    void on_connect(beast::error_code ec,
                    tcp::resolver::results_type::endpoint_type ep) {
        boost::ignore_unused(ep);
        if (ec) return fail(ec, "connect");
        // SNI: Beast removed set_tls_host_name; set it on the OpenSSL handle.
        const int sni_rc = SSL_set_tlsext_host_name(
            ws_.next_layer().native_handle(), owner_.cfg_.bybit_host.c_str());
        if (sni_rc != 1)
            return fail(beast::error_code(
                            static_cast<int>(ERR_get_error()),
                            net::error::get_ssl_category()),
                        "set_tls_host_name");
        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
        ws_.next_layer().async_handshake(
            ssl::stream_base::client,
            beast::bind_front_handler(&Session::on_ssl_handshake,
                                      shared_from_this()));
    }

    void on_ssl_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "tls_handshake");
        beast::get_lowest_layer(ws_).expires_never();
        try {
            beast::get_lowest_layer(ws_).socket().set_option(tcp::no_delay{true});
        } catch (...) { /* non-fatal */ }
        ws_.set_option(websocket::stream_base::decorator(
            [this](websocket::request_type& req) {
                req.set(beast::http::field::host, owner_.cfg_.bybit_host);
                req.set(beast::http::field::user_agent, "lead-lag-arb/1.0");
            }));
        ws_.set_option(websocket::stream_base::timeout::suggested(
            beast::role_type::client));
        ws_.async_handshake(
            owner_.cfg_.bybit_host, owner_.cfg_.bybit_path,
            beast::bind_front_handler(&Session::on_ws_handshake,
                                      shared_from_this()));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        std::printf("[bybit] connected and streaming...\n");
        Telemetry::instance().log(
            std::string("\"bybit\":{\"subscribed\":\"") + subscribe_frame_ +
            "\"}");
        ws_.text(true);
        ws_.async_write(net::buffer(subscribe_frame_),
                        beast::bind_front_handler(&Session::on_subscribed,
                                                  shared_from_this()));
    }

    void on_subscribed(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "subscribe");
        do_read();
    }

    void do_read() {
        buffer_.clear();
        ws_.async_read(buffer_,
                       beast::bind_front_handler(&Session::on_read,
                                                 shared_from_this()));
    }

    void on_read(beast::error_code ec, std::size_t bytes) {
        boost::ignore_unused(bytes);
        if (ec) return fail(ec, "read");
        const std::string text = beast::buffers_to_string(buffer_.data());
        const std::string reply = owner_.handle_message(text);
        if (!reply.empty()) {
            // Must not use a stack string with async_write.
            out_frame_ = reply;
            ws_.text(true);
            ws_.async_write(net::buffer(out_frame_),
                            [self = shared_from_this()](beast::error_code wc,
                                                        std::size_t) {
                                if (wc) self->fail(wc, "pong");
                            });
        }
        do_read();
    }

    void fail(beast::error_code ec, const char* what) {
        if (owner_.stopped_.load()) return;
        Telemetry::instance().log_warn(std::string("\"bybit\":{\"") + what +
                                       "\":\"" + ec.message() + "\"}");
        owner_.session_.reset();
        const double delay_s = owner_.reconnect_delay_ms_ / 1000.0;
        owner_.reconnect_delay_ms_ =
            std::min(owner_.reconnect_delay_ms_ * 2.0, 60000.0);
        owner_.schedule_reconnect(delay_s);
    }

    BybitWs& owner_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    tcp::resolver  resolver_;
    beast::flat_buffer buffer_;
    std::string    out_frame_;
    const std::string subscribe_frame_;
};

// ---------------------------------------------------------------------------
// BybitWs
// ---------------------------------------------------------------------------
BybitWs::BybitWs(const Config& cfg, TickRing& out) : cfg_(cfg), out_(out) {}
BybitWs::~BybitWs() { stop(); }

void BybitWs::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&BybitWs::run, this);
}

void BybitWs::stop() {
    stopped_.store(true);
    if (io_) {
        auto& ioc = *reinterpret_cast<net::io_context*>(io_);
        if (session_) session_->cancel();
        ioc.stop();
    }
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

void BybitWs::run() {
    ssl::context ssl_ctx(ssl::context::tlsv12_client);
    ssl_ctx.set_verify_mode(ssl::verify_none);   // latency — prod: verify_peer
    net::io_context ioc;

    io_ = &ioc;
    ssl_ctx_ = &ssl_ctx;
    reconnect_delay_ms_ = 2000.0;

    spawn_session();

    // Keep the io alive even when the current session dies, so a reconnect
    // timer can respawn it. We post a guard timer that runs forever.
    auto guard = std::make_shared<net::steady_timer>(
        net::make_strand(ioc), std::chrono::hours(24));
    guard->async_wait([](beast::error_code) { /* keep alive */ });

    ioc.run();   // blocks until stop cancels the session and the guard
    io_ = nullptr;
    ssl_ctx_ = nullptr;
    running_.store(false);
}

void BybitWs::spawn_session() {
    if (stopped_.load() || !io_ || !ssl_ctx_) return;
    auto s = std::make_shared<Session>(
        *this, *reinterpret_cast<net::io_context*>(io_),
        *reinterpret_cast<ssl::context*>(ssl_ctx_));
    s->run();
    session_ = s;
}

void BybitWs::schedule_reconnect(double delay_s) {
    Telemetry::instance().log_warn(std::string("\"bybit\":{\"reconnect_in_s\":") +
                                   std::to_string(delay_s) + "}");
    if (stopped_.load() || !io_) return;
    auto& ioc = *reinterpret_cast<net::io_context*>(io_);
    auto timer = std::make_shared<net::steady_timer>(
        net::make_strand(ioc),
        std::chrono::milliseconds(static_cast<long long>(delay_s * 1000)));
    timer->async_wait([this, timer](beast::error_code ec) {
        (void)timer;
        if (ec || stopped_.load()) return;
        spawn_session();
    });
}

std::string BybitWs::handle_message(const std::string& msg) {
    try {
        const auto j = nlohmann::json::parse(msg);
        // Server keep-alive — reply pong if we ever see one.
        if (j.value("op", "") == "ping") return kPong;
        if (j.value("topic", "") != cfg_.bybit_channel + "." +
                                       cfg_.bybit_symbol)
            return "";
        const auto data = j.value("data", nlohmann::json());
        const auto b = data.value("b", nlohmann::json::array());
        const auto a = data.value("a", nlohmann::json::array());
        if (b.empty() || a.empty() || !b[0].is_array() || !a[0].is_array())
            return "";
        // Bybit sends prices as strings, e.g. "10000.0".
        const double bid = std::stod(b[0][0].get<std::string>());
        const double ask = std::stod(a[0][0].get<std::string>());
        if (bid <= 0.0 || ask <= 0.0) return "";

        Tick t;
        t.ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
        t.bid   = bid;
        t.ask   = ask;
        t.last  = (bid + ask) * 0.5;
        t.venue = Venue::Bybit;
        t.valid = 1;
        out_.push(t);
    } catch (...) {
        /* malformed frame — ignore, keep reading */
    }
    return "";
}

}  // namespace llm