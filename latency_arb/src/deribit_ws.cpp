#include "llm/deribit_ws.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>

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
// Build the JSON-RPC subscribe request. id generated per connection.
std::string subscribe_request(const std::string& channel, int id) {
    return std::string("{\"jsonrpc\":\"2.0\",\"method\":\"public/subscribe\","
                       "\"id\":") + std::to_string(id) +
           ",\"params\":{\"channels\":[\"" + channel + "\"]}}";
}
}  // namespace

// ---------------------------------------------------------------------------
// Async session (recreated on every reconnect)
// ---------------------------------------------------------------------------
class DeribitWs::Session : public std::enable_shared_from_this<Session> {
public:
    Session(DeribitWs& owner, net::io_context& io, ssl::context& ctx)
        : owner_(owner),
          ws_(net::make_strand(io), ctx),
          resolver_(net::make_strand(io)) {}

    void run() {
        resolver_.async_resolve(
            "www.deribit.com", "443",
            beast::bind_front_handler(&Session::on_resolve, shared_from_this()));
    }

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
            ws_.next_layer().native_handle(), "www.deribit.com");
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
            [](websocket::request_type& req) {
                req.set(beast::http::field::host, "www.deribit.com");
                req.set(beast::http::field::user_agent, "lead-lag-arb/1.0");
            }));
        ws_.set_option(websocket::stream_base::timeout::suggested(
            beast::role_type::client));
        ws_.async_handshake(
            "www.deribit.com", "/ws/api/v2",
            beast::bind_front_handler(&Session::on_ws_handshake,
                                      shared_from_this()));
    }

    void on_ws_handshake(beast::error_code ec) {
        if (ec) return fail(ec, "ws_handshake");
        // Send subscribe once per connection.
        const std::string channel = "deribit_price_index." +
                                    owner_.cfg_.deribit_symbol + "_usd";
        const int id = 1;
        ws_.text(true);
        ws_.async_write(
            net::buffer(subscribe_request(channel, id)),
            beast::bind_front_handler(&Session::on_subscribed,
                                      shared_from_this()));
    }

    void on_subscribed(beast::error_code ec, std::size_t) {
        if (ec) return fail(ec, "subscribe");
        Telemetry::instance().log(
            "\"deribit\":{\"subscribed\":\"deribit_price_index." +
            owner_.cfg_.deribit_symbol + "_usd\"}");
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
        owner_.handle_message(beast::buffers_to_string(buffer_.data()));
        do_read();
    }

    void fail(beast::error_code ec, const char* what) {
        if (owner_.stopped_.load()) return;
        Telemetry::instance().log_warn(std::string("\"deribit\":{\"") +
                                       what + "\":\"" + ec.message() + "\"}");
        owner_.session_.reset();
        const double delay = owner_.reconnect_delay_s_;
        owner_.reconnect_delay_s_ =
            std::min(owner_.reconnect_delay_s_ * 2.0, 60.0);
        owner_.schedule_reconnect(delay);
    }

    DeribitWs& owner_;
    websocket::stream<beast::ssl_stream<beast::tcp_stream>> ws_;
    tcp::resolver  resolver_;
    beast::flat_buffer buffer_;
};

// ---------------------------------------------------------------------------
// DeribitWs
// ---------------------------------------------------------------------------
DeribitWs::DeribitWs(const Config& cfg, TickRing& out) : cfg_(cfg), out_(out) {}
DeribitWs::~DeribitWs() { stop(); }

void DeribitWs::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&DeribitWs::run, this);
}

void DeribitWs::stop() {
    stopped_.store(true);
    if (io_) {
        auto& ioc = *reinterpret_cast<net::io_context*>(io_);
        if (session_) session_->cancel();
        ioc.stop();
    }
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

void DeribitWs::run() {
    ssl::context ssl_ctx(ssl::context::tlsv12_client);
    ssl_ctx.set_verify_mode(ssl::verify_none);   // latency — prod: verify_peer
    net::io_context ioc;

    io_ = &ioc;
    ssl_ctx_ = &ssl_ctx;
    reconnect_delay_s_ = 2.0;

    spawn_session();

    auto guard = std::make_shared<net::steady_timer>(
        net::make_strand(ioc), std::chrono::hours(24));
    guard->async_wait([](beast::error_code) { /* keep alive */ });

    ioc.run();
    io_ = nullptr;
    ssl_ctx_ = nullptr;
    running_.store(false);
}

void DeribitWs::spawn_session() {
    if (stopped_.load() || !io_ || !ssl_ctx_) return;
    auto s = std::make_shared<Session>(
        *this, *reinterpret_cast<net::io_context*>(io_),
        *reinterpret_cast<ssl::context*>(ssl_ctx_));
    s->run();
    session_ = s;
}

void DeribitWs::schedule_reconnect(double delay_s) {
    Telemetry::instance().log_warn(std::string("\"deribit\":{\"reconnect_in_s\":") +
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

void DeribitWs::handle_message(const std::string& msg) {
    try {
        const auto j = nlohmann::json::parse(msg);
        const auto params = j.value("params", nlohmann::json());
        const auto data   = params.value("data", nlohmann::json());
        const double price = data.value("price", 0.0);
        if (price <= 0.0) return;

        Tick t;
        t.ts_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
        t.bid   = price;   // index feed has a single price
        t.ask   = price;
        t.last  = price;
        t.venue = Venue::Deribit;
        t.valid = 1;
        out_.push(t);
    } catch (...) {
        /* malformed frame — ignore */
    }
}

}  // namespace llm