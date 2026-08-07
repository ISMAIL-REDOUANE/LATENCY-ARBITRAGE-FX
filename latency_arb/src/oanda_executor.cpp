#include "llm/oanda_executor.h"

#include <cstdio>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace boost  = boost::beast;
namespace http   = boost::beast::http;
namespace asio   = boost::asio;
namespace ssl    = boost::asio::ssl;
using   tcp      = boost::asio::ip::tcp;

namespace {

// Minimal JSON string escaper (OANDA queries are bounded; keep it simple).
std::string json_str(const std::string& s) {
    std::ostringstream o;
    o << '"';
    for (char c : s) {
        switch (c) {
            case '"': o << "\\\""; break;
            case '\\': o << "\\\\"; break;
            case '\n': o << "\\n";  break;
            case '\r': o << "\\r";  break;
            default:   o << c;      break;
        }
    }
    o << '"';
    return o.str();
}

std::string trim_view(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return std::string();
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

// Format a pip distance as an OANDA OrderDistance "distance" string. OANDA's
// Document with 5/3 precision expects e.g. "0.00100" for XAU_USD quoted at
// 0.001 pip; the caller supplies pips already scaled to the account's units.
std::string distance_str(double pips) {
    char buf[40];
    std::snprintf(buf, sizeof buf, "%.6f", pips);
    // strip trailing zeros for a clean OANDA-acceptable literal
    std::string s(buf);
    if (s.find('.') != std::string::npos) {
        while (!s.empty() && s.back() == '0') s.pop_back();
        if (!s.empty() && s.back() == '.') s.pop_back();
    }
    return s.empty() ? std::string("0") : s;
}

}  // namespace

namespace llm {

OandaExecutor::OandaExecutor(const Config& cfg)
    : cfg_(cfg), ssl_{boost::asio::ssl::context::tlsv12_client} {}

OandaExecutor::~OandaExecutor() { stop(); }

void OandaExecutor::start() {
    if (running_.exchange(true)) return;
    thread_ = std::thread(&OandaExecutor::worker_loop, this);
}

void OandaExecutor::stop() {
    if (!running_.exchange(false)) return;
    { std::lock_guard<std::mutex> lk(mtx_); wake_ = true; }
    cv_.notify_all();
    if (thread_.joinable()) thread_.join();
}

void OandaExecutor::set_result_callback(
    void (*cb)(const OandaExecutionResult&, void*), void* userdata) {
    cb_ = cb;
    cb_data_ = userdata;
}

void OandaExecutor::execute(const std::string& instrument,
                            const std::string& side, double units,
                            double stoploss_pips, double takeprofit_pips) {
    if (instrument.empty() || (side != "buy" && side != "sell")) return;
    OrderJob j;
    j.instrument     = instrument;
    j.side           = side;
    j.units          = units;
    j.stoploss_pips  = stoploss_pips;
    j.takeprofit_pips= takeprofit_pips;
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (jobs_.size() >= queue_limit_) return;   // back-pressure drop
        jobs_.push_back(std::move(j));
        wake_ = true;
    }
    cv_.notify_all();
}

void OandaExecutor::worker_loop() {
    while (running_.load()) {
        OrderJob job;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [&] { return wake_ || !running_.load(); });
            wake_ = false;
            if (jobs_.empty()) {
                if (!running_.load()) break;
                continue;
            }
            job = std::move(jobs_.front());
            jobs_.pop_front();
        }
        OandaExecutionResult r = send_market_order(job);
        if (cb_) cb_(r, cb_data_);
    }
}

// ---------------------------------------------------------------------------
// One HTTPS POST to OANDA v2. Body follows the MarketOrderRequest schema:
//
//   POST /v3/accounts/{ACCOUNT_ID}/orders
//   Authorization: Bearer {TOKEN}
//   Content-Type: application/json
//
//   {
//     "order": {
//       "type": "MARKET",
//       "instrument": "XAU_USD",
//       "units": "10",
//       "timeInForce": "FOK",
//       "positionFill": "DEFAULT",
//       "stopLossOnFill": { "distance": "0.150" },
//       "takeProfitOnFill": { "distance": "0.350" },
//       "clientOrderID": "..."
//     }
//   }
//
// Stop loss / take profit use "distance" (pip offset) — OANDA converts to the
// absolute price, so we never have to guess a live quote for the exit levels.
// ---------------------------------------------------------------------------
OandaExecutionResult OandaExecutor::send_market_order(const OrderJob& job) {
    OandaExecutionResult out;
    out.order_id = ++next_id_;

    const std::string host = cfg_.oanda_host.empty()
                                 ? std::string("api-fxpractice.oanda.com")
                                 : cfg_.oanda_host;
    const std::string port = "443";
    const std::string target = "/v3/accounts/" + cfg_.oanda_account_id + "/orders";

    // ---- Build the JSON body (exact OANDA v2 schema) -----------------------
    std::ostringstream body;
    body << "{\"order\":{"
         << "\"type\":\"MARKET\","
         << "\"instrument\":"      << json_str(job.instrument) << ','
         << "\"units\":"           << json_str(std::to_string((long long)job.units)) << ','
         << "\"timeInForce\":\"FOK\","
         << "\"positionFill\":\"DEFAULT\","
         << "\"stopLossOnFill\":{\"distance\":\""
             << distance_str(job.stoploss_pips) << "\"},"
         << "\"takeProfitOnFill\":{\"distance\":\""
             << distance_str(job.takeprofit_pips) << "\"}"
         << "}}";

    // ---- ns-resolution latency measurement ---------------------------------
    const auto t0 = std::chrono::high_resolution_clock::now();

    try {
        boost::asio::io_context ioc;
        ssl::stream<tcp::socket> stream(ioc, ssl_);

        tcp::resolver resolver(ioc);
        auto endpoints = resolver.resolve(host, port);
        asio::connect(stream.next_socket(), endpoints);
        stream.handshake(ssl::stream_base::client);

        http::request<http::string_body> req{http::verb::post, target, 11};
        req.set(http::field::host, host);
        req.set(http::field::user_agent, "lead-lag-latency/1.0");
        req.set(http::field::content_type, "application/json");
        req.set(http::field::authorization, "Bearer " + cfg_.oanda_token);
        req.body() = body.str();
        req.prepare_payload();

        http::write(stream, req);

        boost::beast::flat_buffer buffer;
        http::response<http::string_body> res;
        http::read(stream, buffer, res);

        const auto t1 = std::chrono::high_resolution_clock::now();
        out.latency_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        out.http_status = res.result_int();
        out.ok = (res.result_int() >= 200 && res.result_int() < 300);

        if (out.ok) {
            // Best-effort: echo the transaction id when the body carries it.
            // A full JSON parser is intentionally avoided to keep the depot
            // lean; enable a real parser if you need structured fields.
            std::string b = trim_view(res.body());
            const std::string key = "\"id\":";
            size_t p = b.find(key);
            if (p != std::string::npos) {
                out.transaction_id = b.substr(p + key.size());
            }
        } else {
            out.error = "HTTP " + std::to_string(res.result_int()) + ": " +
                        res.body().substr(0, 256);
        }
        beast::error_code ec;
        stream.shutdown(ec);   // TLS shutdown is best-effort
    } catch (const std::exception& e) {
        out.error = std::string("exec exception: ") + e.what();
        out.ok = false;
    } catch (...) {
        out.error = "exec exception (unknown)";
        out.ok = false;
    }

    std::printf("[OANDA] id=%d %s %s  u=%.0f  -> HTTP %d %s  %5.2f ms  %s\n",
                out.order_id, job.instrument.c_str(), job.side.c_str(), job.units,
                out.http_status, out.ok ? "OK" : "FAIL", out.latency_ms,
                out.error.c_str());
    return out;
}

}  // namespace llm