#include "llm/broker_sub.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include <zmq.h>

#include "llm/config.h"
#include "llm/telemetry.h"

namespace llm {

BrokerSub::BrokerSub(const Config& cfg, SpscRing<BrokerQuote, 8192>& out)
    : cfg_(cfg), out_(out) {}
BrokerSub::~BrokerSub() { stop(); }

void BrokerSub::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&BrokerSub::run, this);
}

void BrokerSub::stop() {
    stopped_.store(true);
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

namespace {

// Parse a single field. Handles both "k=v" JSON-ish and plain values by
// locating the delimiter if present.
double fv(const std::string& s) { return std::atof(s.c_str()); }

// Split a delimited line.
std::vector<std::string> split(const std::string& s, char delim) {
    std::vector<std::string> out;
    size_t start = 0;
    for (size_t i = 0; i <= s.size(); ++i) {
        if (i == s.size() || s[i] == delim) {
            out.push_back(s.substr(start, i - start));
            start = i + 1;
        }
    }
    return out;
}

}  // namespace

void BrokerSub::run() {
    void* ctx = ::zmq_ctx_new();
    void* sock = ::zmq_socket(ctx, ZMQ_SUB);
    if (!ctx || !sock) {
        Telemetry::instance().log_error("\"broker\":{\"zmq_init_failed\"}");
        if (sock) ::zmq_close(sock);
        if (ctx)  ::zmq_ctx_term(ctx);
        return;
    }

    ::zmq_setsockopt(sock, ZMQ_RCVHWM, &CompileTime::kZMQRecvHwm, sizeof(int));
    int timeout = CompileTime::kSocketTimeoutMs;
    ::zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(int));
    ::zmq_setsockopt(sock, ZMQ_SUBSCRIBE, cfg_.broker_topic.c_str(),
                     static_cast<int>(cfg_.broker_topic.size()));

    if (::zmq_connect(sock, cfg_.broker_zmq_bind.c_str()) != 0) {
        Telemetry::instance().log_error(
            std::string("\"broker\":{\"connect_failed\":\"") +
            cfg_.broker_zmq_bind + "\"}");
        ::zmq_close(sock);
        ::zmq_ctx_term(ctx);
        return;
    }
    Telemetry::instance().log("\"broker\":{\"subscribed\":\"" +
                              cfg_.broker_topic + "\",\"bind\":\"" +
                              cfg_.broker_zmq_bind + "\"}");

    char buf[4096];
    while (running_.load() && !stopped_.load()) {
        const int n = ::zmq_recv(sock, buf, sizeof(buf) - 1, 0);
        if (n < 0) {
            if (::zmq_errno() == ETERM) break;
            continue;   // transient — keep polling
        }
        buf[n] = '\0';

        // Frame: "<topic> quote|bid,ask,spread,ts,latency_us,session"
        std::string frame(buf);
        auto lp = frame.find(cfg_.broker_topic);
        std::string body = (lp != std::string::npos)
                               ? frame.substr(lp + cfg_.broker_topic.size())
                               : frame;
        // strip leading '|'
        if (!body.empty() && body[0] == '|') body.erase(0, 1);
        auto parts = split(body, ',');

        BrokerQuote q;
        if (parts.size() >= 3) {
            q.bid    = fv(parts[0]);
            q.ask    = fv(parts[1]);
            q.spread = parts.size() >= 3 ? fv(parts[2]) : (q.ask - q.bid);
        }
        if (parts.size() >= 4) q.ts_ms = static_cast<int64_t>(::atoll(parts[3].c_str()));
        if (parts.size() >= 5) q.latency_us = static_cast<int64_t>(::atoll(parts[4].c_str()));
        if (parts.size() >= 6) q.session = static_cast<uint8_t>(::atoi(parts[5].c_str()));
        q.valid = 1;

        if (q.bid > 0.0 && q.ask > 0.0 && q.bid <= q.ask) {
            out_.push(q);
        }
    }

    ::zmq_close(sock);
    ::zmq_ctx_term(ctx);
    running_.store(false);
}

}  // namespace llm