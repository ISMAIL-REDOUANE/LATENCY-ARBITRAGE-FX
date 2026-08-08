#include "llm/zmq_pub.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <utility>
#include <vector>

#include <zmq.h>

#include "llm/config.h"
#include "llm/telemetry.h"

namespace llm {
namespace {

// Encode a Tick into the wire framing used by Python/MT5 bridge and FIX.
std::string tick_frame(const Tick& t) {
    char buf[256];
    const char* venue = "?";
    switch (t.venue) {
        case Venue::Binance: venue = "binance"; break;
        case Venue::Deribit: venue = "deribit"; break;
        case Venue::Bybit:   venue = "bybit";   break;
        case Venue::Broker:  venue = "broker";  break;
        default: break;
    }
    std::snprintf(buf, sizeof(buf),
                  "tick|%lld,%lld,%lld,%lld,%.6f,%.6f,%.6f,%s,%u",
                  static_cast<long long>(t.ts_ms),
                  static_cast<long long>(t.ts_ns),
                  static_cast<long long>(0),       // seq (reserved)
                  static_cast<long long>(t.seq),
                  t.bid, t.ask, t.last, venue, t.valid);
    return buf;
}

std::string quote_frame(const BrokerQuote& q) {
    char buf[160];
    std::snprintf(buf, sizeof(buf),
                  "quote|%lld,%.6f,%.6f,%.6f,%lld,%u,%u",
                  static_cast<long long>(q.ts_ms),
                  q.bid, q.ask, q.spread,
                  static_cast<long long>(q.latency_us),
                  q.session, q.valid);
    return buf;
}

}  // namespace

ZmqPub::ZmqPub(std::string bind) : bind_(std::move(bind)) {}

ZmqPub::~ZmqPub() { stop(); }

void ZmqPub::publish(const Tick& tick) {
    std::lock_guard<std::mutex> lk(mtx_);
    queue_.push_back(Msg{Payload::Tick, tick, BrokerQuote{}, ""});
}

void ZmqPub::publish(const BrokerQuote& q) {
    std::lock_guard<std::mutex> lk(mtx_);
    queue_.push_back(Msg{Payload::Broker, Tick{}, q, ""});
}

void ZmqPub::publish_signal(const std::string& json) {
    std::lock_guard<std::mutex> lk(mtx_);
    queue_.push_back(Msg{Payload::Signal, Tick{}, BrokerQuote{}, json});
}

void ZmqPub::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    shutdown_.store(false);
    thread_ = std::thread(&ZmqPub::thread_loop, this);
}

void ZmqPub::stop() {
    shutdown_.store(true);
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

void ZmqPub::drain(std::vector<Msg>& out) {
    out.clear();
    std::lock_guard<std::mutex> lk(mtx_);
    while (!queue_.empty()) {
        out.push_back(std::move(queue_.front()));
        queue_.pop_front();
    }
}

void ZmqPub::thread_loop() {
    void* ctx = ::zmq_ctx_new();
    if (!ctx) {
        Telemetry::instance().log("\"zmq\":{\"event\":\"ctx_new_failed\"}");
        return;
    }

    void* sock = ::zmq_socket(ctx, ZMQ_PUB);
    if (!sock) {
        Telemetry::instance().log("\"zmq\":{\"event\":\"socket_failed\"}");
        ::zmq_ctx_term(ctx);
        return;
    }
    ::zmq_setsockopt(sock, ZMQ_SNDHWM, &CompileTime::kZMQSendHwm,
                     sizeof(int));
    ::zmq_setsockopt(sock, ZMQ_LINGER, &CompileTime::kSocketTimeoutMs,
                         sizeof(int));

    if (::zmq_bind(sock, bind_.c_str()) != 0) {
        Telemetry::instance().log(std::string("\"zmq\":{\"bind_failed\":\"") +
                                  bind_ + "\"}");
        ::zmq_close(sock);
        ::zmq_ctx_term(ctx);
        return;
    }
    Telemetry::instance().log("\"zmq_pub\":{\"bound\":\"" + bind_ + "\"}");

    std::vector<Msg> batch;
    batch.reserve(256);
    while (!shutdown_.load()) {
        drain(batch);
        if (batch.empty()) {
            // Brief sleep to avoid busy-spinning when idle.
            std::this_thread::sleep_for(std::chrono::microseconds(200));
            continue;
        }
        for (auto& m : batch) {
            std::string frame;
            switch (m.kind) {
                case ZmqPub::Payload::Tick:   frame = tick_frame(m.tick);  break;
                case ZmqPub::Payload::Broker: frame = quote_frame(m.quote); break;
                case ZmqPub::Payload::Signal: frame = std::move(m.signal);  break;
            }
            ::zmq_send(sock, frame.data(), frame.size(), ZMQ_DONTWAIT);
        }
    }

    ::zmq_close(sock);
    ::zmq_ctx_term(ctx);
    Telemetry::instance().log("\"zmq_pub\":{\"stopped\":true}");
}

}  // namespace llm