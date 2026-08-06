#include "llm/fix_client.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
using SOCKET = int;
static constexpr int INVALID_SOCKET = -1;
#endif

#include "llm/telemetry.h"

namespace llm {

namespace {
constexpr char  SOH     = '\x01';
constexpr char  FIX_VER[] = "FIX.4.4";

// UTC "YYYYMMDD-HH:MM:SS.mmm" for tag 52.
std::string utc_now_str() {
    using std::chrono::system_clock;
    const auto now = system_clock::now();
    const std::time_t tt = system_clock::to_time_t(now);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
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

void field(std::string& b, int tag, const std::string& v) {
    b += std::to_string(tag); b += '='; b += v; b += SOH;
}
void field(std::string& b, int tag, long v) { field(b, tag, std::to_string(v)); }
}  // namespace

int64_t now_ms() {
    static auto const origin = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - origin).count();
}

std::string FixClient::build_fix(int msg_type, const std::string& body,
                                 const std::string& sender,
                                 const std::string& target, long seq) {    std::string hdr;
    field(hdr, 35, std::string(1, static_cast<char>(msg_type)));
    field(hdr, 49, sender);
    field(hdr, 56, target);
    field(hdr, 34, seq);
    field(hdr, 52, utc_now_str());
    // BodyLength covers everything after "9=" up to the start of "10=".
    const long blen = static_cast<long>(hdr.size() + body.size());
    std::string msg = std::string("8=") + FIX_VER + SOH + "9=" +
                      std::to_string(blen) + SOH + hdr + body;
    // Checksum over everything from the first byte through the last SOH.
    unsigned int sum = 0;
    for (unsigned char c : msg) sum += c;
    char ck[8];
    std::snprintf(ck, sizeof(ck), "%03u", sum % 256);
    msg += "10=" + std::string(ck) + SOH;
    return msg;
}

std::string FixClient::logon(long seq, const std::string& sender,
                             const std::string& target, int heartbeat_s) {
    std::string b;
    field(b, 98, "0");                                   // EncryptMethod = NONE
    field(b, 108, static_cast<long>(heartbeat_s));
    field(b, 141, "Y");                                  // ResetSeqNumFlag
    return build_fix('A', b, sender, target, seq);
}

std::string FixClient::new_order_single(const Signal& s, const Config& cfg,
                                        long seq, const std::string& sender,
                                        const std::string& target) {
    std::string b;
    field(b, 11, s.reason.empty() ? "LEADLAG" : s.reason);       // ClOrdID
    field(b, 55, s.symbol);                                       // Symbol
    field(b, 54, s.side == Side::Buy ? "1" : "2");                // Side
    field(b, 38, static_cast<long>(cfg.trade_amount > 0 ? cfg.trade_amount : 1));
    field(b, 40, "1");                                            // OrdType=Market
    field(b, 59, "3");                                            // TimeInForce=IOC
    field(b, 1, cfg.dry_run_symbol);                              // Account
    char price[48];
    std::snprintf(price, sizeof(price), "%.6f", s.lead);
    field(b, 44, price);                                          // Price
    return build_fix('D', b, sender, target, seq);
}

std::string FixClient::heartbeat(long seq, const std::string& sender,
                                 const std::string& target) {
    return build_fix('0', std::string(), sender, target, seq);
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------
FixClient::FixClient(const Config& cfg) : cfg_(cfg) {}
FixClient::~FixClient() { stop(); }

void FixClient::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    thread_ = std::thread(&FixClient::session_loop, this);
}

void FixClient::stop() {
    running_.store(false);
    { std::lock_guard<std::mutex> lk(mtx_); wake_ = true; }
    cv_.notify_all();
    close_socket();
    if (thread_.joinable()) thread_.join();
}

void FixClient::send_order(const Signal& s) {
    { std::lock_guard<std::mutex> lk(mtx_); orders_.push_back(OrderJob{s}); wake_ = true; }
    cv_.notify_one();
}

// ---------------------------------------------------------------------------
// Session loop
// ---------------------------------------------------------------------------
void FixClient::session_loop() {
    Telemetry::instance().log("\"fix\":{\"thread_started\":true}");
    while (running_.load()) {
        connected_.store(false);
        if (connect_socket(cfg_.fix_host, cfg_.fix_port)) {
            int one = 1;
#ifdef _WIN32
            setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof(one));
#else
            ::setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif
            const std::string lg =
                logon(seq_out_, cfg_.fix_sender_comp_id,
                      cfg_.fix_target_comp_id, CompileTime::kFIXHeartbeatS);
            if (send_frame(lg)) {
                connected_.store(true);
                Telemetry::instance().log(
                    "\"fix\":{\"logon_sent\":true,\"endpoint\":\"" +
                    cfg_.fix_host + ":" + cfg_.fix_port + "\"}");
                run_event_loop();
            }
        } else {
            Telemetry::instance().log_warn("\"fix\":{\"connect_failed\":\"" +
                                           cfg_.fix_host + ":" + cfg_.fix_port + "\"}");
        }
        close_socket();
        if (!running_.load()) break;
        const int backoff_s = std::max(1, cfg_.ws_reconnect_base_s);
        Telemetry::instance().log_warn("\"fix\":{\"reconnecting_in_s\":" +
                                       std::to_string(backoff_s) + "}");
        std::this_thread::sleep_for(std::chrono::seconds(backoff_s));
    }
    Telemetry::instance().log("\"fix\":{\"thread_stopped\":true}");
}

void FixClient::run_event_loop() {
    int64_t last_beat = now_ms();
    int64_t last_recv = now_ms();
    while (running_.load() && connected_.load()) {
        // Drain outbox.
        {
            std::deque<OrderJob> local;
            {
                std::lock_guard<std::mutex> lk(mtx_);
                local.swap(orders_);
                wake_ = false;
            }
            while (!local.empty()) {
                const std::string frame =
                    new_order_single(local.front().s, cfg_, seq_out_++,
                                     cfg_.fix_sender_comp_id, cfg_.fix_target_comp_id);
                if (!send_frame(frame)) { connected_.store(false); return; }
                local.pop_front();
            }
        }
        // Heartbeat on schedule.
        const int64_t ms = now_ms();
        if (ms - last_beat >= static_cast<int64_t>(CompileTime::kFIXHeartbeatS) * 1000) {
            if (!send_frame(heartbeat(seq_out_++, cfg_.fix_sender_comp_id,
                                      cfg_.fix_target_comp_id))) {
                connected_.store(false); return;
            }
            last_beat = ms;
        }
        // Peer timeout: no data for 1.5x heartbeat interval.
        if (ms - last_recv > static_cast<int64_t>(CompileTime::kFIXHeartbeatS) * 1500) {
            Telemetry::instance().log_warn("\"fix\":{\"peer_timeout\"}");
            connected_.store(false); return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

bool FixClient::send_frame(const std::string& frame) {
    if (fd_ < 0) return false;
#ifdef _WIN32
    return ::send(fd_, frame.data(), static_cast<int>(frame.size()), 0)
           == static_cast<int>(frame.size());
#else
    return ::send(fd_, frame.data(), frame.size(), MSG_NOSIGNAL)
           == static_cast<ptrdiff_t>(frame.size());
#endif
}

bool FixClient::connect_socket(const std::string& host, const std::string& portstr) {
    struct addrinfo hints, *res = nullptr;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    if (::getaddrinfo(host.c_str(), portstr.c_str(), &hints, &res) != 0) return false;

    SOCKET sock = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (sock == INVALID_SOCKET) { ::freeaddrinfo(res); return false; }
    if (::connect(sock, res->ai_addr, static_cast<socklen_t>(res->ai_addrlen)) != 0) {
#ifdef _WIN32
        closesocket(sock);
#else
        ::close(sock);
#endif
        ::freeaddrinfo(res);
        return false;
    }
    ::freeaddrinfo(res);
    fd_ = static_cast<int>(sock);
    return true;
}

void FixClient::close_socket() {
#ifdef _WIN32
    if (fd_ >= 0) closesocket(fd_);
#else
    if (fd_ >= 0) ::close(fd_);
#endif
    fd_ = -1;
    connected_.store(false);
}

}  // namespace llm