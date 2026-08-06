#include "llm/mt5_bridge.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <utility>

#include <zmq.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
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
static constexpr int SOCKET_ERROR   = -1;
#endif

namespace llm {

// ===========================================================================
// Wire parser — pure, allocation-free.
// Engine frame (see src/execution.cpp):
//   signal|BUY|SELL,reason,lead,bid,ask,threshold,edge,ts_ms
// ===========================================================================
bool Mt5Bridge::parse_signal(const std::string& frame, ExecSignal& out) {
    std::memset(&out, 0, sizeof(out));
    out.valid = 0;

    const char* p = frame.c_str();
    // Skip topic / leading pipe.
    if (std::strncmp(p, "signal", 6) == 0) p += 6;
    if (*p == '|') ++p;

    // ---- side ---------------------------------------------------------- //
    if (std::strncmp(p, "BUY", 3) == 0) { out.side = 1; p += 3; }
    else if (std::strncmp(p, "SELL", 4) == 0) { out.side = 2; p += 4; }
    else return false;
    if (*p != ',') return false;
    ++p;

    // ---- reason --------------------------------------------------------- //
    char reason[24];
    std::size_t i = 0;
    while (*p && *p != ',' && i < sizeof(reason) - 1) reason[i++] = *p++;
    reason[i] = '\0';
    if (*p != ',') return false;
    ++p;
    std::memcpy(out.reason, reason, i + 1);

    // ---- numeric fields -------------------------------------------------- //
    auto next_num = [&](double& v) -> bool {
        char num[48];
        std::size_t n = 0;
        while (*p && *p != ',' && n < sizeof(num) - 1) num[n++] = *p++;
        num[n] = '\0';
        if (*p != ',') return false;
        ++p;
        v = std::strtod(num, nullptr);
        return true;
    };

    if (!next_num(out.lead))        return false;
    if (!next_num(out.broker_bid))  return false;
    if (!next_num(out.broker_ask))  return false;
    if (!next_num(out.threshold))   return false;
    if (!next_num(out.edge))        return false;

    // ---- ts_ms (last field, no trailing comma) -------------------------- //
    {
        char num[32];
        std::size_t n = 0;
        while (*p && *p != ',' && n < sizeof(num) - 1) num[n++] = *p++;
        num[n] = '\0';
        out.ts_ms = std::atoll(num);
    }

    std::memcpy(out.symbol, "BTC/USD", 8);
    out.valid = 1;
    return out.side != 0 && out.lead > 0.0;
}

// ===========================================================================
// Lifecycle
// ===========================================================================
Mt5Bridge::Mt5Bridge(std::string sub_bind, std::string bridge_host,
                     int bridge_port)
    : sub_bind_(std::move(sub_bind)),
      bridge_host_(std::move(bridge_host)),
      bridge_port_(bridge_port) {}

Mt5Bridge::~Mt5Bridge() { stop(); }

void Mt5Bridge::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) return;
    stopped_.store(false);
    thread_ = std::thread(&Mt5Bridge::run, this);
}

void Mt5Bridge::stop() {
    stopped_.store(true);
    if (thread_.joinable()) thread_.join();
    running_.store(false);
}

// ===========================================================================
// Subscriber loop
// ===========================================================================
void Mt5Bridge::run() {
    void* ctx = ::zmq_ctx_new();
    if (!ctx) return;
    void* sock = ::zmq_socket(ctx, ZMQ_SUB);
    if (!sock) { ::zmq_ctx_term(ctx); return; }

    int timeout = 100;
    ::zmq_setsockopt(sock, ZMQ_RCVTIMEO, &timeout, sizeof(int));
    ::zmq_setsockopt(sock, ZMQ_SUBSCRIBE, "signal", 6);   // topic prefix

    if (::zmq_connect(sock, sub_bind_.c_str()) != 0) {
        ::zmq_close(sock);
        ::zmq_ctx_term(ctx);
        return;
    }

    char buf[1024];
    while (running_.load() && !stopped_.load()) {
        const int n = ::zmq_recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) continue;   // timeout / transient

        ExecSignal sig;
        std::string frame(buf, static_cast<std::size_t>(n));
        if (!parse_signal(frame, sig)) continue;

        // Zero-latency routing: DLL binding first, direct socket fallback.
        if (!try_dll_execute(sig)) try_socket_execute(sig);
    }

    ::zmq_close(sock);
    ::zmq_ctx_term(ctx);
    running_.store(false);
}

// ===========================================================================
// Execution backend 1: MT5 terminal DLL bridge (Windows).
// Loads the MT5 API bridge DLL and invokes its IOC order entry with the POD.
// The exact exported symbol names must match the bridge DLL built for your
// terminal build (MT5API64.dll exposes the Manager API; a thin IOC bridge
// DLL is the usual deployment). Defaults to the Manager API entry points.
// ===========================================================================
bool Mt5Bridge::try_dll_execute(const ExecSignal& s) {
#ifdef _WIN32
    typedef void* (__stdcall* ConnectFn)(const char*, unsigned int);
    typedef unsigned int (__stdcall* LoginFn)(void*, const char*);
    typedef int (__stdcall* SendOrderFn)(void*, const ExecSignal*);
    typedef void (__stdcall* DisconnectFn)(void*);

    static HMODULE dll = ::LoadLibraryA("MT5API64.dll");
    if (!dll) dll = ::LoadLibraryA("MT5API.dll");
    if (!dll) return false;   // no terminal DLL present — fall back

    auto connect  = reinterpret_cast<ConnectFn>(::GetProcAddress(dll, "ManagerConnect"));
    auto login    = reinterpret_cast<LoginFn>(::GetProcAddress(dll, "ManagerLogin"));
    auto send     = reinterpret_cast<SendOrderFn>(::GetProcAddress(dll, "MTSendOrder"));
    auto disc     = reinterpret_cast<DisconnectFn>(::GetProcAddress(dll, "ManagerDisconnect"));
    if (!connect || !login || !send || !disc) return false;

    void* api = connect("127.0.0.1", 0);   // local terminal connection
    if (!api) return false;
    if (login(api, "") != 0) { disc(api); return false; }
    const int rc = send(api, &s);
    disc(api);
    return rc == 0;
#else
    (void)s;
    return false;   // no MT5 on Linux — caller falls through to socket
#endif
}

// ===========================================================================
// Execution backend 2: direct TCP socket to a bridge EA (cross-platform).
// Sends the fixed-layout ExecSignal POD (raw bytes) over a TCP_NODELAY
// socket. The EA inside MT5 orders with IOC at the market price.
// ===========================================================================
bool Mt5Bridge::send_pod(int fd, const ExecSignal& s) {
#ifdef _WIN32
    return ::send(fd, reinterpret_cast<const char*>(&s), sizeof(s), 0)
           == static_cast<int>(sizeof(s));
#else
    return ::send(fd, reinterpret_cast<const char*>(&s), sizeof(s), MSG_NOSIGNAL)
           == static_cast<ptrdiff_t>(sizeof(s));
#endif
}

bool Mt5Bridge::try_socket_execute(const ExecSignal& s) {
#ifdef _WIN32
    WSADATA wsa;
    ::WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
    struct addrinfo hints, *res = nullptr;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    const std::string port = std::to_string(bridge_port_);
    if (::getaddrinfo(bridge_host_.c_str(), port.c_str(), &hints, &res) != 0)
        return false;

    SOCKET fd = ::socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd == INVALID_SOCKET) { ::freeaddrinfo(res); return false; }

    int one = 1;
#ifdef _WIN32
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char*)&one, sizeof(one));
#else
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif

    const bool ok = ::connect(fd, res->ai_addr,
                              static_cast<socklen_t>(res->ai_addrlen)) == 0
                    && send_pod(fd, s);
    ::freeaddrinfo(res);
#ifdef _WIN32
    ::closesocket(fd);
#else
    ::close(fd);
#endif
    return ok;
}

}  // namespace llm