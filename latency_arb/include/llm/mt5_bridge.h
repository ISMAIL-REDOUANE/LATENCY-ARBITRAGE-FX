#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

namespace llm {

// ---------------------------------------------------------------------------
// Binary POD signal frame — fixed layout, zero dynamic allocation.
//
// Produced by Mt5Bridge::parse_signal() from the engine's ZMQ "signal|"
// frames and consumed directly by the native execution path. The struct is
// trivially copyable and size-stable so it can cross a socket/DLL boundary
// without serialization.
// ---------------------------------------------------------------------------
#pragma pack(push, 1)
struct ExecSignal {
    int64_t  ts_ms;        // decision timestamp (epoch ms)
    double   lead;         // composite lead price
    double   broker_bid;   // broker quote that triggered
    double   broker_ask;
    double   threshold;    // dynamic threshold used
    double   edge;         // lead-vs-broker edge in threshold units
    uint8_t  side;         // 1 = buy, 2 = sell
    uint8_t  valid;        // 0 = invalid, 1 = valid
    char     reason[24];   // null-terminated, e.g. "lead_gt_ask"
    char     symbol[16];   // null-terminated, e.g. "BTC/USD"
};
#pragma pack(pop)

// Native C++ execution client (Windows target where MT5 runs; Linux builds
// provide the parser + a null executor so the unit tests run everywhere).
//
// Pipeline:
//   1. Subscribes to the engine's ZMQ PUB IPC channel (ZMQ_PUB_BIND).
//   2. Parses "signal|..." frames into fixed ExecSignal PODs in place, with
//      zero heap allocation on the hot path.
//   3. Routes each order through one of two zero-latency paths:
//        a. MT5 terminal DLL binding (LoadLibrary of the MT5 API bridge DLL),
//           IOC fill execution at the market price.
//        b. Direct TCP socket to a bridge EA running inside the terminal.
//
// Threading: one subscriber thread owns the ZMQ socket; execution happens
// inline on that thread so there is exactly one hop between signal and order.
class Mt5Bridge {
public:
    // sub_bind  : ZMQ endpoint to subscribe (must match engine ZMQ_PUB_BIND).
    // bridge_host/port : MT5 bridge EA endpoint (direct-socket fallback).
    Mt5Bridge(std::string sub_bind, std::string bridge_host, int bridge_port);
    ~Mt5Bridge();

    Mt5Bridge(const Mt5Bridge&) = delete;
    Mt5Bridge& operator=(const Mt5Bridge&) = delete;

    void start();
    void stop();

    // Exposed for tests: parses one wire frame into a POD (pure, no I/O).
    static bool parse_signal(const std::string& frame, ExecSignal& out);

private:
    void run();

    // Execution backends.
    bool try_dll_execute(const ExecSignal& s);   // MT5 API DLL (LoadLibrary)
    bool try_socket_execute(const ExecSignal& s); // direct TCP to EA

    // Raw TCP write (non-blocking socket, TCP_NODELAY).
    bool send_pod(int fd, const ExecSignal& s);

    std::string sub_bind_;
    std::string bridge_host_;
    int         bridge_port_;

    std::thread   thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
};

}  // namespace llm