#include <cstdio>
#include <cstdlib>
#include <csignal>
#include <string>

#include "llm/mt5_bridge.h"

using namespace llm;

namespace {
volatile std::sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }
}  // namespace

// mt5_bridge — pure C++20 native execution client.
//
//   mt5_bridge [SUB_BIND] [HOST] [PORT]
//
// Defaults come from env (MT5_BRIDGE_HOST / MT5_BRIDGE_PORT / ZMQ_PUB_BIND)
// mirroring the engine's ll.env. Runs until SIGINT/SIGTERM.
int main(int argc, char** argv) {
    const char* sub = std::getenv("ZMQ_PUB_BIND");
    const char* host = std::getenv("MT5_BRIDGE_HOST");
    const char* port = std::getenv("MT5_BRIDGE_PORT");
    if (argc > 1) sub  = argv[1];
    if (argc > 2) host = argv[2];
    if (argc > 3) port = argv[3];

    const std::string sub_bind  = sub  ? sub  : "ipc:///tmp/latency_arb.ipc";
    const std::string br_host   = host ? host : "127.0.0.1";
    const int         br_port   = port ? std::atoi(port) : 6161;

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    Mt5Bridge bridge(sub_bind, br_host, br_port);
    bridge.start();

    std::printf("[mt5_bridge] subscribing %s -> %s:%d\n",
                sub_bind.c_str(), br_host.c_str(), br_port);
    std::fflush(stdout);

    while (!g_stop) { /* bridge thread does the work */ }
    bridge.stop();
    return 0;
}
