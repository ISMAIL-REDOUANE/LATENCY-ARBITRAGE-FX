// ===========================================================================
// ctrader_probe — standalone cTrader Open API (IC Markets) smoke-test CLI.
//
// Reads config from env (CTRADER_CLIENT_ID, CTRADER_CLIENT_SECRET,
// CTRADER_ACCOUNT_ID, CTRADER_ACCESS_TOKEN, CTRADER_HOST, CTRADER_PORT,
// CTRADER_SYMBOL) and submits ONE market order via CTraderExecutor, then
// exits. Lets you verify the TLS endpoint, app+account auth, symbol lookup,
// spot subscription and order round-trip against IC demo without running the
// whole engine.
//
//   export CTRADER_CLIENT_ID=...
//   export CTRADER_CLIENT_SECRET=...
//   export CTRADER_ACCOUNT_ID=...
//   export CTRADER_ACCESS_TOKEN=...
//   export CTRADER_SYMBOL=XAU_USD
//   ./build/ctrader_probe buy 1 2 4
//
// where: side=buy|sell  volume=LOTS  sl-distance=PIPs  tp-distance=PIPs
// ===========================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/ctrader_executor.h"

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: ctrader_probe <buy|sell> <volume-lots> "
                     "<sl-pips> <tp-pips>\n");
        return 2;
    }
    const std::string side  = argv[1];
    const double volume     = std::atof(argv[2]);
    const double sl         = std::atof(argv[3]);
    const double tp         = std::atof(argv[4]);

    llm::Config cfg = llm::Config::from_env();
    if (cfg.ctrader_client_id.empty() || cfg.ctrader_client_secret.empty() ||
        cfg.ctrader_account_id.empty() || cfg.ctrader_access_token.empty()) {
        std::fprintf(stderr,
                     "error: set CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET / "
                     "CTRADER_ACCOUNT_ID / CTRADER_ACCESS_TOKEN env vars "
                     "(CTRADER_HOST/PORT optional, default demo "
                     "openapi.ctrader.com:5030).\n");
        return 1;
    }
    // cTrader symbol names use '/' (e.g. "XAU/USD"); normalize a probe arg
    // like "XAU_USD" into the form the broker regex actually matches.
    std::string symbol = cfg.ctrader_symbol;
    if (symbol.empty()) symbol = "XAU/USD";
    else for (auto& c : symbol) if (c == '_') c = '/';
    std::printf("cTrader probe -> host=%s:%s account=%s symbol=%s\n",
                cfg.ctrader_host.empty() ? "openapi.ctrader.com" : cfg.ctrader_host.c_str(),
                cfg.ctrader_port.empty() ? "5030" : cfg.ctrader_port.c_str(),
                cfg.ctrader_account_id.c_str(), symbol.c_str());

    std::atomic<bool> got{false};
    llm::CTraderExecutor exec(cfg);
    exec.set_result_callback(
        [](const llm::CTraderExecutionResult& r, void* userdata) {
            std::printf("result: ok=%d orderId=%lld posId=%lld latency=%.2fms "
                        "status=%s error=%s\n",
                        r.ok ? 1 : 0,
                        static_cast<long long>(r.order_id),
                        static_cast<long long>(r.position_id),
                        r.latency_ms, r.status.c_str(), r.error.c_str());
            static_cast<std::atomic<bool>*>(userdata)->store(true);
        },
        &got);

    exec.start();
    exec.execute(symbol, side, volume, sl, tp);

    // Wait for the callback (single-shot probe).
    for (int i = 0; i < 400 && !got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    exec.stop();
    return got.load() ? 0 : 1;
}