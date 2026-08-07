// ===========================================================================
// oanda_probe — standalone OANDA v2 smoke-test CLI.
//
// Reads config from env (OANDA_TOKEN, OANDA_ACCOUNT_ID, OANDA_HOST,
// OANDA_INSTRUMENT) and submits ONE market order via OandaExecutor, then
// exits. Lets you verify the endpoint, auth header, and JSON payload against
// OANDA's demo API without running the whole engine.
//
//   export OANDA_TOKEN=...
//   export OANDA_ACCOUNT_ID=...
//   export OANDA_INSTRUMENT=XAU_USD
//   ./build/oanda_probe buy 10 0.15 0.35
//
// where: side=buy|sell  units=10  sl-distance=0.15  tp-distance=0.35
// ===========================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/oanda_executor.h"

int main(int argc, char** argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: oanda_probe <buy|sell> <units> "
                     "<sl-distance> <tp-distance>\n");
        return 2;
    }
    const std::string side = argv[1];
    const double units = std::atof(argv[2]);
    const double sl    = std::atof(argv[3]);
    const double tp    = std::atof(argv[4]);

    llm::Config cfg = llm::Config::from_env();
    if (cfg.oanda_token.empty() || cfg.oanda_account_id.empty()) {
        std::fprintf(stderr,
                     "error: set OANDA_TOKEN and OANDA_ACCOUNT_ID env vars "
                     "(OANDA_HOST optional, defaults to fxpractice demo).\n");
        return 1;
    }
    std::printf("OANDA probe -> host=%s account=%s instrument=%s\n",
                cfg.oanda_host.c_str(), cfg.oanda_account_id.c_str(),
                cfg.oanda_instrument.c_str());

    std::atomic<bool> got{false};
    llm::OandaExecutor exec(cfg);
    exec.set_result_callback(
        [](const llm::OandaExecutionResult& r, void* userdata) {
            std::printf("result: ok=%d http=%d id=%s latency=%.2fms %s\n",
                        r.ok ? 1 : 0, r.http_status,
                        r.transaction_id.empty() ? "-" : r.transaction_id.c_str(),
                        r.latency_ms, r.error.c_str());
            static_cast<std::atomic<bool>*>(userdata)->store(true);
        },
        &got);

    exec.start();
    exec.execute(cfg.oanda_instrument, side, units, sl, tp);

    // Wait for the callback (single-shot probe).
    for (int i = 0; i < 200 && !got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    exec.stop();
    return got.load() ? 0 : 1;
}
