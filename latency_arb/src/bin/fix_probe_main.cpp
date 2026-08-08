// ===========================================================================
// fix_probe — standalone IC Markets cTrader FIX 4.4 smoke-test CLI.
//
// Reads config from env (FIX_HOST, FIX_PORT, FIX_SENDER_COMP_ID,
// FIX_TARGET_COMP_ID, FIX_PASSWORD, FIX_ACCOUNT_ID) and submits ONE NewOrder
// Single (35=D) market order via FixExecutor, waits for its execution report,
// then exits. Lets you verify the TLS endpoint, Logon, tag layout (99/100
// SLTP) and ExecutionReport round-trip against demo without the whole engine.
//
//   export FIX_HOST=demo-uk-eqx-01.p.c-trader.com
//   export FIX_PORT=5211
//   export FIX_SENDER_COMP_ID=demo.icmarkets.10092442
//   export FIX_TARGET_COMP_ID=cServer
//   export FIX_PASSWORD=YourBrokerPass
//   export FIX_ACCOUNT_ID=10092442
//   ./build/fix_probe buy 1 XAU/USD 2300.10 2400.50
//
// where: side=buy|sell  volume=LOTS  symbol  sl-price  tp-price
// ===========================================================================

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/fix_executor.h"

int main(int argc, char** argv) {
    if (argc != 6) {
        std::fprintf(stderr,
                     "usage: fix_probe <buy|sell> <volume-lots> <symbol> "
                     "<sl-price> <tp-price>\n");
        return 2;
    }
    const std::string side   = argv[1];
    const double volume      = std::atof(argv[2]);
    const std::string symbol = argv[3];
    const double sl          = std::atof(argv[4]);
    const double tp          = std::atof(argv[5]);

    llm::Config cfg = llm::Config::from_env();
    if (cfg.fix_password.empty() || cfg.fix_account_id.empty()) {
        std::fprintf(stderr,
                     "error: set FIX_PASSWORD and FIX_ACCOUNT_ID env vars "
                     "(FIX_HOST/FIX_PORT/SENDER/TARGET optional; defaults match "
                     "demo-uk-mach-1.p.c-trader.com:5211).\n");
        return 1;
    }
    std::printf("FIX probe -> host=%s:%s sender=%s target=%s account=%s\n",
                cfg.fix_host.c_str(), cfg.fix_port.c_str(),
                cfg.fix_sender_comp_id.c_str(), cfg.fix_target_comp_id.c_str(),
                cfg.fix_account_id.c_str());

    std::atomic<bool> got{false};
    llm::FixExecutor exec(cfg);
    exec.set_result_callback(
        [](const llm::FixExecutionResult& r, void* userdata) {
            std::printf("result: ok=%d status=%s cl=%s brokerId=%s "
                        "latency=%.2fms error=%s\n",
                        r.ok ? 1 : 0, r.status.c_str(), r.cl_ord_id.c_str(),
                        r.broker_order_id.empty() ? "-" : r.broker_order_id.c_str(),
                        r.latency_ms, r.error.c_str());
            static_cast<std::atomic<bool>*>(userdata)->store(true);
        },
        &got);

    exec.start();
    exec.execute(symbol, side, volume, sl, tp);

    // Wait for the first terminal ExecutionReport (probe is single-shot).
    for (int i = 0; i < 600 && !got.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    exec.stop();
    return got.load() ? 0 : 1;
}