#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <string>
#include <thread>

#ifdef __linux__
#include <sched.h>
#endif

#include "llm/aggregator.h"
#include "llm/binance_ws.h"
#include "llm/broker_sub.h"
#include "llm/config.h"
#include "llm/deribit_ws.h"
#include "llm/execution.h"
#include "llm/fix_client.h"
#include "llm/ring.h"
#include "llm/strategy.h"
#include "llm/telemetry.h"
#include "llm/tick.h"
#include "llm/zmq_pub.h"

using namespace llm;

namespace {

std::atomic<bool> g_shutdown{false};

void on_signal(int) { g_shutdown.store(true); }

// Pin the calling thread to CPU `cpu` if >= 0 (env-configurable). Best-effort.
void pin_cpu(int cpu) {
#ifdef __linux__
    if (cpu < 0) return;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    ::sched_setaffinity(0, sizeof(set), &set);
#endif
}

int cpu_env(const char* name) {
    const char* v = std::getenv(name);
    return (v && v[0]) ? std::atoi(v) : -1;
}

// 5 dedicated roles, one OS thread each:
//   1) Binance reader  -> binance_ring   (own io_context thread)
//   2) Deribit reader  -> deribit_ring   (own io_context thread)
//   3) Broker reader   -> broker_ring    (ZMQ SUB thread)
//   4) Math/Strategy   -> consumes all three rings (single consumer)
//   5) Execution       -> ZMQ PUB + FIX  (dispatcher worker thread)
struct Engine {
    Engine(const Config& cfg)
        : cfg(cfg),
          aggregator(cfg),
          strategy(cfg),
          zmq_pub(cfg.zmq_pub_bind),
          fix(cfg),
          exec(cfg, zmq_pub, fix),
          binance(cfg, binance_ring),
          deribit(cfg, deribit_ring),
          broker(cfg, broker_ring) {}

    const Config& cfg;

    TickRing                     binance_ring;
    TickRing                     deribit_ring;
    SpscRing<BrokerQuote, 8192>  broker_ring;

    Aggregator  aggregator;
    Strategy    strategy;
    ZmqPub      zmq_pub;
    FixClient   fix;
    ExecutionDispatcher exec;

    BinanceWs binance;
    DeribitWs deribit;
    BrokerSub broker;
};

// Strategy / math worker — single consumer of the three lock-free SPSC rings.
void strategy_loop(Engine& e) {
    pin_cpu(cpu_env("CPU_STRATEGY"));
    Telemetry::instance().log("\"thread\":{\"role\":\"strategy\",\"started\":true}");
    while (!g_shutdown.load()) {
        const int64_t now = now_ms();
        auto lead = e.aggregator.update(e.binance_ring, e.deribit_ring, now);
        if (lead.has_value()) {
            BrokerQuote q;
            if (e.broker_ring.pop(q) && q.is_valid()) {
                double th = 0.0;
                const Decision d = e.strategy.evaluate(*lead, q, now, th);
                if (auto sig = e.strategy.maybe_emit(d, *lead, q, now)) {
                    e.exec.submit(*sig);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::microseconds(100));  // ~10kHz
    }
    Telemetry::instance().log("\"thread\":{\"role\":\"strategy\",\"stopped\":true}");
}

}  // namespace

int main() {
    Config cfg = Config::from_env();

    // ---- telemetry (mmap logging) ------------------------------------- //
    Telemetry::instance().configure(cfg.log_dir, cfg.mmap_log_enabled);
    Telemetry::instance().preallocate_mmap(cfg.mmap_prealloc_mb);   // 1MB now
    Telemetry::instance().start();

    Telemetry::instance().log(
        std::string("\"engine\":{\"version\":\"") + CompileTime::kVersion +
        "\",\"dry_run\":" + (cfg.dry_run ? "true" : "false") +
        ",\"fix_enabled\":" + (cfg.fix_enabled ? "true" : "false") + "}");

    // Pre-allocate the big mmap region (10MB) just before engine threads spin.
    Telemetry::instance().preallocate_mmap(cfg.mmap_prealloc_big_mb);

    // Pre-allocate the lock-free nanosecond benchmark ring (Tick->Trade path).
    Telemetry::instance().enable_benchmark_ring();

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    Engine engine(cfg);

    // ---- start the five workers -------------------------------------- //
    // 1) Binance reader (own io_context thread inside start())
    engine.binance.start();
    // 2) Deribit reader
    engine.deribit.start();
    // 3) Broker ZMQ subscriber
    engine.broker.start();
    // 4) ZMQ publisher + FIX session threads
    engine.zmq_pub.start();
    engine.fix.start();
    // 5) Execution dispatcher
    engine.exec.start();

    // Pin the exec dispatcher to its own core before the strategy starts.
    pin_cpu(cpu_env("CPU_EXEC"));

    // Strategy worker (owner of the rings' consumer side).
    std::thread t_strategy(strategy_loop, std::ref(engine));

    Telemetry::instance().log("\"engine\":{\"all_threads_started\":true}");

    // ---- wait for shutdown -------------------------------------------- //
    while (!g_shutdown.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // ---- graceful stop (reverse order) -------------------------------- //
    engine.binance.stop();
    engine.deribit.stop();
    engine.broker.stop();
    if (t_strategy.joinable()) t_strategy.join();
    engine.exec.stop();
    engine.fix.stop();
    engine.zmq_pub.stop();

    Telemetry::instance().stop();
    return 0;
}