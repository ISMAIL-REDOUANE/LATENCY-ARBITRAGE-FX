#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#if defined(_MSC_VER) || defined(__MINGW32__)
#include <intrin.h>
#elif defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace llm {

// ---------------------------------------------------------------------------
// Nanosecond clocks
//
// x86 TSC (rdtsc) is the lowest-overhead timestamp source on the hot path and
// gives sub-nanosecond resolution; std::chrono::steady_clock is a portable
// wall-time fallback used for calibration and non-x86 builds. All timestamp
// helpers here are header-only, inline, and allocation-free.
// ---------------------------------------------------------------------------
inline uint64_t rdtsc() noexcept {
#if (defined(__x86_64__) || defined(__i386__) || defined(_M_X64) || defined(_M_IX86))
    unsigned int aux;
#if defined(_MSC_VER) || defined(__MINGW32__)
    return static_cast<uint64_t>(_rdtscp(&aux));
#elif defined(__GNUC__)
    // GCC/Clang: __rdtscp comes from <x86intrin.h>; falls back to raw RDTSC
    // (via inline asm) if the intrinsic is unavailable for the target.
#if defined(__has_include) && __has_include(<x86intrin.h>)
    return static_cast<uint64_t>(__rdtscp(&aux));
#else
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi) : : "memory");
    return (static_cast<uint64_t>(hi) << 32) | lo;
#endif
#else
    return static_cast<uint64_t>(__rdtscp(&aux));
#endif
#else
    return static_cast<uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
#endif
}

// Steady monotonic clock in nanoseconds (portable; calibrated against TSC).
inline int64_t steady_ns() noexcept {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// ---------------------------------------------------------------------------
// Lock-free, zero-alloc latency sample ring
//
// Fixed-layout sample stamped on the Tick-to-Trade hot path. The ring is a
// pre-allocated power-of-two buffer of these PODs; producers reserve a slot
// with a single relaxed atomic fetch-add and publish it with a release store.
// No mutex, no syscall, and no heap activity is ever touched after the region
// is mapped up front.
// ---------------------------------------------------------------------------
struct LatencySample {
    int64_t  ts_ns;    // steady_ns() at stamp time
    uint64_t tsc;      // raw rdtsc at stamp time
    uint32_t stage;    // pipeline stage id (see kStage*)
    uint32_t seq;      // global monotonic sequence
    std::atomic<uint8_t> valid;  // 1 after the release-store publish
};
static_assert(sizeof(LatencySample) == 32,
              "LatencySample must stay a fixed 32-byte POD");

// Pipeline stage ids stamped on the Tick->Trade path.
enum BenchStage : uint32_t {
    kStageTickRx      = 1,  // WS reader received a market tick
    kStageAggregate   = 2,  // composite lead price computed
    kStageStrategy    = 3,  // signal decision produced
    kStageSignalTx    = 4,  // signal published to ZMQ / MT5
    kStageExecSend    = 5,  // execution order dispatched
};

// ---------------------------------------------------------------------------
// Async telemetry logger + nanosecond benchmark module.
//
// Logging path (unchanged contract):
//   * log()/log_warn()/log_error() format + enqueue and return — the producer
//     never blocks on I/O; a dedicated writer thread drains the queue.
//   * MMAP logging: MMAP_LOG_DIR=1 grows a file and maps it, so the writer
//     only memcpy's into mapped memory (no fsync per line). Pre-allocation
//     steps: MMAP_PREALLOC_MB (1MB) at configure, MMAP_PREALLOC_BIG_MB (10MB)
//     just before engine threads spin up.
//
// Benchmark path (new):
//   * enable_benchmark_ring(bytes) pre-allocates (mmap on POSIX, VirtualAlloc
//     on Windows) a lock-free ring of LatencySample PODs. Call once before
//     start()/threads.
//   * bench_mark(stage) is the hot-path stamp: fetch-add + release-store,
//     zero allocation, callable from any producer thread.
//   * bench_stats() computes min/avg/max + p50/p99 over the most recent
//     inter-sample deltas directly from the ring — lock-free, zero-alloc.
// ---------------------------------------------------------------------------
class Telemetry {
public:
    enum Level : uint8_t { L_INFO, L_WARN, L_ERROR, L_CRITICAL };

    static Telemetry& instance();

    void configure(const std::string& dir, bool mmap_enabled);
    void start();   // spawn writer thread + mmap prealloc
    void stop();    // drain + join

    // Non-blocking producers.
    void log(const std::string& line);
    void log_warn(const std::string& line);
    void log_error(const std::string& line);

    // Pre-allocate the mmap region. Call before threads start (see spec).
    void preallocate_mmap(int mb);

    // ---- nanosecond benchmark module ------------------------------------- //
    // Allocate + map the lock-free latency ring. bytes must be a power of two
    // (or 0 to use the default 1 MiB). Safe to call before start(). Returns
    // false on allocation failure. Never deallocates after enable.
    bool enable_benchmark_ring(std::size_t bytes = 0);

    // Hot-path stamp: records one LatencySample at `stage`. Lock-free and
    // allocation-free. No-op until enable_benchmark_ring() succeeds.
    void bench_mark(uint32_t stage) noexcept;

    // Statistics over the most recent samples collected since enable. Reads
    // the ring directly (lock-free, no heap): walks newest->oldest, computes
    // inter-sample deltas, and returns min/avg/max + p50/p99. `window` caps
    // how many samples are considered (default 8192; bounded by the ring).
    struct BenchStats {
        uint64_t samples = 0;   // deltas examined
        double   avg_dt_ns = 0;
        double   min_dt_ns = 0;
        double   max_dt_ns = 0;
        double   p50_dt_ns = 0;
        double   p99_dt_ns = 0;
        uint64_t first_seq = 0;
        uint64_t last_seq  = 0;
        double   tsc_hz    = 0; // measured rdtsc frequency (calibrated once)
    };
    BenchStats bench_stats(std::size_t window = 8192) const noexcept;

    bool bench_enabled() const noexcept { return ring_base_ != nullptr; }

private:
    Telemetry() = default;
    ~Telemetry();

    void push(std::string line);
    void writer_loop();
    void grow_mmap(int mb);
    void write_line(const std::string& line);

    // Benchmark ring helpers.
    bool   alloc_bench_ring(std::size_t bytes);
    double calibrate_tsc_hz() noexcept;

    struct MapState {
        void*  base = nullptr;
        size_t len  = 0;
        size_t used = 0;
        int    fd   = -1;
    };

    std::string log_name_;
    MapState map_;

    std::mutex              mtx_;
    std::condition_variable cv_;
    std::deque<std::string> queue_;
    std::thread             writer_;
    std::atomic<bool>       running_{false};
    bool                    enabled_ = false;
    bool                    mmap_    = false;
    std::string             dir_     = ".";
    std::string             cur_log_name_;
    FILE*                   log_file_ = nullptr;

    // ---- lock-free latency ring state ------------------------------------ //
    static constexpr std::size_t kDefaultRingBytes = 1u << 20;    // 1 MiB
    static constexpr std::size_t kRingCapacity =
        kDefaultRingBytes / sizeof(LatencySample);                // 32768

    std::atomic<std::size_t> ring_head_{0};   // next slot index to publish
    std::atomic<std::size_t> ring_pub_{0};    // number of publishes (monotonic)
    std::atomic<uint64_t>    ring_seq_{0};    // global sequence
    LatencySample*           ring_base_ = nullptr;
    std::size_t              ring_mask_ = 0;  // power-of-two mask
    std::atomic<double>      ring_tsc_hz_{0};
};

}  // namespace llm
