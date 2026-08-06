#include "llm/telemetry.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <thread>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace llm {

Telemetry& Telemetry::instance() {
    static Telemetry t;
    return t;
}

Telemetry::~Telemetry() { stop(); }

void Telemetry::configure(const std::string& dir, bool mmap_enabled) {
    std::lock_guard<std::mutex> lk(mtx_);
    dir_     = dir.empty() ? "." : dir;
    mmap_    = mmap_enabled;
    enabled_ = true;
}

void Telemetry::start() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        if (running_.load()) return;
        running_.store(true);
    }
    writer_ = std::thread(&Telemetry::writer_loop, this);
}

void Telemetry::stop() {
    running_.store(false);
    cv_.notify_all();
    if (writer_.joinable()) writer_.join();
}

void Telemetry::log(const std::string& line)      { push(line); }
void Telemetry::log_warn(const std::string& line) { push("WARN  " + line); }
void Telemetry::log_error(const std::string& line){ push("ERROR " + line); }

void Telemetry::push(std::string line) {
    auto now = std::chrono::system_clock::now();
    auto tt  = std::chrono::system_clock::to_time_t(now);
    auto ms  = std::chrono::duration_cast<std::chrono::milliseconds>(
                   now.time_since_epoch()).count() % 1000;
    std::tm tmv{};
#ifdef _WIN32
    localtime_s(&tmv, &tt);
#else
    localtime_r(&tt, &tmv);
#endif
    char ts[40];
    std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);
    std::snprintf(ts + std::strlen(ts), sizeof(ts) - std::strlen(ts),
                  ".%03d", static_cast<int>(ms));
    std::string buf = std::string("[") + ts + "] " + line;

    std::lock_guard<std::mutex> lk(mtx_);
    queue_.push_back(std::move(buf));
    cv_.notify_one();
}

void Telemetry::preallocate_mmap(int mb) {
    if (!mmap_) return;
    std::lock_guard<std::mutex> lk(mtx_);
    grow_mmap(mb);
}

void Telemetry::writer_loop() {
    // MMAP: reserve the log region up front so the writer only memcpy's into
    // mapped memory (no fsync / syscall on each line).
    if (mmap_) grow_mmap((int)0);
    for (;;) {
        std::string line;
        {
            std::unique_lock<std::mutex> lk(mtx_);
            cv_.wait(lk, [&] { return !running_.load() || !queue_.empty(); });
            if (!running_.load() && queue_.empty()) break;
            line = std::move(queue_.front());
            queue_.pop_front();
        }
        write_line(line);
    }
    std::lock_guard<std::mutex> lk(mtx_);
    if (map_.fd >= 0) {
#ifdef _WIN32
        _close(map_.fd);
#else
        ::munmap(map_.base, map_.len);
        ::close(map_.fd);
#endif
        map_.base = nullptr;
        map_.fd   = -1;
    }
}

void Telemetry::write_line(const std::string& line) {
#if defined(_WIN32)
    // No mmap on this path for the Windows dev build; fall back to fwrite.
    if (FILE* f = log_file_) { std::fwrite(line.data(), 1, line.size(), f); }
#else
    if (mmap_ && map_.base) {
        if (map_.used + line.size() + 1 <= map_.len) {
            std::memcpy(static_cast<char*>(map_.base) + map_.used,
                        line.data(), line.size());
            map_.used += line.size();
            static_cast<char*>(map_.base)[map_.used++] = '\n';
        }
    } else if (FILE* f = log_file_) {
        std::fwrite(line.data(), 1, line.size(), f);
        std::fputc('\n', f);
    }
#endif
    (void)line;
}

void Telemetry::grow_mmap(int mb) {
#if !defined(_WIN32)
    if (!mmap_) return;
    if (map_.base) return;  // already mapped

    const size_t want = static_cast<size_t>(mb > 0 ? mb : 1) * 1024 * 1024;
    const std::string path = dir_ + "/lead_lag_mmap.log";
    int fd = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) return;
    if (::ftruncate(fd, static_cast<off_t>(want)) != 0) {
        ::close(fd);
        return;
    }
    void* p = ::mmap(nullptr, want, PROT_READ | PROT_WRITE,
                     MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        ::close(fd);
        return;
    }
    map_.base  = p;
    map_.len   = want;
    map_.used  = 0;
    map_.fd    = fd;
#else
    (void)mb;
#endif
}

// ---------------------------------------------------------------------------
// Nanosecond benchmark module: lock-free, zero-allocation latency ring.
// ---------------------------------------------------------------------------

double Telemetry::calibrate_tsc_hz() noexcept {
    // Measure rdtsc frequency using steady_ns over a 50ms window.
    const uint64_t t0 = rdtsc();
    const int64_t  n0 = steady_ns();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const uint64_t t1 = rdtsc();
    const int64_t  n1 = steady_ns();
    const int64_t  dn = n1 - n0;
    if (dn <= 0) return 0.0;
    return static_cast<double>(t1 - t0) * 1e9 / static_cast<double>(dn);
}

bool Telemetry::alloc_bench_ring(std::size_t bytes) {
    if (ring_base_) return true;
    if (bytes == 0) bytes = kDefaultRingBytes;
    // Round down to the largest power of two <= bytes so indexing is a mask.
    std::size_t cap = 1;
    while ((cap << 1) <= bytes) cap <<= 1;
    const std::size_t byte_len = cap * sizeof(LatencySample);

    LatencySample* p = nullptr;
#ifdef _WIN32
    p = static_cast<LatencySample*>(VirtualAlloc(
        nullptr, byte_len, MEM_COMMIT | MEM_RESERVE, PAGE_READWRITE));
    if (!p) return false;
#else
    void* m = ::mmap(nullptr, byte_len, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (m == MAP_FAILED) return false;
    p = static_cast<LatencySample*>(m);
#endif

    ring_base_ = p;
    ring_mask_ = cap - 1;
    ring_head_.store(0);
    ring_pub_.store(0);
    ring_seq_.store(0);
    ring_tsc_hz_.store(calibrate_tsc_hz());
    return true;
}

bool Telemetry::enable_benchmark_ring(std::size_t bytes) {
    std::lock_guard<std::mutex> lk(mtx_);
    return alloc_bench_ring(bytes);
}

void Telemetry::bench_mark(uint32_t stage) noexcept {
    LatencySample* base = ring_base_;
    if (!base) return;

    const std::size_t idx = ring_head_.fetch_add(1, std::memory_order_relaxed) & ring_mask_;
    const int64_t now_ns = steady_ns();
    const uint64_t tsc   = rdtsc();
    const uint64_t seq   = ring_seq_.fetch_add(1, std::memory_order_relaxed);

    LatencySample& s = base[idx];
    s.ts_ns = now_ns;
    s.tsc   = tsc;
    s.stage = stage;
    s.seq   = static_cast<uint32_t>(seq);
    // Publish: ensure the sample fields above are visible before valid.
    s.valid.store(1, std::memory_order_release);
    ring_pub_.fetch_add(1, std::memory_order_relaxed);
}

Telemetry::BenchStats Telemetry::bench_stats(std::size_t window) const noexcept {
    BenchStats st{};
    const LatencySample* base = ring_base_;
    if (!base) return st;

    const std::size_t pub = ring_pub_.load(std::memory_order_acquire);
    st.samples = pub;
    st.tsc_hz  = ring_tsc_hz_.load();
    if (pub == 0) return st;

    // Collect the most recent deltas into a fixed stack window. This is NOT on
    // the hot path, but keeps the whole benchmark module heap-allocation-free.
    static constexpr std::size_t kMaxWindow = 4096;
    const std::size_t count = std::min({pub, window, kMaxWindow});
    double dt[kMaxWindow];
    std::size_t n = 0;

    double sum = 0.0, mn = 1e18, mx = -1e18;
    int64_t prev_ts = 0;
    uint64_t first_seq = 0, last_seq = 0;

    // Walk newest -> oldest, tracking inter-sample deltas.
    // `pub` samples were written sequentially; slot i holds the (i+1)-th
    // publish. Read newest first so we naturally stop at the window.
    for (std::size_t k = 0; k < count; ++k) {
        const std::size_t slot = (pub - 1 - k) & ring_mask_;
        const LatencySample& s = base[slot];
        if (!s.valid.load(std::memory_order_acquire)) continue;
        if (prev_ts == 0) {
            prev_ts = s.ts_ns;
            last_seq = s.seq;
            first_seq = s.seq;
            continue;
        }
        const int64_t delta = prev_ts - s.ts_ns;
        if (delta >= 0) {
            const double d = static_cast<double>(delta);
            dt[n++] = d;
            sum += d;
            if (d < mn) mn = d;
            if (d > mx) mx = d;
        }
        prev_ts = s.ts_ns;
        first_seq = s.seq;
    }

    st.first_seq = first_seq;
    st.last_seq  = last_seq;
    if (n == 0) {
        st.samples = 0;
        return st;
    }

    st.min_dt_ns = mn;
    st.max_dt_ns = mx;
    st.avg_dt_ns = sum / static_cast<double>(n);
    std::sort(dt, dt + n);
    st.p50_dt_ns = dt[n / 2];
    st.p99_dt_ns = dt[static_cast<std::size_t>(0.99 * static_cast<double>(n - 1))];
    st.samples   = n;
    return st;
}

}  // namespace llm