#include "llm/telemetry.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <thread>

#ifdef _WIN32
#include <io.h>
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

}  // namespace llm