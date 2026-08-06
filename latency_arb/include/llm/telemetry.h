#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace llm {

// Async, single-writer telemetry logger.
//
// Producers call log()/log_warn()/log_error(); these only format and push a
// line onto a mutex-protected queue then return, so no producer blocks on I/O.
// A dedicated writer thread drains the queue (batched, non-blocking).
//
// MMAP logging: when MMAP_LOG_DIR=1, the log file is grown/mapped via mmap so
// the writer only writes into mapped memory (no fsync on the hot path).
// Includes pre-allocation steps: MMAP_PREALLOC_MB (1MB) at configure, and
// MMAP_PREALLOC_BIG_MB (10MB) just before the engine threads spin up.
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

private:
    Telemetry() = default;
    ~Telemetry();

    void push(std::string line);
    void writer_loop();
    void grow_mmap(int mb);
    void write_line(const std::string& line);

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
};

}  // namespace llm