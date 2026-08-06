#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#include "llm/tick.h"

namespace llm {

// Single-producer / single-consumer lock-free ring buffer.
//
// Semantics:
//   * Exactly ONE writer thread calls push(); exactly ONE reader thread calls
//     pop(). Violating SPSC is a data race by design.
//   * push() overwrites the oldest slot when full (drop-oldest). The writer
//     never blocks, which is the requirement for the WS read-loop hot path.
//   * Memory ordering: release/acquire pairs make a slot's payload visible to
//     the consumer exactly when it becomes pop-able.
//   * Power-of-two capacity so the wrap uses a mask.
template <typename T, std::size_t Capacity = 4096>
class SpscRing {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");

public:
    static constexpr std::size_t kCapacity = Capacity;
    static constexpr std::size_t kMask     = Capacity - 1;

    SpscRing() = default;
    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    // Writer side. Returns false only if the buffer was full (drop-oldest was
    // applied, so the consumer may have missed the overwritten slot). Never
    // blocks — this is what keeps the WS read loop off the disk/network path.
    bool push(const T& item) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & kMask;
        const bool dropped =
            next == tail_.load(std::memory_order_relaxed); // full before write
        slots_[head] = item;                          // publish payload
        head_.store(next, std::memory_order_release); // then advance head
        return !dropped;
    }

    // Reader side. Returns false when empty.
    bool pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) return false;
        out = slots_[tail];
        tail_.store((tail + 1) & kMask, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return tail_.load(std::memory_order_relaxed) ==
               head_.load(std::memory_order_acquire);
    }

    // Approximate occupancy; safe for gauges/logging only.
    std::size_t size() const {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        return (head - tail) & kMask;
    }

private:
    std::atomic<std::size_t> head_{0};
    std::atomic<std::size_t> tail_{0};
    alignas(64) T slots_[Capacity];  // own cache line, never shared with atomics
};

// Convenience alias for the tick streams used by readers.
using TickRing = SpscRing<Tick, 8192>;

}  // namespace llm
