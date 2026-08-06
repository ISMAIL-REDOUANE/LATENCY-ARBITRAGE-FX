#include <cassert>
#include <atomic>
#include <cstdio>
#include <thread>

#include "llm/ring.h"
#include "llm/tick.h"

using namespace llm;

int main() {
    TickRing ring;
    Tick t;
    t.bid = 100.0;
    t.ask = 100.5;
    t.last = 100.25;
    t.venue = Venue::Binance;
    t.valid = 1;
    t.ts_ms = 1234;

    // Single push / pop.
    assert(ring.empty());
    assert(ring.push(t));
    assert(!ring.empty());
    Tick out;
    assert(ring.pop(out));
    assert(out.bid == 100.0 && out.venue == Venue::Binance);
    assert(ring.empty());
    assert(!ring.pop(out));

    // FIFO order across many items (capacity-1 readable slots).
    const int n = (int)TickRing::kCapacity - 2;
    for (int i = 0; i < n; ++i) {
        Tick x = t;
        x.ts_ms = i;
        x.last = static_cast<double>(i);
        assert(ring.push(x));
    }
    for (int i = 0; i < n; ++i) {
        assert(ring.pop(out));
        assert(out.last == static_cast<double>(i));
    }
    assert(ring.empty());

    // Drop-oldest behavior: push more than capacity; only the newest (capacity-1)
// items must survive, in order.
    {
        const int over = (int)TickRing::kCapacity + 5;
        for (int i = 0; i < over; ++i) {
            Tick x = t;
            x.last = static_cast<double>(i);
            ring.push(x);
        }
        std::size_t drained = 0;
        double prev = -1.0;
        while (ring.pop(out)) {
            assert(out.last > prev);   // ascending, no gaps in the survivors
            prev = out.last;
            ++drained;
        }
        // head wrapped to 5 => slots [0..4] hold the last 5 pushes.
        printf("ring: kCapacity=%zu drained=%zu (expect 5) OK\n",
               TickRing::kCapacity, drained);
        assert(drained == 5);
    }

    // SPSC concurrency smoke test (fits within capacity; no drops).
    const int TOT = (int)TickRing::kCapacity - 4;
    TickRing shared;
    std::thread producer([&] {
        for (int i = 0; i < TOT; ++i) {
            Tick x = t;
            x.last = static_cast<double>(i);
            x.valid = 1;
            shared.push(x);
        }
    });
    std::atomic<long> sum{0}, count{0};
    std::thread consumer([&] {
        Tick x;
        while (count.load() < TOT) {
            if (shared.pop(x)) { sum.fetch_add(static_cast<long>(x.last)); count.fetch_add(1); }
        }
    });
    producer.join();
    consumer.join();
    const long long expected = (long long)TOT * (TOT - 1) / 2;
    assert(count.load() == TOT && (long long)sum.load() == expected);
    printf("spsc: count=%ld sum=%ld expected=%lld OK\n",
           count.load(), sum.load(), expected);

    std::puts("all ring tests passed");
    return 0;
}
