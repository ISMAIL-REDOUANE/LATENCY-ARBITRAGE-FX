#pragma once

#include <atomic>
#include <string>
#include <thread>

#include "llm/config.h"
#include "llm/ring.h"
#include "llm/tick.h"

namespace llm {

// ZeroMQ subscriber thread reading broker quotes (FXCM / IC Markets via the
// OmsBroker-style publisher). Consumes the configured topic, parses the
// OmsBroker quote shape, and pushes BrokerQuotes into the ring handed to it.
//
// Wire shape (OmsBroker quote, pipe-delimited "key,value" style or JSON):
//   quote|bid,ask,spread,timestamp,latency_us,session
//   (see broker_sub.cpp for the exact parse)
class BrokerSub {
public:
    explicit BrokerSub(const Config& cfg, SpscRing<BrokerQuote, 8192>& out);
    ~BrokerSub();

    void start();
    void stop();

private:
    void run();

    const Config& cfg_;
    SpscRing<BrokerQuote, 8192>& out_;
    std::thread   thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
};

}  // namespace llm