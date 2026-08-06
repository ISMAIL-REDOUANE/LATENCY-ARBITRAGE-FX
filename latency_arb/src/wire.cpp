#include "llm/wire.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace llm {

int encode_signal(const Signal& s, char* buf, std::size_t cap) {
    if (cap == 0) return -1;
    const char* side = (s.side == Side::Buy) ? "BUY" : "SELL";
    const int n = std::snprintf(buf, cap,
                                "signal|%s,%s,%.6f,%.6f,%.6f,%.6f,%.6f,%lld",
                                side, s.reason.c_str(), s.lead, s.broker_bid,
                                s.broker_ask, s.threshold, s.edge,
                                static_cast<long long>(s.ts_ms));
    if (n < 0 || static_cast<std::size_t>(n) >= cap) return -1;
    return n;
}

bool decode_signal(const char* frame, std::size_t len, ExecSignal& out) {
    std::memset(&out, 0, sizeof(out));
    out.valid = 0;
    if (!frame) return false;

    const char* p   = frame;
    const char* end = frame + len;

    // ---- topic prefix "signal" (optional, then '|') ---------------------- //
    if (static_cast<std::size_t>(end - p) >= 6 &&
        std::strncmp(p, "signal", 6) == 0) {
        p += 6;
    }
    if (p < end && *p == '|') ++p;

    // ---- side ------------------------------------------------------------ //
    if (static_cast<std::size_t>(end - p) >= 3 &&
        std::strncmp(p, "BUY", 3) == 0) {
        out.side = 1;
        p += 3;
    } else if (static_cast<std::size_t>(end - p) >= 4 &&
               std::strncmp(p, "SELL", 4) == 0) {
        out.side = 2;
        p += 4;
    } else {
        return false;
    }
    if (p >= end || *p != ',') return false;
    ++p;

    // ---- reason (up to next ',') ---------------------------------------- //
    std::size_t i = 0;
    while (p < end && *p != ',' && i < sizeof(out.reason) - 1)
        out.reason[i++] = *p++;
    if (p >= end || *p != ',') return false;
    ++p;
    out.reason[i] = '\0';

    // ---- numeric fields: lead,bid,ask,threshold,edge -------------------- //
    auto next_num = [&](double& v) -> bool {
        char num[48];
        std::size_t n = 0;
        while (p < end && *p != ',' && n < sizeof(num) - 1) num[n++] = *p++;
        if (p >= end || *p != ',') return false;
        ++p;
        num[n] = '\0';
        v = std::strtod(num, nullptr);
        return true;
    };

    if (!next_num(out.lead))       return false;
    if (!next_num(out.broker_bid)) return false;
    if (!next_num(out.broker_ask)) return false;
    if (!next_num(out.threshold))  return false;
    if (!next_num(out.edge))       return false;

    // ---- ts_ms (last field, no trailing comma required) ------------------ //
    {
        char num[32];
        std::size_t n = 0;
        while (p < end && *p != ',' && n < sizeof(num) - 1) num[n++] = *p++;
        num[n] = '\0';
        out.ts_ms = std::atoll(num);
    }

    std::memcpy(out.symbol, "BTC/USD", 8);
    out.valid = 1;
    return out.side != 0 && out.lead > 0.0;
}

bool within_slippage_cap(const Signal& s) {
    const double cap = CompileTime::kMaxSlippageBroker;
    return std::fabs(s.edge) <= cap || s.edge > 0.0;
}

}  // namespace llm
