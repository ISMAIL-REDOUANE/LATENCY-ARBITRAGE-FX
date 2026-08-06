#pragma once

#include <cstddef>

#include "llm/config.h"
#include "llm/mt5_bridge.h"
#include "llm/strategy.h"
#include "llm/tick.h"

namespace llm {

// ---------------------------------------------------------------------------
// Signal wire codec — pure, zero-allocation.
//
// Canonical frame emitted by the execution thread and consumed by the MT5
// bridge (see src/execution.cpp):
//
//   signal|BUY|SELL,reason,lead,bid,ask,threshold,edge,ts_ms
//
// encode_signal() serializes into a caller-provided buffer (returns bytes
// written excluding the NUL, or -1 if the buffer is too small). It performs no
// dynamic allocation and never touches a socket — this is the only path that
// formats a tradeable signal, so the unit tests exercise the exact bytes that
// ship over ZMQ / TCP.
//
// decode_signal() is a bounds-safe parser that fills the fixed-layout
// ExecSignal POD. Malformed or truncated frames return false.
// ---------------------------------------------------------------------------

int encode_signal(const Signal& s, char* buf, std::size_t cap);

bool decode_signal(const char* frame, std::size_t len, ExecSignal& out);

// Final risk gate applied by the dispatcher before publication: rejects
// signals whose edge is worse than the configured broker-venue slippage cap
// (positive edges always pass; negative edges are capped at kMaxSlippageBroker
// pips). Pure predicate, no side effects.
bool within_slippage_cap(const Signal& s);

}  // namespace llm
