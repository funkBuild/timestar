#pragma once

#include <cstddef>

// Reactor yield cadence for chunked CPU-bound loops (reactor-stall prevention,
// Jul 2026 incident).  Loops over many points call
// seastar::coroutine::maybe_yield() after roughly this much work — chunked,
// never per-point (a per-point co_await measured 15x slower on the insert
// path); maybe_yield() is a cheap need_preempt() check that only suspends at
// the scheduler's task quota.
//
// This is the single definition of the chunk size: every yield-chunked loop
// must use these constants so a future retune cannot miss a site.  Safe to
// include from every translation unit, including libtimestar_proto_conv
// (defines no types that collide with proto-generated names).

namespace timestar {

// For loops whose per-element work is small (raw doubles/timestamps, heap ops
// on 8-byte items, serialisation): ~1-5ms per chunk.
inline constexpr size_t kYieldChunkPoints = 65536;

// For loops moving/merging full AggregationState objects (120 bytes each,
// plus mergeForMethod work): the 64k chunk measured 8-10ms on a fast dev box,
// which extrapolates past the 25ms stall-report threshold on a slow 2-vCPU
// container.  8k keeps state chunks in the same ~1-3ms band as the rest.
inline constexpr size_t kYieldChunkStates = 8192;

}  // namespace timestar
