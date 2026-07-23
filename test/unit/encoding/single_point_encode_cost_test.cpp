// Micro-measurement: per-call cost of the WAL entry encoders at n=1.
//
// The scalar fleet-write shape (128k series, one point per series per batch)
// produces WAL entries with a SINGLE timestamp and a SINGLE value. Measured
// server-side, encoding cost ~15us per entry -- roughly a thousand times the
// cost of writing 16 raw bytes -- which capped ingest at ~90k pts/s while the
// reactor sat 100% busy in encode. These tests print the measured per-call
// cost and assert a loose ceiling so a regression (or a fix) is visible.
//
// Not a correctness test in the usual sense: the ceilings are deliberately
// generous (CI machines vary); the printout is the data.

#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/storage/aligned_buffer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <limits>
#include <vector>

namespace {

template <typename F>
double perCallMicros(F&& f, int iters) {
    // Warm up allocators and branch predictors.
    for (int i = 0; i < 200; ++i) {
        f();
    }
    // MIN of several batches, not one long average: a mean absorbs every
    // preemption and cache eviction the rest of the machine inflicts (this
    // test once failed purely because a benchmark was saturating the box),
    // while the fastest batch is a stable estimate of the code's own cost.
    constexpr int kBatches = 5;
    const int perBatch = iters / kBatches;
    double best = std::numeric_limits<double>::infinity();
    for (int b = 0; b < kBatches; ++b) {
        auto t0 = std::chrono::steady_clock::now();
        for (int i = 0; i < perBatch; ++i) {
            f();
        }
        auto t1 = std::chrono::steady_clock::now();
        best = std::min(best, std::chrono::duration<double, std::micro>(t1 - t0).count() / perBatch);
    }
    return best;
}

}  // namespace

// The ceilings pin the OPTIMIZED code's cost.  Unoptimized/instrumented
// builds (-O0, gcov coverage) run the encoders orders of magnitude slower —
// the coverage CI job measured ~1000x — which both blows the ceiling and
// burns half an hour on the measurement loops.  A cost pin on uninstrumented
// machine code is the only meaningful reading, so skip everywhere else.
#ifndef __OPTIMIZE__
    #define SINGLE_POINT_COST_SKIP() \
        GTEST_SKIP() << "cost ceilings are only meaningful in optimized builds (this is -O0/coverage)"
#else
    #define SINGLE_POINT_COST_SKIP() (void)0
#endif

TEST(SinglePointEncodeCost, IntegerEncoderSingleTimestamp) {
    SINGLE_POINT_COST_SKIP();
    std::vector<uint64_t> ts{1'000'000'000'000'000'000ULL};
    AlignedBuffer buf;
    double us = perCallMicros(
        [&] {
            buf.data.clear();
            IntegerEncoder::encodeInto(ts, buf);
        },
        50000);
    printf("[COST] IntegerEncoder::encodeInto n=1: %.3f us/call\n", us);
    EXPECT_LT(us, 5.0) << "Single-timestamp encode cost regressed; scalar fleet writes pay this per series per batch";
}

TEST(SinglePointEncodeCost, FloatEncoderSingleValue) {
    SINGLE_POINT_COST_SKIP();
    std::vector<double> val{42.375};
    AlignedBuffer buf;
    double us = perCallMicros(
        [&] {
            buf.data.clear();
            FloatEncoder::encodeInto(val, buf);
        },
        50000);
    printf("[COST] FloatEncoder::encodeInto n=1: %.3f us/call\n", us);
    EXPECT_LT(us, 5.0) << "Single-value encode cost regressed; scalar fleet writes pay this per series per batch";
}
