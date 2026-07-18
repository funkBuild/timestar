// Regression tests for canonical IEEE-754 special-value semantics in the
// aggregation fold paths (docs/nan_policy.md, CLAUDE.md "Special Float
// Values"):
//
//   - NaN = missing data: skipped by every method on every path, including
//     COUNT (count = number of non-NaN values) and the SIMD batch folds.
//   - ±Inf = valid data: participates arithmetically. SUM/AVG propagate it,
//     MIN/MAX order it, COUNT counts it. Kahan-compensated sums must not
//     degrade Inf to NaN via a poisoned compensation term.
//   - -0.0: preserved by the encoders (see alp_encoder_test.cpp); aggregation
//     may normalize it to +0.0 (IEEE addition semantics) — pinned here.
//
// These pin the memory-store-side folds; the TSM-side (block stats and
// decode-fold) equivalents live in
// test/unit/storage/tsm_nan_special_values_test.cpp.

#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/block_aggregator.hpp"
#include "../../../lib/storage/memory_store.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <vector>

using timestar::AggregationMethod;
using timestar::AggregationState;
using timestar::BlockAggregator;

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kInf = std::numeric_limits<double>::infinity();

// Fold a batch through BlockAggregator's method-aware fold-to-single-state
// mode (the streaming pushdown path; n >= 4 engages the SIMD batch fold).
double foldBatch(AggregationMethod method, const std::vector<double>& values) {
    std::vector<uint64_t> timestamps(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        timestamps[i] = 1000 + i * 1000;
    }
    BlockAggregator agg(0, method);
    agg.enableFoldToSingleState();
    agg.addPoints(timestamps, values);
    return agg.takeSingleState().getValue(method);
}

// Reference scalar fold via AggregationState::addValueForMethod (the
// canonical NaN-skipping per-point path).
double foldScalar(AggregationMethod method, const std::vector<double>& values) {
    AggregationState state;
    for (size_t i = 0; i < values.size(); ++i) {
        state.addValueForMethod(values[i], 1000 + i * 1000, method);
    }
    return state.getValue(method);
}

}  // namespace

// ---------------------------------------------------------------------------
// AggregationState: Kahan compensation must not degrade Inf into NaN.
// ---------------------------------------------------------------------------

TEST(SpecialValuesAggregationState, SumWithInfinityStaysInfinity) {
    AggregationState state;
    state.addValue(kInf, 1000);
    state.addValue(5.0, 2000);
    state.addValue(7.0, 3000);
    EXPECT_EQ(state.getValue(AggregationMethod::SUM), kInf);
    EXPECT_EQ(state.getValue(AggregationMethod::AVG), kInf);
}

TEST(SpecialValuesAggregationState, SumForMethodWithInfinityStaysInfinity) {
    AggregationState state;
    state.addValueForMethod(kInf, 1000, AggregationMethod::SUM);
    state.addValueForMethod(5.0, 2000, AggregationMethod::SUM);
    EXPECT_EQ(state.getValue(AggregationMethod::SUM), kInf);
}

TEST(SpecialValuesAggregationState, InfMinusInfSumIsNaN) {
    // +Inf + -Inf = NaN is the arithmetically canonical aggregate.
    AggregationState state;
    state.addValue(kInf, 1000);
    state.addValue(-kInf, 2000);
    state.addValue(5.0, 3000);
    EXPECT_TRUE(std::isnan(state.getValue(AggregationMethod::SUM)));
    // Min/max still order the infinities.
    EXPECT_EQ(state.getValue(AggregationMethod::MIN), -kInf);
    EXPECT_EQ(state.getValue(AggregationMethod::MAX), kInf);
    // Inf is data, not missing data: it is counted.
    EXPECT_EQ(state.getValue(AggregationMethod::COUNT), 3.0);
}

TEST(SpecialValuesAggregationState, MergePreservesInfinity) {
    AggregationState a;
    a.addValue(kInf, 1000);
    AggregationState b;
    b.addValue(5.0, 2000);
    a.merge(b);
    EXPECT_EQ(a.getValue(AggregationMethod::SUM), kInf);
}

TEST(SpecialValuesAggregationState, MinMaxOverAllInfinities) {
    // Min/max identities are ±Inf, so an all-(+Inf) series yields +Inf —
    // never a garbage finite value like DBL_MAX.
    AggregationState state;
    state.addValue(kInf, 1000);
    state.addValue(kInf, 2000);
    EXPECT_EQ(state.getValue(AggregationMethod::MIN), kInf);
    EXPECT_EQ(state.getValue(AggregationMethod::MAX), kInf);
}

// ---------------------------------------------------------------------------
// BlockAggregator SIMD batch folds (n >= 4): NaN-skip must match the scalar
// fold exactly, for every method.
// ---------------------------------------------------------------------------

TEST(SpecialValuesSimdFold, CountSkipsInteriorNaN) {
    std::vector<double> values = {1.0, kNaN, 3.0, 4.0, kNaN, 6.0, 7.0, 8.0, kNaN, 10.0};
    EXPECT_EQ(foldBatch(AggregationMethod::COUNT, values), 7.0);
    EXPECT_EQ(foldBatch(AggregationMethod::COUNT, values), foldScalar(AggregationMethod::COUNT, values));
}

TEST(SpecialValuesSimdFold, AvgSkipsInteriorNaN) {
    std::vector<double> values = {1.0, kNaN, 3.0, 5.0, kNaN, 7.0, 9.0, 11.0};
    const double expected = (1.0 + 3.0 + 5.0 + 7.0 + 9.0 + 11.0) / 6.0;
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::AVG, values), expected);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::AVG, values), foldScalar(AggregationMethod::AVG, values));
}

TEST(SpecialValuesSimdFold, SumMinMaxSpreadSkipInteriorNaN) {
    std::vector<double> values = {4.0, kNaN, -2.0, 8.0, kNaN, 6.0, 1.0, 3.0};
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::SUM, values), 20.0);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::MIN, values), -2.0);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::MAX, values), 8.0);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::SPREAD, values), 10.0);
}

TEST(SpecialValuesSimdFold, LatestSkipsTrailingNaN) {
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 42.0, kNaN, kNaN};
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::LATEST, values), 42.0);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::LATEST, values), foldScalar(AggregationMethod::LATEST, values));
}

TEST(SpecialValuesSimdFold, FirstSkipsLeadingNaN) {
    std::vector<double> values = {kNaN, kNaN, 42.0, 4.0, 5.0, 6.0, 7.0, 8.0};
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::FIRST, values), 42.0);
    EXPECT_DOUBLE_EQ(foldBatch(AggregationMethod::FIRST, values), foldScalar(AggregationMethod::FIRST, values));
}

TEST(SpecialValuesSimdFold, StddevSkipsInteriorNaN) {
    std::vector<double> values = {2.0, kNaN, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0};
    // Population stddev over the 8 non-NaN values {2,4,4,4,5,5,7,9} = 2.0.
    EXPECT_NEAR(foldBatch(AggregationMethod::STDDEV, values), 2.0, 1e-12);
    EXPECT_NEAR(foldBatch(AggregationMethod::STDDEV, values), foldScalar(AggregationMethod::STDDEV, values), 1e-12);
}

TEST(SpecialValuesSimdFold, SumWithInfinityStaysInfinityAcrossBatches) {
    // Batch 1 drives the running sum to +Inf; batch 2 must not turn it into
    // NaN via a poisoned Kahan compensation term.
    std::vector<uint64_t> ts1 = {1000, 2000, 3000, 4000, 5000};
    std::vector<double> v1 = {kInf, 5.0, 7.0, 9.0, 11.0};
    std::vector<uint64_t> ts2 = {6000, 7000, 8000, 9000, 10000};
    std::vector<double> v2 = {1.0, 2.0, 3.0, 4.0, 5.0};

    BlockAggregator agg(0, AggregationMethod::SUM);
    agg.enableFoldToSingleState();
    agg.addPoints(ts1, v1);
    agg.addPoints(ts2, v2);
    EXPECT_EQ(agg.takeSingleState().getValue(AggregationMethod::SUM), kInf);
}

TEST(SpecialValuesSimdFold, CountCountsInfinity) {
    // Inf is valid data: COUNT includes it (even though the Inf/-Inf mix
    // makes the batch sum NaN, which routes through the scalar fallback).
    std::vector<double> values = {kInf, -kInf, 1.0, 2.0, 3.0, 4.0};
    EXPECT_EQ(foldBatch(AggregationMethod::COUNT, values), 6.0);
}

TEST(SpecialValuesSimdFold, MinMaxOverAllInfinities) {
    std::vector<double> values = {kInf, kInf, kInf, kInf, kInf, kInf};
    EXPECT_EQ(foldBatch(AggregationMethod::MIN, values), kInf);
    EXPECT_EQ(foldBatch(AggregationMethod::MAX, values), kInf);
}

TEST(SpecialValuesSimdFold, NegativeZeroSumNormalizesToPositiveZero) {
    // Documented aggregation semantics: raw reads preserve -0.0 (encoder
    // round-trips it bit-exactly), but SUM starts from +0.0 and
    // (+0.0) + (-0.0) = +0.0 per IEEE 754.
    std::vector<double> values = {-0.0, -0.0, -0.0, -0.0};
    const double sum = foldBatch(AggregationMethod::SUM, values);
    EXPECT_EQ(sum, 0.0);
    EXPECT_FALSE(std::signbit(sum));
}

// ---------------------------------------------------------------------------
// InMemorySeriesStats (memory-store running stats pushdown): must match the
// canonical NaN-skipping semantics so stats-answered queries are identical
// to per-point folds.
// ---------------------------------------------------------------------------

TEST(SpecialValuesMemStoreStats, NanSkippedInAllAccumulators) {
    InMemorySeriesStats stats;
    const double values[] = {1.0, kNaN, 3.0};
    const uint64_t timestamps[] = {1000, 2000, 3000};
    stats.update(values, timestamps, 3);

    EXPECT_TRUE(stats.valid);
    EXPECT_EQ(stats.count, 2u);
    EXPECT_DOUBLE_EQ(stats.compensatedSum(), 4.0);
    EXPECT_DOUBLE_EQ(stats.min, 1.0);
    EXPECT_DOUBLE_EQ(stats.max, 3.0);
    EXPECT_DOUBLE_EQ(stats.mean, 2.0);
    EXPECT_DOUBLE_EQ(stats.firstValue, 1.0);
    EXPECT_DOUBLE_EQ(stats.latestValue, 3.0);
}

TEST(SpecialValuesMemStoreStats, NanBoundariesSkippedForFirstLatest) {
    InMemorySeriesStats stats;
    const double values[] = {kNaN, 7.0, kNaN};
    const uint64_t timestamps[] = {1000, 2000, 3000};
    stats.update(values, timestamps, 3);

    EXPECT_EQ(stats.count, 1u);
    EXPECT_DOUBLE_EQ(stats.firstValue, 7.0);
    EXPECT_EQ(stats.firstTimestamp, 2000u);
    EXPECT_DOUBLE_EQ(stats.latestValue, 7.0);
    EXPECT_EQ(stats.latestTimestamp, 2000u);
}

TEST(SpecialValuesMemStoreStats, AllNanBatchFoldsNothing) {
    InMemorySeriesStats stats;
    const double values[] = {kNaN, kNaN, kNaN, kNaN};
    const uint64_t timestamps[] = {1000, 2000, 3000, 4000};
    stats.update(values, timestamps, 4);

    EXPECT_EQ(stats.count, 0u);
    EXPECT_DOUBLE_EQ(stats.compensatedSum(), 0.0);
}

TEST(SpecialValuesMemStoreStats, InfinitySurvivesSubsequentBatches) {
    InMemorySeriesStats stats;
    const double batch1[] = {kInf, 5.0, 7.0, 9.0};
    const uint64_t ts1[] = {1000, 2000, 3000, 4000};
    stats.update(batch1, ts1, 4);
    EXPECT_EQ(stats.compensatedSum(), kInf);

    const double batch2[] = {1.0, 2.0, 3.0, 4.0};
    const uint64_t ts2[] = {5000, 6000, 7000, 8000};
    stats.update(batch2, ts2, 4);
    // Without the Kahan non-finite guard this degrades to NaN.
    EXPECT_EQ(stats.compensatedSum(), kInf);
    EXPECT_EQ(stats.count, 8u);
    EXPECT_EQ(stats.max, kInf);
    EXPECT_DOUBLE_EQ(stats.min, 1.0);
}

TEST(SpecialValuesMemStoreStats, AllInfinityBatchOrdersCorrectly) {
    // The fused SIMD kernel's all-skipped sentinel conflates all-(+Inf) with
    // all-NaN; the scalar special-value pass must recover the true min/max.
    InMemorySeriesStats stats;
    const double values[] = {kInf, kInf, kInf, kInf};
    const uint64_t timestamps[] = {1000, 2000, 3000, 4000};
    stats.update(values, timestamps, 4);

    EXPECT_EQ(stats.count, 4u);
    EXPECT_EQ(stats.min, kInf);
    EXPECT_EQ(stats.max, kInf);
    EXPECT_EQ(stats.compensatedSum(), kInf);
}

TEST(SpecialValuesMemStoreStats, MatchesAggregationStateOverNanMix) {
    // Placement-independence at the accumulator level: the running stats and
    // the scalar AggregationState fold must produce identical aggregates for
    // the same NaN-carrying data.
    const std::vector<double> values = {1.0, kNaN, 3.0, kNaN, 10.0, -4.0, kNaN, 2.5};
    std::vector<uint64_t> timestamps(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        timestamps[i] = 1000 + i * 1000;
    }

    InMemorySeriesStats stats;
    stats.update(values.data(), timestamps.data(), values.size());

    AggregationState state;
    for (size_t i = 0; i < values.size(); ++i) {
        state.addValue(values[i], timestamps[i]);
    }

    EXPECT_EQ(stats.count, state.count);
    EXPECT_DOUBLE_EQ(stats.compensatedSum(), state.getValue(AggregationMethod::SUM));
    EXPECT_DOUBLE_EQ(stats.min, state.getValue(AggregationMethod::MIN));
    EXPECT_DOUBLE_EQ(stats.max, state.getValue(AggregationMethod::MAX));
    EXPECT_NEAR(stats.m2, state.m2, 1e-9);
    EXPECT_DOUBLE_EQ(stats.firstValue, state.getValue(AggregationMethod::FIRST));
    EXPECT_DOUBLE_EQ(stats.latestValue, state.getValue(AggregationMethod::LATEST));
}
