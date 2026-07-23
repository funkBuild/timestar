// Regression tests: the FILTERED FFOR decode path must honour `timestampSize`
// as an upper bound on emitted values, exactly as the unfiltered fast path does.
//
// FFOR decodes in whole groups of kBlockSize (1024).  The final group can
// therefore hold more entries than the caller asked for -- the unfiltered fast
// path has always clamped for this ("the stream decodes in whole FFOR groups, so
// the final group may contain more entries than requested"), but the filtered
// path iterated the full block_count with no bound.  A request for 1200 values
// from a 1500-value encoding returned 1500.
//
// WHY IT MATTERS
//
// The filtered path is precisely where a caller is MOST likely to request fewer
// values than were encoded, since a time-cut is what shortens the request.
//
// Two consequences, both silent:
//
//   1. PHANTOM POINTS.  The tail of the last group is reconstructed and emitted
//      as real data.  Delta-of-delta reconstruction over that region continues
//      the arithmetic progression, so the extra timestamps look entirely
//      plausible and pass the time filter -- they are not obviously garbage.
//
//   2. TIMESTAMP/VALUE DESYNC.  The inflated count is RETURNED to the caller,
//      which then asks the value decoder for more values than the block holds.
//      TSM keeps timestamps and values in parallel vectors and every consumer
//      indexes values[i] by a timestamp index, so a short value decode is an
//      out-of-bounds read -- the same corruption class as the string-decoder
//      clear() bug (see nonnumeric_bucketed_latest_test.cpp), reached by a
//      different route.

#include "../../../lib/encoding/integer_encoder.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

namespace {

constexpr uint64_t kBase = 1700000000000000000ULL;
constexpr uint64_t kStep = 1000000ULL;  // 1ms

// Evenly-spaced timestamps: the constant-delta shape real series produce, and
// the one whose padding reconstructs into plausible-looking phantom values.
std::vector<uint64_t> makeTimestamps(size_t count) {
    std::vector<uint64_t> v;
    v.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        v.push_back(kBase + static_cast<uint64_t>(i) * kStep);
    }
    return v;
}

struct DecodeOutcome {
    size_t skipped = 0;
    size_t added = 0;
    std::vector<uint64_t> values;
};

DecodeOutcome decodeWith(const AlignedBuffer& encoded, unsigned requested, uint64_t minTime, uint64_t maxTime) {
    Slice slice(encoded.data.data(), encoded.size());
    DecodeOutcome out;
    auto [skipped, added] = IntegerEncoder::decode(slice, requested, out.values, minTime, maxTime);
    out.skipped = skipped;
    out.added = added;
    return out;
}

// A window wide enough to admit every timestamp, but not the sentinel values
// that select the unfiltered fast path -- this is how we force the filtered path.
constexpr uint64_t kOpenMin = 1ULL;
constexpr uint64_t kOpenMax = UINT64_MAX - 1;

}  // namespace

class FforFilteredDecodeBoundsTest : public ::testing::Test {};

// The failure itself: 1500 encoded (two groups: 1024 + 476), 1200 requested.
TEST_F(FforFilteredDecodeBoundsTest, FilteredDecodeStopsAtRequestedCount) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);
    constexpr unsigned kRequested = 1200;

    auto got = decodeWith(encoded, kRequested, kOpenMin, kOpenMax);

    EXPECT_EQ(got.values.size(), got.added) << "returned count disagrees with what was appended";
    ASSERT_LE(got.values.size(), kRequested)
        << "filtered decode emitted " << got.values.size() << " values for a requested count of " << kRequested
        << " -- it ran to the end of the FFOR group instead of stopping at the request";
    EXPECT_EQ(got.values.size(), kRequested);
}

// The emitted values must be the true prefix -- no phantom points, and nothing
// reordered or dropped.
TEST_F(FforFilteredDecodeBoundsTest, FilteredDecodeEmitsExactPrefixWithNoPhantomTimestamps) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);
    constexpr unsigned kRequested = 1200;

    auto got = decodeWith(encoded, kRequested, kOpenMin, kOpenMax);

    ASSERT_EQ(got.values.size(), kRequested);
    for (size_t i = 0; i < kRequested; ++i) {
        ASSERT_EQ(got.values[i], src[i]) << "value " << i << " differs from what was encoded";
    }
}

// The two paths must agree: the clamp is the ONLY difference between them.
TEST_F(FforFilteredDecodeBoundsTest, FilteredAndUnfilteredPathsAgreeForTheSameRequest) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);
    constexpr unsigned kRequested = 1200;

    auto fast = decodeWith(encoded, kRequested, 0, UINT64_MAX);  // sentinels: fast path
    auto filtered = decodeWith(encoded, kRequested, kOpenMin, kOpenMax);

    EXPECT_EQ(fast.values.size(), kRequested) << "fast path regressed";
    EXPECT_EQ(filtered.values, fast.values)
        << "filtered and unfiltered decode disagree -- the answer depends on whether a time filter is active";
}

// Boundaries around the 1024-value group size, where the clamp arithmetic lives.
TEST_F(FforFilteredDecodeBoundsTest, FilteredDecodeRespectsRequestAtGroupBoundaries) {
    const auto src = makeTimestamps(2100);  // three groups: 1024 + 1024 + 52
    auto encoded = IntegerEncoder::encode(src);

    for (unsigned requested : {1u, 2u, 1023u, 1024u, 1025u, 2047u, 2048u, 2100u}) {
        auto got = decodeWith(encoded, requested, kOpenMin, kOpenMax);
        ASSERT_EQ(got.values.size(), requested) << "requested " << requested << ", got " << got.values.size();
        for (size_t i = 0; i < requested; ++i) {
            ASSERT_EQ(got.values[i], src[i]) << "requested " << requested << ", value " << i << " wrong";
        }
    }
}

// Requesting MORE than was encoded must yield everything and stop -- the clamp
// must not truncate a legitimate full read.
TEST_F(FforFilteredDecodeBoundsTest, FilteredDecodeReturnsAllValuesWhenRequestExceedsEncoded) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);

    auto got = decodeWith(encoded, 5000, kOpenMin, kOpenMax);

    ASSERT_EQ(got.values.size(), src.size()) << "clamp truncated a full read";
    EXPECT_EQ(got.values, src);
}

// Skipped values consume request budget too: a point below minTime still occupies
// a logical position, so skipped + added must never exceed the request.
TEST_F(FforFilteredDecodeBoundsTest, SkippedPlusAddedNeverExceedsTheRequest) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);
    constexpr unsigned kRequested = 1200;

    // Skip the first 100 points via minTime.
    const uint64_t minTime = kBase + 100 * kStep;
    auto got = decodeWith(encoded, kRequested, minTime, kOpenMax);

    EXPECT_EQ(got.values.size(), got.added);
    EXPECT_EQ(got.skipped, 100u);
    EXPECT_LE(got.skipped + got.added, kRequested) << "decode processed more logical positions than were requested";
    ASSERT_FALSE(got.values.empty());
    EXPECT_EQ(got.values.front(), minTime);
}

// maxTime must still cut the stream early, independently of the clamp.
TEST_F(FforFilteredDecodeBoundsTest, MaxTimeStillTerminatesTheDecode) {
    const auto src = makeTimestamps(1500);
    auto encoded = IntegerEncoder::encode(src);

    const uint64_t maxTime = kBase + 499 * kStep;  // inclusive => 500 points
    auto got = decodeWith(encoded, 1500, kOpenMin, maxTime);

    ASSERT_EQ(got.values.size(), 500u);
    EXPECT_EQ(got.values.back(), maxTime);
}

// A single sub-group encoding must be unaffected -- this is the common small
// block, and the clamp must not perturb it.
TEST_F(FforFilteredDecodeBoundsTest, SmallSingleGroupDecodeIsUnchanged) {
    const auto src = makeTimestamps(50);
    auto encoded = IntegerEncoder::encode(src);

    auto got = decodeWith(encoded, 50, kOpenMin, kOpenMax);

    ASSERT_EQ(got.values.size(), src.size());
    EXPECT_EQ(got.values, src);
}
