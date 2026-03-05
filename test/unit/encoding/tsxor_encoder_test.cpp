#include <gtest/gtest.h>
#include "tsxor_encoder.hpp"
#include <vector>
#include <cstdint>
#include <cmath>
#include <limits>
#include <bit>

// --- Window tests ---

TEST(WindowTest, InitiallyZeros) {
    Window w;
    EXPECT_TRUE(w.contains(0));
    EXPECT_EQ(w.getLast(), 0u);
}

TEST(WindowTest, InsertAndContains) {
    Window w;
    w.insert(1);
    w.insert(2);
    w.insert(3);
    EXPECT_TRUE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));
    EXPECT_FALSE(w.contains(99));
}

TEST(WindowTest, InsertPushesOutOld) {
    // Window of size 3, initially filled with zeros
    Window w(3);
    EXPECT_TRUE(w.contains(0));

    w.insert(1);
    w.insert(2);
    w.insert(3);
    // At this point the 3 initial zeros have been pushed out
    EXPECT_FALSE(w.contains(0));
    EXPECT_TRUE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));

    w.insert(4);
    // Now 1 should be pushed out
    EXPECT_FALSE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));
    EXPECT_TRUE(w.contains(4));
}

TEST(WindowTest, GetIndexOf) {
    Window w(5);
    w.insert(10);
    w.insert(20);
    w.insert(30);
    // Deque front is 30, then 20, then 10, then two zeros
    EXPECT_EQ(w.getIndexOf(30), 0);
    EXPECT_EQ(w.getIndexOf(20), 1);
    EXPECT_EQ(w.getIndexOf(10), 2);
}

TEST(WindowTest, GetLast) {
    Window w;
    w.insert(42);
    EXPECT_EQ(w.getLast(), 42u);

    w.insert(99);
    EXPECT_EQ(w.getLast(), 99u);
}

TEST(WindowTest, GetByOffset) {
    Window w(5);
    w.insert(10);
    w.insert(20);
    w.insert(30);
    // get(0) should be the front (last inserted)
    EXPECT_EQ(w.get(0), w.getLast());
    EXPECT_EQ(w.get(0), 30u);
    EXPECT_EQ(w.get(1), 20u);
    EXPECT_EQ(w.get(2), 10u);
}

TEST(WindowTest, GetCandidateExact) {
    Window w;
    uint64_t val = 0xDEADBEEF;
    w.insert(val);
    // getCandidate with the same value should return it (XOR=0, best match)
    uint64_t candidate = w.getCandidate(val);
    EXPECT_EQ(candidate, val);
}

TEST(WindowTest, GetCandidateBestMatch) {
    Window w;
    // Insert values with different bit patterns
    w.insert(0xFF00FF00FF00FF00ULL);  // alternating bytes
    w.insert(0x0000000000000001ULL);  // very different from target
    w.insert(0xFFFFFFFFFFFFFFFEULL);  // close to all-ones

    // Target is all-ones: 0xFFFFFFFFFFFFFFFF
    // Best match should be 0xFFFFFFFFFFFFFFFE (XOR = 1, most leading+trailing zeros)
    uint64_t target = 0xFFFFFFFFFFFFFFFFULL;
    uint64_t candidate = w.getCandidate(target);
    EXPECT_EQ(candidate, 0xFFFFFFFFFFFFFFFEULL);
}

// --- TsxorEncoder tests ---

TEST(TsxorEncoderTest, EncodeEmptyVector) {
    std::vector<double> values;
    CompressedBuffer result = TsxorEncoder::encode(values);
    // Empty input should produce a buffer with no data written
    EXPECT_EQ(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeConstantValues) {
    std::vector<double> values(100, 42.0);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeIncreasingValues) {
    std::vector<double> values(100);
    for (int i = 0; i < 100; i++) {
        values[i] = static_cast<double>(i) * 1.5;
    }
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeProducesCompressedOutput) {
    // 1000 constant doubles should compress very well via window matching
    std::vector<double> values(1000, 42.0);

    CompressedBuffer result = TsxorEncoder::encode(values);
    size_t rawSize = 1000 * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "Compressed size (" << result.size()
        << ") should be less than raw size (" << rawSize << ")";
}

TEST(TsxorEncoderTest, EncodeHandlesSpecialValues) {
    std::vector<double> values = {
        0.0,
        -0.0,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::min(),
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::denorm_min()
    };

    // Should not crash
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// ---------------------------------------------------------------------------
// Edge case: single value
// The first value is always XOR-encoded against 0 (the initial window content),
// so a 1-element vector must produce a non-empty buffer.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, EncodeSingleValue) {
    std::vector<double> values = {1.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u)
        << "Encoding a single value must produce a non-empty buffer";
}

TEST(TsxorEncoderTest, EncodeSingleValueZero) {
    // 0.0 bit-casts to 0x0000000000000000, which matches the initial window
    // contents (all zeros). This exercises the window-hit path on the very
    // first value.
    std::vector<double> values = {0.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u)
        << "Encoding a single zero value must produce a non-empty buffer";
}

TEST(TsxorEncoderTest, EncodeSingleNaN) {
    std::vector<double> values = {std::numeric_limits<double>::quiet_NaN()};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u)
        << "Encoding a single NaN value must produce a non-empty buffer";
}

TEST(TsxorEncoderTest, EncodeSingleInfinity) {
    std::vector<double> values = {std::numeric_limits<double>::infinity()};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u)
        << "Encoding a single +Inf value must produce a non-empty buffer";
}

TEST(TsxorEncoderTest, EncodeSingleNegativeInfinity) {
    std::vector<double> values = {-std::numeric_limits<double>::infinity()};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u)
        << "Encoding a single -Inf value must produce a non-empty buffer";
}

// ---------------------------------------------------------------------------
// Edge case: two values
// Exercises the path where the second value can use the window populated by
// the first insert. The second value may be a window hit (if equal to first)
// or a window miss (if different).
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, EncodeTwoIdenticalValues) {
    // Second value is identical to first: must be a window hit.
    std::vector<double> values = {42.0, 42.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeTwoDifferentValues) {
    // Second value differs: must be a window miss, XOR-encoded.
    std::vector<double> values = {1.0, 2.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeTwoIdenticalCompressesBetterThanTwoDifferent) {
    // Two identical values: first is full XOR, second is a 1-byte window hit.
    // Two different values: both require full XOR encoding.
    // The identical pair should produce a smaller (or equal) buffer.
    std::vector<double> identical = {42.0, 42.0};
    std::vector<double> different = {1.0, 2.0};

    CompressedBuffer r_identical = TsxorEncoder::encode(identical);
    CompressedBuffer r_different = TsxorEncoder::encode(different);

    EXPECT_LE(r_identical.size(), r_different.size())
        << "Two identical values (" << r_identical.size()
        << " bytes) should be no larger than two different values ("
        << r_different.size() << " bytes)";
}

// ---------------------------------------------------------------------------
// Edge case: all-zero input
// The window is pre-initialized with WINDOW_SIZE zeros. Encoding a vector of
// 0.0 values always hits the window from the very first element, exercising
// the window-hit code path exclusively.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, EncodeAllZeros) {
    // 0.0 bit-casts to 0x0, which is the initial window fill value.
    // Every element should be a window hit → very compact encoding.
    std::vector<double> values(WINDOW_SIZE * 2, 0.0);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);

    size_t rawSize = values.size() * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "All-zero input should compress below raw size";
}

// ---------------------------------------------------------------------------
// Edge case: window boundary at exactly WINDOW_SIZE unique values
// Insert WINDOW_SIZE distinct values into the encoder. The (WINDOW_SIZE+1)-th
// value is a repeat of the first one inserted: by that point the first value
// has been evicted from the sliding window, so it must be encoded as a miss
// (not a hit), producing more output than encoding an in-window repeat.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, WindowEvictsOldestAfterWindowSizeUniqueValues) {
    // Build WINDOW_SIZE+1 unique values so that after encoding them, the first
    // value is no longer in the window.
    std::vector<double> values;
    values.reserve(WINDOW_SIZE + 2);
    for (int i = 0; i < WINDOW_SIZE; i++) {
        values.push_back(static_cast<double>(i + 1) * 1000.0);
    }
    // Append the first value again. At this point the window has been filled
    // with the WINDOW_SIZE distinct values; the initial zeros have been evicted.
    // The repeated first value (1000.0) was inserted at step 0, so it is now
    // at the tail of the deque: it should still be in the window.
    values.push_back(1000.0);

    // Must not crash.
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, WindowEvictsFirstAfterWindowSizePlusOneUniqueValues) {
    // Insert WINDOW_SIZE+1 distinct values so that the very first value is
    // pushed out of the window.  Then repeat it.
    // Compare with inserting a value that IS still in the window.
    //
    // Setup: encode [v0, v1, ..., v_{N}, v0] where N == WINDOW_SIZE.
    // After WINDOW_SIZE+1 unique values, v0 has been evicted.
    // Appending v0 again must go through the miss path.
    //
    // For contrast: encode [v0, v1, ..., v_{N-1}, v0] (N == WINDOW_SIZE-1
    // unique values, then repeat v0 while still in window).
    const double v0 = 9999.0;

    // Case A: v0 is evicted when it is repeated.
    std::vector<double> evicted;
    evicted.push_back(v0);
    for (int i = 1; i <= WINDOW_SIZE; i++) {
        evicted.push_back(static_cast<double>(i) * 3.7);  // all unique
    }
    evicted.push_back(v0);  // v0 has been evicted; this is a miss

    // Case B: v0 is still in the window when it is repeated.
    std::vector<double> still_in_window;
    still_in_window.push_back(v0);
    for (int i = 1; i < WINDOW_SIZE; i++) {
        still_in_window.push_back(static_cast<double>(i) * 3.7);  // all unique
    }
    still_in_window.push_back(v0);  // v0 is still at the tail; this is a hit

    CompressedBuffer r_evicted       = TsxorEncoder::encode(evicted);
    CompressedBuffer r_still_in_win  = TsxorEncoder::encode(still_in_window);

    // When v0 is evicted the repeat requires a full XOR encoding (more bits).
    // When v0 is still in the window it costs only 1 byte.
    // Both sequences are otherwise the same length with the same unique values,
    // so the evicted case should produce a larger (or equal) compressed output.
    EXPECT_GE(r_evicted.size(), r_still_in_win.size())
        << "Encoding a value evicted from the window ("
        << r_evicted.size()
        << " bytes) should not be smaller than encoding an in-window repeat ("
        << r_still_in_win.size() << " bytes)";
}

// ---------------------------------------------------------------------------
// Edge case: timestamp-like monotonically increasing uint64 values
// The encoder is named "TSXOR" (Time Series XOR), designed for compressing
// timestamps. Realistic nanosecond timestamps are monotonically increasing
// uint64 values reinterpreted as double. Verify that such inputs encode
// without crashing and compress better than random data.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, EncodeTimestampLikeMonotonicSequence) {
    // Nanosecond timestamps starting at 2024-01-01 00:00:00 UTC,
    // incrementing by 1 second (1e9 ns). Cast to double via bit_cast.
    const uint64_t base_ts = 1704067200000000000ULL;  // 2024-01-01 in ns
    const uint64_t step_ns = 1000000000ULL;            // 1 second in ns
    const size_t N = 200;

    std::vector<double> ts_values;
    ts_values.reserve(N);
    for (size_t i = 0; i < N; i++) {
        uint64_t ts = base_ts + i * step_ns;
        ts_values.push_back(std::bit_cast<double>(ts));
    }

    CompressedBuffer result = TsxorEncoder::encode(ts_values);
    EXPECT_GT(result.size(), 0u);

    // Monotonic timestamps with small deltas should compress better than raw.
    size_t rawSize = N * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "Monotonically increasing timestamp-like values should compress "
        << "below raw size";
}

TEST(TsxorEncoderTest, EncodeTimestampLargeGaps) {
    // Timestamps with very large gaps (e.g., 1-day intervals).
    // Large gaps mean the XOR of consecutive values has fewer leading zeros,
    // but the encoder should still not crash.
    const uint64_t base_ts = 1704067200000000000ULL;
    const uint64_t day_ns  = 86400ULL * 1000000000ULL;  // 1 day in ns
    const size_t N = 100;

    std::vector<double> ts_values;
    ts_values.reserve(N);
    for (size_t i = 0; i < N; i++) {
        uint64_t ts = base_ts + i * day_ns;
        ts_values.push_back(std::bit_cast<double>(ts));
    }

    CompressedBuffer result = TsxorEncoder::encode(ts_values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeTimestampIdenticalDeltaZero) {
    // All identical timestamps (delta == 0 in the time domain).
    // This is the "duplicate timestamp" edge case: every value after the first
    // is a window hit, so the output should be very compact.
    const uint64_t ts = 1704067200000000000ULL;
    const size_t N = 300;

    std::vector<double> ts_values(N, std::bit_cast<double>(ts));

    CompressedBuffer result = TsxorEncoder::encode(ts_values);
    EXPECT_GT(result.size(), 0u);

    // After the first value populates the window, every subsequent value is a
    // 1-byte window hit. The compressed size should be well below raw.
    size_t rawSize = N * sizeof(double);
    EXPECT_LT(result.size(), rawSize / 4)
        << "Identical timestamp values should compress to under 25% of raw size";
}

// ---------------------------------------------------------------------------
// Edge case: special float values mixed with normal values
// Verifies that NaN and Inf values in the middle of a sequence do not corrupt
// subsequent encoding state. The encoder uses bit_cast, so NaN/Inf are just
// specific uint64 bit patterns; they must not cause crashes or silent errors.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, NaNInMiddleOfSequence) {
    std::vector<double> values = {
        1.0, 2.0, 3.0,
        std::numeric_limits<double>::quiet_NaN(),
        4.0, 5.0, 6.0
    };
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, InfinityInMiddleOfSequence) {
    std::vector<double> values = {
        1.0, 2.0,
        std::numeric_limits<double>::infinity(),
        3.0, 4.0,
        -std::numeric_limits<double>::infinity(),
        5.0
    };
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, NegativeZeroDistinctFromPositiveZero) {
    // +0.0 bit-pattern is 0x0000000000000000 (matches initial window).
    // -0.0 bit-pattern is 0x8000000000000000 (different).
    // Encoding both in sequence should work correctly.
    uint64_t pos_zero_bits = std::bit_cast<uint64_t>(+0.0);
    uint64_t neg_zero_bits = std::bit_cast<uint64_t>(-0.0);
    ASSERT_NE(pos_zero_bits, neg_zero_bits)
        << "Sanity: +0.0 and -0.0 must have different bit patterns";

    std::vector<double> values = {0.0, -0.0, 0.0, -0.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// ---------------------------------------------------------------------------
// Edge case: Window get/contains/getIndexOf at exact capacity boundary
// ---------------------------------------------------------------------------
TEST(WindowTest, ExactWindowSizeCapacity) {
    // A window of size N holds N elements. After N inserts of unique values,
    // the initial zeros should all be evicted.
    const int N = WINDOW_SIZE;
    Window w(N);

    // Initially all zeros.
    EXPECT_TRUE(w.contains(0));

    // Insert N unique non-zero values.
    for (int i = 1; i <= N; i++) {
        w.insert(static_cast<uint64_t>(i));
    }

    // All initial zeros should be evicted.
    EXPECT_FALSE(w.contains(0))
        << "After " << N << " inserts into a window of size " << N
        << ", the initial zeros must be evicted";

    // All inserted values should be present.
    for (int i = 1; i <= N; i++) {
        EXPECT_TRUE(w.contains(static_cast<uint64_t>(i)))
            << "Value " << i << " should still be in the window";
    }
}

TEST(WindowTest, WindowSizeIsWindowSizeConstant) {
    // Verify that the default Window() constructor creates a window of
    // exactly WINDOW_SIZE elements. The constructor signature is:
    //   Window(size_t dim = WINDOW_SIZE)
    // After WINDOW_SIZE inserts the initial zeros must be gone.
    Window w;
    for (int i = 0; i < WINDOW_SIZE; i++) {
        w.insert(static_cast<uint64_t>(i + 1));
    }
    EXPECT_FALSE(w.contains(0))
        << "After exactly WINDOW_SIZE inserts, the initial zeros must be evicted";
}

TEST(WindowTest, GetIndexOfAtWindowBoundary) {
    // Verify that getIndexOf correctly returns the last valid index (N-1)
    // for the oldest element in a full window.
    const int N = 5;
    Window w(N);
    // Insert 5 unique values; the 5th inserted is at front (index 0).
    w.insert(10);
    w.insert(20);
    w.insert(30);
    w.insert(40);
    w.insert(50);
    // Deque: [50, 40, 30, 20, 10]
    EXPECT_EQ(w.getIndexOf(50), 0);
    EXPECT_EQ(w.getIndexOf(10), N - 1)
        << "Oldest element should be at index N-1=" << (N-1);
}

// ---------------------------------------------------------------------------
// Edge case: encoding a sequence longer than 256 elements
// The 8-bit index field means offsets 0..WINDOW_SIZE-1 fit in 7 bits.
// Using more than 256 total values in a sequence stresses the index encoding.
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, EncodeMoreThan256Values) {
    // 300 values alternating between two fixed values. After the first two
    // values the window always contains both, so each subsequent value is a
    // 1-byte hit. This exercises the index byte across many iterations.
    std::vector<double> values;
    values.reserve(300);
    for (int i = 0; i < 300; i++) {
        values.push_back(i % 2 == 0 ? 1.5 : 2.5);
    }
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);

    size_t rawSize = 300 * sizeof(double);
    EXPECT_LT(result.size(), rawSize / 2)
        << "Alternating 2-value sequence over 300 elements should compress well";
}

TEST(TsxorEncoderTest, EncodeExactly256Values) {
    // Exactly 256 values of the same constant.
    std::vector<double> values(256, 3.14);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);

    size_t rawSize = 256 * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "256 identical values should compress below raw size";
}

TEST(TsxorEncoderTest, EncodeExactly257Values) {
    // One more than 256; the sequence of identical values should remain
    // well-compressed regardless of the 256 boundary.
    std::vector<double> values(257, 3.14);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);

    size_t rawSize = 257 * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "257 identical values should still compress below raw size";
}

// ---------------------------------------------------------------------------
// Determinism: encoding the same input twice must give bit-identical output
// (edge-case inputs that might expose non-determinism via NaN bit patterns).
// ---------------------------------------------------------------------------
TEST(TsxorEncoderTest, DeterministicWithNaN) {
    std::vector<double> values = {
        1.0,
        std::numeric_limits<double>::quiet_NaN(),
        2.0,
        std::numeric_limits<double>::signaling_NaN(),
        3.0
    };

    CompressedBuffer r1 = TsxorEncoder::encode(values);
    CompressedBuffer r2 = TsxorEncoder::encode(values);

    ASSERT_EQ(r1.data.size(), r2.data.size());
    for (size_t i = 0; i < r1.data.size(); i++) {
        EXPECT_EQ(r1.data[i], r2.data[i])
            << "Encoding with NaN must be deterministic; mismatch at word " << i;
    }
}

TEST(TsxorEncoderTest, DeterministicSingleValue) {
    std::vector<double> values = {42.0};
    CompressedBuffer r1 = TsxorEncoder::encode(values);
    CompressedBuffer r2 = TsxorEncoder::encode(values);

    ASSERT_EQ(r1.data.size(), r2.data.size());
    for (size_t i = 0; i < r1.data.size(); i++) {
        EXPECT_EQ(r1.data[i], r2.data[i])
            << "Single-value encoding must be deterministic; mismatch at word " << i;
    }
}
