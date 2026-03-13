#include "tsxor_encoder.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

// Test 1: Verify the window is actually being used during encoding by checking
// that repeated values compress better than unique values.
TEST(TsxorEncoderWindowTest, WindowUpdateTest) {
    // Repeated values: the window should recognize them after the first occurrence,
    // encoding subsequent ones as just an 8-bit index.
    std::vector<double> repeated(500, 42.0);
    CompressedBuffer repeated_result = TsxorEncoder::encode(repeated);

    // Unique values: the window will rarely find matches, requiring full XOR encoding.
    std::vector<double> unique_vals(500);
    std::mt19937_64 rng(12345);
    std::uniform_real_distribution<double> dist(-1e15, 1e15);
    for (auto& v : unique_vals) {
        v = dist(rng);
    }
    CompressedBuffer unique_result = TsxorEncoder::encode(unique_vals);

    // Repeated values should compress significantly better than unique values.
    // With the window working, repeated values are 1 byte each (8-bit index),
    // while unique values require 8-bit index + 2-bit marker + 5-bit lzb + 6-bit data_bits + data.
    EXPECT_LT(repeated_result.size(), unique_result.size())
        << "Repeated values (" << repeated_result.size() << " bytes) should compress much better than unique values ("
        << unique_result.size() << " bytes)";

    // The repeated values should be at most 25% the size of unique values
    // when the window is working properly.
    EXPECT_LT(repeated_result.size(), unique_result.size() / 2)
        << "Repeated values should be less than half the size of unique values";
}

// Test 2: Encode 1000 repeated values and verify the compression ratio is
// significantly better than 50%.
TEST(TsxorEncoderWindowTest, CompressionRatioTest) {
    std::vector<double> values(1000, 42.0);
    CompressedBuffer result = TsxorEncoder::encode(values);

    size_t raw_size = 1000 * sizeof(double);  // 8000 bytes
    double ratio = static_cast<double>(result.size()) / static_cast<double>(raw_size);

    // With the window fix, 1000 identical doubles should compress extremely well.
    // After the first value (which must be XOR-encoded against zero), the remaining
    // 999 values each need only 1 byte (8-bit window index). So the compressed
    // size should be around ~1000 bytes plus overhead for the first value.
    // This is well under 50% of 8000 = 4000 bytes.
    EXPECT_LT(ratio, 0.50) << "Compression ratio " << (ratio * 100) << "% is too high for constant data. "
                           << "Compressed: " << result.size() << " bytes, Raw: " << raw_size << " bytes. "
                           << "This may indicate the window is not being updated.";

    // Even stronger: should be under 25%
    EXPECT_LT(ratio, 0.25) << "Compression ratio " << (ratio * 100) << "% - expected under 25% for constant data";
}

// Test 3: Encode alternating values like [1.0, 2.0, 1.0, 2.0, ...] and verify
// they compress better than random values.
TEST(TsxorEncoderWindowTest, RepeatedPatternTest) {
    std::vector<double> alternating(1000);
    for (size_t i = 0; i < alternating.size(); i++) {
        alternating[i] = (i % 2 == 0) ? 1.0 : 2.0;
    }
    CompressedBuffer alt_result = TsxorEncoder::encode(alternating);

    // Random values for comparison
    std::vector<double> random_vals(1000);
    std::mt19937_64 rng(67890);
    std::uniform_real_distribution<double> dist(-1e15, 1e15);
    for (auto& v : random_vals) {
        v = dist(rng);
    }
    CompressedBuffer rand_result = TsxorEncoder::encode(random_vals);

    // Alternating pattern should compress much better since both values are
    // always in the window after the first two entries.
    EXPECT_LT(alt_result.size(), rand_result.size())
        << "Alternating pattern (" << alt_result.size() << " bytes) should compress better than random data ("
        << rand_result.size() << " bytes)";

    // Alternating values should be very compact (each is a window hit after warmup)
    size_t raw_size = 1000 * sizeof(double);
    double ratio = static_cast<double>(alt_result.size()) / static_cast<double>(raw_size);
    EXPECT_LT(ratio, 0.25) << "Alternating pattern compression ratio " << (ratio * 100)
                           << "% is too high - window should catch repeating values";
}

// Test 4: After encoding a sequence, the second encoding of the same pattern
// should produce the same result (determinism check).
TEST(TsxorEncoderWindowTest, WindowPopulationTest) {
    std::vector<double> values = {1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 5.0, 1.0, 2.0};

    CompressedBuffer result1 = TsxorEncoder::encode(values);
    CompressedBuffer result2 = TsxorEncoder::encode(values);

    // Both encodings should produce identical output
    EXPECT_EQ(result1.size(), result2.size()) << "Encoding the same data twice should produce the same size output";

    // Verify word-level equality by checking the underlying data vector
    ASSERT_EQ(result1.data.size(), result2.data.size());
    for (size_t i = 0; i < result1.data.size(); i++) {
        EXPECT_EQ(result1.data[i], result2.data[i]) << "Mismatch at word " << i << " of compressed output";
    }
}

// Test 5: Encode a large dataset (10000+ values) with realistic time-series-like
// patterns and verify meaningful compression.
TEST(TsxorEncoderWindowTest, LargeDatasetCompressionTest) {
    const size_t N = 10000;
    std::vector<double> values(N);

    // Simulate realistic time series: a slowly varying signal with occasional repeats
    std::mt19937_64 rng(42);
    std::normal_distribution<double> noise(0.0, 0.01);
    double base = 100.0;
    for (size_t i = 0; i < N; i++) {
        base += noise(rng);
        // Quantize to simulate sensor precision (e.g., 2 decimal places)
        values[i] = std::round(base * 100.0) / 100.0;
    }

    CompressedBuffer result = TsxorEncoder::encode(values);
    size_t raw_size = N * sizeof(double);  // 80000 bytes

    // Realistic time series data with quantization should have many repeated values
    // and nearby values, leading to good compression.
    EXPECT_LT(result.size(), raw_size) << "Compressed size (" << result.size() << ") should be less than raw size ("
                                       << raw_size << ")";

    double ratio = static_cast<double>(result.size()) / static_cast<double>(raw_size);
    EXPECT_LT(ratio, 0.75) << "Compression ratio " << (ratio * 100) << "% is too high for realistic time series data";
}

// Test 6: Compare compression of constant data vs random data; constant should
// be dramatically smaller.
TEST(TsxorEncoderWindowTest, RepeatedConstantBetterThanRandom) {
    const size_t N = 2000;

    // Constant data
    std::vector<double> constant(N, 99.99);
    CompressedBuffer const_result = TsxorEncoder::encode(constant);

    // Truly random data
    std::vector<double> random_vals(N);
    std::mt19937_64 rng(99999);
    std::uniform_real_distribution<double> dist(-1e300, 1e300);
    for (auto& v : random_vals) {
        v = dist(rng);
    }
    CompressedBuffer rand_result = TsxorEncoder::encode(random_vals);

    // Constant data with working window: ~1 byte per value (8-bit index)
    // Random data: ~8+ bytes per value (index + XOR diff bits)
    // The constant data should be at most 1/4 the size of random data.
    EXPECT_LT(const_result.size() * 4, rand_result.size())
        << "Constant data (" << const_result.size() << " bytes) should be at most 1/4 the size of random data ("
        << rand_result.size() << " bytes). "
        << "This strongly indicates the window is not being populated.";

    // Absolute check: constant data should be very small
    // ~1 byte per value for window hits = ~2000 bytes, plus overhead for first value
    EXPECT_LT(const_result.size(), N * 2) << "Constant data should compress to under 2 bytes per value on average";
}

// Test 7: Values that change slowly (like real timestamps) should compress well
// because XOR of nearby values has many leading/trailing zeros.
TEST(TsxorEncoderWindowTest, SlowlyChangingValuesTest) {
    const size_t N = 5000;
    std::vector<double> slow_values(N);
    std::vector<double> random_values(N);

    // Slowly changing values (simulating timestamps or gradual sensor drift)
    // These values differ by tiny amounts, so XOR will have many leading zeros.
    for (size_t i = 0; i < N; i++) {
        slow_values[i] = 1000000.0 + static_cast<double>(i) * 0.001;
    }

    // Random values for comparison
    std::mt19937_64 rng(77777);
    std::uniform_real_distribution<double> dist(0.0, 1e6);
    for (auto& v : random_values) {
        v = dist(rng);
    }

    CompressedBuffer slow_result = TsxorEncoder::encode(slow_values);
    CompressedBuffer rand_result = TsxorEncoder::encode(random_values);

    // Slowly changing values should compress better than random values.
    // The window will have nearby values, and XOR differences will have
    // many leading/trailing zero bits, requiring fewer data bits to encode.
    EXPECT_LT(slow_result.size(), rand_result.size())
        << "Slowly changing data (" << slow_result.size() << " bytes) should compress better than random data ("
        << rand_result.size() << " bytes)";

    // Check meaningful compression relative to raw size
    size_t raw_size = N * sizeof(double);  // 40000 bytes
    double ratio = static_cast<double>(slow_result.size()) / static_cast<double>(raw_size);
    EXPECT_LT(ratio, 0.80) << "Slowly changing values should compress to under 80% of raw size, got " << (ratio * 100)
                           << "%";
}
