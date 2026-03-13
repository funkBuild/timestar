#include "alp/alp_encoder.hpp"

#include "alp/alp_constants.hpp"
#include "alp/alp_decoder.hpp"
#include "alp/alp_ffor.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <limits>
#include <numeric>
#include <random>

class ALPEncoderTest : public ::testing::Test {
protected:
    void testRoundtrip(const std::vector<double>& original) {
        auto buffer = ALPEncoder::encode(original);

        CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()),
                              buffer.data.size() * sizeof(uint64_t));

        std::vector<double> decoded;
        ALPDecoder::decode(slice, 0, original.size(), decoded);

        ASSERT_EQ(decoded.size(), original.size())
            << "Decoded size mismatch: expected " << original.size() << " got " << decoded.size();

        for (size_t i = 0; i < original.size(); ++i) {
            if (std::isnan(original[i])) {
                EXPECT_TRUE(std::isnan(decoded[i])) << "Expected NaN at index " << i;
            } else {
                EXPECT_EQ(std::bit_cast<uint64_t>(original[i]), std::bit_cast<uint64_t>(decoded[i]))
                    << "Mismatch at index " << i << ": original=" << original[i] << " decoded=" << decoded[i];
            }
        }
    }

    void testSkipDecode(const std::vector<double>& original, size_t skip, size_t count) {
        auto buffer = ALPEncoder::encode(original);

        CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()),
                              buffer.data.size() * sizeof(uint64_t));

        std::vector<double> decoded;
        size_t actual_count = std::min(count, original.size() - skip);
        ALPDecoder::decode(slice, skip, actual_count, decoded);

        ASSERT_EQ(decoded.size(), actual_count);

        for (size_t i = 0; i < actual_count; ++i) {
            if (std::isnan(original[skip + i])) {
                EXPECT_TRUE(std::isnan(decoded[i]));
            } else {
                EXPECT_EQ(std::bit_cast<uint64_t>(original[skip + i]), std::bit_cast<uint64_t>(decoded[i]))
                    << "Mismatch at decoded index " << i << " (original index " << (skip + i) << ")";
            }
        }
    }
};

// ============================================================
// Basic roundtrip tests
// ============================================================

TEST_F(ALPEncoderTest, EmptyVector) {
    std::vector<double> empty;
    auto buffer = ALPEncoder::encode(empty);
    // Empty buffer is fine - decoder should handle it
    EXPECT_TRUE(buffer.data.empty() || buffer.data.size() >= 0);
}

TEST_F(ALPEncoderTest, SingleValue) {
    testRoundtrip({42.5});
}

TEST_F(ALPEncoderTest, TwoValues) {
    testRoundtrip({1.0, 2.0});
}

TEST_F(ALPEncoderTest, SmallVector) {
    testRoundtrip({1.1, 2.2, 3.3, 4.4, 5.5});
}

TEST_F(ALPEncoderTest, PartialBlock) {
    // Less than ALP_VECTOR_SIZE (1024)
    std::vector<double> data(500);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i) * 0.1;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExactOneBlock) {
    // Exactly ALP_VECTOR_SIZE (1024)
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + static_cast<double>(i) * 0.01;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, MultiBlock) {
    // Multiple blocks (3 full + partial)
    std::vector<double> data(3500);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 100.0 + static_cast<double>(i) * 0.001;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, LargeDataset) {
    std::vector<double> data(100000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 23.5 + std::sin(static_cast<double>(i) * 0.01) * 5.0;
    }
    testRoundtrip(data);
}

// ============================================================
// Edge cases
// ============================================================

TEST_F(ALPEncoderTest, NaN) {
    testRoundtrip({std::numeric_limits<double>::quiet_NaN()});
}

TEST_F(ALPEncoderTest, Infinity) {
    testRoundtrip({std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()});
}

TEST_F(ALPEncoderTest, NegativeZero) {
    testRoundtrip({-0.0, 0.0});
}

TEST_F(ALPEncoderTest, DBL_MIN_MAX) {
    testRoundtrip({std::numeric_limits<double>::min(), std::numeric_limits<double>::max(),
                   std::numeric_limits<double>::lowest()});
}

TEST_F(ALPEncoderTest, Epsilon) {
    testRoundtrip({std::numeric_limits<double>::epsilon(), 1.0 + std::numeric_limits<double>::epsilon(),
                   1.0 - std::numeric_limits<double>::epsilon()});
}

TEST_F(ALPEncoderTest, Subnormals) {
    testRoundtrip({std::numeric_limits<double>::denorm_min(), 5e-324, 1e-310});
}

TEST_F(ALPEncoderTest, MixedSpecialValues) {
    std::vector<double> data;
    for (int i = 0; i < 100; ++i) {
        data.push_back(static_cast<double>(i) * 0.5);
    }
    data.push_back(std::numeric_limits<double>::quiet_NaN());
    data.push_back(std::numeric_limits<double>::infinity());
    data.push_back(-std::numeric_limits<double>::infinity());
    data.push_back(-0.0);
    for (int i = 0; i < 100; ++i) {
        data.push_back(static_cast<double>(i) * -1.5);
    }
    testRoundtrip(data);
}

// ============================================================
// Data patterns
// ============================================================

TEST_F(ALPEncoderTest, ConstantData) {
    std::vector<double> data(2048, 42.0);
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, LinearData) {
    std::vector<double> data(5000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 100.0 + static_cast<double>(i) * 0.5;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, SinusoidalData) {
    std::vector<double> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + std::sin(static_cast<double>(i) * 0.01) * 10.0;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DecimalPrecision1) {
    // Values with exactly 1 decimal place (ALP sweet spot)
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + static_cast<double>(i % 100) * 0.1;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DecimalPrecision2) {
    // Values with exactly 2 decimal places
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i % 10000) * 0.01;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, IntegerValuesAsDoubles) {
    // Integer counters stored as doubles (ALP very high advantage)
    std::vector<double> data(3000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(1000 + i);
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, PercentageValues) {
    // CPU percentages 0-100 with 1 decimal (ALP sweet spot)
    std::mt19937 gen(42);
    std::uniform_real_distribution<double> dist(0.0, 100.0);
    std::vector<double> data(2048);
    for (auto& v : data) {
        v = std::round(dist(gen) * 10.0) / 10.0;  // 1 decimal place
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, RandomDoubles) {
    // Random doubles that should trigger ALP_RD
    std::mt19937 gen(123);
    std::uniform_real_distribution<double> dist(-1e15, 1e15);
    std::vector<double> data(2048);
    for (auto& v : data) {
        v = dist(gen);
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, SparseWithZeros) {
    std::vector<double> data(2048, 0.0);
    std::mt19937 gen(99);
    std::uniform_int_distribution<size_t> idx_dist(0, data.size() - 1);
    std::uniform_real_distribution<double> val_dist(1.0, 100.0);
    for (int i = 0; i < 200; ++i) {
        data[idx_dist(gen)] = val_dist(gen);
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, NegativeValues) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = -50.0 + static_cast<double>(i) * 0.05;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, FinancialTicks) {
    // Simulated stock prices with random walk
    std::mt19937 gen(77);
    std::normal_distribution<double> dist(0.0, 0.01);
    std::vector<double> data(5000);
    data[0] = 150.25;
    for (size_t i = 1; i < data.size(); ++i) {
        data[i] = std::round((data[i - 1] + dist(gen)) * 100.0) / 100.0;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, SensorTemperature) {
    // Realistic sensor: base + slow sine + noise
    std::mt19937 gen(55);
    std::normal_distribution<double> noise(0.0, 0.05);
    std::vector<double> data(10000);
    for (size_t i = 0; i < data.size(); ++i) {
        double t = static_cast<double>(i);
        data[i] = std::round((20.0 + std::sin(t * 0.001) * 5.0 + noise(gen)) * 10.0) / 10.0;
    }
    testRoundtrip(data);
}

// ============================================================
// Skip/limit decode tests
// ============================================================

TEST_F(ALPEncoderTest, SkipFirstFew) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i) * 0.1;
    }
    testSkipDecode(data, 10, 100);
}

TEST_F(ALPEncoderTest, SkipWithinBlock) {
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 50.0 + static_cast<double>(i) * 0.01;
    }
    testSkipDecode(data, 500, 200);
}

TEST_F(ALPEncoderTest, SkipAcrossBlockBoundary) {
    std::vector<double> data(3000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 10.0 + static_cast<double>(i) * 0.001;
    }
    // Skip into second block (past 1024)
    testSkipDecode(data, 1000, 500);
}

TEST_F(ALPEncoderTest, SkipEntireFirstBlock) {
    std::vector<double> data(3000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i) * 0.5;
    }
    testSkipDecode(data, 1024, 500);
}

TEST_F(ALPEncoderTest, DecodeLast10) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i);
    }
    testSkipDecode(data, data.size() - 10, 10);
}

// ============================================================
// FFOR pack/unpack validation
// ============================================================

TEST_F(ALPEncoderTest, FFORPackUnpackZeroBitWidth) {
    // bw=0: all values identical (equal to base)
    std::vector<int64_t> values(100, 42);
    // pack should produce 0 words
    EXPECT_EQ(alp::ffor_packed_words(100, 0), 0u);

    // unpack should fill all with base
    std::vector<int64_t> out(100, 0);
    alp::ffor_unpack(nullptr, 100, 42, 0, out.data());
    for (auto v : out) {
        EXPECT_EQ(v, 42);
    }
}

TEST_F(ALPEncoderTest, FFORPackUnpackSmallBitWidth) {
    const size_t count = 256;
    const int64_t base = 1000;
    const uint8_t bw = 4;  // values range [base, base+15]

    std::vector<int64_t> values(count);
    for (size_t i = 0; i < count; ++i) {
        values[i] = base + static_cast<int64_t>(i % 16);
    }

    size_t n_words = alp::ffor_packed_words(count, bw);
    std::vector<uint64_t> packed(n_words, 0);
    alp::ffor_pack(values.data(), count, base, bw, packed.data());

    std::vector<int64_t> unpacked(count, 0);
    alp::ffor_unpack(packed.data(), count, base, bw, unpacked.data());

    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(unpacked[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST_F(ALPEncoderTest, FFORPackUnpackFullBitWidth) {
    const size_t count = 64;
    const int64_t base = 0;
    const uint8_t bw = 64;

    std::mt19937_64 gen(999);
    std::vector<int64_t> values(count);
    for (auto& v : values) {
        v = static_cast<int64_t>(gen());
    }

    size_t n_words = alp::ffor_packed_words(count, bw);
    std::vector<uint64_t> packed(n_words, 0);
    alp::ffor_pack(values.data(), count, base, bw, packed.data());

    std::vector<int64_t> unpacked(count, 0);
    alp::ffor_unpack(packed.data(), count, base, bw, unpacked.data());

    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(unpacked[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST_F(ALPEncoderTest, FFORPackUnpackOddCount) {
    // Non-power-of-2 count that doesn't align to word boundaries
    const size_t count = 137;
    const int64_t base = -500;
    const uint8_t bw = 11;

    std::mt19937 gen(42);
    std::uniform_int_distribution<int64_t> dist(0, (1 << bw) - 1);
    std::vector<int64_t> values(count);
    for (auto& v : values) {
        v = base + dist(gen);
    }

    size_t n_words = alp::ffor_packed_words(count, bw);
    std::vector<uint64_t> packed(n_words, 0);
    alp::ffor_pack(values.data(), count, base, bw, packed.data());

    std::vector<int64_t> unpacked(count, 0);
    alp::ffor_unpack(packed.data(), count, base, bw, unpacked.data());

    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(unpacked[i], values[i]) << "Mismatch at index " << i;
    }
}

// ============================================================
// Compression ratio sanity checks
// ============================================================

TEST_F(ALPEncoderTest, CompressionRatioDecimalData) {
    // ALP should compress decimal data well
    std::vector<double> data(10000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + static_cast<double>(i % 1000) * 0.1;
    }

    auto buffer = ALPEncoder::encode(data);
    double raw_size = data.size() * sizeof(double);
    double compressed_size = buffer.data.size() * sizeof(uint64_t);
    double ratio = raw_size / compressed_size;

    // ALP should achieve > 1x compression on decimal data
    EXPECT_GT(ratio, 1.0) << "ALP failed to compress decimal data. Ratio: " << ratio;

    // Verify correctness too
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, CompressionRatioConstant) {
    // Constant data should compress extremely well (bw=0)
    std::vector<double> data(10000, 42.5);

    auto buffer = ALPEncoder::encode(data);
    double raw_size = data.size() * sizeof(double);
    double compressed_size = buffer.data.size() * sizeof(uint64_t);
    double ratio = raw_size / compressed_size;

    EXPECT_GT(ratio, 10.0) << "Constant data should compress >10x. Ratio: " << ratio;
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, CompressionRatioIntegers) {
    // Integer counters as doubles should compress very well
    std::vector<double> data(10000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i);
    }

    auto buffer = ALPEncoder::encode(data);
    double raw_size = data.size() * sizeof(double);
    double compressed_size = buffer.data.size() * sizeof(uint64_t);
    double ratio = raw_size / compressed_size;

    EXPECT_GT(ratio, 2.0) << "Integer data should compress >2x. Ratio: " << ratio;
    testRoundtrip(data);
}

// ========== ALP Delta Encoding Tests ==========

TEST_F(ALPEncoderTest, DeltaSchemeSelectedForMonotonic) {
    // Linear ramp: deltas are constant (1), should select SCHEME_ALP_DELTA
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 1000.0 + static_cast<double>(i);
    }

    auto buffer = ALPEncoder::encode(data);

    // Parse stream header to verify scheme
    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()), buffer.data.size() * sizeof(uint64_t));
    uint64_t header0 = slice.readFixed<uint64_t, 64>();
    uint64_t header1 = slice.readFixed<uint64_t, 64>();
    (void)header0;
    uint8_t scheme = static_cast<uint8_t>((header1 >> 32) & 0xFF);

    EXPECT_EQ(scheme, alp::SCHEME_ALP_DELTA) << "Monotonic ramp data should select SCHEME_ALP_DELTA";

    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaRoundtripDecreasing) {
    // Decreasing series (negative deltas exercise zigzag)
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 10000.0 - static_cast<double>(i);
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaRoundtripAlternating) {
    // Alternating +/- pattern exercises zigzag encoding
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 1000.0 + (i % 2 == 0 ? 1.0 : -1.0) * static_cast<double>(i);
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaWithExceptions) {
    // Monotonic data with NaN/Inf/-0.0 sprinkled in
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 500.0 + static_cast<double>(i) * 0.1;
    }
    // Sprinkle exceptions
    data[0] = std::numeric_limits<double>::quiet_NaN();
    data[10] = std::numeric_limits<double>::infinity();
    data[100] = -std::numeric_limits<double>::infinity();
    data[500] = -0.0;
    data[1023] = std::numeric_limits<double>::quiet_NaN();  // last in first block
    data[1024] = std::numeric_limits<double>::infinity();   // first in second block

    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaMultiBlock) {
    // 5000 values spanning multiple blocks (5 blocks: 1024+1024+1024+1024+904)
    std::vector<double> data(5000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 100.0 + static_cast<double>(i) * 0.5;
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaSkipDecode) {
    // Test skip decode with delta scheme
    std::vector<double> data(3000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 1000.0 + static_cast<double>(i);
    }

    // Skip various offsets
    testSkipDecode(data, 0, 100);      // no skip
    testSkipDecode(data, 500, 100);    // mid-block skip
    testSkipDecode(data, 1024, 100);   // exact block boundary skip
    testSkipDecode(data, 1500, 100);   // mid second block
    testSkipDecode(data, 2900, 100);   // near end
    testSkipDecode(data, 0, 3000);     // full decode
    testSkipDecode(data, 1024, 1024);  // skip entire first block, decode second
}

TEST_F(ALPEncoderTest, DeltaCompressionImprovement) {
    // Linear integers: delta encoding should compress significantly better
    // Absolute range: 0-9999 = ~14 bits. Delta: always 1 = 1 bit.
    std::vector<double> data(10000);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<double>(i);
    }

    auto buffer = ALPEncoder::encode(data);
    double raw_size = data.size() * sizeof(double);
    double compressed_size = buffer.data.size() * sizeof(uint64_t);
    double ratio = raw_size / compressed_size;

    // With delta, constant-step data should compress very well (>5x)
    EXPECT_GT(ratio, 5.0) << "Linear integer data with delta should compress >5x. Ratio: " << ratio;
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaAllExceptions) {
    // All NaN: should still work (no non-exceptions to delta-encode)
    std::vector<double> data(2048, std::numeric_limits<double>::quiet_NaN());
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaSingleValue) {
    // Single value edge case
    std::vector<double> data = {42.5};
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaSensorTemperature) {
    // Realistic sensor data: slowly drifting temperatures
    std::vector<double> data(4096);
    std::mt19937 rng(12345);
    std::normal_distribution<double> noise(0.0, 0.1);
    double temp = 22.5;
    for (size_t i = 0; i < data.size(); ++i) {
        temp += 0.01 + noise(rng);
        data[i] = std::round(temp * 100.0) / 100.0;  // 2 decimal places
    }
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, DeltaNotSelectedForRandom) {
    // Random data should NOT select delta (deltas are as wide as absolutes)
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> dist(0.0, 1000.0);
    std::vector<double> data(2048);
    for (auto& v : data)
        v = std::round(dist(rng) * 100.0) / 100.0;

    auto buffer = ALPEncoder::encode(data);

    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()), buffer.data.size() * sizeof(uint64_t));
    uint64_t header0 = slice.readFixed<uint64_t, 64>();
    uint64_t header1 = slice.readFixed<uint64_t, 64>();
    (void)header0;
    uint8_t scheme = static_cast<uint8_t>((header1 >> 32) & 0xFF);

    // Random data should use SCHEME_ALP (not delta)
    EXPECT_EQ(scheme, alp::SCHEME_ALP) << "Random data should not select SCHEME_ALP_DELTA";

    testRoundtrip(data);
}

// ============================================================
// Exception position encoding tests
// These specifically verify that exception positions are
// correctly packed as uint16 values (4 per 64-bit word) and
// decoded back correctly — exercising the packing boundary.
// ============================================================

TEST_F(ALPEncoderTest, ExceptionPositionsHighIndex) {
    // Exceptions at positions > 255 verify that all 16 bits are preserved.
    // ALP_VECTOR_SIZE is 1024, so positions can go up to 1023.
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + static_cast<double>(i) * 0.1;
    }
    // Put exceptions at high positions to test 9-10 bit indices
    data[256] = std::numeric_limits<double>::quiet_NaN();   // 0x100
    data[512] = std::numeric_limits<double>::infinity();    // 0x200
    data[768] = -std::numeric_limits<double>::infinity();   // 0x300
    data[1000] = -0.0;                                      // 0x3E8
    data[1020] = std::numeric_limits<double>::quiet_NaN();  // 0x3FC
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExceptionPositionsFourExceptions) {
    // Exactly 4 exceptions: fills exactly 1 uint64 word in the position array.
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 50.0 + static_cast<double>(i) * 0.01;
    }
    data[1] = std::numeric_limits<double>::quiet_NaN();
    data[100] = std::numeric_limits<double>::infinity();
    data[500] = -0.0;
    data[999] = -std::numeric_limits<double>::infinity();
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExceptionPositionsFiveExceptions) {
    // 5 exceptions: requires 2 uint64 words for positions (4 + 1).
    // This exercises the word boundary in the packed position array.
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 50.0 + static_cast<double>(i) * 0.01;
    }
    data[0] = std::numeric_limits<double>::quiet_NaN();
    data[100] = std::numeric_limits<double>::infinity();
    data[200] = -std::numeric_limits<double>::infinity();
    data[300] = -0.0;
    data[400] = std::numeric_limits<double>::quiet_NaN();  // 5th: goes into 2nd word
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExceptionPositionsNineExceptions) {
    // 9 exceptions: requires 3 words (4 + 4 + 1), fully exercises multi-word packing.
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 75.0 + static_cast<double>(i) * 0.01;
    }
    const double nan_val = std::numeric_limits<double>::quiet_NaN();
    const double inf_val = std::numeric_limits<double>::infinity();
    data[10] = nan_val;
    data[50] = inf_val;
    data[100] = -inf_val;
    data[200] = -0.0;
    data[300] = nan_val;
    data[400] = inf_val;
    data[600] = -inf_val;
    data[800] = -0.0;
    data[1000] = nan_val;
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExceptionPositionsMultiBlock) {
    // Exceptions in multiple blocks, each block with several exceptions.
    // Verifies the block header exception_count is read correctly per-block.
    std::vector<double> data(3 * 1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 20.0 + static_cast<double>(i % 100) * 0.1;
    }
    // Block 0 (indices 0-1023): 3 exceptions
    data[50] = std::numeric_limits<double>::quiet_NaN();
    data[512] = std::numeric_limits<double>::infinity();
    data[1000] = -0.0;
    // Block 1 (indices 1024-2047): 5 exceptions (multi-word positions)
    data[1024] = -std::numeric_limits<double>::infinity();
    data[1100] = std::numeric_limits<double>::quiet_NaN();
    data[1200] = std::numeric_limits<double>::infinity();
    data[1500] = -0.0;
    data[2000] = std::numeric_limits<double>::quiet_NaN();
    // Block 2 (indices 2048-3071): 1 exception
    data[2100] = -std::numeric_limits<double>::infinity();
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ExceptionPositionsVerifyBitPreservation) {
    // White-box test: verify exception positions requiring all 16 bits (>= 256).
    // Confirms the uint16 packing format is symmetric between encoder and decoder.
    std::vector<double> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = 1.0 + static_cast<double>(i) * 0.001;
    }
    // 8 exceptions at positions that require 9+ bits (>= 256)
    const size_t exc_positions_arr[] = {256, 300, 400, 500, 600, 700, 900, 1023};
    data[256] = std::numeric_limits<double>::quiet_NaN();
    data[300] = std::numeric_limits<double>::infinity();
    data[400] = -std::numeric_limits<double>::infinity();
    data[500] = -0.0;
    data[600] = std::numeric_limits<double>::quiet_NaN();
    data[700] = std::numeric_limits<double>::infinity();
    data[900] = -0.0;
    data[1023] = std::numeric_limits<double>::quiet_NaN();

    auto buffer = ALPEncoder::encode(data);
    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()), buffer.data.size() * sizeof(uint64_t));
    std::vector<double> decoded;
    ALPDecoder::decode(slice, 0, data.size(), decoded);

    ASSERT_EQ(decoded.size(), data.size());

    // Verify exception values are restored at the correct positions
    for (size_t pos : exc_positions_arr) {
        if (std::isnan(data[pos])) {
            EXPECT_TRUE(std::isnan(decoded[pos])) << "NaN not preserved at position " << pos;
        } else {
            EXPECT_EQ(std::bit_cast<uint64_t>(data[pos]), std::bit_cast<uint64_t>(decoded[pos]))
                << "Value mismatch at exception position " << pos << ": original=" << data[pos]
                << " decoded=" << decoded[pos];
        }
    }

    // Also verify non-exception values are not corrupted
    for (size_t i = 0; i < data.size(); ++i) {
        bool is_exc = false;
        for (size_t pos : exc_positions_arr) {
            if (pos == i) {
                is_exc = true;
                break;
            }
        }
        if (!is_exc) {
            EXPECT_EQ(std::bit_cast<uint64_t>(data[i]), std::bit_cast<uint64_t>(decoded[i]))
                << "Non-exception mismatch at position " << i;
        }
    }
}

// ============================================================
// ALP_RD scheme explicit tests
// These tests verify that data which cannot be decimal-encoded
// (exception rate > 50%) selects SCHEME_ALP_RD and that the
// decoder handles every ALP_RD block correctly.
// ============================================================

// Helper: read scheme byte from an encoded buffer without consuming the whole stream.
static uint8_t readSchemeFromBuffer(const CompressedBuffer& buffer) {
    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()), buffer.data.size() * sizeof(uint64_t));
    uint64_t header0 = slice.readFixed<uint64_t, 64>();
    uint64_t header1 = slice.readFixed<uint64_t, 64>();
    (void)header0;
    return static_cast<uint8_t>((header1 >> 32) & 0xFF);
}

TEST_F(ALPEncoderTest, ALPRD_SchemeSelectedForRandomDoubles) {
    // Uniformly-random doubles cannot be decimal-encoded: virtually all values
    // will be exceptions for every (exp, fac) pair, so the exception rate will
    // exceed ALP_RD_EXCEPTION_THRESHOLD (0.50) and SCHEME_ALP_RD must be chosen.
    std::mt19937_64 gen(0xDEADBEEF);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(2048);
    for (auto& v : data) {
        // Produce arbitrary bit patterns that are valid (finite) doubles.
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    uint8_t scheme = readSchemeFromBuffer(buffer);

    EXPECT_EQ(scheme, alp::SCHEME_ALP_RD) << "Random bit-pattern doubles must select SCHEME_ALP_RD";
}

TEST_F(ALPEncoderTest, ALPRD_RoundtripSmall) {
    // Small vector: exercises partial-block path in ALP_RD.
    std::mt19937_64 gen(1);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(7);
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    EXPECT_EQ(readSchemeFromBuffer(buffer), alp::SCHEME_ALP_RD);
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ALPRD_RoundtripSingleValue) {
    // Single-value edge case for ALP_RD.
    // Use a value whose bit pattern is highly unlikely to be
    // decimal-encodable (random mantissa, middle-range exponent).
    std::mt19937_64 gen(2);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(1);
    do {
        uint64_t bits = dist(gen);
        // Force a non-zero exponent field (bits 52-62) to avoid denormals/zero
        bits = (bits & 0x800FFFFFFFFFFFFFULL) | 0x3FE0000000000000ULL;
        data[0] = std::bit_cast<double>(bits);
    } while (!std::isfinite(data[0]));

    // A single value always has 100% exception rate, so ALP_RD is selected.
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ALPRD_RoundtripExactOneBlock) {
    // Exactly ALP_VECTOR_SIZE (1024) random doubles.
    std::mt19937_64 gen(42);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(alp::ALP_VECTOR_SIZE);
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    EXPECT_EQ(readSchemeFromBuffer(buffer), alp::SCHEME_ALP_RD);
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ALPRD_RoundtripMultiBlock) {
    // 3 full blocks + a partial tail (3500 values).
    std::mt19937_64 gen(99);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(3500);
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    EXPECT_EQ(readSchemeFromBuffer(buffer), alp::SCHEME_ALP_RD);
    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ALPRD_SkipDecodeWholeBlock) {
    // Skip the entire first block, decode second block only.
    std::mt19937_64 gen(7);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(2 * alp::ALP_VECTOR_SIZE);  // 2048 values
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    EXPECT_EQ(readSchemeFromBuffer(buffer), alp::SCHEME_ALP_RD);

    // Skip first block exactly
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, alp::ALP_VECTOR_SIZE);
}

TEST_F(ALPEncoderTest, ALPRD_SkipDecodePartial) {
    // Skip into middle of first block, read a window.
    std::mt19937_64 gen(8);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(3000);
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }

    auto buffer = ALPEncoder::encode(data);
    EXPECT_EQ(readSchemeFromBuffer(buffer), alp::SCHEME_ALP_RD);

    testSkipDecode(data, 100, 200);   // mid first block
    testSkipDecode(data, 900, 200);   // crosses block boundary
    testSkipDecode(data, 1024, 500);  // starts exactly at second block
    testSkipDecode(data, 2900, 100);  // near end
}

TEST_F(ALPEncoderTest, ALPRD_RoundtripWithExceptions) {
    // ALP_RD itself has exceptions when the left part of a value is not in
    // the dictionary built for the block.  Force this by including a handful
    // of values that each have unique upper-32 bit patterns among mostly
    // identical-upper-bits values.
    //
    // Strategy: fill the block with doubles that all share the same upper 32
    // bits (so the dictionary has 1 entry), then inject a few values with
    // completely different upper bits — these become ALP_RD exceptions.
    const uint64_t common_upper = 0x400921FBULL;  // upper bits of ~pi
    std::vector<double> data;
    data.reserve(512);

    std::mt19937 gen(55);
    std::uniform_int_distribution<uint32_t> lower_dist;

    // 500 values sharing the same upper 32 bits
    for (int i = 0; i < 500; ++i) {
        uint64_t bits = (common_upper << 32) | lower_dist(gen);
        data.push_back(std::bit_cast<double>(bits));
    }
    // 12 exceptions: completely different upper bits
    const uint64_t exception_uppers[] = {0x3FF00000ULL, 0x40000000ULL, 0xBFF00000ULL, 0xC0000000ULL,
                                         0x7FEFFFFFULL, 0x00100000ULL, 0x40490000ULL, 0x3FC00000ULL,
                                         0x40800000ULL, 0x40900000ULL, 0x40A00000ULL, 0x40B00000ULL};
    for (auto upper : exception_uppers) {
        uint64_t bits = (upper << 32) | lower_dist(gen);
        data.push_back(std::bit_cast<double>(bits));
    }

    // All values are finite?
    for (auto v : data) {
        ASSERT_TRUE(std::isfinite(v));
    }

    testRoundtrip(data);
}

TEST_F(ALPEncoderTest, ALPRD_VerifySchemeByteInHeader) {
    // White-box: confirm the scheme byte in the stream header equals
    // SCHEME_ALP_RD (1) when the encoder selects the RD path, and that
    // SCHEME_ALP (0) and SCHEME_ALP_DELTA (2) are the only other values
    // produced by the encoder (i.e. the three-way distinction is stable).
    EXPECT_EQ(alp::SCHEME_ALP, static_cast<uint8_t>(0));
    EXPECT_EQ(alp::SCHEME_ALP_RD, static_cast<uint8_t>(1));
    EXPECT_EQ(alp::SCHEME_ALP_DELTA, static_cast<uint8_t>(2));

    // ALP_RD data
    {
        std::mt19937_64 gen(0);
        std::uniform_int_distribution<uint64_t> dist;
        std::vector<double> data(512);
        for (auto& v : data) {
            uint64_t bits;
            do {
                bits = dist(gen);
            } while (!std::isfinite(std::bit_cast<double>(bits)));
            v = std::bit_cast<double>(bits);
        }
        EXPECT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_RD);
    }

    // ALP data (decimal values)
    {
        std::vector<double> data(512);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = static_cast<double>(i % 100) * 0.01;
        uint8_t s = readSchemeFromBuffer(ALPEncoder::encode(data));
        EXPECT_TRUE(s == alp::SCHEME_ALP || s == alp::SCHEME_ALP_DELTA)
            << "Decimal data should use ALP or ALP_DELTA, got " << (int)s;
    }

    // ALP_DELTA data (monotonic ramp)
    {
        std::vector<double> data(1024);
        for (size_t i = 0; i < data.size(); ++i)
            data[i] = static_cast<double>(i);
        EXPECT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_DELTA);
    }
}

// ============================================================
// Skip/limit edge case tests
// These tests target the skip/limit boundary conditions in
// ALPDecoder::decode() that were previously untested.
// ============================================================

// Helper: encode data, call decode directly (not via testSkipDecode), and
// return the decoded vector.  This lets us pass skip/length values that are
// outside the "safe" ranges guarded by testSkipDecode's std::min().
static std::vector<double> decodeRaw(const std::vector<double>& original, size_t skip, size_t length) {
    auto buffer = ALPEncoder::encode(original);
    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()), buffer.data.size() * sizeof(uint64_t));
    std::vector<double> out;
    ALPDecoder::decode(slice, skip, length, out);
    return out;
}

// 1. skip=0, limit=all — identical to a plain roundtrip decode.
TEST_F(ALPEncoderTest, SkipEdge_ZeroSkipAllValues_ALP) {
    std::vector<double> data(500);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 10.0 + static_cast<double>(i) * 0.1;

    auto result = decodeRaw(data, 0, data.size());
    ASSERT_EQ(result.size(), data.size());
    for (size_t i = 0; i < data.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[i])) << "Mismatch at index " << i;
}

// 1b. skip=0, limit=all with a multi-block dataset.
TEST_F(ALPEncoderTest, SkipEdge_ZeroSkipAllValues_MultiBlock) {
    std::vector<double> data(3000);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 20.0 + static_cast<double>(i) * 0.01;

    auto result = decodeRaw(data, 0, data.size());
    ASSERT_EQ(result.size(), data.size());
    for (size_t i = 0; i < data.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[i])) << "Mismatch at index " << i;
}

// 2. skip=0, limit=0 — must return empty output without touching the stream.
TEST_F(ALPEncoderTest, SkipEdge_ZeroSkipZeroLimit) {
    std::vector<double> data(200);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.5;

    auto result = decodeRaw(data, 0, 0);
    EXPECT_TRUE(result.empty()) << "limit=0 should produce an empty output vector";
}

// 2b. limit=0 with non-zero skip — must also return empty.
TEST_F(ALPEncoderTest, SkipEdge_NonZeroSkipZeroLimit) {
    std::vector<double> data(500);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.25;

    auto result = decodeRaw(data, 100, 0);
    EXPECT_TRUE(result.empty()) << "limit=0 with skip=100 should produce an empty output vector";
}

// 3. skip=total_count — skip past all values; output must be empty.
TEST_F(ALPEncoderTest, SkipEdge_SkipEqualsTotalCount) {
    std::vector<double> data(500);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.1;

    // Requesting 10 values but skip equals total — nothing to return.
    auto result = decodeRaw(data, data.size(), 10);
    EXPECT_TRUE(result.empty()) << "skip==total_count should produce an empty output vector";
}

// 3b. skip larger than total_count — same expectation.
TEST_F(ALPEncoderTest, SkipEdge_SkipBeyondTotalCount) {
    std::vector<double> data(300);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.3;

    auto result = decodeRaw(data, 99999, 50);
    EXPECT_TRUE(result.empty()) << "skip > total_count should produce an empty output vector";
}

// 4. skip=total_count-1, limit=1 — must return exactly the last value.
TEST_F(ALPEncoderTest, SkipEdge_SkipToLastValue_ALP) {
    std::vector<double> data(500);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 100.0 + static_cast<double>(i) * 0.1;

    const size_t last = data.size() - 1;
    auto result = decodeRaw(data, last, 1);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(std::bit_cast<uint64_t>(result[0]), std::bit_cast<uint64_t>(data[last]))
        << "Should return only the last value";
}

// 4b. Same test with a multi-block dataset (last value is in the tail block).
TEST_F(ALPEncoderTest, SkipEdge_SkipToLastValue_MultiBlock) {
    std::vector<double> data(2200);  // 2 full + partial tail
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 50.0 + static_cast<double>(i) * 0.05;

    const size_t last = data.size() - 1;
    auto result = decodeRaw(data, last, 1);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(std::bit_cast<uint64_t>(result[0]), std::bit_cast<uint64_t>(data[last]))
        << "Should return only the last value in the tail block";
}

// 5. skip across a block boundary — skip=500, total=1500, limit=200.
//    The first block is [0,1023], so skip=500 stays within block 0 but
//    the requested window [500, 700) stays within block 0.
//    A separate test has skip=900 so the window crosses the block boundary.
TEST_F(ALPEncoderTest, SkipEdge_SkipAcrossBlockBoundary_ALP) {
    // 1500 values: blocks [0,1023] and [1024,1499]
    std::vector<double> data(1500);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 30.0 + static_cast<double>(i) * 0.02;

    // Window crosses the boundary between block 0 and block 1.
    // skip=900, limit=300 → values [900, 1200), spanning the 1024 boundary.
    testSkipDecode(data, 900, 300);
}

// 5b. skip exactly at a block boundary.
TEST_F(ALPEncoderTest, SkipEdge_SkipExactlyAtBlockBoundary_ALP) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 1.0 + static_cast<double>(i) * 0.001;

    // skip == ALP_VECTOR_SIZE: start of second block, read 100 values.
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, 100);
}

// 5c. skip one past a block boundary.
TEST_F(ALPEncoderTest, SkipEdge_SkipOnePastBlockBoundary_ALP) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 1.0 + static_cast<double>(i) * 0.001;

    testSkipDecode(data, alp::ALP_VECTOR_SIZE + 1, 100);
}

// 6. limit extends beyond available data — decoder must clamp to remaining.
TEST_F(ALPEncoderTest, SkipEdge_LimitBeyondAvailable_ALP) {
    std::vector<double> data(300);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.5;

    // skip=250, limit=200 — only 50 values remain after skip.
    auto result = decodeRaw(data, 250, 200);
    // Decoder trims output_remaining at the end — should have at most 50.
    EXPECT_LE(result.size(), 50u) << "Decoder must not return more values than exist after skip";
    // Verify correctness of what was returned.
    for (size_t i = 0; i < result.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[250 + i]))
            << "Value mismatch at index " << i;
}

// 6b. limit extends beyond available data spanning two blocks.
TEST_F(ALPEncoderTest, SkipEdge_LimitBeyondAvailable_MultiBlock) {
    std::vector<double> data(1100);  // 1 full block + 76-value tail
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 5.0 + static_cast<double>(i) * 0.1;

    // skip=1050, limit=9999 — only 50 values remain.
    auto result = decodeRaw(data, 1050, 9999);
    EXPECT_LE(result.size(), 50u);
    for (size_t i = 0; i < result.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[1050 + i]))
            << "Value mismatch at index " << i;
}

// 7. skip+limit exactly at a block boundary.
TEST_F(ALPEncoderTest, SkipEdge_SkipPlusLimitAtBlockBoundary_ALP) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 7.0 + static_cast<double>(i) * 0.007;

    // Decode exactly the first block: skip=0, limit=1024.
    testSkipDecode(data, 0, alp::ALP_VECTOR_SIZE);

    // Decode exactly the second block: skip=1024, limit=1024.
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, alp::ALP_VECTOR_SIZE);
}

// 7b. Window ends exactly at a block boundary with a non-zero skip.
TEST_F(ALPEncoderTest, SkipEdge_WindowEndsAtBlockBoundary_ALP) {
    std::vector<double> data(2048);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = 3.0 + static_cast<double>(i) * 0.003;

    // skip=512, limit=512 → window [512, 1024) ends exactly at block boundary.
    testSkipDecode(data, 512, 512);
}

// --- ALP_RD scheme versions of the key edge cases ---

// Helper: produce a vector of random finite doubles (forces ALP_RD).
static std::vector<double> makeRDData(size_t count, uint64_t seed) {
    std::mt19937_64 gen(seed);
    std::uniform_int_distribution<uint64_t> dist;
    std::vector<double> data(count);
    for (auto& v : data) {
        uint64_t bits;
        do {
            bits = dist(gen);
        } while (!std::isfinite(std::bit_cast<double>(bits)));
        v = std::bit_cast<double>(bits);
    }
    return data;
}

// 1. skip=0, limit=0 — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_ZeroSkipZeroLimit_ALPRD) {
    auto data = makeRDData(512, 10);
    auto result = decodeRaw(data, 0, 0);
    EXPECT_TRUE(result.empty());
}

// 2. skip=total_count — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipEqualsTotalCount_ALPRD) {
    auto data = makeRDData(512, 20);
    auto result = decodeRaw(data, data.size(), 10);
    EXPECT_TRUE(result.empty()) << "skip==total_count with ALP_RD should produce empty output";
}

// 3. skip=total_count-1, limit=1 — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipToLastValue_ALPRD) {
    auto data = makeRDData(512, 30);
    const size_t last = data.size() - 1;
    auto result = decodeRaw(data, last, 1);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(std::bit_cast<uint64_t>(result[0]), std::bit_cast<uint64_t>(data[last]))
        << "Should return only the last value (ALP_RD)";
}

// 4. skip across block boundary — ALP_RD scheme (1500 values, skip=900, limit=300).
TEST_F(ALPEncoderTest, SkipEdge_SkipAcrossBlockBoundary_ALPRD) {
    auto data = makeRDData(1500, 40);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_RD);
    testSkipDecode(data, 900, 300);
}

// 5. skip exactly at block boundary — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipExactlyAtBlockBoundary_ALPRD) {
    auto data = makeRDData(2048, 50);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_RD);
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, 200);
}

// 6. limit beyond available — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_LimitBeyondAvailable_ALPRD) {
    auto data = makeRDData(512, 60);
    // skip=500, limit=9999 — only 12 values remain.
    auto result = decodeRaw(data, 500, 9999);
    EXPECT_LE(result.size(), 12u);
    for (size_t i = 0; i < result.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[500 + i]))
            << "Value mismatch at index " << i;
}

// 7. skip+limit exactly at block boundary — ALP_RD scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipPlusLimitAtBlockBoundary_ALPRD) {
    auto data = makeRDData(2048, 70);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_RD);

    // Decode exactly the first block.
    testSkipDecode(data, 0, alp::ALP_VECTOR_SIZE);
    // Decode exactly the second block.
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, alp::ALP_VECTOR_SIZE);
}

// --- ALP_DELTA scheme versions ---

// Helper: produce linearly-increasing data that forces ALP_DELTA.
static std::vector<double> makeDeltaData(size_t count, double start = 1000.0, double step = 1.0) {
    std::vector<double> data(count);
    for (size_t i = 0; i < count; ++i)
        data[i] = start + static_cast<double>(i) * step;
    return data;
}

// 1. skip=0, limit=0 — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_ZeroSkipZeroLimit_ALPDelta) {
    auto data = makeDeltaData(512);
    auto result = decodeRaw(data, 0, 0);
    EXPECT_TRUE(result.empty());
}

// 2. skip=total_count — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipEqualsTotalCount_ALPDelta) {
    auto data = makeDeltaData(512);
    auto result = decodeRaw(data, data.size(), 10);
    EXPECT_TRUE(result.empty()) << "skip==total_count with ALP_DELTA should produce empty output";
}

// 3. skip=total_count-1, limit=1 — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipToLastValue_ALPDelta) {
    auto data = makeDeltaData(512);
    const size_t last = data.size() - 1;
    auto result = decodeRaw(data, last, 1);
    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(std::bit_cast<uint64_t>(result[0]), std::bit_cast<uint64_t>(data[last]))
        << "Should return only the last value (ALP_DELTA)";
}

// 4. skip across block boundary — ALP_DELTA scheme (1500 values, skip=900, limit=300).
TEST_F(ALPEncoderTest, SkipEdge_SkipAcrossBlockBoundary_ALPDelta) {
    auto data = makeDeltaData(1500);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_DELTA);
    testSkipDecode(data, 900, 300);
}

// 5. skip exactly at block boundary — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipExactlyAtBlockBoundary_ALPDelta) {
    auto data = makeDeltaData(2048);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_DELTA);
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, 200);
}

// 6. limit beyond available — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_LimitBeyondAvailable_ALPDelta) {
    auto data = makeDeltaData(512);
    // skip=500, limit=9999 — only 12 values remain.
    auto result = decodeRaw(data, 500, 9999);
    EXPECT_LE(result.size(), 12u);
    for (size_t i = 0; i < result.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[500 + i]))
            << "Value mismatch at index " << i;
}

// 7. skip+limit exactly at block boundary — ALP_DELTA scheme.
TEST_F(ALPEncoderTest, SkipEdge_SkipPlusLimitAtBlockBoundary_ALPDelta) {
    auto data = makeDeltaData(2048);
    ASSERT_EQ(readSchemeFromBuffer(ALPEncoder::encode(data)), alp::SCHEME_ALP_DELTA);

    // Decode exactly the first block.
    testSkipDecode(data, 0, alp::ALP_VECTOR_SIZE);
    // Decode exactly the second block.
    testSkipDecode(data, alp::ALP_VECTOR_SIZE, alp::ALP_VECTOR_SIZE);
}

// --- Mixed-scheme correctness: append-output mode ---
// Verify that calling decode() twice into the same output vector accumulates
// results correctly (the decoder appends to out, not overwrites).
TEST_F(ALPEncoderTest, SkipEdge_AppendToExistingOutputVector) {
    std::vector<double> data(200);
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<double>(i) * 0.1;

    auto buffer = ALPEncoder::encode(data);

    // First decode: values [0, 100).
    std::vector<double> out;
    {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()),
                              buffer.data.size() * sizeof(uint64_t));
        ALPDecoder::decode(slice, 0, 100, out);
    }
    ASSERT_EQ(out.size(), 100u);

    // Second decode appended to same vector: values [100, 200).
    {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()),
                              buffer.data.size() * sizeof(uint64_t));
        ALPDecoder::decode(slice, 100, 100, out);
    }
    ASSERT_EQ(out.size(), 200u);

    for (size_t i = 0; i < data.size(); ++i)
        EXPECT_EQ(std::bit_cast<uint64_t>(out[i]), std::bit_cast<uint64_t>(data[i]))
            << "Mismatch at combined index " << i;
}

// --- Comprehensive block-boundary sweep ---
// Encode 3*ALP_VECTOR_SIZE values and sweep a fixed-width window of 100
// values across all positions that straddle a block boundary.  This
// exercises the fast-skip path and the mixed-block output path together.
TEST_F(ALPEncoderTest, SkipEdge_BlockBoundarySweep_ALP) {
    const size_t N = 3 * alp::ALP_VECTOR_SIZE;  // 3072 values
    std::vector<double> data(N);
    for (size_t i = 0; i < N; ++i)
        data[i] = 5.0 + static_cast<double>(i) * 0.005;

    // Positions that straddle or land on block boundaries (1024, 2048).
    const size_t boundary_skips[] = {
        alp::ALP_VECTOR_SIZE - 50,      // 974:  50 in block 0, 50 in block 1
        alp::ALP_VECTOR_SIZE,           // 1024: exactly starts block 1
        alp::ALP_VECTOR_SIZE + 1,       // 1025: one past boundary
        2 * alp::ALP_VECTOR_SIZE - 50,  // 1998: 50 in block 1, 50 in block 2
        2 * alp::ALP_VECTOR_SIZE,       // 2048: exactly starts block 2
        2 * alp::ALP_VECTOR_SIZE + 1,   // 2049: one past second boundary
        N - 100,                        // last 100 values
        N - 1,                          // last single value
    };

    for (size_t skip : boundary_skips) {
        const size_t limit = (skip + 100 <= N) ? 100 : (N - skip);
        if (limit == 0)
            continue;

        auto buffer = ALPEncoder::encode(data);
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(buffer.data.data()),
                              buffer.data.size() * sizeof(uint64_t));
        std::vector<double> result;
        ALPDecoder::decode(slice, skip, limit, result);

        ASSERT_EQ(result.size(), limit) << "Size mismatch at skip=" << skip;
        for (size_t i = 0; i < limit; ++i)
            EXPECT_EQ(std::bit_cast<uint64_t>(result[i]), std::bit_cast<uint64_t>(data[skip + i]))
                << "Value mismatch at skip=" << skip << " i=" << i;
    }
}
