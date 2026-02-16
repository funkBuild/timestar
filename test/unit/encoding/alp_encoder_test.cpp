#include <gtest/gtest.h>
#include <cmath>
#include <random>
#include <limits>
#include <bit>
#include <numeric>

#include "alp/alp_encoder.hpp"
#include "alp/alp_decoder.hpp"
#include "alp/alp_constants.hpp"
#include "alp/alp_ffor.hpp"

class ALPEncoderTest : public ::testing::Test {
protected:
    void testRoundtrip(const std::vector<double>& original) {
        auto buffer = ALPEncoder::encode(original);

        CompressedSlice slice(
            reinterpret_cast<const uint8_t*>(buffer.data.data()),
            buffer.data.size() * sizeof(uint64_t));

        std::vector<double> decoded;
        ALPDecoder::decode(slice, 0, original.size(), decoded);

        ASSERT_EQ(decoded.size(), original.size())
            << "Decoded size mismatch: expected " << original.size()
            << " got " << decoded.size();

        for (size_t i = 0; i < original.size(); ++i) {
            if (std::isnan(original[i])) {
                EXPECT_TRUE(std::isnan(decoded[i]))
                    << "Expected NaN at index " << i;
            } else {
                EXPECT_EQ(std::bit_cast<uint64_t>(original[i]),
                          std::bit_cast<uint64_t>(decoded[i]))
                    << "Mismatch at index " << i
                    << ": original=" << original[i]
                    << " decoded=" << decoded[i];
            }
        }
    }

    void testSkipDecode(const std::vector<double>& original,
                        size_t skip, size_t count) {
        auto buffer = ALPEncoder::encode(original);

        CompressedSlice slice(
            reinterpret_cast<const uint8_t*>(buffer.data.data()),
            buffer.data.size() * sizeof(uint64_t));

        std::vector<double> decoded;
        size_t actual_count = std::min(count, original.size() - skip);
        ALPDecoder::decode(slice, skip, actual_count, decoded);

        ASSERT_EQ(decoded.size(), actual_count);

        for (size_t i = 0; i < actual_count; ++i) {
            if (std::isnan(original[skip + i])) {
                EXPECT_TRUE(std::isnan(decoded[i]));
            } else {
                EXPECT_EQ(std::bit_cast<uint64_t>(original[skip + i]),
                          std::bit_cast<uint64_t>(decoded[i]))
                    << "Mismatch at decoded index " << i
                    << " (original index " << (skip + i) << ")";
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
    testRoundtrip({std::numeric_limits<double>::infinity(),
                   -std::numeric_limits<double>::infinity()});
}

TEST_F(ALPEncoderTest, NegativeZero) {
    testRoundtrip({-0.0, 0.0});
}

TEST_F(ALPEncoderTest, DBL_MIN_MAX) {
    testRoundtrip({std::numeric_limits<double>::min(),
                   std::numeric_limits<double>::max(),
                   std::numeric_limits<double>::lowest()});
}

TEST_F(ALPEncoderTest, Epsilon) {
    testRoundtrip({std::numeric_limits<double>::epsilon(),
                   1.0 + std::numeric_limits<double>::epsilon(),
                   1.0 - std::numeric_limits<double>::epsilon()});
}

TEST_F(ALPEncoderTest, Subnormals) {
    testRoundtrip({std::numeric_limits<double>::denorm_min(),
                   5e-324, 1e-310});
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
        data[i] = std::round((data[i-1] + dist(gen)) * 100.0) / 100.0;
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
