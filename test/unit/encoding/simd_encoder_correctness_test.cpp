#include <gtest/gtest.h>
#include "../../../lib/encoding/float/float_encoder.hpp"
#include "../../../lib/encoding/float/float_encoder_simd.hpp"
#include "../../../lib/encoding/float/float_encoder_avx512.hpp"
#include "../../../lib/encoding/float/float_decoder.hpp"
#include "../../../lib/encoding/float/float_decoder_simd.hpp"
#include "../../../lib/encoding/float/float_decoder_avx512.hpp"
#include "../../../lib/encoding/integer/integer_encoder.hpp"
#include "../../../lib/encoding/integer/integer_encoder_simd.hpp"
#include "../../../lib/encoding/integer/integer_encoder_avx512.hpp"
#include "../../../lib/encoding/zigzag.hpp"
#include <vector>
#include <cmath>
#include <random>
#include <limits>
#include <numeric>
#include <bit>

// ============================================================================
// Helper: decode a CompressedBuffer using a specific float decoder
// ============================================================================

static std::vector<double> decodeWithBasic(const CompressedBuffer& encoded, size_t count) {
    auto rawBytes = reinterpret_cast<const uint8_t*>(encoded.data.data());
    size_t byteLen = encoded.data.size() * sizeof(uint64_t);
    CompressedSlice slice(rawBytes, byteLen);
    std::vector<double> out;
    FloatEncoderBasic::decode(slice, 0, count, out);
    return out;
}

static std::vector<double> decodeWithSIMD(const CompressedBuffer& encoded, size_t count) {
    auto rawBytes = reinterpret_cast<const uint8_t*>(encoded.data.data());
    size_t byteLen = encoded.data.size() * sizeof(uint64_t);
    CompressedSlice slice(rawBytes, byteLen);
    std::vector<double> out;
    FloatDecoderSIMD::decode(slice, 0, count, out);
    return out;
}

static std::vector<double> decodeWithAVX512(const CompressedBuffer& encoded, size_t count) {
    auto rawBytes = reinterpret_cast<const uint8_t*>(encoded.data.data());
    size_t byteLen = encoded.data.size() * sizeof(uint64_t);
    CompressedSlice slice(rawBytes, byteLen);
    std::vector<double> out;
    FloatDecoderAVX512::decode(slice, 0, count, out);
    return out;
}

// ============================================================================
// Float Encoder SIMD Tests
// ============================================================================

class FloatEncoderSIMDCorrectnessTest : public ::testing::Test {
protected:
    bool avx2Available_ = false;
    bool avx512Available_ = false;

    void SetUp() override {
        avx2Available_ = FloatEncoderSIMD::isAvailable();
        avx512Available_ = FloatEncoderAVX512::isAvailable();
    }

    // Verify that SIMD-encoded data decodes to the same values as scalar-encoded data
    void verifyEncoderEquivalence(const std::vector<double>& values, const std::string& description) {
        CompressedBuffer scalarEncoded = FloatEncoderBasic::encode(values);
        std::vector<double> scalarDecoded = decodeWithBasic(scalarEncoded, values.size());

        ASSERT_EQ(scalarDecoded.size(), values.size())
            << description << ": scalar decode size mismatch";

        if (avx2Available_) {
            CompressedBuffer simdEncoded = FloatEncoderSIMD::encode(values);
            std::vector<double> simdDecoded = decodeWithBasic(simdEncoded, values.size());

            ASSERT_EQ(simdDecoded.size(), values.size())
                << description << ": SIMD encode size mismatch";

            for (size_t i = 0; i < values.size(); i++) {
                if (std::isnan(values[i])) {
                    EXPECT_TRUE(std::isnan(simdDecoded[i]))
                        << description << ": SIMD NaN mismatch at index " << i;
                } else {
                    EXPECT_EQ(std::bit_cast<uint64_t>(scalarDecoded[i]),
                              std::bit_cast<uint64_t>(simdDecoded[i]))
                        << description << ": SIMD value mismatch at index " << i
                        << " (scalar=" << scalarDecoded[i] << ", simd=" << simdDecoded[i] << ")";
                }
            }
        }

        if (avx512Available_) {
            CompressedBuffer avx512Encoded = FloatEncoderAVX512::encode(values);
            std::vector<double> avx512Decoded = decodeWithBasic(avx512Encoded, values.size());

            ASSERT_EQ(avx512Decoded.size(), values.size())
                << description << ": AVX512 encode size mismatch";

            for (size_t i = 0; i < values.size(); i++) {
                if (std::isnan(values[i])) {
                    EXPECT_TRUE(std::isnan(avx512Decoded[i]))
                        << description << ": AVX512 NaN mismatch at index " << i;
                } else {
                    EXPECT_EQ(std::bit_cast<uint64_t>(scalarDecoded[i]),
                              std::bit_cast<uint64_t>(avx512Decoded[i]))
                        << description << ": AVX512 value mismatch at index " << i
                        << " (scalar=" << scalarDecoded[i] << ", avx512=" << avx512Decoded[i] << ")";
                }
            }
        }
    }

    // Verify that all decoder variants produce the same output for a given encoded buffer
    void verifyDecoderEquivalence(const CompressedBuffer& encoded, size_t count, const std::string& description) {
        std::vector<double> scalarDecoded = decodeWithBasic(encoded, count);
        ASSERT_EQ(scalarDecoded.size(), count)
            << description << ": scalar decode size mismatch";

        if (avx2Available_) {
            std::vector<double> simdDecoded = decodeWithSIMD(encoded, count);
            ASSERT_EQ(simdDecoded.size(), count)
                << description << ": SIMD decode size mismatch";

            for (size_t i = 0; i < count; i++) {
                if (std::isnan(scalarDecoded[i])) {
                    EXPECT_TRUE(std::isnan(simdDecoded[i]))
                        << description << ": SIMD decoder NaN mismatch at index " << i;
                } else {
                    EXPECT_EQ(std::bit_cast<uint64_t>(scalarDecoded[i]),
                              std::bit_cast<uint64_t>(simdDecoded[i]))
                        << description << ": SIMD decoder mismatch at index " << i
                        << " (scalar=" << scalarDecoded[i] << ", simd=" << simdDecoded[i] << ")";
                }
            }
        }

        if (avx512Available_) {
            std::vector<double> avx512Decoded = decodeWithAVX512(encoded, count);
            ASSERT_EQ(avx512Decoded.size(), count)
                << description << ": AVX512 decode size mismatch";

            for (size_t i = 0; i < count; i++) {
                if (std::isnan(scalarDecoded[i])) {
                    EXPECT_TRUE(std::isnan(avx512Decoded[i]))
                        << description << ": AVX512 decoder NaN mismatch at index " << i;
                } else {
                    EXPECT_EQ(std::bit_cast<uint64_t>(scalarDecoded[i]),
                              std::bit_cast<uint64_t>(avx512Decoded[i]))
                        << description << ": AVX512 decoder mismatch at index " << i
                        << " (scalar=" << scalarDecoded[i] << ", avx512=" << avx512Decoded[i] << ")";
                }
            }
        }
    }
};

// --- Encoder correctness: encode with SIMD, verify roundtrip ---

TEST_F(FloatEncoderSIMDCorrectnessTest, EmptyInput) {
    verifyEncoderEquivalence({}, "empty");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, SingleValue) {
    verifyEncoderEquivalence({42.0}, "single value");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, TwoValues) {
    verifyEncoderEquivalence({1.0, 2.0}, "two values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, ThreeValues) {
    verifyEncoderEquivalence({1.0, 2.0, 3.0}, "three values (less than SIMD width)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, FourValues_ExactSIMDWidth) {
    verifyEncoderEquivalence({10.0, 20.0, 30.0, 40.0}, "four values (exact AVX2 width)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, FiveValues_OneOverSIMDWidth) {
    verifyEncoderEquivalence({1.1, 2.2, 3.3, 4.4, 5.5}, "five values (one over AVX2 width)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, EightValues_ExactAVX512Width) {
    verifyEncoderEquivalence({1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0},
                              "eight values (exact AVX512 width)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, NineValues_OneOverAVX512Width) {
    verifyEncoderEquivalence({1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0},
                              "nine values (one over AVX512 width)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, AllIdentical) {
    std::vector<double> values(100, 42.0);
    verifyEncoderEquivalence(values, "100 identical values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, AllZeros) {
    std::vector<double> values(100, 0.0);
    verifyEncoderEquivalence(values, "100 zeros");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, ZeroAndNegZero) {
    std::vector<double> values = {0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0, 0.0, -0.0};
    verifyEncoderEquivalence(values, "zero and negative zero");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, Infinities) {
    double inf = std::numeric_limits<double>::infinity();
    std::vector<double> values = {inf, -inf, inf, 0.0, -inf, 1.0, inf, -inf, 0.5, inf};
    verifyEncoderEquivalence(values, "infinities");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, NaNValues) {
    double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<double> values = {nan, 1.0, nan, 2.0, nan, 3.0, nan, 4.0, nan, 5.0};
    verifyEncoderEquivalence(values, "NaN values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, Denormals) {
    double denorm = std::numeric_limits<double>::denorm_min();
    std::vector<double> values = {denorm, -denorm, denorm * 2, denorm * 3,
                                   0.0, denorm, -denorm, 1.0, denorm, -denorm};
    verifyEncoderEquivalence(values, "denormals");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, MaxAndMinValues) {
    double maxv = std::numeric_limits<double>::max();
    double minv = std::numeric_limits<double>::lowest();
    double tiniest = std::numeric_limits<double>::min();  // smallest positive normal
    std::vector<double> values = {maxv, minv, tiniest, -tiniest, maxv, 0.0, minv, tiniest, maxv, minv};
    verifyEncoderEquivalence(values, "max and min boundary values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, MixedSpecialValues) {
    double nan = std::numeric_limits<double>::quiet_NaN();
    double inf = std::numeric_limits<double>::infinity();
    double denorm = std::numeric_limits<double>::denorm_min();
    double maxv = std::numeric_limits<double>::max();
    std::vector<double> values = {0.0, -0.0, nan, inf, -inf, denorm, -denorm, maxv, 1.0, 0.5};
    verifyEncoderEquivalence(values, "mixed special values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, MonotonicallyIncreasing) {
    std::vector<double> values(200);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = static_cast<double>(i) * 0.1;
    }
    verifyEncoderEquivalence(values, "monotonically increasing");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, MonotonicallyDecreasing) {
    std::vector<double> values(200);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1000.0 - static_cast<double>(i) * 0.5;
    }
    verifyEncoderEquivalence(values, "monotonically decreasing");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, SineWave) {
    std::vector<double> values(256);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = std::sin(static_cast<double>(i) * 0.1);
    }
    verifyEncoderEquivalence(values, "sine wave");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, RandomValues) {
    std::mt19937_64 rng(12345);
    std::uniform_real_distribution<double> dist(-1e6, 1e6);

    std::vector<double> values(1000);
    for (auto& v : values) {
        v = dist(rng);
    }
    verifyEncoderEquivalence(values, "1000 random values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, LargeDataset) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> dist(-1e15, 1e15);

    std::vector<double> values(10000);
    for (auto& v : values) {
        v = dist(rng);
    }
    verifyEncoderEquivalence(values, "10000 random values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, SmallDifferences) {
    // Values that differ only in least significant bits
    std::vector<double> values(100);
    double base = 1000000.0;
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = base + static_cast<double>(i) * 1e-10;
    }
    verifyEncoderEquivalence(values, "small differences (near-identical values)");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, AlternatingSign) {
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = (i % 2 == 0) ? static_cast<double>(i) : -static_cast<double>(i);
    }
    verifyEncoderEquivalence(values, "alternating sign");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, RepeatingPattern) {
    std::vector<double> pattern = {1.0, 2.0, 3.0, 2.0, 1.0};
    std::vector<double> values;
    for (int rep = 0; rep < 50; rep++) {
        values.insert(values.end(), pattern.begin(), pattern.end());
    }
    verifyEncoderEquivalence(values, "repeating pattern (250 values)");
}

// --- Decoder correctness: verify all decoders agree on scalar-encoded data ---

TEST_F(FloatEncoderSIMDCorrectnessTest, DecoderEquivalence_SmallSizes) {
    // Test every size from 1 to 20 to exercise boundary conditions
    for (size_t n = 1; n <= 20; n++) {
        std::vector<double> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = static_cast<double>(i) * 1.5 + 0.1;
        }
        CompressedBuffer encoded = FloatEncoderBasic::encode(values);
        verifyDecoderEquivalence(encoded, n, "decoder small size=" + std::to_string(n));
    }
}

TEST_F(FloatEncoderSIMDCorrectnessTest, DecoderEquivalence_RandomData) {
    std::mt19937_64 rng(99);
    std::uniform_real_distribution<double> dist(-1e8, 1e8);

    std::vector<double> values(500);
    for (auto& v : values) {
        v = dist(rng);
    }

    CompressedBuffer encoded = FloatEncoderBasic::encode(values);
    verifyDecoderEquivalence(encoded, values.size(), "decoder 500 random values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, DecoderEquivalence_SpecialValues) {
    double nan = std::numeric_limits<double>::quiet_NaN();
    double inf = std::numeric_limits<double>::infinity();
    std::vector<double> values = {0.0, -0.0, inf, -inf, nan, 1.0, -1.0, 0.5, nan, inf};

    CompressedBuffer encoded = FloatEncoderBasic::encode(values);
    verifyDecoderEquivalence(encoded, values.size(), "decoder special values");
}

TEST_F(FloatEncoderSIMDCorrectnessTest, CrossEncoderDecoderCompatibility) {
    // Verify: data encoded by any encoder can be decoded by any decoder
    std::mt19937_64 rng(777);
    std::uniform_real_distribution<double> dist(-100.0, 100.0);

    std::vector<double> values(128);
    for (auto& v : values) {
        v = dist(rng);
    }

    // Encode with scalar
    CompressedBuffer scalarEncoded = FloatEncoderBasic::encode(values);
    std::vector<double> scalarBaseline = decodeWithBasic(scalarEncoded, values.size());

    // Each SIMD encoder's output should be decodable by all decoders
    if (avx2Available_) {
        CompressedBuffer simdEncoded = FloatEncoderSIMD::encode(values);
        verifyDecoderEquivalence(simdEncoded, values.size(), "cross-compat SIMD encoded");

        // Verify SIMD-encoded data produces correct roundtrip
        std::vector<double> simdRoundtrip = decodeWithBasic(simdEncoded, values.size());
        for (size_t i = 0; i < values.size(); i++) {
            EXPECT_EQ(std::bit_cast<uint64_t>(scalarBaseline[i]),
                      std::bit_cast<uint64_t>(simdRoundtrip[i]))
                << "Cross-compat SIMD->Basic mismatch at index " << i;
        }
    }

    if (avx512Available_) {
        CompressedBuffer avx512Encoded = FloatEncoderAVX512::encode(values);
        verifyDecoderEquivalence(avx512Encoded, values.size(), "cross-compat AVX512 encoded");

        std::vector<double> avx512Roundtrip = decodeWithBasic(avx512Encoded, values.size());
        for (size_t i = 0; i < values.size(); i++) {
            EXPECT_EQ(std::bit_cast<uint64_t>(scalarBaseline[i]),
                      std::bit_cast<uint64_t>(avx512Roundtrip[i]))
                << "Cross-compat AVX512->Basic mismatch at index " << i;
        }
    }
}

// ============================================================================
// Integer Encoder SIMD Tests
// ============================================================================

class IntegerEncoderSIMDCorrectnessTest : public ::testing::Test {
protected:
    bool avx2Available_ = false;
    bool avx512Available_ = false;

    void SetUp() override {
        avx2Available_ = IntegerEncoderSIMD::isAvailable();
        avx512Available_ = IntegerEncoderAVX512::isAvailable();
    }

    // Encode with a given encoder, decode, and return decoded values
    static std::vector<uint64_t> roundtrip(
        AlignedBuffer (*encodeFn)(const std::vector<uint64_t>&),
        std::pair<size_t, size_t> (*decodeFn)(Slice&, unsigned int, std::vector<uint64_t>&, uint64_t, uint64_t),
        const std::vector<uint64_t>& values)
    {
        AlignedBuffer encoded = encodeFn(values);
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded;
        decodeFn(slice, values.size(), decoded, 0, UINT64_MAX);
        return decoded;
    }

    void verifyEncoderEquivalence(const std::vector<uint64_t>& values, const std::string& description) {
        // Scalar baseline
        std::vector<uint64_t> scalarResult = roundtrip(
            IntegerEncoderBasic::encode, IntegerEncoderBasic::decode, values);

        ASSERT_EQ(scalarResult.size(), values.size())
            << description << ": scalar roundtrip size mismatch";
        for (size_t i = 0; i < values.size(); i++) {
            EXPECT_EQ(scalarResult[i], values[i])
                << description << ": scalar roundtrip value mismatch at index " << i;
        }

        if (avx2Available_) {
            std::vector<uint64_t> simdResult = roundtrip(
                IntegerEncoderSIMD::encode, IntegerEncoderSIMD::decode, values);

            ASSERT_EQ(simdResult.size(), values.size())
                << description << ": SIMD roundtrip size mismatch";
            for (size_t i = 0; i < values.size(); i++) {
                EXPECT_EQ(simdResult[i], values[i])
                    << description << ": SIMD roundtrip mismatch at index " << i
                    << " (expected=" << values[i] << ", got=" << simdResult[i] << ")";
            }

            // Also cross-check: SIMD encoded data decoded by scalar
            AlignedBuffer simdEncoded = IntegerEncoderSIMD::encode(values);
            Slice simdSlice(simdEncoded.data.data(), simdEncoded.size());
            std::vector<uint64_t> crossDecoded;
            IntegerEncoderBasic::decode(simdSlice, values.size(), crossDecoded, 0, UINT64_MAX);

            ASSERT_EQ(crossDecoded.size(), values.size())
                << description << ": SIMD->Basic cross decode size mismatch";
            for (size_t i = 0; i < values.size(); i++) {
                EXPECT_EQ(crossDecoded[i], values[i])
                    << description << ": SIMD->Basic cross decode mismatch at index " << i;
            }
        }

        if (avx512Available_) {
            std::vector<uint64_t> avx512Result = roundtrip(
                IntegerEncoderAVX512::encode, IntegerEncoderAVX512::decode, values);

            ASSERT_EQ(avx512Result.size(), values.size())
                << description << ": AVX512 roundtrip size mismatch";
            for (size_t i = 0; i < values.size(); i++) {
                EXPECT_EQ(avx512Result[i], values[i])
                    << description << ": AVX512 roundtrip mismatch at index " << i
                    << " (expected=" << values[i] << ", got=" << avx512Result[i] << ")";
            }

            // Cross-check: AVX512 encoded data decoded by scalar
            AlignedBuffer avx512Encoded = IntegerEncoderAVX512::encode(values);
            Slice avx512Slice(avx512Encoded.data.data(), avx512Encoded.size());
            std::vector<uint64_t> crossDecoded;
            IntegerEncoderBasic::decode(avx512Slice, values.size(), crossDecoded, 0, UINT64_MAX);

            ASSERT_EQ(crossDecoded.size(), values.size())
                << description << ": AVX512->Basic cross decode size mismatch";
            for (size_t i = 0; i < values.size(); i++) {
                EXPECT_EQ(crossDecoded[i], values[i])
                    << description << ": AVX512->Basic cross decode mismatch at index " << i;
            }
        }
    }
};

TEST_F(IntegerEncoderSIMDCorrectnessTest, EmptyInput) {
    verifyEncoderEquivalence({}, "empty");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, SingleValue) {
    verifyEncoderEquivalence({1000}, "single value");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, TwoValues) {
    verifyEncoderEquivalence({1000, 2000}, "two values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, ThreeValues) {
    verifyEncoderEquivalence({1000, 2000, 3000}, "three values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, FourValues) {
    verifyEncoderEquivalence({1000, 2000, 3000, 4000}, "four values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, NineValues) {
    verifyEncoderEquivalence({100, 200, 300, 400, 500, 600, 700, 800, 900}, "nine values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, SixteenValues) {
    std::vector<uint64_t> values(16);
    for (size_t i = 0; i < 16; i++) {
        values[i] = 1000 + i * 100;
    }
    verifyEncoderEquivalence(values, "sixteen values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, MonotonicallyIncreasingTimestamps) {
    // Simulate nanosecond timestamps with 1-second spacing
    std::vector<uint64_t> values(200);
    uint64_t start = 1700000000000000000ULL;
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = start + i * 1000000000ULL;
    }
    verifyEncoderEquivalence(values, "monotonic timestamps 200 values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, IrregularTimestamps) {
    // Timestamps with varying intervals
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<uint64_t> intervalDist(100000, 10000000);

    std::vector<uint64_t> values(100);
    values[0] = 1700000000000000000ULL;
    for (size_t i = 1; i < values.size(); i++) {
        values[i] = values[i - 1] + intervalDist(rng);
    }
    verifyEncoderEquivalence(values, "irregular timestamps");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, ConstantValues) {
    std::vector<uint64_t> values(100, 42);
    verifyEncoderEquivalence(values, "100 constant values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, ConstantDelta) {
    // All deltas are the same (constant increment)
    std::vector<uint64_t> values(100);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1000 + i * 10;
    }
    verifyEncoderEquivalence(values, "constant delta");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, ZeroValues) {
    std::vector<uint64_t> values(50, 0);
    verifyEncoderEquivalence(values, "all zeros");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, LargeValues) {
    std::vector<uint64_t> values = {
        UINT64_MAX - 100, UINT64_MAX - 50, UINT64_MAX - 25, UINT64_MAX - 10,
        UINT64_MAX - 5, UINT64_MAX - 2, UINT64_MAX - 1, UINT64_MAX,
    };
    verifyEncoderEquivalence(values, "large values near UINT64_MAX");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, SmallValues) {
    std::vector<uint64_t> values = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    verifyEncoderEquivalence(values, "small sequential values");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, LargeDataset) {
    // Simulates a realistic workload: 10k timestamps
    std::vector<uint64_t> values(10000);
    uint64_t ts = 1700000000000000000ULL;
    std::mt19937_64 rng(123);
    std::uniform_int_distribution<uint64_t> jitter(0, 1000);

    for (size_t i = 0; i < values.size(); i++) {
        ts += 1000000000ULL + jitter(rng);  // ~1 second + jitter
        values[i] = ts;
    }
    verifyEncoderEquivalence(values, "10000 timestamps with jitter");
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, AlternatingSizes) {
    // Test each size from 1 to 25 to exercise SIMD boundary conditions
    for (size_t n = 1; n <= 25; n++) {
        std::vector<uint64_t> values(n);
        for (size_t i = 0; i < n; i++) {
            values[i] = 1000 + i * 100;
        }
        verifyEncoderEquivalence(values, "size=" + std::to_string(n));
    }
}

TEST_F(IntegerEncoderSIMDCorrectnessTest, TimeRangeFiltering) {
    // Test that decode with time range filtering works correctly across encoders
    std::vector<uint64_t> values(100);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = 1000 + i * 10;
    }

    uint64_t minTime = 1200;
    uint64_t maxTime = 1500;

    // Scalar baseline
    AlignedBuffer scalarEncoded = IntegerEncoderBasic::encode(values);
    Slice scalarSlice(scalarEncoded.data.data(), scalarEncoded.size());
    std::vector<uint64_t> scalarFiltered;
    auto [scalarSkipped, scalarAdded] = IntegerEncoderBasic::decode(
        scalarSlice, values.size(), scalarFiltered, minTime, maxTime);

    if (avx2Available_) {
        AlignedBuffer simdEncoded = IntegerEncoderSIMD::encode(values);
        Slice simdSlice(simdEncoded.data.data(), simdEncoded.size());
        std::vector<uint64_t> simdFiltered;
        auto [simdSkipped, simdAdded] = IntegerEncoderSIMD::decode(
            simdSlice, values.size(), simdFiltered, minTime, maxTime);

        EXPECT_EQ(scalarSkipped, simdSkipped) << "SIMD skip count mismatch";
        EXPECT_EQ(scalarAdded, simdAdded) << "SIMD add count mismatch";
        ASSERT_EQ(scalarFiltered.size(), simdFiltered.size()) << "SIMD filtered size mismatch";
        for (size_t i = 0; i < scalarFiltered.size(); i++) {
            EXPECT_EQ(scalarFiltered[i], simdFiltered[i])
                << "SIMD filtered value mismatch at index " << i;
        }
    }

    if (avx512Available_) {
        AlignedBuffer avx512Encoded = IntegerEncoderAVX512::encode(values);
        Slice avx512Slice(avx512Encoded.data.data(), avx512Encoded.size());
        std::vector<uint64_t> avx512Filtered;
        auto [avx512Skipped, avx512Added] = IntegerEncoderAVX512::decode(
            avx512Slice, values.size(), avx512Filtered, minTime, maxTime);

        EXPECT_EQ(scalarSkipped, avx512Skipped) << "AVX512 skip count mismatch";
        EXPECT_EQ(scalarAdded, avx512Added) << "AVX512 add count mismatch";
        ASSERT_EQ(scalarFiltered.size(), avx512Filtered.size()) << "AVX512 filtered size mismatch";
        for (size_t i = 0; i < scalarFiltered.size(); i++) {
            EXPECT_EQ(scalarFiltered[i], avx512Filtered[i])
                << "AVX512 filtered value mismatch at index " << i;
        }
    }
}

// --- ZigZag SIMD correctness (implicitly tested via integer encoder, but also explicit) ---

TEST_F(IntegerEncoderSIMDCorrectnessTest, ZigZagRoundtripVariousValues) {
    // Verify that the zigzag encode/decode roundtrip is correct for various values
    std::vector<int64_t> testValues = {
        0, 1, -1, 2, -2, 127, -128, 255, -256,
        INT64_MAX, INT64_MIN, INT64_MAX - 1, INT64_MIN + 1,
        1000000, -1000000, 123456789, -123456789
    };

    for (int64_t v : testValues) {
        uint64_t encoded = ZigZag::zigzagEncode(v);
        int64_t decoded = ZigZag::zigzagDecode(encoded);
        EXPECT_EQ(v, decoded) << "ZigZag roundtrip failed for value " << v;
    }
}
