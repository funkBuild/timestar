#include <gtest/gtest.h>
#include "integer_encoder.hpp"
#include "integer/integer_encoder_ffor.hpp"
#include "zigzag.hpp"
#include "tsm.hpp"
#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

#include <vector>
#include <cstdint>
#include <climits>
#include <variant>
#include <string>
#include <numeric>
#include <random>

// The variant used by the HTTP write handler and internally for field values.
using TimeStarValue = std::variant<double, bool, std::string, int64_t>;

class IntegerTypeTest : public ::testing::Test {
protected:
    // Helper: ZigZag-encode a vector of int64_t, run through IntegerEncoder
    // encode/decode, ZigZag-decode back, and verify equality with the original.
    void verifyIntegerEncoderRoundTrip(const std::vector<int64_t>& original) {
        // ZigZag encode int64_t -> uint64_t
        std::vector<uint64_t> zigzagged = ZigZag::zigzagEncodeVector(original);
        ASSERT_EQ(zigzagged.size(), original.size());

        // Encode with IntegerEncoder
        AlignedBuffer encoded = IntegerEncoder::encode(zigzagged);
        ASSERT_GT(encoded.size(), 0u) << "Encoded buffer should not be empty";

        // Decode
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded;
        auto [skipped, added] = IntegerEncoder::decode(
            slice, static_cast<unsigned int>(zigzagged.size()), decoded, 0, UINT64_MAX);

        ASSERT_EQ(skipped, 0u);
        ASSERT_EQ(added, original.size());
        ASSERT_EQ(decoded.size(), original.size());

        // ZigZag decode uint64_t -> int64_t
        std::vector<int64_t> recovered = ZigZag::zigzagDecodeVector(decoded);
        ASSERT_EQ(recovered.size(), original.size());

        for (size_t i = 0; i < original.size(); i++) {
            EXPECT_EQ(recovered[i], original[i])
                << "IntegerEncoder round-trip mismatch at index " << i
                << ": expected " << original[i] << " got " << recovered[i];
        }
    }

    // Helper: same round-trip but using IntegerEncoderFFOR.
    void verifyFFORRoundTrip(const std::vector<int64_t>& original) {
        std::vector<uint64_t> zigzagged = ZigZag::zigzagEncodeVector(original);
        ASSERT_EQ(zigzagged.size(), original.size());

        AlignedBuffer encoded = IntegerEncoderFFOR::encode(zigzagged);
        ASSERT_GT(encoded.size(), 0u) << "FFOR encoded buffer should not be empty";

        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded;
        auto [skipped, added] = IntegerEncoderFFOR::decode(
            slice, static_cast<unsigned int>(zigzagged.size()), decoded, 0, UINT64_MAX);

        ASSERT_EQ(skipped, 0u);
        ASSERT_EQ(added, original.size());
        ASSERT_EQ(decoded.size(), original.size());

        std::vector<int64_t> recovered = ZigZag::zigzagDecodeVector(decoded);
        ASSERT_EQ(recovered.size(), original.size());

        for (size_t i = 0; i < original.size(); i++) {
            EXPECT_EQ(recovered[i], original[i])
                << "FFOR round-trip mismatch at index " << i
                << ": expected " << original[i] << " got " << recovered[i];
        }
    }
};

// ============================================================================
// 1. IntegerEncoder int64_t round-trip tests
// ============================================================================

TEST_F(IntegerTypeTest, IntegerEncoder_PositiveValues) {
    std::vector<int64_t> values = {1, 2, 3, 100, 1000, 999999};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_NegativeValues) {
    std::vector<int64_t> values = {-1, -2, -3, -100, -1000, -999999};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_Zero) {
    std::vector<int64_t> values = {0};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_AllZeros) {
    std::vector<int64_t> values(100, 0);
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_INT64_MIN) {
    std::vector<int64_t> values = {INT64_MIN};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_INT64_MAX) {
    std::vector<int64_t> values = {INT64_MAX};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_ExtremeBoundaries) {
    std::vector<int64_t> values = {INT64_MIN, INT64_MAX, 0, INT64_MIN, INT64_MAX};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_MixedPositiveNegative) {
    std::vector<int64_t> values = {-50, 100, -200, 300, -400, 500, 0, -1, 1};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_AlternatingSign) {
    std::vector<int64_t> values;
    for (int i = 0; i < 200; i++) {
        values.push_back(i % 2 == 0 ? static_cast<int64_t>(i) * 1000
                                     : -static_cast<int64_t>(i) * 1000);
    }
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_LargeBlock) {
    // 1500 values: a mix of negative, zero, and positive
    std::vector<int64_t> values(1500);
    for (int i = 0; i < 1500; i++) {
        values[i] = static_cast<int64_t>(i) - 750;
    }
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_LargeBlockRandomized) {
    std::mt19937_64 rng(42);  // deterministic seed
    std::uniform_int_distribution<int64_t> dist(INT64_MIN / 2, INT64_MAX / 2);
    std::vector<int64_t> values(2000);
    for (auto& v : values) {
        v = dist(rng);
    }
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_SingleNegativeValue) {
    std::vector<int64_t> values = {-42};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_IncreasingThenDecreasing) {
    std::vector<int64_t> values;
    for (int64_t i = -500; i <= 500; i++) {
        values.push_back(i);
    }
    for (int64_t i = 499; i >= -500; i--) {
        values.push_back(i);
    }
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_PowersOfTwo) {
    std::vector<int64_t> values;
    for (int shift = 0; shift < 62; shift++) {
        values.push_back(static_cast<int64_t>(1LL << shift));
        values.push_back(-static_cast<int64_t>(1LL << shift));
    }
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_NearZero) {
    std::vector<int64_t> values = {-3, -2, -1, 0, 1, 2, 3};
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, IntegerEncoder_RepeatedValue) {
    std::vector<int64_t> values(500, -12345);
    verifyIntegerEncoderRoundTrip(values);
}

// ============================================================================
// 2. IntegerEncoderFFOR round-trip tests with ZigZag-encoded int64_t
// ============================================================================

TEST_F(IntegerTypeTest, FFOR_UniformValues) {
    std::vector<int64_t> values(256, 42);
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_UniformNegativeValues) {
    std::vector<int64_t> values(256, -42);
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_SmallRangePositive) {
    // All values within a narrow positive range
    std::vector<int64_t> values;
    for (int i = 0; i < 512; i++) {
        values.push_back(1000 + (i % 10));
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_SmallRangeNegative) {
    // All values within a narrow negative range
    std::vector<int64_t> values;
    for (int i = 0; i < 512; i++) {
        values.push_back(-1000 - (i % 10));
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_SmallRangeMixed) {
    // Values straddling zero
    std::vector<int64_t> values;
    for (int i = 0; i < 512; i++) {
        values.push_back(-5 + (i % 11));
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_WideRange) {
    // Wide spread of values
    std::vector<int64_t> values;
    for (int i = 0; i < 1024; i++) {
        values.push_back(static_cast<int64_t>(i) * 1000000 - 500000000);
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_ExtremeValues) {
    std::vector<int64_t> values = {INT64_MIN, INT64_MAX, 0, INT64_MIN, INT64_MAX};
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_NearInt64Boundaries) {
    std::vector<int64_t> values = {
        INT64_MIN, INT64_MIN + 1, INT64_MIN + 2,
        INT64_MAX - 2, INT64_MAX - 1, INT64_MAX,
        0, -1, 1
    };
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_LargeBlockRandomized) {
    std::mt19937_64 rng(123);
    std::uniform_int_distribution<int64_t> dist(INT64_MIN / 2, INT64_MAX / 2);
    std::vector<int64_t> values(2048);
    for (auto& v : values) {
        v = dist(rng);
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_SingleValue) {
    std::vector<int64_t> values = {-99999};
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_TwoValues) {
    std::vector<int64_t> values = {INT64_MIN, INT64_MAX};
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_ExceedsBlockSize) {
    // FFOR block size is 1024; test with values exceeding one block
    std::vector<int64_t> values(1500);
    for (int i = 0; i < 1500; i++) {
        values[i] = static_cast<int64_t>(i) - 750;
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_MultipleFullBlocks) {
    // 3 full FFOR blocks + a partial block
    std::vector<int64_t> values(3500);
    for (size_t i = 0; i < values.size(); i++) {
        values[i] = static_cast<int64_t>(i % 1000) - 500;
    }
    verifyFFORRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_AllZeros) {
    std::vector<int64_t> values(1024, 0);
    verifyFFORRoundTrip(values);
}

// ============================================================================
// 3. TSMValueType enum tests
// ============================================================================

TEST_F(IntegerTypeTest, TSMValueType_IntegerHasValue3) {
    EXPECT_EQ(static_cast<int>(TSMValueType::Integer), 3);
}

TEST_F(IntegerTypeTest, TSMValueType_AllEnumValues) {
    EXPECT_EQ(static_cast<int>(TSMValueType::Float), 0);
    EXPECT_EQ(static_cast<int>(TSMValueType::Boolean), 1);
    EXPECT_EQ(static_cast<int>(TSMValueType::String), 2);
    EXPECT_EQ(static_cast<int>(TSMValueType::Integer), 3);
}

TEST_F(IntegerTypeTest, TSMValueType_GetValueType_Int64) {
    // TSM::getValueType<int64_t>() should return TSMValueType::Integer
    constexpr TSMValueType vt = TSM::getValueType<int64_t>();
    EXPECT_EQ(vt, TSMValueType::Integer);
}

TEST_F(IntegerTypeTest, TSMValueType_GetValueType_AllTypes) {
    EXPECT_EQ(TSM::getValueType<double>(), TSMValueType::Float);
    EXPECT_EQ(TSM::getValueType<bool>(), TSMValueType::Boolean);
    EXPECT_EQ(TSM::getValueType<std::string>(), TSMValueType::String);
    EXPECT_EQ(TSM::getValueType<int64_t>(), TSMValueType::Integer);
}

// ============================================================================
// 4. Memory Store variant index tests
// ============================================================================

TEST_F(IntegerTypeTest, Variant_Int64AtIndex3) {
    // The variant std::variant<double, bool, std::string, int64_t>
    // should have int64_t at index 3, matching TSMValueType::Integer.
    TimeStarValue val = static_cast<int64_t>(42);
    EXPECT_EQ(val.index(), 3u);
    EXPECT_EQ(val.index(), static_cast<size_t>(TSMValueType::Integer));
}

TEST_F(IntegerTypeTest, Variant_DoubleAtIndex0) {
    TimeStarValue val = 3.14;
    EXPECT_EQ(val.index(), 0u);
    EXPECT_EQ(val.index(), static_cast<size_t>(TSMValueType::Float));
}

TEST_F(IntegerTypeTest, Variant_BoolAtIndex1) {
    TimeStarValue val = true;
    EXPECT_EQ(val.index(), 1u);
    EXPECT_EQ(val.index(), static_cast<size_t>(TSMValueType::Boolean));
}

TEST_F(IntegerTypeTest, Variant_StringAtIndex2) {
    TimeStarValue val = std::string("hello");
    EXPECT_EQ(val.index(), 2u);
    EXPECT_EQ(val.index(), static_cast<size_t>(TSMValueType::String));
}

TEST_F(IntegerTypeTest, Variant_Int64GetWorks) {
    TimeStarValue val = static_cast<int64_t>(-12345);
    ASSERT_TRUE(std::holds_alternative<int64_t>(val));
    EXPECT_EQ(std::get<int64_t>(val), -12345);
}

TEST_F(IntegerTypeTest, Variant_Int64NegativeExtremes) {
    TimeStarValue val = INT64_MIN;
    ASSERT_TRUE(std::holds_alternative<int64_t>(val));
    EXPECT_EQ(std::get<int64_t>(val), INT64_MIN);
}

TEST_F(IntegerTypeTest, Variant_Int64PositiveExtremes) {
    TimeStarValue val = INT64_MAX;
    ASSERT_TRUE(std::holds_alternative<int64_t>(val));
    EXPECT_EQ(std::get<int64_t>(val), INT64_MAX);
}

TEST_F(IntegerTypeTest, Variant_AllIndicesMatchTSMValueType) {
    // Verify the compile-time guarantee that variant indices line up with the enum
    static_assert(std::is_same_v<std::variant_alternative_t<0, TimeStarValue>, double>,
                  "Index 0 must be double (Float)");
    static_assert(std::is_same_v<std::variant_alternative_t<1, TimeStarValue>, bool>,
                  "Index 1 must be bool (Boolean)");
    static_assert(std::is_same_v<std::variant_alternative_t<2, TimeStarValue>, std::string>,
                  "Index 2 must be std::string (String)");
    static_assert(std::is_same_v<std::variant_alternative_t<3, TimeStarValue>, int64_t>,
                  "Index 3 must be int64_t (Integer)");

    // Runtime check that the enum values match the indices
    EXPECT_EQ(static_cast<size_t>(TSMValueType::Float), 0u);
    EXPECT_EQ(static_cast<size_t>(TSMValueType::Boolean), 1u);
    EXPECT_EQ(static_cast<size_t>(TSMValueType::String), 2u);
    EXPECT_EQ(static_cast<size_t>(TSMValueType::Integer), 3u);
}

// ============================================================================
// 4b. Varint boundary values through IntegerEncoder and FFOR
// ============================================================================

TEST_F(IntegerTypeTest, IntegerEncoder_VarintBoundaryValues) {
    // Varint encoding boundaries: 7-bit, 14-bit, 21-bit, 28-bit thresholds
    std::vector<int64_t> values = {
        0, 1, -1,
        127, 128, -128, -129,              // 7-bit boundary
        16383, 16384, -16383, -16384,      // 14-bit boundary
        2097151, 2097152,                   // 21-bit boundary
        268435455, 268435456,              // 28-bit boundary
        INT64_MIN, INT64_MAX,
    };
    verifyIntegerEncoderRoundTrip(values);
}

TEST_F(IntegerTypeTest, FFOR_VarintBoundaryValues) {
    std::vector<int64_t> values = {
        0, 1, -1,
        127, 128, -128, -129,
        16383, 16384, -16383, -16384,
        2097151, 2097152,
        268435455, 268435456,
        INT64_MIN, INT64_MAX,
    };
    verifyFFORRoundTrip(values);
}

// ============================================================================
// 5. ZigZag + encoder consistency cross-checks
// ============================================================================

TEST_F(IntegerTypeTest, ZigZagPreservation_ThroughIntegerEncoder) {
    // Verify that individual ZigZag values survive the full encode/decode pipeline
    std::vector<int64_t> specials = {
        0, 1, -1, 127, -128, 255, -256, 32767, -32768,
        65535, -65536, INT32_MAX, INT32_MIN,
        static_cast<int64_t>(INT32_MAX) + 1,
        static_cast<int64_t>(INT32_MIN) - 1,
        INT64_MAX, INT64_MIN
    };
    verifyIntegerEncoderRoundTrip(specials);
}

TEST_F(IntegerTypeTest, ZigZagPreservation_ThroughFFOR) {
    std::vector<int64_t> specials = {
        0, 1, -1, 127, -128, 255, -256, 32767, -32768,
        65535, -65536, INT32_MAX, INT32_MIN,
        static_cast<int64_t>(INT32_MAX) + 1,
        static_cast<int64_t>(INT32_MIN) - 1,
        INT64_MAX, INT64_MIN
    };
    verifyFFORRoundTrip(specials);
}

TEST_F(IntegerTypeTest, IntegerEncoder_And_FFOR_ProduceSameResults) {
    // Both encoders should decode to identical values for the same input
    std::mt19937_64 rng(77);
    std::uniform_int_distribution<int64_t> dist(-1000000, 1000000);
    std::vector<int64_t> original(1024);
    for (auto& v : original) {
        v = dist(rng);
    }

    std::vector<uint64_t> zigzagged = ZigZag::zigzagEncodeVector(original);

    // Encode/decode with IntegerEncoder
    AlignedBuffer enc1 = IntegerEncoder::encode(zigzagged);
    Slice slice1(enc1.data.data(), enc1.size());
    std::vector<uint64_t> dec1;
    IntegerEncoder::decode(slice1, static_cast<unsigned int>(zigzagged.size()),
                           dec1, 0, UINT64_MAX);

    // Encode/decode with FFOR
    AlignedBuffer enc2 = IntegerEncoderFFOR::encode(zigzagged);
    Slice slice2(enc2.data.data(), enc2.size());
    std::vector<uint64_t> dec2;
    IntegerEncoderFFOR::decode(slice2, static_cast<unsigned int>(zigzagged.size()),
                               dec2, 0, UINT64_MAX);

    ASSERT_EQ(dec1.size(), dec2.size());
    for (size_t i = 0; i < dec1.size(); i++) {
        EXPECT_EQ(dec1[i], dec2[i])
            << "IntegerEncoder and FFOR disagree at index " << i;
    }

    // Both should recover the original int64_t values
    std::vector<int64_t> recovered1 = ZigZag::zigzagDecodeVector(dec1);
    std::vector<int64_t> recovered2 = ZigZag::zigzagDecodeVector(dec2);

    for (size_t i = 0; i < original.size(); i++) {
        EXPECT_EQ(recovered1[i], original[i]);
        EXPECT_EQ(recovered2[i], original[i]);
    }
}
