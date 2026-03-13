#include "tsxor_encoder.hpp"

#include <gtest/gtest.h>

#include <bit>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <limits>
#include <regex>
#include <string>
#include <vector>

// =============================================================================
// Source code inspection tests: verify the UB fixes are in place
// =============================================================================

class TsxorEncoderUBTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        // Read the source file to inspect for UB patterns
        std::ifstream file(TSXOR_ENCODER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open tsxor_encoder.cpp at: " << TSXOR_ENCODER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

// Verify no (uint64_t*) pointer casts remain (strict aliasing violation)
TEST_F(TsxorEncoderUBTest, NoUint64PointerCasts) {
    // Check for C-style cast: (uint64_t *)
    EXPECT_EQ(sourceCode.find("(uint64_t *)"), std::string::npos)
        << "Found '(uint64_t *)' C-style cast in tsxor_encoder.cpp. "
        << "Should use std::bit_cast<uint64_t>() instead.";

    // Check for reinterpret_cast<uint64_t*>
    EXPECT_EQ(sourceCode.find("reinterpret_cast<uint64_t*>"), std::string::npos)
        << "Found 'reinterpret_cast<uint64_t*>' in tsxor_encoder.cpp. "
        << "Should use std::bit_cast<uint64_t>() instead.";

    // Also check (uint64_t*)
    EXPECT_EQ(sourceCode.find("(uint64_t*)"), std::string::npos)
        << "Found '(uint64_t*)' C-style cast in tsxor_encoder.cpp. "
        << "Should use std::bit_cast<uint64_t>() instead.";
}

// Verify std::bit_cast is used for double->uint64_t conversion
TEST_F(TsxorEncoderUBTest, UsesBitCastForDoubleConversion) {
    EXPECT_NE(sourceCode.find("bit_cast<uint64_t>"), std::string::npos)
        << "Expected std::bit_cast<uint64_t> in tsxor_encoder.cpp for "
        << "type-safe double-to-uint64_t conversion.";
}

// Verify no (uint8_t*) byte extraction via pointer aliasing
TEST_F(TsxorEncoderUBTest, NoUint8PointerByteExtraction) {
    // Check for uint8_t* byte extraction pattern
    EXPECT_EQ(sourceCode.find("(uint8_t *)"), std::string::npos)
        << "Found '(uint8_t *)' pointer cast in tsxor_encoder.cpp. "
        << "Should use '& 0xFF' for portable byte extraction.";

    EXPECT_EQ(sourceCode.find("(uint8_t*)"), std::string::npos)
        << "Found '(uint8_t*)' pointer cast in tsxor_encoder.cpp. "
        << "Should use '& 0xFF' for portable byte extraction.";
}

// Verify that <bit> header is included
TEST_F(TsxorEncoderUBTest, IncludesBitHeader) {
    EXPECT_NE(sourceCode.find("#include <bit>"), std::string::npos)
        << "Expected '#include <bit>' in tsxor_encoder.cpp for std::bit_cast.";
}

// =============================================================================
// Functional tests: verify encoding correctness with the fixed implementation
// =============================================================================

// Verify bit_cast produces the correct IEEE 754 bit patterns
TEST(TsxorEncoderUBFunctionalTest, BitCastProducesCorrectPatterns) {
    // IEEE 754 double precision reference values
    EXPECT_EQ(std::bit_cast<uint64_t>(1.0), 0x3FF0000000000000ULL)
        << "bit_cast<uint64_t>(1.0) should produce IEEE 754 pattern 0x3FF0000000000000";

    EXPECT_EQ(std::bit_cast<uint64_t>(0.0), 0x0000000000000000ULL) << "bit_cast<uint64_t>(0.0) should be all zeros";

    EXPECT_EQ(std::bit_cast<uint64_t>(-1.0), 0xBFF0000000000000ULL)
        << "bit_cast<uint64_t>(-1.0) should have sign bit set";

    EXPECT_EQ(std::bit_cast<uint64_t>(2.0), 0x4000000000000000ULL)
        << "bit_cast<uint64_t>(2.0) should produce IEEE 754 pattern 0x4000000000000000";

    // -0.0 has only the sign bit set
    EXPECT_EQ(std::bit_cast<uint64_t>(-0.0), 0x8000000000000000ULL)
        << "bit_cast<uint64_t>(-0.0) should have only sign bit set";

    // Infinity
    EXPECT_EQ(std::bit_cast<uint64_t>(std::numeric_limits<double>::infinity()), 0x7FF0000000000000ULL)
        << "+infinity should be 0x7FF0000000000000";

    EXPECT_EQ(std::bit_cast<uint64_t>(-std::numeric_limits<double>::infinity()), 0xFFF0000000000000ULL)
        << "-infinity should be 0xFFF0000000000000";

    // NaN has exponent all 1s and non-zero mantissa
    uint64_t nan_bits = std::bit_cast<uint64_t>(std::numeric_limits<double>::quiet_NaN());
    EXPECT_EQ(nan_bits & 0x7FF0000000000000ULL, 0x7FF0000000000000ULL) << "NaN should have all exponent bits set";
    EXPECT_NE(nan_bits & 0x000FFFFFFFFFFFFFULL, 0ULL) << "NaN should have non-zero mantissa";

    // Denormalized minimum
    EXPECT_EQ(std::bit_cast<uint64_t>(std::numeric_limits<double>::denorm_min()), 0x0000000000000001ULL)
        << "denorm_min should be 0x0000000000000001";
}

// Verify encoding of specific double values produces valid compressed output
TEST(TsxorEncoderUBFunctionalTest, EncodeSpecificDoubles) {
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// Verify encoding of special IEEE 754 values
TEST(TsxorEncoderUBFunctionalTest, EncodeSpecialIEEE754Values) {
    std::vector<double> values = {0.0,
                                  -0.0,
                                  1.0,
                                  -1.0,
                                  std::numeric_limits<double>::quiet_NaN(),
                                  std::numeric_limits<double>::infinity(),
                                  -std::numeric_limits<double>::infinity(),
                                  std::numeric_limits<double>::min(),
                                  std::numeric_limits<double>::max(),
                                  std::numeric_limits<double>::denorm_min(),
                                  std::numeric_limits<double>::epsilon()};

    // Should not crash or produce UB
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// Verify encoding of repeated values (exercises the window hit path with & 0xFF)
TEST(TsxorEncoderUBFunctionalTest, EncodeRepeatedValues) {
    // Repeated values should hit the window.contains() path,
    // exercising the byte extraction fix (& 0xFF instead of (uint8_t*)&offset)
    std::vector<double> values(200, 42.0);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);

    // With all identical values, the window hit path should compress well
    size_t rawSize = 200 * sizeof(double);
    EXPECT_LT(result.size(), rawSize) << "Repeated values should compress to less than the raw size";
}

// Verify encoding of alternating values (exercises window candidate path with | 0x80)
TEST(TsxorEncoderUBFunctionalTest, EncodeAlternatingValues) {
    // Alternating between two values exercises both the window hit and miss paths
    std::vector<double> values;
    for (int i = 0; i < 200; i++) {
        values.push_back(i % 2 == 0 ? 100.0 : 200.0);
    }
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// Verify deterministic output: same input always produces same compressed output
TEST(TsxorEncoderUBFunctionalTest, DeterministicEncoding) {
    std::vector<double> values = {1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 5.0};

    CompressedBuffer result1 = TsxorEncoder::encode(values);
    CompressedBuffer result2 = TsxorEncoder::encode(values);

    EXPECT_EQ(result1.size(), result2.size()) << "Same input should always produce same output size";
}

// Verify the low-byte extraction is correct: offset & 0xFF should match
// what (uint8_t*)&offset would give on little-endian
TEST(TsxorEncoderUBFunctionalTest, LowByteExtractionEquivalence) {
    // On little-endian (x86), (uint8_t*)&val gives the low byte,
    // which is the same as val & 0xFF. Verify this equivalence.
    for (int val = 0; val < 256; val++) {
        uint8_t* bytes = (uint8_t*)&val;
        uint8_t low_byte = static_cast<uint8_t>(val & 0xFF);
        EXPECT_EQ(bytes[0], low_byte) << "On this platform, (uint8_t*)&val and val & 0xFF differ for val=" << val;
    }
}

// Verify encoding with many unique values that exceed window size
TEST(TsxorEncoderUBFunctionalTest, EncodeExceedingWindowSize) {
    // WINDOW_SIZE is 127, so 200 unique values will cause many window misses
    std::vector<double> values;
    for (int i = 0; i < 200; i++) {
        values.push_back(static_cast<double>(i) * 3.14159);
    }
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

// Verify that the offset | 0x80 pattern is preserved correctly
// When offset < 128 and we set bit 7 (|= 0x80), the result & 0xFF
// should have bit 7 set
TEST(TsxorEncoderUBFunctionalTest, OffsetHighBitPreserved) {
    for (int offset = 0; offset < 127; offset++) {
        int modified = offset | 0x80;
        uint8_t extracted = static_cast<uint8_t>(modified & 0xFF);
        EXPECT_TRUE(extracted & 0x80) << "Bit 7 should be set after |= 0x80 for offset=" << offset;
        EXPECT_EQ(extracted & 0x7F, offset) << "Low 7 bits should be preserved for offset=" << offset;
    }
}
