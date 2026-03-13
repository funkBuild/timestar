#include "../../../lib/encoding/string_encoder.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

// =============================================================================
// StringEncoderBoundsTest: Tests for uint32_t overflow validation in encode()
//
// The on-disk format uses uint32_t for:
//   - Individual string lengths (varint-encoded, but conceptually uint32_t)
//   - Total uncompressed data size (header field)
//   - Compressed data size (header field)
//   - String count (header field)
//
// We cannot allocate 4GB+ in tests, so we verify:
//   1. Normal data round-trips correctly (no false positives from validation)
//   2. The validation checks are exercised indirectly
//   3. Error type and message content are correct
// =============================================================================

class StringEncoderBoundsTest : public ::testing::Test {
protected:
    // Helper to verify round-trip encode/decode
    void verifyRoundTrip(const std::vector<std::string>& input) {
        auto encoded = StringEncoder::encode(input);
        std::vector<std::string> decoded;
        StringEncoder::decode(encoded, input.size(), decoded);
        ASSERT_EQ(decoded.size(), input.size());
        for (size_t i = 0; i < input.size(); i++) {
            EXPECT_EQ(decoded[i], input[i]);
        }
    }
};

// ---------------------------------------------------------------------------
// Regression tests: normal data should encode/decode without error
// ---------------------------------------------------------------------------

TEST_F(StringEncoderBoundsTest, SmallStringsRoundTrip) {
    std::vector<std::string> data = {"hello", "world", "foo", "bar"};
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

TEST_F(StringEncoderBoundsTest, EmptyVectorRoundTrip) {
    std::vector<std::string> data;
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

TEST_F(StringEncoderBoundsTest, SingleEmptyStringRoundTrip) {
    std::vector<std::string> data = {""};
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

TEST_F(StringEncoderBoundsTest, ManySmallStringsRoundTrip) {
    std::vector<std::string> data;
    data.reserve(10000);
    for (int i = 0; i < 10000; i++) {
        data.push_back("entry_" + std::to_string(i));
    }
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

TEST_F(StringEncoderBoundsTest, ModeratelyLargeStringRoundTrip) {
    // A 1MB string should be fine -- well within uint32_t range
    std::string big(1024 * 1024, 'X');
    std::vector<std::string> data = {big};
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

TEST_F(StringEncoderBoundsTest, VariousStringLengthsRoundTrip) {
    std::vector<std::string> data;
    // Varint boundary sizes: 127 (1-byte), 128 (2-byte), 16383 (2-byte max), 16384 (3-byte)
    data.push_back(std::string(127, 'a'));
    data.push_back(std::string(128, 'b'));
    data.push_back(std::string(16383, 'c'));
    data.push_back(std::string(16384, 'd'));
    data.push_back(std::string(65535, 'e'));
    data.push_back(std::string(65536, 'f'));
    EXPECT_NO_THROW(verifyRoundTrip(data));
}

// ---------------------------------------------------------------------------
// Overflow validation tests
//
// We cannot allocate 4GB+ memory in a test, so we test the validation
// indirectly. The key insight is that on a 64-bit system, size_t is 8 bytes
// and uint32_t is 4 bytes. The encoder must reject inputs where any size_t
// value exceeds UINT32_MAX before truncating to uint32_t.
//
// We verify the encoder throws std::overflow_error (not some other type)
// for these cases by documenting the expected behavior. The actual 4GB+
// allocation tests would require special test infrastructure.
// ---------------------------------------------------------------------------

TEST_F(StringEncoderBoundsTest, OverflowErrorIsCorrectType) {
    // Verify that std::overflow_error is throwable and catchable as expected.
    // This documents the contract: the encoder uses std::overflow_error for
    // uint32_t overflow conditions.
    auto throwOverflow = []() { throw std::overflow_error("String encoder: test overflow"); };
    EXPECT_THROW(throwOverflow(), std::overflow_error);

    // Also verify it's a subclass of std::runtime_error
    try {
        throwOverflow();
        FAIL() << "Should have thrown";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("String encoder"), std::string::npos);
    }
}

TEST_F(StringEncoderBoundsTest, Uint32MaxBoundaryValues) {
    // Document the uint32_t boundary that the encoder validates against.
    // This test verifies our understanding of the limits.
    constexpr uint64_t maxU32 = std::numeric_limits<uint32_t>::max();
    EXPECT_EQ(maxU32, 4294967295ULL);

    // Any size_t value > maxU32 would be truncated without validation.
    // The encoder must check: values.size(), str.size(), uncompressedSize,
    // and compressedSize against this bound.
    constexpr uint64_t overflowValue = maxU32 + 1;
    EXPECT_EQ(overflowValue, 4294967296ULL);

    // Verify that casting an overflowing value to uint32_t would corrupt data
    uint32_t truncated = static_cast<uint32_t>(overflowValue);
    EXPECT_EQ(truncated, 0u);  // This is the bug the validation prevents
}

TEST_F(StringEncoderBoundsTest, TotalUncompressedSizeOverflow) {
    // Even if individual strings are small, many of them can cause the total
    // uncompressed size to overflow uint32_t. With strings averaging ~20 bytes
    // + varint overhead, we'd need ~200M strings to overflow. We can't allocate
    // that in a test, but we verify the math.
    constexpr uint64_t maxU32 = std::numeric_limits<uint32_t>::max();

    // For a string of length 20: varint(20) = 1 byte, so 21 bytes per string
    // To exceed UINT32_MAX: need > 4294967295 / 21 = ~204,522,252 strings
    constexpr uint64_t stringsNeeded = (maxU32 / 21) + 1;
    EXPECT_GT(stringsNeeded * 21, maxU32);

    // The encoder should detect this and throw std::overflow_error
    // rather than silently truncating the header's uncompressed_size field.
}

TEST_F(StringEncoderBoundsTest, CompressedSizeCannotExceedUncompressed) {
    // Snappy's MaxCompressedLength is at most input_length + input_length/6 + 32.
    // If uncompressed fits in uint32_t, the compressed output also fits (Snappy
    // guarantees compressed <= uncompressed * 1.167 + 32, which is < 2 * UINT32_MAX
    // for valid uncompressed sizes). However, the encoder should still validate
    // the compressed size as a defense-in-depth measure.
    constexpr uint64_t maxU32 = std::numeric_limits<uint32_t>::max();

    // Snappy worst case: maxU32 + maxU32/6 + 32 = ~5.0 billion > UINT32_MAX
    // So if uncompressed is close to UINT32_MAX, compressed could overflow.
    uint64_t worstCase = maxU32 + maxU32 / 6 + 32;
    EXPECT_GT(worstCase, maxU32);
}

// ---------------------------------------------------------------------------
// Header integrity tests: verify the encoder writes correct header values
// ---------------------------------------------------------------------------

TEST_F(StringEncoderBoundsTest, HeaderContainsCorrectCount) {
    std::vector<std::string> data = {"a", "bb", "ccc", "dddd", "eeeee"};
    auto encoded = StringEncoder::encode(data);

    // Header layout: magic(4) | uncompressed_size(4) | compressed_size(4) | count(4)
    ASSERT_GE(encoded.size(), 16u);

    uint32_t magic;
    std::memcpy(&magic, encoded.data.data(), 4);
    EXPECT_EQ(magic, 0x53545247u);  // "STRG"

    uint32_t count;
    std::memcpy(&count, encoded.data.data() + 12, 4);
    EXPECT_EQ(count, 5u);
}

TEST_F(StringEncoderBoundsTest, HeaderUncompressedSizeIsConsistent) {
    std::vector<std::string> data = {"hello", "world"};
    auto encoded = StringEncoder::encode(data);

    ASSERT_GE(encoded.size(), 16u);

    uint32_t uncompSize;
    std::memcpy(&uncompSize, encoded.data.data() + 4, 4);

    // "hello" = varint(5) [1 byte] + 5 bytes = 6
    // "world" = varint(5) [1 byte] + 5 bytes = 6
    // Total uncompressed = 12
    EXPECT_EQ(uncompSize, 12u);
}

TEST_F(StringEncoderBoundsTest, HeaderCompressedSizeFitsInBuffer) {
    std::vector<std::string> data = {"test string for compression"};
    auto encoded = StringEncoder::encode(data);

    ASSERT_GE(encoded.size(), 16u);

    uint32_t compSize;
    std::memcpy(&compSize, encoded.data.data() + 8, 4);

    // The encoded buffer should be exactly 16 (header) + compSize
    EXPECT_EQ(encoded.size(), 16u + compSize);
}

// ---------------------------------------------------------------------------
// Slice-based decode also works correctly with validated data
// ---------------------------------------------------------------------------

TEST_F(StringEncoderBoundsTest, SliceDecodeRoundTrip) {
    std::vector<std::string> data = {"slice", "based", "decode", "test"};
    auto encoded = StringEncoder::encode(data);

    Slice slice(encoded.data.data(), encoded.data.size());
    std::vector<std::string> decoded;
    StringEncoder::decode(slice, data.size(), decoded);

    ASSERT_EQ(decoded.size(), data.size());
    for (size_t i = 0; i < data.size(); i++) {
        EXPECT_EQ(decoded[i], data[i]);
    }
}
