/**
 * Unit tests for LevelDBIndex string set encoding/decoding
 * Tests the bounds checking fix in decodeStringSet
 */

#include <gtest/gtest.h>
#include "../../../lib/index/leveldb_index.hpp"
#include <set>
#include <string>
#include <cstring>

class StringSetEncodingTest : public ::testing::Test {
protected:
    void SetUp() override {}
};

TEST_F(StringSetEncodingTest, EncodeDecodeBasic) {
    std::set<std::string> input = {"apple", "banana", "cherry"};
    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    EXPECT_EQ(input, decoded);
}

TEST_F(StringSetEncodingTest, EncodeDecodeEmpty) {
    std::set<std::string> input;
    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, EncodeDecodeSingleElement) {
    std::set<std::string> input = {"test"};
    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    EXPECT_EQ(input, decoded);
}

TEST_F(StringSetEncodingTest, DecodeEmptyString) {
    std::string empty;
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(empty);

    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, DecodeTruncatedLength) {
    // Create encoded data, then truncate it mid-length-prefix
    std::set<std::string> input = {"test"};
    std::string encoded = LevelDBIndex::encodeStringSet(input);

    // Truncate to just 1 byte (incomplete length prefix)
    std::string truncated = encoded.substr(0, 1);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(truncated);

    // Should return empty set, not crash
    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, DecodeTruncatedString) {
    // Create encoded data with a valid length but truncated string data
    std::set<std::string> input = {"hello_world"};
    std::string encoded = LevelDBIndex::encodeStringSet(input);

    // Truncate to just length + partial string (less than indicated length)
    std::string truncated = encoded.substr(0, sizeof(uint32_t) + 3);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(truncated);

    // Should return empty set (incomplete string)
    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, DecodeMalformedLengthExceedsBuffer) {
    // Create malformed data where length prefix exceeds remaining buffer
    std::string malformed;
    uint32_t len = 1000;  // Length says 1000 bytes
    malformed.append(reinterpret_cast<char*>(&len), sizeof(len));
    malformed += "short";  // But only 5 bytes of data

    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(malformed);

    // Should return empty set, not crash or read beyond buffer
    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, DecodeValidThenMalformed) {
    // Create data with one valid entry followed by malformed data
    std::set<std::string> input = {"valid"};
    std::string encoded = LevelDBIndex::encodeStringSet(input);

    // Append malformed length that exceeds remaining buffer
    uint32_t badLen = 5000;
    encoded.append(reinterpret_cast<char*>(&badLen), sizeof(badLen));
    encoded += "x";  // Much less than 5000 bytes

    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    // Should decode the valid entry and stop at the malformed one
    EXPECT_EQ(decoded.size(), 1);
    EXPECT_TRUE(decoded.count("valid") > 0);
}

TEST_F(StringSetEncodingTest, DecodeLargeValidDataset) {
    // Create a large dataset to ensure no issues with normal operation
    std::set<std::string> input;
    for (int i = 0; i < 100; ++i) {
        input.insert("element_" + std::to_string(i));
    }

    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    EXPECT_EQ(input, decoded);
}

TEST_F(StringSetEncodingTest, EncodeDecodeSpecialCharacters) {
    std::set<std::string> input = {
        "hello world",
        "path/to/file",
        "key=value",
        "newline\n\ttab"
    };

    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    EXPECT_EQ(input, decoded);
}

// Regression test: uint16_t length prefix overflow.
//
// With a uint16_t length prefix, any string longer than 65535 bytes overflows.
// The original code cast str.length() to uint16_t without a guard, causing silent
// data truncation (e.g., 70000 -> 4464 stored bytes). A later guard added a throw
// but still failed to encode large strings. After the fix (uint32_t prefix), strings
// up to ~4 GB can round-trip correctly.
//
// This test also covers the "summing to >65536 bytes" scenario: two large strings
// whose combined size exceeds UINT16_MAX, exercising the case where the *total*
// encoded payload is far larger than 65535 bytes while each individual string also
// exceeds the old per-entry limit.
TEST_F(StringSetEncodingTest, EncodeLargeStringExceedingUint16Max) {
    // Single tag value larger than UINT16_MAX (65535) bytes.
    // With the old uint16_t length prefix this either throws or silently truncates
    // the stored bytes.  After the uint32_t fix it must round-trip correctly.
    const size_t largeSize = 70000;  // > UINT16_MAX (65535)
    std::string largeValue(largeSize, 'x');

    std::set<std::string> input = {largeValue};
    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    ASSERT_EQ(decoded.size(), 1u);
    EXPECT_EQ(*decoded.begin(), largeValue);
}

// Two strings that each individually exceed UINT16_MAX; their total encoded size
// (>130,000 bytes) is far above 65535, exercising the accumulation path.
TEST_F(StringSetEncodingTest, EncodeTwoLargeStringsExceedingUint16Max) {
    const size_t size1 = 70000;
    const size_t size2 = 66000;
    std::string val1(size1, 'a');
    std::string val2(size2, 'b');

    std::set<std::string> input = {val1, val2};
    std::string encoded = LevelDBIndex::encodeStringSet(input);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(encoded);

    ASSERT_EQ(decoded.size(), 2u);
    EXPECT_TRUE(decoded.count(val1));
    EXPECT_TRUE(decoded.count(val2));
}
