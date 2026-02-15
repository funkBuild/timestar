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
    std::string truncated = encoded.substr(0, sizeof(uint16_t) + 3);
    std::set<std::string> decoded = LevelDBIndex::decodeStringSet(truncated);

    // Should return empty set (incomplete string)
    EXPECT_TRUE(decoded.empty());
}

TEST_F(StringSetEncodingTest, DecodeMalformedLengthExceedsBuffer) {
    // Create malformed data where length prefix exceeds remaining buffer
    std::string malformed;
    uint16_t len = 1000;  // Length says 1000 bytes
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
    uint16_t badLen = 5000;
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
