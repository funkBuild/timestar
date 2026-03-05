#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <snappy.h>

#include "../../../lib/encoding/string_encoder.hpp"
#include "../../../lib/storage/compressed_buffer.hpp"

class StringEncoderTest : public ::testing::Test {
protected:
    std::vector<std::string> generateTestStrings(size_t count, size_t avgLength = 20) {
        std::vector<std::string> strings;
        strings.reserve(count);
        
        std::default_random_engine generator;
        std::uniform_int_distribution<int> lengthDist(5, avgLength * 2);
        std::uniform_int_distribution<char> charDist('a', 'z');
        
        for (size_t i = 0; i < count; i++) {
            size_t length = lengthDist(generator);
            std::string str;
            str.reserve(length);
            
            for (size_t j = 0; j < length; j++) {
                str += charDist(generator);
            }
            strings.push_back(str);
        }
        
        return strings;
    }
    
    std::vector<std::string> generateRepetitiveStrings(size_t count) {
        std::vector<std::string> strings;
        strings.reserve(count);
        
        // Create strings with repetitive patterns for better compression
        std::vector<std::string> patterns = {
            "sensor_data_",
            "temperature_reading_",
            "status_update_",
            "error_message_",
            "warning_level_"
        };
        
        for (size_t i = 0; i < count; i++) {
            std::string str = patterns[i % patterns.size()] + std::to_string(i / patterns.size());
            strings.push_back(str);
        }
        
        return strings;
    }
};

TEST_F(StringEncoderTest, BasicEncodeDecodeTest) {
    std::vector<std::string> testStrings = {
        "hello",
        "world",
        "this is a test",
        "TSM string encoding",
        "with Snappy compression"
    };
    
    // Encode
    auto encoded = StringEncoder::encode(testStrings);
    
    // Decode
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    // Verify
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, EmptyStringsTest) {
    std::vector<std::string> testStrings = {
        "",
        "not empty",
        "",
        "",
        "another string",
        ""
    };
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, LargeStringsTest) {
    std::vector<std::string> testStrings;
    
    // Create some large strings
    for (int i = 0; i < 10; i++) {
        std::string largeStr(1000 + i * 100, 'a' + i);
        testStrings.push_back(largeStr);
    }
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, ManyStringsTest) {
    const size_t numStrings = 10000;
    auto testStrings = generateTestStrings(numStrings);
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, CompressionRatioTest) {
    // Test with repetitive strings that should compress well
    const size_t numStrings = 1000;
    auto testStrings = generateRepetitiveStrings(numStrings);
    
    // Calculate uncompressed size
    size_t uncompressedSize = 0;
    for (const auto& str : testStrings) {
        uncompressedSize += sizeof(uint32_t) + str.size(); // length prefix + data
    }
    
    auto encoded = StringEncoder::encode(testStrings);
    size_t compressedSize = encoded.size();
    
    // Should achieve some compression with repetitive data
    double compressionRatio = static_cast<double>(uncompressedSize) / compressedSize;
    EXPECT_GT(compressionRatio, 1.5); // Expect at least 1.5x compression
    
    // Verify data integrity
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, SpecialCharactersTest) {
    std::vector<std::string> testStrings = {
        "Hello\nWorld",
        "Tab\tSeparated",
        "Null\0Character",
        "UTF-8: ñ, é, ü, 中文",
        "Symbols: !@#$%^&*()",
        "Quotes: \"single' and double\"",
        "Path: /usr/local/bin/test.exe",
        "JSON: {\"key\": \"value\", \"number\": 123}"
    };
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);
    
    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

TEST_F(StringEncoderTest, PartialDecodeTest) {
    std::vector<std::string> testStrings = {
        "first",
        "second",
        "third",
        "fourth",
        "fifth"
    };
    
    auto encoded = StringEncoder::encode(testStrings);
    
    // Decode only first 3 strings
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, 3, decoded);
    
    EXPECT_EQ(decoded.size(), 3);
    EXPECT_EQ(decoded[0], "first");
    EXPECT_EQ(decoded[1], "second");
    EXPECT_EQ(decoded[2], "third");
}

TEST_F(StringEncoderTest, SingleStringTest) {
    std::vector<std::string> testStrings = {"single string test"};
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, 1, decoded);
    
    EXPECT_EQ(decoded.size(), 1);
    EXPECT_EQ(decoded[0], "single string test");
}

TEST_F(StringEncoderTest, EmptyVectorTest) {
    std::vector<std::string> testStrings;
    
    auto encoded = StringEncoder::encode(testStrings);
    
    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, 0, decoded);
    
    EXPECT_EQ(decoded.size(), 0);
    EXPECT_GT(encoded.size(), 0); // Should have header even if empty
}

TEST_F(StringEncoderTest, MaxLengthPrefixTest) {
    // Test that length prefixes work correctly for various sizes
    std::vector<std::string> testStrings;

    // Add strings of increasing sizes
    testStrings.push_back(std::string(255, 'a'));      // Max for 1 byte
    testStrings.push_back(std::string(256, 'b'));      // Requires 2 bytes
    testStrings.push_back(std::string(65535, 'c'));    // Max for 2 bytes
    testStrings.push_back(std::string(65536, 'd'));    // Requires 4 bytes

    auto encoded = StringEncoder::encode(testStrings);

    std::vector<std::string> decoded;
    StringEncoder::decode(encoded, testStrings.size(), decoded);

    EXPECT_EQ(decoded.size(), testStrings.size());
    for (size_t i = 0; i < testStrings.size(); i++) {
        EXPECT_EQ(decoded[i].size(), testStrings[i].size());
        EXPECT_EQ(decoded[i], testStrings[i]);
    }
}

// Verify that a malformed encoded buffer whose varint length field claims
// the string is 1000 bytes long but only 5 bytes remain in the
// decompressed payload throws std::runtime_error rather than reading past
// the end of the buffer.  This exercises the bounds check
//   if (slice.offset + strLen > slice.length_)
// which can silently wrap to zero when strLen is near UINT32_MAX; the fix
// uses the overflow-safe form
//   if (strLen > slice.length_ - slice.offset)
TEST_F(StringEncoderTest, StringEncoder_Decode_InvalidLengthThrows) {
    // Build a minimal malformed uncompressed payload:
    //   - varint(1000): two bytes 0xe8 0x07  (1000 = 0b1111101000;
    //                                         low 7 bits = 0x68 | 0x80 = 0xe8,
    //                                         next 7 bits = 0x07)
    //   - 5 bytes of payload (far fewer than the claimed 1000)
    // Total uncompressed size = 7 bytes.
    std::vector<uint8_t> uncompressed = {
        0xe8, 0x07,             // varint: length = 1000
        'a',  'b',  'c',  'd',  'e'   // only 5 bytes of actual data
    };
    const uint32_t uncompSize = static_cast<uint32_t>(uncompressed.size()); // 7

    // Snappy-compress the malformed payload.
    size_t maxCompLen = snappy::MaxCompressedLength(uncompressed.size());
    std::vector<char> compressed(maxCompLen);
    size_t compSize = 0;
    snappy::RawCompress(reinterpret_cast<const char*>(uncompressed.data()),
                        uncompressed.size(),
                        compressed.data(),
                        &compSize);

    // Assemble the full encoded buffer with the standard 16-byte header.
    AlignedBuffer buf;
    uint32_t magic    = 0x53545247u;  // "STRG"
    uint32_t count    = 1u;
    uint32_t cSize    = static_cast<uint32_t>(compSize);
    buf.write(magic);
    buf.write(uncompSize);
    buf.write(cSize);
    buf.write(count);
    buf.write_bytes(compressed.data(), compSize);

    // decode() must detect that strLen (1000) exceeds the bytes remaining
    // after the varint (5) and throw rather than read out-of-bounds.
    std::vector<std::string> out;
    EXPECT_THROW(StringEncoder::decode(buf, 1, out), std::runtime_error);
}

// Verify that a varint with the continuation bit set on all 5 bytes (meaning
// a 6th byte would be required to complete the value, exceeding uint32_t
// capacity) causes readVarInt to throw std::runtime_error("VarInt too large")
// rather than silently truncating to 32 bits and returning a wrong length.
//
// A 5-byte varint with all continuation bits set: {0x80, 0x80, 0x80, 0x80, 0x80}
// readVarInt reads bytes 1-4 normally (shift reaches 28 which is not > 28),
// then reads byte 5 (shift becomes 35, which is > 28) and finds byte & 0x80
// still set, so it must throw rather than break.
TEST_F(StringEncoderTest, StringEncoder_Decode_VarIntTooLargeThrows) {
    // Uncompressed payload: a 5-byte varint where every byte has the
    // continuation bit (0x80) set.  This encodes a value that would require
    // a 6th byte and is therefore unrepresentable as uint32_t.
    std::vector<uint8_t> uncompressed = {
        0x80, 0x80, 0x80, 0x80, 0x80  // 5 continuation bytes — varint too large
    };
    const uint32_t uncompSize = static_cast<uint32_t>(uncompressed.size());

    size_t maxCompLen = snappy::MaxCompressedLength(uncompressed.size());
    std::vector<char> compressed(maxCompLen);
    size_t compSize = 0;
    snappy::RawCompress(reinterpret_cast<const char*>(uncompressed.data()),
                        uncompressed.size(),
                        compressed.data(),
                        &compSize);

    // Build the 16-byte header + compressed payload.
    AlignedBuffer buf;
    uint32_t magic = 0x53545247u;  // "STRG"
    uint32_t count = 1u;
    uint32_t cSize = static_cast<uint32_t>(compSize);
    buf.write(magic);
    buf.write(uncompSize);
    buf.write(cSize);
    buf.write(count);
    buf.write_bytes(compressed.data(), compSize);

    // readVarInt must throw when it encounters the continuation bit on the
    // 5th byte, not silently truncate and return a garbage string length.
    std::vector<std::string> out;
    EXPECT_THROW(StringEncoder::decode(buf, 1, out), std::runtime_error);
}