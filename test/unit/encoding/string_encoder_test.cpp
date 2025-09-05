#include <gtest/gtest.h>
#include <vector>
#include <string>
#include <random>
#include <chrono>

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