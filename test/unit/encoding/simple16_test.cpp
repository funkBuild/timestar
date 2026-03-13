#include "../../../lib/encoding/simple16.hpp"

#include "../../../lib/encoding/integer_encoder.hpp"

#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <vector>

TEST(Simple16Test, EncodeLargeTimestamps) {
    // Test with the problematic timestamp that broke Simple8B
    uint64_t problematic_timestamp = 1756563343901168896ULL;

    std::cout << "Testing timestamp: " << problematic_timestamp << std::endl;
    std::cout << "Timestamp in hex: 0x" << std::hex << problematic_timestamp << std::dec << std::endl;
    std::cout << "Timestamp requires " << (64 - __builtin_clzll(problematic_timestamp)) << " bits" << std::endl;

    // Test 1: Direct Simple16 encoding of the timestamp
    {
        std::vector<uint64_t> values = {problematic_timestamp};
        std::cout << "\nTest 1: Direct Simple16 encoding of single large timestamp" << std::endl;

        AlignedBuffer encoded = Simple16::encode(values);
        std::cout << "Encoded size: " << encoded.size() << " bytes" << std::endl;
        EXPECT_GT(encoded.size(), 0);

        // Decode and verify
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded = Simple16::decode(slice, 1);

        ASSERT_EQ(decoded.size(), 1);
        EXPECT_EQ(decoded[0], problematic_timestamp);
        std::cout << "Successfully encoded and decoded large timestamp!" << std::endl;
    }

    // Test 2: Multiple large timestamps
    {
        std::vector<uint64_t> values = {
            problematic_timestamp,
            problematic_timestamp + 1000000000,  // 1 second later
            problematic_timestamp + 2000000000   // 2 seconds later
        };
        std::cout << "\nTest 2: Simple16 encoding of multiple large timestamps" << std::endl;

        AlignedBuffer encoded = Simple16::encode(values);
        std::cout << "Encoded size: " << encoded.size() << " bytes for " << values.size() << " values" << std::endl;

        // Decode and verify
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded = Simple16::decode(slice, values.size());

        ASSERT_EQ(decoded.size(), values.size());
        for (size_t i = 0; i < values.size(); i++) {
            EXPECT_EQ(decoded[i], values[i]);
        }
        std::cout << "Successfully encoded and decoded multiple large timestamps!" << std::endl;
    }

    // Test 3: Using IntegerEncoder with large timestamps
    {
        std::vector<uint64_t> timestamps = {
            problematic_timestamp,
            problematic_timestamp + 1000000000,  // 1 second later
            problematic_timestamp + 2000000000   // 2 seconds later
        };
        std::cout << "\nTest 3: IntegerEncoder with large timestamps" << std::endl;
        std::cout << "First timestamp: " << timestamps[0] << std::endl;
        std::cout << "Delta to second: " << (timestamps[1] - timestamps[0]) << std::endl;

        AlignedBuffer encoded = IntegerEncoder::encode(timestamps);
        std::cout << "Encoded size: " << encoded.size() << " bytes" << std::endl;

        // Decode and verify
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded;
        auto [skipped, added] = IntegerEncoder::decode(slice, timestamps.size(), decoded, 0, UINT64_MAX);

        EXPECT_EQ(skipped, 0);
        EXPECT_EQ(added, timestamps.size());
        ASSERT_EQ(decoded.size(), timestamps.size());

        for (size_t i = 0; i < timestamps.size(); i++) {
            EXPECT_EQ(decoded[i], timestamps[i]) << "Mismatch at index " << i;
        }

        std::cout << "IntegerEncoder successfully handled large timestamps!" << std::endl;
    }
}

TEST(Simple16Test, MixedSizeValues) {
    // Test with values of different bit widths
    std::vector<uint64_t> values;

    // Small values (2-bit)
    for (int i = 0; i < 10; i++) {
        values.push_back(i % 4);
    }

    // Medium values (8-bit)
    for (int i = 0; i < 10; i++) {
        values.push_back(200 + i);
    }

    // Large values (32-bit)
    for (int i = 0; i < 5; i++) {
        values.push_back(1000000000ULL + i);
    }

    // Very large values (>60-bit)
    values.push_back(1ULL << 62);        // 62-bit value
    values.push_back((1ULL << 63) - 1);  // 63-bit value

    std::cout << "Testing mixed size values (" << values.size() << " total)" << std::endl;

    AlignedBuffer encoded = Simple16::encode(values);
    std::cout << "Encoded size: " << encoded.size() << " bytes" << std::endl;

    // Decode and verify
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded = Simple16::decode(slice, values.size());

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }

    std::cout << "Successfully encoded and decoded mixed size values!" << std::endl;
}

TEST(Simple16Test, CompressionEfficiency) {
    // Test compression with different data patterns

    // Pattern 1: Small deltas (should compress well)
    {
        std::vector<uint64_t> values;
        uint64_t base = 1000000000000ULL;
        for (int i = 0; i < 1000; i++) {
            values.push_back(base + i);
        }

        AlignedBuffer encoded = Simple16::encode(values);
        size_t encodedSize = encoded.size();
        size_t rawSize = values.size() * sizeof(uint64_t);
        double ratio = (double)rawSize / encodedSize;

        std::cout << "Small deltas: Raw size = " << rawSize << ", Encoded size = " << encodedSize
                  << ", Compression ratio = " << ratio << ":1" << std::endl;

        // Verify decode
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded = Simple16::decode(slice, values.size());
        ASSERT_EQ(decoded.size(), values.size());
    }

    // Pattern 2: Random values (less compressible)
    {
        std::vector<uint64_t> values;
        std::mt19937_64 gen(42);
        std::uniform_int_distribution<uint64_t> dist(0, (1ULL << 30) - 1);

        for (int i = 0; i < 1000; i++) {
            values.push_back(dist(gen));
        }

        AlignedBuffer encoded = Simple16::encode(values);
        size_t encodedSize = encoded.size();
        size_t rawSize = values.size() * sizeof(uint64_t);
        double ratio = (double)rawSize / encodedSize;

        std::cout << "Random 30-bit values: Raw size = " << rawSize << ", Encoded size = " << encodedSize
                  << ", Compression ratio = " << ratio << ":1" << std::endl;

        // Verify decode
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded = Simple16::decode(slice, values.size());
        ASSERT_EQ(decoded.size(), values.size());
    }
}

TEST(Simple16Test, MaximumValues) {
    // Test maximum values for different bit widths
    std::vector<uint64_t> values = {
        (1ULL << 60) - 1,  // Max 60-bit (works with Simple8B)
        1ULL << 60,        // 60-bit + 1 (fails with Simple8B)
        1ULL << 61,        // 61-bit
        1ULL << 62,        // 62-bit
        1ULL << 63,        // 63-bit
        UINT64_MAX         // Max 64-bit
    };

    std::cout << "Testing maximum values:" << std::endl;
    for (auto v : values) {
        std::cout << "  " << v << " (requires " << (64 - __builtin_clzll(v)) << " bits)" << std::endl;
    }

    AlignedBuffer encoded = Simple16::encode(values);
    std::cout << "Encoded size: " << encoded.size() << " bytes" << std::endl;

    // Decode and verify
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded = Simple16::decode(slice, values.size());

    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); i++) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i << ": expected " << values[i] << ", got "
                                         << decoded[i];
    }

    std::cout << "Successfully handled all maximum values including 64-bit!" << std::endl;
}