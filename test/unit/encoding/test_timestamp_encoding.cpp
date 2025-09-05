#include <gtest/gtest.h>
#include "../../../lib/encoding/simple16.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include <iostream>
#include <vector>
#include <limits>

TEST(TimestampEncodingTest, MaxUint64Value) {
    // Test encoding UINT64_MAX
    uint64_t max_val = UINT64_MAX;
    std::cout << "Testing UINT64_MAX: " << max_val << std::endl;
    std::cout << "In hex: 0x" << std::hex << max_val << std::dec << std::endl;
    
    // Test 1: Direct Simple16 encoding
    {
        std::vector<uint64_t> values = {max_val};
        AlignedBuffer encoded = Simple16::encode(values);
        
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded = Simple16::decode(slice, 1);
        
        ASSERT_EQ(decoded.size(), 1);
        EXPECT_EQ(decoded[0], max_val);
    }
    
    // Test 2: IntegerEncoder with UINT64_MAX
    {
        std::vector<uint64_t> timestamps = {max_val - 2, max_val - 1, max_val};
        std::cout << "Testing sequence ending with UINT64_MAX" << std::endl;
        
        AlignedBuffer encoded = IntegerEncoder::encode(timestamps);
        
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> decoded;
        auto [skipped, added] = IntegerEncoder::decode(slice, 3, decoded, 0, UINT64_MAX);
        
        EXPECT_EQ(added, 3);
        ASSERT_EQ(decoded.size(), 3);
        EXPECT_EQ(decoded[2], max_val);
    }
}

TEST(TimestampEncodingTest, LargeSignedValues) {
    // Test with INT64_MAX (signed interpretation issues)
    int64_t max_signed = INT64_MAX;
    uint64_t as_unsigned = static_cast<uint64_t>(max_signed);
    
    std::cout << "Testing INT64_MAX as uint64: " << as_unsigned << std::endl;
    
    std::vector<uint64_t> values = {as_unsigned - 1, as_unsigned, as_unsigned + 1};
    
    AlignedBuffer encoded = IntegerEncoder::encode(values);
    
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] = IntegerEncoder::decode(slice, 3, decoded, 0, UINT64_MAX);
    
    EXPECT_EQ(added, 3);
    ASSERT_EQ(decoded.size(), 3);
    EXPECT_EQ(decoded[0], as_unsigned - 1);
    EXPECT_EQ(decoded[1], as_unsigned);
    EXPECT_EQ(decoded[2], as_unsigned + 1);
}

TEST(TimestampEncodingTest, ZigZagOverflow) {
    // Test potential overflow in zigzag encoding with large deltas
    std::vector<uint64_t> timestamps = {
        0,
        UINT64_MAX / 2,
        UINT64_MAX
    };
    
    std::cout << "Testing large deltas:" << std::endl;
    for (size_t i = 1; i < timestamps.size(); i++) {
        int64_t delta = static_cast<int64_t>(timestamps[i] - timestamps[i-1]);
        std::cout << "  Delta " << i << ": " << delta << std::endl;
    }
    
    // This might cause issues with zigzag encoding if not handled properly
    AlignedBuffer encoded = IntegerEncoder::encode(timestamps);
    
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] = IntegerEncoder::decode(slice, 3, decoded, 0, UINT64_MAX);
    
    EXPECT_EQ(added, 3);
    if (decoded.size() == 3) {
        EXPECT_EQ(decoded[0], timestamps[0]);
        EXPECT_EQ(decoded[1], timestamps[1]);
        EXPECT_EQ(decoded[2], timestamps[2]);
    }
}