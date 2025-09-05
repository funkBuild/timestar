#include <gtest/gtest.h>
#include "../../../lib/encoding/simple8b.hpp"
#include "../../../lib/encoding/simple8b_exception.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include <iostream>
#include <vector>

TEST(Simple8BDebug, ReproducePackFailed) {
    // This test reproduces the exact timestamp that causes "pack failed"
    // from the HTTP write test with measurement "temperature"
    
    // The timestamp from the Python test that causes the error
    uint64_t problematic_timestamp = 1756563343901168896ULL;
    
    std::cout << "Testing timestamp: " << problematic_timestamp << std::endl;
    std::cout << "Timestamp in hex: 0x" << std::hex << problematic_timestamp << std::dec << std::endl;
    
    // Test 1: Direct Simple8B encoding of the timestamp - should throw exception
    {
        std::vector<uint64_t> values = {problematic_timestamp};
        std::cout << "\nTest 1: Direct Simple8B encoding of single timestamp" << std::endl;
        
        bool exception_thrown = false;
        try {
            AlignedBuffer encoded = Simple8B::encode(values);
            std::cout << "ERROR: Should have thrown exception!" << std::endl;
        } catch (const Simple8BValueTooLargeException& e) {
            exception_thrown = true;
            std::cout << "Exception correctly thrown: " << e.what() << std::endl;
            EXPECT_EQ(e.getValue(), problematic_timestamp);
            EXPECT_EQ(e.getOffset(), 0);
        }
        EXPECT_TRUE(exception_thrown);
    }
    
    // Test 2: Encoding two timestamps - first one should throw
    {
        std::vector<uint64_t> values = {
            problematic_timestamp,
            problematic_timestamp + 1000000000  // 1 second later
        };
        std::cout << "\nTest 2: Simple8B encoding of two timestamps" << std::endl;
        
        try {
            AlignedBuffer encoded = Simple8B::encode(values);
            std::cout << "ERROR: Should have thrown exception!" << std::endl;
        } catch (const Simple8BValueTooLargeException& e) {
            std::cout << "Exception correctly thrown for first value: " << e.what() << std::endl;
        }
    }
    
    // Test 3: Using IntegerEncoder (as used in TSM writer)
    {
        std::vector<uint64_t> timestamps = {
            problematic_timestamp,
            problematic_timestamp + 1000000000  // 1 second later
        };
        std::cout << "\nTest 3: IntegerEncoder with timestamps" << std::endl;
        std::cout << "First timestamp: " << timestamps[0] << std::endl;
        std::cout << "Second timestamp: " << timestamps[1] << std::endl;
        std::cout << "Delta: " << (timestamps[1] - timestamps[0]) << std::endl;
        
        // IntegerEncoder uses Simple8B internally, so it will also throw
        try {
            AlignedBuffer encoded = IntegerEncoder::encode(timestamps);
            std::cout << "ERROR: Should have thrown exception!" << std::endl;
        } catch (const Simple8BValueTooLargeException& e) {
            std::cout << "Exception correctly propagated through IntegerEncoder: " << e.what() << std::endl;
        }
    }
    
    // Test 4: Check bit width requirements
    {
        std::cout << "\nTest 4: Analyzing bit requirements" << std::endl;
        std::cout << "Timestamp requires " << (64 - __builtin_clzll(problematic_timestamp)) << " bits" << std::endl;
        
        // The maximum value that can be stored with each Simple8B selector
        std::vector<std::pair<int, uint64_t>> max_values = {
            {0, 0},  // Special case for runs of 1s
            {1, 0},  // Special case for runs of 1s  
            {2, (1ULL << 1) - 1},   // 60 values of 1 bit each
            {3, (1ULL << 2) - 1},   // 30 values of 2 bits each
            {4, (1ULL << 3) - 1},   // 20 values of 3 bits each
            {5, (1ULL << 4) - 1},   // 15 values of 4 bits each
            {6, (1ULL << 5) - 1},   // 12 values of 5 bits each
            {7, (1ULL << 6) - 1},   // 10 values of 6 bits each
            {8, (1ULL << 7) - 1},   // 8 values of 7 bits each
            {9, (1ULL << 8) - 1},   // 7 values of 8 bits each
            {10, (1ULL << 10) - 1}, // 6 values of 10 bits each
            {11, (1ULL << 12) - 1}, // 5 values of 12 bits each
            {12, (1ULL << 15) - 1}, // 4 values of 15 bits each
            {13, (1ULL << 20) - 1}, // 3 values of 20 bits each
            {14, (1ULL << 30) - 1}, // 2 values of 30 bits each
            {15, (1ULL << 60) - 1}  // 1 value of 60 bits
        };
        
        bool can_encode = false;
        for (const auto& [selector, max_val] : max_values) {
            if (problematic_timestamp <= max_val) {
                std::cout << "Can be encoded with selector " << selector 
                         << " (max value: " << max_val << ")" << std::endl;
                can_encode = true;
                break;
            }
        }
        
        if (!can_encode) {
            std::cout << "ERROR: Timestamp " << problematic_timestamp 
                     << " CANNOT be encoded with any Simple8B selector!" << std::endl;
            std::cout << "Maximum encodable value is " << ((1ULL << 60) - 1) 
                     << " but timestamp is " << problematic_timestamp << std::endl;
        }
    }
}

TEST(Simple8BDebug, TestMaxValues) {
    // Test the maximum values that Simple8B can handle
    
    uint64_t max_60_bit = (1ULL << 60) - 1;
    uint64_t over_60_bit = 1ULL << 60;
    
    std::cout << "Max 60-bit value: " << max_60_bit << std::endl;
    std::cout << "Over 60-bit value: " << over_60_bit << std::endl;
    
    // This should work
    {
        std::vector<uint64_t> values = {max_60_bit};
        std::cout << "Encoding max 60-bit value..." << std::endl;
        AlignedBuffer encoded = Simple8B::encode(values);
        EXPECT_GT(encoded.size(), 0);
    }
    
    // This should throw exception
    {
        std::vector<uint64_t> values = {over_60_bit};
        std::cout << "Encoding over 60-bit value (should throw exception)..." << std::endl;
        
        try {
            AlignedBuffer encoded = Simple8B::encode(values);
            FAIL() << "Should have thrown Simple8BValueTooLargeException";
        } catch (const Simple8BValueTooLargeException& e) {
            std::cout << "Exception correctly thrown: " << e.what() << std::endl;
            EXPECT_EQ(e.getValue(), over_60_bit);
            EXPECT_EQ(Simple8BValueTooLargeException::getMaxEncodableValue(), max_60_bit);
        }
    }
    
    // Test the problematic timestamp
    uint64_t problematic = 1756563343901168896ULL;
    std::cout << "\nProblematic timestamp analysis:" << std::endl;
    std::cout << "Value: " << problematic << std::endl;
    std::cout << "In binary: requires " << (64 - __builtin_clzll(problematic)) << " bits" << std::endl;
    std::cout << "Exceeds 60-bit limit: " << (problematic > max_60_bit ? "YES" : "NO") << std::endl;
    
    if (problematic > max_60_bit) {
        uint64_t overflow_bits = 64 - __builtin_clzll(problematic) - 60;
        std::cout << "Overflow by " << overflow_bits << " bits" << std::endl;
    }
}

