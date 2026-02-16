#include <gtest/gtest.h>
#include <storage/bloom_filter.hpp>

#include <fstream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

// Helper to create a bloom filter with reasonable parameters
static bloom_filter make_bloom_filter(unsigned long long projected = 1000, double fpp = 0.01) {
    bloom_parameters params;
    params.projected_element_count = projected;
    params.false_positive_probability = fpp;
    params.random_seed = 0xA5A5A5A55A5A5A5AULL;
    params.compute_optimal_parameters();
    return bloom_filter(params);
}

// ------------------------------------------------------------------
// Functional test: Correctness preserved
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, CorrectnessPreserved) {
    auto bf = make_bloom_filter();

    std::vector<std::string> keys = {
        "temperature", "humidity", "pressure",
        "cpu_usage", "memory_free", "disk_io",
        "series_key_1234567890", "a", "ab"
    };

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key))
            << "Bloom filter must find inserted key: " << key;
    }
}

// ------------------------------------------------------------------
// Functional test: False negative check
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, NoFalseNegatives) {
    auto bf = make_bloom_filter(10000, 0.001);

    // Insert a large number of entries and verify none are missed
    std::vector<std::string> keys;
    keys.reserve(500);
    for (int i = 0; i < 500; ++i) {
        keys.push_back("series_" + std::to_string(i));
    }

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key))
            << "False negative detected for key: " << key;
    }
}

// ------------------------------------------------------------------
// Functional test: Unaligned data (remainder paths)
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, UnalignedData) {
    auto bf = make_bloom_filter();

    // Strings of various lengths to exercise all remainder paths in hash_ap:
    //   len 1 -> 1-byte remainder only
    //   len 2 -> 2-byte remainder
    //   len 3 -> 2-byte + 1-byte remainder
    //   len 4 -> 4-byte remainder
    //   len 5 -> 4-byte + 1-byte remainder
    //   len 6 -> 4-byte + 2-byte remainder
    //   len 7 -> 4-byte + 2-byte + 1-byte remainder
    //   len 8 -> one full 8-byte iteration, no remainder
    //   len 9 -> one 8-byte iteration + 1-byte remainder
    //   len 10 -> one 8-byte iteration + 2-byte remainder
    //   len 11 -> one 8-byte iteration + 2-byte + 1-byte remainder
    //   len 12 -> one 8-byte iteration + 4-byte remainder
    //   len 13 -> one 8-byte iteration + 4-byte + 1-byte remainder
    //   len 14 -> one 8-byte iteration + 4-byte + 2-byte remainder
    //   len 15 -> one 8-byte iteration + 4-byte + 2-byte + 1-byte remainder

    std::vector<std::string> keys;
    for (int len = 1; len <= 15; ++len) {
        keys.push_back(std::string(len, 'x'));
    }

    // Also add some varied-content strings of specific remainder lengths
    keys.push_back("ABCDE");     // 5 bytes: 4-byte + 1-byte remainder
    keys.push_back("ABCDEF");    // 6 bytes: 4-byte + 2-byte remainder
    keys.push_back("ABCDEFG");   // 7 bytes: 4-byte + 2-byte + 1-byte remainder

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key))
            << "Failed for key of length " << key.size() << ": " << key;
    }
}

// ------------------------------------------------------------------
// Functional test: Hash consistency
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, HashConsistency) {
    // Create two separate bloom filters with identical parameters
    auto bf1 = make_bloom_filter();
    auto bf2 = make_bloom_filter();

    std::vector<std::string> keys = {
        "a", "ab", "abc", "abcd", "abcde", "abcdef",
        "abcdefg", "abcdefgh", "abcdefghi",
        "this_is_a_longer_key_for_testing_hash_consistency"
    };

    for (const auto& key : keys) {
        bf1.insert(key);
        bf2.insert(key);
    }

    // Both filters should be identical since they use the same parameters and data
    EXPECT_EQ(bf1, bf2) << "Two bloom filters with identical inputs must produce identical state";

    // Also verify contains is consistent
    for (const auto& key : keys) {
        EXPECT_EQ(bf1.contains(key), bf2.contains(key))
            << "Inconsistent contains() result for key: " << key;
    }
}

// ------------------------------------------------------------------
// Functional test: Integer type inserts
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, IntegerTypeInserts) {
    auto bf = make_bloom_filter();

    std::vector<uint64_t> series_ids = {1, 42, 100, 999, 123456789, 0xDEADBEEFULL};

    for (auto id : series_ids) {
        bf.insert(id);
    }

    for (auto id : series_ids) {
        EXPECT_TRUE(bf.contains(id))
            << "Bloom filter must find inserted uint64_t: " << id;
    }
}

// ------------------------------------------------------------------
// Source-inspection test: No reinterpret_cast in hash_ap()
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, SourceNoReinterpretCast) {
#ifndef BLOOM_FILTER_SOURCE_PATH
    GTEST_SKIP() << "BLOOM_FILTER_SOURCE_PATH not defined";
#else
    std::ifstream file(BLOOM_FILTER_SOURCE_PATH);
    ASSERT_TRUE(file.is_open())
        << "Cannot open bloom_filter.hpp at: " << BLOOM_FILTER_SOURCE_PATH;

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

    // Find the hash_ap function definition (not a call site)
    // The definition has the form: hash_ap(const unsigned char*
    auto hash_ap_start = content.find("hash_ap(const unsigned char*");
    ASSERT_NE(hash_ap_start, std::string::npos)
        << "Could not find hash_ap function definition in source";

    // Find the function body (from first { after hash_ap to matching })
    auto body_start = content.find('{', hash_ap_start);
    ASSERT_NE(body_start, std::string::npos);

    // Walk to find the matching closing brace
    int depth = 1;
    size_t pos = body_start + 1;
    while (pos < content.size() && depth > 0) {
        if (content[pos] == '{') ++depth;
        else if (content[pos] == '}') --depth;
        ++pos;
    }

    std::string hash_ap_body = content.substr(body_start, pos - body_start);

    // Verify no reinterpret_cast<const unsigned int*> in hash_ap
    EXPECT_EQ(hash_ap_body.find("reinterpret_cast<const unsigned int*>"), std::string::npos)
        << "hash_ap() must not use reinterpret_cast<const unsigned int*> (strict aliasing violation)";

    // Verify no reinterpret_cast<const unsigned short*> in hash_ap
    EXPECT_EQ(hash_ap_body.find("reinterpret_cast<const unsigned short*>"), std::string::npos)
        << "hash_ap() must not use reinterpret_cast<const unsigned short*> (strict aliasing violation)";
#endif
}

// ------------------------------------------------------------------
// Source-inspection test: Uses std::memcpy for type punning
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, SourceUsesMemcpy) {
#ifndef BLOOM_FILTER_SOURCE_PATH
    GTEST_SKIP() << "BLOOM_FILTER_SOURCE_PATH not defined";
#else
    std::ifstream file(BLOOM_FILTER_SOURCE_PATH);
    ASSERT_TRUE(file.is_open())
        << "Cannot open bloom_filter.hpp at: " << BLOOM_FILTER_SOURCE_PATH;

    std::string content((std::istreambuf_iterator<char>(file)),
                         std::istreambuf_iterator<char>());

    // Find the hash_ap function definition (not a call site)
    // The definition has the form: hash_ap(const unsigned char*
    auto hash_ap_start = content.find("hash_ap(const unsigned char*");
    ASSERT_NE(hash_ap_start, std::string::npos)
        << "Could not find hash_ap function definition in source";

    auto body_start = content.find('{', hash_ap_start);
    ASSERT_NE(body_start, std::string::npos);

    int depth = 1;
    size_t pos = body_start + 1;
    while (pos < content.size() && depth > 0) {
        if (content[pos] == '{') ++depth;
        else if (content[pos] == '}') --depth;
        ++pos;
    }

    std::string hash_ap_body = content.substr(body_start, pos - body_start);

    // Verify std::memcpy is used in hash_ap
    EXPECT_NE(hash_ap_body.find("memcpy"), std::string::npos)
        << "hash_ap() must use std::memcpy for type punning instead of reinterpret_cast";

    // Verify the include for cstring is present in the file
    EXPECT_NE(content.find("#include <cstring>"), std::string::npos)
        << "bloom_filter.hpp must include <cstring> for std::memcpy";
#endif
}
