#include <storage/bloom_filter.hpp>

#include <gtest/gtest.h>

#include <fstream>
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
        "temperature",           "humidity", "pressure", "cpu_usage", "memory_free", "disk_io",
        "series_key_1234567890", "a",        "ab"};

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key)) << "Bloom filter must find inserted key: " << key;
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
        EXPECT_TRUE(bf.contains(key)) << "False negative detected for key: " << key;
    }
}

// ------------------------------------------------------------------
// Functional test: Unaligned data (remainder paths)
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, UnalignedData) {
    auto bf = make_bloom_filter();

    // Strings of various lengths to exercise hash function with all input sizes
    std::vector<std::string> keys;
    for (int len = 1; len <= 15; ++len) {
        keys.push_back(std::string(len, 'x'));
    }

    // Also add some varied-content strings of specific lengths
    keys.push_back("ABCDE");
    keys.push_back("ABCDEF");
    keys.push_back("ABCDEFG");

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key)) << "Failed for key of length " << key.size() << ": " << key;
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
        "a",      "ab",      "abc",      "abcd",      "abcde",
        "abcdef", "abcdefg", "abcdefgh", "abcdefghi", "this_is_a_longer_key_for_testing_hash_consistency"};

    for (const auto& key : keys) {
        bf1.insert(key);
        bf2.insert(key);
    }

    // Both filters should be identical since they use the same parameters and data
    EXPECT_EQ(bf1, bf2) << "Two bloom filters with identical inputs must produce identical state";

    // Also verify contains is consistent
    for (const auto& key : keys) {
        EXPECT_EQ(bf1.contains(key), bf2.contains(key)) << "Inconsistent contains() result for key: " << key;
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
        EXPECT_TRUE(bf.contains(id)) << "Bloom filter must find inserted uint64_t: " << id;
    }
}

// ------------------------------------------------------------------
// Source-inspection test: Uses xxHash, not custom hash_ap
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, SourceUsesXxHash) {
#ifndef BLOOM_FILTER_SOURCE_PATH
    GTEST_SKIP() << "BLOOM_FILTER_SOURCE_PATH not defined";
#else
    std::ifstream file(BLOOM_FILTER_SOURCE_PATH);
    ASSERT_TRUE(file.is_open()) << "Cannot open bloom_filter.hpp at: " << BLOOM_FILTER_SOURCE_PATH;

    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // Verify xxhash is included
    EXPECT_NE(content.find("#include <xxhash.h>"), std::string::npos)
        << "bloom_filter.hpp must include <xxhash.h>";

    // Verify XXH3_128bits is used for hashing
    EXPECT_NE(content.find("XXH3_128bits"), std::string::npos)
        << "bloom_filter.hpp must use XXH3_128bits for hashing";

    // Verify no srand/rand usage (old library used these)
    EXPECT_EQ(content.find("srand("), std::string::npos)
        << "bloom_filter.hpp must not use srand()";
    EXPECT_EQ(content.find("rand()"), std::string::npos)
        << "bloom_filter.hpp must not use rand()";

    // Verify no hash_ap function (replaced by xxHash)
    EXPECT_EQ(content.find("hash_ap("), std::string::npos)
        << "bloom_filter.hpp must not contain the old hash_ap function";
#endif
}

// ------------------------------------------------------------------
// Source-inspection test: No strict aliasing violations
// ------------------------------------------------------------------
TEST(BloomFilterAliasingTest, SourceNoAliasingViolations) {
#ifndef BLOOM_FILTER_SOURCE_PATH
    GTEST_SKIP() << "BLOOM_FILTER_SOURCE_PATH not defined";
#else
    std::ifstream file(BLOOM_FILTER_SOURCE_PATH);
    ASSERT_TRUE(file.is_open()) << "Cannot open bloom_filter.hpp at: " << BLOOM_FILTER_SOURCE_PATH;

    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());

    // Verify no reinterpret_cast<const unsigned int*> (strict aliasing violation)
    EXPECT_EQ(content.find("reinterpret_cast<const unsigned int*>"), std::string::npos)
        << "bloom_filter.hpp must not use reinterpret_cast<const unsigned int*>";

    // Verify no reinterpret_cast<const unsigned short*> (strict aliasing violation)
    EXPECT_EQ(content.find("reinterpret_cast<const unsigned short*>"), std::string::npos)
        << "bloom_filter.hpp must not use reinterpret_cast<const unsigned short*>";
#endif
}
