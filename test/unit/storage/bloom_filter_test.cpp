#include <storage/bloom_filter.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

// Helper to create a bloom filter with reasonable parameters.
static bloom_filter make_bloom_filter(unsigned long long projected = 1000, double fpp = 0.01) {
    bloom_parameters params;
    params.projected_element_count = projected;
    params.false_positive_probability = fpp;
    params.random_seed = 0xA5A5A5A55A5A5A5AULL;
    params.compute_optimal_parameters();
    return bloom_filter(params);
}

// ==========================================================================
// Insert + contains round-trip
// ==========================================================================
TEST(StorageBloomFilterTest, InsertAndContainsRoundTrip) {
    auto bf = make_bloom_filter();

    std::vector<std::string> keys = {
        "temperature", "humidity", "pressure", "cpu_usage",
        "memory_free", "disk_io", "series_key_1234567890", "a", "ab"
    };

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key)) << "Must find inserted key: " << key;
    }
}

// ==========================================================================
// No false negatives with many insertions
// ==========================================================================
TEST(StorageBloomFilterTest, NoFalseNegatives) {
    auto bf = make_bloom_filter(10000, 0.001);

    std::vector<std::string> keys;
    keys.reserve(500);
    for (int i = 0; i < 500; ++i) {
        keys.push_back("series_" + std::to_string(i));
    }

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key)) << "False negative for key: " << key;
    }
}

// ==========================================================================
// Empty filter returns false for all queries
// ==========================================================================
TEST(StorageBloomFilterTest, EmptyFilterReturnsFalse) {
    auto bf = make_bloom_filter();

    EXPECT_FALSE(bf.contains(std::string("anything")));
    EXPECT_FALSE(bf.contains(std::string("test")));
    EXPECT_FALSE(bf.contains(std::string("")));

    uint64_t id = 42;
    EXPECT_FALSE(bf.contains(id));
}

// ==========================================================================
// Default-constructed filter returns false
// ==========================================================================
TEST(StorageBloomFilterTest, DefaultConstructedReturnsFalse) {
    bloom_filter bf;

    EXPECT_FALSE(bf.contains(std::string("anything")));
    EXPECT_EQ(bf.size(), 0ULL);
    EXPECT_TRUE(!bf);  // operator! returns true for empty
}

// ==========================================================================
// False positive rate validation
// ==========================================================================
TEST(StorageBloomFilterTest, FalsePositiveRateValidation) {
    constexpr size_t N = 10000;
    constexpr double targetFPP = 0.01;
    auto bf = make_bloom_filter(N, targetFPP);

    // Insert N keys
    for (size_t i = 0; i < N; ++i) {
        std::string key = "inserted_" + std::to_string(i);
        bf.insert(key);
    }

    // Test M non-keys and count false positives
    constexpr size_t M = 100000;
    size_t falsePositives = 0;
    for (size_t i = 0; i < M; ++i) {
        std::string key = "NOT_inserted_" + std::to_string(i);
        if (bf.contains(key)) {
            ++falsePositives;
        }
    }

    double observedFPP = static_cast<double>(falsePositives) / static_cast<double>(M);
    // Allow up to 3x the target FPP to account for statistical variation
    EXPECT_LT(observedFPP, targetFPP * 3.0)
        << "Observed FPP " << observedFPP << " exceeds 3x target " << targetFPP
        << " (false positives: " << falsePositives << "/" << M << ")";
    // Also check it's not unreasonably low (would indicate a bug)
    // With 10K elements and 1% FPP, we expect ~1000 FPs in 100K tests
    // A zero-FP result would be suspicious
    EXPECT_GT(falsePositives, 0u) << "Zero false positives is suspicious for a 1% FPP filter";
}

// ==========================================================================
// Hash consistency: two filters with same params produce identical results
// ==========================================================================
TEST(StorageBloomFilterTest, HashConsistency) {
    auto bf1 = make_bloom_filter();
    auto bf2 = make_bloom_filter();

    std::vector<std::string> keys = {
        "a", "ab", "abc", "abcd", "abcde",
        "abcdef", "abcdefg", "abcdefgh", "abcdefghi",
        "this_is_a_longer_key_for_testing_hash_consistency"
    };

    for (const auto& key : keys) {
        bf1.insert(key);
        bf2.insert(key);
    }

    EXPECT_EQ(bf1, bf2) << "Identical inputs must produce identical state";

    for (const auto& key : keys) {
        EXPECT_EQ(bf1.contains(key), bf2.contains(key));
    }
}

// ==========================================================================
// POD type inserts (uint64_t)
// ==========================================================================
TEST(StorageBloomFilterTest, IntegerTypeInserts) {
    auto bf = make_bloom_filter();

    std::vector<uint64_t> ids = {1, 42, 100, 999, 123456789, 0xDEADBEEFULL};

    for (auto id : ids) {
        bf.insert(id);
    }

    for (auto id : ids) {
        EXPECT_TRUE(bf.contains(id)) << "Must find inserted uint64_t: " << id;
    }
}

// ==========================================================================
// std::array<uint8_t, 16> — the SeriesId128 path
// ==========================================================================
TEST(StorageBloomFilterTest, ArrayInsertAndContains) {
    auto bf = make_bloom_filter();

    std::array<uint8_t, 16> key1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    std::array<uint8_t, 16> key2 = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    std::array<uint8_t, 16> key3 = {};  // all zeros

    bf.insert(key1);
    bf.insert(key2);
    bf.insert(key3);

    EXPECT_TRUE(bf.contains(key1));
    EXPECT_TRUE(bf.contains(key2));
    EXPECT_TRUE(bf.contains(key3));
}

// ==========================================================================
// Binary key support (embedded nulls)
// ==========================================================================
TEST(StorageBloomFilterTest, BinaryKeysWithEmbeddedNulls) {
    auto bf = make_bloom_filter();

    // Keys with embedded null bytes
    unsigned char key1[] = {0x00, 0x01, 0x02, 0x03, 0x00, 0x05};
    unsigned char key2[] = {0x00, 0x00, 0x00, 0x00};
    unsigned char key3[] = {0xFF, 0x00, 0xFF, 0x00, 0xFF};

    bf.insert(key1, sizeof(key1));
    bf.insert(key2, sizeof(key2));
    bf.insert(key3, sizeof(key3));

    EXPECT_TRUE(bf.contains(key1, sizeof(key1)));
    EXPECT_TRUE(bf.contains(key2, sizeof(key2)));
    EXPECT_TRUE(bf.contains(key3, sizeof(key3)));
}

// ==========================================================================
// Single key
// ==========================================================================
TEST(StorageBloomFilterTest, SingleKey) {
    auto bf = make_bloom_filter();

    bf.insert(std::string("only_key"));
    EXPECT_TRUE(bf.contains(std::string("only_key")));
}

// ==========================================================================
// Zero-length key
// ==========================================================================
TEST(StorageBloomFilterTest, ZeroLengthKey) {
    auto bf = make_bloom_filter();

    bf.insert(std::string(""));
    EXPECT_TRUE(bf.contains(std::string("")));

    // Also test via raw pointer path
    bf.insert(static_cast<const unsigned char*>(nullptr), 0);
    // xxHash handles zero-length gracefully
}

// ==========================================================================
// Large filter: 100K+ keys
// ==========================================================================
TEST(StorageBloomFilterTest, LargeFilter100K) {
    constexpr size_t N = 100000;
    auto bf = make_bloom_filter(N, 0.01);

    for (size_t i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        bf.insert(key);
    }

    // Verify no false negatives
    for (size_t i = 0; i < N; ++i) {
        std::string key = "key_" + std::to_string(i);
        ASSERT_TRUE(bf.contains(key)) << "False negative at key_" << i;
    }

    EXPECT_EQ(bf.element_count(), N);
}

// ==========================================================================
// Clear resets the filter
// ==========================================================================
TEST(StorageBloomFilterTest, ClearResetsFilter) {
    auto bf = make_bloom_filter();

    bf.insert(std::string("test_key"));
    EXPECT_TRUE(bf.contains(std::string("test_key")));

    bf.clear();
    EXPECT_FALSE(bf.contains(std::string("test_key")));
    EXPECT_EQ(bf.element_count(), 0ULL);
}

// ==========================================================================
// Unaligned data: strings of various lengths exercise all hash paths
// ==========================================================================
TEST(StorageBloomFilterTest, UnalignedData) {
    auto bf = make_bloom_filter();

    std::vector<std::string> keys;
    for (int len = 1; len <= 32; ++len) {
        keys.push_back(std::string(len, 'x'));
    }
    keys.push_back("ABCDE");
    keys.push_back("ABCDEF");
    keys.push_back("ABCDEFG");

    for (const auto& key : keys) {
        bf.insert(key);
    }

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.contains(key)) << "Failed for key of length " << key.size();
    }
}

// ==========================================================================
// Effective FPP is reasonable
// ==========================================================================
TEST(StorageBloomFilterTest, EffectiveFPP) {
    auto bf = make_bloom_filter(1000, 0.01);

    for (int i = 0; i < 1000; ++i) {
        bf.insert(std::string("key_" + std::to_string(i)));
    }

    double fpp = bf.effective_fpp();
    EXPECT_GT(fpp, 0.0);
    EXPECT_LT(fpp, 0.1);  // Should be near 0.01, definitely under 10%
}

// ==========================================================================
// operator! for empty vs non-empty
// ==========================================================================
TEST(StorageBloomFilterTest, OperatorBang) {
    bloom_filter empty;
    EXPECT_TRUE(!empty);

    auto bf = make_bloom_filter();
    EXPECT_FALSE(!bf);
}

// ==========================================================================
// Assignment from constructed filter
// ==========================================================================
TEST(StorageBloomFilterTest, Assignment) {
    bloom_filter bf;
    EXPECT_TRUE(!bf);

    bf = make_bloom_filter();
    EXPECT_FALSE(!bf);

    bf.insert(std::string("test"));
    EXPECT_TRUE(bf.contains(std::string("test")));
}

// ==========================================================================
// Table access for raw bytes
// ==========================================================================
TEST(StorageBloomFilterTest, TableAccess) {
    auto bf = make_bloom_filter(100, 0.01);

    // Table should exist and be non-null
    EXPECT_NE(bf.table(), nullptr);
    EXPECT_GT(bf.table_size_bytes(), 0u);
    EXPECT_EQ(bf.table_size_bytes(), bf.size() / 8);

    // Empty filter should have all zeros
    for (std::size_t i = 0; i < bf.table_size_bytes(); ++i) {
        EXPECT_EQ(bf.table()[i], 0u);
    }

    // After insert, at least one byte should be non-zero
    bf.insert(std::string("test"));
    bool anyNonZero = false;
    for (std::size_t i = 0; i < bf.table_size_bytes(); ++i) {
        if (bf.table()[i] != 0) {
            anyNonZero = true;
            break;
        }
    }
    EXPECT_TRUE(anyNonZero);
}

// ==========================================================================
// Optimal parameters computed correctly
// ==========================================================================
TEST(StorageBloomFilterTest, OptimalParameters) {
    bloom_parameters params;
    params.projected_element_count = 1000;
    params.false_positive_probability = 0.01;
    EXPECT_TRUE(params.compute_optimal_parameters());

    EXPECT_GT(params.optimal_parameters.number_of_hashes, 0u);
    EXPECT_GT(params.optimal_parameters.table_size, 0ULL);
    // table_size should be a multiple of 8
    EXPECT_EQ(params.optimal_parameters.table_size % 8, 0ULL);

    // For 1000 elements and 1% FPP, k should be around 7
    EXPECT_GE(params.optimal_parameters.number_of_hashes, 5u);
    EXPECT_LE(params.optimal_parameters.number_of_hashes, 10u);
}

// ==========================================================================
// Invalid parameters
// ==========================================================================
TEST(StorageBloomFilterTest, InvalidParameters) {
    bloom_parameters params;

    params.projected_element_count = 0;
    params.false_positive_probability = 0.01;
    EXPECT_FALSE(params.compute_optimal_parameters());

    params.projected_element_count = 1000;
    params.false_positive_probability = 0.0;
    EXPECT_FALSE(params.compute_optimal_parameters());

    params.false_positive_probability = 1.0;
    EXPECT_FALSE(params.compute_optimal_parameters());

    params.false_positive_probability = -0.5;
    EXPECT_FALSE(params.compute_optimal_parameters());
}

// ==========================================================================
// char* + length insert/contains path
// ==========================================================================
TEST(StorageBloomFilterTest, CharPtrInsertAndContains) {
    auto bf = make_bloom_filter();

    const char* key = "hello_world";
    bf.insert(key, std::strlen(key));
    EXPECT_TRUE(bf.contains(key, std::strlen(key)));
}

// ==========================================================================
// string_view insert/contains path
// ==========================================================================
TEST(StorageBloomFilterTest, StringViewInsertAndContains) {
    auto bf = make_bloom_filter();

    std::string_view sv = "test_string_view";
    bf.insert(sv);
    EXPECT_TRUE(bf.contains(sv));
}

// ==========================================================================
// Very small FPP produces larger filter
// ==========================================================================
TEST(StorageBloomFilterTest, SmallFPPProducesLargerFilter) {
    auto bf_low = make_bloom_filter(1000, 0.1);
    auto bf_high = make_bloom_filter(1000, 0.0001);

    EXPECT_GT(bf_high.size(), bf_low.size())
        << "Lower FPP should produce a larger filter";
}

// ==========================================================================
// Multiple different seed values produce different filters
// ==========================================================================
TEST(StorageBloomFilterTest, DifferentSeedsProduceDifferentBits) {
    bloom_parameters p1, p2;
    p1.projected_element_count = p2.projected_element_count = 1000;
    p1.false_positive_probability = p2.false_positive_probability = 0.01;
    p1.random_seed = 1;
    p2.random_seed = 2;
    p1.compute_optimal_parameters();
    p2.compute_optimal_parameters();

    bloom_filter bf1(p1);
    bloom_filter bf2(p2);

    bf1.insert(std::string("test_key"));
    bf2.insert(std::string("test_key"));

    // Both should contain the key
    EXPECT_TRUE(bf1.contains(std::string("test_key")));
    EXPECT_TRUE(bf2.contains(std::string("test_key")));

    // But the bit patterns should differ (with overwhelming probability)
    EXPECT_NE(bf1, bf2);
}

// ==========================================================================
// Seed accessor
// ==========================================================================
TEST(StorageBloomFilterTest, SeedAccessor) {
    bloom_parameters params;
    params.projected_element_count = 1000;
    params.false_positive_probability = 0.01;
    params.random_seed = 0xDEADBEEF42ULL;
    params.compute_optimal_parameters();

    bloom_filter bf(params);
    EXPECT_EQ(bf.seed(), 0xDEADBEEF42ULL);
}
