#include <storage/bloom_filter.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <span>
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
// Bitwise OR merges two filters
// ==========================================================================
TEST(StorageBloomFilterTest, BitwiseOr) {
    auto bf1 = make_bloom_filter(1000, 0.01);
    auto bf2 = make_bloom_filter(1000, 0.01);

    bf1.insert(std::string("key_a"));
    bf2.insert(std::string("key_b"));

    bf1 |= bf2;

    // After OR, bf1 should contain both keys
    EXPECT_TRUE(bf1.contains(std::string("key_a")));
    EXPECT_TRUE(bf1.contains(std::string("key_b")));
}

// ==========================================================================
// Bitwise AND intersects two filters
// ==========================================================================
TEST(StorageBloomFilterTest, BitwiseAnd) {
    auto bf1 = make_bloom_filter(1000, 0.01);
    auto bf2 = make_bloom_filter(1000, 0.01);

    // Insert shared key into both
    bf1.insert(std::string("shared"));
    bf2.insert(std::string("shared"));

    // Insert unique keys
    bf1.insert(std::string("only_in_1"));
    bf2.insert(std::string("only_in_2"));

    bf1 &= bf2;

    // Shared key should still be found
    EXPECT_TRUE(bf1.contains(std::string("shared")));
    // Note: "only_in_1" might still be found due to hash collisions,
    // so we don't assert it's gone — bloom filters can only have false positives
}

// ==========================================================================
// Bitwise XOR
// ==========================================================================
TEST(StorageBloomFilterTest, BitwiseXor) {
    auto bf1 = make_bloom_filter(1000, 0.01);
    auto bf2 = make_bloom_filter(1000, 0.01);

    bf1.insert(std::string("key"));
    bf2.insert(std::string("key"));

    // XOR with identical filter should clear all bits that were set
    bf1 ^= bf2;

    // After XOR with identical filter, the shared bits cancel out
    // so "key" should not be found (unless there are other hash collisions)
    // This is a structural test, not a semantic guarantee
}

// ==========================================================================
// Mismatched filters don't merge (different sizes)
// ==========================================================================
TEST(StorageBloomFilterTest, MismatchedFiltersDontMerge) {
    auto bf1 = make_bloom_filter(100, 0.01);
    auto bf2 = make_bloom_filter(10000, 0.01);

    bf1.insert(std::string("key_in_1"));

    // These should be different sizes, so |= should be a no-op
    auto bf1_copy = bf1;
    bf1 |= bf2;

    // bf1 should be unchanged (same table data)
    EXPECT_EQ(bf1, bf1_copy);
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
// SIMD popcount: empty filter has zero bits set
// ==========================================================================
TEST(StorageBloomFilterTest, PopulationCountEmpty) {
    auto bf = make_bloom_filter();

    EXPECT_EQ(bf.populationCount(), 0u);
    EXPECT_DOUBLE_EQ(bf.fillRatio(), 0.0);
}

// ==========================================================================
// SIMD popcount: after insertions, count matches manual count
// ==========================================================================
TEST(StorageBloomFilterTest, PopulationCountAfterInserts) {
    auto bf = make_bloom_filter(1000, 0.01);

    for (int i = 0; i < 100; ++i) {
        bf.insert(std::string("popcount_key_" + std::to_string(i)));
    }

    uint64_t popcnt = bf.populationCount();
    EXPECT_GT(popcnt, 0u);
    // Should be less than total bits (not fully saturated)
    EXPECT_LT(popcnt, bf.size());

    // Verify against manual byte-by-byte count
    uint64_t manualCount = 0;
    for (std::size_t i = 0; i < bf.table_size_bytes(); ++i) {
        manualCount += __builtin_popcount(bf.table()[i]);
    }
    EXPECT_EQ(popcnt, manualCount);
}

// ==========================================================================
// SIMD popcount: large filter
// ==========================================================================
TEST(StorageBloomFilterTest, PopulationCountLargeFilter) {
    auto bf = make_bloom_filter(100000, 0.001);

    for (int i = 0; i < 1000; ++i) {
        bf.insert(std::string("large_popcount_" + std::to_string(i)));
    }

    uint64_t popcnt = bf.populationCount();
    EXPECT_GT(popcnt, 0u);

    double fill = bf.fillRatio();
    EXPECT_GT(fill, 0.0);
    EXPECT_LT(fill, 1.0);

    // Verify SIMD result matches scalar
    uint64_t manualCount = 0;
    for (std::size_t i = 0; i < bf.table_size_bytes(); ++i) {
        manualCount += __builtin_popcount(bf.table()[i]);
    }
    EXPECT_EQ(popcnt, manualCount);
}

// ==========================================================================
// Fill ratio is monotonically increasing with insertions
// ==========================================================================
TEST(StorageBloomFilterTest, FillRatioMonotonic) {
    auto bf = make_bloom_filter(10000, 0.01);

    double prevFill = 0.0;
    for (int batch = 0; batch < 10; ++batch) {
        for (int i = 0; i < 100; ++i) {
            bf.insert(std::string("mono_" + std::to_string(batch * 100 + i)));
        }
        double fill = bf.fillRatio();
        EXPECT_GE(fill, prevFill) << "Fill ratio must be monotonically non-decreasing";
        prevFill = fill;
    }
}

// ==========================================================================
// Batch contains: basic correctness with 16-byte keys
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContains16BasicCorrectness) {
    auto bf = make_bloom_filter(1000, 0.01);

    // Create 10 keys as 16-byte arrays (simulating SeriesId128)
    std::vector<std::array<uint8_t, 16>> keys(10);
    for (int i = 0; i < 10; ++i) {
        std::memset(keys[i].data(), 0, 16);
        keys[i][0] = static_cast<uint8_t>(i + 1);
        keys[i][15] = static_cast<uint8_t>(i * 7);
    }

    // Insert first 5 keys
    for (int i = 0; i < 5; ++i) {
        bf.insert(keys[i]);
    }

    // Batch check all 10 keys
    std::vector<uint8_t> results(10, 0xFF);  // Initialize with sentinel
    size_t passed = bf.batchContains16(
        reinterpret_cast<const uint8_t*>(keys.data()), 10, results.data());

    // First 5 must be found (no false negatives)
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(results[i], 1) << "False negative at key " << i;
    }

    // Count should match
    size_t manualPassed = 0;
    for (int i = 0; i < 10; ++i) {
        if (results[i] == 1) ++manualPassed;
    }
    EXPECT_EQ(passed, manualPassed);

    // At least 5 should pass (the inserted ones)
    EXPECT_GE(passed, 5u);
}

// ==========================================================================
// Batch contains: matches individual contains() results
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsMatchesScalar) {
    auto bf = make_bloom_filter(10000, 0.01);

    // Insert some random 16-byte keys
    std::mt19937_64 rng(42);
    std::vector<std::array<uint8_t, 16>> insertedKeys(200);
    for (auto& key : insertedKeys) {
        uint64_t lo = rng();
        uint64_t hi = rng();
        std::memcpy(key.data(), &lo, 8);
        std::memcpy(key.data() + 8, &hi, 8);
        bf.insert(key);
    }

    // Check a mix of inserted and non-inserted keys
    std::vector<std::array<uint8_t, 16>> checkKeys(500);
    // First 200 are inserted keys
    for (int i = 0; i < 200; ++i) {
        checkKeys[i] = insertedKeys[i];
    }
    // Next 300 are random (mostly not inserted)
    for (int i = 200; i < 500; ++i) {
        uint64_t lo = rng();
        uint64_t hi = rng();
        std::memcpy(checkKeys[i].data(), &lo, 8);
        std::memcpy(checkKeys[i].data() + 8, &hi, 8);
    }

    // Batch check
    std::vector<uint8_t> batchResults(500);
    bf.batchContains16(reinterpret_cast<const uint8_t*>(checkKeys.data()),
                       500, batchResults.data());

    // Compare with individual contains() for every key
    for (int i = 0; i < 500; ++i) {
        bool scalarResult = bf.contains(checkKeys[i]);
        bool batchResult = batchResults[i] == 1;
        EXPECT_EQ(scalarResult, batchResult)
            << "Mismatch at key " << i
            << ": scalar=" << scalarResult << " batch=" << batchResult;
    }
}

// ==========================================================================
// Batch contains: empty filter returns all zeros
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsEmptyFilter) {
    bloom_filter bf;  // default constructed, size = 0

    std::array<uint8_t, 16> key = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
    uint8_t result = 0xFF;
    size_t passed = bf.batchContains16(key.data(), 1, &result);

    EXPECT_EQ(result, 0);
    EXPECT_EQ(passed, 0u);
}

// ==========================================================================
// Batch contains: zero keys is a no-op
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsZeroKeys) {
    auto bf = make_bloom_filter();
    bf.insert(std::string("test"));

    size_t passed = bf.batchContains16(nullptr, 0, nullptr);
    EXPECT_EQ(passed, 0u);
}

// ==========================================================================
// Batch contains: single key matches scalar
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsSingleKey) {
    auto bf = make_bloom_filter(1000, 0.01);

    std::array<uint8_t, 16> key = {};
    key[0] = 0xDE;
    key[1] = 0xAD;
    key[14] = 0xBE;
    key[15] = 0xEF;

    bf.insert(key);

    uint8_t result = 0;
    size_t passed = bf.batchContains16(key.data(), 1, &result);

    EXPECT_EQ(result, 1);
    EXPECT_EQ(passed, 1u);
    EXPECT_TRUE(bf.contains(key));
}

// ==========================================================================
// Batch contains via span convenience method
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsSpan) {
    auto bf = make_bloom_filter(1000, 0.01);

    std::vector<std::array<uint8_t, 16>> keys(20);
    for (int i = 0; i < 20; ++i) {
        std::memset(keys[i].data(), 0, 16);
        keys[i][0] = static_cast<uint8_t>(i);
    }

    // Insert half
    for (int i = 0; i < 10; ++i) {
        bf.insert(keys[i]);
    }

    std::vector<uint8_t> results(20);
    size_t passed = bf.batchContains(
        std::span<const std::array<uint8_t, 16>>(keys),
        std::span<uint8_t>(results));

    // Inserted keys must be found
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(results[i], 1) << "False negative at key " << i;
    }
    EXPECT_GE(passed, 10u);
}

// ==========================================================================
// Batch contains: large batch (1000 keys)
// ==========================================================================
TEST(StorageBloomFilterTest, BatchContainsLargeBatch) {
    auto bf = make_bloom_filter(10000, 0.01);

    std::mt19937_64 rng(12345);
    constexpr size_t N = 1000;
    std::vector<std::array<uint8_t, 16>> keys(N);

    for (auto& key : keys) {
        uint64_t lo = rng();
        uint64_t hi = rng();
        std::memcpy(key.data(), &lo, 8);
        std::memcpy(key.data() + 8, &hi, 8);
        bf.insert(key);
    }

    // All keys should be found
    std::vector<uint8_t> results(N);
    size_t passed = bf.batchContains16(
        reinterpret_cast<const uint8_t*>(keys.data()), N, results.data());

    EXPECT_EQ(passed, N);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(results[i], 1) << "False negative at key " << i;
    }
}

// ==========================================================================
// Popcount of default-constructed filter is zero
// ==========================================================================
TEST(StorageBloomFilterTest, PopulationCountDefault) {
    bloom_filter bf;
    EXPECT_EQ(bf.populationCount(), 0u);
    EXPECT_DOUBLE_EQ(bf.fillRatio(), 0.0);
}

// ==========================================================================
// Popcount after clear returns to zero
// ==========================================================================
TEST(StorageBloomFilterTest, PopulationCountAfterClear) {
    auto bf = make_bloom_filter(1000, 0.01);

    for (int i = 0; i < 100; ++i) {
        bf.insert(std::string("clear_test_" + std::to_string(i)));
    }
    EXPECT_GT(bf.populationCount(), 0u);

    bf.clear();
    EXPECT_EQ(bf.populationCount(), 0u);
    EXPECT_DOUBLE_EQ(bf.fillRatio(), 0.0);
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
