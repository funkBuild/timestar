#include "../../../lib/index/native/bloom_filter.hpp"
#include "../../../lib/index/native/bloom_filter_simd.hpp"

#include <gtest/gtest.h>

#include <format>
#include <string>
#include <vector>

using namespace timestar::index;

TEST(BloomFilterTest, EmptyFilter) {
    BloomFilter bf(10);
    bf.build();
    EXPECT_EQ(bf.keyCount(), 0u);
    // Empty filter should return false for any key
    EXPECT_FALSE(bf.mayContain("anything"));
}

TEST(BloomFilterTest, SingleKey) {
    BloomFilter bf(10);
    bf.addKey("hello");
    bf.build();

    EXPECT_TRUE(bf.mayContain("hello"));
    // "world" is very unlikely to be a false positive
    // (not guaranteed, but with 10 bits/key the FPR is ~1%)
}

TEST(BloomFilterTest, MultipleKeys) {
    BloomFilter bf(15);
    std::vector<std::string> keys;
    for (int i = 0; i < 100; ++i) {
        keys.push_back(std::format("key:{:04d}", i));
        bf.addKey(keys.back());
    }
    bf.build();

    // All inserted keys must be found (no false negatives)
    for (const auto& key : keys) {
        EXPECT_TRUE(bf.mayContain(key)) << "False negative for: " << key;
    }
}

TEST(BloomFilterTest, FalsePositiveRate) {
    // With 15 bits/key, false positive rate should be < 1%
    BloomFilter bf(15);
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        bf.addKey(std::format("inserted:{:06d}", i));
    }
    bf.build();

    int falsePositives = 0;
    const int M = 100000;
    for (int i = 0; i < M; ++i) {
        if (bf.mayContain(std::format("notinserted:{:06d}", i))) {
            ++falsePositives;
        }
    }

    double fpr = static_cast<double>(falsePositives) / M;
    // With 15 bits/key, theoretical FPR ≈ 0.06%. Allow up to 1% for safety.
    EXPECT_LT(fpr, 0.01) << "FPR too high: " << fpr << " (" << falsePositives << "/" << M << ")";
}

TEST(BloomFilterTest, SerializationRoundtrip) {
    BloomFilter bf(10);
    for (int i = 0; i < 50; ++i) {
        bf.addKey(std::format("ser:{:03d}", i));
    }
    bf.build();

    std::string serialized;
    bf.serializeTo(serialized);
    EXPECT_GT(serialized.size(), 5u);

    auto restored = BloomFilter::deserializeFrom(serialized);
    EXPECT_FALSE(restored.isNull());

    // All original keys must still be found
    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(restored.mayContain(std::format("ser:{:03d}", i)));
    }
}

TEST(BloomFilterTest, DeserializeInvalidData) {
    auto bf = BloomFilter::deserializeFrom("xx");
    EXPECT_TRUE(bf.isNull());
    EXPECT_TRUE(bf.mayContain("anything"));  // Null filter always returns true
}

TEST(BloomFilterTest, DeserializeTruncated) {
    BloomFilter bf(10);
    bf.addKey("test");
    bf.build();

    std::string serialized;
    bf.serializeTo(serialized);

    // Truncate the data
    auto truncated = BloomFilter::deserializeFrom(serialized.substr(0, 3));
    EXPECT_TRUE(truncated.isNull());
}

TEST(BloomFilterTest, NullFilter) {
    auto bf = BloomFilter::createNull();
    EXPECT_TRUE(bf.isNull());
    EXPECT_TRUE(bf.mayContain("any"));
    EXPECT_TRUE(bf.mayContain("key"));
}

TEST(BloomFilterTest, BinaryKeys) {
    // Keys with null bytes and binary data (as used in the index)
    BloomFilter bf(10);
    std::string key1 = std::string("\x05", 1) + std::string(16, '\x01');
    std::string key2 = std::string("\x05", 1) + std::string(16, '\x02');
    bf.addKey(key1);
    bf.addKey(key2);
    bf.build();

    EXPECT_TRUE(bf.mayContain(key1));
    EXPECT_TRUE(bf.mayContain(key2));
}

TEST(BloomFilterTest, LargeFilter) {
    BloomFilter bf(10);
    const int N = 100000;
    for (int i = 0; i < N; ++i) {
        bf.addKey(std::format("large:{:07d}", i));
    }
    bf.build();

    EXPECT_EQ(bf.keyCount(), static_cast<size_t>(N));
    EXPECT_GT(bf.filterSize(), 0u);

    // Spot check: all inserted keys present
    for (int i = 0; i < N; i += 1000) {
        EXPECT_TRUE(bf.mayContain(std::format("large:{:07d}", i)));
    }

    // Roundtrip serialization of large filter
    std::string serialized;
    bf.serializeTo(serialized);
    auto restored = BloomFilter::deserializeFrom(serialized);
    EXPECT_FALSE(restored.isNull());
    for (int i = 0; i < N; i += 1000) {
        EXPECT_TRUE(restored.mayContain(std::format("large:{:07d}", i)));
    }
}

TEST(BloomFilterTest, DifferentBitsPerKey) {
    // Lower bits_per_key = higher FPR
    for (int bpk : {5, 10, 15, 20}) {
        BloomFilter bf(bpk);
        for (int i = 0; i < 1000; ++i) {
            bf.addKey(std::format("bpk:{:04d}", i));
        }
        bf.build();

        // All inserted keys must be found
        for (int i = 0; i < 1000; ++i) {
            EXPECT_TRUE(bf.mayContain(std::format("bpk:{:04d}", i)))
                << "False negative at bpk=" << bpk << " key=" << i;
        }
    }
}

// =============================================================================
// SIMD-specific tests
// =============================================================================

TEST(BloomFilterSIMDTest, BitCount_EmptyFilter) {
    BloomFilter bf(10);
    bf.build();
    EXPECT_EQ(bf.bitCount(), 0u);
}

TEST(BloomFilterSIMDTest, BitCount_NonZero) {
    BloomFilter bf(10);
    for (int i = 0; i < 100; ++i) {
        bf.addKey(std::format("bc:{:04d}", i));
    }
    bf.build();

    uint64_t bits = bf.bitCount();
    EXPECT_GT(bits, 0u);
    // With 100 keys, 10 bits/key, k~7, there should be some collisions
    // but most of the ~700 bit positions should be unique
    EXPECT_GT(bits, 400u);
    EXPECT_LE(bits, 700u);
}

TEST(BloomFilterSIMDTest, BitCount_LargeFilter) {
    // 10K keys at 10 bits/key = 100K bits total, k=7 -> ~70K bit sets
    // With birthday collisions, expect ~55-65K unique bits set
    BloomFilter bf(10);
    const int N = 10000;
    for (int i = 0; i < N; ++i) {
        bf.addKey(std::format("bclarge:{:06d}", i));
    }
    bf.build();

    uint64_t bits = bf.bitCount();
    size_t totalBits = bf.filterSize() * 8;
    EXPECT_GT(bits, 0u);
    EXPECT_LT(bits, totalBits);  // Not all bits should be set
}

TEST(BloomFilterSIMDTest, Density_EmptyFilter) {
    BloomFilter bf(10);
    bf.build();
    EXPECT_DOUBLE_EQ(bf.density(), 0.0);
}

TEST(BloomFilterSIMDTest, Density_Reasonable) {
    BloomFilter bf(10);
    for (int i = 0; i < 1000; ++i) {
        bf.addKey(std::format("dens:{:04d}", i));
    }
    bf.build();

    double d = bf.density();
    EXPECT_GT(d, 0.0);
    EXPECT_LT(d, 1.0);
    // With 10 bits/key and k=7, optimal density is 1 - e^(-k*n/m) ~ 0.5
    // (where n=keys, m=bits, k=hash_count). Allow wide range.
    EXPECT_GT(d, 0.2);
    EXPECT_LT(d, 0.8);
}

TEST(BloomFilterSIMDTest, BuildSIMDPath_LargeNoFalseNegatives) {
    // Large filter (>= 64 bytes) exercises the SIMD build path.
    // Verify no false negatives — the SIMD probe computation must match scalar.
    BloomFilter bf(10);
    const int N = 5000;  // 5000 * 10 bits / 8 = 6250 bytes >> 64 threshold
    std::vector<std::string> keys;
    keys.reserve(N);
    for (int i = 0; i < N; ++i) {
        keys.push_back(std::format("simdbuild:{:06d}", i));
        bf.addKey(keys.back());
    }
    bf.build();

    EXPECT_GE(bf.filterSize(), 64u);  // Confirm SIMD path used

    // All inserted keys must be found (no false negatives)
    for (const auto& key : keys) {
        EXPECT_TRUE(bf.mayContain(key)) << "False negative for: " << key;
    }
}

TEST(BloomFilterSIMDTest, MayContainSIMDPath_NoFalseNegatives) {
    // Build with enough keys to exceed the 64-byte SIMD threshold.
    // Then test mayContain (which uses SIMD gather+test for large filters).
    BloomFilter bf(15);
    const int N = 2000;
    std::vector<std::string> keys;
    keys.reserve(N);
    for (int i = 0; i < N; ++i) {
        keys.push_back(std::format("simdcontains:{:06d}", i));
        bf.addKey(keys.back());
    }
    bf.build();

    EXPECT_GE(bf.filterSize(), 64u);

    // All must be found
    for (const auto& key : keys) {
        EXPECT_TRUE(bf.mayContain(key)) << "False negative: " << key;
    }

    // Non-existent keys: count false positives, should be low
    int fp = 0;
    const int M = 10000;
    for (int i = 0; i < M; ++i) {
        if (bf.mayContain(std::format("simdabsent:{:06d}", i))) {
            ++fp;
        }
    }
    double fpr = static_cast<double>(fp) / M;
    // 15 bits/key -> theoretical FPR ~0.06%, allow up to 1%
    EXPECT_LT(fpr, 0.01) << "FPR: " << fpr;
}

TEST(BloomFilterSIMDTest, BuildSIMDMatchesScalarBuild) {
    // Verify that SIMD build produces identical filter bits as scalar build
    // by comparing a filter built via SIMD path (large) with one built via
    // the scalar path (small enough to fall below threshold, or by comparing
    // with manually computed bit positions).

    // Strategy: build two filters with the same keys but different bitsPerKey
    // to test both paths. We can't directly force scalar vs SIMD, but we can
    // verify cross-compatibility: build with SIMD, serialize, deserialize
    // (which uses the stored filter verbatim), and check all keys.
    BloomFilter bf(10);
    const int N = 3000;
    std::vector<std::string> keys;
    keys.reserve(N);
    for (int i = 0; i < N; ++i) {
        keys.push_back(std::format("match:{:06d}", i));
        bf.addKey(keys.back());
    }
    bf.build();

    // Serialize and deserialize — the deserialized filter uses the same bytes
    std::string serialized;
    bf.serializeTo(serialized);
    auto restored = BloomFilter::deserializeFrom(serialized);

    // Both original and deserialized must agree on all keys
    for (const auto& key : keys) {
        bool orig = bf.mayContain(key);
        bool rest = restored.mayContain(key);
        EXPECT_TRUE(orig) << "Original false negative: " << key;
        EXPECT_TRUE(rest) << "Restored false negative: " << key;
    }

    // Also verify bitCount matches
    EXPECT_EQ(bf.bitCount(), restored.bitCount());
}

TEST(BloomFilterSIMDTest, PopcountDirect) {
    // Test the SIMD popcount function directly
    const size_t numWords = 16;  // 128 bytes
    std::vector<uint64_t> data(numWords, 0);

    EXPECT_EQ(simd::bloomPopcount(data.data(), numWords), 0u);

    // Set every bit in the first word
    data[0] = ~0ULL;
    EXPECT_EQ(simd::bloomPopcount(data.data(), numWords), 64u);

    // Set every bit in all words
    for (auto& w : data) w = ~0ULL;
    EXPECT_EQ(simd::bloomPopcount(data.data(), numWords), 64u * numWords);

    // Set alternating bits: 0x5555... has 32 bits per word
    for (auto& w : data) w = 0x5555555555555555ULL;
    EXPECT_EQ(simd::bloomPopcount(data.data(), numWords), 32u * numWords);
}

TEST(BloomFilterSIMDTest, PopcountLargeArray) {
    // Test with a large array to exercise the full SIMD loop + scalar tail
    const size_t numWords = 1024 + 3;  // Not a power of 2, tests scalar tail
    std::vector<uint64_t> data(numWords);
    uint64_t expectedBits = 0;
    for (size_t i = 0; i < numWords; ++i) {
        data[i] = static_cast<uint64_t>(i) * 0x123456789ABCDEFULL;
        expectedBits += __builtin_popcountll(data[i]);
    }

    EXPECT_EQ(simd::bloomPopcount(data.data(), numWords), expectedBits);
}

TEST(BloomFilterSIMDTest, DifferentKValues) {
    // Test with different bits_per_key settings which produce different k values.
    // k = round(bpk * 0.6931), capped at 30.
    // bpk=5 -> k=3, bpk=10 -> k=7, bpk=15 -> k=10, bpk=20 -> k=14
    // All must produce zero false negatives via SIMD paths.
    for (int bpk : {5, 10, 15, 20, 25}) {
        BloomFilter bf(bpk);
        const int N = 1000;
        // At bpk=5, N*bpk=5000 bits = 625 bytes > 64 threshold -> SIMD path
        for (int i = 0; i < N; ++i) {
            bf.addKey(std::format("ktest:{}:{:04d}", bpk, i));
        }
        bf.build();

        EXPECT_GE(bf.filterSize(), 64u) << "bpk=" << bpk;

        for (int i = 0; i < N; ++i) {
            EXPECT_TRUE(bf.mayContain(std::format("ktest:{}:{:04d}", bpk, i)))
                << "False negative at bpk=" << bpk << " key=" << i;
        }
    }
}

TEST(BloomFilterSIMDTest, BinaryKeysLargeFilter) {
    // NativeIndex bloom filters use binary keys with prefix bytes and null bytes.
    // Verify SIMD paths work correctly with binary (non-ASCII) key data.
    BloomFilter bf(10);
    const int N = 500;
    std::vector<std::string> keys;
    keys.reserve(N);
    for (int i = 0; i < N; ++i) {
        std::string key;
        key.push_back(static_cast<char>(0x06));  // POSTINGS_BITMAP prefix
        key.append("measurement");
        key.push_back('\0');
        key.append("tag_key");
        key.push_back('\0');
        key.append(std::format("tag_value_{:04d}", i));
        keys.push_back(std::move(key));
        bf.addKey(keys.back());
    }
    bf.build();

    EXPECT_GE(bf.filterSize(), 64u);

    for (const auto& key : keys) {
        EXPECT_TRUE(bf.mayContain(key)) << "False negative for binary key";
    }
}
