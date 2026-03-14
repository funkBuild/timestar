#include "../../../lib/index/native/bloom_filter.hpp"

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
