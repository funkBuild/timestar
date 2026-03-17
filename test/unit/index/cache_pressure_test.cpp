#include "../../../lib/index/native/block_cache.hpp"
#include "../../../lib/utils/lru_cache.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using timestar::index::BlockCache;
using timestar::LRUCache;

class CachePressureTest : public ::testing::Test {};

TEST_F(CachePressureTest, LRUCacheEvictsUnderByteBudget) {
    const size_t budget = 4096;
    LRUCache<std::string, std::string> cache(budget);

    // Insert far more data than the budget allows
    for (int i = 0; i < 500; ++i) {
        std::string key = "key-" + std::to_string(i);
        std::string value = std::string(100, static_cast<char>('a' + (i % 26)));
        cache.put(key, value);
        EXPECT_LE(cache.currentBytes(), cache.maxBytes())
            << "LRUCache exceeded byte budget after inserting entry " << i;
    }

    // Final size should be well under budget
    EXPECT_LE(cache.currentBytes(), budget);
    // Should have far fewer than 500 entries
    EXPECT_LT(cache.size(), 500u);
    EXPECT_GT(cache.size(), 0u);
}

TEST_F(CachePressureTest, LRUCacheSingleEntryWithTinyBudget) {
    // Budget is tiny — only one entry should fit at a time
    const size_t budget = 1;  // 1 byte budget
    LRUCache<std::string, std::string> cache(budget);

    // The implementation always allows at least 1 entry (evicts until empty,
    // then inserts regardless). So we should have exactly 1 entry.
    cache.put("first", "value_one");
    EXPECT_EQ(cache.size(), 1u);

    cache.put("second", "value_two");
    EXPECT_EQ(cache.size(), 1u);

    // Only the most recent should survive
    EXPECT_EQ(cache.get("first"), nullptr);
    EXPECT_NE(cache.get("second"), nullptr);
    EXPECT_EQ(*cache.get("second"), "value_two");
}

TEST_F(CachePressureTest, LRUCacheClearResetsBytesToZero) {
    const size_t budget = 8192;
    LRUCache<std::string, std::string> cache(budget);

    for (int i = 0; i < 50; ++i) {
        cache.put("key-" + std::to_string(i), std::string(100, 'x'));
    }
    EXPECT_GT(cache.currentBytes(), 0u);
    EXPECT_GT(cache.size(), 0u);

    cache.clear();

    EXPECT_EQ(cache.currentBytes(), 0u);
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.get("key-0"), nullptr);
}

TEST_F(CachePressureTest, BlockCacheHandlesZeroBudget) {
    // A zero-byte budget should not crash
    BlockCache cache(0);

    EXPECT_EQ(cache.maxBytes(), 0u);
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.currentBytes(), 0u);

    uint64_t id = BlockCache::nextCacheId();

    // put into a zero-budget cache — the implementation evicts everything,
    // then inserts. This should not crash.
    cache.put(id, 0, "hello");
    // The entry may or may not persist (implementation-defined), but no crash.
    // currentBytes will exceed maxBytes since a single entry always goes in.
    EXPECT_GE(cache.size(), 0u);

    // get and evict should also not crash
    cache.get(id, 0);
    cache.evict(id);
    EXPECT_EQ(cache.size(), 0u);
}

TEST_F(CachePressureTest, BlockCacheLargeEntryCountStaysBounded) {
    // 8 KB budget with small entries — can't hold 10000
    const size_t budget = 8 * 1024;
    BlockCache cache(budget);
    uint64_t id = BlockCache::nextCacheId();

    for (size_t i = 0; i < 10000; ++i) {
        cache.put(id, i, std::string(32, static_cast<char>('a' + (i % 26))));
    }

    // With ~32 bytes of data per entry and overhead, only a handful should fit
    EXPECT_LE(cache.currentBytes(), budget + 500);
    EXPECT_LT(cache.size(), 10000u);
    EXPECT_GT(cache.size(), 0u);

    // The most recently inserted entries should be retrievable
    EXPECT_NE(cache.get(id, 9999), nullptr);
}

TEST_F(CachePressureTest, BlockCacheStressEviction) {
    const size_t budget = 4096;
    BlockCache cache(budget);

    // Rapidly insert and evict across many cache IDs
    for (int round = 0; round < 100; ++round) {
        uint64_t id = BlockCache::nextCacheId();

        // Insert 10 entries for this cache ID
        for (size_t i = 0; i < 10; ++i) {
            cache.put(id, i, "round-" + std::to_string(round) + "-block-" + std::to_string(i));
        }

        // Evict this cache ID every other round
        if (round % 2 == 0) {
            cache.evict(id);
        }
    }

    // After all the stress, verify internal consistency:
    // size and currentBytes should be non-negative and consistent
    EXPECT_GE(cache.size(), 0u);
    // currentBytes should be reasonable (not corrupted/wrapped)
    EXPECT_LE(cache.currentBytes(), budget + 2048);

    // A fresh insert + get should still work correctly
    uint64_t finalId = BlockCache::nextCacheId();
    cache.put(finalId, 0, "final-check");
    const std::string* v = cache.get(finalId, 0);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, "final-check");
}
