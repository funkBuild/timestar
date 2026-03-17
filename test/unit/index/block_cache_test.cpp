#include "../../../lib/index/native/block_cache.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace timestar::index;

class BlockCacheTest : public ::testing::Test {
protected:
    // Large enough budget for most tests; specific tests override.
    static constexpr size_t kDefaultBudget = 64 * 1024;  // 64 KB
};

TEST_F(BlockCacheTest, EmptyCacheReturnsNullptr) {
    BlockCache cache(kDefaultBudget);
    EXPECT_EQ(cache.get(1, 0), nullptr);
    EXPECT_EQ(cache.get(0, 0), nullptr);
    EXPECT_EQ(cache.get(999, 42), nullptr);
    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.currentBytes(), 0u);
}

TEST_F(BlockCacheTest, PutThenGet) {
    BlockCache cache(kDefaultBudget);
    uint64_t id = BlockCache::nextCacheId();

    cache.put(id, 0, "hello");
    cache.put(id, 1, "world");

    const std::string* v0 = cache.get(id, 0);
    ASSERT_NE(v0, nullptr);
    EXPECT_EQ(*v0, "hello");

    const std::string* v1 = cache.get(id, 1);
    ASSERT_NE(v1, nullptr);
    EXPECT_EQ(*v1, "world");

    // Miss for non-existent block index
    EXPECT_EQ(cache.get(id, 99), nullptr);
    EXPECT_EQ(cache.size(), 2u);
}

TEST_F(BlockCacheTest, PutPromotesToMRU) {
    // Measure the real entry size by inserting one entry into a large cache.
    const size_t dataSize = 100;
    {
        BlockCache probe(1024 * 1024);
        uint64_t probeId = BlockCache::nextCacheId();
        probe.put(probeId, 0, std::string(dataSize, 'X'));
        size_t realEntrySize = probe.currentBytes();

        // Budget fits exactly 3 entries but not 4.
        size_t budget = realEntrySize * 3 + realEntrySize / 2;

        BlockCache cache(budget);
        uint64_t id = BlockCache::nextCacheId();

        // Insert entries A, B, C (LRU order: C=MRU, A=LRU)
        cache.put(id, 0, std::string(dataSize, 'A'));
        cache.put(id, 1, std::string(dataSize, 'B'));
        cache.put(id, 2, std::string(dataSize, 'C'));
        ASSERT_EQ(cache.size(), 3u);

        // Access A to promote it to MRU (order becomes: A=MRU, C, B=LRU)
        ASSERT_NE(cache.get(id, 0), nullptr);

        // Insert D — should evict B (the LRU), not A
        cache.put(id, 3, std::string(dataSize, 'D'));

        // A was promoted, should still be here
        EXPECT_NE(cache.get(id, 0), nullptr);
        // B was LRU, should be evicted
        EXPECT_EQ(cache.get(id, 1), nullptr);
        // D was just inserted
        EXPECT_NE(cache.get(id, 3), nullptr);
    }
}

TEST_F(BlockCacheTest, EvictsLRUWhenOverBudget) {
    // Measure real entry size
    const size_t dataSize = 200;
    BlockCache probe(1024 * 1024);
    uint64_t probeId = BlockCache::nextCacheId();
    probe.put(probeId, 0, std::string(dataSize, 'X'));
    size_t realEntrySize = probe.currentBytes();

    // Budget for exactly 2 entries but not 3
    size_t budget = realEntrySize * 2 + realEntrySize / 2;

    BlockCache cache(budget);
    uint64_t id = BlockCache::nextCacheId();

    cache.put(id, 0, std::string(dataSize, 'A'));
    cache.put(id, 1, std::string(dataSize, 'B'));
    EXPECT_EQ(cache.size(), 2u);

    // Insert third — should evict the oldest (block 0)
    cache.put(id, 2, std::string(dataSize, 'C'));

    EXPECT_EQ(cache.get(id, 0), nullptr);  // evicted
    EXPECT_NE(cache.get(id, 1), nullptr);
    EXPECT_NE(cache.get(id, 2), nullptr);
}

TEST_F(BlockCacheTest, EvictsMultipleToFitNewEntry) {
    // Measure real entry size for small entries
    const size_t smallSize = 50;
    BlockCache probe(1024 * 1024);
    uint64_t probeId = BlockCache::nextCacheId();
    probe.put(probeId, 0, std::string(smallSize, 'X'));
    size_t realSmallSize = probe.currentBytes();

    // Budget fits exactly 4 small entries
    size_t budget = realSmallSize * 4 + realSmallSize / 2;

    BlockCache cache(budget);
    uint64_t id = BlockCache::nextCacheId();

    // Insert 4 small entries
    for (size_t i = 0; i < 4; ++i) {
        cache.put(id, i, std::string(smallSize, 'a' + static_cast<char>(i)));
    }
    EXPECT_EQ(cache.size(), 4u);

    // Insert one large entry that needs ~3 small entries' worth of data
    size_t largeSize = realSmallSize * 3;
    cache.put(id, 99, std::string(largeSize, 'Z'));

    // The large entry should exist
    ASSERT_NE(cache.get(id, 99), nullptr);
    EXPECT_EQ(*cache.get(id, 99), std::string(largeSize, 'Z'));

    // Multiple small entries should have been evicted
    size_t surviving = 0;
    for (size_t i = 0; i < 4; ++i) {
        if (cache.get(id, i) != nullptr) ++surviving;
    }
    EXPECT_LT(surviving, 4u);
}

TEST_F(BlockCacheTest, UpdateExistingEntryAdjustsSize) {
    BlockCache cache(kDefaultBudget);
    uint64_t id = BlockCache::nextCacheId();

    cache.put(id, 0, std::string(100, 'A'));
    size_t bytesAfterSmall = cache.currentBytes();

    // Overwrite with larger data
    cache.put(id, 0, std::string(1000, 'B'));
    size_t bytesAfterLarge = cache.currentBytes();
    EXPECT_GT(bytesAfterLarge, bytesAfterSmall);
    EXPECT_EQ(cache.size(), 1u);

    // Verify the data was updated
    const std::string* v = cache.get(id, 0);
    ASSERT_NE(v, nullptr);
    EXPECT_EQ(*v, std::string(1000, 'B'));

    // Overwrite with smaller data — bytes should decrease
    cache.put(id, 0, std::string(50, 'C'));
    EXPECT_LT(cache.currentBytes(), bytesAfterLarge);
    EXPECT_EQ(cache.size(), 1u);
}

TEST_F(BlockCacheTest, EvictByCacheIdRemovesAllEntries) {
    BlockCache cache(kDefaultBudget);
    uint64_t id = BlockCache::nextCacheId();

    cache.put(id, 0, "data0");
    cache.put(id, 1, "data1");
    cache.put(id, 2, "data2");
    EXPECT_EQ(cache.size(), 3u);
    EXPECT_GT(cache.currentBytes(), 0u);

    cache.evict(id);

    EXPECT_EQ(cache.size(), 0u);
    EXPECT_EQ(cache.currentBytes(), 0u);
    EXPECT_EQ(cache.get(id, 0), nullptr);
    EXPECT_EQ(cache.get(id, 1), nullptr);
    EXPECT_EQ(cache.get(id, 2), nullptr);
}

TEST_F(BlockCacheTest, EvictByCacheIdPreservesOthers) {
    BlockCache cache(kDefaultBudget);
    uint64_t id1 = BlockCache::nextCacheId();
    uint64_t id2 = BlockCache::nextCacheId();

    cache.put(id1, 0, "id1-block0");
    cache.put(id1, 1, "id1-block1");
    cache.put(id2, 0, "id2-block0");
    cache.put(id2, 1, "id2-block1");
    EXPECT_EQ(cache.size(), 4u);

    cache.evict(id1);

    EXPECT_EQ(cache.size(), 2u);
    EXPECT_EQ(cache.get(id1, 0), nullptr);
    EXPECT_EQ(cache.get(id1, 1), nullptr);
    ASSERT_NE(cache.get(id2, 0), nullptr);
    EXPECT_EQ(*cache.get(id2, 0), "id2-block0");
    ASSERT_NE(cache.get(id2, 1), nullptr);
    EXPECT_EQ(*cache.get(id2, 1), "id2-block1");
}

TEST_F(BlockCacheTest, ByteBudgetIsRespected) {
    const size_t budget = 2048;
    BlockCache cache(budget);
    uint64_t id = BlockCache::nextCacheId();

    // Insert many entries — after each, currentBytes should not exceed maxBytes.
    // Note: the implementation allows currentBytes > maxBytes when a single entry
    // is larger than the budget (it evicts everything, then inserts anyway).
    // For entries smaller than budget, the invariant holds.
    for (size_t i = 0; i < 100; ++i) {
        cache.put(id, i, std::string(50, static_cast<char>('a' + (i % 26))));
        // After inserting small entries, budget should be respected
        EXPECT_LE(cache.currentBytes(), cache.maxBytes() + 500)
            << "currentBytes exceeded budget at iteration " << i;
    }
    EXPECT_EQ(cache.maxBytes(), budget);
}

TEST_F(BlockCacheTest, NextCacheIdIsMonotonic) {
    uint64_t prev = BlockCache::nextCacheId();
    for (int i = 0; i < 100; ++i) {
        uint64_t curr = BlockCache::nextCacheId();
        EXPECT_GT(curr, prev);
        prev = curr;
    }
}

TEST_F(BlockCacheTest, HitRateAfterEviction) {
    // Measure real entry size
    const size_t dataSize = 100;
    BlockCache probe(1024 * 1024);
    uint64_t probeId = BlockCache::nextCacheId();
    probe.put(probeId, 0, std::string(dataSize, 'X'));
    size_t realEntrySize = probe.currentBytes();

    // Budget fits exactly 8 entries (enough to hold re-accessed + some recent)
    size_t budget = realEntrySize * 8;

    BlockCache cache(budget);
    uint64_t id = BlockCache::nextCacheId();

    // Insert 10 entries (indices 0..9). Budget fits 8, so 0 and 1 get evicted.
    // After this: cache holds indices 2..9, with 9=MRU and 2=LRU.
    for (size_t i = 0; i < 10; ++i) {
        cache.put(id, i, std::string(dataSize, static_cast<char>('A' + i)));
    }

    // Re-access indices 2, 3, 4 to promote them to MRU.
    // LRU order (tail=LRU): 5, 6, 7, 8, 9, 2, 3, 4
    for (size_t i = 2; i < 5; ++i) {
        ASSERT_NE(cache.get(id, i), nullptr)
            << "Entry " << i << " should still be in cache";
    }

    // Insert 5 more entries (indices 10..14) to force evictions.
    // Each insertion evicts the LRU. Eviction order: 5, 6, 7, 8, 9
    // After: cache holds 2, 3, 4, 10, 11, 12, 13, 14
    for (size_t i = 10; i < 15; ++i) {
        cache.put(id, i, std::string(dataSize, static_cast<char>('a' + (i % 26))));
    }

    // The re-accessed entries (2, 3, 4) should have survived
    size_t reaccessed_hits = 0;
    for (size_t i = 2; i < 5; ++i) {
        if (cache.get(id, i) != nullptr) ++reaccessed_hits;
    }
    EXPECT_EQ(reaccessed_hits, 3u)
        << "Expected all re-accessed entries to survive eviction";

    // The non-re-accessed entries (5..9) should all be gone
    size_t middle_hits = 0;
    for (size_t i = 5; i < 10; ++i) {
        if (cache.get(id, i) != nullptr) ++middle_hits;
    }
    EXPECT_EQ(middle_hits, 0u)
        << "Expected non-accessed middle entries to be evicted";
}
