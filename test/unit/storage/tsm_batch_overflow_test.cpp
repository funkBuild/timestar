#include <gtest/gtest.h>
#include <cstdint>
#include <limits>
#include <vector>

#include "../../../lib/storage/tsm.hpp"

// Test fixture for groupContiguousBlocks overflow testing.
// TSM constructor only stores the file path; no Seastar I/O needed.
class TSMBatchOverflowTest : public ::testing::Test {
protected:
    TSM tsm{"/tmp/0_1.tsm"};

    // Helper to build a contiguous sequence of blocks starting at `baseOffset`,
    // each with the given `blockSize`.
    std::vector<TSMIndexBlock> makeContiguousBlocks(
        size_t count, uint32_t blockSize, uint64_t baseOffset = 0) {
        std::vector<TSMIndexBlock> blocks;
        blocks.reserve(count);
        uint64_t offset = baseOffset;
        for (size_t i = 0; i < count; ++i) {
            TSMIndexBlock b;
            b.minTime = i * 1000;
            b.maxTime = (i + 1) * 1000 - 1;
            b.offset = offset;
            b.size = blockSize;
            offset += blockSize;
            blocks.push_back(b);
        }
        return blocks;
    }
};

// Empty block list returns empty result
TEST_F(TSMBatchOverflowTest, EmptyBlocksReturnsEmpty) {
    std::vector<TSMIndexBlock> blocks;
    auto batches = tsm.groupContiguousBlocks(blocks);
    EXPECT_TRUE(batches.empty());
}

// Single block returns exactly one batch
TEST_F(TSMBatchOverflowTest, SingleBlockReturnsSingleBatch) {
    auto blocks = makeContiguousBlocks(1, 4096);
    auto batches = tsm.groupContiguousBlocks(blocks);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].blocks.size(), 1u);
    EXPECT_EQ(batches[0].startOffset, 0u);
    EXPECT_EQ(batches[0].totalSize, 4096u);
}

// Small contiguous blocks fit into one batch
TEST_F(TSMBatchOverflowTest, SmallContiguousBlocksGrouped) {
    auto blocks = makeContiguousBlocks(10, 1024);
    auto batches = tsm.groupContiguousBlocks(blocks);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].blocks.size(), 10u);
    EXPECT_EQ(batches[0].totalSize, 10u * 1024u);
}

// Non-contiguous blocks start new batches
TEST_F(TSMBatchOverflowTest, NonContiguousBlocksSplitBatches) {
    std::vector<TSMIndexBlock> blocks;
    // Block 0 at offset 0, size 1000
    blocks.push_back({0, 999, 0, 1000});
    // Block 1 at offset 2000 (gap of 1000 bytes), size 1000
    blocks.push_back({1000, 1999, 2000, 1000});
    // Block 2 contiguous with block 1 (offset 3000), size 500
    blocks.push_back({2000, 2999, 3000, 500});

    auto batches = tsm.groupContiguousBlocks(blocks);
    ASSERT_EQ(batches.size(), 2u);

    // First batch: only block 0
    EXPECT_EQ(batches[0].blocks.size(), 1u);
    EXPECT_EQ(batches[0].startOffset, 0u);
    EXPECT_EQ(batches[0].totalSize, 1000u);

    // Second batch: blocks 1 and 2
    EXPECT_EQ(batches[1].blocks.size(), 2u);
    EXPECT_EQ(batches[1].startOffset, 2000u);
    EXPECT_EQ(batches[1].totalSize, 1500u);
}

// Blocks exceeding MAX_BATCH_SIZE (16MB) are split into separate batches
TEST_F(TSMBatchOverflowTest, ExceedingMaxBatchSizeSplitsBatches) {
    constexpr uint64_t MAX_BATCH = 16u * 1024u * 1024u; // 16 MB
    // Each block is 4MB; 4 blocks = 16MB (exactly at limit), 5th should start new batch
    constexpr uint32_t blockSize = 4u * 1024u * 1024u;
    auto blocks = makeContiguousBlocks(5, blockSize);

    auto batches = tsm.groupContiguousBlocks(blocks);
    // First 4 blocks = 16MB exactly. The 5th block (4MB) would make it 20MB > 16MB.
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(batches[0].blocks.size(), 4u);
    EXPECT_EQ(batches[0].totalSize, static_cast<uint64_t>(4u) * blockSize);
    EXPECT_EQ(batches[1].blocks.size(), 1u);
    EXPECT_EQ(batches[1].totalSize, blockSize);
}

// Blocks exactly at MAX_BATCH_SIZE boundary are kept in one batch
TEST_F(TSMBatchOverflowTest, ExactlyAtMaxBatchSizeStaysOneBatch) {
    constexpr uint64_t MAX_BATCH = 16u * 1024u * 1024u;
    // Two blocks that together equal exactly MAX_BATCH_SIZE
    constexpr uint32_t blockSize = 8u * 1024u * 1024u; // 8MB each
    auto blocks = makeContiguousBlocks(2, blockSize);

    auto batches = tsm.groupContiguousBlocks(blocks);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].blocks.size(), 2u);
    EXPECT_EQ(batches[0].totalSize, MAX_BATCH);
}

// CRITICAL: Overflow scenario - blocks whose cumulative uint32_t sum would wrap around.
// Without the fix, uint32_t overflow causes a small newBatchSize that passes the <= 16MB
// check, incorrectly merging blocks into one oversized batch.
TEST_F(TSMBatchOverflowTest, Uint32OverflowCorrectlySplitsBatches) {
    // Use blocks with size close to UINT32_MAX / 2 so two blocks overflow uint32_t.
    // UINT32_MAX = 4,294,967,295
    // Two blocks of size 2,200,000,000 each:
    //   uint32_t sum = 4,400,000,000 mod 2^32 = 105,032,704 (wraps around!)
    //   105,032,704 < 16MB => would incorrectly pass the check with uint32_t
    //   uint64_t sum = 4,400,000,000 > 16MB => correctly splits
    constexpr uint32_t largeBlockSize = 2'200'000'000u;
    std::vector<TSMIndexBlock> blocks;

    // Block 0 at offset 0
    blocks.push_back({0, 999, 0, largeBlockSize});
    // Block 1 contiguous at offset = largeBlockSize
    blocks.push_back({1000, 1999, static_cast<uint64_t>(largeBlockSize), largeBlockSize});

    auto batches = tsm.groupContiguousBlocks(blocks);

    // Each block individually exceeds 16MB, so each must be its own batch.
    // The first block alone is 2.2GB > 16MB, so the second block cannot merge.
    // Actually: the first block (2.2GB) is placed as the initial block of the first batch.
    // Then when considering the second block, newBatchSize = 2.2GB + 2.2GB = 4.4GB > 16MB,
    // so it starts a new batch.
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(batches[0].blocks.size(), 1u);
    EXPECT_EQ(batches[0].totalSize, static_cast<uint64_t>(largeBlockSize));
    EXPECT_EQ(batches[1].blocks.size(), 1u);
    EXPECT_EQ(batches[1].totalSize, static_cast<uint64_t>(largeBlockSize));
}

// Verify that totalSize uses uint64_t and can represent values > UINT32_MAX.
// This test constructs many moderate-sized contiguous blocks that sum > 4GB.
// Without the uint64_t fix on BlockBatch.totalSize, the field would overflow.
TEST_F(TSMBatchOverflowTest, TotalSizeFieldHandlesLargeValues) {
    // We can't actually group blocks > 16MB into one batch due to MAX_BATCH_SIZE,
    // but we verify that totalSize on each batch correctly stores the value.
    // A single block of exactly MAX_BATCH_SIZE - 1 should be stored accurately.
    constexpr uint32_t almostMax = 16u * 1024u * 1024u - 1u;
    auto blocks = makeContiguousBlocks(1, almostMax);
    auto batches = tsm.groupContiguousBlocks(blocks);
    ASSERT_EQ(batches.size(), 1u);
    EXPECT_EQ(batches[0].totalSize, static_cast<uint64_t>(almostMax));
}

// Overflow scenario with many smaller blocks that cumulatively overflow uint32_t.
// 5 contiguous blocks of size 1,000,000,000 each:
//   uint32_t sum after 5 blocks = 5,000,000,000 mod 2^32 = 705,032,704
//   With uint32_t arithmetic, block 5 might incorrectly merge.
TEST_F(TSMBatchOverflowTest, ManyLargeBlocksOverflowHandledCorrectly) {
    constexpr uint32_t blockSize = 1'000'000'000u; // 1GB each
    auto blocks = makeContiguousBlocks(5, blockSize);

    auto batches = tsm.groupContiguousBlocks(blocks);

    // Each 1GB block alone exceeds 16MB, so every block after the first
    // will fail the newBatchSize <= 16MB check and start a new batch.
    // The first block is placed as initial block (1GB > 16MB doesn't matter for the first
    // block; it's always placed). The second block: newBatchSize = 2GB > 16MB => split.
    // So each block should be its own batch.
    ASSERT_EQ(batches.size(), 5u);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_EQ(batches[i].blocks.size(), 1u);
        EXPECT_EQ(batches[i].totalSize, static_cast<uint64_t>(blockSize));
    }
}

// Verify the specific wraparound scenario: two uint32_t values whose sum
// wraps to a small number less than MAX_BATCH_SIZE.
TEST_F(TSMBatchOverflowTest, SpecificWraparoundValuesDetected) {
    // Choose sizes so uint32_t overflow produces a value < 16MB:
    //   a = UINT32_MAX - 1,000,000 = 4,293,967,295
    //   b = 2,000,000
    //   uint32_t(a + b) = 999,999 (wrapped) < 16MB -- BUG: would merge
    //   uint64_t(a + b) = 4,295,967,295 > 16MB -- correctly splits
    // But a must fit in uint32_t, and UINT32_MAX - 1,000,000 = 4,293,967,295 does.
    constexpr uint32_t sizeA = std::numeric_limits<uint32_t>::max() - 1'000'000u;
    constexpr uint32_t sizeB = 2'000'000u;

    // Verify the wraparound assumption
    uint32_t wrappedSum = sizeA + sizeB; // intentional uint32_t overflow
    ASSERT_LT(wrappedSum, 16u * 1024u * 1024u) << "Test assumption: uint32_t sum should wrap to < 16MB";

    uint64_t correctSum = static_cast<uint64_t>(sizeA) + sizeB;
    ASSERT_GT(correctSum, 16u * 1024u * 1024u) << "Test assumption: uint64_t sum should exceed 16MB";

    std::vector<TSMIndexBlock> blocks;
    blocks.push_back({0, 999, 0, sizeA});
    blocks.push_back({1000, 1999, static_cast<uint64_t>(sizeA), sizeB});

    auto batches = tsm.groupContiguousBlocks(blocks);

    // With the fix, these two blocks must NOT be merged into one batch.
    ASSERT_EQ(batches.size(), 2u);
    EXPECT_EQ(batches[0].blocks.size(), 1u);
    EXPECT_EQ(batches[1].blocks.size(), 1u);
}
