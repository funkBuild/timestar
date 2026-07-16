#include "../../../lib/index/native/index_wal.hpp"
#include "../../../lib/index/native/memtable.hpp"
#include "../../../lib/index/native/write_batch.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>

using namespace timestar::index;

class IndexWALTest : public ::testing::Test {
public:
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "timestar_wal_test";
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

SEASTAR_TEST_F(IndexWALTest, OpenAndClose) {
    auto wal = co_await IndexWAL::open(self->dir_);
    EXPECT_EQ(wal.sequenceNumber(), 0u);
    co_await wal.close();
}

SEASTAR_TEST_F(IndexWALTest, AppendAndReplay) {
    {
        auto wal = co_await IndexWAL::open(self->dir_);

        IndexWriteBatch batch1;
        batch1.put("key1", "val1");
        batch1.put("key2", "val2");
        co_await wal.append(batch1);

        IndexWriteBatch batch2;
        batch2.put("key3", "val3");
        batch2.remove("key1");
        co_await wal.append(batch2);

        EXPECT_EQ(wal.sequenceNumber(), 2u);
        co_await wal.close();
    }

    // Reopen and replay
    {
        auto wal = co_await IndexWAL::open(self->dir_);
        MemTable mt;
        auto count = co_await wal.replay(mt);

        EXPECT_EQ(count, 2u);
        EXPECT_TRUE(mt.isTombstone("key1"));
        EXPECT_EQ(*mt.get("key2"), "val2");
        EXPECT_EQ(*mt.get("key3"), "val3");
        co_await wal.close();
    }
}

SEASTAR_TEST_F(IndexWALTest, Rotate) {
    auto wal = co_await IndexWAL::open(self->dir_);

    IndexWriteBatch batch;
    batch.put("key1", "val1");
    co_await wal.append(batch);

    auto oldPath = co_await wal.rotate();
    EXPECT_TRUE(std::filesystem::exists(oldPath));

    // New WAL should be writable
    IndexWriteBatch batch2;
    batch2.put("key2", "val2");
    co_await wal.append(batch2);

    // Replay new WAL
    MemTable mt;
    auto count = co_await wal.replay(mt);
    EXPECT_EQ(count, 1u);  // Only key2 in new WAL
    EXPECT_EQ(*mt.get("key2"), "val2");

    co_await wal.close();

    // Clean up old WAL
    co_await IndexWAL::deleteFile(oldPath);
    EXPECT_FALSE(std::filesystem::exists(oldPath));
}

SEASTAR_TEST_F(IndexWALTest, EmptyReplay) {
    auto wal = co_await IndexWAL::open(self->dir_);
    MemTable mt;
    auto count = co_await wal.replay(mt);
    EXPECT_EQ(count, 0u);
    EXPECT_TRUE(mt.empty());
    co_await wal.close();
}

SEASTAR_TEST_F(IndexWALTest, SyncMakesAppendsDurableWithoutClose) {
    auto wal = co_await IndexWAL::open(self->dir_);

    IndexWriteBatch b;
    b.put("crashkey", "crashval");
    co_await wal.append(b);  // sits in the 1MB user-space buffer
    co_await wal.sync();

    // Read back with an independent instance while the writer is still open —
    // equivalent to a crash right after sync (no close, no destructor net).
    {
        auto reader = co_await IndexWAL::open(self->dir_);
        MemTable mt;
        auto count = co_await reader.replay(mt);
        EXPECT_EQ(count, 1u);
        EXPECT_EQ(mt.get("crashkey").value_or(""), "crashval");
        co_await reader.close();
    }

    // The writer must be able to continue appending after a sync: the synced
    // partial tail block is rewritten in place by the next flush.
    IndexWriteBatch b2;
    b2.put("second", "2");
    co_await wal.append(b2);
    co_await wal.close();

    auto reader2 = co_await IndexWAL::open(self->dir_);
    MemTable mt2;
    auto count2 = co_await reader2.replay(mt2);
    EXPECT_EQ(count2, 2u);
    EXPECT_EQ(*mt2.get("crashkey"), "crashval");
    EXPECT_EQ(*mt2.get("second"), "2");
    co_await reader2.close();
}

SEASTAR_TEST_F(IndexWALTest, ReplayKeepsOldGenerationsUntilPurge) {
    std::string old1, old2;
    {
        auto wal = co_await IndexWAL::open(self->dir_);
        IndexWriteBatch b1;
        b1.put("k1", "v1");
        co_await wal.append(b1);
        old1 = co_await wal.rotate();

        IndexWriteBatch b2;
        b2.put("k2", "v2");
        co_await wal.append(b2);
        old2 = co_await wal.rotate();

        IndexWriteBatch b3;
        b3.put("k3", "v3");
        co_await wal.append(b3);
        co_await wal.close();
    }

    auto wal2 = co_await IndexWAL::open(self->dir_);
    MemTable mt;
    auto count = co_await wal2.replay(mt);
    EXPECT_EQ(count, 3u);

    // Replay must NOT delete the consumed generations — the replayed data is
    // volatile until the caller flushes it to an SSTable.
    EXPECT_TRUE(std::filesystem::exists(old1));
    EXPECT_TRUE(std::filesystem::exists(old2));

    co_await wal2.purgeReplayedFiles();
    EXPECT_FALSE(std::filesystem::exists(old1));
    EXPECT_FALSE(std::filesystem::exists(old2));

    co_await wal2.close();
}
