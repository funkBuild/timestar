#include "../../../lib/index/native/compaction.hpp"
#include "../../../lib/index/native/manifest.hpp"
#include "../../../lib/index/native/sstable.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <format>

using namespace timestar::index;

class CompactionTest : public ::testing::Test {
public:
    void SetUp() override {
        dir_ = std::filesystem::temp_directory_path() / "timestar_compaction_test";
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }

    std::string sstFilename(uint64_t fileNumber) {
        char buf[32];
        snprintf(buf, sizeof(buf), "idx_%06lu.sst", fileNumber);
        return dir_ + "/" + buf;
    }

    std::string dir_;
};

SEASTAR_TEST_F(CompactionTest, CompactFourL0Files) {
    auto manifest = co_await Manifest::open(self->dir_);

    // Create 4 L0 SSTable files with interleaved keys
    for (int fileIdx = 0; fileIdx < 4; ++fileIdx) {
        uint64_t fn = manifest.nextFileNumber();
        auto path = self->sstFilename(fn);
        auto writer = co_await SSTableWriter::create(path, 512);

        for (int i = fileIdx; i < 100; i += 4) {
            writer.add(std::format("key:{:04d}", i), std::format("val_f{}:{:04d}", fileIdx, i));
        }

        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        co_await manifest.addFile(meta);
    }

    EXPECT_EQ(manifest.filesAtLevel(0).size(), 4u);

    // Run compaction
    CompactionConfig config;
    config.level0Threshold = 4;
    CompactionEngine compactor(self->dir_, manifest, config);
    co_await compactor.maybeCompact();

    // Should now have 0 L0 files and 1 L1 file
    EXPECT_EQ(manifest.filesAtLevel(0).size(), 0u);
    EXPECT_EQ(manifest.filesAtLevel(1).size(), 1u);

    // Verify the merged file contains all 100 keys
    auto l1Files = manifest.filesAtLevel(1);
    auto reader = co_await SSTableReader::open(self->sstFilename(l1Files[0].fileNumber));
    auto it = reader->newIterator();
    co_await it->seekToFirst();

    int count = 0;
    while (it->valid()) {
        EXPECT_EQ(it->key(), std::format("key:{:04d}", count));
        ++count;
        co_await it->next();
    }
    EXPECT_EQ(count, 100);

    co_await reader->close();
    co_await manifest.close();
}

SEASTAR_TEST_F(CompactionTest, CompactAll) {
    auto manifest = co_await Manifest::open(self->dir_);

    // Create 2 files with overlapping keys
    for (int fileIdx = 0; fileIdx < 2; ++fileIdx) {
        uint64_t fn = manifest.nextFileNumber();
        auto path = self->sstFilename(fn);
        auto writer = co_await SSTableWriter::create(path, 512);

        for (int i = 0; i < 50; ++i) {
            // Different values for same keys
            writer.add(std::format("dup:{:04d}", i), std::format("v{}", fileIdx));
        }
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        co_await manifest.addFile(meta);
    }

    CompactionEngine compactor(self->dir_, manifest);
    co_await compactor.compactAll();

    // Should merge into single file
    EXPECT_EQ(manifest.files().size(), 1u);

    co_await manifest.close();
}

SEASTAR_TEST_F(CompactionTest, NoCompactionNeeded) {
    auto manifest = co_await Manifest::open(self->dir_);

    // Only 2 files — below threshold of 4
    for (int i = 0; i < 2; ++i) {
        uint64_t fn = manifest.nextFileNumber();
        auto path = self->sstFilename(fn);
        auto writer = co_await SSTableWriter::create(path);
        writer.add("key", "val");
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        co_await manifest.addFile(meta);
    }

    CompactionEngine compactor(self->dir_, manifest);
    co_await compactor.maybeCompact();

    // No compaction — still 2 L0 files
    EXPECT_EQ(manifest.filesAtLevel(0).size(), 2u);

    co_await manifest.close();
}
