#include "../../../lib/index/native/compaction.hpp"
#include "../../../lib/index/native/manifest.hpp"
#include "../../../lib/index/native/sstable.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <format>
#include <map>
#include <set>

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

// ---------------------------------------------------------------------------
// Tiered compaction gate: ~40 simulated memtable flushes must keep every
// level bounded (L0 < level0Threshold, L1/L2 < levelThreshold) via the
// L0→L1→L2→L3 cascade in maybeCompact(), and all data must stay readable.
// Before the tiered policy, L1 grew forever (one file per 4 flushes).
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionTest, TieredCascadeKeepsLevelsBounded) {
    auto manifest = co_await Manifest::open(self->dir_);

    CompactionConfig config;
    config.level0Threshold = 4;
    config.levelThreshold = 8;
    CompactionEngine compactor(self->dir_, manifest, config);

    constexpr int kFlushes = 40;
    constexpr int kKeysPerFlush = 20;

    for (int flush = 0; flush < kFlushes; ++flush) {
        // Simulate a memtable flush: write one L0 SSTable.
        uint64_t fn = manifest.nextFileNumber();
        auto writer = co_await SSTableWriter::create(self->sstFilename(fn), 512);
        for (int i = 0; i < kKeysPerFlush; ++i) {
            // Unique keys per flush plus one shared key updated by every flush
            // (verifies newest-wins survives multi-level merges).
            writer.add(std::format("flush:{:03d}:key:{:03d}", flush, i), std::format("v{}:{}", flush, i));
        }
        writer.add("shared:latest", std::format("from_flush_{}", flush));
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        co_await manifest.addFile(meta);

        // Same driver as the flush path: cascade until no compaction picked.
        co_await compactor.maybeCompact();

        // Invariant: every level stays below its threshold after maybeCompact.
        EXPECT_LT(manifest.filesAtLevel(0).size(), 4u) << "flush " << flush;
        EXPECT_LT(manifest.filesAtLevel(1).size(), 8u) << "flush " << flush;
        EXPECT_LT(manifest.filesAtLevel(2).size(), 8u) << "flush " << flush;
    }

    // 40 flushes = 10 L0→L1 compactions = 1 L1→L2 compaction (at 8) + 2 L1 left.
    EXPECT_EQ(manifest.filesAtLevel(0).size(), 0u);
    EXPECT_EQ(manifest.filesAtLevel(1).size(), 2u);
    EXPECT_EQ(manifest.filesAtLevel(2).size(), 1u);

    // All keys must still be readable from the merged files, newest wins.
    // Scan every live file, newest (highest fileNumber) first for dup resolution.
    auto files = manifest.files();
    std::sort(files.begin(), files.end(),
              [](const SSTableMetadata& a, const SSTableMetadata& b) { return a.fileNumber > b.fileNumber; });

    std::map<std::string, std::string> merged;
    for (const auto& f : files) {
        auto reader = co_await SSTableReader::open(self->sstFilename(f.fileNumber));
        auto it = reader->newIterator();
        co_await it->seekToFirst();
        while (it->valid()) {
            merged.emplace(std::string(it->key()), std::string(it->value()));  // first (newest) wins
            co_await it->next();
        }
        co_await reader->close();
    }

    EXPECT_EQ(merged.size(), static_cast<size_t>(kFlushes * kKeysPerFlush + 1));
    for (int flush = 0; flush < kFlushes; ++flush) {
        for (int i = 0; i < kKeysPerFlush; i += 7) {
            auto key = std::format("flush:{:03d}:key:{:03d}", flush, i);
            EXPECT_TRUE(merged.contains(key)) << key;
            if (merged.contains(key)) {
                EXPECT_EQ(merged[key], std::format("v{}:{}", flush, i));
            }
        }
    }
    EXPECT_EQ(merged["shared:latest"], std::format("from_flush_{}", kFlushes - 1));

    co_await manifest.close();
}

// ---------------------------------------------------------------------------
// Tombstone GC: when a compaction's input set is every live file (full
// compaction) and all inputs are older than the grace period, tombstone
// sentinels ("\0") are dropped and the deleted keys stay dead.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionTest, TombstoneGCOnFullCompaction) {
    auto manifest = co_await Manifest::open(self->dir_);

    const std::string tombstone("\0", 1);
    const uint64_t nowNs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
    const uint64_t agedTimestamp = nowNs - 20ULL * 24 * 3600 * 1'000'000'000;  // 20 days ago

    // File 1: live keys. Files 2-4: tombstones shadowing half of them + fresh keys.
    for (int fileIdx = 0; fileIdx < 4; ++fileIdx) {
        uint64_t fn = manifest.nextFileNumber();
        auto writer = co_await SSTableWriter::create(self->sstFilename(fn), 512);
        if (fileIdx == 0) {
            for (int i = 0; i < 40; ++i) {
                writer.add(std::format("key:{:04d}", i), std::format("val:{}", i));
            }
        } else {
            // Each later file deletes a distinct slice of file 1's keys
            for (int i = (fileIdx - 1) * 10; i < fileIdx * 10; ++i) {
                writer.add(std::format("key:{:04d}", i), tombstone);
            }
            writer.add(std::format("live:{:04d}", fileIdx), "alive");
        }
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        meta.writeTimestamp = agedTimestamp;  // Age past the grace period
        co_await manifest.addFile(meta);
    }

    CompactionConfig config;
    config.level0Threshold = 4;
    config.tombstoneGracePeriodMs = 10ULL * 24 * 3600 * 1000;  // 10 days
    CompactionEngine compactor(self->dir_, manifest, config);
    co_await compactor.maybeCompact();  // input = all 4 live files → full compaction

    EXPECT_EQ(manifest.files().size(), 1u);
    if (manifest.files().size() != 1) {
        co_await manifest.close();
        co_return;
    }
    auto reader = co_await SSTableReader::open(self->sstFilename(manifest.files()[0].fileNumber));
    auto it = reader->newIterator();
    co_await it->seekToFirst();
    size_t tombstonesSeen = 0;
    std::set<std::string> keys;
    while (it->valid()) {
        if (it->value() == tombstone) {
            ++tombstonesSeen;
        }
        keys.insert(std::string(it->key()));
        co_await it->next();
    }
    co_await reader->close();

    EXPECT_EQ(tombstonesSeen, 0u) << "aged tombstones must be GC'd in a full compaction";
    // keys 0..29 were deleted, 30..39 survive, plus 3 "live:" keys
    EXPECT_EQ(keys.size(), 13u);
    EXPECT_FALSE(keys.contains("key:0000"));
    EXPECT_FALSE(keys.contains("key:0029"));
    EXPECT_TRUE(keys.contains("key:0030"));
    EXPECT_TRUE(keys.contains("key:0039"));
    EXPECT_TRUE(keys.contains("live:0001"));

    co_await manifest.close();
}

// ---------------------------------------------------------------------------
// Tombstone safety: a PARTIAL compaction (a non-input file still live) must
// retain tombstones, or deleted data in the excluded file would resurrect.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(CompactionTest, TombstonesRetainedInPartialCompaction) {
    auto manifest = co_await Manifest::open(self->dir_);

    const std::string tombstone("\0", 1);
    const uint64_t agedTimestamp = 1'000'000'000ULL;  // ancient — grace period satisfied

    // A pre-existing L2 file holding the key the tombstone deletes — NOT part
    // of the upcoming L0 compaction.
    {
        uint64_t fn = manifest.nextFileNumber();
        auto writer = co_await SSTableWriter::create(self->sstFilename(fn), 512);
        writer.add("doomed", "old_value");
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 2;
        meta.writeTimestamp = agedTimestamp;
        co_await manifest.addFile(meta);
    }

    // 4 aged L0 files, one carrying the tombstone for "doomed"
    for (int fileIdx = 0; fileIdx < 4; ++fileIdx) {
        uint64_t fn = manifest.nextFileNumber();
        auto writer = co_await SSTableWriter::create(self->sstFilename(fn), 512);
        if (fileIdx == 0) {
            writer.add("doomed", tombstone);  // sorted: "doomed" < "k0"
        }
        writer.add(std::format("k{}", fileIdx), "v");
        auto meta = co_await writer.finish();
        meta.fileNumber = fn;
        meta.level = 0;
        meta.writeTimestamp = agedTimestamp;
        co_await manifest.addFile(meta);
    }

    CompactionConfig config;
    config.level0Threshold = 4;
    CompactionEngine compactor(self->dir_, manifest, config);
    co_await compactor.maybeCompact();  // L0→L1 only; L2 file excluded → NOT full

    // The tombstone must survive into the L1 output so "doomed" stays deleted.
    auto l1Files = manifest.filesAtLevel(1);
    EXPECT_EQ(l1Files.size(), 1u);
    if (l1Files.size() != 1) {
        co_await manifest.close();
        co_return;
    }
    auto reader = co_await SSTableReader::open(self->sstFilename(l1Files[0].fileNumber));
    auto val = co_await reader->get("doomed");
    EXPECT_TRUE(val.has_value()) << "tombstone dropped in partial compaction — deleted key would resurrect";
    if (val.has_value()) {
        EXPECT_EQ(*val, tombstone);
    }
    co_await reader->close();

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
