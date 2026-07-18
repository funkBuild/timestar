// Fault-injection tests for the native index write path.
//
// Live ENOSPC injection is not feasible here without an invasive filesystem
// seam: IndexWAL, Manifest and SSTableWriter all call seastar::open_file_dma
// directly on internally-built paths, and no loop device / small tmpfs is
// available without root. Instead these tests simulate the *on-disk aftermath*
// of ENOSPC / torn writes / crashes — a prefix of the data persisted, followed
// by garbage or nothing — by truncating and corrupting the real files between
// sessions, then assert that recovery restores every previously-durable record
// (data checks, not just "no crash").

#include "../../../lib/index/native/index_wal.hpp"
#include "../../../lib/index/native/manifest.hpp"
#include "../../../lib/index/native/memtable.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/index/native/write_batch.hpp"

#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

using namespace timestar::index;

namespace {

std::string readWholeFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void writeWholeFile(const std::string& path, const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
}

void appendLE32(std::string& out, uint32_t v) {
    char buf[4];
    for (int i = 0; i < 4; ++i)
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    out.append(buf, 4);
}

uint32_t readLE32(const std::string& data, size_t off) {
    return static_cast<uint32_t>(static_cast<uint8_t>(data[off])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data[off + 1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data[off + 2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data[off + 3])) << 24);
}

// Parse WAL frame boundaries from raw file bytes.
// Frame layout: record_length(4) | crc(4) | sequence(8) | payload.
// record_length covers crc + sequence + payload.
std::vector<size_t> parseWalBoundaries(const std::string& data) {
    std::vector<size_t> starts;
    size_t pos = 0;
    while (pos + 4 <= data.size()) {
        uint32_t innerLen = readLE32(data, pos);
        if (pos + 4 + innerLen > data.size())
            break;
        starts.push_back(pos);
        pos += 4 + innerLen;
    }
    // The pristine file must parse exactly — otherwise the test itself is wrong.
    EXPECT_EQ(pos, data.size()) << "pristine WAL did not parse cleanly";
    return starts;
}

std::string testKey(int i) { return std::format("fault_key_{:02d}", i); }

// Distinct sizes so record boundaries never coincide with DMA-block multiples
// by accident, and content is verifiable per record.
std::string testValue(int i) {
    return std::format("val{:02d}-", i) + std::string(10 + 29 * static_cast<size_t>(i), static_cast<char>('A' + i));
}

}  // namespace

// ============================================================================
// IndexWAL: torn / short writes
// ============================================================================

class IndexWalFaultInjectionTest : public ::testing::Test {
public:
    void SetUp() override {
        root_ = (std::filesystem::temp_directory_path() / "timestar_wal_fault_test").string();
        std::filesystem::remove_all(root_);
        std::filesystem::create_directories(root_);
    }
    void TearDown() override { std::filesystem::remove_all(root_); }

    // Write `n` records into a fresh WAL dir and return the raw file bytes.
    seastar::future<std::string> buildPristineWal(std::string dir, int n) {
        auto wal = co_await IndexWAL::open(dir);
        for (int i = 0; i < n; ++i) {
            IndexWriteBatch b;
            b.put(testKey(i), testValue(i));
            co_await wal.append(b);
        }
        co_await wal.close();
        co_return readWholeFile(dir + "/idx_000000.wal");
    }

    // Replay the WAL in `dir` and assert exactly records [0, expected) are
    // present with correct values, and later records are absent.
    seastar::future<> assertReplayPrefix(std::string dir, uint64_t expected, int total, std::string what) {
        auto wal = co_await IndexWAL::open(dir);
        MemTable mt;
        auto count = co_await wal.replay(mt);
        EXPECT_EQ(count, expected) << what;
        for (int i = 0; i < total; ++i) {
            auto v = mt.get(testKey(i));
            if (static_cast<uint64_t>(i) < expected) {
                EXPECT_TRUE(v.has_value()) << what << " — acked record " << i << " lost";
                if (v.has_value()) {
                    EXPECT_EQ(std::string(*v), testValue(i)) << what << " — record " << i << " corrupted";
                }
            } else {
                EXPECT_FALSE(v.has_value()) << what << " — phantom record " << i << " appeared";
            }
        }
        co_await wal.close();
    }

    std::string root_;
};

// Torn write / short write: a flush persisted only a prefix of the log before
// a "crash". Truncate a multi-record WAL at EVERY structural boundary of every
// record — mid-length-header, mid-CRC, mid-sequence, mid-payload, one byte
// short, and the exact record boundary — and assert every fully-persisted
// record before the cut is recovered intact, with the torn tail discarded.
SEASTAR_TEST_F(IndexWalFaultInjectionTest, TruncationAtEveryStructuralBoundaryRecoversPrefix) {
    constexpr int kRecords = 5;
    auto pristineDir = self->root_ + "/pristine";
    std::filesystem::create_directories(pristineDir);
    auto pristine = co_await self->buildPristineWal(pristineDir, kRecords);
    EXPECT_FALSE(pristine.empty());

    auto starts = parseWalBoundaries(pristine);
    EXPECT_EQ(starts.size(), static_cast<size_t>(kRecords));
    if (starts.size() != static_cast<size_t>(kRecords))
        co_return;

    struct Cut {
        size_t offset;
        uint64_t expected;
        std::string what;
    };
    std::vector<Cut> cuts;
    for (int k = 0; k < kRecords; ++k) {
        size_t s = starts[static_cast<size_t>(k)];
        size_t e = (k + 1 < kRecords) ? starts[static_cast<size_t>(k) + 1] : pristine.size();
        size_t payload = e - s - 16;
        auto exp = static_cast<uint64_t>(k);
        cuts.push_back({s, exp, std::format("cut at exact boundary of record {}", k)});
        cuts.push_back({s + 2, exp, std::format("cut mid-length-header of record {}", k)});
        cuts.push_back({s + 6, exp, std::format("cut mid-CRC of record {}", k)});
        cuts.push_back({s + 12, exp, std::format("cut mid-sequence of record {}", k)});
        cuts.push_back({s + 16 + payload / 2, exp, std::format("cut mid-payload of record {}", k)});
        cuts.push_back({e - 1, exp, std::format("cut one byte short of record {}", k)});
    }
    // Sanity: the untouched file replays everything.
    cuts.push_back({pristine.size(), kRecords, "no cut (full file)"});

    int caseNum = 0;
    for (const auto& cut : cuts) {
        auto dir = self->root_ + std::format("/case_{:03d}", caseNum++);
        std::filesystem::create_directories(dir);
        writeWholeFile(dir + "/idx_000000.wal", pristine.substr(0, cut.offset));
        co_await self->assertReplayPrefix(dir, cut.expected, kRecords, cut.what);
    }
}

// ENOSPC-aftermath emulation: every acked (synced) record is on disk, followed
// by a garbage tail in the shapes a failed/partial flush can leave behind.
// Replay must recover ALL acked records and discard the tail — it must not
// lose earlier data or fabricate records from the garbage.
SEASTAR_TEST_F(IndexWalFaultInjectionTest, GarbageTailAfterSyncedRecordsIsDiscarded) {
    constexpr int kRecords = 3;
    auto pristineDir = self->root_ + "/pristine";
    std::filesystem::create_directories(pristineDir);
    auto pristine = co_await self->buildPristineWal(pristineDir, kRecords);
    auto starts = parseWalBoundaries(pristine);
    EXPECT_EQ(starts.size(), static_cast<size_t>(kRecords));
    if (starts.size() != static_cast<size_t>(kRecords))
        co_return;

    struct Tail {
        std::string bytes;
        std::string what;
    };
    std::vector<Tail> tails;

    // (a) Partial length header — fewer than 4 bytes persisted.
    tails.push_back({std::string("\x01\x02\x03", 3), "partial length header"});

    // (b) Length header promising far more bytes than exist (torn mid-record).
    {
        std::string t;
        appendLE32(t, 1u << 20);
        appendLE32(t, 0xDEADBEEF);
        t += "junkjunk";
        tails.push_back({t, "length header promising 1MB"});
    }

    // (c) A structurally complete record whose payload was corrupted (CRC
    //     mismatch) — e.g. a block half-written over old data.
    {
        size_t r0end = starts[1];
        std::string frame = pristine.substr(0, r0end);
        frame[20] = static_cast<char>(frame[20] ^ 0x5A);  // flip a payload byte
        tails.push_back({frame, "complete frame with corrupt CRC"});
    }

    // (d) Undersized record length (< CRC+sequence minimum of 12).
    {
        std::string t;
        appendLE32(t, 4);
        t += "12345678";
        tails.push_back({t, "undersized record length"});
    }

    // (e) Zero-filled DMA padding block: crash between sync()'s padded
    //     dma_write and the logical truncate leaves trailing zeros.
    tails.push_back({std::string(4096, '\0'), "zero-filled DMA padding"});

    int caseNum = 0;
    for (const auto& tail : tails) {
        auto dir = self->root_ + std::format("/tail_{:03d}", caseNum++);
        std::filesystem::create_directories(dir);
        writeWholeFile(dir + "/idx_000000.wal", pristine + tail.bytes);
        co_await self->assertReplayPrefix(dir, kRecords, kRecords, tail.what);
    }
}

// Multi-generation crash: an old rotated generation is intact on disk, the
// current generation is torn mid-record. Replay must recover every record of
// the old generation plus the intact prefix of the new one, and must NOT
// delete the consumed files before purge.
SEASTAR_TEST_F(IndexWalFaultInjectionTest, OldGenerationIntactNewGenerationTorn) {
    auto dir = self->root_ + "/multigen";
    std::filesystem::create_directories(dir);
    {
        auto wal = co_await IndexWAL::open(dir);
        for (int i = 0; i < 2; ++i) {
            IndexWriteBatch b;
            b.put(testKey(i), testValue(i));
            co_await wal.append(b);
        }
        auto oldPath = co_await wal.rotate();  // gen 0 sealed, intact
        EXPECT_EQ(oldPath, dir + "/idx_000000.wal");
        for (int i = 2; i < 4; ++i) {
            IndexWriteBatch b;
            b.put(testKey(i), testValue(i));
            co_await wal.append(b);
        }
        co_await wal.close();
    }

    // Tear the CURRENT generation: keep record 2 whole, cut record 3 mid-frame.
    auto gen1Path = dir + "/idx_000001.wal";
    auto gen1 = readWholeFile(gen1Path);
    auto starts = parseWalBoundaries(gen1);
    EXPECT_EQ(starts.size(), 2u);
    if (starts.size() != 2u)
        co_return;
    writeWholeFile(gen1Path, gen1.substr(0, starts[1] + 7));

    auto wal = co_await IndexWAL::open(dir);
    MemTable mt;
    auto count = co_await wal.replay(mt);
    EXPECT_EQ(count, 3u);
    for (int i = 0; i < 3; ++i) {
        auto v = mt.get(testKey(i));
        EXPECT_TRUE(v.has_value()) << "record " << i << " lost";
        if (v.has_value()) {
            EXPECT_EQ(std::string(*v), testValue(i));
        }
    }
    EXPECT_FALSE(mt.get(testKey(3)).has_value());

    // Replayed data is volatile until flushed — files must survive replay.
    EXPECT_TRUE(std::filesystem::exists(dir + "/idx_000000.wal"));
    EXPECT_TRUE(std::filesystem::exists(gen1Path));
    co_await wal.close();
}

// ============================================================================
// NativeIndex: full-stack recovery over injected faults
// ============================================================================

class NativeIndexFaultInjectionTest : public ::testing::Test {
public:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }

    // Latest WAL generation file for shard 0.
    static std::string currentWalPath() {
        std::string best;
        for (const auto& entry : std::filesystem::directory_iterator("shard_0/native_index/wal")) {
            if (entry.path().extension() == ".wal" && entry.path().string() > best) {
                best = entry.path().string();
            }
        }
        return best;
    }
};

// A crash leaves acked series records in the WAL followed by a torn tail (the
// on-disk shape of an ENOSPC / power-cut mid-flush). Reopen must recover every
// acked series through the public API, keep serving lookups, and survive a
// further clean write cycle.
SEASTAR_TEST_F(NativeIndexFaultInjectionTest, TornWalTailStillRecoversAckedSeries) {
    SeriesId128 idA, idB;
    {
        NativeIndex index(0);
        co_await index.open();
        idA = co_await index.getOrCreateSeriesId("fault_m", {{"host", "a"}}, "f");
        idB = co_await index.getOrCreateSeriesId("fault_m", {{"host", "b"}}, "f");
        // no close() — simulated crash; destructor safety-net persists the WAL
    }

    // Inject the torn tail after the acked records.
    auto walPath = NativeIndexFaultInjectionTest::currentWalPath();
    EXPECT_FALSE(walPath.empty()) << "no WAL file found after crash";
    if (walPath.empty())
        co_return;
    auto data = readWholeFile(walPath);
    EXPECT_FALSE(data.empty());
    std::string torn;
    appendLE32(torn, 1u << 16);   // record_length promising 64KB
    appendLE32(torn, 0xDEADBEEF); // bogus CRC
    torn += "enospc-garbage";
    writeWholeFile(walPath, data + torn);

    SeriesId128 idC;
    {
        NativeIndex index(0);
        co_await index.open();

        auto metaA = co_await index.getSeriesMetadata(idA);
        EXPECT_TRUE(metaA.has_value()) << "acked series A lost after torn WAL tail";
        if (metaA.has_value()) {
            EXPECT_EQ(metaA->measurement, "fault_m");
            EXPECT_EQ(metaA->tags["host"], "a");
        }

        auto metaB = co_await index.getSeriesMetadata(idB);
        EXPECT_TRUE(metaB.has_value()) << "acked series B lost after torn WAL tail";
        if (metaB.has_value()) {
            EXPECT_EQ(metaB->tags["host"], "b");
        }

        // Identity must round-trip: the same key must resolve to the same ID.
        auto again = co_await index.getOrCreateSeriesId("fault_m", {{"host", "a"}}, "f");
        EXPECT_EQ(again, idA);

        // Tag postings must be repaired for the crash window.
        auto byTag = co_await index.findSeriesByTag("fault_m", "host", "b");
        EXPECT_EQ(byTag.size(), 1u);
        if (byTag.size() == 1) {
            EXPECT_EQ(byTag[0], idB);
        }

        // The index must remain writable after recovery.
        idC = co_await index.getOrCreateSeriesId("fault_m", {{"host", "c"}}, "f");
        co_await index.close();
    }

    // Everything survives a further clean reopen (recovered data now durable
    // in an SSTable, not resting on the WAL that carried the garbage).
    {
        NativeIndex index(0);
        co_await index.open();
        EXPECT_TRUE((co_await index.getSeriesMetadata(idA)).has_value());
        EXPECT_TRUE((co_await index.getSeriesMetadata(idB)).has_value());
        EXPECT_TRUE((co_await index.getSeriesMetadata(idC)).has_value());
        co_await index.close();
    }
}

// SSTable flush interrupted: the .sst file was (partially) written but the
// crash hit before the manifest referenced it. Reopen must not open or trust
// the orphan partial file — including when the recovery flush reuses the same
// file number — and all acked data must come back via WAL replay.
SEASTAR_TEST_F(NativeIndexFaultInjectionTest, OrphanPartialSSTableIgnoredAndAckedDataRecovered) {
    SeriesId128 idA, idB;
    {
        NativeIndex index(0);
        co_await index.open();
        idA = co_await index.getOrCreateSeriesId("orphan_m", {{"host", "a"}}, "f");
        idB = co_await index.getOrCreateSeriesId("orphan_m", {{"host", "b"}}, "f");
        // no close() — crash before any memtable flush: manifest lists 0 files
    }

    // Simulate the interrupted flush: partial garbage at the exact path the
    // recovery flush will allocate next (fresh manifest → file number 1), plus
    // a stray orphan with an unrelated number.
    writeWholeFile("shard_0/native_index/idx_000001.sst", std::string(100, 'X'));
    writeWholeFile("shard_0/native_index/idx_000042.sst", "partial sstable garbage");

    {
        NativeIndex index(0);
        // open() must not throw on the orphans (they are not in the manifest)
        // and must flush the WAL-replayed memtable to a real SSTable.
        co_await index.open();

        auto metaA = co_await index.getSeriesMetadata(idA);
        EXPECT_TRUE(metaA.has_value()) << "acked series A lost after interrupted SSTable flush";
        if (metaA.has_value()) {
            EXPECT_EQ(metaA->measurement, "orphan_m");
        }
        auto metaB = co_await index.getSeriesMetadata(idB);
        EXPECT_TRUE(metaB.has_value()) << "acked series B lost after interrupted SSTable flush";

        auto byTag = co_await index.findSeriesByTag("orphan_m", "host", "a");
        EXPECT_EQ(byTag.size(), 1u);
        if (byTag.size() == 1) {
            EXPECT_EQ(byTag[0], idA);
        }

        co_await index.close();
    }

    // After the clean close the data lives in a real SSTable written over /
    // alongside the orphans. A further reopen must read it back fine.
    {
        NativeIndex index(0);
        co_await index.open();
        auto metaA = co_await index.getSeriesMetadata(idA);
        EXPECT_TRUE(metaA.has_value());
        if (metaA.has_value()) {
            EXPECT_EQ(metaA->tags["host"], "a");
        }
        auto metaB = co_await index.getSeriesMetadata(idB);
        EXPECT_TRUE(metaB.has_value());
        if (metaB.has_value()) {
            EXPECT_EQ(metaB->tags["host"], "b");
        }
        co_await index.close();
    }
}

// ============================================================================
// Manifest: ENOSPC-during-append variant not covered by manifest_test.cpp
// ============================================================================

class ManifestFaultInjectionTest : public ::testing::Test {
public:
    void SetUp() override {
        dir_ = (std::filesystem::temp_directory_path() / "timestar_manifest_fault_test").string();
        std::filesystem::remove_all(dir_);
        std::filesystem::create_directories(dir_);
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

// ENOSPC (or power cut) during a manifest append can persist FEWER than the
// 8 header bytes of the next frame. manifest_test.cpp covers a full header +
// partial payload (TornTailDiscardedOnRecovery); this covers the partial-
// header tail, which takes a different branch in recover(). The manifest must
// keep all prior records, rewrite itself clean, and stay appendable.
SEASTAR_TEST_F(ManifestFaultInjectionTest, PartialFrameHeaderTailDiscardedOnRecovery) {
    {
        auto m = co_await Manifest::open(self->dir_);
        SSTableMetadata f1;
        f1.fileNumber = m.nextFileNumber();
        f1.level = 0;
        f1.minKey = "aaa";
        f1.maxKey = "mmm";
        co_await m.addFile(f1);

        SSTableMetadata f2;
        f2.fileNumber = m.nextFileNumber();
        f2.level = 1;
        f2.minKey = "nnn";
        f2.maxKey = "zzz";
        co_await m.addFile(f2);
        co_await m.close();
    }

    // Append 5 garbage bytes: less than the [len(4)][crc(4)] frame header.
    auto path = self->dir_ + "/MANIFEST";
    auto data = readWholeFile(path);
    EXPECT_FALSE(data.empty());
    writeWholeFile(path, data + std::string("\x13\x37\x00\xff\x42", 5));

    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 2u) << "records before the torn header lost";
        if (m.files().size() == 2) {
            EXPECT_EQ(m.files()[0].fileNumber, 1u);
            EXPECT_EQ(m.files()[0].minKey, "aaa");
            EXPECT_EQ(m.files()[1].fileNumber, 2u);
            EXPECT_EQ(m.files()[1].maxKey, "zzz");
        }
        co_await m.close();
    }

    // open() rewrote a clean snapshot: no garbage remains, appends still work.
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 2u);
        SSTableMetadata f3;
        f3.fileNumber = m.nextFileNumber();
        f3.level = 0;
        co_await m.addFile(f3);
        co_await m.close();
    }
    {
        auto m = co_await Manifest::open(self->dir_);
        EXPECT_EQ(m.files().size(), 3u);
        co_await m.close();
    }
}
