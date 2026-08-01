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

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <format>
#include <fstream>
#include <iterator>
#include <seastar/core/coroutine.hh>
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

std::string testKey(int i) {
    return std::format("fault_key_{:02d}", i);
}

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
// by an incomplete tail in the shapes a failed/partial flush can leave behind.
// Replay must recover ALL acked records and discard only the incomplete tail.
SEASTAR_TEST_F(IndexWalFaultInjectionTest, IncompleteTailAfterSyncedRecordsIsDiscarded) {
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

    // (c) Zero-filled DMA padding block: crash between sync()'s padded
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

// A fully present record is not a crash tear. Silently discarding one with a
// bad checksum would let startup serve an index missing acknowledged metadata.
SEASTAR_TEST_F(IndexWalFaultInjectionTest, CompleteCorruptFrameFailsClosedAndIsPreserved) {
    auto pristineDir = self->root_ + "/corrupt";
    std::filesystem::create_directories(pristineDir);
    auto bytes = co_await self->buildPristineWal(pristineDir, 2);
    auto starts = parseWalBoundaries(bytes);
    EXPECT_EQ(starts.size(), 2u);
    if (starts.size() != 2u)
        co_return;
    bytes[starts[1] + 20] = static_cast<char>(bytes[starts[1] + 20] ^ 0x5A);

    const auto path = pristineDir + "/idx_000000.wal";
    writeWholeFile(path, bytes);
    auto wal = co_await IndexWAL::open(pristineDir);
    MemTable mt;
    bool failed = false;
    try {
        (void)co_await wal.replay(mt);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_EQ(readWholeFile(path), bytes);
    co_await wal.close();
}

SEASTAR_TEST_F(IndexWalFaultInjectionTest, DiscoveryRejectsNumericPrefixAliasAndPreservesIt) {
    auto dir = self->root_ + "/bad_name";
    std::filesystem::create_directories(dir);
    const auto malformed = dir + "/idx_000000junk.wal";
    writeWholeFile(malformed, "acknowledged-index-bytes");

    bool failed = false;
    try {
        (void)co_await IndexWAL::open(dir);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_EQ(readWholeFile(malformed), "acknowledged-index-bytes");
}

SEASTAR_TEST_F(IndexWalFaultInjectionTest, DiscoveryRejectsSymlinkedWal) {
    const auto dir = self->root_ + "/symlink";
    std::filesystem::create_directories(dir);
    const auto external = self->root_ + "/external.wal";
    writeWholeFile(external, "external-index-bytes");
    const auto link = dir + "/idx_000000.wal";
    std::filesystem::create_symlink("../external.wal", link);

    bool failed = false;
    try {
        (void)co_await IndexWAL::open(dir);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_TRUE(std::filesystem::is_symlink(link));
    EXPECT_EQ(readWholeFile(external), "external-index-bytes");
}

SEASTAR_TEST_F(IndexWalFaultInjectionTest, FreshOpenRefusesNonemptyCollision) {
    const auto dir = self->root_ + "/collision";
    std::filesystem::create_directories(dir);
    auto wal = co_await IndexWAL::open(dir);
    const auto path = dir + "/idx_000000.wal";
    writeWholeFile(path, "existing-index-wal");

    IndexWriteBatch batch;
    batch.put("new", "value");
    co_await wal.append(batch);
    bool failed = false;
    try {
        co_await wal.sync();
    } catch (const std::runtime_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_EQ(readWholeFile(path), "existing-index-wal");
}

SEASTAR_TEST_F(IndexWalFaultInjectionTest, GenerationExhaustionCannotWrap) {
    const auto dir = self->root_ + "/generation_exhaustion";
    std::filesystem::create_directories(dir);
    const auto path = dir + "/idx_18446744073709551615.wal";
    writeWholeFile(path, "");

    auto wal = co_await IndexWAL::open(dir);
    MemTable mt;
    EXPECT_EQ(co_await wal.replay(mt), 0u);
    bool failed = false;
    try {
        (void)co_await wal.rotate();
    } catch (const std::overflow_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_TRUE(std::filesystem::exists(path));
    EXPECT_FALSE(std::filesystem::exists(dir + "/idx_000000.wal"));
    co_await wal.close();
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

SEASTAR_TEST_F(IndexWalFaultInjectionTest, TornSealedGenerationFailsClosed) {
    auto dir = self->root_ + "/sealed_torn";
    std::filesystem::create_directories(dir);
    {
        auto wal = co_await IndexWAL::open(dir);
        IndexWriteBatch oldBatch;
        oldBatch.put("old", "durable");
        co_await wal.append(oldBatch);
        (void)co_await wal.rotate();

        IndexWriteBatch currentBatch;
        currentBatch.put("current", "durable");
        co_await wal.append(currentBatch);
        co_await wal.close();
    }

    const auto oldPath = dir + "/idx_000000.wal";
    auto oldBytes = readWholeFile(oldPath);
    EXPECT_GT(oldBytes.size(), 1u);
    oldBytes.pop_back();
    writeWholeFile(oldPath, oldBytes);

    auto wal = co_await IndexWAL::open(dir);
    MemTable mt;
    bool failed = false;
    try {
        (void)co_await wal.replay(mt);
    } catch (const std::runtime_error&) {
        failed = true;
    }
    EXPECT_TRUE(failed);
    EXPECT_TRUE(std::filesystem::exists(oldPath));
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

    static std::string currentSSTablePath() {
        for (const auto& entry : std::filesystem::directory_iterator("shard_0/native_index")) {
            if (entry.path().extension() == ".sst") {
                return entry.path().string();
            }
        }
        return {};
    }

    static std::vector<std::string> sstablePaths() {
        std::vector<std::string> paths;
        for (const auto& entry : std::filesystem::directory_iterator("shard_0/native_index")) {
            if (entry.path().extension() == ".sst") {
                paths.push_back(entry.path().string());
            }
        }
        std::sort(paths.begin(), paths.end());
        return paths;
    }
};

// A crash may leave the first frame incomplete, so replay legitimately yields
// no records while the recovered path is still nonempty. Startup must rotate
// that generation before the first new mutation instead of truncating it lazily.
SEASTAR_TEST_F(NativeIndexFaultInjectionTest, TornFirstFrameRotatesBeforeNewWrites) {
    const std::string walDir = "shard_0/native_index/wal";
    std::filesystem::create_directories(walDir);
    writeWholeFile(walDir + "/idx_000000.wal", std::string("\x10\x00\x00", 3));

    SeriesId128 id;
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        EXPECT_FALSE(std::filesystem::exists(walDir + "/idx_000000.wal"));
        id = co_await index.getOrCreateSeriesId("first_torn", {{"host", "a"}}, "value");
        co_await index.close();
    }

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        EXPECT_TRUE((co_await index.getSeriesMetadata(id)).has_value());
        co_await index.close();
    }
}

// A crash leaves acked series records in the WAL followed by a torn tail (the
// on-disk shape of an ENOSPC / power-cut mid-flush). Reopen must recover every
// acked series through the public API, keep serving lookups, and survive a
// further clean write cycle.
SEASTAR_TEST_F(NativeIndexFaultInjectionTest, TornWalTailStillRecoversAckedSeries) {
    SeriesId128 idA, idB;
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        idA = co_await index.getOrCreateSeriesId("fault_m", {{"host", "a"}}, "f");
        idB = co_await index.getOrCreateSeriesId("fault_m", {{"host", "b"}}, "f");
        // Stop owned background coroutines without a clean state flush, then
        // let the IndexWAL destructor safety-net persist the buffered WAL.
        co_await index.abandonForTesting();
    }

    // Inject the torn tail after the acked records.
    auto walPath = NativeIndexFaultInjectionTest::currentWalPath();
    EXPECT_FALSE(walPath.empty()) << "no WAL file found after crash";
    if (walPath.empty())
        co_return;
    auto data = readWholeFile(walPath);
    EXPECT_FALSE(data.empty());
    std::string torn;
    appendLE32(torn, 1u << 16);    // record_length promising 64KB
    appendLE32(torn, 0xDEADBEEF);  // bogus CRC
    torn += "enospc-garbage";
    writeWholeFile(walPath, data + torn);

    SeriesId128 idC;
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
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
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        EXPECT_TRUE((co_await index.getSeriesMetadata(idA)).has_value());
        EXPECT_TRUE((co_await index.getSeriesMetadata(idB)).has_value());
        EXPECT_TRUE((co_await index.getSeriesMetadata(idC)).has_value());
        co_await index.close();
    }
}

// SSTable flush interrupted: the .sst file was (partially) written but the
// crash hit before the manifest referenced it. Reopen must durably reclaim the
// orphan partial file before a recovery flush reuses the same file number, and
// all acked data must come back via WAL replay.
SEASTAR_TEST_F(NativeIndexFaultInjectionTest, OrphanPartialSSTableReclaimedAndAckedDataRecovered) {
    SeriesId128 idA, idB;
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        idA = co_await index.getOrCreateSeriesId("orphan_m", {{"host", "a"}}, "f");
        idB = co_await index.getOrCreateSeriesId("orphan_m", {{"host", "b"}}, "f");
        // Simulated crash before any memtable flush: manifest lists 0 files.
        co_await index.abandonForTesting();
    }

    // Simulate the interrupted flush: partial garbage at the exact path the
    // recovery flush will allocate next (fresh manifest → file number 1), plus
    // a stray orphan with an unrelated number.
    writeWholeFile("shard_0/native_index/idx_000001.sst", std::string(100, 'X'));
    writeWholeFile("shard_0/native_index/idx_000042.sst", "partial sstable garbage");

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        // open() must reclaim the orphans (they are not in the manifest) and
        // flush the WAL-replayed memtable to a real SSTable.
        co_await index.open();
        EXPECT_FALSE(std::filesystem::exists("shard_0/native_index/idx_000042.sst"));

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

    // After the clean close the unrelated crash output is still absent. A
    // further strict reopen validates that every remaining SSTable is live and
    // must read both recovered series back.
    EXPECT_FALSE(std::filesystem::exists("shard_0/native_index/idx_000042.sst"));
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
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

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, UnreferencedCanonicalSSTableIsReclaimedWithoutDeletingLiveFile) {
    SeriesId128 id;
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        id = co_await index.getOrCreateSeriesId("obsolete_sst", {{"host", "live"}}, "value");
        co_await index.close();
    }

    const auto livePath = NativeIndexFaultInjectionTest::currentSSTablePath();
    EXPECT_FALSE(livePath.empty());
    if (livePath.empty()) {
        co_return;
    }
    const std::string orphanPath = "shard_0/native_index/idx_000042.sst";
    std::filesystem::copy_file(livePath, orphanPath);
    EXPECT_TRUE(std::filesystem::exists(orphanPath));

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        EXPECT_TRUE(std::filesystem::exists(livePath));
        EXPECT_FALSE(std::filesystem::exists(orphanPath));
        EXPECT_TRUE((co_await index.getSeriesMetadata(id)).has_value());
        co_await index.close();
    }
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, NonCanonicalSSTableArtifactFailsClosedAndIsPreserved) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        co_await index.close();
    }

    const std::string artifactPath = "shard_0/native_index/idx_1.sst";
    const std::string contents = "possible durable generation";
    writeWholeFile(artifactPath, contents);

    NativeIndex index(timestar::StorageLayout("."), 0);
    EXPECT_THROW(co_await index.open(), std::runtime_error);
    EXPECT_EQ(readWholeFile(artifactPath), contents);
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, SymlinkedOrphanSSTableFailsClosedWithoutTouchingTarget) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        co_await index.close();
    }

    const auto target = std::filesystem::absolute("native_index_orphan_target");
    const std::string contents = "must remain untouched";
    writeWholeFile(target.string(), contents);
    const std::string linkPath = "shard_0/native_index/idx_000042.sst";
    std::filesystem::create_symlink(target, linkPath);

    NativeIndex index(timestar::StorageLayout("."), 0);
    EXPECT_THROW(co_await index.open(), std::runtime_error);
    EXPECT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(linkPath)));
    EXPECT_EQ(readWholeFile(target.string()), contents);
    std::filesystem::remove(target);
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, StaleManifestTemporaryFileIsReclaimed) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        co_await index.close();
    }

    const std::string tempPath = "shard_0/native_index/MANIFEST.tmp";
    writeWholeFile(tempPath, "complete but unpublished snapshot attempt");

    NativeIndex index(timestar::StorageLayout("."), 0);
    co_await index.open();
    EXPECT_FALSE(std::filesystem::exists(tempPath));
    co_await index.close();
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, MissingManifestSSTableFailsClosed) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        (void)co_await index.getOrCreateSeriesId("missing_sst", {{"host", "a"}}, "value");
        co_await index.close();
    }

    const auto sstPath = NativeIndexFaultInjectionTest::currentSSTablePath();
    EXPECT_FALSE(sstPath.empty());
    if (sstPath.empty())
        co_return;
    std::filesystem::remove(sstPath);

    NativeIndex index(timestar::StorageLayout("."), 0);
    EXPECT_THROW(co_await index.open(), std::runtime_error);
    EXPECT_FALSE(std::filesystem::exists(sstPath));
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, SymlinkedManifestSSTableFailsClosed) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        (void)co_await index.getOrCreateSeriesId("symlink_sst", {{"host", "a"}}, "value");
        co_await index.close();
    }

    const auto sstPath = NativeIndexFaultInjectionTest::currentSSTablePath();
    EXPECT_FALSE(sstPath.empty());
    if (sstPath.empty())
        co_return;
    const auto savedPath = sstPath + ".saved";
    std::filesystem::rename(sstPath, savedPath);
    std::filesystem::create_symlink(std::filesystem::path(savedPath).filename(), sstPath);

    NativeIndex index(timestar::StorageLayout("."), 0);
    EXPECT_THROW(co_await index.open(), std::runtime_error);
    EXPECT_TRUE(std::filesystem::is_symlink(std::filesystem::symlink_status(sstPath)));
}

SEASTAR_TEST_F(NativeIndexFaultInjectionTest, SwappedManifestSSTableFailsMetadataBinding) {
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        (void)co_await index.getOrCreateSeriesId("first", {{"host", "a"}}, "value");
        co_await index.close();
    }
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        (void)co_await index.getOrCreateSeriesId("second_measurement_with_different_size",
                                                 {{"host", std::string(200, 'b')}}, "different_field");
        co_await index.close();
    }

    const auto paths = NativeIndexFaultInjectionTest::sstablePaths();
    EXPECT_EQ(paths.size(), 2u);
    if (paths.size() != 2)
        co_return;
    writeWholeFile(paths[0], readWholeFile(paths[1]));

    NativeIndex index(timestar::StorageLayout("."), 0);
    EXPECT_THROW(co_await index.open(), std::runtime_error);
    EXPECT_TRUE(std::filesystem::exists(paths[0]));
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
