// WAL group commit (wal_sync_mode): an acknowledged write must be durable.
//
// WHY THIS TEST EXISTS
//
// Before the Jul 22 2026 fix, WAL::insert/insertBatch acknowledged writes
// from seastar's 256KiB output_stream buffer, which drains only when FULL —
// no flush timer, fdatasync only at rollover/close.  A SIGKILL therefore
// lost the buffered tail of ACKED writes, and because the window was BYTES
// (not time or points), RLE-compressed bool series lost 10-100x more points
// than float series, and a slow shard held acked data volatile indefinitely
// (measured: 1,000 acked points still 100% volatile after 60 seconds idle).
//
// The contract pinned here: in the default mode ("always"), the moment an
// insert future resolves, its bytes are ON DISK — readable by a WALReader
// opening the file independently, with NO close() and NO explicit flush.
// The legacy behaviour is preserved behind wal_sync_mode="rollover", and
// this test also pins the hazard it carries, so the difference between the
// modes stays visible and deliberate.

#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>

namespace fs = std::filesystem;

namespace {

// Build a small float insert: a few hundred encoded bytes — far below the
// 256KiB stream buffer, so nothing here would reach disk without an
// explicit durability mechanism.
TimeStarInsert<double> smallInsert(const std::string& measurement, uint64_t tsBase, size_t points) {
    TimeStarInsert<double> ins(measurement, "v");
    for (size_t i = 0; i < points; ++i) {
        ins.addValue(tsBase + i * 1'000'000'000ULL, static_cast<double>(i) * 0.5);
    }
    return ins;
}

// Count the points a fresh WALReader can recover from the file RIGHT NOW —
// the file is opened independently by name, exactly like crash recovery
// would, while the writing WAL object stays open.
seastar::future<size_t> pointsOnDisk(unsigned int walSeq, const SeriesId128& seriesId) {
    auto store = std::make_shared<MemoryStore>(9999);
    WALReader reader(WAL::sequenceNumberToFilename(walSeq));
    co_await reader.readAll(store.get());
    auto* result = store->querySeries<double>(seriesId);
    co_return result ? result->timestamps.size() : 0;
}

}  // namespace

class WALGroupCommitTest : public ::testing::Test {
public:
    timestar::TimestarConfig savedConfig_;

    void SetUp() override {
        savedConfig_ = timestar::config();
        cleanTestShardDirectories();
    }
    void TearDown() override {
        timestar::setGlobalConfig(savedConfig_);
        cleanTestShardDirectories();
    }

    void setSyncMode(const std::string& mode, uint32_t intervalMs = 100) {
        auto cfg = savedConfig_;
        cfg.storage.wal_sync_mode = mode;
        cfg.storage.wal_sync_interval_ms = intervalMs;
        timestar::setGlobalConfig(cfg);
    }
};

// Default mode: the ack IS the durability point.  No close, no flush — the
// insert future resolving must mean an independent reader sees the data.
SEASTAR_TEST_F(WALGroupCommitTest, AlwaysModeAckImpliesOnDisk) {
    self->setSyncMode("always");
    auto store = std::make_shared<MemoryStore>(4200);
    WAL wal(4200);
    co_await wal.init(store.get());

    auto ins = smallInsert("gc_always", 1'700'000'000'000'000'000ULL, 100);
    const SeriesId128 id = ins.seriesId128();
    auto result = co_await wal.insert(ins);
    EXPECT_EQ(result, WALInsertResult::Success);

    const size_t recovered = co_await pointsOnDisk(4200, id);
    EXPECT_EQ(recovered, 100u) << "acked write was not durable — the ack must imply bytes on disk";

    co_await wal.close();
}

// Group commit round-sharing: many concurrent writers, every ack durable.
SEASTAR_TEST_F(WALGroupCommitTest, AlwaysModeConcurrentWritersAllDurable) {
    self->setSyncMode("always");
    auto store = std::make_shared<MemoryStore>(4201);
    WAL wal(4201);
    co_await wal.init(store.get());

    constexpr size_t kWriters = 32;
    constexpr size_t kPointsEach = 50;
    std::vector<TimeStarInsert<double>> inserts;
    std::vector<SeriesId128> ids;
    for (size_t w = 0; w < kWriters; ++w) {
        inserts.push_back(smallInsert("gc_conc_" + std::to_string(w), 1'700'000'000'000'000'000ULL, kPointsEach));
        ids.push_back(inserts.back().seriesId128());
    }
    std::vector<seastar::future<WALInsertResult>> futs;
    for (auto& ins : inserts) {
        futs.push_back(wal.insert(ins));
    }
    auto results = co_await seastar::when_all_succeed(futs.begin(), futs.end());
    for (auto r : results) {
        EXPECT_EQ(r, WALInsertResult::Success);
    }

    // Every acked writer's data must be independently recoverable.
    auto probe = std::make_shared<MemoryStore>(9998);
    WALReader reader(WAL::sequenceNumberToFilename(4201));
    co_await reader.readAll(probe.get());
    for (size_t w = 0; w < kWriters; ++w) {
        const auto* res = probe->querySeries<double>(ids[w]);
        if (res == nullptr) {
            ADD_FAILURE() << "writer " << w << " acked but not on disk";
            continue;
        }
        EXPECT_EQ(res->timestamps.size(), kPointsEach) << "writer " << w;
    }

    co_await wal.close();
}

// Legacy mode: pins the hazard the default fixes — a small acked write stays
// in the stream buffer (invisible to recovery) until rollover/close.  If
// this test ever starts failing because the data IS on disk, "rollover" has
// silently stopped meaning what its documentation says.
SEASTAR_TEST_F(WALGroupCommitTest, RolloverModeKeepsSmallAckedWritesVolatile) {
    self->setSyncMode("rollover");
    auto store = std::make_shared<MemoryStore>(4202);
    WAL wal(4202);
    co_await wal.init(store.get());

    auto ins = smallInsert("gc_legacy", 1'700'000'000'000'000'000ULL, 100);
    const SeriesId128 id = ins.seriesId128();
    auto result = co_await wal.insert(ins);
    EXPECT_EQ(result, WALInsertResult::Success);

    EXPECT_EQ(co_await pointsOnDisk(4202, id), 0u)
        << "rollover mode unexpectedly flushed a small write — mode wiring is broken";

    // Clean close drains the buffer: now it must be recoverable.
    co_await wal.close();
    EXPECT_EQ(co_await pointsOnDisk(4202, id), 100u);
}

// Interval mode: ack-immediate, but the periodic round bounds the volatile
// window in TIME — the data must be on disk well within a few intervals.
SEASTAR_TEST_F(WALGroupCommitTest, IntervalModeFlushesWithinTheWindow) {
    self->setSyncMode("interval", /*intervalMs=*/25);
    auto store = std::make_shared<MemoryStore>(4203);
    WAL wal(4203);
    co_await wal.init(store.get());

    auto ins = smallInsert("gc_interval", 1'700'000'000'000'000'000ULL, 100);
    const SeriesId128 id = ins.seriesId128();
    auto result = co_await wal.insert(ins);
    EXPECT_EQ(result, WALInsertResult::Success);

    // Poll rather than sleep-once: timers on a loaded CI box can lag.
    size_t recovered = 0;
    for (int attempt = 0; attempt < 80 && recovered == 0; ++attempt) {
        co_await seastar::sleep(std::chrono::milliseconds(25));
        recovered = co_await pointsOnDisk(4203, id);
    }
    EXPECT_EQ(recovered, 100u) << "interval mode never flushed within ~2s of a 25ms window";

    co_await wal.close();
}
