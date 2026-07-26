// Regression gate for the READ-PATH CLOBBER in getSeriesGroupedByTag
// (write-scaleout debt D-2, filed as F5 in the Phase 5 review).
//
// THE DEFECT. `getSeriesGroupedByTag` decides which postings bitmaps are cache
// MISSES inside a `kvPrefixScan` callback, and loads them into `bitmapCache_`
// only after the whole scan finishes. The scan SUSPENDS (SSTable block reads),
// so between the miss decision and the load there is an arbitrarily long window
// in which another insert coroutine on the shard can create -- and DIRTY -- an
// entry for exactly that key. The load loop then did:
//
//     entry->bitmap = roaring::Roaring::readSafe(value.data(), value.size());
//     entry->dirty  = false;
//
// which overwrote the concurrent insert's local IDs (lost from memory) and
// cleared the flag that would have persisted them, so `flushDirtyBitmaps` also
// skipped the entry (lost from disk). A series silently vanished from every
// tag-filtered query, permanently -- the postings crash-window repair only
// covers local IDs at or above the persisted watermark, and these are below it.
//
// THE FIX is the merge-don't-assign rule the two read accessors
// (`getPostingsBitmapByKey` / `getDayBitmapByKey`) already follow: `|=`, and
// never touch `dirty`. A genuinely fresh entry is empty and clean, so the
// intended path is unchanged.
//
// WHAT THIS TEST DOES. It puts a long scan (thousands of persisted tag values)
// and a stream of inserts that reuse EARLY-SORTING tag values on the reactor at
// the same time, so many inserts land inside the window. Every such insert is
// then checked twice: in memory (the read must not have destroyed a completed
// write) and after a close+reopen (it must have been persisted).
#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/index/key_encoding.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/sleep.hh>
#include <string>
#include <vector>

using namespace timestar::index;
namespace ke = timestar::index::keys;

namespace {

class PostingsReadClobberTest : public ::testing::Test {
public:
    void SetUp() override {
        std::filesystem::remove_all("shard_0/native_index");
        savedConfig_ = timestar::config();
        auto cfg = savedConfig_;
        // Small enough that the fixture data lands in SSTables (so the scan does
        // real block reads and therefore really suspends), large enough that the
        // flush cadence is not itself the thing under test.
        cfg.index.write_buffer_size = 64 * 1024;
        timestar::setGlobalConfig(cfg);
    }
    void TearDown() override {
        timestar::setGlobalConfig(savedConfig_);
        std::filesystem::remove_all("shard_0/native_index");
    }
    timestar::TimestarConfig savedConfig_;
};

constexpr const char* kMeasurement = "clobber";
constexpr int kTargets = 200;   // tag values an insert will re-use during the scan
constexpr int kFillers = 4000;  // tag values that only make the scan long

// Targets sort BEFORE fillers, so their scan callbacks (and hence their
// miss decisions) fire in the first block the scan reads.
std::string targetValue(int i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "a_%04d", i);
    return buf;
}
std::string fillerValue(int i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "z_%05d", i);
    return buf;
}

TimeStarInsert<double> makeInsert(const std::string& host, const std::string& gen, uint64_t ts) {
    TimeStarInsert<double> insert(kMeasurement, "value");
    insert.tags = {{"host", host}, {"gen", gen}};
    insert.timestamps = {ts};
    insert.values = {1.0};
    return insert;
}

}  // namespace

SEASTAR_TEST_F(PostingsReadClobberTest, GroupedByTagLoadDoesNotClobberAConcurrentInsert) {
    const uint64_t ts = 21000ULL * ke::NS_PER_DAY;

    // --- Fixture: every tag value has a PERSISTED postings bitmap, and the
    // bitmap cache is cold (fresh reopen), so the scan classifies all of them as
    // misses and defers their load to the loop under test.
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        std::vector<TimeStarInsert<double>> inserts;
        inserts.reserve(kTargets + kFillers);
        for (int i = 0; i < kTargets; ++i)
            inserts.push_back(makeInsert(targetValue(i), "1", ts));
        for (int i = 0; i < kFillers; ++i)
            inserts.push_back(makeInsert(fillerValue(i), "1", ts));
        for (const auto& ins : inserts)
            co_await index.indexInsert(ins);
        co_await index.close();
    }

    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();

        // Start the long scan, then let it get past its first block read (and
        // therefore past the target values' miss decisions) before any insert
        // creates a cache entry for one of them.
        auto scan = index.getSeriesGroupedByTag(kMeasurement, "host");
        co_await seastar::sleep(std::chrono::microseconds(200));

        // A second series per target tag value, all in flight at once so they
        // create their (dirty) cache entries while the scan is still running --
        // i.e. after the miss decision and before the batch load.
        std::vector<TimeStarInsert<double>> second;
        second.reserve(kTargets);
        for (int i = 0; i < kTargets; ++i)
            second.push_back(makeInsert(targetValue(i), "2", ts));
        std::vector<seastar::future<SeriesId128>> pending;
        pending.reserve(second.size());
        for (const auto& ins : second)
            pending.push_back(index.indexInsert(ins));
        co_await seastar::when_all_succeed(pending.begin(), pending.end());

        co_await std::move(scan);

        // (1) IN MEMORY. Each insert above completed before this read, so the
        // read must see it. Pre-fix the scan's load loop had assigned the
        // persisted bitmap over the entry the insert had just dirtied.
        size_t lostInMemory = 0;
        for (int i = 0; i < kTargets; ++i) {
            auto ids = co_await index.findSeriesByTag(kMeasurement, "host", targetValue(i));
            if (ids.size() < 2)
                ++lostInMemory;
        }
        EXPECT_EQ(lostInMemory, 0u) << lostInMemory << " of " << kTargets
                                    << " tag values lost a completed insert's local ID: getSeriesGroupedByTag's "
                                    << "batch load ASSIGNED over a cache entry a concurrent insert had created and "
                                    << "dirtied during the scan's suspension (write-scaleout debt D-2)";
        co_await index.close();
    }

    // (2) ON DISK. Clearing `dirty` was the other half: flushDirtyBitmaps then
    // skipped the entry, so even the adds that survived in memory were never
    // written.
    {
        NativeIndex index(timestar::StorageLayout("."), 0);
        co_await index.open();
        size_t lostOnDisk = 0;
        for (int i = 0; i < kTargets; ++i) {
            auto ids = co_await index.findSeriesByTag(kMeasurement, "host", targetValue(i));
            if (ids.size() < 2)
                ++lostOnDisk;
        }
        EXPECT_EQ(lostOnDisk, 0u) << lostOnDisk << " of " << kTargets
                                  << " tag values did not persist a completed insert: the read path "
                                  << "cleared the dirty flag that flushDirtyBitmaps needs (debt D-2)";
        co_await index.close();
    }
}
