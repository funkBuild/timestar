#include "../../../lib/storage/migrate_vshard.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_result.hpp"
#include "../../../lib/storage/tsm_writer.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class MigrateVShardTest : public ::testing::Test {
protected:
    std::string dir = "test_migrate_vshard";
    void SetUp() override { fs::create_directories(dir); }
    void TearDown() override { fs::remove_all(dir); }
};

struct SeriesData {
    std::string key;
    std::vector<uint64_t> ts;
    std::vector<double> vs;
};

seastar::future<seastar::shared_ptr<::TSM>> writeFile(std::string p, uint64_t seq,
                                                      const std::vector<SeriesData>& series) {
    {
        TSMWriter writer(p);
        for (const auto& s : series) {
            std::vector<uint64_t> ts = s.ts;
            std::vector<double> vs = s.vs;
            writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey(s.key), ts, vs);
        }
        writer.writeIndex();
        writer.close();
    }
    auto tsm = seastar::make_shared<::TSM>(p);
    tsm->tierNum = 0;
    tsm->seqNum = seq;
    co_await tsm->open();
    co_await tsm->readSparseIndex();
    co_return tsm;
}

seastar::future<> testMigrateResolvesAndFloorsRevisions(std::string dir) {
    const std::string keyA = "a,loc=west v";
    const std::string keyB = "b,host=h9 v";
    const auto sA = SeriesId128::fromSeriesKey(keyA);
    const auto sB = SeriesId128::fromSeriesKey(keyB);
    const uint16_t vA = timestar::virtualShard(sA);
    const uint16_t vB = timestar::virtualShard(sB);

    // File 0 (older): A={100->1, 200->2}, B (a different series) lives here too.
    std::vector<SeriesData> file0;
    file0.push_back(SeriesData{keyA, {100, 200}, {1.0, 2.0}});
    file0.push_back(SeriesData{keyB, {100}, {10.0}});
    auto f0 = co_await writeFile(dir + "/00_0000000000.tsm", 0, file0);
    // File 1 (newer): A={200->22, 300->3}.
    std::vector<SeriesData> file1;
    file1.push_back(SeriesData{keyA, {200, 300}, {22.0, 3.0}});
    auto f1 = co_await writeFile(dir + "/00_0000000001.tsm", 1, file1);

    const std::string outPath = dir + "/01_0000000000.tsm";
    const size_t written = co_await timestar::migrateVShardToFile(timestar::VShardId{vA}, {f0, f1}, outPath);

    co_await f0->close();
    co_await f1->close();

    // Read the migrated output.
    auto out = seastar::make_shared<::TSM>(outPath);
    co_await out->open();
    co_await out->readSparseIndex();

    // Migrated data is at the migrated floor -- no revisions written.
    EXPECT_EQ(out->fileFormatVersion(), 4u);
    EXPECT_EQ(out->maxRevision(), 0u) << "migrated data must sit at the migrated-floor revision 0";

    // Series A is present with the LWW-resolved view (newest wins at ts 200).
    TSMResult<double> ra(0);
    co_await out->readSeries<double>(sA, 0, UINT64_MAX, ra);
    auto [ats, avs] = ra.getAllData();
    EXPECT_EQ(ats, (std::vector<uint64_t>{100, 200, 300}));
    EXPECT_EQ(avs, (std::vector<double>{1.0, 22.0, 3.0}));

    // Only VShard-vA series are migrated. If B maps to a different VShard it must
    // be absent; the written count matches the vA series in the sources.
    if (vB != vA) {
        EXPECT_EQ(written, 1u) << "only series A (VShard vA) migrated";
        EXPECT_FALSE(out->getSeriesType(sB).has_value()) << "series B (other VShard) must not be migrated";
    } else {
        EXPECT_EQ(written, 2u);  // rare hash collision: both in vA
    }

    co_await out->close();
    co_return;
}
TEST_F(MigrateVShardTest, MigrateResolvesAndFloorsRevisions) {
    seastar::async([&] { testMigrateResolvesAndFloorsRevisions(dir).get(); }).get();
}

// A type-flipped series (same key, different value type across files) must keep
// only its NEWEST-type data and must NOT abort the migration with a block
// type-mismatch throw (adversarial-review F1).
seastar::future<> testTypeFlipKeepsNewestTypeNoThrow(std::string dir) {
    const std::string key = "flip,loc=x v";
    const auto s = SeriesId128::fromSeriesKey(key);
    const uint16_t vs = timestar::virtualShard(s);

    // Older file: series as FLOAT.
    std::vector<SeriesData> floatFile;
    floatFile.push_back(SeriesData{key, {100}, {1.0}});
    seastar::shared_ptr<::TSM> f0 = co_await writeFile(dir + "/00_0000000000.tsm", 0, floatFile);
    // Newer file: SAME series key as INTEGER (a corrupt type flip).
    {
        TSMWriter w(dir + "/00_0000000001.tsm");
        std::vector<uint64_t> ts = {200};
        std::vector<int64_t> vs2 = {7};
        w.writeSeries(TSMValueType::Integer, s, ts, vs2);
        w.writeIndex();
        w.close();
    }
    auto f1 = seastar::make_shared<::TSM>(dir + "/00_0000000001.tsm");
    f1->tierNum = 0;
    f1->seqNum = 1;
    co_await f1->open();
    co_await f1->readSparseIndex();

    const std::string outPath = dir + "/01_0000000000.tsm";
    // co_await directly: a block type-mismatch throw would propagate and fail the
    // test -- so reaching the assertion below IS the no-throw guarantee.
    const size_t written = co_await timestar::migrateVShardToFile(timestar::VShardId{vs}, {f0, f1}, outPath);
    EXPECT_EQ(written, 1u);

    co_await f0->close();
    co_await f1->close();

    auto out = seastar::make_shared<::TSM>(outPath);
    co_await out->open();
    co_await out->readSparseIndex();
    // Newest type (Integer) wins; the older Float generation is dropped.
    auto t = out->getSeriesType(s);
    EXPECT_TRUE(t.has_value() && *t == TSMValueType::Integer) << "newest-type (Integer) must win the flip";
    TSMResult<int64_t> r(0);
    co_await out->readSeries<int64_t>(s, 0, UINT64_MAX, r);
    auto [rts, rvs] = r.getAllData();
    EXPECT_EQ(rts, (std::vector<uint64_t>{200}));
    EXPECT_EQ(rvs, (std::vector<int64_t>{7}));
    co_await out->close();
    co_return;
}
TEST_F(MigrateVShardTest, TypeFlipKeepsNewestTypeNoThrow) {
    seastar::async([&] { testTypeFlipKeepsNewestTypeNoThrow(dir).get(); }).get();
}

}  // namespace
