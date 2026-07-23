#include "../../../lib/storage/vshard_snapshot_extents.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/vshard_extent_map.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class VShardSnapshotExtentsTest : public ::testing::Test {
protected:
    std::string dir = "test_vshard_extents";
    void SetUp() override { fs::create_directories(dir); }
    void TearDown() override { fs::remove_all(dir); }
    std::string path(const std::string& f) { return dir + "/" + f; }
};

struct SeriesSpec {
    std::string key;
    std::vector<uint64_t> revs;
};

seastar::future<> testExtentsGroupByVShard(std::string p) {
    const std::vector<SeriesSpec> specs = {
        {"a,loc=west v", {5, 6, 7}},
        {"b,loc=east v", {40, 42, 41}},
        {"c,host=h1 v", {100, 100, 100}},
    };

    // Expected per-VShard revision range, derived from virtualShard() + revs.
    std::map<uint16_t, timestar::RevisionRange> expected;
    for (const auto& s : specs) {
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(s.key));
        for (uint64_t r : s.revs)
            expected[vs].merge(timestar::RevisionRange{r, r, /*empty=*/false});
    }

    {
        TSMWriter writer(p);
        for (const auto& s : specs) {
            std::vector<uint64_t> ts;
            std::vector<double> vs;
            for (size_t i = 0; i < s.revs.size(); ++i) {
                ts.push_back(1000 + i * 10);
                vs.push_back(static_cast<double>(i));
            }
            std::vector<uint64_t> revs = s.revs;
            writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey(s.key), ts, vs, revs);
        }
        writer.writeIndex();
        writer.close();
    }

    TSM tsm(p);
    co_await tsm.open();
    co_await tsm.readSparseIndex();

    timestar::VShardExtentMap map;
    co_await timestar::addTsmFileExtents(map, /*fileId=*/42, tsm);

    // Every expected VShard is present with the right union range and file id.
    std::vector<timestar::VShardId> gotVShards = map.vshards();
    EXPECT_EQ(gotVShards.size(), expected.size());
    for (const auto& [vs, range] : expected) {
        const timestar::VShardId id{vs};
        auto extents = map.extents(id);
        EXPECT_EQ(extents.size(), 1u) << "vshard " << vs << " should have exactly one file extent";
        if (extents.size() == 1u) {
            EXPECT_EQ(extents[0].fileId, 42u);
            EXPECT_EQ(extents[0].revRange.minRev, range.minRev) << "vshard " << vs;
            EXPECT_EQ(extents[0].revRange.maxRev, range.maxRev) << "vshard " << vs;
        }
    }
    co_return;
}
TEST_F(VShardSnapshotExtentsTest, ExtentsGroupByVShard) {
    seastar::async([&] { testExtentsGroupByVShard(path("0_1.tsm")).get(); }).get();
}

}  // namespace
