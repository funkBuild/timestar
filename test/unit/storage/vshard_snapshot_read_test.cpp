#include "../../../lib/storage/vshard_snapshot_read.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/vshard_extent_map.hpp"
#include "../../../lib/storage/vshard_snapshot_builder.hpp"
#include "../../../lib/storage/vshard_verification_hash.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class VShardSnapshotReadTest : public ::testing::Test {
protected:
    std::string dir = "test_vshard_snapshot_read";
    void SetUp() override { fs::create_directories(dir); }
    void TearDown() override { fs::remove_all(dir); }
    std::string path(const std::string& f) { return dir + "/" + f; }
};

seastar::future<> writeFloatFile(std::string p, uint64_t seq, const std::string& seriesKey, std::vector<uint64_t> ts,
                                 std::vector<double> vs, seastar::shared_ptr<::TSM>& out) {
    {
        TSMWriter writer(p);
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey(seriesKey), ts, vs);
        writer.writeIndex();
        writer.close();
    }
    out = seastar::make_shared<::TSM>(p);
    out->tierNum = 0;
    out->seqNum = seq;
    co_await out->open();
    co_await out->readSparseIndex();
    co_return;
}

// Two files hold the same series with an overlapping timestamp; the newer file
// (higher dataRank) must win at that timestamp in the resolved view, so the
// snapshot's verification hash matches an independent hash of the LWW result.
seastar::future<> testResolvedViewIsNewestWins(std::string dir) {
    const std::string key = "temp,loc=west v";
    const auto series = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(series)};

    seastar::shared_ptr<::TSM> older, newer;
    co_await writeFloatFile(dir + "/00_0000000000.tsm", 0, key, {100, 200}, {1.0, 2.0}, older);
    co_await writeFloatFile(dir + "/00_0000000001.tsm", 1, key, {200, 300}, {22.0, 3.0}, newer);

    timestar::VShardSnapshotBuilder builder(vshard);
    // Pass files in the "wrong" (oldest-first) order to prove the reader sorts.
    co_await timestar::feedVShardResolvedView(vshard, {older, newer}, builder);
    const auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));

    // Expected resolved view: newest file wins at ts 200 (22.0).
    timestar::VShardVerificationHash expected;
    expected.addFloat(series, 100, 1.0);
    expected.addFloat(series, 200, 22.0);
    expected.addFloat(series, 300, 3.0);
    EXPECT_EQ(manifest.verificationHash, expected.digestHex()) << "resolved view must be newest-wins LWW";

    co_await older->close();
    co_await newer->close();
    co_return;
}
TEST_F(VShardSnapshotReadTest, ResolvedViewIsNewestWins) {
    seastar::async([&] { testResolvedViewIsNewestWins(dir).get(); }).get();
}

}  // namespace
