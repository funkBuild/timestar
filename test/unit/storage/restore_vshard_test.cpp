#include "../../../lib/storage/restore_vshard.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/vshard_snapshot_builder.hpp"
#include "../../../lib/storage/vshard_snapshot_read.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

namespace {

class RestoreVShardTest : public ::testing::Test {
protected:
    std::string dir = "test_restore_vshard";
    void SetUp() override {
        fs::create_directories(dir + "/src");
        fs::create_directories(dir + "/dst");
    }
    void TearDown() override { fs::remove_all(dir); }
};

seastar::future<seastar::shared_ptr<::TSM>> writeSrc(std::string p, uint64_t seq, const std::string& key,
                                                     std::vector<uint64_t> ts, std::vector<double> vs) {
    {
        TSMWriter w(p);
        w.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey(key), ts, vs);
        w.writeIndex();
        w.close();
    }
    auto tsm = seastar::make_shared<::TSM>(p);
    tsm->tierNum = 0;
    tsm->seqNum = seq;
    co_await tsm->open();
    co_await tsm->readSparseIndex();
    co_return tsm;
}

seastar::future<> testRestoreVerifiesAndInstalls(std::string dir) {
    const std::string key = "r,loc=x v";
    const auto s = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(s)};

    std::vector<uint64_t> ts = {100, 200};
    std::vector<double> vs = {1.0, 2.0};
    auto src = co_await writeSrc(dir + "/src/00_0000000000.tsm", 0, key, ts, vs);
    std::vector<seastar::shared_ptr<::TSM>> files = {src};

    // Build the snapshot manifest from the source file.
    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, files, builder);
    const auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));

    const std::string target = dir + "/dst/00_0000000000.tsm";

    // A tampered manifest must be REJECTED and install nothing.
    auto bad = manifest;
    bad.verificationHash = std::string(32, 'f');
    const bool badOk = co_await timestar::restoreVShardSnapshot(bad, files, {target});
    EXPECT_FALSE(badOk);
    EXPECT_FALSE(fs::exists(target)) << "a rejected restore must install no files";

    // The correct manifest installs the file.
    const bool ok = co_await timestar::restoreVShardSnapshot(manifest, files, {target});
    EXPECT_TRUE(ok);
    EXPECT_TRUE(fs::exists(target)) << "a verified restore installs the file";
    co_await src->close();

    // The installed file is a valid TSM reproducing the data.
    auto restored = seastar::make_shared<::TSM>(target);
    co_await restored->open();
    co_await restored->readSparseIndex();
    TSMResult<double> r(0);
    co_await restored->readSeries<double>(s, 0, UINT64_MAX, r);
    auto [rts, rvs] = r.getAllData();
    EXPECT_EQ(rts, (std::vector<uint64_t>{100, 200}));
    EXPECT_EQ(rvs, (std::vector<double>{1.0, 2.0}));
    co_await restored->close();
    co_return;
}
TEST_F(RestoreVShardTest, VerifiesAndInstalls) {
    seastar::async([&] { testRestoreVerifiesAndInstalls(dir).get(); }).get();
}

}  // namespace
