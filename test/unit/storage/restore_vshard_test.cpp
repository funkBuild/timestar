#include "../../../lib/storage/restore_vshard.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/vshard_snapshot_builder.hpp"
#include "../../../lib/storage/vshard_snapshot_read.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
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

// A source file whose revision exceeds the manifest's snapshotRevision is not the
// data the manifest describes -> rejected, install nothing (review MEDIUM-LOW).
seastar::future<> testRestoreRejectsWatermarkMismatch(std::string dir) {
    const std::string key = "wm,loc=x v";
    const auto s = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(s)};

    // Source with a real revision (block maxRev = 50 -> file maxRevision 50).
    std::vector<uint64_t> ts = {100};
    std::vector<double> vs = {1.0};
    std::vector<uint64_t> revs = {50};
    seastar::shared_ptr<::TSM> src;
    {
        TSMWriter w(dir + "/src/00_0000000000.tsm");
        w.writeSeries(TSMValueType::Float, s, ts, vs, revs);
        w.writeIndex();
        w.close();
    }
    src = seastar::make_shared<::TSM>(dir + "/src/00_0000000000.tsm");
    src->tierNum = 0;
    src->seqNum = 0;
    co_await src->open();
    co_await src->readSparseIndex();
    std::vector<seastar::shared_ptr<::TSM>> files = {src};

    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, files, builder);
    auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));
    // Force the manifest watermark BELOW the source's revision (50).
    manifest.snapshotRevision = 10;

    const std::string target = dir + "/dst/00_0000000000.tsm";
    const bool ok = co_await timestar::restoreVShardSnapshot(manifest, files, {target});
    EXPECT_FALSE(ok) << "source revision beyond snapshotRevision must be rejected";
    EXPECT_FALSE(fs::exists(target));
    co_await src->close();
    co_return;
}
TEST_F(RestoreVShardTest, RejectsWatermarkMismatch) {
    seastar::async([&] { testRestoreRejectsWatermarkMismatch(dir).get(); }).get();
}

// All-or-nothing: if the second file's copy fails (its parent path is a FILE, not
// a dir), the first file must NOT be left installed (review HIGH).
seastar::future<> testRestoreAllOrNothingOnInstallFailure(std::string dir) {
    const std::string key = "aon,loc=x v";
    const auto s = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(s)};

    std::vector<uint64_t> ts = {100};
    std::vector<double> vs = {1.0};
    auto src0 = co_await writeSrc(dir + "/src/00_0000000000.tsm", 0, key, ts, vs);
    auto src1 = co_await writeSrc(dir + "/src/00_0000000001.tsm", 1, key, ts, vs);
    std::vector<seastar::shared_ptr<::TSM>> files = {src0, src1};

    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, files, builder);
    const auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));

    const std::string good = dir + "/dst/00_0000000000.tsm";
    // A regular file that blocks create_directories() for target1's parent.
    { std::ofstream(dir + "/dst/blocker") << "x"; }
    const std::string bad = dir + "/dst/blocker/00_0000000000.tsm";  // parent "blocker" is a file

    bool threw = false;
    try {
        co_await timestar::restoreVShardSnapshot(manifest, files, {good, bad});
    } catch (...) {
        threw = true;
    }
    EXPECT_TRUE(threw) << "an install I/O failure propagates";
    EXPECT_FALSE(fs::exists(good)) << "all-or-nothing: the first file must not be installed on a later copy failure";
    co_await src0->close();
    co_await src1->close();
    co_return;
}
TEST_F(RestoreVShardTest, AllOrNothingOnInstallFailure) {
    seastar::async([&] { testRestoreAllOrNothingOnInstallFailure(dir).get(); }).get();
}

}  // namespace
