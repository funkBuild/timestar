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

// Intra-file duplicate timestamps must resolve LAST-write-wins (keep-last),
// matching the query path -- NOT keep-first. Regression for the emplace bug.
seastar::future<> testIntraFileDuplicateKeepsLast(std::string dir) {
    const std::string key = "dup,loc=x v";
    const auto series = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(series)};

    // One file whose series has a duplicate at ts 200: the later position (9.0)
    // is the newer write and must win.
    seastar::shared_ptr<::TSM> file;
    co_await writeFloatFile(dir + "/00_0000000000.tsm", 0, key, {100, 200, 200, 300}, {1.0, 5.0, 9.0, 3.0}, file);

    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, {file}, builder);
    const auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));

    timestar::VShardVerificationHash expected;
    expected.addFloat(series, 100, 1.0);
    expected.addFloat(series, 200, 9.0);  // keep-last, not 5.0
    expected.addFloat(series, 300, 3.0);
    EXPECT_EQ(manifest.verificationHash, expected.digestHex()) << "intra-file duplicate must keep the last write";

    co_await file->close();
    co_return;
}
TEST_F(VShardSnapshotReadTest, IntraFileDuplicateKeepsLast) {
    seastar::async([&] { testIntraFileDuplicateKeepsLast(dir).get(); }).get();
}

// Restore verification: a snapshot created from files verifies against those same
// files (hash matches); a manifest with a different hash is rejected.
seastar::future<> testVerifyRoundTrip(std::string dir) {
    const std::string key = "verify,loc=x v";
    const auto series = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(series)};

    seastar::shared_ptr<::TSM> f0, f1;
    co_await writeFloatFile(dir + "/00_0000000000.tsm", 0, key, {100, 200}, {1.0, 2.0}, f0);
    co_await writeFloatFile(dir + "/00_0000000001.tsm", 1, key, {200, 300}, {22.0, 3.0}, f1);
    std::vector<seastar::shared_ptr<::TSM>> files = {f0, f1};

    // Create the snapshot manifest from these files.
    timestar::VShardSnapshotBuilder builder(vshard);
    co_await timestar::feedVShardResolvedView(vshard, files, builder);
    const auto manifest = builder.build(timestar::VShardExtentMap{}, std::string(32, 'c'));

    // Verifying against the SAME files must succeed.
    const bool ok = co_await timestar::verifyVShardSnapshot(manifest, files);
    EXPECT_TRUE(ok) << "a snapshot must verify against the files it was created from";

    // A manifest with a tampered hash must be rejected.
    auto tampered = manifest;
    tampered.verificationHash = std::string(32, 'f');  // wrong but valid-shaped
    const bool bad = co_await timestar::verifyVShardSnapshot(tampered, files);
    EXPECT_FALSE(bad) << "a mismatched verification hash must be rejected";

    co_await f0->close();
    co_await f1->close();
    co_return;
}
TEST_F(VShardSnapshotReadTest, VerifyRoundTripAndTamperRejected) {
    seastar::async([&] { testVerifyRoundTrip(dir).get(); }).get();
}

}  // namespace
