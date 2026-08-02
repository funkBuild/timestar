#include "../../../lib/storage/compact_vshard.hpp"

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

class CompactVShardTest : public ::testing::Test {
protected:
    std::string dir = "test_compact_vshard";
    void SetUp() override { fs::create_directories(dir); }
    void TearDown() override { fs::remove_all(dir); }
};

// Write a single float series (with revisions) to a v1 file and open it.
seastar::future<seastar::shared_ptr<::TSM>> writeRevFile(std::string p, uint64_t seq, const std::string& key,
                                                         std::vector<uint64_t> ts, std::vector<double> vs,
                                                         std::vector<uint64_t> revs) {
    {
        TSMWriter writer(p);
        writer.writeSeries(TSMValueType::Float, SeriesId128::fromSeriesKey(key), ts, vs, revs);
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

seastar::future<> testCompactPreservesRevisionRange(std::string dir) {
    const std::string key = "a,loc=west v";
    const auto s = SeriesId128::fromSeriesKey(key);
    const timestar::VShardId vshard{timestar::virtualShard(s)};

    std::vector<uint64_t> t0 = {100, 200}, r0 = {5, 6};
    std::vector<double> v0 = {1.0, 2.0};
    auto f0 = co_await writeRevFile(dir + "/00_0000000000.tsm", 0, key, t0, v0, r0);
    std::vector<uint64_t> t1 = {200, 300}, r1 = {60, 61};
    std::vector<double> v1 = {22.0, 3.0};
    auto f1 = co_await writeRevFile(dir + "/00_0000000001.tsm", 1, key, t1, v1, r1);

    const std::string out = dir + "/01_0000000000.tsm";
    const size_t n = co_await timestar::compactVShardToFile(vshard, {f0, f1}, out);
    EXPECT_EQ(n, 1u);
    co_await f0->close();
    co_await f1->close();

    auto tsm = seastar::make_shared<::TSM>(out);
    co_await tsm->open();
    co_await tsm->readSparseIndex();
    // Revisions PRESERVED (union max 61), unlike migration which floors to 0.
    EXPECT_EQ(tsm->maxRevision(), 61u) << "compaction must preserve the revision range max";
    // LWW-resolved data (newest wins at ts 200).
    TSMResult<double> r(0);
    co_await tsm->readSeries<double>(s, 0, UINT64_MAX, r);
    auto [rts, rvs] = r.getAllData();
    EXPECT_EQ(rts, (std::vector<uint64_t>{100, 200, 300}));
    EXPECT_EQ(rvs, (std::vector<double>{1.0, 22.0, 3.0}));
    co_await tsm->close();
    co_return;
}
TEST_F(CompactVShardTest, CompactPreservesRevisionRange) {
    seastar::async([&] { testCompactPreservesRevisionRange(dir).get(); }).get();
}

seastar::future<> testPartitionByVShardIsPure(std::string dir) {
    const std::string keyA = "a,loc=west v";
    const std::string keyB = "b,host=h9 v";
    const auto sA = SeriesId128::fromSeriesKey(keyA);
    const auto sB = SeriesId128::fromSeriesKey(keyB);
    const uint16_t vA = timestar::virtualShard(sA);
    const uint16_t vB = timestar::virtualShard(sB);
    const size_t expectedFiles = (vA == vB) ? 1u : 2u;  // rare collision -> both in one file

    std::vector<uint64_t> ta = {100}, ra = {5};
    std::vector<double> va = {1.0};
    auto fA = co_await writeRevFile(dir + "/00_0000000000.tsm", 0, keyA, ta, va, ra);
    std::vector<uint64_t> tb = {100}, rb = {9};
    std::vector<double> vb = {2.0};
    auto fB = co_await writeRevFile(dir + "/00_0000000001.tsm", 1, keyB, tb, vb, rb);

    // Output files must follow the TSM tier_seq.tsm convention (use the vshard as
    // the sequence number, tier 9 to distinguish partitioned output).
    auto pathFor = [&](timestar::VShardId vs) {
        char name[64];
        snprintf(name, sizeof(name), "/09_%010u.tsm", static_cast<unsigned>(vs.value()));
        return dir + name;
    };
    auto parts = co_await timestar::partitionByVShard({fA, fB}, pathFor);
    co_await fA->close();
    co_await fB->close();

    EXPECT_EQ(parts.size(), expectedFiles) << "one pure file per VShard";
    for (const auto& [vs, path] : parts) {
        auto tsm = seastar::make_shared<::TSM>(path);
        co_await tsm->open();
        co_await tsm->readSparseIndex();
        // Every series in the file belongs to this file's VShard (VShard-pure).
        for (const auto& sid : tsm->getSeriesIds())
            EXPECT_EQ(timestar::virtualShard(sid), vs.value()) << "file for vshard " << vs.value() << " is not pure";
        co_await tsm->close();
    }
    co_return;
}
TEST_F(CompactVShardTest, PartitionByVShardIsPure) {
    seastar::async([&] { testPartitionByVShardIsPure(dir).get(); }).get();
}

}  // namespace
