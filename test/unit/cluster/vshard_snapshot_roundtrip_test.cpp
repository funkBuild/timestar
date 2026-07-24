// Integration M3 (snapshots): the Engine-level InstallSnapshot round trip.
// buildVShardSnapshotFiles on a source Engine (flushed to TSM) produces a
// self-contained (manifest + file bytes); installVShardSnapshotFiles on a FRESH
// Engine reproduces the data exactly. Corrupted bytes must fail verification and
// install NOTHING. Single-core (shard 0), so vshardsCohesiveOnCores(1) holds and
// the source's file set is the whole VShard.
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // vshardsCohesiveOnCores

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
namespace fs = std::filesystem;

namespace {

// Insert three points, flush to a TSM file, and return the VShard + the built
// snapshot (manifest + file bytes).
std::pair<VShardSnapshotManifest, std::vector<std::pair<std::string, std::string>>>
buildSourceSnapshot(Engine& src, VShardId* outVShard) {
    src.setRevisionAssignment(true);  // stamp revisions so snapshotRevision > 0

    TimeStarInsert<double> probe("snaprt", "value");
    probe.addTag("host", "h1");
    const SeriesId128 sid = probe.seriesId128();
    *outVShard = VShardId{virtualShard(sid)};

    {
        TimeStarInsert<double> ins("snaprt", "value");
        ins.addTag("host", "h1");
        ins.addValue(1000, 1.5);
        ins.addValue(2000, 2.5);
        ins.addValue(3000, 3.5);
        src.insert(std::move(ins)).get();
    }
    src.rolloverMemoryStore().get();
    for (int i = 0; i < 300 && src.getTSMFileCount() == 0; ++i)
        seastar::sleep(std::chrono::milliseconds(100)).get();
    EXPECT_GT(src.getTSMFileCount(), 0u) << "flush did not produce a TSM file";

    return src.buildVShardSnapshotFiles(*outVShard, std::string(32, '0')).get();
}

}  // namespace

TEST(VShardSnapshotRoundtrip, InstallReproducesSourceData) {
    seastar::thread([] {
        ASSERT_TRUE(vshardsCohesiveOnCores(seastar::smp::count))
            << "this test assumes a cohesive core count (run under a power-of-two -c)";
        fs::remove_all("snaprt_src");
        fs::remove_all("snaprt_dst");

        Engine src(StorageLayout("snaprt_src").anchored());
        src.init().get();
        VShardId vshard{0};
        auto [manifest, files] = buildSourceSnapshot(src, &vshard);
        EXPECT_TRUE(manifest.valid());
        EXPECT_EQ(manifest.vshard, vshard);
        ASSERT_FALSE(files.empty()) << "the flushed file must be shipped";

        // Install into a fresh Engine and read the data back.
        Engine dst(StorageLayout("snaprt_dst").anchored());
        dst.init().get();
        const bool ok = dst.installVShardSnapshotFiles(manifest, files).get();
        EXPECT_TRUE(ok);

        // A snapshot ships DATA files, not index entries (catalog reconciliation is a
        // separate schema-broadcast mechanism), so verify the installed data by the
        // pre-computed seriesId -- the identity the data plane carries in its commands.
        TimeStarInsert<double> keyProbe("snaprt", "value");
        keyProbe.addTag("host", "h1");
        const SeriesId128 sid = keyProbe.seriesId128();
        auto resultOpt = dst.query("snaprt,host=h1 value", sid, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps, (std::vector<uint64_t>{1000, 2000, 3000}));
        ASSERT_EQ(result.values.size(), 3u);
        EXPECT_DOUBLE_EQ(result.values[0], 1.5);
        EXPECT_DOUBLE_EQ(result.values[1], 2.5);
        EXPECT_DOUBLE_EQ(result.values[2], 3.5);

        src.stop().get();
        dst.stop().get();
        fs::remove_all("snaprt_src");
        fs::remove_all("snaprt_dst");
    })
        .join()
        .get();
}

TEST(VShardSnapshotRoundtrip, DamagedFileFailsRestoreAndInstallsNothing) {
    seastar::thread([] {
        ASSERT_TRUE(vshardsCohesiveOnCores(seastar::smp::count));
        fs::remove_all("snaprt_src2");
        fs::remove_all("snaprt_dst2");

        Engine src(StorageLayout("snaprt_src2").anchored());
        src.init().get();
        VShardId vshard{0};
        auto [manifest, files] = buildSourceSnapshot(src, &vshard);
        ASSERT_FALSE(files.empty());

        // Damage the shipped file (truncate to half): restore must reject it (a short
        // file fails to open or fails the resolved-view verification), so the install
        // either returns false or throws -- but must NEVER install partial data.
        auto corrupt = files;
        corrupt[0].second.resize(corrupt[0].second.size() / 2);

        Engine dst(StorageLayout("snaprt_dst2").anchored());
        dst.init().get();
        bool installed = true;
        try {
            installed = dst.installVShardSnapshotFiles(manifest, corrupt).get();
        } catch (...) {
            installed = false;  // a corrupt file may fail to open -> throw; also "not installed"
        }
        EXPECT_FALSE(installed) << "corrupted snapshot bytes must not install";

        // Whichever way it failed, the destination holds no data for the series.
        TimeStarInsert<double> keyProbe("snaprt", "value");
        keyProbe.addTag("host", "h1");
        const SeriesId128 sid = keyProbe.seriesId128();
        auto resultOpt = dst.query("snaprt,host=h1 value", sid, 0, UINT64_MAX).get();
        const bool empty = !resultOpt.has_value() ||
                           std::get<QueryResult<double>>(resultOpt.value()).timestamps.empty();
        EXPECT_TRUE(empty) << "a failed snapshot install must leave no partial data";

        src.stop().get();
        dst.stop().get();
        fs::remove_all("snaprt_src2");
        fs::remove_all("snaprt_dst2");
    })
        .join()
        .get();
}
