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
buildSourceSnapshot(Engine& src, VShardId* outVShard, SeriesId128* outOtherSid = nullptr) {
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

    // Put an unrelated VShard in the SAME memstore/TSM. Tier-0 files are
    // intentionally multiplexed; the snapshot producer must extract only the
    // requested VShard instead of copying this raw source file wholesale.
    for (int i = 0;; ++i) {
        TimeStarInsert<double> other("snaprt_other", "value");
        other.addTag("host", "h" + std::to_string(i));
        const auto otherSid = other.seriesId128();
        if (virtualShard(otherSid) == outVShard->value())
            continue;
        other.addValue(1000, 99.0);
        src.insert(std::move(other)).get();
        if (outOtherSid)
            *outOtherSid = otherSid;
        break;
    }
    src.rolloverMemoryStore().get();
    for (int i = 0; i < 300 && src.getTSMFileCount() == 0; ++i)
        seastar::sleep(std::chrono::milliseconds(100)).get();
    EXPECT_GT(src.getTSMFileCount(), 0u) << "flush did not produce a TSM file";

    // The payload does not carry tombstone sidecars. It must therefore
    // materialise their resolved effect into the shipped TSM; copying the raw
    // source bytes would either resurrect this point or fail verification on
    // the receiver.
    EXPECT_TRUE(src.deleteRange("snaprt,host=h1 value", 2000, 2000).get());

    auto built = src.buildVShardSnapshotFiles(*outVShard).get();
    return {std::move(built.manifest), std::move(built.files)};
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
        SeriesId128 otherSid;
        auto [manifest, files] = buildSourceSnapshot(src, &vshard, &otherSid);
        EXPECT_TRUE(manifest.valid());
        EXPECT_EQ(manifest.vshard, vshard);
        ASSERT_EQ(files.size(), 1u) << "a snapshot is one resolved VShard-pure object";

        // Install into a fresh Engine and read the data back.
        Engine dst(StorageLayout("snaprt_dst").anchored());
        dst.init().get();
        const bool ok = dst.installVShardSnapshotFiles(manifest, files).get();
        EXPECT_TRUE(ok);

        // This low-level Engine test installs only the data half of the bundle,
        // so verify it by precomputed id. EngineSnapshotApply exercises the full
        // production bundle and normal NativeIndex discovery.
        TimeStarInsert<double> keyProbe("snaprt", "value");
        keyProbe.addTag("host", "h1");
        const SeriesId128 sid = keyProbe.seriesId128();
        auto resultOpt = dst.query("snaprt,host=h1 value", sid, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps, (std::vector<uint64_t>{1000, 3000}));
        ASSERT_EQ(result.values.size(), 2u);
        EXPECT_DOUBLE_EQ(result.values[0], 1.5);
        EXPECT_DOUBLE_EQ(result.values[1], 3.5);

        auto unrelated = dst.query("snaprt_other value", otherSid, 0, UINT64_MAX).get();
        EXPECT_FALSE(unrelated.has_value()) << "snapshot leaked a series from an unrelated VShard";

        // Re-applying the exact object is an idempotent recovery operation (for
        // example, after data publication but before catalog reconstruction).
        EXPECT_TRUE(dst.installVShardSnapshotFiles(manifest, files).get());

        // A genuinely different live/non-empty install is not generation-atomic
        // yet. It must refuse explicitly instead of mixing generations.
        auto different = manifest;
        different.verificationHash = std::string(32, 'f');
        if (different.verificationHash == manifest.verificationHash)
            different.verificationHash = std::string(32, 'e');
        EXPECT_FALSE(dst.installVShardSnapshotFiles(different, files).get());

        // A destination with only derived catalog/index state is not fresh
        // either. Installing an empty-on-disk generation over it would merge
        // discovery state that the data manifest does not describe.
        fs::remove_all("snaprt_indexed");
        Engine indexed(StorageLayout("snaprt_indexed").anchored());
        indexed.init().get();
        MetadataOp op;
        op.seriesId = sid;
        op.measurement = "snaprt";
        op.fieldName = "value";
        op.tags = {{"host", "h1"}};
        op.valueType = TSMValueType::Float;
        indexed.indexMetadataBatch({op}).get();
        EXPECT_FALSE(indexed.installVShardSnapshotFiles(manifest, files).get());

        src.stop().get();
        dst.stop().get();
        indexed.stop().get();
        fs::remove_all("snaprt_src");
        fs::remove_all("snaprt_dst");
        fs::remove_all("snaprt_indexed");
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

        // Peer-controlled names are metadata only and must never escape the
        // staging directory or become live target paths.
        auto traversal = files;
        traversal[0].first = "../../snapshot_escape.tsm";
        EXPECT_FALSE(dst.installVShardSnapshotFiles(manifest, std::move(traversal)).get());
        EXPECT_FALSE(fs::exists("snapshot_escape.tsm"));

        src.stop().get();
        dst.stop().get();
        fs::remove_all("snaprt_src2");
        fs::remove_all("snaprt_dst2");
    })
        .join()
        .get();
}
