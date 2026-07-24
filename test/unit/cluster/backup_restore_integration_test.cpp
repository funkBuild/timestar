// M6 backup/restore gate (in-process): composes the BackupRestore brick with the
// REAL VShard snapshot payloads. A cluster backup is exported from a live Engine's
// VShard snapshot, restored into a FRESH cluster UUID (old identity scrubbed, hashes
// verified, completeness checked, fail-closed on any mismatch), and the verified
// snapshot is installed into a fresh Engine that serves the data. Fail-closed cases:
// same-UUID restore, tampered snapshot, and a truncated backup are all refused.
#include "../../../lib/cluster/data/snapshot_payload.hpp"
#include "../../../lib/cluster/features/backup_restore.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // vshardsCohesiveOnCores

#include <functional>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
namespace fs = std::filesystem;

namespace {
// A deterministic content hash so a single flipped byte is detected at restore.
std::string contentHash(const std::string& s) { return std::to_string(std::hash<std::string>{}(s)); }
}  // namespace

TEST(BackupRestoreIntegration, ExportRestoreInstallAndFailClosed) {
    seastar::thread([] {
        ASSERT_TRUE(vshardsCohesiveOnCores(seastar::smp::count));
        fs::remove_all("bkp_src");
        fs::remove_all("bkp_dst");

        // --- Live source: build a real VShard snapshot payload. ---
        VShardId vshard{0};
        SeriesId128 sid;
        const std::string key = "bkp,host=h1 value";
        std::string snapBytes;
        {
            Engine src(StorageLayout("bkp_src").anchored());
            src.init().get();
            src.setRevisionAssignment(true);
            TimeStarInsert<double> probe("bkp", "value");
            probe.addTag("host", "h1");
            sid = probe.seriesId128();
            vshard = VShardId{virtualShard(sid)};
            {
                TimeStarInsert<double> ins("bkp", "value");
                ins.addTag("host", "h1");
                ins.addValue(1000, 12.0);
                src.insert(std::move(ins)).get();
            }
            src.rolloverMemoryStore().get();
            for (int i = 0; i < 300 && src.getTSMFileCount() == 0; ++i)
                seastar::sleep(std::chrono::milliseconds(100)).get();
            auto built = src.buildVShardSnapshotFiles(vshard, std::string(32, '0')).get();
            data::SnapshotPayload p;
            p.manifest = std::move(built.first);
            for (auto& [n, b] : built.second)
                p.files.push_back(data::SnapshotFile{std::move(n), std::move(b)});
            snapBytes = data::encodeSnapshotPayload(p);
            src.stop().get();
        }
        ASSERT_FALSE(snapBytes.empty());

        using features::BackupRestore;
        const std::string srcUuid = "cluster-src-uuid";
        auto backup = BackupRestore::exportCluster(srcUuid, {{vshard.value(), snapBytes}}, contentHash);
        EXPECT_EQ(backup.expectedVShards, 1u);

        // Restore into a FRESH cluster UUID: verifies + scrubs old identity.
        auto restored = BackupRestore::restore(backup, "cluster-new-uuid", contentHash);
        ASSERT_TRUE(restored.ok) << restored.error;
        EXPECT_EQ(restored.newClusterUuid, "cluster-new-uuid");
        ASSERT_EQ(restored.vshards.size(), 1u);

        // Install the verified snapshot into a fresh Engine and read the data back.
        {
            auto payload = data::decodeSnapshotPayload(restored.vshards[0].snapshot);
            ASSERT_TRUE(payload.has_value()) << "restored snapshot bytes must decode";
            std::vector<std::pair<std::string, std::string>> files;
            for (auto& f : payload->files)
                files.emplace_back(f.name, f.bytes);
            Engine dst(StorageLayout("bkp_dst").anchored());
            dst.init().get();
            EXPECT_TRUE(dst.installVShardSnapshotFiles(payload->manifest, std::move(files)).get());
            auto r = dst.query(key, sid, 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_DOUBLE_EQ(std::get<QueryResult<double>>(r.value()).values[0], 12.0)
                << "restored cluster serves the backed-up data";
            dst.stop().get();
        }

        // Fail-closed: restoring under the SOURCE uuid is refused (identity scrub).
        EXPECT_FALSE(BackupRestore::restore(backup, srcUuid, contentHash).ok);

        // Fail-closed: a tampered snapshot fails the hash check -> nothing restored.
        {
            auto tampered = backup;
            tampered.vshards[0].snapshot[tampered.vshards[0].snapshot.size() / 2] ^= 0xff;
            auto rr = BackupRestore::restore(tampered, "cluster-new-uuid", contentHash);
            EXPECT_FALSE(rr.ok);
            EXPECT_TRUE(rr.vshards.empty()) << "a hash mismatch restores nothing";
        }

        // Fail-closed: a truncated backup (fewer units than expected) is refused.
        {
            auto truncated = backup;
            truncated.expectedVShards = 2;  // claims 2 but carries 1
            EXPECT_FALSE(BackupRestore::restore(truncated, "cluster-new-uuid", contentHash).ok);
        }

        fs::remove_all("bkp_src");
        fs::remove_all("bkp_dst");
    })
        .join()
        .get();
}
