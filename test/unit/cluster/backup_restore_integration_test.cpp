// Backup/restore composition gate (in-process): a real Engine produces a TSP1
// VShard unit, the complete 4,096-unit TSBK v1 manifest validates into a fresh
// cluster UUID, and that real unit installs into a fresh Engine which serves the
// original data. Same-UUID restore, corrupt TSP1 and incomplete manifest fail.
#include "../../../lib/cluster/control/durable_control_map.hpp"
#include "../../../lib/cluster/control/group0_state_machine.hpp"
#include "../../../lib/cluster/data/snapshot_payload.hpp"
#include "../../../lib/cluster/features/backup_restore.hpp"
#include "../../../lib/cluster/features/cluster_restore_seeder.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"           // vshardsCohesiveOnCores
#include "../../../lib/storage/series_catalog.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
namespace fs = std::filesystem;

namespace {
uint64_t fnv1a(std::string_view value) {
    uint64_t hash = 1469598103934665603ull;
    for (unsigned char byte : value) {
        hash ^= byte;
        hash *= 1099511628211ull;
    }
    return hash;
}

void writeBytes(const fs::path& path, std::string_view bytes) {
    if (!path.parent_path().empty())
        fs::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    if (!output)
        throw std::runtime_error("failed to write backup test fixture " + path.string());
}

std::array<uint8_t, 16> hexBytes(std::string_view value) {
    const auto nibble = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9')
            return static_cast<uint8_t>(c - '0');
        return static_cast<uint8_t>(c - 'a' + 10);
    };
    std::array<uint8_t, 16> out{};
    for (size_t i = 0; i < out.size(); ++i)
        out[i] = static_cast<uint8_t>((nibble(value[2 * i]) << 4) | nibble(value[2 * i + 1]));
    return out;
}

std::string emptyVShardSnapshot(uint16_t vshard) {
    data::SnapshotPayload payload;
    payload.manifest.vshard = VShardId{vshard};
    payload.manifest.snapshotRevision = 1;
    payload.manifest.verificationHash = std::string(32, '0');
    SeriesCatalog catalog;
    payload.catalog = catalog.snapshot();
    payload.manifest.catalogHash = SeriesCatalog::snapshotHash(payload.catalog);
    return data::encodeSnapshotPayload(std::move(payload));
}

features::VShardBackupUnit unitFor(uint16_t vshard, const std::string& bytes) {
    auto payload = data::decodeSnapshotPayload(bytes);
    if (!payload || payload->manifest.vshard.value() != vshard)
        throw std::logic_error("invalid backup test TSP1 unit");
    return features::VShardBackupUnit{vshard,
                                      payload->manifest.snapshotRevision,
                                      payload->manifest.verificationHash,
                                      payload->manifest.catalogHash,
                                      bytes.size(),
                                      fnv1a(bytes)};
}
}  // namespace

TEST(BackupRestoreIntegration, ExportRestoreInstallAndFailClosed) {
    seastar::thread([] {
        ASSERT_TRUE(vshardsCohesiveOnCores(seastar::smp::count));
        fs::remove_all("bkp_src");
        fs::remove_all("bkp_dst");
        fs::remove_all("bkp_archive");
        fs::remove_all("bkp_stage");
        fs::remove_all("bkp_stage_fresh");

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
            auto built = src.buildVShardSnapshotFiles(vshard).get();
            data::SnapshotPayload p;
            p.manifest = std::move(built.manifest);
            p.catalog = std::move(built.catalog);
            for (auto& [n, b] : built.files)
                p.files.push_back(data::SnapshotFile{std::move(n), std::move(b)});
            snapBytes = data::encodeSnapshotPayload(p);
            src.stop().get();
        }
        ASSERT_FALSE(snapBytes.empty());

        // The staging API is bounded, durable, idempotent, and refuses to
        // relabel a source TSP1 as another VShard. A partial archive cannot be
        // published as complete.
        writeBytes("bkp_source.tsp1", snapBytes);
        auto sourceInspection = data::inspectSnapshotPayloadFile("bkp_source.tsp1").get();
        ASSERT_TRUE(sourceInspection);
        EXPECT_EQ(sourceInspection->manifest.vshard, vshard);
        ASSERT_TRUE(
            features::ClusterBackupArchive::stageVShard("bkp_stage_fresh", vshard.value(), "bkp_source.tsp1").get())
            << "staging must create a previously absent archive root";
        fs::remove_all("bkp_stage_fresh");
        const auto interruptedStage =
            fs::path("bkp_stage") / (features::BackupRestore::unitRelativePath(vshard.value()) + ".partial");
        writeBytes(interruptedStage, "interrupted");
        auto staged = features::ClusterBackupArchive::stageVShard("bkp_stage", vshard.value(), "bkp_source.tsp1").get();
        ASSERT_TRUE(staged);
        EXPECT_FALSE(fs::exists(interruptedStage));
        const auto stagedFinal = fs::path("bkp_stage") / features::BackupRestore::unitRelativePath(vshard.value());
        fs::create_hard_link(stagedFinal, interruptedStage);
        EXPECT_EQ(features::ClusterBackupArchive::stageVShard("bkp_stage", vshard.value(), "bkp_source.tsp1").get(),
                  staged);
        EXPECT_FALSE(fs::exists(interruptedStage)) << "retry must close the crash window after final-link publication";
        EXPECT_FALSE(
            features::ClusterBackupArchive::stageVShard(
                "bkp_stage", static_cast<uint16_t>((vshard.value() + 1) % VIRTUAL_SHARD_COUNT), "bkp_source.tsp1")
                .get());

        // Assemble one canonical artifact per VShard. One is the live Engine
        // payload above; the rest are minimal exact-v1 empty VShard payloads.
        features::ClusterBackupManifest backup;
        backup.sourceClusterUuid = "00112233445566778899aabbccddeeff";
        backup.control.policies.emplace("schema/imported", control::PolicyCell{7, "portable-value"});
        backup.vshards.reserve(VIRTUAL_SHARD_COUNT);
        std::vector<std::string> emptyUnits(VIRTUAL_SHARD_COUNT);
        for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i) {
            const std::string& bytes = i == vshard.value() ? snapBytes : (emptyUnits[i] = emptyVShardSnapshot(i));
            writeBytes(fs::path("bkp_archive") / features::BackupRestore::unitRelativePath(i), bytes);
            backup.vshards.push_back(unitFor(i, bytes));
            if ((i & 63u) == 63u)
                seastar::thread::yield();
        }
        ASSERT_TRUE(backup.valid());
        EXPECT_FALSE(features::ClusterBackupArchive::publish("bkp_stage", backup).get())
            << "one staged unit is never a complete cluster archive";
        writeBytes("bkp_archive/.manifest.tsbk1.partial", "interrupted");
        ASSERT_TRUE(features::ClusterBackupArchive::publish("bkp_archive", backup).get());
        EXPECT_FALSE(fs::exists("bkp_archive/.manifest.tsbk1.partial"));
        fs::create_hard_link("bkp_archive/manifest.tsbk1", "bkp_archive/.manifest.tsbk1.partial");
        EXPECT_TRUE(features::ClusterBackupArchive::publish("bkp_archive", backup).get());
        EXPECT_FALSE(fs::exists("bkp_archive/.manifest.tsbk1.partial"))
            << "retry must close the crash window after manifest-link publication";
        auto decodedManifest = features::ClusterBackupArchive::validate("bkp_archive").get();
        ASSERT_TRUE(decodedManifest);
        EXPECT_EQ(*decodedManifest, backup);

        auto restored = features::BackupRestore::planRestore(*decodedManifest, "ffeeddccbbaa00998877665544332211");
        ASSERT_TRUE(restored.ok) << restored.error;
        EXPECT_EQ(restored.newClusterUuid, "ffeeddccbbaa00998877665544332211");
        ASSERT_EQ(restored.vshards.size(), VIRTUAL_SHARD_COUNT);
        EXPECT_EQ(restored.vshards[vshard.value()], backup.vshards[vshard.value()]);

        // Install the verified snapshot into a fresh Engine and read the data back.
        {
            auto payload = data::decodeSnapshotPayload(snapBytes);
            ASSERT_TRUE(payload.has_value()) << "restored snapshot bytes must decode";
            std::vector<std::pair<std::string, std::string>> files;
            for (auto& f : payload->files)
                files.emplace_back(f.name, f.bytes);
            Engine dst(StorageLayout("bkp_dst").anchored());
            dst.init().get();
            EXPECT_TRUE(dst.installVShardSnapshotFiles(payload->manifest, std::move(files)).get());
            EXPECT_TRUE(dst.installVShardSnapshotCatalog(payload->manifest.vshard, payload->catalog,
                                                         payload->manifest.catalogHash)
                            .get());

            // Catalog identity includes the durable value type. A catalog with
            // a valid self-hash but a type that disagrees with the TSM must not
            // be accepted as reconstructed discovery state.
            SeriesCatalog wrongCatalog;
            CatalogEntry wrongEntry;
            wrongEntry.measurement = "bkp";
            wrongEntry.tags = {{"host", "h1"}};
            wrongEntry.field = "value";
            wrongEntry.valueType = TSMValueType::Integer;
            EXPECT_TRUE(wrongCatalog.apply(CatalogRecord{sid, std::move(wrongEntry)}));
            auto wrongBytes = wrongCatalog.snapshot();
            EXPECT_FALSE(dst.installVShardSnapshotCatalog(payload->manifest.vshard, wrongBytes,
                                                          SeriesCatalog::snapshotHash(wrongBytes))
                             .get());
            auto r = dst.query(key, sid, 0, UINT64_MAX).get();
            ASSERT_TRUE(r.has_value());
            EXPECT_DOUBLE_EQ(std::get<QueryResult<double>>(r.value()).values[0], 12.0)
                << "restored cluster serves the backed-up data";
            dst.stop().get();
        }

        // Fail-closed: restoring under the SOURCE uuid is refused (identity scrub).
        EXPECT_FALSE(features::BackupRestore::planRestore(backup, backup.sourceClusterUuid).ok);

        // Fail-closed: a tampered snapshot fails the hash check -> nothing restored.
        {
            const uint16_t target = static_cast<uint16_t>((vshard.value() + 1) % VIRTUAL_SHARD_COUNT);
            const auto path = fs::path("bkp_archive") / features::BackupRestore::unitRelativePath(target);
            auto tampered = emptyUnits[target];
            tampered[tampered.size() / 2] ^= 0xff;
            writeBytes(path, tampered);
            EXPECT_FALSE(features::ClusterBackupArchive::validate("bkp_archive").get());
            writeBytes(path, emptyUnits[target]);
            ASSERT_TRUE(features::ClusterBackupArchive::validate("bkp_archive").get());
        }

        // Fail-closed: extra and missing filesystem entries are not aliases for
        // a complete canonical 4,096-unit artifact set.
        {
            writeBytes("bkp_archive/vshards/alias.tsp1", emptyUnits[0]);
            EXPECT_FALSE(features::ClusterBackupArchive::validate("bkp_archive").get());
            fs::remove("bkp_archive/vshards/alias.tsp1");

            const auto missing = fs::path("bkp_archive") / features::BackupRestore::unitRelativePath(0);
            const std::string missingBytes = vshard.value() == 0 ? snapBytes : emptyUnits[0];
            fs::remove(missing);
            EXPECT_FALSE(features::ClusterBackupArchive::validate("bkp_archive").get());
            writeBytes(missing, missingBytes);
            ASSERT_TRUE(features::ClusterBackupArchive::validate("bkp_archive").get());
        }

        // Seed a genuinely new Raft generation. Only two VShards belong to this
        // node in the target map, keeping the gate bounded while still proving
        // interrupted progress, revision-one rebasing, fresh membership, and
        // portable Group-0 recovery.
        {
            const uint16_t second = static_cast<uint16_t>((vshard.value() + 1) % VIRTUAL_SHARD_COUNT);
            features::ClusterRestoreSeedRequest request;
            request.archiveDirectory = "bkp_archive";
            request.dataDirectory = "bkp_seed";
            request.newClusterUuid = "ffeeddccbbaa00998877665544332211";
            request.clusterUuidBytes = hexBytes(request.newClusterUuid);
            request.bootId.fill(0x5a);
            request.self = 1;
            request.controlSeed = 1;
            request.coreCount = 1;
            request.servingMap.epoch = 1;
            for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i)
                request.servingMap.placement[i] = (i == vshard.value() || i == second)
                                                      ? std::vector<raft::NodeId>{1, 2, 3}
                                                      : std::vector<raft::NodeId>{2, 3, 4};
            request.controlSeedRecord =
                control::NodeRecord{1, "new-node-uuid", "127.0.0.1:8086", "rack-a", control::NodeState::Joining};

            size_t durable = 0;
            request.vshardDurableForTesting = [&](uint16_t) {
                if (++durable == 1)
                    throw std::runtime_error("injected restore interruption");
            };
            EXPECT_THROW(features::ClusterRestoreSeeder::seed(request).get(), std::runtime_error);
            EXPECT_EQ(features::ClusterRestoreSeeder::inspectTarget("bkp_seed"),
                      features::ClusterRestoreTargetState::InProgress);

            auto conflicting = request;
            conflicting.coreCount = 2;
            EXPECT_THROW(features::ClusterRestoreSeeder::seed(std::move(conflicting)).get(), std::runtime_error);

            request.vshardDurableForTesting = {};
            auto seeded = features::ClusterRestoreSeeder::seed(request).get();
            EXPECT_TRUE(seeded.resumed);
            EXPECT_EQ(seeded.localVShards, 2u);
            EXPECT_EQ(features::ClusterRestoreSeeder::inspectTarget("bkp_seed"),
                      features::ClusterRestoreTargetState::Complete);
            auto cached = control::DurableControlMapStore("bkp_seed").load();
            ASSERT_TRUE(cached);
            EXPECT_EQ(*cached, request.servingMap);

            auto alreadyComplete = features::ClusterRestoreSeeder::seed(request).get();
            EXPECT_TRUE(alreadyComplete.resumed);
            EXPECT_EQ(alreadyComplete.localVShards, 2u);
            EXPECT_EQ(alreadyComplete.seededThisRun, 0u);

            JournalSegmentHeader header;
            header.clusterUuid = request.clusterUuidBytes;
            header.bootId.fill(0xa5);
            {
                header.coreNumber = 0;
                JournalWriter writer("bkp_seed/cluster_raft/group0", header, 1u << 20);
                auto records = writer.open().get();
                auto state = raft::recoverRaftState(records, VShardId{0});
                ASSERT_TRUE(state.snapshot);
                EXPECT_EQ(state.hardState, (raft::HardState{1, raft::kNoNode}));
                control::Group0StateMachine sm;
                sm.applySnapshot(*state.snapshot).get();
                EXPECT_EQ(sm.state().clusterUuid, request.newClusterUuid);
                EXPECT_EQ(sm.state().nodes.size(), 1u);
                EXPECT_EQ(sm.state().metaVoters, std::vector<raft::NodeId>{1});
                EXPECT_EQ(sm.state().policies.at("schema/imported"), (control::PolicyCell{7, "portable-value"}));
                EXPECT_TRUE(sm.state().joinTokens.empty());
                EXPECT_TRUE(sm.state().jobs.empty());
                writer.close().get();
            }
            for (uint16_t imported : {vshard.value(), second}) {
                header.coreNumber = 0;
                const auto directory = fs::path("bkp_seed/cluster_raft") / ("vshard_" + std::to_string(imported));
                JournalWriter writer(directory, header, 1u << 20);
                auto records = writer.open().get();
                auto state = raft::recoverRaftState(records, VShardId{imported}, directory / "snapshot_sidecars");
                ASSERT_TRUE(state.snapshot);
                ASSERT_TRUE(state.snapshot->file);
                EXPECT_TRUE(state.snapshotFromPeer);
                EXPECT_EQ(state.snapshot->term, 1u);
                EXPECT_EQ(state.snapshot->config.voters, (std::vector<raft::NodeId>{1, 2, 3}));
                auto info = data::inspectSnapshotPayloadFile(state.snapshot->file->path).get();
                ASSERT_TRUE(info);
                EXPECT_EQ(info->manifest.vshard, VShardId{imported});
                EXPECT_GE(info->manifest.snapshotRevision, 2u) << "Raft snapshots cannot use the index-zero sentinel";
                EXPECT_EQ(state.snapshot->index + 1, info->manifest.snapshotRevision);
                EXPECT_EQ(state.hardState, (raft::HardState{1, raft::kNoNode}));
                writer.close().get();
            }
        }

        fs::remove_all("bkp_src");
        fs::remove_all("bkp_dst");
        fs::remove_all("bkp_archive");
        fs::remove_all("bkp_stage");
        fs::remove_all("bkp_stage_fresh");
        fs::remove_all("bkp_seed");
        fs::remove("bkp_source.tsp1");
    })
        .join()
        .get();
}
