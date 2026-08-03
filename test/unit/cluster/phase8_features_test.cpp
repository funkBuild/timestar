// Phase 8 feature-completion bricks: cluster-aware streaming (backfill/live at one
// barrier, no loss / no silent duplication), backup/restore (fresh cluster UUID,
// scrubbed membership, verified), conservative
// routing summaries (no false negatives), and hierarchical query merge.
#include "../../../lib/cluster/features/backup_restore.hpp"
#include "../../../lib/cluster/features/cluster_backup_export.hpp"
#include "../../../lib/cluster/features/operator_surface.hpp"
#include "../../../lib/cluster/features/routing_summary.hpp"
#include "../../../lib/cluster/features/stream_subscription.hpp"
#include "../../../lib/utils/crc32.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace timestar;
using namespace timestar::control;
using namespace timestar::features;

namespace {
StreamEvent ev(uint16_t vs, Term t, uint64_t idx) {
    return StreamEvent{vs, t, idx, "op" + std::to_string(idx), "p" + std::to_string(idx)};
}
std::vector<uint64_t> indicesOf(const std::vector<StreamEvent>& v) {
    std::vector<uint64_t> out;
    for (const auto& e : v)
        out.push_back(e.index);
    return out;
}
}  // namespace

// ---- Cluster-aware streaming: the backfill -> live one-barrier gate ----

// The full committed log is 1..20. The subscription buffered live events starting
// before the barrier (10..20) and backfills through the barrier (1..12). The
// delivered stream must be EXACTLY 1..20, in order, once each -- no gap (loss),
// no repeat (silent duplication) across the seam.
TEST(StreamSubscription, BackfillLiveOneBarrierNoLossNoDup) {
    std::vector<StreamEvent> backfill, live;
    for (uint64_t i = 1; i <= 12; ++i)
        backfill.push_back(ev(1, 5, i));  // committed state through the barrier
    for (uint64_t i = 10; i <= 20; ++i)
        live.push_back(ev(1, 5, i));  // buffered from before the barrier, overlaps + beyond

    SubscriptionCursor cursor;
    auto delivered = BackfillLiveStream::deliver(cursor, /*barrier=*/12, backfill, live);

    std::vector<uint64_t> expect;
    for (uint64_t i = 1; i <= 20; ++i)
        expect.push_back(i);
    EXPECT_EQ(indicesOf(delivered), expect);  // exactly 1..20, no gap, no dup
    EXPECT_EQ(cursor.lastIndex(), 20u);
}

// A duplicate delivery (at-least-once channel re-sends) is dropped by commit
// position; the resume position never goes backwards.
TEST(StreamSubscription, DedupByCommitPosition) {
    SubscriptionCursor c;
    EXPECT_TRUE(c.accept(ev(1, 5, 1)));
    EXPECT_TRUE(c.accept(ev(1, 5, 2)));
    EXPECT_FALSE(c.accept(ev(1, 5, 2)));  // re-sent: duplicate
    EXPECT_FALSE(c.accept(ev(1, 5, 1)));  // older re-send: duplicate
    EXPECT_TRUE(c.accept(ev(1, 5, 3)));
    EXPECT_EQ(c.lastIndex(), 3u);
}

// A placement change elects a new leader (higher term); the subscription
// re-registers from the last delivered commit position. Re-sent events at or
// below that position are deduped; commit indices remain monotonic across terms.
TEST(StreamSubscription, ReRegisterAcrossPlacementChange) {
    SubscriptionCursor c;
    for (uint64_t i = 1; i <= 12; ++i)
        c.accept(ev(1, 5, i));
    EXPECT_EQ(c.lastIndex(), 12u);
    // New term after re-registration; the source re-sends from 10 (at-least-once).
    std::vector<StreamEvent> resend;
    for (uint64_t i = 10; i <= 18; ++i)
        resend.push_back(ev(1, /*term=*/6, i));
    std::vector<uint64_t> got;
    for (const auto& e : resend)
        if (c.accept(e))
            got.push_back(e.index);
    EXPECT_EQ(got, (std::vector<uint64_t>{13, 14, 15, 16, 17, 18}));  // 10..12 deduped
    EXPECT_EQ(c.term(), 6u);
}

// An event from a strictly OLDER term than already delivered is a stale/rogue
// source: fail closed.
TEST(StreamSubscription, TermRegressionRejected) {
    SubscriptionCursor c;
    c.accept(ev(1, 5, 5));
    EXPECT_THROW(c.accept(ev(1, 4, 6)), StreamRegression);
}

// A cursor RESUMED from a client token drops re-sent events at/below the resume
// position (no re-delivery), so a rogue/stale first event below it cannot poison
// the cursor into dropping legitimate later events.
TEST(StreamSubscription, ResumeFromTokenDropsAlreadySeen) {
    SubscriptionCursor c(/*resumeTerm=*/6, /*resumeIndex=*/100);
    EXPECT_FALSE(c.accept(ev(1, 6, 50)));   // below resume: already delivered
    EXPECT_FALSE(c.accept(ev(1, 6, 100)));  // at resume: already delivered
    EXPECT_TRUE(c.accept(ev(1, 6, 101)));   // first new event after the resume point
    EXPECT_EQ(c.lastIndex(), 101u);
}

// ---- Backup / restore ----

const ClusterBackupAuthenticationKey& backupAuthenticationKey() {
    static const auto key = ClusterBackupAuthenticationKey::fromHex(
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    return key;
}

ClusterBackupManifest completeBackup() {
    ClusterBackupManifest backup;
    backup.sourceClusterUuid = "00112233445566778899aabbccddeeff";
    backup.vshards.reserve(VIRTUAL_SHARD_COUNT);
    for (uint16_t i = 0; i < VIRTUAL_SHARD_COUNT; ++i) {
        backup.vshards.push_back(VShardBackupUnit{i, uint64_t{i} + 1, "00112233445566778899aabbccddeeff",
                                                  "ffeeddccbbaa00998877665544332211", 12, std::string(64, 'a')});
    }
    backup.authenticate(backupAuthenticationKey());
    return backup;
}

TEST(BackupRestore, ExactV1ManifestRoundTripPlansFreshClusterRestore) {
    auto backup = completeBackup();
    backup.control.policies.emplace("schema/cpu", PolicyCell{7, "portable-schema"});
    backup.authenticate(backupAuthenticationKey());
    ASSERT_TRUE(backup.valid());
    ASSERT_TRUE(backup.authenticatedBy(backupAuthenticationKey()));
    const auto encoded = backup.encode();
    auto decoded = ClusterBackupManifest::decode(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(*decoded, backup);

    auto plan = BackupRestore::planRestore(*decoded, "ffeeddccbbaa00998877665544332211",
                                           backupAuthenticationKey());
    ASSERT_TRUE(plan.ok) << plan.error;
    EXPECT_EQ(plan.newClusterUuid, "ffeeddccbbaa00998877665544332211");
    EXPECT_EQ(plan.vshards.size(), VIRTUAL_SHARD_COUNT);
    EXPECT_EQ(plan.control, backup.control);
}

TEST(BackupRestore, RejectsSameOrNonCanonicalUuidAndIncompleteManifest) {
    auto backup = completeBackup();
    auto same = BackupRestore::planRestore(backup, backup.sourceClusterUuid, backupAuthenticationKey());
    EXPECT_FALSE(same.ok);
    EXPECT_FALSE(
        BackupRestore::planRestore(backup, "FFEEDDCCBBAA00998877665544332211", backupAuthenticationKey()).ok);

    backup.vshards.pop_back();
    EXPECT_FALSE(backup.valid());
    auto truncated =
        BackupRestore::planRestore(backup, "ffeeddccbbaa00998877665544332211", backupAuthenticationKey());
    EXPECT_FALSE(truncated.ok);
    EXPECT_TRUE(truncated.vshards.empty());
}

TEST(BackupRestore, ManifestDecoderRejectsCorruptionUnknownVersionAndAliases) {
    auto encoded = completeBackup().encode();
    encoded[encoded.size() / 2] ^= 0x40;
    EXPECT_FALSE(ClusterBackupManifest::decode(encoded).has_value());

    auto badVersion = completeBackup().encode();
    badVersion[4] = 2;
    const uint32_t crc = CRC32::compute(badVersion.data(), badVersion.size() - 4);
    for (int i = 0; i < 4; ++i)
        badVersion[badVersion.size() - 4 + i] = static_cast<char>((crc >> (8 * i)) & 0xff);
    EXPECT_FALSE(ClusterBackupManifest::decode(badVersion).has_value());

    auto retaggedCorruption = completeBackup();
    retaggedCorruption.sourceClusterUuid[0] = '1';
    auto structurallyValidTamper = ClusterBackupManifest::decode(retaggedCorruption.encode());
    ASSERT_TRUE(structurallyValidTamper);
    EXPECT_FALSE(structurallyValidTamper->authenticatedBy(backupAuthenticationKey()));

    const auto wrongKey = ClusterBackupAuthenticationKey::fromHex(
        "1123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
    EXPECT_FALSE(completeBackup().authenticatedBy(wrongKey));
    EXPECT_FALSE(BackupRestore::planRestore(completeBackup(), "ffeeddccbbaa00998877665544332211", wrongKey).ok);

    auto duplicate = completeBackup();
    duplicate.vshards[17].vshard = 16;
    EXPECT_FALSE(duplicate.valid());
    EXPECT_THROW((void)duplicate.encode(), std::invalid_argument);
}

TEST(BackupRestore, AuthenticationKeyFileMustBeOwnerOnlyCanonicalAndNotASymlink) {
    namespace fs = std::filesystem;
    const fs::path root = "backup_auth_key_test";
    const fs::path keyFile = root / "key";
    const fs::path alias = root / "alias";
    fs::remove_all(root);
    fs::create_directory(root);
    {
        std::ofstream out(keyFile);
        out << "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n";
    }
    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace);
    EXPECT_EQ(ClusterBackupAuthenticationKey::load(keyFile), backupAuthenticationKey());

    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::group_read, fs::perm_options::replace);
    EXPECT_THROW((void)ClusterBackupAuthenticationKey::load(keyFile), std::invalid_argument);
    fs::permissions(keyFile, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace);
    fs::create_symlink(fs::absolute(keyFile), alias);
    EXPECT_THROW((void)ClusterBackupAuthenticationKey::load(alias), std::system_error);
    fs::remove_all(root);
}

TEST(BackupRestore, PortableControlCaptureScrubsAuthorityAndFencesActiveSweep) {
    Group0State state;
    state.clusterUuid = "00112233445566778899aabbccddeeff";
    state.nodes.emplace(1, NodeRecord{1, "node", "host:1", "rack", NodeState::Active});
    state.jobs.emplace("move", Job{"move", 2, false, "membership-authority"});
    state.joinTokens.insert("secret");
    state.policies.emplace("schema/cpu", PolicyCell{3, "portable"});
    state.policies.emplace(retentionPolicyKey("cpu"),
                           PolicyCell{3, encodeRetentionPolicyValue(RetentionPolicyValue{"1h", 3'600'000'000'000})});
    state.lastRetentionSweepId = 9;
    state.retentionCutoffs.emplace("cpu", RetentionCutoffRecord{3, 1000});

    auto portable = BackupRestore::capturePortableControl(state);
    ASSERT_TRUE(portable.has_value());
    EXPECT_EQ(portable->policies, state.policies);
    EXPECT_EQ(portable->lastRetentionSweepId, 9u);
    EXPECT_EQ(portable->retentionCutoffs, state.retentionCutoffs);

    state.retentionSweep = RetentionSweep{10, "cpu", 3, 2000, 17};
    EXPECT_FALSE(BackupRestore::capturePortableControl(state).has_value())
        << "a partially fanned-out retention decision has no portable restore state";
}

TEST(BackupRestore, PortableControlAndExportCheckpointHaveOneChecksummedV1Identity) {
    PortableControlBackup portable;
    portable.policies.emplace("schema/cpu", PolicyCell{3, "portable"});
    const auto portableBytes = portable.encode();
    auto decodedPortable = PortableControlBackup::decode(portableBytes);
    ASSERT_TRUE(decodedPortable);
    EXPECT_EQ(*decodedPortable, portable);
    auto corruptPortable = portableBytes;
    corruptPortable[corruptPortable.size() / 2] ^= 0x20;
    EXPECT_FALSE(PortableControlBackup::decode(corruptPortable));

    ClusterBackupExportCheckpoint checkpoint;
    checkpoint.operationId = "11112222333344445555666677778888";
    checkpoint.sourceClusterUuid = "00112233445566778899aabbccddeeff";
    checkpoint.servingMap.epoch = 9;
    for (uint16_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard)
        checkpoint.servingMap.placement.emplace(vshard, std::vector<raft::NodeId>{1, 2, 3});
    checkpoint.control = portable;
    ASSERT_TRUE(checkpoint.valid());
    const auto encoded = checkpoint.encode();
    auto decoded = ClusterBackupExportCheckpoint::decode(encoded);
    ASSERT_TRUE(decoded);
    EXPECT_EQ(*decoded, checkpoint);

    auto corrupt = encoded;
    corrupt[corrupt.size() / 2] ^= 0x40;
    EXPECT_FALSE(ClusterBackupExportCheckpoint::decode(corrupt));
    checkpoint.operationId = std::string(32, '0');
    EXPECT_FALSE(checkpoint.valid()) << "the peer protocol reserves the all-zero operation identity";
}

TEST(BackupRestore, DurableExportCheckpointRefusesOperationMixingAndRetainsProgress) {
    namespace fs = std::filesystem;
    const fs::path archive = "backup_export_checkpoint_test";
    fs::remove_all(archive);
    fs::remove_all(archive.string() + ".export.v1");

    ClusterBackupExportCheckpoint checkpoint;
    checkpoint.operationId = "11112222333344445555666677778888";
    checkpoint.sourceClusterUuid = "00112233445566778899aabbccddeeff";
    checkpoint.servingMap.epoch = 4;
    for (uint16_t vshard = 0; vshard < VIRTUAL_SHARD_COUNT; ++vshard)
        checkpoint.servingMap.placement.emplace(vshard, std::vector<raft::NodeId>{1, 2, 3});

    DurableClusterBackupExport durable(archive);
    EXPECT_EQ(durable.createOrLoad(checkpoint), checkpoint);
    ASSERT_TRUE(durable.load());
    EXPECT_EQ(*durable.load(), checkpoint);

    const auto interrupted = durable.prepareDownload(7);
    {
        std::ofstream out(interrupted, std::ios::binary);
        out << "interrupted";
    }
    EXPECT_TRUE(fs::exists(interrupted));
    const auto resumed = durable.prepareDownload(8);
    EXPECT_FALSE(fs::exists(interrupted));
    EXPECT_NE(resumed, interrupted);

    auto conflict = checkpoint;
    conflict.operationId = "99990000aaaabbbbccccddddeeeeffff";
    EXPECT_THROW((void)durable.createOrLoad(conflict), std::invalid_argument);
    EXPECT_EQ(*durable.load(), checkpoint) << "a conflicting resume must not replace the first operation fence";

    fs::remove_all(archive.string() + ".export.v1");
    fs::create_directories(archive);
    {
        std::ofstream out(archive / "foreign");
        out << "not this operation";
    }
    EXPECT_THROW((void)DurableClusterBackupExport(archive).createOrLoad(checkpoint), std::invalid_argument);
    fs::remove_all(archive);
    fs::remove_all(archive.string() + ".export.v1");
}

TEST(BackupRestore, PortableControlRejectsInconsistentRetentionState) {
    auto backup = completeBackup();
    backup.control.lastRetentionSweepId = 1;
    EXPECT_FALSE(backup.valid()) << "a sweep id without any completed cutoff is not restorable state";

    backup = completeBackup();
    backup.control.policies.emplace(retentionPolicyKey("cpu"), PolicyCell{1, "not-TSRP1"});
    EXPECT_FALSE(backup.valid());

    backup = completeBackup();
    backup.control.lastRetentionSweepId = 1;
    backup.control.retentionCutoffs.emplace("cpu", RetentionCutoffRecord{2, 100});
    backup.control.policies.emplace(
        retentionPolicyKey("cpu"),
        PolicyCell{1, encodeRetentionPolicyValue(RetentionPolicyValue{"1h", 3'600'000'000'000})});
    EXPECT_FALSE(backup.valid()) << "a cutoff cannot name a policy version which was never committed";
}

TEST(BackupRestore, UnitPathsAreCanonicalAndBounded) {
    EXPECT_EQ(BackupRestore::unitRelativePath(0), "vshards/0000.tsp1");
    EXPECT_EQ(BackupRestore::unitRelativePath(42), "vshards/0042.tsp1");
    EXPECT_EQ(BackupRestore::unitRelativePath(4095), "vshards/4095.tsp1");
    EXPECT_THROW((void)BackupRestore::unitRelativePath(4096), std::out_of_range);
}

// ---- Conservative routing summaries ----

TEST(RoutingSummary, PrunesOnlyWhenSealedAndCurrent_NoFalseNegative) {
    RoutingSummary s;
    std::vector<uint16_t> all = {10, 20, 30, 42};
    s.beginEpoch(7);
    s.observe("cpu", 10);
    s.observe("cpu", 42);
    // NOT sealed yet -> must fan out to all (summary still building).
    EXPECT_EQ(s.candidates("cpu", all, 7), all);
    s.seal();
    // Sealed + current epoch -> prune a known measurement to its observed set.
    EXPECT_EQ(s.candidates("cpu", all, 7).size(), 2u);
    // Unknown measurement even in a sealed epoch -> conservative fan-out.
    EXPECT_EQ(s.candidates("mystery", all, 7), all);
    // STALE: a placement change bumped the query's pinned epoch past the summary's
    // -> must fan out to all (never a false negative from a lagging summary).
    EXPECT_EQ(s.candidates("cpu", all, 8), all);
}

// ---- Hierarchical query merge ----

TEST(HierarchicalMerge, EqualsFlatFoldEvenForNonAssociativeSums) {
    using timestar::data::AggState;
    using timestar::data::DataPoint;
    using timestar::data::QueryPartial;
    auto sid = [](const std::string& k) { return SeriesId128::fromSeriesKey(k); };

    // Sums chosen so FP addition is NON-associative: a reordering tree would give a
    // different rounding. The canonical sequential fold must match the flat fold
    // EXACTLY, so the answer never depends on merge topology / coordinator load.
    std::vector<double> sums = {1.0, 1e16, -1e16, 1.0, 3.0};
    std::vector<QueryPartial> parts;
    for (size_t p = 0; p < sums.size(); ++p) {
        QueryPartial q;
        q.raw.push_back({sid("s"), static_cast<uint64_t>(p), static_cast<double>(p)});
        AggState st;
        st.count = 1;
        st.sum = sums[p];
        st.min = st.max = sums[p];
        q.perSeries[sid("agg")] = st;
        parts.push_back(q);
    }
    // Flat reference: sequential left-fold in input order (the coordinator's order).
    QueryPartial flat;
    for (const auto& q : parts) {
        flat.raw.insert(flat.raw.end(), q.raw.begin(), q.raw.end());
        for (const auto& [s, st] : q.perSeries)
            flat.perSeries[s].merge(st);
    }
    QueryPartial merged = HierarchicalMerge::merge(parts);
    EXPECT_EQ(merged.raw.size(), flat.raw.size());
    EXPECT_EQ(merged.perSeries, flat.perSeries);  // bit-exact, topology-independent
    // And the raw multiset is preserved.
    EXPECT_EQ(merged.raw.size(), 5u);
}

// ---- Operator surface (rebalance/repair/read-decision) ----

TEST(OperatorSurface, RebalanceStatusAndControls) {
    timestar::movement::MovementThrottle t(timestar::movement::SloBudgets{});
    RebalanceOps ops(t);
    EXPECT_TRUE(ops.status().running);
    ops.pause();
    EXPECT_FALSE(ops.status().running);
    ops.resume();
    EXPECT_TRUE(ops.status().running);
    ops.cancel();
    EXPECT_TRUE(ops.status().cancelled);
    EXPECT_FALSE(ops.status().running);
}
TEST(OperatorSurface, RepairStatusReportsQuarantine) {
    timestar::movement::Scrubber s([](const std::string&) { return std::string("bad"); });
    s.scrub({{"objX", "good"}});
    auto rs = repairStatus(s);
    EXPECT_EQ(rs.quarantinedCount, 1u);
    EXPECT_EQ(rs.quarantined[0], "objX");
}
TEST(OperatorSurface, ReplicaDecisionTrace) {
    auto d = traceReplicaDecision(7, "linearizable", {3, 5, 9});
    EXPECT_EQ(d.chosen, 3);  // front of the preference order
    EXPECT_EQ(d.eligible.size(), 3u);
    auto none = traceReplicaDecision(7, "bounded_staleness", {});
    EXPECT_EQ(none.chosen, 0);  // no eligible replica -> fails closed
}
