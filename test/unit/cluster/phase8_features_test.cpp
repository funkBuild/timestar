// Phase 8 feature-completion bricks: cluster-aware streaming (backfill/live at one
// barrier, no loss / no silent duplication), backup/restore (fresh cluster UUID,
// scrubbed membership, verified), conservative
// routing summaries (no false negatives), and hierarchical query merge.
#include "../../../lib/cluster/features/backup_restore.hpp"
#include "../../../lib/cluster/features/operator_surface.hpp"
#include "../../../lib/cluster/features/routing_summary.hpp"
#include "../../../lib/cluster/features/stream_subscription.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

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

std::string fakeHash(const std::string& s) {
    return "h:" + std::to_string(s.size()) + ":" + (s.empty() ? "" : std::string(1, s[0]));
}

TEST(BackupRestore, ExportRestoreRoundTripIntoNewClusterUuid) {
    auto backup = BackupRestore::exportCluster("cluster-OLD", {{1, "snapA"}, {2, "snapB"}}, fakeHash);
    EXPECT_EQ(backup.sourceClusterUuid, "cluster-OLD");
    ASSERT_EQ(backup.vshards.size(), 2u);

    auto r = BackupRestore::restore(backup, "cluster-NEW", fakeHash);
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(r.newClusterUuid, "cluster-NEW");
    ASSERT_EQ(r.vshards.size(), 2u);
    EXPECT_EQ(r.vshards[0].snapshot, "snapA");  // generation-one state restored
}

TEST(BackupRestore, RejectsSameUuidAndFailsClosedOnCorruption) {
    auto backup = BackupRestore::exportCluster("cluster-OLD", {{1, "snapA"}}, fakeHash);
    // Restoring under the SAME UUID would resurrect the old cluster identity.
    auto same = BackupRestore::restore(backup, "cluster-OLD", fakeHash);
    EXPECT_FALSE(same.ok);
    // A corrupt unit fails the whole restore closed (nothing restored).
    backup.vshards[0].verificationHash = "WRONG";
    auto corrupt = BackupRestore::restore(backup, "cluster-NEW", fakeHash);
    EXPECT_FALSE(corrupt.ok);
    EXPECT_TRUE(corrupt.vshards.empty());
}
TEST(BackupRestore, RejectsTruncatedAndEmptyBackups) {
    auto backup = BackupRestore::exportCluster("cluster-OLD", {{1, "a"}, {2, "b"}, {3, "c"}}, fakeHash);
    EXPECT_EQ(backup.expectedVShards, 3u);
    // Drop a unit: a truncated backup must NOT restore as a valid partial cluster.
    backup.vshards.pop_back();
    auto truncated = BackupRestore::restore(backup, "cluster-NEW", fakeHash);
    EXPECT_FALSE(truncated.ok);
    // An empty backup fails closed too.
    ClusterBackup empty;
    empty.sourceClusterUuid = "cluster-OLD";
    EXPECT_FALSE(BackupRestore::restore(empty, "cluster-NEW", fakeHash).ok);
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
