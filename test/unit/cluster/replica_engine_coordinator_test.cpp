// M4: the production replica query coordinator over ReplicaEngineReaders. Each
// VShard contributes EXACTLY ONCE; a failed replica is retried/hedged to the next;
// a VShard no replica can serve fails the query (or is reported missing under
// allowPartial). Tested with in-memory read doubles (no Engine/Raft).
#include "../../../lib/cluster/integration/replica_engine_coordinator.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
// A read double: serves a partial reporting `seriesFound`, or throws when `fail`.
class FakeReader : public cluster::EngineReplicaReadFace {
public:
    uint64_t seriesFound = 1;
    bool fail = false;
    int calls = 0;
    seastar::future<cluster::ReplicaReadOutcome> read(data::NodeQueryRequest, data::ReadConsistency,
                                                      data::ReadEnvelope, uint64_t) override {
        ++calls;
        if (fail)
            return seastar::make_exception_future<cluster::ReplicaReadOutcome>(std::runtime_error("replica down"));
        data::NodeQueryPartial p;
        p.seriesFound = seriesFound;
        return seastar::make_ready_future<cluster::ReplicaReadOutcome>(
            cluster::ReplicaReadOutcome{std::move(p), {}});
    }
};

cluster::EngineReplicaQueryResult run(std::vector<cluster::EngineVShardReplicas> placement, unsigned hedge,
                                      bool allowPartial) {
    cluster::ReplicaEngineQueryCoordinator coord(std::move(placement), hedge, allowPartial);
    return coord.query({}, data::ReadConsistency::Linearizable, {}, 0).get();
}
}  // namespace

TEST(ReplicaEngineCoordinator, EachVShardContributesOnce) {
    seastar::thread([] {
        FakeReader a, b;
        a.seriesFound = 3;
        b.seriesFound = 5;
        auto res = run({{1, {&a}}, {2, {&b}}}, /*hedge=*/1, /*allowPartial=*/false);
        EXPECT_EQ(res.partial.seriesFound, 8u);
        EXPECT_TRUE(res.missing.empty());
        EXPECT_EQ(a.calls, 1);
        EXPECT_EQ(b.calls, 1);
    }).join().get();
}

TEST(ReplicaEngineCoordinator, RetriesToNextReplicaOnFailure) {
    seastar::thread([] {
        FakeReader down, up;
        down.fail = true;
        up.seriesFound = 4;
        // hedge=1: first replica fails, coordinator advances to the second.
        auto res = run({{7, {&down, &up}}}, /*hedge=*/1, /*allowPartial=*/false);
        EXPECT_EQ(res.partial.seriesFound, 4u);
        EXPECT_EQ(down.calls, 1);
        EXPECT_EQ(up.calls, 1);
    }).join().get();
}

TEST(ReplicaEngineCoordinator, HedgeTakesFirstSuccessExactlyOnce) {
    seastar::thread([] {
        FakeReader a, b;
        a.seriesFound = 2;
        b.seriesFound = 100;
        // hedge=2: BOTH launch in one wave, but only the FIRST success merges -- the
        // VShard contributes once, not twice.
        auto res = run({{9, {&a, &b}}}, /*hedge=*/2, /*allowPartial=*/false);
        EXPECT_EQ(res.partial.seriesFound, 2u) << "only the first success is merged";
        EXPECT_EQ(a.calls, 1);
        EXPECT_EQ(b.calls, 1) << "the hedge wave launched both";
    }).join().get();
}

TEST(ReplicaEngineCoordinator, NoServingReplicaFailsClosed) {
    seastar::thread([] {
        FakeReader down;
        down.fail = true;
        bool threw = false;
        try {
            run({{3, {&down}}}, /*hedge=*/1, /*allowPartial=*/false);
        } catch (const data::QueryIncomplete&) {
            threw = true;
        }
        EXPECT_TRUE(threw) << "a VShard no replica can serve must fail the query, not omit silently";
    }).join().get();
}

TEST(ReplicaEngineCoordinator, AllowPartialReportsMissing) {
    seastar::thread([] {
        FakeReader ok, down;
        ok.seriesFound = 6;
        down.fail = true;
        auto res = run({{1, {&ok}}, {2, {&down}}}, /*hedge=*/1, /*allowPartial=*/true);
        EXPECT_EQ(res.partial.seriesFound, 6u);
        ASSERT_EQ(res.missing.size(), 1u);
        EXPECT_EQ(res.missing[0], 2u) << "the unservable VShard is named, never silently dropped";
    }).join().get();
}
