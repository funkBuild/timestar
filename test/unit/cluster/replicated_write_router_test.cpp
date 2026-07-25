// Integration M3 + write-scaleout 3a/3b: ReplicatedBatchWriteRouter routes a WriteBatch
// to each VShard's LEADER (local ProposeSink vs remote proposeWrite), commits on quorum,
// follows a LEADER HINT when its guess was stale, retries ONLY the slices that failed,
// and fails the whole write retryably when the budget is spent -- never a silent partial
// ack. In-memory doubles (no sockets/Raft).
#include "../../../lib/cluster/data/replicated_write_router.hpp"

#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/utils/series_key.hpp"

#include <gtest/gtest.h>

#include <map>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <string>
#include <vector>

using namespace timestar::data;
using timestar::buildSeriesKey;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {

// Records batches proposed locally (this node leads these VShards).
class LocalSink : public ProposeSink {
public:
    std::vector<std::string> keys;
    bool committed = true;
    seastar::future<bool> proposeBatch(WriteBatch batch) override {
        for (auto& s : batch.series)
            keys.push_back(s.seriesKey);
        return seastar::make_ready_future<bool>(committed);
    }
};

// Records batches forwarded to remote leaders via proposeWrite.
class RemoteTransport : public NodeTransport {
public:
    std::map<NodeId, std::vector<std::string>> keys;
    bool committed = true;
    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<bool> proposeWrite(NodeId to, WriteBatch batch) override {
        for (auto& s : batch.series)
            keys[to].push_back(s.seriesKey);
        return seastar::make_ready_future<bool>(committed);
    }
};

// Leader unknown -> the router falls back to the placement primary (exercises the
// primary-routing path these tests assert).
class NoLeaderResolver : public LeaderResolver {
public:
    NodeId leaderOf(uint16_t) const override { return timestar::raft::kNoNode; }
};

// A resolver whose answers a test controls per VShard.
class MapLeaderResolver : public LeaderResolver {
public:
    std::map<uint16_t, NodeId> leaders;
    NodeId leaderOf(uint16_t vs) const override {
        auto it = leaders.find(vs);
        return it == leaders.end() ? timestar::raft::kNoNode : it->second;
    }
};

// ---------------------------------------------------------------------------
// 3a/3b doubles: sinks/transports that answer the HINTED entry points, so a test can
// script per-attempt, per-VShard outcomes.

// Scripted local sink. `rejectUntilAttempt[vs] = n` rejects that VShard on attempts
// 1..n-1 and commits from attempt n; `hintFor[vs]` is the leader it names on rejection.
class ScriptedLocalSink : public ProposeSink {
public:
    std::map<uint16_t, unsigned> rejectUntilAttempt;
    std::map<uint16_t, NodeId> hintFor;
    std::map<uint16_t, WriteFailure> kindFor;
    std::vector<std::exception_ptr> throwOnAttempt;  // index = attempt-1; null = no throw
    unsigned attempts = 0;
    std::vector<std::vector<uint16_t>> seen;  // VShards proposed, per attempt
    // Reproduce ReplicatedVShardHost's membership-check shape: propose NOTHING and name
    // only ONE VShard in the reject list. Every other double here models the (false)
    // assumption "rejects == everything uncommitted"; this one models what the real sink
    // actually does, which is what made the reject-set contract unsafe.
    bool subsetRejectProposeNothing = false;

    seastar::future<bool> proposeBatch(WriteBatch) override {
        return seastar::make_exception_future<bool>(std::runtime_error("unused"));
    }
    seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view) override {
        const unsigned attempt = ++attempts;
        std::vector<uint16_t> vs;
        for (const auto* g : view)
            vs.push_back(g->first);
        seen.push_back(vs);
        if (attempt <= throwOnAttempt.size() && throwOnAttempt[attempt - 1])
            return seastar::make_exception_future<ProposeOutcome>(throwOnAttempt[attempt - 1]);
        ProposeOutcome out;
        if (subsetRejectProposeNothing && attempt == 1) {
            // ATTEMPT 1 ONLY: committedVShards stays EMPTY -- nothing was proposed --
            // while `rejects` names a single VShard. A caller that subtracts rejects acks
            // the other 59 here and then commits the named one on attempt 2, so the WRITE
            // SUCCEEDS having replicated 1 of 60 slices. Rejecting forever would only
            // show a wrong retry set; failing on attempt 2 is what makes the bogus ACK
            // observable.
            if (!view.empty())
                out.rejects.push_back(
                    SliceReject{view.front()->first, timestar::raft::kNoNode, WriteFailure::NotLeader});
            out.committed = false;
            return seastar::make_ready_future<ProposeOutcome>(std::move(out));
        }
        for (const auto* g : view) {
            auto it = rejectUntilAttempt.find(g->first);
            if (it != rejectUntilAttempt.end() && attempt < it->second) {
                const auto k = kindFor.count(g->first) ? kindFor.at(g->first) : WriteFailure::NotLeader;
                const NodeId h = hintFor.count(g->first) ? hintFor.at(g->first) : timestar::raft::kNoNode;
                out.rejects.push_back(SliceReject{g->first, h, k});
            } else {
                out.committedVShards.push_back(g->first);  // the AUTHORITATIVE half
            }
        }
        out.committed = out.rejects.empty();
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

// Scripted remote transport: records which node received which VShards, per attempt.
class ScriptedTransport : public NodeTransport {
public:
    std::map<NodeId, std::vector<uint16_t>> received;
    std::set<NodeId> deadNodes;    // throw a transport error for these
    std::set<NodeId> notLeaderOf;  // reject everything (hintless) for these
    unsigned calls = 0;

    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<ProposeOutcome> proposeWriteHinted(NodeId to, VShardBatchView view, OptDeadline) override {
        ++calls;
        for (const auto* g : view)
            received[to].push_back(g->first);
        if (deadNodes.count(to))
            return seastar::make_exception_future<ProposeOutcome>(std::runtime_error("connection is closed"));
        ProposeOutcome out;
        if (notLeaderOf.count(to)) {
            for (const auto* g : view)
                out.rejects.push_back(SliceReject{g->first, timestar::raft::kNoNode, WriteFailure::NotLeader});
        } else {
            for (const auto* g : view)
                out.committedVShards.push_back(g->first);
        }
        out.committed = out.rejects.empty();
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

ControlMap rf3Map(unsigned n) {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v) {
        std::vector<NodeId> reps;
        for (uint16_t k = 0; k < 3; ++k)
            reps.push_back(static_cast<NodeId>(((v + k) % n) + 1));
        m.placement[v] = std::move(reps);  // primary = reps[0] = leader hint
    }
    return m;
}

// Every VShard's primary is node 1, so the whole batch routes LOCAL by default.
ControlMap allLocalMap() {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v)
        m.placement[v] = {1, 2, 3};
    return m;
}

WriteBatch manySeries(int n) {
    WriteBatch b;
    for (int i = 0; i < n; ++i) {
        WriteSeries s;
        s.seriesKey = buildSeriesKey("m", {{"host", "h" + std::to_string(i)}}, "v");
        s.type = TSMValueType::Float;
        s.timestamps = {1000};
        s.values = std::vector<double>{1.0};
        b.series.push_back(std::move(s));
    }
    return b;
}

std::vector<uint16_t> vshardsOf(const WriteBatch& b) {
    std::set<uint16_t> vs;
    for (const auto& s : b.series) {
        WriteSeries copy = s;
        vs.insert(vshardOf(copy));
    }
    return {vs.begin(), vs.end()};
}

seastar::future<> testRoutesToLeaders() {
    const unsigned N = 3;
    VShardDirectory dir(1, rf3Map(N));  // self = node 1
    LocalSink local;
    RemoteTransport client;
    NoLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    WriteBatch batch = manySeries(90);
    // Expected split by leader (primary), computed independently from the directory.
    std::map<NodeId, std::vector<std::string>> expect;
    for (auto& s : batch.series) {
        uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(s.seriesKey));
        expect[dir.ownerOf(vs)].push_back(s.seriesKey);
    }
    EXPECT_EQ(expect.size(), 3u);  // leaders spread across all 3 nodes

    co_await router.write(std::move(batch));

    // Local (node 1) group replicated through the local sink.
    auto le = expect[1];
    std::sort(le.begin(), le.end());
    std::sort(local.keys.begin(), local.keys.end());
    EXPECT_EQ(local.keys, le);
    // Remote groups forwarded to their leaders (2 and 3); node 1 never self-forwards.
    EXPECT_EQ(client.keys.count(1), 0u);
    for (NodeId peer : {2, 3}) {
        auto got = client.keys[peer];
        std::sort(got.begin(), got.end());
        auto exp = expect[peer];
        std::sort(exp.begin(), exp.end());
        EXPECT_EQ(got, exp) << "leader " << peer;
    }
}

seastar::future<> testStaleLeaderFailsWrite() {
    VShardDirectory dir(1, rf3Map(3));
    LocalSink local;
    local.committed = false;  // local leader stale (not actually leader), forever
    RemoteTransport client;
    NoLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    bool retryable = false;
    try {
        co_await router.write(manySeries(30));
    } catch (const RetryableWriteError&) {
        retryable = true;
    }
    EXPECT_TRUE(retryable) << "a permanently stale leader must fail the write RETRYABLY, after the budget";
}

seastar::future<> testUnassignedRejects() {
    ControlMap empty;
    empty.epoch = 1;  // no placement -> every VShard unassigned
    VShardDirectory dir(1, empty);
    LocalSink local;
    RemoteTransport client;
    NoLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    bool threw = false;
    try {
        co_await router.write(manySeries(10));
    } catch (const UnassignedVShardError&) {
        threw = true;
    }
    EXPECT_TRUE(threw);
    EXPECT_TRUE(local.keys.empty());  // nothing replicated
    EXPECT_TRUE(client.keys.empty());
}

// 3a: the deposed-primary case. Placement still names node 1, node 1 hosts the group but
// no longer leads it and says so (hint = 2). Without the hint the retry would go back to
// node 1 for ever; with it, the second attempt reaches node 2 and commits.
seastar::future<> testLeaderHintRedirectsTheRetry() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(4);
    const auto vs = vshardsOf(batch);
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;  // knows nothing -> falls back to the placement primary (1)
    for (uint16_t v : vs) {
        local.rejectUntilAttempt[v] = 1000;  // node 1 NEVER commits it
        local.hintFor[v] = 2;                // "node 2 leads it now"
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    co_await router.write(std::move(batch));

    EXPECT_EQ(local.attempts, 1u) << "the stale primary must be asked exactly once";
    EXPECT_EQ(client.calls, 1u);
    auto got = client.received[2];
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, vs) << "the retry must follow the hint to node 2";
}

// 3b: only the FAILED slice is re-dispatched. A batch spanning several VShards where one
// loses its leader must not re-propose the ones that already committed.
seastar::future<> testRetriesOnlyTheFailedSlice() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(60);
    const auto vs = vshardsOf(batch);
    EXPECT_GE(vs.size(), 3u);
    const uint16_t flapping = vs.front();

    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    local.rejectUntilAttempt[flapping] = 2;  // rejected on attempt 1, commits on attempt 2
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    co_await router.write(std::move(batch));

    EXPECT_EQ(local.attempts, 2u);
    if (local.seen.size() == 2) {
        EXPECT_EQ(local.seen[0].size(), vs.size()) << "attempt 1 dispatches everything";
        EXPECT_EQ(local.seen[1], std::vector<uint16_t>{flapping})
            << "attempt 2 must carry ONLY the slice that did not commit";
    }
    EXPECT_EQ(client.calls, 0u);
}

// 3b: an AMBIGUOUS failure (leadership lost after the entry was appended) is retried.
// This is the routine outcome of a leadership TRANSFER and is what the rolling-rebalance
// gate depends on; the re-apply is value-idempotent under LWW (see write_errors.hpp).
seastar::future<> testAmbiguousLeadershipLossIsRetried() {
    VShardDirectory dir(1, allLocalMap());
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    local.throwOnAttempt.push_back(
        std::make_exception_ptr(timestar::raft::LeadershipLostError("propose: leadership lost before commit")));
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    co_await router.write(manySeries(6));  // must NOT throw

    EXPECT_EQ(local.attempts, 2u) << "an ambiguous leadership loss must be retried, not surfaced";
}

// 3b: a FATAL failure is not retried and not downgraded -- it propagates as thrown, so
// an oversized frame stays a 413 and a journal fault stays visible.
seastar::future<> testFatalFailureIsNotRetried() {
    VShardDirectory dir(1, allLocalMap());
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    local.throwOnAttempt.push_back(std::make_exception_ptr(WriteFrameTooLargeError("too big")));
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    bool tooLarge = false;
    try {
        co_await router.write(manySeries(6));
    } catch (const WriteFrameTooLargeError&) {
        tooLarge = true;
    } catch (const std::exception& e) {
        ADD_FAILURE() << "expected the fatal error unchanged, got: " << e.what();
    }
    EXPECT_TRUE(tooLarge);
    EXPECT_EQ(local.attempts, 1u) << "a fatal failure must not spend the retry budget";
}

// 3b: a transport error to a peer is ambiguous+retryable; when the leader map advances
// in the meantime the retry lands on the new leader and the write succeeds.
seastar::future<> testTransportErrorRetriesAgainstTheAdvancedMap() {
    VShardDirectory dir(1, rf3Map(3));
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    WriteBatch batch = manySeries(40);
    // Point every VShard at node 2, which is "down" on the first attempt.
    for (uint16_t v : vshardsOf(batch))
        leaders.leaders[v] = 2;
    client.deadNodes.insert(2);
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    // Failover: after the first attempt, leadership has moved to node 3.
    auto f = router.write(std::move(batch));
    for (auto& [v, l] : leaders.leaders)
        l = 3;
    co_await std::move(f);

    EXPECT_FALSE(client.received[3].empty()) << "the retry must reach the new leader";
    EXPECT_EQ(local.attempts, 0u);
}

// F1 REGRESSION. A sink that reports a STRICT-SUBSET reject list while committing
// NOTHING must fail the whole write -- never ack the slices it merely failed to mention.
//
// This is the shape ReplicatedVShardHost's membership check produces by construction: it
// finds one group it does not host, proposes NONE of them, and names only that one. The
// router used to derive "uncommitted == rejects", so it retried that single VShard and
// ACKED the other 59 having never replicated a byte of them -- ack-without-commit, silent
// data loss. It is reachable in production because ClusterDataPlane::start opens the
// data-plane listener before instantiating the groups, so a joining or restarting node
// serves proposes for seconds while hosting only some of them.
//
// The old matched==0 guard did not catch it: one reject DID match the view, so `matched`
// was 1 and the partial sailed through. Pre-fix this test observed acked=true with 60
// VShards in the batch and 1 proposed.
seastar::future<> testStrictSubsetRejectNeverAcks() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(60);
    const auto vs = vshardsOf(batch);
    EXPECT_GE(vs.size(), 3u) << "the batch must span several VShards for this to be a real test";

    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    local.subsetRejectProposeNothing = true;  // commits nothing, names exactly one VShard
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    bool acked = false;
    try {
        co_await router.write(std::move(batch));
        acked = true;
    } catch (const RetryableWriteError&) {}

    // THE ASSERTION: an ack is only honest if every VShard in the batch was actually
    // proposed on the attempt that committed it. Attempt 1 proposed NOTHING, so a `true`
    // here means 59 slices were acked without ever reaching Raft.
    std::set<uint16_t> everCommitted;
    if (local.seen.size() >= 2)
        everCommitted.insert(local.seen.back().begin(), local.seen.back().end());
    EXPECT_TRUE(!acked || everCommitted.size() == vs.size())
        << "ACKED with " << vs.size() << " VShards in the batch but only " << everCommitted.size()
        << " ever proposed -- the reject list was trusted as the complete uncommitted set";
    EXPECT_TRUE(acked) << "with the committed-set contract the retry re-dispatches all " << vs.size()
                       << " slices and the write legitimately succeeds";
}

// The hostile/buggy-peer variant: a reply naming VShards that were never in the view must
// not remove anything from the retry set.
seastar::future<> testRejectsOutsideTheViewCannotAck() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(20);
    const auto vs = vshardsOf(batch);

    class LyingSink : public ProposeSink {
    public:
        unsigned attempts = 0;
        seastar::future<bool> proposeBatch(WriteBatch) override {
            return seastar::make_exception_future<bool>(std::runtime_error("unused"));
        }
        seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view) override {
            ++attempts;
            ProposeOutcome out;
            out.committed = false;
            // Claims to have committed VShards it was never asked about, and rejects one
            // that is not in the view either.
            out.committedVShards = {60000, 60001, 60002};
            out.rejects.push_back(SliceReject{59999, 4, WriteFailure::NotLeader});
            (void)view;
            return seastar::make_ready_future<ProposeOutcome>(std::move(out));
        }
    } local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    bool acked = false;
    try {
        co_await router.write(std::move(batch));
        acked = true;
    } catch (const RetryableWriteError&) {}
    EXPECT_FALSE(acked) << "a reply naming VShards outside the view must not ack anything";
    EXPECT_GE(vs.size(), 1u);
}

}  // namespace

TEST(ReplicatedBatchWriteRouterTest, StrictSubsetRejectNeverAcks) {
    testStrictSubsetRejectNeverAcks().get();
}
TEST(ReplicatedBatchWriteRouterTest, RejectsOutsideTheViewCannotAck) {
    testRejectsOutsideTheViewCannotAck().get();
}
TEST(ReplicatedBatchWriteRouterTest, RoutesEachSeriesToItsVShardLeader) { testRoutesToLeaders().get(); }
TEST(ReplicatedBatchWriteRouterTest, StaleLeaderFailsWholeWriteRetryably) {
    testStaleLeaderFailsWrite().get();
}
TEST(ReplicatedBatchWriteRouterTest, UnassignedVShardRejects) { testUnassignedRejects().get(); }
TEST(ReplicatedBatchWriteRouterTest, LeaderHintRedirectsTheRetry) {
    testLeaderHintRedirectsTheRetry().get();
}
TEST(ReplicatedBatchWriteRouterTest, RetriesOnlyTheFailedSlice) {
    testRetriesOnlyTheFailedSlice().get();
}
TEST(ReplicatedBatchWriteRouterTest, AmbiguousLeadershipLossIsRetried) {
    testAmbiguousLeadershipLossIsRetried().get();
}
TEST(ReplicatedBatchWriteRouterTest, FatalFailureIsNotRetried) {
    testFatalFailureIsNotRetried().get();
}
TEST(ReplicatedBatchWriteRouterTest, TransportErrorRetriesAgainstTheAdvancedMap) {
    testTransportErrorRetriesAgainstTheAdvancedMap().get();
}
