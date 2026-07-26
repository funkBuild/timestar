// Integration M3 + write-scaleout 3a/3b: ReplicatedBatchWriteRouter routes a WriteBatch
// to each VShard's LEADER (local ProposeSink vs remote proposeWrite), commits on quorum,
// follows a LEADER HINT when its guess was stale, retries ONLY the slices that failed,
// and fails the whole write retryably when the budget is spent -- never a silent partial
// ack. In-memory doubles (no sockets/Raft).
#include "../../../lib/cluster/data/replicated_write_router.hpp"

#include "../../../lib/cluster/reconnect_policy.hpp"
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

// A resolver that changes its mind: node 1 leads until it has been consulted
// `switchAfter` times, then node 2 does. It models the ordinary case a self-naming hint
// HIDES -- the leadership the coordinator was refusing for has MOVED, and only
// re-resolving can find it (write-scaleout 5 review, F1).
class SwitchingLeaderResolver : public LeaderResolver {
public:
    // Node 1 leads until the local sink has refused once (i.e. for the whole of attempt
    // 1); node 2 leads from then on. Keyed off the sink's own attempt counter so the
    // switch lands exactly on the attempt boundary, whatever order the router resolves
    // VShards in.
    const unsigned* localAttempts = nullptr;
    NodeId leaderOf(uint16_t) const override {
        return (localAttempts && *localAttempts > 0) ? static_cast<NodeId>(2) : static_cast<NodeId>(1);
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
    seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view, OptDeadline) override {
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

// ---------------------------------------------------------------------------
// F1 (write-scaleout 5 review): A SELF-NAMING HINT MUST NOT STEER THE RETRY.
//
// THE MECHANISM THIS PINS. `RaftGroup::propose*` returns a bare `false` for BOTH
// "role != Leader" and "I AM the leader but a transfer is in flight". The local sink used
// to label both `NotLeader` and attach `g->leader()` as the hint -- which, when the
// refuser IS the leader, is ITSELF. The router accepted that hint, re-bucketed the slice
// into its LOCAL view, and asked the same refusing group again. All six attempts went to
// the same place, NO RPC EVER LEFT THE NODE, and the client got a 503 saying "not-leader"
// -- which is why the one-node-down symptom read as a routing bug rather than a refusal.
//
// The router must DROP a hint that names the target that just rejected, so the next
// attempt re-resolves from the live leader view instead of looping.
seastar::future<> testSelfNamingHintIsIgnored() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(4);
    const auto vs = vshardsOf(batch);
    ScriptedLocalSink local;
    ScriptedTransport client;
    // Attempt 1 resolves LOCAL (node 1 leads); the local sink refuses and names ITSELF.
    // By attempt 2 the transfer has completed and the live view says node 2 -- which the
    // retry can only discover if the self-naming hint was DROPPED. If the hint is kept it
    // pins the slice to node 1 for the whole budget, the resolver is never consulted
    // again, and no RPC is ever made: the exact loop F1 removes.
    SwitchingLeaderResolver leaders;
    leaders.localAttempts = &local.attempts;
    for (uint16_t v : vs) {
        local.rejectUntilAttempt[v] = 1000;  // node 1 never commits it
        local.hintFor[v] = 1;                // ...and names ITSELF as the leader
        local.kindFor[v] = WriteFailure::LeaderRefused;
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    co_await router.write(std::move(batch));

    EXPECT_EQ(local.attempts, 1u)
        << "the refusing node was asked again: a hint naming the rejecter steered the retry back to it, "
        << "which is the six-attempts-no-RPC loop F1 removes";
    EXPECT_EQ(client.calls, 1u) << "the retry must leave the node";
    auto got = client.received[2];
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, vs) << "with the self-hint dropped, the retry re-resolves to the real leader";
}

// The 503's REASON must be what the target reported. Before F1 there was no way for a
// leader refusing its OWN proposal to say so -- the sink labelled it `NotLeader` -- so the
// 503 said "not-leader" and pointed the next investigator at routing, which was working
// correctly. This pins the new label all the way through to the client-visible message.
// (The router's unconditional `noteKind(NotLeader)` was scoped to the names-nothing case
// in the same change; that half is a clarity fix, not a behaviour change, because pacing
// takes the max over classes.)
seastar::future<> testRefusalReasonIsReportedNotManufactured() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(4);
    const auto vs = vshardsOf(batch);
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    for (uint16_t v : vs) {
        local.rejectUntilAttempt[v] = 1000;  // never commits, so the budget is exhausted
        local.kindFor[v] = WriteFailure::LeaderRefused;
        leaders.leaders[v] = 1;  // node 1 really is the leader; there is nowhere else to go
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    bool threw = false;
    try {
        co_await router.write(std::move(batch));
    } catch (const RetryableWriteError& e) {
        threw = true;
        const std::string what = e.what();
        EXPECT_NE(what.find("leader-refused-mid-transfer"), std::string::npos)
            << "the 503 manufactured its reason instead of reporting the one the target gave: " << what;
        EXPECT_EQ(what.find("not-leader"), std::string::npos)
            << "a leader that refused its own proposal must not be reported as not-leader: " << what;
    }
    EXPECT_TRUE(threw) << "a slice that never commits must fail the write";
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
        seastar::future<ProposeOutcome> proposeVShardBatchesHinted(VShardBatchView view, OptDeadline) override {
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

// write-scaleout 3f: the router must push a DEADLINE into every remote attempt.
//
// The overall budget is only checked BETWEEN attempts, so without this a peer that accepts
// the connection and then goes silent holds one attempt open indefinitely -- and the batch
// holds its WriteAdmission charge for as long as it does, taking the whole shard to 503
// behind it. The transport's own honouring of the deadline is seastar rpc's time_point
// overload; what is asserted here is that a deadline is SET, and set to the per-attempt
// bound rather than the whole 1.5s budget.
seastar::future<> testRemoteAttemptsCarryADeadline() {
    VShardDirectory dir(1, rf3Map(3));

    class DeadlineRecordingTransport : public NodeTransport {
    public:
        std::vector<OptDeadline> deadlines;
        seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
        seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
            return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
        }
        seastar::future<ProposeOutcome> proposeWriteHinted(NodeId, VShardBatchView view,
                                                           OptDeadline deadline) override {
            deadlines.push_back(deadline);
            ProposeOutcome out;
            for (const auto* g : view)
                out.committedVShards.push_back(g->first);
            out.committed = true;
            return seastar::make_ready_future<ProposeOutcome>(std::move(out));
        }
    } client;
    ScriptedLocalSink local;
    MapLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    const auto before = seastar::lowres_clock::now();
    co_await router.write(manySeries(60));

    EXPECT_FALSE(client.deadlines.empty()) << "the batch must have reached remote leaders";
    for (const auto& d : client.deadlines) {
        EXPECT_TRUE(d.has_value()) << "a remote attempt with NO deadline can hang forever";
        if (!d)
            continue;
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(*d - before).count();
        EXPECT_GT(ms, 0) << "the deadline is already in the past";
        EXPECT_LE(
            ms,
            std::chrono::duration_cast<std::chrono::milliseconds>(ReplicatedBatchWriteRouter::kAttemptTimeout).count() +
                50)
            << "an attempt got " << ms << "ms, i.e. more than the per-attempt bound -- one silent peer "
            << "would then hold the whole write budget";
        EXPECT_LE(ms,
                  std::chrono::duration_cast<std::chrono::milliseconds>(ReplicatedBatchWriteRouter::kDeadline).count())
            << "an attempt deadline may never exceed the overall one";
    }
}

// ---------------------------------------------------------------------------
// write-scaleout 4a: the retry schedule must OUTLAST the transport's reconnect backoff.
//
// `DataPlaneRpc::clientFor` hands back the DEAD client for `cluster::kReconnectBackoff`
// after a connection dies, so every attempt inside that window fast-fails on the same
// dead socket without the transport ever being asked to re-dial. With the pre-4a flat
// 20 ms pause the whole 6-attempt budget (5 x 20 = 100 ms) fitted INSIDE one 200 ms
// window: the retry loop existed but could not reach the thing it was retrying, and a
// TCP blip against a healthy peer became a client 5xx. [D6]
//
// This transport models exactly that: it throws for the first `blip` of wall-clock time
// and commits after. It is failing PURELY on elapsed time, so it measures the schedule.
class BlipTransport : public NodeTransport {
public:
    std::chrono::milliseconds blip{0};
    seastar::lowres_clock::time_point start;
    unsigned attempts = 0;

    seastar::future<> forwardWriteBatch(NodeId, WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<NodeQueryPartial> queryNode(NodeId, NodeQueryRequest) override {
        return seastar::make_exception_future<NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<ProposeOutcome> proposeWriteHinted(NodeId, VShardBatchView view, OptDeadline) override {
        ++attempts;
        if (seastar::lowres_clock::now() < start + blip)
            return seastar::make_exception_future<ProposeOutcome>(std::runtime_error("connection is closed"));
        ProposeOutcome out;
        for (const auto* g : view)
            out.committedVShards.push_back(g->first);
        out.committed = true;
        return seastar::make_ready_future<ProposeOutcome>(std::move(out));
    }
};

seastar::future<> testBlipLongerThanTheBackoffIsAbsorbed() {
    VShardDirectory dir(1, rf3Map(3));
    BlipTransport client;
    // A blip strictly LONGER than one reconnect window. Under the pre-4a flat schedule
    // the six attempts are all spent before this elapses and the write fails.
    client.blip = timestar::cluster::kReconnectBackoff + std::chrono::milliseconds(60);
    client.start = seastar::lowres_clock::now();
    ScriptedLocalSink local;
    MapLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    // Must NOT throw: a transient reset resolves inside the write deadline.
    co_await router.write(manySeries(60));
    EXPECT_GT(client.attempts, 1u) << "the blip must actually have been retried";
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(seastar::lowres_clock::now() - client.start);
    EXPECT_LE(elapsed.count(),
              std::chrono::duration_cast<std::chrono::milliseconds>(ReplicatedBatchWriteRouter::kDeadline).count())
        << "absorbing the blip must still respect the write deadline";
}

}  // namespace

// The pacing table itself, checked as arithmetic rather than by running the clock. This is
// the invariant that makes the behavioural test above possible; if someone shortens the
// schedule or lengthens the transport backoff, THIS is what says so.
TEST(WriteRetryPacingTest, TransportScheduleOutlastsTheReconnectBackoff) {
    using namespace timestar::data;
    const auto span = writeRetryScheduleSpan(WriteFailure::Transport, ReplicatedBatchWriteRouter::kMaxAttempts);
    EXPECT_GT(span.count(), 2 * timestar::cluster::kReconnectBackoff.count())
        << "a full retry budget must cover several reconnect windows, or every attempt "
        << "fast-fails on the same dead client (write-scaleout 4a / [D6])";
    EXPECT_LT(span.count(),
              std::chrono::duration_cast<std::chrono::milliseconds>(ReplicatedBatchWriteRouter::kDeadline).count())
        << "the pauses alone must not exhaust the write deadline -- the attempts need room too";
    // The pre-4a schedule, stated so the regression is legible: a FLAT base delay on every
    // attempt covers (kMaxAttempts-1) x 20 ms = 100 ms, which is HALF a reconnect window.
    // That is the whole of [D6] in one line of arithmetic.
    const auto flatSpan = kWriteRetryDelayBase * (ReplicatedBatchWriteRouter::kMaxAttempts - 1);
    EXPECT_LT(flatSpan.count(), timestar::cluster::kReconnectBackoff.count())
        << "sanity: the flat schedule this replaced really did fit inside one backoff window";
}

TEST(WriteRetryPacingTest, LeaderShapedFailuresStayFast) {
    using namespace timestar::data;
    // A not-leader retry goes to a DIFFERENT node; backing off would add hundreds of ms of
    // p99 to every routine leadership transfer for no availability gain. That is why the
    // FIRST retries stay at the base delay -- a transfer completes in single-digit ms, so
    // it is always absorbed inside this window.
    //
    // (Past kFastLeaderRetries the same classes DO escalate, to span an election rather
    // than a transfer -- debt D-14, pinned by
    // WriteRetryPacing.ElectionShapedClassesEscalateAfterTheFastRetries. This test owns the
    // fast half.)
    for (unsigned a = 1; a <= kFastLeaderRetries; ++a) {
        EXPECT_EQ(writeFailureRetryDelay(WriteFailure::NotLeader, a), kWriteRetryDelayBase);
        EXPECT_EQ(writeFailureRetryDelay(WriteFailure::LeadershipLost, a), kWriteRetryDelayBase);
    }
    // ShardStopping is a pure RE-ROUTE, so it never escalates at all.
    for (unsigned a = 1; a <= ReplicatedBatchWriteRouter::kMaxElectionAttempts; ++a)
        EXPECT_EQ(writeFailureRetryDelay(WriteFailure::ShardStopping, a), kWriteRetryDelayBase);
    // ... while transport-shaped ones grow, and are capped.
    EXPECT_EQ(writeFailureRetryDelay(WriteFailure::Transport, 1), kWriteRetryDelayBase);
    EXPECT_GT(writeFailureRetryDelay(WriteFailure::Transport, 3), writeFailureRetryDelay(WriteFailure::Transport, 1));
    EXPECT_EQ(writeFailureRetryDelay(WriteFailure::Transport, 99), kWriteRetryDelayMax);
}

TEST(WriteRetryPacingTest, AmbiguityTaxonomyIsUnchangedByThePacing) {
    using namespace timestar::data;
    // 4a changes WHEN a class is retried, never WHETHER. The 3b taxonomy is the contract.
    EXPECT_TRUE(isRetryableWriteFailure(WriteFailure::Transport));
    EXPECT_TRUE(isRetryableWriteFailure(WriteFailure::LeadershipLost));
    EXPECT_TRUE(isAmbiguousWriteFailure(WriteFailure::Transport));
    EXPECT_TRUE(isAmbiguousWriteFailure(WriteFailure::LeadershipLost));
    EXPECT_FALSE(isRetryableWriteFailure(WriteFailure::Unassigned));
    EXPECT_FALSE(isRetryableWriteFailure(WriteFailure::Fatal));
}

TEST(ReplicatedBatchWriteRouterTest, BlipLongerThanTheReconnectBackoffIsAbsorbed) {
    testBlipLongerThanTheBackoffIsAbsorbed().get();
}

TEST(ReplicatedBatchWriteRouterTest, RemoteAttemptsCarryADeadline) {
    testRemoteAttemptsCarryADeadline().get();
}
TEST(ReplicatedBatchWriteRouterTest, StrictSubsetRejectNeverAcks) {
    testStrictSubsetRejectNeverAcks().get();
}
TEST(ReplicatedBatchWriteRouterTest, RejectsOutsideTheViewCannotAck) {
    testRejectsOutsideTheViewCannotAck().get();
}
TEST(ReplicatedBatchWriteRouterTest, RoutesEachSeriesToItsVShardLeader) {
    testRoutesToLeaders().get();
}
TEST(ReplicatedBatchWriteRouterTest, StaleLeaderFailsWholeWriteRetryably) {
    testStaleLeaderFailsWrite().get();
}
TEST(ReplicatedBatchWriteRouterTest, UnassignedVShardRejects) {
    testUnassignedRejects().get();
}
TEST(ReplicatedBatchWriteRouterTest, LeaderHintRedirectsTheRetry) {
    testLeaderHintRedirectsTheRetry().get();
}
TEST(ReplicatedBatchWriteRouterTest, SelfNamingHintIsIgnored) {
    testSelfNamingHintIsIgnored().get();
}
TEST(ReplicatedBatchWriteRouterTest, RefusalReasonIsReportedNotManufactured) {
    testRefusalReasonIsReportedNotManufactured().get();
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

// --- debt D-14: the election window -------------------------------------------------
//
// A batch whose only remaining problem is that a group has no leader YET is waiting on an
// election (2.5-5 s at production timing) against a 1.5 s deadline, so it failed -- and
// with ~1000 VShards per batch and ~1/3 of groups re-electing after a node dies, nearly
// every batch in that window failed. That is the measured one-node-down 503 band.

// An election-shaped failure may outlast the BASE budget: 8 attempts is past kMaxAttempts
// (6) and the pacing past the third retry is 400 ms, so this write only completes if both
// halves of the extension are in place.
seastar::future<> testElectionShapedFailureOutlastsTheBaseBudget() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(4);
    const auto vs = vshardsOf(batch);
    ScriptedLocalSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    for (uint16_t v : vs) {
        leaders.leaders[v] = 1;  // we lead it; nobody else to ask
        local.rejectUntilAttempt[v] = 8;
        local.kindFor[v] = WriteFailure::NotLeader;  // "no leader yet" -- an election is running
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);

    const auto t0 = std::chrono::steady_clock::now();
    co_await router.write(std::move(batch));  // must NOT throw
    const auto elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_EQ(local.attempts, 8u) << "the batch gave up before the election could complete";
    EXPECT_GT(local.attempts, ReplicatedBatchWriteRouter::kMaxAttempts)
        << "it committed inside the BASE attempt budget, so this proves nothing about the extension";
    // Elapsed is asserted only as an UPPER bound: the pauses are jittered +/-25%, so a
    // lower bound on wall-clock time would be a flaky way to say what the attempt count
    // above says exactly.
    EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
              ReplicatedBatchWriteRouter::kElectionDeadline.count())
        << "the wait must stay inside the election cap";
}

// THE OTHER HALF, and the one that keeps [D6] intact: a TRANSPORT failure must NOT buy the
// extended window. A dead peer is not waiting for an election, and holding the batch (and
// its in-flight-bytes charge) for 6 s against a peer that is simply gone is the opposite of
// what 4a is for.
seastar::future<> testTransportFailureDoesNotGetTheElectionWindow() {
    VShardDirectory dir(1, rf3Map(3));
    WriteBatch batch = manySeries(30);
    ScriptedLocalSink local;
    ScriptedTransport client;
    client.deadNodes = {2, 3};  // every remote slice fails on the transport, forever
    NoLeaderResolver leaders;   // route by placement primary
    for (uint16_t v : vshardsOf(batch))
        local.rejectUntilAttempt[v] = 0;  // our own slices commit; only the remote ones fail

    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    const auto t0 = std::chrono::steady_clock::now();
    bool threw = false;
    try {
        co_await router.write(std::move(batch));
    } catch (const RetryableWriteError& e) {
        threw = true;
        EXPECT_NE(std::string(e.what()).find("transport"), std::string::npos) << e.what();
    }
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_TRUE(threw) << "a permanently dead peer must still fail the batch closed";
    EXPECT_LT(ms, ReplicatedBatchWriteRouter::kElectionDeadline.count())
        << "a transport failure was granted the election window (" << ms << " ms) -- [D6] regressed";
    EXPECT_LT(ms, 2500) << "it should fail at the BASE deadline (~1.5 s), not the extended one";
}

// A MIXED batch is judged pessimistically: one transport failure puts the WHOLE batch back
// on the base deadline. Otherwise a single leaderless slice would buy 6 s of patience that
// a dead peer then spends.
seastar::future<> testOneTransportFailureRevokesTheElectionWindow() {
    VShardDirectory dir(1, rf3Map(3));
    WriteBatch batch = manySeries(30);
    ScriptedLocalSink local;
    ScriptedTransport client;
    client.deadNodes = {3};  // node 3's slices: transport
    NoLeaderResolver leaders;
    for (uint16_t v : vshardsOf(batch)) {
        local.rejectUntilAttempt[v] = 1000;  // our slices: election-shaped, forever
        local.kindFor[v] = WriteFailure::NotLeader;
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_THROW(co_await router.write(std::move(batch)), RetryableWriteError);
    const auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    EXPECT_LT(ms, 2500) << "the mixed batch was granted the election window (" << ms << " ms)";
}

// The wake seam: a write that GIVES UP with a peer unreachable asks the local plane to
// un-hibernate the groups that still believe that peer leads them, so the election runs at
// the normal timeout rather than the 1-in-10 hibernation-stretched one.
class WakeCountingSink : public ScriptedLocalSink {
public:
    std::map<NodeId, size_t> wakes;
    size_t wakeGroupsLedBy(NodeId n) override {
        ++wakes[n];
        return 7;  // pretend some groups were woken, so the log line is exercised too
    }
};

seastar::future<> testUnreachablePeerWakesItsGroups() {
    VShardDirectory dir(1, rf3Map(3));
    WriteBatch batch = manySeries(30);
    WakeCountingSink local;
    ScriptedTransport client;
    client.deadNodes = {2, 3};
    NoLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    EXPECT_THROW(co_await router.write(std::move(batch)), RetryableWriteError);

    EXPECT_GE(local.wakes[2] + local.wakes[3], 1u) << "an unreachable peer's groups were never woken";
    // ONE wake per peer for the whole write: it happens at give-up, not per attempt.
    EXPECT_EQ(local.wakes[2], 1u);
    EXPECT_EQ(local.wakes[3], 1u);
}

// THE [D6] SHAPE: a connection that dies and comes back is absorbed by the retry budget,
// and must leave the Raft groups completely alone. Waking here put a third of the map on
// full-rate ticking during a reset storm, and followers ticking at full rate through
// dropped heartbeats campaign against a leader that is perfectly alive -- measured as 3
// bench 503s on the reset gate.
class RecoveringTransport : public ScriptedTransport {
public:
    unsigned failAttempts = 2;
    unsigned seen = 0;
    seastar::future<ProposeOutcome> proposeWriteHinted(NodeId to, VShardBatchView view, OptDeadline d) override {
        if (++seen <= failAttempts)
            return seastar::make_exception_future<ProposeOutcome>(std::runtime_error("connection is closed"));
        return ScriptedTransport::proposeWriteHinted(to, view, d);
    }
};

seastar::future<> testRecoveredBlipNeverWakes() {
    VShardDirectory dir(1, rf3Map(3));
    WriteBatch batch = manySeries(30);
    WakeCountingSink local;
    RecoveringTransport client;
    NoLeaderResolver leaders;
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    co_await router.write(std::move(batch));  // recovers inside the budget
    EXPECT_TRUE(local.wakes.empty()) << "a blip that the retry absorbed still disturbed the Raft groups";
}

// A LOCAL failure must not wake anything: there is no unreachable peer to wake groups
// behind, and the node we would name is ourselves.
seastar::future<> testLocalFailureDoesNotWake() {
    VShardDirectory dir(1, allLocalMap());
    WriteBatch batch = manySeries(4);
    WakeCountingSink local;
    ScriptedTransport client;
    MapLeaderResolver leaders;
    for (uint16_t v : vshardsOf(batch)) {
        leaders.leaders[v] = 1;
        local.rejectUntilAttempt[v] = 2;
        local.kindFor[v] = WriteFailure::NotLeader;
    }
    ReplicatedBatchWriteRouter router(dir, local, client, leaders);
    co_await router.write(std::move(batch));
    EXPECT_TRUE(local.wakes.empty());
}

// The pacing table itself: fast while a transfer is plausible, then spanning an election.
TEST(WriteRetryPacing, ElectionShapedClassesEscalateAfterTheFastRetries) {
    for (auto f : {WriteFailure::NotLeader, WriteFailure::LeaderRefused, WriteFailure::LeadershipLost}) {
        EXPECT_TRUE(isElectionWaitFailure(f));
        for (unsigned a = 1; a <= kFastLeaderRetries; ++a)
            EXPECT_EQ(writeFailureRetryDelay(f, a), kWriteRetryDelayBase) << "attempt " << a;
        EXPECT_EQ(writeFailureRetryDelay(f, kFastLeaderRetries + 1), kElectionRetryDelay);
        EXPECT_EQ(writeFailureRetryDelay(f, 12), kElectionRetryDelay);
    }
    // Transport is untouched -- it keeps the 4a geometric schedule.
    EXPECT_FALSE(isElectionWaitFailure(WriteFailure::Transport));
    EXPECT_FALSE(isElectionWaitFailure(WriteFailure::Overloaded));
    EXPECT_EQ(writeFailureRetryDelay(WriteFailure::Transport, 1), kWriteRetryDelayBase);
    EXPECT_EQ(writeFailureRetryDelay(WriteFailure::Transport, 6), kWriteRetryDelayMax);
    // ShardStopping is a re-route, not a wait.
    EXPECT_EQ(writeFailureRetryDelay(WriteFailure::ShardStopping, 9), kWriteRetryDelayBase);
}

TEST(ReplicatedBatchWriteRouterTest, ElectionShapedFailureOutlastsTheBaseBudget) {
    testElectionShapedFailureOutlastsTheBaseBudget().get();
}
TEST(ReplicatedBatchWriteRouterTest, TransportFailureDoesNotGetTheElectionWindow) {
    testTransportFailureDoesNotGetTheElectionWindow().get();
}
TEST(ReplicatedBatchWriteRouterTest, OneTransportFailureRevokesTheElectionWindow) {
    testOneTransportFailureRevokesTheElectionWindow().get();
}
TEST(ReplicatedBatchWriteRouterTest, UnreachablePeerWakesItsGroups) {
    testUnreachablePeerWakesItsGroups().get();
}
TEST(ReplicatedBatchWriteRouterTest, LocalFailureDoesNotWake) {
    testLocalFailureDoesNotWake().get();
}
TEST(ReplicatedBatchWriteRouterTest, RecoveredBlipNeverWakes) {
    testRecoveredBlipNeverWakes().get();
}

TEST(ReplicatedBatchWriteRouterTest, TransportErrorRetriesAgainstTheAdvancedMap) {
    testTransportErrorRetriesAgainstTheAdvancedMap().get();
}
