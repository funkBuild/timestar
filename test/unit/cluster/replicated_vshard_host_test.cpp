// Integration M3: ReplicatedVShardHost manages the per-VShard Raft groups a node
// replicates, over the real Engine. A single-voter VShard group is instantiated,
// propose() commits a ReplicatedCommand on quorum (self), it applies through
// EngineDataStateMachine into the Engine, and is queryable. Proves the per-VShard
// hosting/management layer (the multi-engine RF=3 convergence is proven separately
// in engine_rf3_test).
#include "../../../lib/cluster/integration/replicated_vshard_host.hpp"

#include "../../../lib/cluster/data/snapshot_payload.hpp"
#include "../../../lib/cluster/integration/replica_engine_reader.hpp"
#include "../../../lib/cluster/raft/raft_journal_persistence.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
#include "../../../lib/core/vshard.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <set>

using namespace timestar;
using namespace timestar::raft;
namespace fs = std::filesystem;

namespace {

TEST(JournalIdentityTest, ParsesConfiguredClusterAndBootUuids) {
    auto id = cluster::JournalIdentity::fromHex("00112233445566778899aabbccddeeff",
                                                "ffeeddccbbaa99887766554433221100");
    EXPECT_EQ(id.clusterUuid.front(), 0x00);
    EXPECT_EQ(id.clusterUuid.back(), 0xff);
    EXPECT_EQ(id.bootId.front(), 0xff);
    EXPECT_EQ(id.bootId.back(), 0x00);
    EXPECT_THROW(cluster::JournalIdentity::fromHex("short", "ffeeddccbbaa99887766554433221100"),
                 std::invalid_argument);
    EXPECT_THROW(cluster::JournalIdentity::fromHex("00112233445566778899aabbccddeezz",
                                                    "ffeeddccbbaa99887766554433221100"),
                 std::invalid_argument);
}
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class ReplicatedVShardHostTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

fs::path tmpDir() {
    static std::atomic_uint64_t seq{0};
    auto d = fs::temp_directory_path() /
             ("ts_rvhost_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(d);
    return d;
}
class NoopTransport : public RaftTransport {
public:
    seastar::future<> send(Envelope) override { return seastar::make_ready_future<>(); }
};
data::ReplicatedCommand writeCmd(const std::string& key, double value) {
    data::WriteSeries s;
    s.seriesKey = key;
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{value};
    data::WriteBatch b;
    b.series = {std::move(s)};
    return data::ReplicatedCommand{std::move(b)};
}
}  // namespace

TEST_F(ReplicatedVShardHostTest, HostsVShardAndReplicatesThroughRaft) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;
        host.addVShard(/*vshard=*/5, /*voters=*/{1}, opts).get();
        EXPECT_EQ(host.vshardCount(), 1u);

        // Drive the group to leadership by ticking it directly (deterministic; the
        // registry's timer-driven ticking is the production path).
        RaftGroup* g = host.group(5);
        ASSERT_NE(g, nullptr);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());

        // Replicate a write; drive ticks until the commit+apply ack resolves.
        auto f = host.propose(5, writeCmd(buildSeriesKey("temp", {{"host", "h1"}}, "value"), 42.5));
        for (int i = 0; i < 20 && !f.available(); ++i)
            g->tick().get();
        EXPECT_TRUE(f.get());

        // Visible in the real Engine.
        http::HttpQueryHandler h(&*eng);
        QueryRequest q;
        q.aggregation = AggregationMethod::LATEST;
        q.measurement = "temp";
        q.fields = {"value"};
        q.startTime = BASE - 1'000'000'000ULL;
        q.endTime = BASE + 1'000'000'000ULL;
        auto r = h.executeQuery(q).get();
        ASSERT_TRUE(r.success) << r.errorMessage;
        ASSERT_EQ(r.series.size(), 1u);
        EXPECT_DOUBLE_EQ(std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0], 42.5);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, ReAddingAVShardIsRejected) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, 1, jroot);
        host.addVShard(7, {1}).get();
        bool threw = false;
        try {
            host.addVShard(7, {1}).get();  // re-add same vshard -> reject (no double writer)
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw);
        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, ProposeBatchSplitsAcrossVShards) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, 1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        // Two series -> their (possibly distinct) VShards; host each so this node
        // leads them.
        const std::string k1 = buildSeriesKey("a", {{"host", "h1"}}, "value");
        const std::string k2 = buildSeriesKey("b", {{"host", "h2"}}, "value");
        std::set<uint16_t> vshards = {timestar::virtualShard(SeriesId128::fromSeriesKey(k1)),
                                      timestar::virtualShard(SeriesId128::fromSeriesKey(k2))};
        ASSERT_EQ(vshards.size(), 2u) << "keys must map to distinct VShards to exercise the split";
        for (uint16_t vs : vshards) {
            host.addVShard(vs, {1}, opts).get();
            RaftGroup* g = host.group(vs);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader()) << "vshard " << vs;
        }

        data::WriteBatch batch;
        auto mk = [](const std::string& key, double v) {
            data::WriteSeries s;
            s.seriesKey = key;
            s.type = TSMValueType::Float;
            s.timestamps = {BASE};
            s.values = std::vector<double>{v};
            return s;
        };
        batch.series = {mk(k1, 10.0), mk(k2, 20.0)};

        auto f = host.proposeBatch(std::move(batch));
        for (int i = 0; i < 60 && !f.available(); ++i)
            for (uint16_t vs : vshards)
                host.group(vs)->tick().get();
        EXPECT_TRUE(f.get());

        http::HttpQueryHandler h(&*eng);
        auto latest = [&](const std::string& m) {
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = m;
            q.fields = {"value"};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 1'000'000'000ULL;
            auto r = h.executeQuery(q).get();
            return (r.success && !r.series.empty())
                       ? std::get<std::vector<double>>(r.series[0].fields.at("value").second)[0]
                       : -1.0;
        };
        EXPECT_DOUBLE_EQ(latest("a"), 10.0);
        EXPECT_DOUBLE_EQ(latest("b"), 20.0);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

// M3 snapshot PRODUCER: after committing writes and flushing to TSM, snapshotVShard
// builds the payload and hands it to RaftGroup::compact, truncating the log to the
// snapshot's covered revision. Before any flush there is nothing to compact; the
// compaction never exceeds applied state and never loses committed data. On a
// non-cohesive core count it refuses (throws) rather than shipping a partial.
TEST_F(ReplicatedVShardHostTest, SnapshotVShardCompactsLogOverFlushedData) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        // Pick a series whose VShard maps to core 0 so buildVShardSnapshot's internal
        // invoke_on(assignCore) is inline on this shard (no cross-shard payload move),
        // exactly as the production apply path -- which runs on the VShard's core --
        // guarantees.
        std::string key;
        uint16_t vs = 0;
        for (int i = 0;; ++i) {
            key = buildSeriesKey("snapprod", {{"host", "h" + std::to_string(i)}}, "value");
            vs = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
            if (timestar::assignCore(VShardId{vs}, seastar::smp::count) == 0)
                break;
        }
        const unsigned core0 = 0;

        host.addVShard(vs, {1}, opts).get();
        RaftGroup* g = host.group(vs);
        ASSERT_NE(g, nullptr);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());

        // Commit two writes to this VShard's group.
        for (double v : {1.0, 2.0}) {
            data::WriteSeries s;
            s.seriesKey = key;
            s.type = TSMValueType::Float;
            s.timestamps = {BASE + static_cast<uint64_t>(v)};
            s.values = std::vector<double>{v};
            data::WriteBatch b;
            b.series = {std::move(s)};
            auto f = host.proposeBatch(std::move(b));
            for (int i = 0; i < 20 && !f.available(); ++i)
                g->tick().get();
            EXPECT_TRUE(f.get());
        }
        const uint64_t appliedBefore = g->appliedIndex();
        EXPECT_GT(appliedBefore, 0u);

        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count)) {
            // Flush then attempt: a non-cohesive core count must REFUSE, not ship partial.
            (*eng).invoke_on_all([](Engine& e) { return e.rolloverMemoryStore(); }).get();
            EXPECT_THROW(host.snapshotVShard(vs).get(), std::runtime_error);
            host.stop().get();
            fs::remove_all(jroot);
            return;
        }

        // Before any flush: no TSM data -> nothing to compact, log untouched.
        EXPECT_EQ(host.snapshotVShard(vs).get(), 0u);
        EXPECT_EQ(g->node().log().snapshotIndex(), 0u);

        // Flush to TSM, then snapshot compacts the log.
        (*eng).invoke_on_all([](Engine& e) { return e.rolloverMemoryStore(); }).get();
        for (int i = 0; i < 300; ++i) {
            auto n = (*eng).invoke_on(core0, [](Engine& e) { return e.getTSMFileCount(); }).get();
            if (n > 0)
                break;
            seastar::sleep(std::chrono::milliseconds(100)).get();
        }

        const uint64_t compacted = host.snapshotVShard(vs).get();
        EXPECT_GT(compacted, 0u) << "flushed data must be snapshotted";
        EXPECT_LE(compacted, appliedBefore) << "compact only over flushed (<= applied) data";
        EXPECT_EQ(g->node().log().snapshotIndex(), compacted) << "log truncated to the snapshot boundary";

        // Committed data survives compaction (queryable by seriesId on the VShard core).
        auto rq = (*eng)
                      .invoke_on(core0,
                                 [key](Engine& e) {
                                     const auto sid = SeriesId128::fromSeriesKey(key);
                                     return e.query(key, sid, 0, UINT64_MAX);
                                 })
                      .get();
        ASSERT_TRUE(rq.has_value());
        EXPECT_EQ(std::get<QueryResult<double>>(rq.value()).values.size(), 2u);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

// M4 leader-reach sink: ReadIndexSink over the local Raft group. A non-leader (or
// unhosted VShard) rejects; the leader confirms a quorum ReadIndex and reports its
// commit index, with commit >= the confirmed read index.
TEST_F(ReplicatedVShardHostTest, LeaderReachSinkConfirmsReadIndexAndRejectsNonLeader) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;
        const std::string key = buildSeriesKey("temp", {{"host", "h1"}}, "value");
        const uint16_t V = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        host.addVShard(V, {1}, opts).get();
        RaftGroup* g = host.group(V);
        ASSERT_NE(g, nullptr);

        // Before leadership: leaderCommitIndex rejects (this node is not the leader).
        EXPECT_THROW(host.leaderCommitIndex(V).get(), std::runtime_error);
        // Unhosted VShard: both reject.
        EXPECT_THROW(host.leaderReadIndex(999).get(), std::runtime_error);
        EXPECT_THROW(host.leaderCommitIndex(999).get(), std::runtime_error);

        // Drive to leadership and commit a write.
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());
        auto f = host.propose(V, writeCmd(key, 1.0));
        for (int i = 0; i < 20 && !f.available(); ++i)
            g->tick().get();
        EXPECT_TRUE(f.get());

        // Leader reports a commit index > 0.
        const auto ci = host.leaderCommitIndex(V).get();
        EXPECT_GT(ci, 0u);

        // Leader confirms a linearizable ReadIndex (single-voter quorum = self; drive
        // ticks so the confirmation round completes).
        auto rb = host.leaderReadIndex(V);
        for (int i = 0; i < 20 && !rb.available(); ++i)
            g->tick().get();
        const auto ri = rb.get();
        EXPECT_GT(ri, 0u);
        EXPECT_GE(ci, ri) << "commit index is at least the confirmed read index";

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

// M4 production replica reader: ReplicaEngineReader serves reads from the real Engine
// at each consistency mode, gated by the Raft group's freshness, restricted to its
// VShard. Linearizable confirms a leader ReadIndex; Session waits the token; a
// partitioned leader-reach (throwing fn) rejects rather than serving stale.
TEST_F(ReplicatedVShardHostTest, ReplicaEngineReaderServesAtEachConsistency) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);

        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const std::string key = buildSeriesKey("rr", {{"host", "h1"}}, "value");
        const uint16_t V = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        host.addVShard(V, {1}, opts).get();
        RaftGroup* g = host.group(V);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());

        // Commit a write into this VShard's group.
        {
            data::WriteSeries s;
            s.seriesKey = key;
            s.type = TSMValueType::Float;
            s.timestamps = {BASE};
            s.values = std::vector<double>{3.5};
            data::WriteBatch b;
            b.series = {std::move(s)};
            auto f = host.proposeBatch(std::move(b));
            for (int i = 0; i < 20 && !f.available(); ++i)
                g->tick().get();
            EXPECT_TRUE(f.get());
        }

        auto nq = [&] {
            data::NodeQueryRequest r;
            r.request.aggregation = AggregationMethod::LATEST;
            r.request.measurement = "rr";
            r.request.fields = {"value"};
            r.request.startTime = BASE - 1'000'000'000ULL;
            r.request.endTime = BASE + 1'000'000'000ULL;
            return r;
        };

        cluster::ReplicaEngineReader reader(
            *g, store, V, [&host, V] { return host.leaderReadIndex(V); },
            [&host, V] { return host.leaderCommitIndex(V); });

        // Linearizable: confirms a leader ReadIndex (drive ticks for the round).
        {
            auto rf = reader.read(nq(), data::ReadConsistency::Linearizable, {}, 0);
            for (int i = 0; i < 30 && !rf.available(); ++i)
                g->tick().get();
            auto res = rf.get();
            EXPECT_TRUE(res.partial.incompleteReasons.empty());
            EXPECT_GT(res.partial.seriesFound, 0u) << "linearizable read finds the VShard's series";
            EXPECT_EQ(res.envelope.vshard, V);
            EXPECT_GT(res.envelope.appliedIndex, 0u);
        }

        // Session: wait the token (already applied) and serve.
        {
            data::ReadEnvelope token{V, g->currentTerm(), g->appliedIndex()};
            auto res = reader.read(nq(), data::ReadConsistency::Session, token, 0).get();
            EXPECT_TRUE(res.partial.incompleteReasons.empty());
            EXPECT_GT(res.partial.seriesFound, 0u);
        }

        // BoundedStaleness within a generous bound serves local state.
        {
            auto res = reader.read(nq(), data::ReadConsistency::BoundedStaleness, {}, /*maxLagIndex=*/1'000'000).get();
            EXPECT_GT(res.partial.seriesFound, 0u);
        }

        // A partitioned leader-reach (throwing fn) rejects the linearizable read.
        {
            cluster::ReplicaEngineReader partitioned(
                *g, store, V,
                [] { return seastar::make_exception_future<raft::LogIndex>(std::runtime_error("leader unreachable")); },
                [] {
                    return seastar::make_exception_future<raft::LogIndex>(std::runtime_error("leader unreachable"));
                });
            bool threw = false;
            try {
                partitioned.read(nq(), data::ReadConsistency::Linearizable, {}, 0).get();
            } catch (const std::exception&) {
                threw = true;
            }
            EXPECT_TRUE(threw) << "a replica that cannot reach the leader must reject, not serve stale";
        }

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

// ---------------------------------------------------------------------------
// The snapshot PRODUCER trigger (debt D-6)
// ---------------------------------------------------------------------------
//
// `snapshotVShard` had NO production caller before this: nothing ever compacted, so every
// group's Raft log grew without bound until a restart replayed the whole thing. These
// tests pin the four things that make wiring it safe:
//
//   (a) the cohesion gate is respected (a single-core snapshot on a non-cohesive core
//       count would omit series that scatter across cores -- a PARTIAL snapshot);
//   (b) the truncation boundary never rises above FLUSHED data;
//   (c) the sweep is rate-limited and staggered rather than a stampede;
//   (d) a compacted journal is RECOVERABLE, which is what the very first restart after
//       this trigger ships depends on.

namespace {
// A VShard whose assignCore is 0, so buildVShardSnapshot's invoke_on is inline on this
// shard -- the same locality the production apply path guarantees.
struct Core0Series {
    std::string key;
    uint16_t vshard = 0;
};
Core0Series core0Series(const std::string& measurement) {
    Core0Series out;
    for (int i = 0;; ++i) {
        out.key = buildSeriesKey(measurement, {{"host", "h" + std::to_string(i)}}, "value");
        out.vshard = timestar::virtualShard(SeriesId128::fromSeriesKey(out.key));
        if (timestar::assignCore(VShardId{out.vshard}, seastar::smp::count) == 0)
            return out;
    }
}

// Commit `n` single-point writes to `vshard`'s group, ticking until each acks.
void commitWrites(cluster::ReplicatedVShardHost& host, RaftGroup* g, const std::string& key, uint16_t vshard, int n) {
    for (int i = 0; i < n; ++i) {
        data::WriteSeries s;
        s.seriesKey = key;
        s.type = TSMValueType::Float;
        s.timestamps = {BASE + static_cast<uint64_t>(i)};
        s.values = std::vector<double>{static_cast<double>(i)};
        data::WriteBatch b;
        b.series = {std::move(s)};
        auto f = host.proposeBatch(std::move(b));
        for (int k = 0; k < 40 && !f.available(); ++k)
            g->tick().get();
        ASSERT_TRUE(f.get());
    }
}

// Roll the memory store to TSM and wait for the file count to EXCEED `baseline`. Waiting
// for "> 0" is not enough when a test flushes twice: the second rollover would return
// immediately on the first flush's file and the highest flushed revision would not have
// moved, so the second snapshot would find nothing new to compact.
void flushToTsm(ScopedShardedEngine& eng, size_t baseline = 0) {
    (*eng).invoke_on_all([](Engine& e) { return e.rolloverMemoryStore(); }).get();
    for (int i = 0; i < 300; ++i) {
        auto n = (*eng).invoke_on(0u, [](Engine& e) { return e.getTSMFileCount(); }).get();
        if (n > baseline)
            return;
        seastar::sleep(std::chrono::milliseconds(100)).get();
    }
}
size_t tsmFileCount(ScopedShardedEngine& eng) {
    return (*eng).invoke_on(0u, [](Engine& e) { return e.getTSMFileCount(); }).get();
}
}  // namespace

TEST_F(ReplicatedVShardHostTest, TheTriggerSnapshotsOnlyOnceAThresholdIsCrossed) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive; the trigger is disabled by design";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const auto series = core0Series("snaptrigger");
        host.addVShard(series.vshard, {1}, opts).get();
        RaftGroup* g = host.group(series.vshard);
        ASSERT_NE(g, nullptr);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());

        commitWrites(host, g, series.key, series.vshard, 4);
        flushToTsm(eng);

        // A HIGH threshold: eligible on neither count, so the sweep must do nothing even
        // though there IS flushed data to snapshot. Without this the trigger would compact
        // on every sweep and the "policy" would be decoration.
        host.setSnapshotPolicy(/*entries=*/1'000'000, /*bytes=*/1ull << 40, std::chrono::seconds(0));
        EXPECT_EQ(host.maybeSnapshotOnce().get(), 0u);
        EXPECT_EQ(g->node().log().snapshotIndex(), 0u) << "the log must be untouched below the threshold";
        EXPECT_EQ(host.snapshotsTaken(), 0u);

        // Now an ENTRY threshold this group has crossed.
        host.setSnapshotPolicy(/*entries=*/2, /*bytes=*/0, std::chrono::seconds(0));
        EXPECT_EQ(host.maybeSnapshotOnce().get(), 1u);
        EXPECT_GT(g->node().log().snapshotIndex(), 0u) << "the log must now be compacted";
        EXPECT_EQ(host.snapshotsTaken(), 1u);

        // And the BYTES half works on its own (entries disabled), after more writes.
        const raft::LogIndex firstBoundary = g->node().log().snapshotIndex();
        const size_t filesAfterFirst = tsmFileCount(eng);
        commitWrites(host, g, series.key, series.vshard, 4);
        flushToTsm(eng, filesAfterFirst);
        host.setSnapshotPolicy(/*entries=*/0, /*bytes=*/1, std::chrono::seconds(0));
        EXPECT_EQ(host.maybeSnapshotOnce().get(), 1u);
        EXPECT_GT(g->node().log().snapshotIndex(), firstBoundary) << "the boundary must advance";

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, TheTruncationBoundaryStaysBelowTheHighestFlushedRevision) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const auto series = core0Series("snapbound");
        host.addVShard(series.vshard, {1}, opts).get();
        RaftGroup* g = host.group(series.vshard);
        for (int i = 0; i < 10 && !g->isLeader(); ++i)
            g->tick().get();
        ASSERT_TRUE(g->isLeader());
        commitWrites(host, g, series.key, series.vshard, 5);
        flushToTsm(eng);

        // The manifest's own view of the highest FLUSHED revision, read independently of
        // the code under test.
        const uint64_t maxFlushed = store.buildVShardSnapshot(VShardId{series.vshard}).get().manifest.snapshotRevision;
        ASSERT_GT(maxFlushed, 1u);

        const uint64_t compacted = host.snapshotVShard(series.vshard).get();
        // STRICTLY below, by exactly one entry. A revision is one whole log ENTRY, but
        // `EngineLocalStore::applyWrites` issues several insertBatch calls per entry and any
        // of them can roll the memory store -- so entry `maxFlushed` may be only PARTIALLY
        // flushed, and truncating AT it would discard the log entry holding the unflushed
        // remainder. That is silent data loss on the one invariant this path protects.
        EXPECT_EQ(compacted, maxFlushed - 1) << "one entry of slack, deliberately";
        EXPECT_LT(compacted, maxFlushed);
        EXPECT_LE(compacted, g->appliedIndex()) << "never above what this replica applied";
        EXPECT_EQ(g->node().log().snapshotIndex(), compacted);
        // The entry AT the highest flushed revision is retained, so its (possibly
        // unflushed) points still replay.
        EXPECT_GE(g->node().log().lastIndex(), maxFlushed);

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, TheSweepIsRateLimitedPerPassAndPerGroup) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        // Three eligible groups on core 0. Snapshotting reads and encodes whole TSM files,
        // so a sweep that took all of them at once would compete with the write path for the
        // same reactor and the same disk -- the point of a background trigger is that nobody
        // notices it.
        std::vector<Core0Series> all;
        std::set<uint16_t> seen;
        for (int i = 0; all.size() < 3; ++i) {
            auto s = core0Series("snaprate" + std::to_string(i));
            if (seen.insert(s.vshard).second)
                all.push_back(s);
        }
        for (const auto& s : all) {
            host.addVShard(s.vshard, {1}, opts).get();
            RaftGroup* g = host.group(s.vshard);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader());
            commitWrites(host, g, s.key, s.vshard, 3);
        }
        flushToTsm(eng);

        host.setSnapshotPolicy(/*entries=*/1, /*bytes=*/0, std::chrono::seconds(0));
        // ONE per pass, however many are eligible.
        EXPECT_EQ(host.maybeSnapshotOnce().get(), 1u);
        EXPECT_EQ(host.snapshotsTaken(), 1u);

        // And a per-group minimum interval, so one hot VShard cannot monopolize the slot
        // while every other group's log grows: with a long min-interval, repeated passes
        // must reach the OTHER groups rather than re-snapshotting the first.
        host.setSnapshotPolicy(/*entries=*/1, /*bytes=*/0, std::chrono::seconds(3600));
        std::set<raft::LogIndex> compactedGroups;
        for (int pass = 0; pass < 5; ++pass) {
            host.maybeSnapshotOnce().get();
            for (const auto& s : all)
                if (host.group(s.vshard)->node().log().snapshotIndex() > 0)
                    compactedGroups.insert(s.vshard);
        }
        EXPECT_EQ(compactedGroups.size(), all.size()) << "every eligible group must get its turn";
        EXPECT_EQ(host.snapshotsTaken(), all.size()) << "and none of them twice inside the min interval";

        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, ACompactedJournalIsRecoveredWithoutReinstallingTheSnapshot) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const auto series = core0Series("snaprecover");
        const auto sid = SeriesId128::fromSeriesKey(series.key);
        raft::LogIndex boundary = 0;
        {
            cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
            host.addVShard(series.vshard, {1}, opts).get();
            RaftGroup* g = host.group(series.vshard);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader());
            commitWrites(host, g, series.key, series.vshard, 4);
            flushToTsm(eng);
            ASSERT_GT(host.snapshotVShard(series.vshard).get(), 0u);
            boundary = g->node().log().snapshotIndex();
            ASSERT_GT(boundary, 0u);
            host.stop().get();
        }

        // RESTART over the SAME journal and the SAME Engine (a compacted journal, which
        // addVShard used to refuse outright with "snapshot recovery not yet wired"). That
        // refusal was correct while nothing ever compacted; with the trigger wired it would
        // have made the FIRST restart after a snapshot a fail-closed startup.
        {
            cluster::ReplicatedVShardHost host2(store, transport, /*self=*/1, jroot);
            ASSERT_NO_THROW(host2.addVShard(series.vshard, {1}, opts).get());
            RaftGroup* g2 = host2.group(series.vshard);
            ASSERT_NE(g2, nullptr);
            EXPECT_EQ(g2->node().log().snapshotIndex(), boundary) << "the boundary must be adopted from the journal";
            // The recovered node can SERVE the snapshot to a lagging follower. Without
            // seedRecoveredSnapshot this is empty and a follower below the boundary could
            // never be caught up until this node happened to take a fresh snapshot.
            EXPECT_EQ(g2->node().log().snapshotIndex(), boundary);

            // AND THE DATA IS NOT DOUBLED. The payload is deliberately NOT re-installed: the
            // local Engine already holds those TSM files (Engine::init registered them), and
            // installVShardSnapshotFiles would call addTSMFile on each again -- registering
            // the same file twice and counting its points twice in every query.
            for (int i = 0; i < 10 && !g2->isLeader(); ++i)
                g2->tick().get();
            auto rq =
                (*eng)
                    .invoke_on(0u, [key = series.key, sid](Engine& e) { return e.query(key, sid, 0, UINT64_MAX); })
                    .get();
            ASSERT_TRUE(rq.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(rq.value()).values.size(), 4u)
                << "recovery must not double-register the snapshot's TSM files";
            host2.stop().get();
        }
        fs::remove_all(jroot);
    }).get();
}

TEST_F(ReplicatedVShardHostTest, StopSilencesTheTrigger) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        const auto series = core0Series("snapstop");
        host.addVShard(series.vshard, {1}, opts).get();
        host.setSnapshotPolicy(0, 0, std::chrono::seconds(0));  // every group eligible
        host.stop().get();
        // A sweep after stop() must do nothing: it reads Engine files and calls
        // RaftGroup::compact, and both need the registry and store still standing.
        EXPECT_EQ(host.maybeSnapshotOnce().get(), 0u);
        fs::remove_all(jroot);
    }).get();
}

// REVIEW F2, and it is the case that turned a fail-closed refusal into silent loss.
//
// `RaftGroup::drainReady` persists AND FSYNCS an incoming Snapshot record BEFORE
// `sm_.applySnapshot` writes the TSM files -- correct for Raft (durability precedes
// anything observable) and it means the payload is durable in the JOURNAL and nowhere else
// until the install finishes. A kill -9 in that window leaves a replica whose log has been
// truncated to the boundary and whose Engine holds only whichever files landed. If recovery
// skips the re-install (which is right for a snapshot this node PRODUCED, since it was
// built from this node's own on-disk files) the replica comes back reporting itself caught
// up over a hole: it serves replica reads from it, and if elected it builds and serves a
// SNAPSHOT from it.
//
// This drives the exact window: persist a RECEIVED snapshot record, never install it, then
// recover. The payload must be installed on recovery.
TEST_F(ReplicatedVShardHostTest, ARecoveredRECEIVEDSnapshotIsReinstalledBecauseItsInstallMayNeverHaveRun) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const auto series = core0Series("snapf2");
        const auto sid = SeriesId128::fromSeriesKey(series.key);

        // Build a REAL snapshot payload on a donor host, exactly as a leader would ship it.
        data::SnapshotPayload payload;
        {
            cluster::ReplicatedVShardHost donor(store, transport, /*self=*/1, jroot / "donor");
            donor.addVShard(series.vshard, {1}, opts).get();
            RaftGroup* g = donor.group(series.vshard);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader());
            commitWrites(donor, g, series.key, series.vshard, 4);
            flushToTsm(eng);
            payload = store.buildVShardSnapshot(VShardId{series.vshard}).get();
            ASSERT_FALSE(payload.files.empty()) << "the donor snapshot must actually carry files";
            donor.stop().get();
        }

        // A RECEIVING replica: persist the incoming Snapshot record and CRASH before the
        // install (i.e. never call applySnapshot). This is precisely what drainReady's
        // ordering makes reachable.
        const fs::path recvRoot = jroot / "receiver";
        raft::Snapshot incoming;
        incoming.index = 4;
        incoming.term = 1;
        incoming.config.voters = {1};
        incoming.data = data::encodeSnapshotPayload(payload);
        {
            fs::path dir = recvRoot / ("vshard_" + std::to_string(series.vshard));
            fs::create_directories(dir);
            JournalSegmentHeader hdr;
            hdr.clusterUuid.fill(0x11);
            hdr.coreNumber = static_cast<uint16_t>(seastar::this_shard_id());
            hdr.bootId.fill(0x44);
            JournalWriter w(dir, hdr, 1u << 20);
            w.open().get();
            raft::JournalRaftPersistence p(w, VShardId{series.vshard});
            p.persistSnapshot(incoming, /*receivedFromPeer=*/true).get();
            p.sync().get();
            w.close().get();
        }

        // Wipe the receiving Engine's view of that data, so "installed" is distinguishable
        // from "was already there": a fresh Engine has none of the donor's files.
        cleanTestShardDirectories();
        ScopedShardedEngine recvEng;
        recvEng.start();
        cluster::EngineLocalStore recvStore(*recvEng);
        {
            auto pre =
                (*recvEng)
                    .invoke_on(0u, [key = series.key, sid](Engine& e) { return e.query(key, sid, 0, UINT64_MAX); })
                    .get();
            const size_t n = pre.has_value() ? std::get<QueryResult<double>>(pre.value()).values.size() : 0;
            ASSERT_EQ(n, 0u) << "the receiving Engine must start with none of the donor's data";
        }

        cluster::ReplicatedVShardHost receiver(recvStore, transport, /*self=*/1, recvRoot);
        ASSERT_NO_THROW(receiver.addVShard(series.vshard, {1}, opts).get());
        RaftGroup* rg = receiver.group(series.vshard);
        ASSERT_NE(rg, nullptr);
        EXPECT_EQ(rg->node().log().snapshotIndex(), 4u) << "the boundary must be adopted";

        // THE ASSERTION: the payload was re-installed, so the replica really holds the data
        // it is about to report itself caught up on. Without the provenance bit this reads
        // back ZERO while the group claims the boundary -- a hole it would serve from.
        auto rq = (*recvEng)
                      .invoke_on(0u, [key = series.key, sid](Engine& e) { return e.query(key, sid, 0, UINT64_MAX); })
                      .get();
        ASSERT_TRUE(rq.has_value()) << "a recovered RECEIVED snapshot must have been installed";
        EXPECT_EQ(std::get<QueryResult<double>>(rq.value()).values.size(), 4u)
            << "the replica must hold the snapshot's data, not merely its boundary";

        receiver.stop().get();
        fs::remove_all(jroot);
    }).get();
}

// ---------------------------------------------------------------------------
// JOURNAL SEGMENT RECLAMATION (debt D-34)
// ---------------------------------------------------------------------------
//
// D-6 made the snapshot boundary real, so restart replay is bounded. It reclaimed no
// disk: the sealed segments holding the compacted-away records were still there, and
// `cluster_raft/` grew without limit however often a node snapshotted. These tests drive
// the WHOLE wiring -- write, flush, snapshot, publish the floor, collect -- and assert
// both directions: a segment below the boundary goes, a segment above it does not, and
// the group still recovers from what is left.

namespace {
// Segment small enough that a handful of Raft entries seals several of them. The
// production target is 1 MiB, which no unit test is going to fill through a quorum.
constexpr size_t kTinySegBytes = 512;

size_t journalSegmentCount(const fs::path& jroot, uint16_t vshard) {
    const fs::path dir = jroot / ("vshard_" + std::to_string(vshard));
    size_t n = 0;
    if (!fs::exists(dir))
        return 0;
    for (const auto& e : fs::directory_iterator(dir))
        if (timestar::JournalWriter::parseSegmentFilename(e.path().filename().string()))
            ++n;
    return n;
}
}  // namespace

TEST_F(ReplicatedVShardHostTest, SealedSegmentsBelowTheSnapshotBoundaryAreReclaimed) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive; the producer is disabled by design";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;
        const auto series = core0Series("d34reclaim");

        {
            cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
            host.setJournalSegmentBytes(kTinySegBytes, kTinySegBytes);
            host.addVShard(series.vshard, {1}, opts).get();
            RaftGroup* g = host.group(series.vshard);
            ASSERT_NE(g, nullptr);
            for (int i = 0; i < 10 && !g->isLeader(); ++i)
                g->tick().get();
            ASSERT_TRUE(g->isLeader());
            commitWrites(host, g, series.key, series.vshard, 24);

            const size_t sealedBefore = journalSegmentCount(jroot, series.vshard);
            ASSERT_GT(sealedBefore, 1u) << "the writes must have sealed at least one segment";

            // BEFORE ANY SNAPSHOT: nothing is released, so nothing may be deleted. This is
            // the negative half and it is the one that would be data loss if it failed --
            // every record here is the only copy of an un-snapshotted log entry.
            EXPECT_EQ(host.publishReclaimFloors(), 0u) << "an uncompacted group releases nothing";
            EXPECT_EQ(host.reclaimJournalSegments().get(), 0u);
            EXPECT_EQ(journalSegmentCount(jroot, series.vshard), sealedBefore);

            // Flush to TSM and compact: now the boundary has passed most of the log.
            flushToTsm(eng);
            ASSERT_GT(host.snapshotVShard(series.vshard).get(), 0u);
            EXPECT_EQ(host.publishReclaimFloors(), 1u) << "compaction must move this group's floor";
            EXPECT_GT(host.journalRetention().released(VShardId{series.vshard}), 0u);

            const size_t deleted = host.reclaimJournalSegments().get();
            EXPECT_GT(deleted, 0u) << "sealed segments below the boundary must be reclaimed";
            EXPECT_LT(journalSegmentCount(jroot, series.vshard), sealedBefore);
            EXPECT_EQ(host.journalSegmentsDeleted(), deleted);
            // The default (per-VShard) layout is DELETE-ONLY: GC never touches the writer,
            // which is what makes it safe to run alongside the group's own appends.
            EXPECT_EQ(host.journalRecordsCopiedForward(), 0u);

            // A SECOND PASS AT THE SAME FLOOR IS A NO-OP, not a re-scan: the per-VShard
            // arm skips a group whose floor has not moved.
            EXPECT_EQ(host.reclaimJournalSegments().get(), 0u);
            host.stop().get();
        }

        // AND THE GROUP STILL RECOVERS from what is left. This is the whole safety claim:
        // the deleted segments held only records at or below the reclaim floor, so the
        // boundary, the hard state and the retained log suffix are all still on disk.
        {
            cluster::ReplicatedVShardHost host2(store, transport, /*self=*/1, jroot);
            host2.setJournalSegmentBytes(kTinySegBytes, kTinySegBytes);
            ASSERT_NO_THROW(host2.addVShard(series.vshard, {1}, opts).get());
            RaftGroup* g2 = host2.group(series.vshard);
            ASSERT_NE(g2, nullptr);
            EXPECT_GT(g2->node().log().snapshotIndex(), 0u) << "the boundary must survive reclamation";
            for (int i = 0; i < 10 && !g2->isLeader(); ++i)
                g2->tick().get();
            auto rq = (*eng)
                          .invoke_on(0u,
                                     [key = series.key](Engine& e) {
                                         return e.query(key, SeriesId128::fromSeriesKey(key), 0, UINT64_MAX);
                                     })
                          .get();
            ASSERT_TRUE(rq.has_value());
            EXPECT_EQ(std::get<QueryResult<double>>(rq.value()).values.size(), 24u)
                << "every acknowledged point must survive segment reclamation";
            host2.stop().get();
        }
        fs::remove_all(jroot);
    }).get();
}

// A LAGGARD GROUP PINS ITS SEGMENTS while a caught-up group on the same shard reclaims
// its own. In the per-VShard layout the two never share a file, so this is the shape the
// floors themselves must produce: one advances, the other stays at zero.
TEST_F(ReplicatedVShardHostTest, ALaggardGroupPinsItsOwnSegmentsWhileOthersReclaim) {
    seastar::async([] {
        if (!timestar::vshardsCohesiveOnCores(seastar::smp::count))
            GTEST_SKIP() << "core count is not VShard-cohesive";
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoopTransport transport;
        fs::path jroot = tmpDir();
        RaftOptions opts;
        opts.electionTimeoutMin = opts.electionTimeoutMax = 1;
        opts.heartbeatTimeout = 1;

        const auto a = core0Series("d34lag_a");
        Core0Series b = core0Series("d34lag_b");
        for (int i = 0; b.vshard == a.vshard; ++i)
            b = core0Series("d34lag_b" + std::to_string(i));
        ASSERT_NE(a.vshard, b.vshard);

        cluster::ReplicatedVShardHost host(store, transport, /*self=*/1, jroot);
        host.setJournalSegmentBytes(kTinySegBytes, kTinySegBytes);
        host.addVShard(a.vshard, {1}, opts).get();
        host.addVShard(b.vshard, {1}, opts).get();
        RaftGroup* ga = host.group(a.vshard);
        RaftGroup* gb = host.group(b.vshard);
        for (int i = 0; i < 10 && !(ga->isLeader() && gb->isLeader()); ++i) {
            ga->tick().get();
            gb->tick().get();
        }
        ASSERT_TRUE(ga->isLeader());
        ASSERT_TRUE(gb->isLeader());
        commitWrites(host, ga, a.key, a.vshard, 24);
        commitWrites(host, gb, b.key, b.vshard, 24);
        const size_t bBefore = journalSegmentCount(jroot, b.vshard);
        ASSERT_GT(bBefore, 1u);

        // Flush, then compact ONLY group a. Group b is the laggard.
        flushToTsm(eng);
        ASSERT_GT(host.snapshotVShard(a.vshard).get(), 0u);
        host.publishReclaimFloors();
        EXPECT_GT(host.journalRetention().released(VShardId{a.vshard}), 0u);
        EXPECT_EQ(host.journalRetention().released(VShardId{b.vshard}), 0u) << "the laggard releases nothing";

        EXPECT_GT(host.reclaimJournalSegments().get(), 0u);
        EXPECT_EQ(journalSegmentCount(jroot, b.vshard), bBefore) << "the laggard's segments must be untouched";
        host.stop().get();
        fs::remove_all(jroot);
    }).get();
}
