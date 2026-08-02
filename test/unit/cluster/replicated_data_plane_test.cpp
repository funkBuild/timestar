// Integration M3 GATE (assembly): the RF=3 node composition (ReplicatedDataPlane =
// per-VShard Raft host + leader-aware write router over the REAL Engine), verified
// with THREE nodes in-process. A follower's write routes to the VShard LEADER
// (proposeWrite), replicates through Raft, and converges on all three real Engines.
// Uses an in-memory Raft transport + a direct proposeWrite transport + a REDUCED
// 1-VShard placement so the assembly is verified without the 4096-group startup cost.
#include "../../../lib/cluster/integration/replicated_data_plane.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <deque>
#include <filesystem>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/thread.hh>

using namespace timestar;
using namespace timestar::raft;
using timestar::control::ControlMap;
namespace fs = std::filesystem;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

fs::path tmpDir(const std::string& t) {
    static std::atomic_uint64_t seq{0};
    auto d = fs::temp_directory_path() /
             ("ts_rdp_" + t + "_" + std::to_string(::getpid()) + "_" + std::to_string(seq.fetch_add(1)));
    fs::remove_all(d);
    return d;
}

// In-memory Raft transport: send enqueues; the router delivers to the addressed
// node's RaftGroupRegistry.
class MemRaftRouter {
public:
    std::map<NodeId, RaftGroupRegistry*> regs;
    std::deque<Envelope> q;
    void enqueue(Envelope e) { q.push_back(std::move(e)); }
    seastar::future<> pump() {
        int guard = 0;
        while (!q.empty() && guard++ < 200000) {
            Envelope e = std::move(q.front());
            q.pop_front();
            auto it = regs.find(e.message.to);
            if (it != regs.end() && it->second)
                co_await it->second->deliver(std::move(e));
        }
    }
};
class MemRaftTransport : public RaftTransport {
public:
    explicit MemRaftTransport(MemRaftRouter& r) : r_(r) {}
    seastar::future<> send(Envelope env) override {
        r_.enqueue(std::move(env));
        return seastar::make_ready_future<>();
    }

private:
    MemRaftRouter& r_;
};

// Direct propose transport: forwards each VShard batch to the target node's host in-process.
class DirectProposeTransport : public data::NodeTransport {
public:
    std::map<NodeId, cluster::ReplicatedDataPlane*>* nodes = nullptr;
    seastar::future<> forwardWriteBatch(NodeId, data::WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<data::NodeQueryPartial> queryNode(NodeId, data::NodeQueryRequest) override {
        return seastar::make_exception_future<data::NodeQueryPartial>(std::runtime_error("unused"));
    }
    seastar::future<data::ProposeOutcome> proposeWriteHinted(NodeId to, data::VShardBatchView view,
                                                             data::OptDeadline) override {
        data::ProposeOutcome out;
        for (const auto* group : view) {
            if (co_await (*nodes).at(to)->host().proposeBatch(group->second))
                out.committedVShards.push_back(group->first);
            else
                out.rejects.push_back({group->first, timestar::raft::kNoNode, data::WriteFailure::NotLeader});
        }
        out.committed = out.rejects.empty();
        co_return out;
    }
};

RaftOptions optsFor(NodeId id, NodeId leader) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == leader ? 2 : (id == (leader % 3) + 1 ? 6 : 30));
    o.heartbeatTimeout = 1;
    return o;
}

double latestOn(ScopedShardedEngine& e, const std::string& m) {
    http::HttpQueryHandler h(&*e);
    QueryRequest q;
    q.aggregation = AggregationMethod::LATEST;
    q.measurement = m;
    q.fields = {"value"};
    q.startTime = BASE - 1'000'000'000ULL;
    q.endTime = BASE + 1'000'000'000ULL;
    auto r = h.executeQuery(q).get();
    if (!r.success || r.series.empty())
        return -1;
    auto* v = std::get_if<std::vector<double>>(&r.series[0].fields.at("value").second);
    return (v && !v->empty()) ? (*v)[0] : -1;
}

struct Node {
    std::unique_ptr<ScopedShardedEngine> engine;
    std::unique_ptr<cluster::EngineLocalStore> store;
    std::unique_ptr<MemRaftTransport> raft;
    std::unique_ptr<cluster::ReplicatedDataPlane> rdp;
};

class ReplicatedDataPlaneTest : public ::testing::Test {};
}  // namespace

TEST_F(ReplicatedDataPlaneTest, FollowerWriteReplicatesRf3AcrossThreeNodes) {
    seastar::async([] {
        const std::vector<NodeId> voters = {1, 2, 3};
        const NodeId leaderNode = 1;

        // The series -> its VShard; a REDUCED placement assigning ONLY that VShard to
        // all three nodes (primary = leader = node 1). This is what avoids hosting all
        // 4096 groups while exercising the identical assembly path.
        const std::string key = buildSeriesKey("cpu", {{"host", "h1"}}, "value");
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(key));
        ControlMap map;
        map.epoch = 1;
        map.placement[vs] = {1, 2, 3};  // primary/leader = node 1
        data::VShardDirectory dir1(1, map), dir2(2, map), dir3(3, map);
        const data::VShardDirectory* dirs[3] = {&dir1, &dir2, &dir3};

        MemRaftRouter raftRouter;
        DirectProposeTransport proposeT;
        std::map<NodeId, cluster::ReplicatedDataPlane*> nodePtrs;
        proposeT.nodes = &nodePtrs;

        std::map<NodeId, Node> nodes;
        std::vector<fs::path> dirsToClean;
        for (NodeId id : voters) {
            Node n;
            fs::path edir = tmpDir("e" + std::to_string(id));
            dirsToClean.push_back(edir);
            n.engine = std::make_unique<ScopedShardedEngine>();
            n.engine->startAt(edir.string());
            n.store = std::make_unique<cluster::EngineLocalStore>(**n.engine);
            n.raft = std::make_unique<MemRaftTransport>(raftRouter);
            fs::path jdir = tmpDir("j" + std::to_string(id));
            dirsToClean.push_back(jdir);
            n.rdp = std::make_unique<cluster::ReplicatedDataPlane>(*n.store, *n.raft, proposeT, *dirs[id - 1], id,
                                                                   jdir, std::chrono::milliseconds(20));
            n.rdp->addVShard(vs, voters, optsFor(id, leaderNode)).get();
            raftRouter.regs[id] = &n.rdp->host().registry();
            nodePtrs[id] = n.rdp.get();
            nodes[id] = std::move(n);
        }

        auto tickPump = [&](int rounds) {
            for (int i = 0; i < rounds; ++i) {
                for (auto& [id, n] : nodes)
                    if (auto* g = n.rdp->host().group(vs))
                        g->tick().get();
                raftRouter.pump().get();
            }
        };

        // Elect node 1 as the VShard leader.
        tickPump(40);
        ASSERT_TRUE(nodes[leaderNode].rdp->host().group(vs)->isLeader());

        // Write from a FOLLOWER (node 2): the router sees VShard led by node 1 and
        // forwards via proposeWrite; node 1 replicates through Raft to 2 and 3.
        data::WriteSeries s;
        s.seriesKey = key;
        s.type = TSMValueType::Float;
        s.timestamps = {BASE};
        s.values = std::vector<double>{77.0};
        data::WriteBatch batch;
        batch.series = {std::move(s)};

        auto f = nodes[2].rdp->write(std::move(batch));
        for (int i = 0; i < 80 && !f.available(); ++i) {
            for (auto& [id, n] : nodes)
                if (auto* g = n.rdp->host().group(vs))
                    g->tick().get();
            raftRouter.pump().get();
        }
        f.get();  // resolves on durable quorum commit
        tickPump(20);  // let followers apply

        // Converged on ALL THREE real Engines.
        for (NodeId id : voters)
            EXPECT_DOUBLE_EQ(latestOn(*nodes[id].engine, "cpu"), 77.0) << "node " << id;

        for (auto& [id, n] : nodes)
            n.rdp->stop().get();
        nodes.clear();
        for (auto& d : dirsToClean)
            fs::remove_all(d);
    }).get();
}
