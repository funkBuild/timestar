// Group-0 control plane end to end: cluster init on one node, then admitting
// nodes in distinct failure domains GROWS the meta-voter set across those
// domains via joint consensus -- with no operator placement (Phase 3 gate 2).
// Runs the real group-0 RaftGroup + Group0StateMachine + Group0Controller over
// an in-memory router (no sockets).
#include "../../../lib/cluster/control/group0_controller.hpp"

#include "../../../lib/cluster/raft/raft_group.hpp"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/timed_out_error.hh>
#include <seastar/util/later.hh>
#include <vector>

using namespace timestar::control;
using timestar::raft::Envelope;
using timestar::raft::HardState;
using timestar::raft::Message;
using timestar::raft::NodeId;
using timestar::raft::RaftGroup;
using timestar::raft::RaftLog;
using timestar::raft::RaftNode;
using timestar::raft::RaftOptions;
using timestar::raft::RaftPersistence;
using timestar::raft::RaftTransport;
using timestar::raft::Snapshot;
namespace movement = timestar::movement;

namespace {

class NoopPersistence : public RaftPersistence {
public:
    seastar::future<> persistHardState(HardState) override { return seastar::make_ready_future<>(); }
    seastar::future<> persistEntries(std::vector<timestar::raft::LogEntry>) override {
        return seastar::make_ready_future<>();
    }
    seastar::future<> persistSnapshot(Snapshot, bool) override { return seastar::make_ready_future<>(); }
    seastar::future<> sync() override { return seastar::make_ready_future<>(); }
};

class Router;
class RouterTransport : public RaftTransport {
public:
    explicit RouterTransport(Router& r) : r_(r) {}
    seastar::future<> send(Envelope env) override;

private:
    Router& r_;
};

class Router {
public:
    void setGroup(NodeId id, RaftGroup* g) { groups_[id] = g; }
    void enqueue(Envelope e) { queue_.push_back(std::move(e)); }
    seastar::future<> pump() {
        int guard = 0;
        while (!queue_.empty() && guard++ < 200000) {
            Envelope e = std::move(queue_.front());
            queue_.pop_front();
            auto it = groups_.find(e.message.to);
            if (it != groups_.end() && it->second)
                co_await it->second->step(std::move(e.message));
        }
    }

private:
    std::map<NodeId, RaftGroup*> groups_;
    std::deque<Envelope> queue_;
};

seastar::future<> RouterTransport::send(Envelope env) {
    r_.enqueue(std::move(env));
    return seastar::make_ready_future<>();
}

struct NodeBox {
    std::unique_ptr<NoopPersistence> persistence;
    std::unique_ptr<Group0StateMachine> sm;
    std::unique_ptr<RaftGroup> group;
};

using Nodes = std::map<NodeId, NodeBox>;

seastar::future<> tickAndPump(Nodes& nodes, Router& router) {
    co_await router.pump();
    for (auto& [id, node] : nodes)
        co_await node.group->tick();
    co_await router.pump();
    // The in-memory persistence and transport commonly return ready futures.
    // Yield explicitly so the controller continuation awakened by an applied
    // proposal can issue its next bootstrap command before this driver burns
    // through every round in one reactor task.
    co_await seastar::yield();
}

template <typename T>
seastar::future<T> drive(seastar::future<T> future, Nodes& nodes, Router& router, int rounds = 200) {
    for (int i = 0; i < rounds && !future.available(); ++i)
        co_await tickAndPump(nodes, router);
    if (!future.available()) {
        const auto& first = nodes.begin()->second;
        throw std::runtime_error(
            "group0 test future did not settle: commit=" + std::to_string(first.group->commitIndex()) +
            " applied=" + std::to_string(first.group->appliedIndex()) + " state-applied=" +
            std::to_string(first.sm->state().appliedIndex) + " cluster=" + first.sm->state().clusterUuid);
    }
    co_return co_await std::move(future);
}

seastar::future<> drive(seastar::future<> future, Nodes& nodes, Router& router, int rounds = 200) {
    for (int i = 0; i < rounds && !future.available(); ++i)
        co_await tickAndPump(nodes, router);
    if (!future.available()) {
        const auto& first = nodes.begin()->second;
        throw std::runtime_error(
            "group0 test future did not settle: commit=" + std::to_string(first.group->commitIndex()) +
            " applied=" + std::to_string(first.group->appliedIndex()) + " state-applied=" +
            std::to_string(first.sm->state().appliedIndex) + " cluster=" + first.sm->state().clusterUuid);
    }
    co_await std::move(future);
}

RaftOptions optsFor(NodeId id) {
    RaftOptions o;
    o.electionTimeoutMin = o.electionTimeoutMax = (id == 1 ? 2 : 50);  // node 1 leads
    o.heartbeatTimeout = 1;
    return o;
}

NodeRecord rec(NodeId id, std::string dom) {
    NodeRecord r;
    r.raftId = id;
    r.uuid = "uuid-" + std::to_string(id);
    r.address = "mem:" + std::to_string(id);
    r.failureDomain = std::move(dom);
    r.state = NodeState::Active;
    return r;
}

ControlMap initialServingMap(std::vector<NodeId> replicas = {1, 2, 3}) {
    ControlMap map;
    map.epoch = 1;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement.emplace(vshard, replicas);
    return map;
}

FrozenDeletePlan frozenPlan(char id, char fingerprint, std::vector<FrozenDeleteTarget> targets) {
    return FrozenDeletePlan{std::string(32, id), std::string(32, fingerprint), 1'800'000'000'000, std::move(targets)};
}

UpsertJob advanceMove(std::string id, movement::MoveStep step, const movement::MovePlan& plan) {
    movement::MoveJob move(plan, step);
    return UpsertJob{std::move(id), static_cast<uint32_t>(step), move.done(), move.encode()};
}

seastar::future<> testClusterInitGrowsMetaVotersAcrossDomains() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;

    // group-0 voters start as {1}; nodes 2 and 3 are observers that know that.
    auto makeNode = [&](NodeId id, std::vector<NodeId> knownVoters) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, knownVoters, RaftLog{}, HardState{}, optsFor(id));
        box.group =
            std::make_unique<RaftGroup>(/*groupId=*/0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1, {1});
    makeNode(2, {1});
    makeNode(3, {1});

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, /*metaTarget=*/3);

    // cluster init on node 1 (group of one).
    co_await nodes[1].group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1].group->isLeader());
    co_await drive(controller.initCluster("cluster-xyz", rec(1, "rack-a")), nodes, router);
    EXPECT_EQ(nodes[1].sm->state().clusterUuid, "cluster-xyz");
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1}));

    // Admission alone must never add a lagging voter. Add it as a learner,
    // replicate the complete tail, then reconcile: meta voters -> {1,2}.
    co_await drive(controller.admitNode(rec(2, "rack-b")), nodes, router);
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Joining);
    EXPECT_FALSE(co_await controller.reconcileMetaVoters());
    EXPECT_FALSE(co_await controller.activateCaughtUpLearner(2));
    EXPECT_TRUE(co_await drive(controller.addLearner(2), nodes, router));
    for (int i = 0; i < 3 && !controller.learnerCaughtUp(2); ++i)
        co_await tickAndPump(nodes, router);
    EXPECT_TRUE(controller.learnerCaughtUp(2));
    EXPECT_TRUE(co_await drive(controller.activateCaughtUpLearner(2), nodes, router));
    for (int i = 0; i < 3 && !controller.learnerCaughtUp(2); ++i)
        co_await tickAndPump(nodes, router);
    EXPECT_TRUE(controller.learnerCaughtUp(2));
    EXPECT_TRUE(co_await drive(controller.reconcileMetaVoters(), nodes, router));
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1, 2}));
    co_await controller.admitNode(rec(2, "rack-b"));
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Active)
        << "an admission retry must never regress an active node to Joining";
    EXPECT_TRUE(co_await controller.activateCaughtUpLearner(2))
        << "activation remains idempotent after learner promotion";

    // Repeat for node 3. It cannot enter Cnew before learner catch-up.
    co_await drive(controller.admitNode(rec(3, "rack-c")), nodes, router);
    EXPECT_EQ(nodes[1].sm->state().nodes.at(3).state, NodeState::Joining);
    EXPECT_FALSE(co_await controller.reconcileMetaVoters());
    EXPECT_TRUE(co_await drive(controller.addLearner(3), nodes, router));
    for (int i = 0; i < 3 && !controller.learnerCaughtUp(3); ++i)
        co_await tickAndPump(nodes, router);
    EXPECT_TRUE(controller.learnerCaughtUp(3));
    EXPECT_TRUE(co_await drive(controller.activateCaughtUpLearner(3), nodes, router));
    for (int i = 0; i < 3 && !controller.learnerCaughtUp(3); ++i)
        co_await tickAndPump(nodes, router);
    EXPECT_TRUE(controller.learnerCaughtUp(3));
    EXPECT_TRUE(co_await drive(controller.reconcileMetaVoters(), nodes, router));
    EXPECT_EQ(nodes[1].group->node().config().voters, (std::vector<NodeId>{1, 2, 3}));
    EXPECT_FALSE(nodes[1].group->node().config().joint());

    // The state machine mirrors the voter set, and the new voters converged on
    // the full control state (cluster id, all node records, meta voters).
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().metaVoters, (std::vector<NodeId>{1, 2, 3}));
    for (NodeId id : {2u, 3u}) {
        EXPECT_EQ(nodes[id].sm->state().clusterUuid, "cluster-xyz") << "node " << id;
        EXPECT_EQ(nodes[id].sm->state().nodes.size(), 3u) << "node " << id;
    }
    // Failure-domain diversity: the three voters span three distinct domains.
    std::set<std::string> domains;
    for (NodeId v : nodes[1].group->node().config().voters)
        domains.insert(nodes[1].sm->state().nodes.at(v).failureDomain);
    EXPECT_EQ(domains.size(), 3u);

    // A controller epoch was stamped.
    EXPECT_GT(nodes[1].sm->state().controllerTerm, 0u);
}

seastar::future<> testReadBarrierReconcilesControlMap() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;
    auto makeNode = [&](NodeId id, std::vector<NodeId> knownVoters) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, knownVoters, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1, {1, 2, 3});
    makeNode(2, {1, 2, 3});
    makeNode(3, {1, 2, 3});

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1].group->isLeader());
    auto init = controller.initCluster("c1", rec(1, "rack-a"));
    EXPECT_FALSE(init.available()) << "RF=3 bootstrap must not ack a local-only append";
    co_await drive(std::move(init), nodes, router);
    EXPECT_EQ(nodes[1].sm->state().metaVoters, (std::vector<NodeId>{1, 2, 3}));

    const ControlMap serving = initialServingMap();
    auto publish = controller.publishInitialServingMap(serving);
    EXPECT_FALSE(publish.available()) << "serving-map publication must wait for quorum apply";
    EXPECT_TRUE(co_await drive(std::move(publish), nodes, router));
    EXPECT_EQ(nodes[1].sm->state().servingMap, serving);
    EXPECT_TRUE(co_await controller.publishInitialServingMap(serving));
    ControlMap conflicting = serving;
    conflicting.placement.at(0) = {3, 2, 1};
    EXPECT_FALSE(co_await controller.publishInitialServingMap(std::move(conflicting)));

    // A movement plan atomically binds the current source membership, desired
    // target, persisted job, and next map epoch.
    auto addDestination = controller.proposeCommand(UpsertNode{rec(4, "rack-d")});
    EXPECT_TRUE(co_await drive(std::move(addDestination), nodes, router));
    auto placement = controller.planVShardMove("move-5", 5, /*destination=*/4, /*victim=*/3);
    EXPECT_FALSE(placement.available()) << "control mutation must wait for quorum commit and apply";
    EXPECT_TRUE(co_await drive(std::move(placement), nodes, router));
    const uint64_t epoch = nodes[1].sm->state().mapEpoch;
    EXPECT_GE(epoch, 1u);

    // Reconcile via a ReadIndex barrier: start it, drive the confirmation round,
    // then await it. The barrier index is at least the committed placement, and
    // the state read behind it reflects the new epoch (linearizable).
    auto rb = nodes[1].group->readBarrier();  // enqueues the heartbeat round
    timestar::raft::LogIndex readIndex = co_await drive(std::move(rb), nodes, router);
    EXPECT_GT(readIndex, 0u);
    EXPECT_EQ(nodes[1].sm->state().mapEpoch, epoch);
    EXPECT_EQ(nodes[1].sm->state().desiredPlacement.at(5), (std::vector<NodeId>{1, 2, 4}));
    EXPECT_TRUE(nodes[1].sm->state().jobs.contains("move-5"));

    const auto planned = movement::MoveJob::decode(nodes[1].sm->state().jobs.at("move-5").payload);
    if (!planned)
        throw std::runtime_error("controller persisted an invalid movement job");
    for (const auto step : {movement::MoveStep::LearnerAdded, movement::MoveStep::CaughtUp,
                            movement::MoveStep::Promoted, movement::MoveStep::OldRemoved, movement::MoveStep::Done}) {
        auto persist = controller.proposeCommand(advanceMove("move-5", step, planned->plan()));
        EXPECT_FALSE(persist.available()) << "job progress must wait for quorum apply";
        EXPECT_TRUE(co_await drive(std::move(persist), nodes, router));
    }

    auto cutover = controller.publishCompletedMove("move-5");
    EXPECT_FALSE(cutover.available()) << "serving-map cutover must wait for quorum apply";
    EXPECT_TRUE(co_await drive(std::move(cutover), nodes, router));
    ControlMap expected = serving;
    expected.epoch = 2;
    expected.placement.at(5) = {1, 2, 4};
    co_await router.pump();
    for (const auto& [id, node] : nodes)
        EXPECT_EQ(node.sm->state().servingMap, expected) << "node " << id;
    const auto committed = nodes[1].group->commitIndex();
    EXPECT_TRUE(co_await controller.publishCompletedMove("move-5"));
    EXPECT_EQ(nodes[1].group->commitIndex(), committed)
        << "an exact cutover retry should not append another group-0 entry";

    // A follower's read barrier is rejected (redirect to the leader).
    bool rejected = false;
    try {
        co_await nodes[2].group->readBarrier();
    } catch (const std::runtime_error&) {
        rejected = true;
    }
    EXPECT_TRUE(rejected);
}

seastar::future<> testJoinTokenGatesAdmission() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;
    auto makeNode = [&](NodeId id) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, {1}, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1);

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    co_await controller.initCluster("c1", rec(1, "rack-a"));
    co_await router.pump();

    // A node presenting an UNMINTED token is rejected.
    EXPECT_FALSE(co_await controller.admitNodeWithToken(rec(9, "rack-x"), "bad-token"));
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().nodes.count(9), 0u);
    EXPECT_FALSE(co_await controller.mintJoinToken(std::string(kMaxJoinTokenBytes + 1, 'x')))
        << "an operator token cannot inflate every command and control snapshot without bound";
    EXPECT_FALSE(co_await controller.admitNodeWithToken(rec(9, "rack-x"), std::string(kMaxJoinTokenBytes + 1, 'x')));

    // Mint a token, then the node joins with it -> admitted, token consumed.
    co_await controller.mintJoinToken("join-42");
    co_await router.pump();
    EXPECT_TRUE(co_await controller.admitNodeWithToken(rec(2, "rack-b"), "join-42"));
    co_await router.pump();
    EXPECT_EQ(nodes[1].sm->state().nodes.count(2), 1u);
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Joining);
    EXPECT_EQ(nodes[1].sm->state().joinTokens.count("join-42"), 0u);
    EXPECT_TRUE(co_await controller.admitNodeWithToken(rec(2, "rack-b"), "join-42"))
        << "an ambiguous retry for the admitted identity is idempotent";
    EXPECT_FALSE(co_await controller.admitNodeWithToken(rec(3, "rack-c"), "join-42"))
        << "a consumed token cannot admit another identity";

    co_await controller.mintJoinToken("still-live");
    co_await router.pump();
    EXPECT_FALSE(co_await controller.admitNodeWithToken(rec(2, "rack-b"), "still-live"))
        << "an existing identity must not appear to consume a still-live token";
    EXPECT_EQ(nodes[1].sm->state().joinTokens.count("still-live"), 1u);

    bool conflictingInitRejected = false;
    try {
        co_await controller.initCluster("different-cluster", rec(1, "rack-a"));
    } catch (const std::runtime_error&) {
        conflictingInitRejected = true;
    }
    EXPECT_TRUE(conflictingInitRejected);
    EXPECT_EQ(nodes[1].sm->state().clusterUuid, "c1");
}

seastar::future<> testNodeRemovalRequiresDrainAndClearedReferences() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;
    transports.push_back(std::make_unique<RouterTransport>(router));
    NodeBox box;
    box.persistence = std::make_unique<NoopPersistence>();
    box.sm = std::make_unique<Group0StateMachine>();
    RaftNode rn(1, {1}, RaftLog{}, HardState{}, optsFor(1));
    box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
    router.setGroup(1, box.group.get());
    nodes[1] = std::move(box);

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 1);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    co_await controller.initCluster("c1", rec(1, "rack-a"));
    EXPECT_TRUE(co_await controller.publishInitialServingMap(initialServingMap({1})));
    EXPECT_TRUE(co_await controller.proposeCommand(UpsertNode{rec(2, "rack-b")}));

    EXPECT_FALSE(co_await controller.removeDrainedNode(2));
    EXPECT_TRUE(co_await controller.drainNode(2));
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Draining);
    EXPECT_TRUE(co_await controller.drainNode(2)) << "a drain retry must be idempotent";
    EXPECT_TRUE(co_await controller.removeDrainedNode(2));
    EXPECT_EQ(nodes[1].sm->state().nodes.at(2).state, NodeState::Removed);
    EXPECT_TRUE(co_await controller.removeDrainedNode(2)) << "a remove retry must be idempotent";
}

seastar::future<> testDeletePlanFreezesFirstExpansion() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;
    transports.push_back(std::make_unique<RouterTransport>(router));
    NodeBox box;
    box.persistence = std::make_unique<NoopPersistence>();
    box.sm = std::make_unique<Group0StateMachine>();
    RaftNode rn(1, {1}, RaftLog{}, HardState{}, optsFor(1));
    box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
    router.setGroup(1, box.group.get());
    nodes[1] = std::move(box);

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    auto beforeLeadership = co_await controller.freezeDeletePlan(frozenPlan('1', 'a', {{"m,host=a value", 10, 20}}));
    EXPECT_EQ(beforeLeadership.status, FreezeDeletePlanStatus::NotLeader);

    co_await nodes[1].group->campaign();
    co_await router.pump();
    co_await controller.initCluster("c1", rec(1, "rack-a"));
    co_await router.pump();
    EXPECT_TRUE(co_await controller.publishInitialServingMap(initialServingMap({1})));

    const auto original = frozenPlan('1', 'a', {{"m,host=a value", 10, 20}});
    auto identity = original;
    identity.targets.clear();
    EXPECT_EQ(controller.lookupDeletePlan(identity).status, FreezeDeletePlanStatus::NotFound);
    auto stored = co_await controller.freezeDeletePlan(original);
    EXPECT_EQ(stored.status, FreezeDeletePlanStatus::Stored);
    EXPECT_EQ(stored.plan, original);
    EXPECT_EQ(controller.lookupDeletePlan(identity).plan, original);

    auto changedExpansion = original;
    changedExpansion.targets.push_back({"m,host=new value", 10, 20});
    auto retry = co_await controller.freezeDeletePlan(std::move(changedExpansion));
    EXPECT_EQ(retry.status, FreezeDeletePlanStatus::Stored);
    EXPECT_EQ(retry.plan, original) << "a catalog change during retry must return the first committed expansion";

    auto conflictingBody = original;
    conflictingBody.requestFingerprint = std::string(32, 'b');
    auto conflict = co_await controller.freezeDeletePlan(std::move(conflictingBody));
    EXPECT_EQ(conflict.status, FreezeDeletePlanStatus::Conflict);
    EXPECT_EQ(conflict.plan, original);
    EXPECT_EQ(nodes[1].sm->state().frozenDeletePlans.at(original.requestId), original);

    // A retained key becomes reusable once its conservative one-hour + future
    // skew window has passed. Lookup must report a miss so the caller can
    // discover a new operation, and apply atomically prunes/replaces the old one.
    auto replacement = original;
    replacement.issuedAtMs += kFrozenDeletePlanRetentionMs + kFrozenDeletePlanFutureSkewMs + 1;
    replacement.targets = {{"m,host=reused value", 30, 40}};
    auto replacementIdentity = replacement;
    replacementIdentity.targets.clear();
    EXPECT_EQ(controller.lookupDeletePlan(replacementIdentity).status, FreezeDeletePlanStatus::NotFound);
    auto replaced = co_await controller.freezeDeletePlan(replacement);
    EXPECT_EQ(replaced.status, FreezeDeletePlanStatus::Stored);
    EXPECT_EQ(replaced.plan, replacement);
    EXPECT_EQ(nodes[1].sm->state().frozenDeletePlans.at(original.requestId), replacement);
}

seastar::future<> testDeletePlanProposalDeadlineBoundsQuorumLoss() {
    Router router;
    std::vector<std::unique_ptr<RouterTransport>> transports;
    Nodes nodes;
    auto makeNode = [&](NodeId id) {
        transports.push_back(std::make_unique<RouterTransport>(router));
        NodeBox box;
        box.persistence = std::make_unique<NoopPersistence>();
        box.sm = std::make_unique<Group0StateMachine>();
        RaftNode rn(id, {1, 2, 3}, RaftLog{}, HardState{}, optsFor(id));
        box.group = std::make_unique<RaftGroup>(0, std::move(rn), *box.persistence, *transports.back(), *box.sm);
        router.setGroup(id, box.group.get());
        nodes[id] = std::move(box);
    };
    makeNode(1);
    makeNode(2);
    makeNode(3);

    Group0Controller controller(*nodes[1].group, *nodes[1].sm, 3);
    co_await nodes[1].group->campaign();
    co_await router.pump();
    EXPECT_TRUE(nodes[1].group->isLeader());
    co_await drive(controller.initCluster("c1", rec(1, "rack-a")), nodes, router);
    EXPECT_TRUE(co_await drive(controller.publishInitialServingMap(initialServingMap()), nodes, router));
    // Leave node 1 believing it is leader, but make its next entry unable to
    // reach either follower. Group 0 intentionally does not use CheckQuorum,
    // so the controller's default proposal deadline is what bounds this request.
    router.setGroup(2, nullptr);
    router.setGroup(3, nullptr);
    Group0Controller boundedController(*nodes[1].group, *nodes[1].sm, 3, std::chrono::milliseconds(20));
    bool timedOut = false;
    try {
        (void)co_await boundedController.freezeDeletePlan(frozenPlan('1', 'a', {{"m,host=a value", 10, 20}}));
    } catch (const seastar::timed_out_error&) {
        timedOut = true;
    }
    EXPECT_TRUE(timedOut) << "a quorum-lost group-0 leader must apply its default request deadline";
    EXPECT_EQ(nodes[1].group->pendingApplyWaiters(), 0u);
    EXPECT_TRUE(nodes[1].sm->state().frozenDeletePlans.empty())
        << "an uncommitted timed-out proposal must not become a visible frozen plan";
}

}  // namespace

TEST(Group0ControllerTest, ClusterInitGrowsMetaVotersAcrossFailureDomains) {
    testClusterInitGrowsMetaVotersAcrossDomains().get();
}

TEST(Group0ControllerTest, JoinTokenGatesAdmission) {
    testJoinTokenGatesAdmission().get();
}

TEST(Group0ControllerTest, NodeRemovalRequiresDrainAndClearedReferences) {
    testNodeRemovalRequiresDrainAndClearedReferences().get();
}

TEST(Group0ControllerTest, ReadBarrierReconcilesControlMap) {
    testReadBarrierReconcilesControlMap().get();
}

TEST(Group0ControllerTest, DeletePlanFreezesFirstExpansion) {
    testDeletePlanFreezesFirstExpansion().get();
}

TEST(Group0ControllerTest, DeletePlanProposalDeadlineBoundsQuorumLoss) {
    testDeletePlanProposalDeadlineBoundsQuorumLoss().get();
}
