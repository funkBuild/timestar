#include "../../../lib/cluster/control/group0_state_machine.hpp"

#include "../../../lib/cluster/control/control_command.hpp"
#include "../../../lib/cluster/integration/controller_job_driver.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;
namespace movement = timestar::movement;

namespace {
NodeRecord node(NodeId id, std::string uuid, NodeState state = NodeState::Active) {
    return NodeRecord{id, std::move(uuid), "127.0.0.1:" + std::to_string(id), "rack-a", state};
}

ControlMap servingMap() {
    ControlMap map;
    map.epoch = 1;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement.emplace(vshard, std::vector<NodeId>{1, 2, 3});
    return map;
}

movement::MovePlan movePlan() {
    return movement::MovePlan{/*vshard=*/7, /*dest=*/4, /*victim=*/1, /*mapEpoch=*/2,
                              /*sourceVoters=*/{1, 2, 3}};
}

UpsertJob advance(std::string id, movement::MoveStep step, movement::MovePlan plan = movePlan()) {
    movement::MoveJob move(std::move(plan), step);
    return UpsertJob{std::move(id), static_cast<uint32_t>(step), move.done(), move.encode()};
}

FrozenDeletePlan plan() {
    return FrozenDeletePlan{std::string(32, '1'),
                            std::string(32, 'a'),
                            1'800'000'000'000,
                            {{"cpu,host=a value", 10, 20}, {"cpu,host=b value", 30, 40}}};
}
}  // namespace

TEST(ControlCommandV1, RoundTripsEveryCommand) {
    const std::vector<ControlCommand> commands = {InitCluster{"cluster-a"},
                                                  UpsertNode{node(7, "uuid-7")},
                                                  SetNodeState{7, NodeState::Draining},
                                                  PlanVShardMove{"move-42", movePlan()},
                                                  SetMetaVoters{{1, 2, 3}},
                                                  CasPolicy{"schema/cpu/value", 0, "float"},
                                                  SetControllerTerm{9, 3},
                                                  advance("move-42", movement::MoveStep::Done),
                                                  MintJoinToken{"token"},
                                                  AdmitWithToken{node(8, "uuid-8", NodeState::Joining), "token"},
                                                  StoreFrozenDeletePlan{plan()},
                                                  PublishServingMap{ControlMap{1, {{0, {1, 2, 3}}}, {{0, 7}}}, {}}};

    for (const auto& command : commands) {
        const auto encoded = encodeCommand(command);
        ASSERT_EQ(encoded.substr(0, 4), "TCC1");
        const auto decoded = decodeCommand(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->index(), command.index());
    }
}

TEST(ControlCommandV1, RejectsTruncationCorruptionAndTrailingBytes) {
    const auto encoded = encodeCommand(StoreFrozenDeletePlan{plan()});
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeCommand(encoded.substr(0, n))) << n;
    auto badMagic = encoded;
    badMagic[0] ^= 1;
    EXPECT_FALSE(decodeCommand(badMagic));
    EXPECT_FALSE(decodeCommand(encoded + "x"));

    auto move = encodeCommand(PlanVShardMove{"move-42", movePlan()});
    const auto nested = move.find("TSMJ1");
    ASSERT_NE(nested, std::string::npos);
    move[nested + 4] = '2';
    EXPECT_FALSE(decodeCommand(move)) << "TCC1 must not hide a retired nested movement version";
}

TEST(ControlRpcV1, FrozenPlanFramesRoundTripAndFailClosed) {
    for (auto operation : {FrozenDeletePlanRpcOperation::Lookup, FrozenDeletePlanRpcOperation::Freeze}) {
        auto requestPlan = plan();
        if (operation == FrozenDeletePlanRpcOperation::Lookup)
            requestPlan.targets.clear();
        FrozenDeletePlanRpcRequest request{operation, std::move(requestPlan)};
        const auto encoded = encodeFrozenDeletePlanRpcRequest(request);
        ASSERT_EQ(static_cast<uint8_t>(encoded[0]), 1u);
        const auto decoded = decodeFrozenDeletePlanRpcRequest(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->operation, operation);
        EXPECT_EQ(decoded->plan, request.plan);
        for (size_t n = 0; n < encoded.size(); ++n)
            EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(encoded.substr(0, n))) << n;
    }

    for (auto status :
         {FreezeDeletePlanStatus::Stored, FreezeDeletePlanStatus::NotFound, FreezeDeletePlanStatus::NotLeader,
          FreezeDeletePlanStatus::Conflict, FreezeDeletePlanStatus::Capacity, FreezeDeletePlanStatus::Invalid}) {
        const bool carriesPlan = status == FreezeDeletePlanStatus::Stored || status == FreezeDeletePlanStatus::Conflict;
        FreezeDeletePlanResult result{status, carriesPlan ? plan() : FrozenDeletePlan{}};
        const auto decoded = decodeFrozenDeletePlanRpcResult(encodeFrozenDeletePlanRpcResult(result));
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->status, status);
    }
}

TEST(ControlRpcV1, JoinFramesRoundTripAndValidateState) {
    ControlJoinRequest request{std::string(32, 'a'), node(7, std::string(32, 'b'), NodeState::Joining), "token"};
    const auto encoded = encodeControlJoinRequest(request);
    EXPECT_EQ(decodeControlJoinRequest(encoded), request);
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeControlJoinRequest(encoded.substr(0, n))) << n;

    request.record.state = NodeState::Active;
    EXPECT_THROW(encodeControlJoinRequest(request), std::invalid_argument);
    for (const auto result :
         {ControlJoinResult{ControlJoinStatus::NotLeader, 0}, ControlJoinResult{ControlJoinStatus::Rejected, 1},
          ControlJoinResult{ControlJoinStatus::Joining, 1}, ControlJoinResult{ControlJoinStatus::Active, 1}})
        EXPECT_EQ(decodeControlJoinResult(encodeControlJoinResult(result)), result);
}

TEST(ControlRpcV1, MoveDestinationFramesRoundTripAndRejectMalformedInput) {
    EnsureMoveDestinationRequest request{std::string(32, 'a'), "move-7", 9, 2};
    const auto encoded = encodeEnsureMoveDestinationRequest(request);
    EXPECT_EQ(decodeEnsureMoveDestinationRequest(encoded), request);
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeEnsureMoveDestinationRequest(encoded.substr(0, n))) << n;
    EXPECT_FALSE(decodeEnsureMoveDestinationRequest(encoded + "x"));
    request.controllerTerm = 0;
    EXPECT_THROW(encodeEnsureMoveDestinationRequest(request), std::invalid_argument);

    for (auto status : {EnsureMoveDestinationStatus::Ready, EnsureMoveDestinationStatus::Rejected,
                        EnsureMoveDestinationStatus::Unavailable}) {
        const EnsureMoveDestinationResult result{status};
        EXPECT_EQ(decodeEnsureMoveDestinationResult(encodeEnsureMoveDestinationResult(result)), result);
    }
}

TEST(ControllerJobDriverV1, DestinationAuthorizationComesOnlyFromExactCommittedState) {
    Group0State state;
    state.clusterUuid = std::string(32, 'a');
    state.controllerTerm = 9;
    state.controllerLeader = 2;
    state.mapEpoch = 2;
    state.nodes.emplace(4, node(4, std::string(32, '4')));
    state.servingMap = servingMap();
    movement::MoveJob move(movePlan());
    state.desiredPlacement.emplace(7, move.targetVoters());
    state.jobs.emplace("move-7", Job{"move-7", 0, false, move.encode()});
    const EnsureMoveDestinationRequest request{state.clusterUuid, "move-7", 9, 2};

    auto authorized = timestar::cluster::ControllerJobDriver::authorizeDestination(state, 4, request);
    ASSERT_TRUE(authorized);
    EXPECT_EQ(authorized->plan(), move.plan());

    auto staleTerm = request;
    staleTerm.controllerTerm = 8;
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::authorizeDestination(state, 4, staleTerm));
    auto wrongJob = request;
    wrongJob.jobId = "other";
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::authorizeDestination(state, 4, wrongJob));
    state.nodes.at(4).state = NodeState::Draining;
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::authorizeDestination(state, 4, request));
}

TEST(MoveJobV1, RecoveredJointConfigurationsAreAuthorizedOnlyAtTheirAdjacentStep) {
    movement::MoveJob planned(movePlan(), movement::MoveStep::Planned);
    EXPECT_TRUE(planned.acceptsConfig({1, 2, 3}, {1, 2, 3}, {4}));
    EXPECT_FALSE(planned.acceptsConfig({2, 3, 4}, {1, 2, 3, 4}, {}));

    movement::MoveJob caughtUp(movePlan(), movement::MoveStep::CaughtUp);
    EXPECT_TRUE(caughtUp.acceptsConfig({1, 2, 3, 4}, {1, 2, 3}, {}));
    EXPECT_FALSE(caughtUp.acceptsConfig({2, 3, 4}, {1, 2, 3, 4}, {}));

    movement::MoveJob promoted(movePlan(), movement::MoveStep::Promoted);
    EXPECT_TRUE(promoted.acceptsConfig({2, 3, 4}, {1, 2, 3, 4}, {}));
    EXPECT_FALSE(promoted.acceptsConfig({2, 3, 4}, {1, 2, 3}, {}));
}

TEST(Group0StateMachineV1, AppliesIdentityPolicyMembershipAndTokens) {
    Group0StateMachine sm;
    EXPECT_TRUE(sm.applyCommand(InitCluster{"cluster-a"}));
    EXPECT_FALSE(sm.applyCommand(InitCluster{"cluster-b"}));
    EXPECT_TRUE(sm.applyCommand(UpsertNode{node(1, "uuid-1")}));
    EXPECT_FALSE(sm.applyCommand(UpsertNode{node(2, "uuid-1")}));
    EXPECT_TRUE(sm.applyCommand(PublishServingMap{servingMap(), {}}));
    EXPECT_TRUE(sm.applyCommand(UpsertNode{node(4, "uuid-4")}));
    EXPECT_TRUE(sm.applyCommand(PlanVShardMove{"move-7", movePlan()}));
    EXPECT_EQ(sm.state().desiredPlacement.at(7), (std::vector<NodeId>{4, 2, 3}));
    EXPECT_EQ(sm.state().jobs.at("move-7").step, 0u);
    EXPECT_FALSE(sm.applyCommand(PlanVShardMove{"move-8", movePlan()}))
        << "only one unfinished topology mutation is allowed";
    EXPECT_EQ(sm.state().mapEpoch, 2u);
    EXPECT_TRUE(sm.applyCommand(SetMetaVoters{{1, 2, 3}}));
    EXPECT_TRUE(sm.applyCommand(CasPolicy{"schema/cpu/value", 0, "float"}));
    EXPECT_FALSE(sm.applyCommand(CasPolicy{"schema/cpu/value", 0, "integer"}));
    EXPECT_EQ(sm.state().policies.at("schema/cpu/value").version, 1u);

    EXPECT_TRUE(sm.applyCommand(MintJoinToken{"join-once"}));
    EXPECT_TRUE(sm.applyCommand(AdmitWithToken{node(5, "uuid-5", NodeState::Joining), "join-once"}));
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{node(6, "uuid-6", NodeState::Joining), "join-once"}));
    EXPECT_TRUE(sm.applyCommand(SetNodeState{5, NodeState::Active}));
}

TEST(Group0StateMachineV1, ServingMapCutoverRequiresExactCompletedMovementJob) {
    Group0StateMachine sm;
    const auto map = servingMap();
    EXPECT_TRUE(sm.applyCommand(PublishServingMap{map, {}}));
    EXPECT_FALSE(sm.applyCommand(PublishServingMap{map, {}}));
    EXPECT_TRUE(sm.applyCommand(UpsertNode{node(4, "uuid-4")}));
    ASSERT_TRUE(sm.applyCommand(PlanVShardMove{"move-7", movePlan()}));

    ControlMap next = map;
    next.epoch = 2;
    next.placement.at(7) = {4, 2, 3};
    EXPECT_FALSE(sm.applyCommand(PublishServingMap{next, "move-7"}))
        << "routing must not cut over before the membership job is done";
    EXPECT_FALSE(sm.applyCommand(advance("move-7", movement::MoveStep::CaughtUp)))
        << "durable job progress cannot skip LearnerAdded";
    auto inconsistent = advance("move-7", movement::MoveStep::LearnerAdded);
    inconsistent.step = static_cast<uint32_t>(movement::MoveStep::CaughtUp);
    EXPECT_FALSE(sm.applyCommand(inconsistent)) << "the replicated job summary must match its TSMJ1 payload";
    auto changed = movePlan();
    changed.dest = 5;
    EXPECT_FALSE(sm.applyCommand(advance("move-7", movement::MoveStep::LearnerAdded, changed)))
        << "a persisted job id cannot be rebound to another topology plan";
    for (auto step : {movement::MoveStep::LearnerAdded, movement::MoveStep::CaughtUp, movement::MoveStep::Promoted,
                      movement::MoveStep::OldRemoved, movement::MoveStep::Done})
        ASSERT_TRUE(sm.applyCommand(advance("move-7", step))) << static_cast<unsigned>(step);

    auto wrong = next;
    wrong.placement.at(8) = {4, 2, 3};
    EXPECT_FALSE(sm.applyCommand(PublishServingMap{wrong, "move-7"}))
        << "one completed job cannot authorize an unrelated VShard cutover";
    EXPECT_TRUE(sm.applyCommand(PublishServingMap{next, "move-7"}));
    EXPECT_EQ(sm.state().servingMap, next);
    EXPECT_FALSE(sm.applyCommand(PublishServingMap{next, "move-7"}));

    EXPECT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{plan()}));
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{plan()}));
    auto conflicting = plan();
    conflicting.requestFingerprint = std::string(32, 'b');
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{conflicting}));
    EXPECT_EQ(sm.state().frozenDeletePlans.at(plan().requestId), plan());
}

TEST(Group0SnapshotV1, RoundTripsAndRejectsMalformedState) {
    Group0StateMachine source;
    ASSERT_TRUE(source.applyCommand(InitCluster{"cluster-a"}));
    ASSERT_TRUE(source.applyCommand(UpsertNode{node(1, "uuid-1")}));
    ASSERT_TRUE(source.applyCommand(PublishServingMap{servingMap(), {}}));
    ASSERT_TRUE(source.applyCommand(StoreFrozenDeletePlan{plan()}));

    const auto snapshot = source.snapshot();
    ASSERT_GE(snapshot.size(), 8u);
    EXPECT_EQ(snapshot.substr(0, 8), "TSG0SNP1");
    Group0StateMachine restored;
    EXPECT_TRUE(restored.loadSnapshot(snapshot));
    EXPECT_EQ(restored.state(), source.state());
    for (size_t n = 0; n < snapshot.size(); ++n) {
        Group0StateMachine truncated;
        EXPECT_FALSE(truncated.loadSnapshot(snapshot.substr(0, n))) << n;
    }
    auto corrupt = snapshot;
    corrupt[0] ^= 1;
    EXPECT_FALSE(restored.loadSnapshot(corrupt));
    EXPECT_EQ(restored.state(), source.state());
}

TEST(Group0SnapshotV1, DurableServingMapHighWaterDoesNotRegressDuringReplay) {
    Group0StateMachine source;
    const auto initial = servingMap();
    ASSERT_TRUE(source.applyCommand(PublishServingMap{initial, {}}));
    ASSERT_TRUE(source.applyCommand(UpsertNode{node(4, "uuid-4")}));
    ASSERT_TRUE(source.applyCommand(PlanVShardMove{"move-7", movePlan()}));
    for (const auto step : {movement::MoveStep::LearnerAdded, movement::MoveStep::CaughtUp,
                            movement::MoveStep::Promoted, movement::MoveStep::OldRemoved, movement::MoveStep::Done})
        ASSERT_TRUE(source.applyCommand(advance("move-7", step)));

    ControlMap cutover = initial;
    cutover.epoch = 2;
    cutover.placement.at(7) = {4, 2, 3};
    const std::string beforeCutover = source.snapshot();

    Group0StateMachine recovered;
    recovered.expectServingMap(cutover);
    size_t cacheWrites = 0;
    recovered.setServingMapObserver([&cacheWrites](ControlMap) {
        ++cacheWrites;
        return seastar::make_ready_future<>();
    });
    ASSERT_TRUE(recovered.loadSnapshot(beforeCutover));
    EXPECT_EQ(recovered.state().servingMap, initial) << "the retained Group-0 log still has to replay the cutover";

    timestar::raft::LogEntry entry;
    entry.term = 3;
    entry.index = 50;
    entry.data = encodeCommand(PublishServingMap{cutover, "move-7"});
    EXPECT_NO_THROW(recovered.apply(std::move(entry)).get());
    EXPECT_EQ(recovered.state().servingMap, cutover);
    EXPECT_EQ(cacheWrites, 0u) << "replaying an older snapshot/log must not rewrite an already-current durable cache";

    Group0StateMachine staleCache;
    staleCache.expectServingMap(initial);
    std::optional<ControlMap> published;
    staleCache.setServingMapObserver([&published](ControlMap map) {
        published = std::move(map);
        return seastar::make_ready_future<>();
    });
    timestar::raft::Snapshot snapshot;
    snapshot.index = 50;
    snapshot.term = 3;
    snapshot.data = recovered.snapshot();
    EXPECT_NO_THROW(staleCache.applySnapshot(std::move(snapshot)).get());
    ASSERT_TRUE(published);
    EXPECT_EQ(*published, cutover) << "a newer recovered map must advance the durable local routing cache";
}
