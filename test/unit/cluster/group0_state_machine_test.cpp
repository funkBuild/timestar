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
                                                  PublishServingMap{ControlMap{1, {{0, {1, 2, 3}}}, {{0, 7}}}, {}},
                                                  StartRetentionSweep{1, "cpu", 1, 2'000, 1'000},
                                                  AdvanceRetentionSweep{1, "cpu", 1, 1'000, 32}};

    for (const auto& command : commands) {
        const auto encoded = encodeCommand(command);
        ASSERT_EQ(encoded.substr(0, 4), "TCC1");
        const auto decoded = decodeCommand(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->index(), command.index());
    }
}

TEST(ControlCommandV1, RetentionFramesAreExactAndBounded) {
    const auto start = StartRetentionSweep{1, "cpu", 7, 10'000, 9'000};
    const auto decodedStart = decodeCommand(encodeCommand(start));
    ASSERT_TRUE(decodedStart);
    EXPECT_EQ(std::get<StartRetentionSweep>(*decodedStart).sweepId, start.sweepId);
    EXPECT_EQ(std::get<StartRetentionSweep>(*decodedStart).measurement, start.measurement);
    EXPECT_EQ(std::get<StartRetentionSweep>(*decodedStart).policyVersion, start.policyVersion);
    EXPECT_EQ(std::get<StartRetentionSweep>(*decodedStart).issuedAtNanos, start.issuedAtNanos);
    EXPECT_EQ(std::get<StartRetentionSweep>(*decodedStart).cutoffTime, start.cutoffTime);

    const auto advance = AdvanceRetentionSweep{1, "cpu", 7, 9'000, kRetentionFanoutBatch};
    const auto decodedAdvance = decodeCommand(encodeCommand(advance));
    ASSERT_TRUE(decodedAdvance);
    EXPECT_EQ(std::get<AdvanceRetentionSweep>(*decodedAdvance).nextVShard, kRetentionFanoutBatch);

    EXPECT_THROW(encodeCommand(StartRetentionSweep{0, "cpu", 7, 10'000, 9'000}), std::invalid_argument);
    EXPECT_THROW(encodeCommand(StartRetentionSweep{1, "", 7, 10'000, 9'000}), std::invalid_argument);
    EXPECT_THROW(encodeCommand(StartRetentionSweep{1, "cpu", 0, 10'000, 9'000}), std::invalid_argument);
    EXPECT_THROW(encodeCommand(AdvanceRetentionSweep{1, "cpu", 7, 9'000,
                                                     static_cast<uint32_t>(timestar::VIRTUAL_SHARD_COUNT) + 1}),
                 std::invalid_argument);
}

TEST(ControlCommandV1, RejectsTruncationCorruptionAndTrailingBytes) {
    const auto encoded = encodeCommand(StoreFrozenDeletePlan{plan()});
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeCommand(encoded.substr(0, n))) << n;
    auto badMagic = encoded;
    badMagic[0] ^= 1;
    EXPECT_FALSE(decodeCommand(badMagic));
    auto unknownVersion = encoded;
    unknownVersion[3] = '2';
    EXPECT_FALSE(decodeCommand(unknownVersion));
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

TEST(ControlRpcV1, MoveActuationFramesAreBoundedAndCarryExactNextJob) {
    movement::MoveJob next(movePlan(), movement::MoveStep::LearnerAdded);
    ActuateMoveResult advanced{ActuateMoveStatus::Advanced, 2,
                               Job{"move-7", static_cast<uint32_t>(next.step()), next.done(), next.encode()}};
    const auto encoded = encodeActuateMoveResult(advanced);
    EXPECT_EQ(decodeActuateMoveResult(encoded), advanced);
    for (size_t n = 0; n < encoded.size(); ++n)
        EXPECT_FALSE(decodeActuateMoveResult(encoded.substr(0, n))) << n;
    EXPECT_FALSE(decodeActuateMoveResult(encoded + "x"));

    for (auto status : {ActuateMoveStatus::NotLeader, ActuateMoveStatus::Rejected, ActuateMoveStatus::Unavailable}) {
        const ActuateMoveResult result{status, 3, {}};
        EXPECT_EQ(decodeActuateMoveResult(encodeActuateMoveResult(result)), result);
    }
    advanced.job.step += 1;
    EXPECT_THROW(encodeActuateMoveResult(advanced), std::invalid_argument);

    movement::MovePlan largest{/*vshard=*/7, /*dest=*/2000, /*victim=*/1, /*mapEpoch=*/2, {}};
    for (NodeId id = 1; id <= 1024; ++id)
        largest.sourceVoters.push_back(id);
    movement::MoveJob largestNext(std::move(largest), movement::MoveStep::LearnerAdded);
    const ActuateMoveResult largestResult{
        ActuateMoveStatus::Advanced, 2,
        Job{std::string(kMaxControlJobIdBytes, 'j'), static_cast<uint32_t>(largestNext.step()), largestNext.done(),
            largestNext.encode()}};
    const auto largestFrame = encodeActuateMoveResult(largestResult);
    EXPECT_LE(largestFrame.size(), kMaxActuateMoveFrameBytes);
    EXPECT_EQ(decodeActuateMoveResult(largestFrame), largestResult)
        << "every structurally valid v1 movement job must fit its actuator frame";
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

TEST(ControllerJobDriverV1, ActuationRequiresAnActiveReplicaAndExactlyOneForwardStep) {
    Group0State state;
    state.clusterUuid = std::string(32, 'a');
    state.controllerTerm = 9;
    state.controllerLeader = 2;
    state.mapEpoch = 2;
    for (NodeId id : {1, 2, 3, 4, 5})
        state.nodes.emplace(id, node(id, std::string(32, static_cast<char>('0' + id))));
    state.servingMap = servingMap();
    movement::MoveJob planned(movePlan());
    state.desiredPlacement.emplace(7, planned.targetVoters());
    const Job current{"move-7", 0, false, planned.encode()};
    state.jobs.emplace(current.id, current);
    const ActuateMoveRequest request{state.clusterUuid, current.id, 9, 2};

    EXPECT_TRUE(timestar::cluster::ControllerJobDriver::authorizeActuation(state, 1, request));
    EXPECT_TRUE(timestar::cluster::ControllerJobDriver::authorizeActuation(state, 4, request));
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::authorizeActuation(state, 5, request));
    state.nodes.at(1).state = NodeState::Draining;
    EXPECT_TRUE(timestar::cluster::ControllerJobDriver::authorizeActuation(state, 1, request))
        << "a draining victim must be able to transfer leadership and finish evacuation";
    state.nodes.at(2).state = NodeState::Draining;
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::authorizeActuation(state, 2, request))
        << "draining does not grant unrelated actuation authority";

    movement::MoveJob learnerAdded(movePlan(), movement::MoveStep::LearnerAdded);
    const Job next{current.id, static_cast<uint32_t>(learnerAdded.step()), learnerAdded.done(), learnerAdded.encode()};
    EXPECT_TRUE(timestar::cluster::ControllerJobDriver::isNextMoveJob(current, next));
    movement::MoveJob skipped(movePlan(), movement::MoveStep::CaughtUp);
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::isNextMoveJob(
        current, Job{current.id, static_cast<uint32_t>(skipped.step()), skipped.done(), skipped.encode()}));
    auto changedPlan = movePlan();
    changedPlan.vshard = 8;
    movement::MoveJob changed(std::move(changedPlan), movement::MoveStep::LearnerAdded);
    EXPECT_FALSE(timestar::cluster::ControllerJobDriver::isNextMoveJob(
        current, Job{current.id, static_cast<uint32_t>(changed.step()), changed.done(), changed.encode()}));
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

TEST(Group0StateMachineV1, RetentionCasAndFanoutAreCrashReplaySafe) {
    Group0StateMachine sm;
    const RetentionPolicyValue policy{"1s", 1'000};
    const std::string key = retentionPolicyKey("cpu");
    EXPECT_TRUE(encodeRetentionPolicyValue({"overflow", UINT64_MAX}).empty());
    ASSERT_TRUE(sm.applyCommand(CasPolicy{key, 0, encodeRetentionPolicyValue(policy)}));
    EXPECT_EQ(sm.state().policies.at(key), (PolicyCell{1, encodeRetentionPolicyValue(policy)}));
    EXPECT_FALSE(sm.applyCommand(CasPolicy{key, 0, encodeRetentionPolicyValue({"2s", 2'000})}));

    EXPECT_FALSE(sm.applyCommand(StartRetentionSweep{1, "cpu", 1, 1'000, 1}))
        << "issued time must be strictly beyond the TTL";
    EXPECT_FALSE(sm.applyCommand(StartRetentionSweep{1, "cpu", 1, 5'000, 3'999}))
        << "replicas independently verify the leader-computed cutoff";
    ASSERT_TRUE(sm.applyCommand(StartRetentionSweep{1, "cpu", 1, 5'000, 4'000}));
    EXPECT_FALSE(sm.applyCommand(CasPolicy{key, 1, encodeRetentionPolicyValue({"2s", 2'000})}))
        << "a policy cannot change under an in-flight exact-version fanout";
    EXPECT_FALSE(sm.applyCommand(AdvanceRetentionSweep{1, "cpu", 1, 4'000, kRetentionFanoutBatch + 1}));

    for (uint32_t next = kRetentionFanoutBatch; next < timestar::VIRTUAL_SHARD_COUNT; next += kRetentionFanoutBatch)
        ASSERT_TRUE(sm.applyCommand(AdvanceRetentionSweep{1, "cpu", 1, 4'000, next})) << next;
    ASSERT_TRUE(sm.applyCommand(
        AdvanceRetentionSweep{1, "cpu", 1, 4'000, static_cast<uint32_t>(timestar::VIRTUAL_SHARD_COUNT)}));
    EXPECT_FALSE(sm.state().retentionSweep);
    EXPECT_EQ(sm.state().lastRetentionSweepId, 1u);
    EXPECT_EQ(sm.state().retentionCutoffs.at("cpu"), (RetentionCutoffRecord{1, 4'000}));
    EXPECT_FALSE(sm.applyCommand(
        AdvanceRetentionSweep{1, "cpu", 1, 4'000, static_cast<uint32_t>(timestar::VIRTUAL_SHARD_COUNT)}));

    ASSERT_TRUE(sm.applyCommand(CasPolicy{key, 1, encodeRetentionPolicyValue({"2s", 2'000})}));
    ASSERT_TRUE(sm.applyCommand(CasPolicy{key, 2, {}}));
    EXPECT_TRUE(sm.state().policies.at(key).value.empty());
    EXPECT_FALSE(sm.applyCommand(StartRetentionSweep{2, "cpu", 3, 10'000, 8'000}));
}

TEST(Group0StateMachineV1, RetentionSweepAndCutoffSurviveSnapshot) {
    Group0StateMachine source;
    const auto encoded = encodeRetentionPolicyValue({"15m", kRetentionSweepIntervalNanos});
    ASSERT_TRUE(source.applyCommand(CasPolicy{retentionPolicyKey("a"), 0, encoded}));
    ASSERT_TRUE(source.applyCommand(CasPolicy{retentionPolicyKey("b"), 0, encoded}));
    const uint64_t issued = 2 * kRetentionSweepIntervalNanos;
    ASSERT_TRUE(source.applyCommand(StartRetentionSweep{1, "a", 1, issued, kRetentionSweepIntervalNanos}));
    ASSERT_TRUE(source.applyCommand(AdvanceRetentionSweep{1, "a", 1, kRetentionSweepIntervalNanos, 25}));

    Group0StateMachine restored;
    ASSERT_TRUE(restored.loadSnapshot(source.snapshot()));
    EXPECT_EQ(restored.state(), source.state());

    for (uint32_t next = 25 + kRetentionFanoutBatch; next < timestar::VIRTUAL_SHARD_COUNT;
         next += kRetentionFanoutBatch)
        ASSERT_TRUE(restored.applyCommand(AdvanceRetentionSweep{1, "a", 1, kRetentionSweepIntervalNanos, next}))
            << next;
    ASSERT_TRUE(restored.applyCommand(AdvanceRetentionSweep{1, "a", 1, kRetentionSweepIntervalNanos,
                                                            static_cast<uint32_t>(timestar::VIRTUAL_SHARD_COUNT)}));
    Group0StateMachine completed;
    ASSERT_TRUE(completed.loadSnapshot(restored.snapshot()));
    EXPECT_EQ(completed.state().retentionCutoffs.at("a"), (RetentionCutoffRecord{1, kRetentionSweepIntervalNanos}));
}

TEST(Group0StateMachineV1, NodeLifecycleIsForwardOnly) {
    Group0StateMachine sm;
    ASSERT_TRUE(sm.applyCommand(UpsertNode{node(7, "uuid-7", NodeState::Joining)}));

    EXPECT_FALSE(sm.applyCommand(UpsertNode{node(7, "uuid-7", NodeState::Active)}));
    EXPECT_EQ(sm.state().nodes.at(7).state, NodeState::Joining);
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Draining}));
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Removed}));
    EXPECT_TRUE(sm.applyCommand(SetNodeState{7, NodeState::Active}));
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Joining}));
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Active}));
    EXPECT_TRUE(sm.applyCommand(SetNodeState{7, NodeState::Draining}));
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Active}));
    EXPECT_TRUE(sm.applyCommand(SetNodeState{7, NodeState::Removed}));
    EXPECT_FALSE(sm.applyCommand(SetNodeState{7, NodeState::Draining}));
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

TEST(Group0StateMachineV1, NextMoveAtomicallyReplacesCompletedHistory) {
    Group0StateMachine sm;
    ControlMap current = servingMap();
    ASSERT_TRUE(sm.applyCommand(PublishServingMap{current, {}}));
    ASSERT_TRUE(sm.applyCommand(UpsertNode{node(4, "uuid-4")}));
    ASSERT_TRUE(sm.applyCommand(UpsertNode{node(5, "uuid-5")}));
    ASSERT_TRUE(sm.applyCommand(PlanVShardMove{"first", movePlan()}));
    for (auto step : {movement::MoveStep::LearnerAdded, movement::MoveStep::CaughtUp, movement::MoveStep::Promoted,
                      movement::MoveStep::OldRemoved, movement::MoveStep::Done})
        ASSERT_TRUE(sm.applyCommand(advance("first", step)));
    current.epoch = 2;
    current.placement.at(7) = {4, 2, 3};
    ASSERT_TRUE(sm.applyCommand(PublishServingMap{current, "first"}));

    movement::MovePlan second{/*vshard=*/8, /*dest=*/5, /*victim=*/3, /*mapEpoch=*/3,
                              /*sourceVoters=*/{1, 2, 3}};
    ASSERT_TRUE(sm.applyCommand(PlanVShardMove{"second", second}));
    ASSERT_EQ(sm.state().jobs.size(), 1u);
    EXPECT_TRUE(sm.state().jobs.contains("second"));
    EXPECT_FALSE(sm.state().jobs.contains("first"));
    ASSERT_EQ(sm.state().desiredPlacement.size(), 1u);
    EXPECT_EQ(sm.state().desiredPlacement.at(8), (std::vector<NodeId>{1, 2, 5}));
    EXPECT_EQ(sm.snapshot().substr(0, 8), "TSG0SNP1")
        << "the bounded current-only history remains part of the same exact v1 snapshot";
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
    auto unknownVersion = snapshot;
    unknownVersion[7] = '2';
    EXPECT_FALSE(restored.loadSnapshot(unknownVersion));
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
