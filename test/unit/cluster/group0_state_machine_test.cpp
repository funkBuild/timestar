#include "../../../lib/cluster/control/control_command.hpp"
#include "../../../lib/cluster/control/group0_state_machine.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;

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

FrozenDeletePlan plan() {
    return FrozenDeletePlan{std::string(32, '1'), std::string(32, 'a'), 1'800'000'000'000,
                            {{"cpu,host=a value", 10, 20}, {"cpu,host=b value", 30, 40}}};
}
}  // namespace

TEST(ControlCommandV1, RoundTripsEveryCommand) {
    const std::vector<ControlCommand> commands = {
        InitCluster{"cluster-a"},
        UpsertNode{node(7, "uuid-7")},
        SetNodeState{7, NodeState::Draining},
        SetDesiredPlacement{42, {1, 2, 3}},
        SetMetaVoters{{1, 2, 3}},
        CasPolicy{"schema/cpu/value", 0, "float"},
        SetControllerTerm{9, 3},
        UpsertJob{"job-1", 5, true, "payload"},
        MintJoinToken{"token"},
        AdmitWithToken{node(8, "uuid-8", NodeState::Joining), "token"},
        StoreFrozenDeletePlan{plan()},
        SetInitialServingMap{ControlMap{1, {{0, {1, 2, 3}}}, {{0, 7}}}}};

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

    for (auto status : {FreezeDeletePlanStatus::Stored, FreezeDeletePlanStatus::NotFound,
                        FreezeDeletePlanStatus::NotLeader, FreezeDeletePlanStatus::Conflict,
                        FreezeDeletePlanStatus::Capacity, FreezeDeletePlanStatus::Invalid}) {
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
    for (const auto result : {ControlJoinResult{ControlJoinStatus::NotLeader, 0},
                              ControlJoinResult{ControlJoinStatus::Rejected, 1},
                              ControlJoinResult{ControlJoinStatus::Joining, 1},
                              ControlJoinResult{ControlJoinStatus::Active, 1}})
        EXPECT_EQ(decodeControlJoinResult(encodeControlJoinResult(result)), result);
}

TEST(Group0StateMachineV1, AppliesIdentityPolicyMembershipAndTokens) {
    Group0StateMachine sm;
    EXPECT_TRUE(sm.applyCommand(InitCluster{"cluster-a"}));
    EXPECT_FALSE(sm.applyCommand(InitCluster{"cluster-b"}));
    EXPECT_TRUE(sm.applyCommand(UpsertNode{node(1, "uuid-1")}));
    EXPECT_FALSE(sm.applyCommand(UpsertNode{node(2, "uuid-1")}));
    EXPECT_TRUE(sm.applyCommand(SetDesiredPlacement{7, {1, 2, 3}}));
    EXPECT_EQ(sm.state().mapEpoch, 1u);
    EXPECT_TRUE(sm.applyCommand(SetMetaVoters{{1, 2, 3}}));
    EXPECT_TRUE(sm.applyCommand(CasPolicy{"schema/cpu/value", 0, "float"}));
    EXPECT_FALSE(sm.applyCommand(CasPolicy{"schema/cpu/value", 0, "integer"}));
    EXPECT_EQ(sm.state().policies.at("schema/cpu/value").version, 1u);

    EXPECT_TRUE(sm.applyCommand(MintJoinToken{"join-once"}));
    EXPECT_TRUE(sm.applyCommand(AdmitWithToken{node(4, "uuid-4", NodeState::Joining), "join-once"}));
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{node(5, "uuid-5", NodeState::Joining), "join-once"}));
    EXPECT_TRUE(sm.applyCommand(SetNodeState{4, NodeState::Active}));
}

TEST(Group0StateMachineV1, ServingMapAndFrozenPlansAreSingleAssignmentAndIdempotent) {
    Group0StateMachine sm;
    const auto map = servingMap();
    EXPECT_TRUE(sm.applyCommand(SetInitialServingMap{map}));
    EXPECT_FALSE(sm.applyCommand(SetInitialServingMap{map}));
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
    ASSERT_TRUE(source.applyCommand(SetInitialServingMap{servingMap()}));
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
