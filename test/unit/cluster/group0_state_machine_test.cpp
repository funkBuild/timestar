// Group-0 control state machine: deterministic command apply + CAS + monotonic
// controller-term fencing + idempotent jobs + snapshot round-trip, plus the
// command wire codec (round-trip + truncation robustness). All pure -- no reactor.
#include "../../../lib/cluster/control/control_command.hpp"
#include "../../../lib/cluster/control/group0_state_machine.hpp"

#include <gtest/gtest.h>

using namespace timestar::control;

namespace {

NodeRecord node(NodeId id, std::string uuid, std::string fd, NodeState st = NodeState::Active) {
    NodeRecord r;
    r.raftId = id;
    r.uuid = std::move(uuid);
    r.address = "127.0.0.1:" + std::to_string(id);
    r.failureDomain = std::move(fd);
    r.state = st;
    return r;
}

}  // namespace

TEST(ControlCommandCodecTest, RoundTripAllCommands) {
    std::vector<ControlCommand> cmds = {
        InitCluster{"cluster-abc"},
        UpsertNode{node(7, "uuid-7", "rack-a")},
        SetNodeState{7, NodeState::Draining},
        SetDesiredPlacement{42, {1, 2, 3}},
        SetMetaVoters{{1, 2, 3}},
        CasPolicy{"schema/m/f", 0, "float"},
        SetControllerTerm{9, 3},
        UpsertJob{"job-1", 5, true, "move v42"},
        MintJoinToken{"tok-abc"},
        AdmitWithToken{node(8, "u8", "rack-h"), "tok-abc"},
    };
    for (const auto& c : cmds) {
        auto back = decodeCommand(encodeCommand(c));
        ASSERT_TRUE(back.has_value());
        EXPECT_EQ(back->index(), c.index());  // same alternative
    }
    // Spot-check a couple of payloads survive.
    auto up = decodeCommand(encodeCommand(UpsertNode{node(7, "uuid-7", "rack-a")}));
    ASSERT_TRUE(up && std::holds_alternative<UpsertNode>(*up));
    EXPECT_EQ(std::get<UpsertNode>(*up).record.failureDomain, "rack-a");
    auto cas = decodeCommand(encodeCommand(CasPolicy{"k", 3, "v"}));
    ASSERT_TRUE(cas && std::holds_alternative<CasPolicy>(*cas));
    EXPECT_EQ(std::get<CasPolicy>(*cas).expectedVersion, 3u);
}

TEST(ControlCommandCodecTest, TruncatedAndUnknownRejected) {
    std::string full = encodeCommand(UpsertNode{node(7, "uuid-7", "rack-a")});
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeCommand(full.substr(0, n)).has_value()) << "prefix " << n;
    EXPECT_TRUE(decodeCommand(full).has_value());
    EXPECT_FALSE(decodeCommand(std::string(1, static_cast<char>(0xEE))).has_value());  // unknown tag
    EXPECT_FALSE(decodeCommand("").has_value());
}

TEST(Group0StateMachineTest, AppliesCoreCommands) {
    Group0StateMachine sm;
    sm.applyCommand(InitCluster{"cluster-1"});
    sm.applyCommand(InitCluster{"cluster-2"});  // never re-inits
    EXPECT_EQ(sm.state().clusterUuid, "cluster-1");

    sm.applyCommand(UpsertNode{node(1, "u1", "rack-a")});
    sm.applyCommand(UpsertNode{node(2, "u2", "rack-b")});
    EXPECT_EQ(sm.state().nodes.size(), 2u);
    sm.applyCommand(SetNodeState{2, NodeState::Draining});
    EXPECT_EQ(sm.state().nodes.at(2).state, NodeState::Draining);
    sm.applyCommand(SetNodeState{99, NodeState::Down});  // unknown node: no-op
    EXPECT_EQ(sm.state().nodes.size(), 2u);

    sm.applyCommand(SetMetaVoters{{1, 2}});
    EXPECT_EQ(sm.state().metaVoters, (std::vector<NodeId>{1, 2}));
}

TEST(Group0StateMachineTest, DesiredPlacementBumpsEpoch) {
    Group0StateMachine sm;
    EXPECT_EQ(sm.state().mapEpoch, 0u);
    sm.applyCommand(SetDesiredPlacement{5, {1, 2, 3}});
    EXPECT_EQ(sm.state().mapEpoch, 1u);
    sm.applyCommand(SetDesiredPlacement{6, {2, 3, 4}});
    EXPECT_EQ(sm.state().mapEpoch, 2u);
    EXPECT_EQ(sm.state().desiredPlacement.at(5), (std::vector<NodeId>{1, 2, 3}));
}

TEST(Group0StateMachineTest, CasSucceedsOnMatchFailsOnStale) {
    Group0StateMachine sm;
    // New key: expectedVersion 0 succeeds, version -> 1.
    EXPECT_TRUE(sm.applyCommand(CasPolicy{"schema/m/f", 0, "float"}));
    EXPECT_EQ(sm.state().policies.at("schema/m/f").version, 1u);
    EXPECT_EQ(sm.state().policies.at("schema/m/f").value, "float");
    // Stale expected version fails, state unchanged (lost update rejected).
    EXPECT_FALSE(sm.applyCommand(CasPolicy{"schema/m/f", 0, "int"}));
    EXPECT_EQ(sm.state().policies.at("schema/m/f").value, "float");
    // Correct expected version succeeds.
    EXPECT_TRUE(sm.applyCommand(CasPolicy{"schema/m/f", 1, "double"}));
    EXPECT_EQ(sm.state().policies.at("schema/m/f").version, 2u);
    EXPECT_EQ(sm.state().policies.at("schema/m/f").value, "double");
}

TEST(Group0StateMachineTest, ControllerTermIsMonotonic) {
    Group0StateMachine sm;
    sm.applyCommand(SetControllerTerm{5, 1});
    EXPECT_EQ(sm.state().controllerTerm, 5u);
    EXPECT_EQ(sm.state().controllerLeader, 1u);
    // A lower/equal term from a deposed controller is ignored (fencing).
    sm.applyCommand(SetControllerTerm{3, 2});
    EXPECT_EQ(sm.state().controllerTerm, 5u);
    EXPECT_EQ(sm.state().controllerLeader, 1u);
    sm.applyCommand(SetControllerTerm{7, 2});
    EXPECT_EQ(sm.state().controllerTerm, 7u);
    EXPECT_EQ(sm.state().controllerLeader, 2u);
}

TEST(Group0StateMachineTest, JobsAreIdempotentAndMonotonic) {
    Group0StateMachine sm;
    sm.applyCommand(UpsertJob{"j1", 1, false, "p"});
    sm.applyCommand(UpsertJob{"j1", 2, false, "p"});
    EXPECT_EQ(sm.state().jobs.at("j1").step, 2u);
    // Re-applying an older step (crash-resume replay) never regresses.
    sm.applyCommand(UpsertJob{"j1", 1, false, "p"});
    EXPECT_EQ(sm.state().jobs.at("j1").step, 2u);
    sm.applyCommand(UpsertJob{"j1", 3, true, "p"});
    EXPECT_TRUE(sm.state().jobs.at("j1").done);
    // Completion is sticky.
    sm.applyCommand(UpsertJob{"j1", 3, false, "p"});
    EXPECT_TRUE(sm.state().jobs.at("j1").done);
}

TEST(Group0StateMachineTest, JoinTokenGatesAdmission) {
    Group0StateMachine sm;
    NodeRecord n = node(5, "u5", "rack-e", NodeState::Joining);
    // Without a minted token, admission is rejected (never implicit init).
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{n, "tok-1"}));
    EXPECT_EQ(sm.state().nodes.count(5), 0u);
    // Mint the token, then admit -> node recorded Active, token consumed.
    sm.applyCommand(MintJoinToken{"tok-1"});
    EXPECT_EQ(sm.state().joinTokens.count("tok-1"), 1u);
    EXPECT_TRUE(sm.applyCommand(AdmitWithToken{n, "tok-1"}));
    EXPECT_EQ(sm.state().nodes.at(5).state, NodeState::Active);
    EXPECT_EQ(sm.state().joinTokens.count("tok-1"), 0u);  // single-use
    // Replaying the same token is rejected (already consumed).
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{node(6, "u6", "rack-f"), "tok-1"}));
    EXPECT_EQ(sm.state().nodes.count(6), 0u);
}

TEST(Group0StateMachineTest, FailedCasDoesNotCreatePhantomCell) {
    Group0StateMachine sm;
    // CAS with a non-zero expected version on an absent key must fail AND leave
    // no trace (no phantom version-0 cell).
    EXPECT_FALSE(sm.applyCommand(CasPolicy{"absent", 5, "v"}));
    EXPECT_EQ(sm.state().policies.count("absent"), 0u);
    EXPECT_TRUE(sm.state().policies.empty());
}

TEST(Group0StateMachineTest, JobPayloadDoesNotRegressOnOldReplay) {
    Group0StateMachine sm;
    sm.applyCommand(UpsertJob{"j", 3, false, "new"});
    EXPECT_EQ(sm.state().jobs.at("j").payload, "new");
    // An out-of-order replay of an older step must not overwrite the newer payload.
    sm.applyCommand(UpsertJob{"j", 1, false, "old"});
    EXPECT_EQ(sm.state().jobs.at("j").step, 3u);
    EXPECT_EQ(sm.state().jobs.at("j").payload, "new");
}

TEST(Group0StateMachineTest, UndecodableCommittedCommandIsFatal) {
    Group0StateMachine sm;
    timestar::raft::LogEntry e;
    e.index = 1;
    e.type = timestar::raft::EntryType::Normal;
    e.data = std::string(1, static_cast<char>(0xEE));  // unknown command tag
    EXPECT_THROW((void)sm.apply(e), std::runtime_error);
}

TEST(Group0StateMachineTest, SnapshotRoundTrip) {
    Group0StateMachine sm;
    sm.applyCommand(InitCluster{"cluster-x"});
    sm.applyCommand(UpsertNode{node(1, "u1", "rack-a")});
    sm.applyCommand(UpsertNode{node(2, "u2", "rack-b", NodeState::Draining)});
    sm.applyCommand(SetDesiredPlacement{7, {1, 2, 3}});
    sm.applyCommand(SetMetaVoters{{1, 2, 3}});
    sm.applyCommand(CasPolicy{"schema/m/f", 0, "float"});
    sm.applyCommand(SetControllerTerm{4, 1});
    sm.applyCommand(UpsertJob{"j1", 2, true, "done"});
    sm.applyCommand(MintJoinToken{"pending-token"});

    const std::string blob = sm.snapshot();
    Group0StateMachine restored;
    ASSERT_TRUE(restored.loadSnapshot(blob));
    EXPECT_EQ(restored.state(), sm.state());

    // A corrupt snapshot leaves the target unchanged.
    Group0StateMachine other;
    other.applyCommand(InitCluster{"keep-me"});
    EXPECT_FALSE(other.loadSnapshot(blob.substr(0, blob.size() / 2)));
    EXPECT_EQ(other.state().clusterUuid, "keep-me");
}
