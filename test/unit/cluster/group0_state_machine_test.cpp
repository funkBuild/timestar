// Group-0 control state machine: deterministic command apply + CAS + monotonic
// controller-term fencing + idempotent jobs + snapshot round-trip, plus the
// command wire codec (round-trip + truncation robustness). All pure -- no reactor.
#include "../../../lib/cluster/control/control_command.hpp"
#include "../../../lib/cluster/control/group0_state_machine.hpp"
#include "../../../lib/cluster/data/journal_format.hpp"
#include "../../../lib/core/vshard.hpp"

#include <gtest/gtest.h>

#include <cstdio>
#include <seastar/core/thread.hh>

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

ControlMap completeServingMap(uint64_t epoch = 1) {
    ControlMap map;
    map.epoch = epoch;
    for (uint16_t vshard = 0; vshard < timestar::VIRTUAL_SHARD_COUNT; ++vshard)
        map.placement.emplace(vshard, std::vector<NodeId>{1, 2, 3});
    return map;
}

FrozenDeletePlan deletePlan(char id, char fingerprint, uint64_t issuedAtMs,
                            std::vector<FrozenDeleteTarget> targets = {}) {
    return FrozenDeletePlan{std::string(32, id), std::string(32, fingerprint), issuedAtMs, std::move(targets)};
}

uint64_t readU64(const std::string& bytes, size_t& offset) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes.at(offset++))) << (8 * i);
    return value;
}

void writeU64(std::string& bytes, size_t offset, uint64_t value) {
    for (unsigned i = 0; i < 8; ++i)
        bytes.at(offset + i) = static_cast<char>((value >> (8 * i)) & 0xff);
}

void skipString(const std::string& bytes, size_t& offset) {
    const uint64_t size = readU64(bytes, offset);
    offset += size;
    ASSERT_LE(offset, bytes.size());
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
        StoreFrozenDeletePlan{deletePlan('1', 'a', 1'800'000'000'000,
                                               {{"m,host=a value", 10, 20}})},
        SetActiveVersion{5, {1, 2, 3}},
        SetInitialServingMap{ControlMap{1, {{0, {1, 2, 3}}}, {{0, 7}}}},
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
    auto serving = decodeCommand(encodeCommand(SetInitialServingMap{ControlMap{1, {{7, {2, 3}}}, {{7, 4}}}}));
    ASSERT_TRUE(serving && std::holds_alternative<SetInitialServingMap>(*serving));
    EXPECT_EQ(std::get<SetInitialServingMap>(*serving).map.placement.at(7), (std::vector<NodeId>{2, 3}));
    EXPECT_EQ(std::get<SetInitialServingMap>(*serving).map.groups.at(7), 4);
    auto activation = decodeCommand(encodeCommand(SetActiveVersion{5, {1, 2, 3}}));
    ASSERT_TRUE(activation && std::holds_alternative<SetActiveVersion>(*activation));
    EXPECT_EQ(std::get<SetActiveVersion>(*activation).coveredVoters, (std::vector<NodeId>{1, 2, 3}));
    auto plan = decodeCommand(encodeCommand(StoreFrozenDeletePlan{
        deletePlan('1', 'a', 1'800'000'000'000, {{"m,host=a value", 10, 20}})}));
    ASSERT_TRUE(plan && std::holds_alternative<StoreFrozenDeletePlan>(*plan));
    EXPECT_EQ(std::get<StoreFrozenDeletePlan>(*plan).plan.targets[0].seriesKey, "m,host=a value");
}

TEST(ControlCommandCodecTest, TruncatedAndUnknownRejected) {
    std::string full = encodeCommand(UpsertNode{node(7, "uuid-7", "rack-a")});
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeCommand(full.substr(0, n)).has_value()) << "prefix " << n;
    EXPECT_TRUE(decodeCommand(full).has_value());
    EXPECT_FALSE(decodeCommand(std::string(1, static_cast<char>(0xEE))).has_value());  // unknown tag
    EXPECT_FALSE(decodeCommand("").has_value());
}

TEST(ControlCommandCodecTest, TrailingBytesRejected) {
    std::string full = encodeCommand(SetMetaVoters{{1, 2, 3}});
    full.push_back('\0');
    EXPECT_FALSE(decodeCommand(full).has_value());
}

TEST(ControlCommandCodecTest, CoveredActivationCannotTruncateIntoLegacyActivation) {
    const std::string covered = encodeCommand(SetActiveVersion{5, {1, 2, 3}});
    for (size_t n = 0; n < covered.size(); ++n)
        EXPECT_FALSE(decodeCommand(covered.substr(0, n)).has_value()) << "prefix " << n;

    std::string legacy = covered.substr(0, 1 + sizeof(uint64_t));
    legacy.front() = 11;  // historical, uncovered SetActiveVersion tag
    auto decoded = decodeCommand(legacy);
    ASSERT_TRUE(decoded && std::holds_alternative<SetActiveVersion>(*decoded));
    EXPECT_TRUE(std::get<SetActiveVersion>(*decoded).coveredVoters.empty());
}

TEST(ControlCommandCodecTest, FrozenDeletePlanRejectsEveryTruncatedPrefix) {
    const std::string full = encodeCommand(StoreFrozenDeletePlan{
        deletePlan('1', 'a', 1'800'000'000'000,
                   {{"m,host=a value", 10, 20}, {"m,host=b value", 30, 40}})});
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodeCommand(full.substr(0, n)).has_value()) << "prefix " << n;
    EXPECT_TRUE(decodeCommand(full).has_value());
}

TEST(ControlCommandCodecTest, FrozenDeletePlanRpcRequestsRoundTripAndFailClosed) {
    const FrozenDeletePlan lookupPlan = deletePlan('1', 'a', 1'800'000'000'000);
    const FrozenDeletePlan freezePlan = deletePlan(
        '2', 'b', 1'800'000'000'001,
        {{"m,host=a value", 10, 20}, {"m,host=b value", 30, 40}});

    for (const auto& request :
         {FrozenDeletePlanRpcRequest{FrozenDeletePlanRpcOperation::Lookup, lookupPlan},
          FrozenDeletePlanRpcRequest{FrozenDeletePlanRpcOperation::Freeze, freezePlan}}) {
        const std::string encoded = encodeFrozenDeletePlanRpcRequest(request);
        auto decoded = decodeFrozenDeletePlanRpcRequest(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->operation, request.operation);
        EXPECT_EQ(decoded->plan, request.plan);
        for (size_t n = 0; n < encoded.size(); ++n)
            EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(encoded.substr(0, n))) << "prefix " << n;

        std::string trailing = encoded;
        trailing.push_back('\0');
        EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(trailing));
    }

    std::string unknown = encodeFrozenDeletePlanRpcRequest(
        {FrozenDeletePlanRpcOperation::Lookup, lookupPlan});
    unknown.front() = static_cast<char>(0xff);
    EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(unknown));

    auto lookupWithTargets = freezePlan;
    EXPECT_THROW(encodeFrozenDeletePlanRpcRequest(
                     {FrozenDeletePlanRpcOperation::Lookup, lookupWithTargets}),
                 std::invalid_argument);
    std::string mislabeled = encodeFrozenDeletePlanRpcRequest(
        {FrozenDeletePlanRpcOperation::Freeze, lookupWithTargets});
    mislabeled.front() = static_cast<char>(FrozenDeletePlanRpcOperation::Lookup);
    EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(mislabeled));
    EXPECT_FALSE(decodeFrozenDeletePlanRpcRequest(
        std::string(kMaxFrozenDeletePlanBytes + 1, '\0')));
}

TEST(ControlCommandCodecTest, FrozenDeletePlanRpcResultsRoundTripAndFailClosed) {
    const FrozenDeletePlan plan = deletePlan(
        '1', 'a', 1'800'000'000'000, {{"m,host=a value", 10, 20}});
    const std::vector<FreezeDeletePlanResult> results = {
        {FreezeDeletePlanStatus::Stored, plan},
        {FreezeDeletePlanStatus::Conflict, plan},
        {FreezeDeletePlanStatus::NotFound, {}},
        {FreezeDeletePlanStatus::NotLeader, {}},
        {FreezeDeletePlanStatus::Capacity, {}},
        {FreezeDeletePlanStatus::FormatInactive, {}},
        {FreezeDeletePlanStatus::Invalid, {}},
    };
    for (const auto& result : results) {
        const std::string encoded = encodeFrozenDeletePlanRpcResult(result);
        auto decoded = decodeFrozenDeletePlanRpcResult(encoded);
        ASSERT_TRUE(decoded);
        EXPECT_EQ(decoded->status, result.status);
        EXPECT_EQ(decoded->plan, result.plan);
        for (size_t n = 0; n < encoded.size(); ++n)
            EXPECT_FALSE(decodeFrozenDeletePlanRpcResult(encoded.substr(0, n))) << "prefix " << n;

        std::string trailing = encoded;
        trailing.push_back('\0');
        EXPECT_FALSE(decodeFrozenDeletePlanRpcResult(trailing));
    }

    EXPECT_THROW(encodeFrozenDeletePlanRpcResult({FreezeDeletePlanStatus::NotFound, plan}),
                 std::invalid_argument);
    EXPECT_THROW(encodeFrozenDeletePlanRpcResult(
                     {static_cast<FreezeDeletePlanStatus>(0xff), {}}),
                 std::invalid_argument);
    EXPECT_FALSE(decodeFrozenDeletePlanRpcResult(std::string(1, static_cast<char>(0xff))));

    std::string planOnNoPlanStatus =
        encodeFrozenDeletePlanRpcResult({FreezeDeletePlanStatus::Stored, plan});
    planOnNoPlanStatus.front() = static_cast<char>(FreezeDeletePlanStatus::NotFound);
    EXPECT_FALSE(decodeFrozenDeletePlanRpcResult(planOnNoPlanStatus));
    EXPECT_FALSE(decodeFrozenDeletePlanRpcResult(
        std::string(kMaxFrozenDeletePlanBytes + 1, '\0')));
}

TEST(ControlCommandCodecTest, NarrowFieldsAndEnumsFailClosed) {
    std::string state = encodeCommand(SetNodeState{7, NodeState::Joining});
    state.back() = static_cast<char>(0xff);
    EXPECT_FALSE(decodeCommand(state).has_value());

    std::string record = encodeCommand(UpsertNode{node(7, "uuid-7", "rack-a")});
    record.back() = static_cast<char>(0xff);
    EXPECT_FALSE(decodeCommand(record).has_value());

    std::string job = encodeCommand(UpsertJob{"j", 1, false, "payload"});
    const size_t stepOffset = 1 + 8 + 1;  // tag + job-id length + "j"
    job.at(stepOffset + 4) = 1;           // 2^32 + 1 cannot fit uint32_t
    EXPECT_FALSE(decodeCommand(job).has_value());
    job = encodeCommand(UpsertJob{"j", 1, false, "payload"});
    job.at(stepOffset + 8) = 2;  // booleans are exactly 0 or 1
    EXPECT_FALSE(decodeCommand(job).has_value());

    std::string version = encodeCommand(SetActiveVersion{1, {1}});
    version.at(1 + 4) = 1;  // 2^32 + 1 cannot fit uint32_t
    EXPECT_FALSE(decodeCommand(version).has_value());
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

TEST(Group0StateMachineTest, PlacementRetriesAreIdempotentAndInvalidControlDataIsRejected) {
    Group0StateMachine sm;
    EXPECT_FALSE(sm.applyCommand(InitCluster{""}));
    EXPECT_TRUE(sm.applyCommand(InitCluster{"cluster-safe"}));
    EXPECT_FALSE(sm.applyCommand(InitCluster{"cluster-other"}));
    EXPECT_EQ(sm.state().clusterUuid, "cluster-safe");

    EXPECT_TRUE(sm.applyCommand(UpsertNode{node(1, "u1", "rack-a")}));
    EXPECT_FALSE(sm.applyCommand(UpsertNode{node(2, "u1", "rack-b")}))
        << "one persistent node UUID cannot occupy two Raft ids";
    EXPECT_FALSE(sm.applyCommand(UpsertNode{node(0, "u0", "rack-z")}));
    EXPECT_EQ(sm.state().nodes.size(), 1u);

    EXPECT_TRUE(sm.applyCommand(SetDesiredPlacement{5, {1, 2, 3}}));
    const uint64_t committedEpoch = sm.state().mapEpoch;
    EXPECT_FALSE(sm.applyCommand(SetDesiredPlacement{5, {1, 2, 3}}))
        << "an ambiguous retry must not invent another topology epoch";
    EXPECT_FALSE(sm.applyCommand(SetDesiredPlacement{5, {1, 1, 3}}));
    EXPECT_FALSE(sm.applyCommand(SetDesiredPlacement{5, {0, 2, 3}}));
    EXPECT_FALSE(sm.applyCommand(SetDesiredPlacement{timestar::VIRTUAL_SHARD_COUNT, {1, 2, 3}}));
    EXPECT_EQ(sm.state().mapEpoch, committedEpoch);
    EXPECT_EQ(sm.state().desiredPlacement.at(5), (std::vector<NodeId>{1, 2, 3}));

    EXPECT_FALSE(sm.applyCommand(SetMetaVoters{{1, 1}}));
    EXPECT_TRUE(sm.applyCommand(SetMetaVoters{{1, 2, 3}}));
    EXPECT_FALSE(sm.applyCommand(SetMetaVoters{{1, 2, 3}}));
    EXPECT_FALSE(sm.applyCommand(MintJoinToken{""}));
    EXPECT_FALSE(sm.applyCommand(SetControllerTerm{1, 0}));
    EXPECT_EQ(sm.state().controllerTerm, 0u);
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

TEST(Group0StateMachineTest, ControllerEpochComesFromCommittedLogTerm) {
    Group0StateMachine sm;
    timestar::raft::LogEntry entry{
        /*term=*/7,
        /*index=*/1,
        timestar::raft::EntryType::Normal,
        encodeCommand(SetControllerTerm{/*untrusted payload term=*/999, /*leader=*/2})};
    EXPECT_NO_THROW(sm.apply(std::move(entry)).get());
    EXPECT_EQ(sm.state().controllerTerm, 7u);
    EXPECT_EQ(sm.state().controllerLeader, 2u);
}

TEST(Group0StateMachineTest, JobsAreIdempotentAndMonotonic) {
    Group0StateMachine sm;
    EXPECT_TRUE(sm.applyCommand(UpsertJob{"j1", 1, false, "p"}));
    EXPECT_TRUE(sm.applyCommand(UpsertJob{"j1", 2, false, "p"}));
    EXPECT_EQ(sm.state().jobs.at("j1").step, 2u);
    // Re-applying an older step (crash-resume replay) never regresses.
    EXPECT_FALSE(sm.applyCommand(UpsertJob{"j1", 1, false, "p"}));
    EXPECT_EQ(sm.state().jobs.at("j1").step, 2u);
    EXPECT_FALSE(sm.applyCommand(UpsertJob{"j1", 2, false, "conflict"}));
    EXPECT_EQ(sm.state().jobs.at("j1").payload, "p");
    EXPECT_TRUE(sm.applyCommand(UpsertJob{"j1", 3, true, "p"}));
    EXPECT_TRUE(sm.state().jobs.at("j1").done);
    // Completion is sticky.
    EXPECT_FALSE(sm.applyCommand(UpsertJob{"j1", 3, false, "p"}));
    EXPECT_FALSE(sm.applyCommand(UpsertJob{"j1", 4, false, "resurrect"}));
    EXPECT_TRUE(sm.state().jobs.at("j1").done);
    EXPECT_EQ(sm.state().jobs.at("j1").step, 3u);
}

TEST(Group0StateMachineTest, JoinTokenGatesAdmission) {
    Group0StateMachine sm;
    NodeRecord n = node(5, "u5", "rack-e", NodeState::Active);
    // Without a minted token, admission is rejected (never implicit init).
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{n, "tok-1"}));
    EXPECT_EQ(sm.state().nodes.count(5), 0u);
    // Even an encoded Active state cannot bypass learner-first admission.
    sm.applyCommand(MintJoinToken{"tok-1"});
    EXPECT_EQ(sm.state().joinTokens.count("tok-1"), 1u);
    EXPECT_TRUE(sm.applyCommand(AdmitWithToken{n, "tok-1"}));
    EXPECT_EQ(sm.state().nodes.at(5).state, NodeState::Joining);
    EXPECT_EQ(sm.state().joinTokens.count("tok-1"), 0u);  // single-use
    // Replaying the same token is rejected (already consumed).
    EXPECT_FALSE(sm.applyCommand(AdmitWithToken{node(6, "u6", "rack-f"), "tok-1"}));
    EXPECT_EQ(sm.state().nodes.count(6), 0u);
}

TEST(Group0StateMachineTest, FrozenDeletePlanIsFirstWriterWinsAndSnapshotDurable) {
    Group0StateMachine sm;
    const auto original = deletePlan('1', 'a', 1'800'000'000'000,
                                     {{"m,host=a value", 10, 20}});
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{original}))
        << "a plan cannot exist before the complete serving map";
    ASSERT_TRUE(sm.applyCommand(SetInitialServingMap{completeServingMap()}));
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{original}))
        << "group-0 tag 14 must stay disabled under the older format line";
    ASSERT_TRUE(sm.applyCommand(SetActiveVersion{
        timestar::data::kFrozenDeletePlanActivationVersion, {1, 2, 3}}));
    ASSERT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{original}));

    auto reexpanded = original;
    reexpanded.targets = {{"m,host=a value", 10, 20}, {"m,host=new value", 10, 20}};
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{reexpanded}));
    EXPECT_EQ(sm.state().frozenDeletePlans.at(original.requestId), original)
        << "a retry must retain the first expansion, not newly discovered series";

    auto conflictingBody = original;
    conflictingBody.requestFingerprint = std::string(32, 'b');
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{conflictingBody}));
    EXPECT_EQ(sm.state().frozenDeletePlans.at(original.requestId), original);

    const auto empty = deletePlan('2', 'c', original.issuedAtMs);
    ASSERT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{empty}));
    ASSERT_TRUE(sm.state().frozenDeletePlans.at(empty.requestId).targets.empty());

    Group0StateMachine restored;
    ASSERT_TRUE(restored.loadSnapshot(sm.snapshot()));
    EXPECT_EQ(restored.state().frozenDeletePlans, sm.state().frozenDeletePlans);
}

TEST(Group0StateMachineTest, FrozenDeletePlanRetentionAllowsFutureSkewButBoundsCount) {
    Group0StateMachine sm;
    ASSERT_TRUE(sm.applyCommand(SetInitialServingMap{completeServingMap()}));
    ASSERT_TRUE(sm.applyCommand(SetActiveVersion{
        timestar::data::kFrozenDeletePlanActivationVersion, {1, 2, 3}}));
    const uint64_t base = 1'800'000'000'000;
    const auto oldest = deletePlan('1', 'a', base);
    ASSERT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{oldest}));

    // A request timestamp may legally be five minutes in the future. It must
    // not shorten an older request's real one-hour retry window.
    ASSERT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{
        deletePlan('2', 'b', base + kFrozenDeletePlanRetentionMs)}));
    EXPECT_TRUE(sm.state().frozenDeletePlans.contains(oldest.requestId));
    ASSERT_TRUE(sm.applyCommand(StoreFrozenDeletePlan{
        deletePlan('3', 'c', base + kFrozenDeletePlanRetentionMs + kFrozenDeletePlanFutureSkewMs + 1)}));
    EXPECT_FALSE(sm.state().frozenDeletePlans.contains(oldest.requestId));

    Group0StateMachine bounded;
    ASSERT_TRUE(bounded.applyCommand(SetInitialServingMap{completeServingMap()}));
    ASSERT_TRUE(bounded.applyCommand(SetActiveVersion{
        timestar::data::kFrozenDeletePlanActivationVersion, {1, 2, 3}}));
    seastar::thread([&bounded, base] {
        for (size_t i = 0; i < kMaxFrozenDeletePlans; ++i) {
            char key[33];
            std::snprintf(key, sizeof(key), "%032zx", i + 1);
            ASSERT_TRUE(bounded.applyCommand(StoreFrozenDeletePlan{
                FrozenDeletePlan{key, std::string(32, 'd'), base, {}}}));
            if ((i & 31u) == 31u)
                seastar::thread::yield();
        }
    }).join().get();
    EXPECT_FALSE(bounded.applyCommand(StoreFrozenDeletePlan{
        FrozenDeletePlan{std::string(32, 'e'), std::string(32, 'f'), base, {}}}));
    EXPECT_EQ(bounded.state().frozenDeletePlans.size(), kMaxFrozenDeletePlans);
}

TEST(Group0StateMachineTest, FrozenDeletePlanRejectsNonCanonicalOrOversizeTargets) {
    Group0StateMachine sm;
    ASSERT_TRUE(sm.applyCommand(SetInitialServingMap{completeServingMap()}));
    ASSERT_TRUE(sm.applyCommand(SetActiveVersion{
        timestar::data::kFrozenDeletePlanActivationVersion, {1, 2, 3}}));
    auto unsorted = deletePlan('1', 'a', 1'800'000'000'000,
                               {{"z value", 1, 2}, {"a value", 1, 2}});
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{unsorted}));
    auto oversize = deletePlan('2', 'b', 1'800'000'000'000,
                               {{std::string(kMaxFrozenDeletePlanBytes, 'x'), 1, 2}});
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{oversize}));
    auto badId = deletePlan('G', 'b', 1'800'000'000'000);
    EXPECT_FALSE(sm.applyCommand(StoreFrozenDeletePlan{badId}));
    EXPECT_TRUE(sm.state().frozenDeletePlans.empty());
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
    EXPECT_THROW(sm.apply(e).get(), std::runtime_error);
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

    // Neither an incomplete optional format field nor an unknown extension may
    // be ignored: that would let replicas at different format knowledge install
    // different logical state at the same Raft snapshot boundary.
    std::string trailing = blob;
    trailing.push_back('\0');
    EXPECT_FALSE(other.loadSnapshot(trailing));
    EXPECT_EQ(other.state().clusterUuid, "keep-me");

    std::string legacy = blob.substr(0, blob.size() - sizeof(uint64_t));
    Group0StateMachine legacyRestored;
    ASSERT_TRUE(legacyRestored.loadSnapshot(legacy));
    EXPECT_EQ(legacyRestored.state().activeFormatVersion, 1u);
    legacy.push_back('\0');  // partial optional format field
    EXPECT_FALSE(other.loadSnapshot(legacy));
    EXPECT_EQ(other.state().clusterUuid, "keep-me");
}

TEST(Group0StateMachineTest, SemanticallyInvalidSnapshotLeavesOldStateUntouched) {
    Group0StateMachine source;
    ASSERT_TRUE(source.applyCommand(InitCluster{"cluster-snapshot"}));
    ASSERT_TRUE(source.applyCommand(UpsertNode{node(1, "uuid-1", "rack-a")}));
    const std::string valid = source.snapshot();

    Group0StateMachine target;
    ASSERT_TRUE(target.applyCommand(InitCluster{"keep-me"}));

    // Locate the first node state in the documented snapshot encoding and make
    // it an unknown enum value. Recovery must reject the whole snapshot.
    std::string invalidState = valid;
    size_t offset = 0;
    skipString(invalidState, offset);  // cluster UUID
    offset += 8 * 4;                  // epoch, applied index, controller term/leader
    ASSERT_EQ(readU64(invalidState, offset), 1u);
    offset += 8;  // node id
    skipString(invalidState, offset);
    skipString(invalidState, offset);
    skipString(invalidState, offset);
    invalidState.at(offset) = static_cast<char>(0xff);
    EXPECT_FALSE(target.loadSnapshot(invalidState));
    EXPECT_EQ(target.state().clusterUuid, "keep-me");

    // A controller term and leader are one fence: neither half may exist alone.
    std::string invalidFence = valid;
    offset = 0;
    skipString(invalidFence, offset);
    offset += 8 * 2;  // epoch + applied index
    writeU64(invalidFence, offset, 1);  // leader remains kNoNode
    EXPECT_FALSE(target.loadSnapshot(invalidFence));
    EXPECT_EQ(target.state().clusterUuid, "keep-me");

    std::string invalidVersion = valid;
    writeU64(invalidVersion, invalidVersion.size() - 8, 0);
    EXPECT_FALSE(target.loadSnapshot(invalidVersion));
    EXPECT_EQ(target.state().clusterUuid, "keep-me");
}

TEST(Group0StateMachineTest, CorruptSnapshotApplyIsFatalAndKeepsOldState) {
    Group0StateMachine sm;
    sm.applyCommand(InitCluster{"keep-me"});

    timestar::raft::Snapshot bad;
    bad.index = 9;
    bad.data = "not a group0 snapshot";
    EXPECT_THROW(sm.applySnapshot(std::move(bad)).get(), std::runtime_error);
    EXPECT_EQ(sm.state().clusterUuid, "keep-me");
    EXPECT_EQ(sm.state().appliedIndex, 0u);
}

TEST(Group0StateMachineTest, LocalPersistentIdentityFencesCommandsAndSnapshots) {
    const NodeRecord local = node(1, "local-uuid", "rack-a");
    Group0StateMachine sm;
    sm.expectLocalIdentity("cluster-a", local);

    EXPECT_THROW(sm.apply(timestar::raft::LogEntry{1, 1, timestar::raft::EntryType::Normal,
                                                   encodeCommand(InitCluster{"cluster-b"})})
                     .get(),
                 std::runtime_error);
    EXPECT_TRUE(sm.state().clusterUuid.empty());

    EXPECT_NO_THROW(sm.apply(timestar::raft::LogEntry{1, 2, timestar::raft::EntryType::Normal,
                                                      encodeCommand(InitCluster{"cluster-a"})})
                        .get());
    EXPECT_THROW(sm.apply(timestar::raft::LogEntry{
                              1, 3, timestar::raft::EntryType::Normal,
                              encodeCommand(UpsertNode{node(1, "other-uuid", "rack-a")})})
                     .get(),
                 std::runtime_error);
    EXPECT_EQ(sm.state().nodes.count(1), 0u);

    Group0StateMachine foreign;
    foreign.applyCommand(InitCluster{"cluster-a"});
    foreign.applyCommand(UpsertNode{node(2, "local-uuid", "rack-b")});
    EXPECT_FALSE(sm.loadSnapshot(foreign.snapshot()))
        << "the local persistent UUID cannot reappear under another Raft id";
    EXPECT_EQ(sm.state().clusterUuid, "cluster-a");
    EXPECT_EQ(sm.state().nodes.count(2), 0u);
}

TEST(Group0StateMachineTest, InitialServingMapIsCompleteImmutableAndSnapshotted) {
    Group0StateMachine sm;
    ControlMap incomplete{1, {{0, {1, 2, 3}}}};
    EXPECT_FALSE(sm.applyCommand(SetInitialServingMap{incomplete}));
    EXPECT_EQ(sm.state().servingMap.epoch, 0u);

    const ControlMap initial = completeServingMap();
    EXPECT_TRUE(sm.applyCommand(SetInitialServingMap{initial}));
    EXPECT_EQ(sm.state().servingMap, initial);
    EXPECT_FALSE(sm.applyCommand(SetInitialServingMap{initial}));

    ControlMap conflict = initial;
    conflict.placement.at(0) = {3, 2, 1};
    EXPECT_FALSE(sm.applyCommand(SetInitialServingMap{conflict}));
    EXPECT_FALSE(sm.applyCommand(SetInitialServingMap{completeServingMap(2)}));
    EXPECT_EQ(sm.state().servingMap, initial);

    Group0StateMachine restored;
    ASSERT_TRUE(restored.loadSnapshot(sm.snapshot()));
    EXPECT_EQ(restored.state().servingMap, initial);
}

TEST(Group0StateMachineTest, ServingMapPublicationRetriesBeforeAppliedAdvance) {
    Group0StateMachine sm;
    unsigned attempts = 0;
    sm.setServingMapObserver([&](ControlMap) {
        EXPECT_EQ(sm.state().servingMap.epoch, 0u);
        EXPECT_EQ(sm.state().appliedIndex, 0u);
        if (attempts++ == 0)
            return seastar::make_exception_future<>(std::runtime_error("injected cache failure"));
        return seastar::make_ready_future<>();
    });
    const std::string command = encodeCommand(SetInitialServingMap{completeServingMap()});
    timestar::raft::LogEntry entry{1, 9, timestar::raft::EntryType::Normal, command};
    EXPECT_THROW(sm.apply(entry).get(), std::runtime_error);
    EXPECT_EQ(sm.state().appliedIndex, 0u);
    EXPECT_EQ(sm.state().servingMap.epoch, 0u);
    EXPECT_NO_THROW(sm.apply(std::move(entry)).get());
    EXPECT_EQ(attempts, 2u);
    EXPECT_EQ(sm.state().appliedIndex, 9u);
}

TEST(Group0StateMachineTest, FormatPublicationRetriesBeforeAppliedAdvance) {
    Group0StateMachine sm;
    ASSERT_TRUE(sm.applyCommand(SetMetaVoters{{1}}));
    ASSERT_TRUE(sm.applyCommand(SetInitialServingMap{completeServingMap()}));
    unsigned attempts = 0;
    uint32_t published = 0;
    sm.setActiveFormatObserver([&](uint32_t version) {
        ++attempts;
        published = version;
        EXPECT_EQ(sm.state().activeFormatVersion, 1u);
        EXPECT_EQ(sm.state().appliedIndex, 0u);
        if (attempts == 1)
            return seastar::make_exception_future<>(std::runtime_error("injected format publication failure"));
        return seastar::make_ready_future<>();
    });
    const timestar::raft::LogEntry entry{
        1, 11, timestar::raft::EntryType::Normal, encodeCommand(SetActiveVersion{5, {1, 2, 3}})};

    EXPECT_THROW(sm.apply(entry).get(), std::runtime_error);
    EXPECT_EQ(sm.state().activeFormatVersion, 1u);
    EXPECT_EQ(sm.state().appliedIndex, 0u);
    EXPECT_NO_THROW(sm.apply(entry).get());
    EXPECT_EQ(attempts, 2u);
    EXPECT_EQ(published, 5u);
    EXPECT_EQ(sm.state().appliedIndex, 11u);
}

TEST(Group0StateMachineTest, SnapshotRecoveryPublishesFormatBeforeAppliedAdvance) {
    Group0StateMachine source;
    ASSERT_TRUE(source.applyCommand(InitCluster{"cluster-format-snapshot"}));
    ASSERT_TRUE(source.applyCommand(SetMetaVoters{{1}}));
    ASSERT_TRUE(source.applyCommand(SetInitialServingMap{completeServingMap()}));
    ASSERT_NO_THROW(source.apply(timestar::raft::LogEntry{
                                      3, 19, timestar::raft::EntryType::Normal,
                                      encodeCommand(SetActiveVersion{5, {1, 2, 3}})})
                        .get());

    Group0StateMachine restored;
    uint32_t published = 0;
    unsigned attempts = 0;
    restored.setActiveFormatObserver([&](uint32_t version) {
        ++attempts;
        published = version;
        EXPECT_EQ(restored.state().activeFormatVersion, 1u);
        EXPECT_EQ(restored.state().appliedIndex, 0u);
        if (attempts == 1)
            return seastar::make_exception_future<>(std::runtime_error("injected snapshot publication failure"));
        return seastar::make_ready_future<>();
    });
    auto snapshot = [&] {
        timestar::raft::Snapshot value;
        value.index = 19;
        value.term = 3;
        value.config.voters = {1};
        value.data = source.snapshot();
        return value;
    };

    EXPECT_THROW(restored.applySnapshot(snapshot()).get(), std::runtime_error);
    EXPECT_EQ(restored.state().activeFormatVersion, 1u);
    EXPECT_EQ(restored.state().appliedIndex, 0u);
    EXPECT_NO_THROW(restored.applySnapshot(snapshot()).get());
    EXPECT_EQ(attempts, 2u);
    EXPECT_EQ(published, 5u);
    EXPECT_EQ(restored.state().activeFormatVersion, 5u);
    EXPECT_EQ(restored.state().activeFormatVoters, (std::vector<NodeId>{1, 2, 3}));
    EXPECT_EQ(restored.state().appliedIndex, 19u);

    // Compaction cannot erase the voter proof and leave only the activated
    // scalar. A historical/meta-only snapshot is rejected before its format is
    // published or its boundary exposed.
    std::string uncovered = source.snapshot();
    uncovered.resize(uncovered.size() - (2 + 3) * sizeof(uint64_t));
    Group0StateMachine unsafe;
    unsafe.setActiveFormatObserver([](uint32_t) {
        return seastar::make_exception_future<>(std::logic_error("unsafe format was published"));
    });
    timestar::raft::Snapshot unsafeSnapshot;
    unsafeSnapshot.index = 19;
    unsafeSnapshot.term = 3;
    unsafeSnapshot.config.voters = {1};
    unsafeSnapshot.data = std::move(uncovered);
    EXPECT_THROW(unsafe.applySnapshot(std::move(unsafeSnapshot)).get(), std::runtime_error);
    EXPECT_EQ(unsafe.state().activeFormatVersion, 1u);
}

TEST(Group0StateMachineTest, FormatSnapshotSurvivesLaterMetaVoterChange) {
    Group0StateMachine source;
    ASSERT_TRUE(source.applyCommand(SetMetaVoters{{1}}));
    ASSERT_TRUE(source.applyCommand(SetInitialServingMap{completeServingMap()}));
    ASSERT_TRUE(source.applyCommand(SetActiveVersion{5, {1, 2, 3}}));
    ASSERT_TRUE(source.applyCommand(SetMetaVoters{{1, 4}}));

    Group0StateMachine restored;
    EXPECT_TRUE(restored.loadSnapshot(source.snapshot()));
    EXPECT_EQ(restored.state().activeFormatVersion, 5u);
    EXPECT_EQ(restored.state().activeFormatVoters, (std::vector<NodeId>{1, 2, 3}));
    EXPECT_EQ(restored.state().metaVoters, (std::vector<NodeId>{1, 4}));
}

TEST(Group0StateMachineTest, FormatActivationRejectsMetaOnlyOrNonCanonicalCoverage) {
    Group0StateMachine sm;
    ASSERT_TRUE(sm.applyCommand(SetMetaVoters{{1}}));
    ASSERT_TRUE(sm.applyCommand(SetInitialServingMap{completeServingMap()}));

    EXPECT_FALSE(sm.applyCommand(SetActiveVersion{5, {1}}));
    EXPECT_FALSE(sm.applyCommand(SetActiveVersion{5}));
    EXPECT_FALSE(sm.applyCommand(SetActiveVersion{5, {3, 2, 1}}));
    EXPECT_FALSE(sm.applyCommand(SetActiveVersion{5, {1, 2, 2, 3}}));
    EXPECT_FALSE(sm.applyCommand(SetActiveVersion{5, {1, 2, 3, 4}}));
    EXPECT_EQ(sm.state().activeFormatVersion, 1u);
    EXPECT_TRUE(sm.applyCommand(SetActiveVersion{5, {1, 2, 3}}));
    EXPECT_EQ(sm.state().activeFormatVersion, 5u);
    EXPECT_EQ(sm.state().activeFormatVoters, (std::vector<NodeId>{1, 2, 3}));
}

TEST(Group0StateMachineTest, LocalServingMapExpectationRejectsConflictingCommit) {
    const ControlMap expected = completeServingMap();
    ControlMap conflicting = expected;
    conflicting.placement.at(17) = {3, 2, 1};

    Group0StateMachine sm;
    sm.expectInitialServingMap(expected);
    EXPECT_THROW(sm.apply(timestar::raft::LogEntry{
                              1, 1, timestar::raft::EntryType::Normal,
                              encodeCommand(SetInitialServingMap{std::move(conflicting)})})
                     .get(),
                 std::runtime_error);
    EXPECT_EQ(sm.state().servingMap.epoch, 0u);

    Group0StateMachine foreign;
    foreign.applyCommand(SetInitialServingMap{completeServingMap()});
    auto wrong = completeServingMap();
    wrong.placement.at(17) = {3, 2, 1};
    Group0StateMachine snapshotTarget;
    snapshotTarget.expectInitialServingMap(std::move(wrong));
    EXPECT_FALSE(snapshotTarget.loadSnapshot(foreign.snapshot()));
}
