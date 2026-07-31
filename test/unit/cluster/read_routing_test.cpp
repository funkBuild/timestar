// Debt D-13: a coordinator that hosts a STRICT SUBSET of the VShards must still route
// every one of them somewhere.
//
// The bug this pins: `gatherLeaders` recorded `leaderOf(vs)` for all 4096 VShards, and
// `leaderOf` answers kNoNode for a group this node does not replicate -- identical to
// "hosted, no leader elected". Every non-hosted VShard therefore counted as leaderless
// and the read failed QUERY_INCOMPLETE after its retries. On a 5-node RF=3 cluster a
// coordinator hosts ~2458 of 4096, so EVERY read failed, permanently.
#include "../../../lib/cluster/data/read_routing.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;
using timestar::raft::kNoNode;

namespace {

constexpr uint16_t kVShards = 512;  // enough to exercise the shape; the real map is 4096
constexpr NodeId kSelf = 2;
constexpr size_t kNodes = 5;
constexpr size_t kRf = 3;

// RF=3 over 5 nodes, round-robin -- the deposed-primary gate's topology.
ControlMap rf3Over5() {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t vs = 0; vs < kVShards; ++vs) {
        std::vector<NodeId> reps;
        for (size_t r = 0; r < kRf; ++r)
            reps.push_back(static_cast<NodeId>((vs + r) % kNodes) + 1);
        m.placement[vs] = reps;
    }
    return m;
}

bool replicates(const ControlMap& m, uint16_t vs, NodeId n) {
    const auto& reps = m.placement.at(vs);
    return std::find(reps.begin(), reps.end(), n) != reps.end();
}

std::set<uint16_t> allVShards() {
    std::set<uint16_t> s;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        s.insert(vs);
    return s;
}

}  // namespace

// THE headline property: every VShard gets exactly one target, none is dropped, none is
// asked twice, and nothing is called leaderless merely for not being ours.
TEST(ReadRouting, StrictSubsetHostRoutesEveryVShardExactlyOnce) {
    const ControlMap map = rf3Over5();
    VShardDirectory dir(kSelf, map);

    // Our live view: the VShards we HOST, all with an elected leader. Leadership is
    // spread over the replica sets the way a balanced cluster spreads it.
    std::map<uint16_t, NodeId> hosted;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        if (replicates(map, vs, kSelf))
            hosted[vs] = map.placement.at(vs)[vs % kRf];
    ASSERT_LT(hosted.size(), static_cast<size_t>(kVShards)) << "the fixture must be a STRICT subset";
    ASSERT_GT(hosted.size(), 0u);

    ReadRouting plan = planReadRouting(allVShards(), hosted, {}, dir, kSelf);

    EXPECT_EQ(plan.leaderless, 0u) << "not hosting a VShard is not the same as it having no leader";
    EXPECT_TRUE(plan.unassigned.empty());

    std::map<uint16_t, NodeId> targetOf;
    for (const auto& [node, vshards] : plan.byNode)
        for (uint16_t vs : vshards) {
            EXPECT_EQ(targetOf.count(vs), 0u) << "VShard " << vs << " routed to two nodes (double count)";
            targetOf[vs] = node;
        }
    EXPECT_EQ(targetOf.size(), static_cast<size_t>(kVShards)) << "a VShard was silently dropped from the query";

    // Every VShard we do not host is routed to a node that REPLICATES it (so it can
    // resolve leadership at all) and is named for resolution there.
    std::map<uint16_t, NodeId> resolveTarget;
    for (const auto& [node, vshards] : plan.resolveAt)
        for (uint16_t vs : vshards)
            resolveTarget[vs] = node;
    for (uint16_t vs = 0; vs < kVShards; ++vs) {
        if (hosted.count(vs)) {
            EXPECT_EQ(resolveTarget.count(vs), 0u) << "we host " << vs << "; we resolve it ourselves";
            EXPECT_EQ(targetOf.at(vs), hosted.at(vs));
        } else {
            ASSERT_EQ(resolveTarget.count(vs), 1u) << "VShard " << vs << " must be resolved by its holder";
            EXPECT_EQ(resolveTarget.at(vs), targetOf.at(vs));
            EXPECT_TRUE(replicates(map, vs, targetOf.at(vs)))
                << "VShard " << vs << " routed to a node that holds no replica of it";
            EXPECT_EQ(targetOf.at(vs), dir.ownerOf(vs));  // the placement primary, absent a hint
        }
    }
    // resolveAt is always a subset of byNode.
    for (const auto& [node, vshards] : plan.resolveAt)
        for (uint16_t vs : vshards) {
            const auto& asked = plan.byNode.at(node);
            EXPECT_NE(std::find(asked.begin(), asked.end(), vs), asked.end());
        }
}

// A HOSTED VShard with no elected leader keeps its old meaning: fail-closed. This is the
// half of the accounting that must NOT be relaxed by the fix.
TEST(ReadRouting, HostedWithoutALeaderStillCountsLeaderless) {
    const ControlMap map = rf3Over5();
    VShardDirectory dir(kSelf, map);
    std::map<uint16_t, NodeId> hosted;
    size_t expectLeaderless = 0;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        if (replicates(map, vs, kSelf)) {
            const bool leaderless = (vs % 7) == 0;
            hosted[vs] = leaderless ? kNoNode : map.placement.at(vs)[0];
            expectLeaderless += leaderless ? 1 : 0;
        }
    ASSERT_GT(expectLeaderless, 0u);

    ReadRouting plan = planReadRouting(allVShards(), hosted, {}, dir, kSelf);
    EXPECT_EQ(plan.leaderless, expectLeaderless);
    for (const auto& [node, vshards] : plan.byNode)
        for (uint16_t vs : vshards)
            EXPECT_FALSE(hosted.count(vs) && hosted.at(vs) == kNoNode) << "a leaderless VShard was routed anyway";
}

// A hint learned from an earlier redirect wins over the placement primary -- that is
// what makes the steady state one round trip instead of two.
TEST(ReadRouting, HintsOverrideThePlacementPrimaryButNeverForHostedVShards) {
    const ControlMap map = rf3Over5();
    VShardDirectory dir(kSelf, map);
    std::map<uint16_t, NodeId> hosted;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        if (replicates(map, vs, kSelf))
            hosted[vs] = map.placement.at(vs)[0];

    // Pick one hosted and one non-hosted VShard.
    uint16_t hostedVs = 0, remoteVs = 0;
    for (uint16_t vs = 0; vs < kVShards; ++vs) {
        if (hosted.count(vs) && !hostedVs)
            hostedVs = vs;
        if (!hosted.count(vs) && !remoteVs)
            remoteVs = vs;
    }
    ASSERT_NE(remoteVs, 0);

    const NodeId hinted = map.placement.at(remoteVs).back();  // a replica that is NOT the primary
    ASSERT_NE(hinted, dir.ownerOf(remoteVs));
    std::map<uint16_t, NodeId> hints{{remoteVs, hinted}, {hostedVs, 999}};

    ReadRouting plan = planReadRouting(allVShards(), hosted, hints, dir, kSelf);
    auto targetOf = [&](uint16_t vs) {
        for (const auto& [node, list] : plan.byNode)
            if (std::find(list.begin(), list.end(), vs) != list.end())
                return node;
        return kNoNode;
    };
    EXPECT_EQ(targetOf(remoteVs), hinted);
    // A hint for a VShard we HOST is ignored: our live Raft view is authoritative there,
    // which is what stops a cached hint from surviving a leadership transfer.
    EXPECT_EQ(targetOf(hostedVs), hosted.at(hostedVs));
}

// A hint naming US for a VShard we do not host is stale by construction (we cannot lead
// what we do not replicate); fall back to the directory rather than reading it locally
// without any leadership check.
TEST(ReadRouting, SelfNamingHintFallsBackToTheDirectory) {
    const ControlMap map = rf3Over5();
    VShardDirectory dir(kSelf, map);
    std::map<uint16_t, NodeId> hosted;  // host nothing
    uint16_t vs = 0;
    while (replicates(map, vs, kSelf))
        ++vs;
    ReadRouting plan = planReadRouting({vs}, hosted, {{vs, kSelf}}, dir, kSelf);
    ASSERT_EQ(plan.byNode.size(), 1u);
    EXPECT_EQ(plan.byNode.begin()->first, dir.ownerOf(vs));
    EXPECT_EQ(plan.leaderless, 0u);
}

// The directory says we hold it, our registry has no group for it (a shard starting or
// stopping). Reading it locally would bypass every leadership check, so it is
// unresolvable -- retry, then fail closed.
TEST(ReadRouting, DirectoryPointsAtUsButWeHaveNoGroupIsLeaderless) {
    ControlMap map;
    map.epoch = 1;
    map.placement[5] = {kSelf, 3, 4};
    VShardDirectory dir(kSelf, map);
    ReadRouting plan = planReadRouting({5}, /*hosted=*/{}, {}, dir, kSelf);
    EXPECT_EQ(plan.leaderless, 1u);
    EXPECT_TRUE(plan.byNode.empty());
}

TEST(ReadRouting, UnassignedVShardIsReportedNotRouted) {
    ControlMap map;
    map.epoch = 1;
    map.placement[5] = {};  // present but empty
    VShardDirectory dir(kSelf, map);
    ReadRouting plan = planReadRouting({5, 6}, /*hosted=*/{}, {}, dir, kSelf);
    EXPECT_EQ(plan.unassigned, (std::vector<uint16_t>{5, 6}));
    EXPECT_TRUE(plan.byNode.empty());
}

// RF == N: the coordinator hosts everything, so nothing is ever handed to the directory
// and no request carries a resolve list. This is the byte-identical-behaviour claim.
TEST(ReadRouting, RfEqualsNRoutesEntirelyFromTheLocalView) {
    ControlMap map;
    map.epoch = 1;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        map.placement[vs] = {1, 2, 3};
    VShardDirectory dir(kSelf, map);
    std::map<uint16_t, NodeId> hosted;
    for (uint16_t vs = 0; vs < kVShards; ++vs)
        hosted[vs] = static_cast<NodeId>(vs % 3) + 1;

    ReadRouting plan = planReadRouting(allVShards(), hosted, {}, dir, kSelf);
    EXPECT_TRUE(plan.resolveAt.empty()) << "an RF == N coordinator must never ask a peer to resolve";
    EXPECT_EQ(plan.leaderless, 0u);
    size_t routed = 0;
    for (const auto& [node, list] : plan.byNode) {
        routed += list.size();
        for (uint16_t vs : list)
            EXPECT_EQ(hosted.at(vs), node);
    }
    EXPECT_EQ(routed, static_cast<size_t>(kVShards));
}

// --- one round's bookkeeping ----------------------------------------------------------
//
// These pin the parts of ClusterDataPlane::queryReplicated's loop that nothing but the live
// 5-node gate covered: which VShards a target's answer retires, which hints survive it, and
// -- the F1 defect -- what happens to a hint whose target has died.

TEST(ReadRoundBookkeeping, AnAnswerRetiresOnlyTheVShardsItDidNotRedirect) {
    std::set<uint16_t> outstanding{1, 2, 3, 4};
    std::map<uint16_t, NodeId> hints;
    std::vector<VShardRedirect> redirects{
        {2, 5, true},  // node 5 leads it: keep it outstanding, remember where to go
        {3, 0, true},  // hosted, leaderless: keep it outstanding, remember nothing
    };
    const bool learned = applyReadRedirects(/*target=*/4, {1, 2, 3, 4}, /*permittedRedirects=*/{1, 2, 3, 4}, redirects,
                                            outstanding, hints);
    EXPECT_TRUE(learned);
    EXPECT_EQ(outstanding, (std::set<uint16_t>{2, 3}));
    EXPECT_EQ(hints.at(2), 5u);
    EXPECT_EQ(hints.count(3), 0u);
}

TEST(ReadRoundBookkeeping, ARedirectForAVShardWeDidNotAskAboutIsIgnored) {
    std::set<uint16_t> outstanding{1, 2};
    std::map<uint16_t, NodeId> hints{{99, 3}};
    // A hostile/buggy peer names a VShard outside its view, and names ITSELF for one it has.
    std::vector<VShardRedirect> redirects{{99, 7, true}, {1, 4, true}};
    applyReadRedirects(/*target=*/4, {1, 2}, /*permittedRedirects=*/{1, 2}, redirects, outstanding, hints);
    EXPECT_EQ(hints.at(99), 3u) << "a peer steered a slice it was never asked about";
    EXPECT_EQ(hints.count(1), 0u) << "a target naming ITSELF is not a hint";
    EXPECT_EQ(outstanding, (std::set<uint16_t>{1})) << "VShard 1 was redirected, so it stays outstanding";
}

// THE F1 REGRESSION TEST. A hint is otherwise only ever dropped on a REPLY, and a dead node
// cannot reply -- so a cached redirect naming a node that then died pinned every subsequent
// read to a corpse: planReadRouting preferred the hint, the RPC threw, and the whole query
// failed QUERY_INCOMPLETE naming that node, permanently (the placement map is immutable and
// the cache has no TTL). Removing the erase inside applyReadTargetUnreachable fails this.
TEST(ReadRoundBookkeeping, AnUnreachableTargetsHintsAreForgottenSoTheNextRoundReResolves) {
    ControlMap map;
    map.epoch = 1;
    map.placement[7] = {3, 4, 5};  // primary 3; we (node 2) hold no replica
    VShardDirectory dir(kSelf, map);

    // A redirect earlier taught us that node 5 leads VShard 7, so that is where we go.
    std::map<uint16_t, NodeId> hints{{7, 5}};
    ReadRouting first = planReadRouting({7}, /*hostedLeaders=*/{}, hints, dir, kSelf);
    ASSERT_EQ(first.byNode.size(), 1u);
    ASSERT_EQ(first.byNode.begin()->first, 5u) << "the hint must be preferred while it is plausible";

    // Node 5 is now unreachable.
    applyReadTargetUnreachable(/*target=*/5, {7}, hints);
    EXPECT_EQ(hints.count(7), 0u) << "the hint pointing at the dead node survived";

    // ...so the NEXT round falls back to the placement directory instead of asking the
    // corpse again. Without the erase this re-routes to node 5 forever.
    ReadRouting second = planReadRouting({7}, /*hostedLeaders=*/{}, hints, dir, kSelf);
    ASSERT_EQ(second.byNode.size(), 1u);
    EXPECT_EQ(second.byNode.begin()->first, dir.ownerOf(7))
        << "the read is still pinned to the unreachable node -- this is a permanent outage, not a retry";
    EXPECT_NE(second.byNode.begin()->first, 5u);
    EXPECT_EQ(second.leaderless, 0u);
    EXPECT_TRUE(second.unassigned.empty());
}

// Only hints that pointed AT the unreachable node are forgotten: an unrelated VShard's hint
// is still good, and throwing it away would cost a redirect round for no reason.
TEST(ReadRoundBookkeeping, UnreachabilityDoesNotForgetOtherNodesHints) {
    std::map<uint16_t, NodeId> hints{{7, 5}, {8, 4}};
    applyReadTargetUnreachable(/*target=*/5, {7, 8}, hints);
    EXPECT_EQ(hints.count(7), 0u);
    EXPECT_EQ(hints.at(8), 4u);
}

// THE D-25 REVIEW FINDING. A target may only redirect the VShards it was asked to RESOLVE,
// not everything in its read filter. The two sets are the same only at RF == N; below that
// the coordinator asks a holder for VShards it has ALREADY resolved locally (it hosts them)
// alongside the ones it wants resolved, and a redirect for the first kind is a bogus claim
// about a group whose leadership this node reads directly.
//
// The consequence of conflating them is not merely a stale hint. `planReadRouting` re-reads
// its own live leadership for a HOSTED VShard and ignores hints for it, so the coordinator
// routes it straight back to the same target, which redirects it again: the whole retry
// budget in a loop, and then a spurious "leader unreachable" once the resolve list empties
// and the VShard can never leave `outstanding`.
TEST(ReadRoundBookkeeping, ARedirectForAVShardTheTargetWasNotAskedToResolveIsIgnored) {
    std::set<uint16_t> outstanding{1, 2, 3};
    std::map<uint16_t, NodeId> hints;
    // We asked node 4 for {1,2,3} but only asked it to RESOLVE {3}: 1 and 2 are ours and we
    // already know who leads them. It redirects all three anyway.
    std::vector<VShardRedirect> redirects{{1, 9, true}, {2, 9, true}, {3, 5, true}};
    const bool learned =
        applyReadRedirects(/*target=*/4, {1, 2, 3}, /*permittedRedirects=*/{3}, redirects, outstanding, hints);
    EXPECT_TRUE(learned);
    EXPECT_EQ(hints.count(1), 0u) << "a target redirected a VShard the coordinator resolves itself";
    EXPECT_EQ(hints.count(2), 0u);
    EXPECT_EQ(hints.at(3), 5u) << "the permitted redirect must still be honoured";
    // 1 and 2 were ANSWERED (asked without being asked to resolve), so they retire; only the
    // permitted redirect stays outstanding. Retiring them is what stops the loop.
    EXPECT_EQ(outstanding, (std::set<uint16_t>{3}));

    // NEGATIVE CONTROL: the identical redirects with 1 and 2 INSIDE the permitted set are
    // legitimate and are honoured, so this is about standing, not about the redirects.
    std::set<uint16_t> outstanding2{1, 2, 3};
    std::map<uint16_t, NodeId> hints2;
    applyReadRedirects(/*target=*/4, {1, 2, 3}, /*permittedRedirects=*/{1, 2, 3}, redirects, outstanding2, hints2);
    EXPECT_EQ(hints2.at(1), 9u);
    EXPECT_EQ(hints2.at(2), 9u);
    EXPECT_EQ(outstanding2, (std::set<uint16_t>{1, 2, 3}));
}
