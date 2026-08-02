// Debt D-13: the HOLDER side of an RF < N read. A coordinator cannot resolve the
// leader of a VShard it does not host, so it routes those by the placement directory
// and asks the holder to resolve them. This is that resolution: answer only what we
// LEAD, redirect the rest, and never let a redirected VShard's data into the answer as
// well (which would double-count it against the node the coordinator re-asks).
#include "../../../lib/cluster/data/leader_filtered_node_store.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>
#include <stdexcept>

using namespace timestar::data;

namespace {

// Records the filter it was asked for and answers with a marker partial, so a test can
// see exactly which VShards reached the real store.
class RecordingStore : public NodeStore {
public:
    seastar::future<> applyWrites(WriteBatch) override { return seastar::make_ready_future<>(); }
    seastar::future<bool> applyDelete(std::string, uint64_t, uint64_t) override {
        return seastar::make_ready_future<bool>(true);
    }
    seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest req) override {
        ++calls;
        lastFilter = req.vshards;
        NodeQueryPartial p;
        p.seriesFound = 1;
        return seastar::make_ready_future<NodeQueryPartial>(std::move(p));
    }
    seastar::future<PatternSeriesResult> findPatternSeries(PatternSeriesRequest req) override {
        ++patternCalls;
        lastPatternFilter = req.vshards;
        PatternSeriesResult result;
        result.seriesKeys = {"cpu,host=a usage"};
        return seastar::make_ready_future<PatternSeriesResult>(std::move(result));
    }
    int calls = 0;
    std::vector<uint16_t> lastFilter;
    int patternCalls = 0;
    std::vector<uint16_t> lastPatternFilter;
};

constexpr timestar::raft::NodeId kSelf = 2;

// Node 2 leads VShard 10, hosts 11 with no leader elected, hosts 12 led by node 3, and
// does not host 13 at all.
LeaderFilteredNodeStore::ResolveFn fixedResolver() {
    return [](std::vector<uint16_t> vshards) {
        std::vector<VShardRedirect> out;
        for (uint16_t vs : vshards) {
            switch (vs) {
                case 10:
                    out.push_back(VShardRedirect{10, kSelf, true});
                    break;
                case 11:
                    out.push_back(VShardRedirect{11, 0, true});
                    break;
                case 12:
                    out.push_back(VShardRedirect{12, 3, true});
                    break;
                case 13:
                    out.push_back(VShardRedirect{13, 0, false});
                    break;
                default:
                    break;  // deliberately unanswered -- see the test for it
            }
        }
        return seastar::make_ready_future<std::vector<VShardRedirect>>(std::move(out));
    };
}

}  // namespace

// The RF == N path: no resolve list, so the store is a pass-through. This is what keeps
// a 3-node RF=3 cluster (and every pre-D-13 peer) on exactly the old code path.
TEST(LeaderFilteredNodeStore, PassesThroughWhenNothingToResolve) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        NodeQueryRequest req;
        req.vshards = {10, 11, 12, 13};  // named, but NOT named for resolution
        auto out = store.queryLocal(req).get();
        EXPECT_EQ(inner.calls, 1);
        EXPECT_EQ(inner.lastFilter, (std::vector<uint16_t>{10, 11, 12, 13}));
        EXPECT_TRUE(out.redirects.empty());
        EXPECT_EQ(out.seriesFound, 1u);
    }).get();
}

TEST(LeaderFilteredNodeStore, AnswersLedVShardsAndRedirectsTheRest) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        NodeQueryRequest req;
        req.vshards = {10, 11, 12, 13};
        req.resolveVShards = {10, 11, 12, 13};
        auto out = store.queryLocal(req).get();

        // Only the VShard we LEAD reached the store. 11 (leaderless), 12 (led
        // elsewhere) and 13 (not ours) were excluded from the read filter, so their
        // data is not in these partials -- the coordinator asks exactly one other node.
        ASSERT_EQ(inner.calls, 1);
        EXPECT_EQ(inner.lastFilter, (std::vector<uint16_t>{10}));

        std::map<uint16_t, VShardRedirect> byVs;
        for (const auto& r : out.redirects)
            byVs[r.vshard] = r;
        ASSERT_EQ(byVs.size(), 3u);
        EXPECT_TRUE(byVs.at(11).hosted);
        EXPECT_EQ(byVs.at(11).leader, 0u);  // hosted + leaderless => the coordinator fails closed
        EXPECT_TRUE(byVs.at(12).hosted);
        EXPECT_EQ(byVs.at(12).leader, 3u);  // a real hint: re-ask node 3
        EXPECT_FALSE(byVs.at(13).hosted);
        EXPECT_EQ(byVs.count(10), 0u);  // answered, not redirected
    }).get();
}

// The contract that makes "never double-count" hold at the edge: if EVERY named VShard
// is redirected, the inner store must not be called at all. An empty filter means "no
// restriction" downstream, which would return this node's whole local answer -- data
// for VShards the coordinator is simultaneously asking someone else about.
TEST(LeaderFilteredNodeStore, AllRedirectedNeverReachesTheStore) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        NodeQueryRequest req;
        req.vshards = {11, 12, 13};
        req.resolveVShards = {11, 12, 13};
        auto out = store.queryLocal(req).get();
        EXPECT_EQ(inner.calls, 0);
        EXPECT_EQ(out.redirects.size(), 3u);
        EXPECT_TRUE(out.partials.empty());
        EXPECT_EQ(out.seriesFound, 0u);
    }).get();
}

// A VShard the resolver says nothing about (its shard was stopping, say) must come back
// as unresolved -- not be answered from this node's replica on the quiet assumption that
// silence means "mine".
TEST(LeaderFilteredNodeStore, UnansweredByResolverIsRedirectedNotServed) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        NodeQueryRequest req;
        req.vshards = {10, 99};
        req.resolveVShards = {10, 99};
        auto out = store.queryLocal(req).get();
        EXPECT_EQ(inner.lastFilter, (std::vector<uint16_t>{10}));
        ASSERT_EQ(out.redirects.size(), 1u);
        EXPECT_EQ(out.redirects[0].vshard, 99);
        EXPECT_FALSE(out.redirects[0].hosted);
    }).get();
}

// Resolution applies ONLY to the named subset: a VShard the coordinator resolved itself
// is answered without a leadership check even if we do not lead it. That is not a
// loophole -- the coordinator hosts it, so its own Raft view named us -- and it is what
// makes the mixed case (some hosted, some directory-routed) a single request.
TEST(LeaderFilteredNodeStore, OnlyTheNamedSubsetIsChecked) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        NodeQueryRequest req;
        req.vshards = {10, 12, 13};
        req.resolveVShards = {13};  // only 13 needs resolving
        auto out = store.queryLocal(req).get();
        EXPECT_EQ(inner.lastFilter, (std::vector<uint16_t>{10, 12}));
        ASSERT_EQ(out.redirects.size(), 1u);
        EXPECT_EQ(out.redirects[0].vshard, 13);
    }).get();
}

// A resolve list with NO RESOLVER is a construction error, not a pass-through (found in the
// D-25 review). Passing it through is the exact hazard the exchange exists to prevent --
// this node answers from replicas it may not lead while the coordinator counts it as a
// leader read -- and it is INVISIBLE to the coordinator's v1 handshake, because a
// mis-wired node advertises the same version as a correctly wired one.
TEST(LeaderFilteredNodeStore, AResolveListWithNoResolverFailsLoudlyRatherThanServingAFollowerRead) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, LeaderFilteredNodeStore::ResolveFn{});  // no resolver wired
        NodeQueryRequest req;
        req.vshards = {10, 11};
        req.resolveVShards = {11};
        EXPECT_THROW(store.queryLocal(req).get(), std::logic_error);
        EXPECT_EQ(inner.calls, 0) << "the un-resolvable read must never reach the store";

        // NEGATIVE CONTROL: with nothing to resolve there is nothing to be wrong about, so a
        // resolver-less store is still a legitimate pass-through -- which is what an RF == N
        // node and every pre-D-13 peer look like.
        NodeQueryRequest plain;
        plain.vshards = {10, 11};
        auto out = store.queryLocal(plain).get();
        EXPECT_EQ(inner.calls, 1);
        EXPECT_TRUE(out.redirects.empty());
    }).get();
}

TEST(LeaderFilteredNodeStore, PatternDiscoveryNeverIncludesRedirectedReplicaCatalogs) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        PatternSeriesRequest req;
        req.selector.measurement = "cpu";
        req.vshards = {10, 11, 12, 13};
        req.resolveVShards = req.vshards;
        req.maxSeries = 10;
        auto out = store.findPatternSeries(std::move(req)).get();

        ASSERT_EQ(inner.patternCalls, 1);
        EXPECT_EQ(inner.lastPatternFilter, (std::vector<uint16_t>{10}));
        EXPECT_EQ(out.seriesKeys, (std::vector<std::string>{"cpu,host=a usage"}));
        ASSERT_EQ(out.redirects.size(), 3u);
    }).get();
}

TEST(LeaderFilteredNodeStore, FullyRedirectedPatternDiscoveryDoesNotUseEmptyAsUnrestricted) {
    seastar::async([] {
        RecordingStore inner;
        LeaderFilteredNodeStore store(inner, kSelf, fixedResolver());
        PatternSeriesRequest req;
        req.selector.measurement = "cpu";
        req.vshards = {11, 12, 13};
        req.resolveVShards = req.vshards;
        req.maxSeries = 10;
        auto out = store.findPatternSeries(std::move(req)).get();
        EXPECT_EQ(inner.patternCalls, 0);
        EXPECT_TRUE(out.seriesKeys.empty());
        EXPECT_EQ(out.redirects.size(), 3u);
    }).get();
}
