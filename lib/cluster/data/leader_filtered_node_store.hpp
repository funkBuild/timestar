#pragma once

#include "node_query.hpp"
#include "node_store.hpp"

#include <algorithm>
#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace timestar::data {

// A NodeStore decorator that answers a peer's read ONLY for the VShards this node
// actually LEADS, redirecting the rest (debt D-13).
//
// WHY THIS EXISTS. A coordinator can resolve the current leader of a VShard only if it
// HOSTS that VShard: leadership lives in the local Raft registry, and a node holds no
// group for a VShard it does not replicate. At RF == N that is every VShard and the
// coordinator resolves the whole map itself. At RF < N it is RF/N of the map, and the
// pre-D-13 coordinator read the missing entries as kNoNode -- "no elected leader" --
// and failed every read closed. The fix routes those VShards by the placement
// directory and asks the HOLDER to resolve them, which is what this decorator does on
// the holder's side.
//
// It is deliberately a no-op unless the coordinator asks: only VShards named in
// `req.resolveVShards` are leadership-checked. Everything else is passed straight
// through to the inner store exactly as before, so the RF == N path (and every
// pre-D-13 peer) is untouched.
//
// CONTRACTS.
//  - A redirected VShard is REMOVED from the inner store's read filter, so this node's
//    partials never contain data for a VShard it also redirected -- the coordinator
//    re-asks exactly one other node and cannot double-count.
//  - A VShard is either answered or redirected. Never silently dropped: an empty
//    `vshards` filter means "no restriction" downstream, so a request whose entire
//    filter was redirected must NOT reach the inner store at all.
//  - `hosted=false` (this node holds no replica) and `leader=kNoNode` (hosted, no
//    election) are both reported, and both fail the coordinator's read closed once its
//    retry budget is spent.
//  - A resolve list with NO RESOLVER is a construction error and THROWS. It used to pass
//    straight through, which is the exact hazard the whole exchange exists to prevent --
//    this node answering from a replica it may not lead while the coordinator counts it as
//    a leader read -- and it is INVISIBLE to the coordinator's wire-version gate (debt
//    D-25), because a mis-wired node advertises the same version as a correct one. There is
//    no safe silent behaviour here: answering is a stale read and staying quiet is a
//    dropped VShard, so it fails loudly at the one place that can tell the difference.
class LeaderFilteredNodeStore : public NodeStore {
public:
    // Resolve leadership for a set of VShards on THIS node. One entry per input VShard,
    // in any order. Asynchronous because the groups live on the shards that own them.
    using ResolveFn = std::function<seastar::future<std::vector<VShardRedirect>>(std::vector<uint16_t>)>;

    LeaderFilteredNodeStore(NodeStore& inner, NodeId self, ResolveFn resolve)
        : inner_(inner), self_(self), resolve_(std::move(resolve)) {}

    seastar::future<> applyWrites(WriteBatch batch) override { return inner_.applyWrites(std::move(batch)); }
    seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end) override {
        return inner_.applyDelete(std::move(seriesKey), start, end);
    }
    seastar::future<MetadataResult> queryMetadata(MetadataRequest req) override {
        return inner_.queryMetadata(std::move(req));
    }

    seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest req) override {
        if (req.resolveVShards.empty())
            co_return co_await inner_.queryLocal(std::move(req));
        if (!resolve_)
            throw std::logic_error("LeaderFilteredNodeStore: asked to resolve " +
                                   std::to_string(req.resolveVShards.size()) +
                                   " VShard(s) with no resolver wired -- answering them from this node's replicas "
                                   "would be the follower read the resolve exchange exists to prevent");

        std::vector<VShardRedirect> resolved = co_await resolve_(req.resolveVShards);
        std::vector<VShardRedirect> redirects;
        std::set<uint16_t> drop;
        for (const auto& r : resolved) {
            if (r.hosted && r.leader == self_)
                continue;  // we lead it: answer it below
            redirects.push_back(r);
            drop.insert(r.vshard);
        }
        // A VShard the resolver did not answer for at all is treated as unresolvable
        // rather than quietly answered from this node's (possibly stale) replica.
        std::set<uint16_t> answered;
        for (const auto& r : resolved)
            answered.insert(r.vshard);
        for (uint16_t vs : req.resolveVShards) {
            if (answered.count(vs))
                continue;
            redirects.push_back(VShardRedirect{vs, kNoNode, false});
            drop.insert(vs);
        }

        if (drop.empty())
            co_return co_await inner_.queryLocal(std::move(req));

        std::vector<uint16_t> keep;
        keep.reserve(req.vshards.size());
        for (uint16_t vs : req.vshards)
            if (!drop.count(vs))
                keep.push_back(vs);
        if (keep.empty()) {
            // EVERY VShard we were asked about went back as a redirect. Calling the
            // inner store with an empty filter would mean "no restriction" and return
            // this node's whole local answer -- data for VShards the coordinator is
            // simultaneously asking someone else about, i.e. a double count.
            NodeQueryPartial out;
            out.redirects = std::move(redirects);
            co_return out;
        }
        req.vshards = std::move(keep);
        req.resolveVShards.clear();
        NodeQueryPartial out = co_await inner_.queryLocal(std::move(req));
        out.redirects = std::move(redirects);
        co_return out;
    }

private:
    NodeStore& inner_;
    NodeId self_ = kNoNode;
    ResolveFn resolve_;
};

}  // namespace timestar::data
