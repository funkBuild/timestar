#pragma once

#include "../raft/raft_types.hpp"  // NodeId, kNoNode
#include "vshard_directory.hpp"

#include <cstdint>
#include <map>
#include <set>
#include <vector>

namespace timestar::data {

// Where each still-unanswered VShard of a replicated read must be asked (debt D-13).
struct ReadRouting {
    // target node -> the VShards to ask it for (its NodeQueryRequest::vshards).
    std::map<NodeId, std::vector<uint16_t>> byNode;
    // target node -> the SUBSET of the above whose leadership that node must resolve
    // itself (its NodeQueryRequest::resolveVShards). Always a subset of byNode[target].
    std::map<NodeId, std::vector<uint16_t>> resolveAt;
    // VShards this node HOSTS with no elected leader, plus ones the directory routes
    // back to us that our own registry has no group for. Both are "we hold a replica
    // and cannot name a leader" -- retry, then fail the read closed.
    size_t leaderless = 0;
    // VShards with no replica at all in the placement map. Retrying cannot fix it.
    std::vector<uint16_t> unassigned;
};

// Plan one round of a replicated read.
//
// `hostedLeaders` is this node's LIVE Raft view and contains ONLY VShards it hosts: a
// missing key means "not ours", a present kNoNode means "ours, no leader elected". That
// distinction is the whole of D-13 -- conflating them made a coordinator that hosts a
// strict subset (any RF < N cluster) call most of the map leaderless and fail every read.
//
// For a VShard we do not host we cannot know the leader: no group, no leadership. We
// route it by `hints` (learned from an earlier redirect) or else by the placement
// directory's primary, and mark it for the target to resolve -- the same "route by
// primary, follow the leader hint it answers with" shape the write path uses.
inline ReadRouting planReadRouting(const std::set<uint16_t>& outstanding,
                                   const std::map<uint16_t, NodeId>& hostedLeaders,
                                   const std::map<uint16_t, NodeId>& hints, const VShardDirectory& dir, NodeId self) {
    ReadRouting out;
    for (uint16_t vs : outstanding) {
        auto it = hostedLeaders.find(vs);
        if (it != hostedLeaders.end()) {
            // Hosted here: our own live view is authoritative and is re-read on every
            // attempt, so a cached hint can never survive a leadership transfer.
            if (it->second == raft::kNoNode)
                ++out.leaderless;
            else
                out.byNode[it->second].push_back(vs);
            continue;
        }
        NodeId target = raft::kNoNode;
        if (auto h = hints.find(vs); h != hints.end())
            target = h->second;
        if (target == raft::kNoNode || target == self)
            target = dir.ownerOf(vs);  // a hint naming US is stale by construction
        if (target == raft::kNoNode) {
            out.unassigned.push_back(vs);
            continue;
        }
        if (target == self) {
            // The directory says we hold it but our registry has no group for it (a
            // shard still starting, or already stopping). Reading it locally would
            // bypass every leadership check -- the local sink does none -- so treat it
            // as unresolvable: retry, then fail closed.
            ++out.leaderless;
            continue;
        }
        out.byNode[target].push_back(vs);
        out.resolveAt[target].push_back(vs);
    }
    return out;
}

}  // namespace timestar::data
