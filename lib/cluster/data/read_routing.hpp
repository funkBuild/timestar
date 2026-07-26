#pragma once

#include "../raft/raft_types.hpp"  // NodeId, kNoNode
#include "node_query.hpp"          // VShardRedirect
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

// --- one round's bookkeeping, per target ---------------------------------------------
//
// Extracted from ClusterDataPlane::queryReplicated for the same reason planReadRouting was:
// the loop's contracts (a VShard leaves `outstanding` exactly once; a hint is only ever
// learned for a VShard that target was asked about; a hint is FORGOTTEN the moment it
// cannot be trusted) are the whole of D-13's correctness, and they were resting entirely on
// a live 5-node gate.

// Apply a target's answer. Every VShard it was `asked` about is answered -- and therefore
// leaves `outstanding` -- EXCEPT the ones it redirected, which stay outstanding to be
// re-asked at the node it named. Returns true if a usable new hint was learned (which the
// caller spends a redirect round on rather than a sleep).
//
// A redirect naming a VShard this target was NOT asked about is ignored: a buggy or hostile
// peer must not steer another target's slice, nor hold a VShard outstanding forever.
inline bool applyReadRedirects(NodeId target, const std::vector<uint16_t>& asked,
                               const std::vector<VShardRedirect>& redirects, std::set<uint16_t>& outstanding,
                               std::map<uint16_t, NodeId>& hints) {
    const std::set<uint16_t> askedHere(asked.begin(), asked.end());
    std::set<uint16_t> redirected;
    bool learned = false;
    for (const auto& rd : redirects) {
        if (!askedHere.count(rd.vshard))
            continue;
        redirected.insert(rd.vshard);
        if (rd.hosted && rd.leader != raft::kNoNode && rd.leader != target) {
            hints[rd.vshard] = rd.leader;
            learned = true;
        } else {
            // Hosted with no elected leader, or the holder does not host it after all
            // (placement skew). Drop any stale hint so the next round re-resolves from the
            // directory, and let the retry budget decide between an election in progress
            // and a genuine outage.
            hints.erase(rd.vshard);
        }
    }
    for (uint16_t vs : asked)
        if (!redirected.count(vs))
            outstanding.erase(vs);
    return learned;
}

// Apply a target's UNREACHABILITY, and FORGET every hint that pointed at it.
//
// THE HINT CACHE HAS TO BE INVALIDATED HERE OR IT IS A PERMANENT OUTAGE. A hint is only
// ever dropped on a REPLY (see applyReadRedirects), and a dead node cannot reply: once a
// redirect had named node X as the leader of VShard v and X then died, planReadRouting
// preferred that hint on every subsequent attempt of every subsequent query, the RPC threw,
// and the whole read failed QUERY_INCOMPLETE naming X -- forever, since the placement map
// is immutable and the cache has no TTL. That is exactly the one-node-down read
// unavailability this project has chased before, reintroduced through the cache.
//
// Forgetting is safe and cheap: the next round falls back to the placement directory, whose
// primary is a different replica, and if the hint was right after all the holder simply
// redirects us again (one round trip, which is what the cache costs when it is wrong).
inline void applyReadTargetUnreachable(NodeId target, const std::vector<uint16_t>& asked,
                                       std::map<uint16_t, NodeId>& hints) {
    for (uint16_t vs : asked) {
        auto it = hints.find(vs);
        if (it != hints.end() && it->second == target)
            hints.erase(it);
    }
}

}  // namespace timestar::data
