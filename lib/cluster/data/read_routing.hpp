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
                                   const std::map<uint16_t, NodeId>& hints, const VShardDirectory& dir, NodeId self,
                                   const std::map<uint16_t, std::set<NodeId>>& excludedTargets = {}) {
    ReadRouting out;
    for (uint16_t vs : outstanding) {
        const auto excludedIt = excludedTargets.find(vs);
        const auto isExcluded = [&excludedIt, &excludedTargets](NodeId node) {
            return excludedIt != excludedTargets.end() && excludedIt->second.count(node) != 0;
        };
        const auto placementIt = dir.map().placement.find(vs);
        const auto alternateReplica = [&]() {
            if (placementIt == dir.map().placement.end())
                return raft::kNoNode;
            for (NodeId replica : placementIt->second)
                if (replica != self && !isExcluded(replica))
                    return replica;
            return raft::kNoNode;
        };

        auto it = hostedLeaders.find(vs);
        if (it != hostedLeaders.end()) {
            // Hosted here: our own live view is authoritative and is re-read on every
            // attempt, so a cached hint can never survive a leadership transfer.
            if (it->second == raft::kNoNode)
                ++out.leaderless;
            else if (!isExcluded(it->second))
                out.byNode[it->second].push_back(vs);
            else if (NodeId target = alternateReplica(); target != raft::kNoNode) {
                // Our live view still names a leader whose transport attempt failed.
                // Ask another holder to resolve leadership instead of pinning the read
                // to the dead node until this local group's election view catches up.
                out.byNode[target].push_back(vs);
                out.resolveAt[target].push_back(vs);
            } else {
                ++out.leaderless;
            }
            continue;
        }
        NodeId target = raft::kNoNode;
        if (auto h = hints.find(vs); h != hints.end() && !isExcluded(h->second))
            target = h->second;
        if (target == raft::kNoNode || target == self)
            target = dir.ownerOf(vs);  // a hint naming US is stale by construction
        if (target != raft::kNoNode && isExcluded(target))
            target = alternateReplica();
        if (target == raft::kNoNode) {
            if (placementIt == dir.map().placement.end() || placementIt->second.empty())
                out.unassigned.push_back(vs);
            else
                ++out.leaderless;  // assigned, but every safe target was exhausted
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
// TWO SETS, AND THEY ARE NOT THE SAME SET (found in the D-25 review). `asked` is the
// target's whole read filter -- what leaves `outstanding` on a plain answer -- while
// `permittedRedirects` is the SUBSET the target was asked to RESOLVE (its
// `NodeQueryRequest::resolveVShards`), which is the only thing it has been given standing to
// redirect. Using `asked` for both let a drifted or hostile peer redirect a VShard the
// COORDINATOR ITSELF HOSTS: `planReadRouting` re-reads its own live leadership for a hosted
// VShard and ignores hints for it, so the coordinator routed it straight back to the same
// target, which redirected it again -- the full retry budget burned in a loop, and then a
// spurious "leader unreachable" (with its `wakeFollowersOf` amplification) once the resolve
// list emptied and the VShard could never leave `outstanding`. A redirect outside the
// permitted set is ignored, which means the VShard is treated as ANSWERED and leaves
// `outstanding` -- the target was asked for it without being asked to resolve it, so its
// answer is the answer.
inline bool applyReadRedirects(NodeId target, const std::vector<uint16_t>& asked,
                               const std::vector<uint16_t>& permittedRedirects,
                               const std::vector<VShardRedirect>& redirects, std::set<uint16_t>& outstanding,
                               std::map<uint16_t, NodeId>& hints) {
    const std::set<uint16_t> permitted(permittedRedirects.begin(), permittedRedirects.end());
    std::set<uint16_t> redirected;
    bool learned = false;
    for (const auto& rd : redirects) {
        if (!permitted.count(rd.vshard))
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
