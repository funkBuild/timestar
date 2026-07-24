#include "meta_voters.hpp"

#include <algorithm>
#include <set>
#include <string>

namespace timestar::control {

std::vector<NodeId> selectMetaVoters(const std::map<NodeId, NodeRecord>& nodes,
                                     const std::vector<NodeId>& current, unsigned target) {
    if (target == 0)
        return {};
    const std::set<NodeId> currentSet(current.begin(), current.end());

    // Bucket eligible (Active) nodes by failure domain. std::map iterates in a
    // canonical (sorted) order so the result is node-independent.
    std::map<std::string, std::vector<NodeId>> byDomain;
    for (const auto& [id, rec] : nodes) {
        if (rec.state == NodeState::Active)
            byDomain[rec.failureDomain].push_back(id);
    }
    if (byDomain.empty())
        return {};

    // Within each domain, prefer nodes that are CURRENTLY voters (churn
    // minimization), then by ascending id. `nodes` already gave us ascending id.
    for (auto& [dom, ids] : byDomain) {
        std::stable_partition(ids.begin(), ids.end(),
                              [&](NodeId id) { return currentSet.count(id) != 0; });
    }

    // Order domains: those already hosting a current voter first (stability),
    // then the rest -- both alphabetically for determinism.
    std::vector<std::string> domains;
    domains.reserve(byDomain.size());
    for (const auto& [dom, ids] : byDomain)
        domains.push_back(dom);
    std::stable_sort(domains.begin(), domains.end(), [&](const std::string& a, const std::string& b) {
        auto hasVoter = [&](const std::string& d) {
            for (NodeId id : byDomain.at(d))
                if (currentSet.count(id))
                    return true;
            return false;
        };
        const bool av = hasVoter(a), bv = hasVoter(b);
        if (av != bv)
            return av;  // voter-hosting domains first
        return a < b;   // else alphabetical (domains already came sorted)
    });

    // Round-robin: one node per domain per round, maximizing distinct domains
    // before doubling up in any one.
    std::vector<NodeId> result;
    for (size_t round = 0; result.size() < target; ++round) {
        bool progressed = false;
        for (const std::string& dom : domains) {
            const auto& ids = byDomain.at(dom);
            if (ids.size() > round) {
                result.push_back(ids[round]);
                progressed = true;
                if (result.size() == target)
                    break;
            }
        }
        if (!progressed)
            break;  // exhausted all nodes
    }

    std::sort(result.begin(), result.end());  // canonical set
    return result;
}

bool metaVotersDiffer(const std::vector<NodeId>& current, const std::vector<NodeId>& desired) {
    return std::set<NodeId>(current.begin(), current.end()) !=
           std::set<NodeId>(desired.begin(), desired.end());
}

}  // namespace timestar::control
