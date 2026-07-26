#pragma once

#include "group0_state.hpp"

#include <map>
#include <vector>

namespace timestar::control {

// Auto-select the group-0 meta voter set from the current node records, WITHOUT
// operator placement (the plan's self-managed membership). The goal is fault
// tolerance across failure domains: pick up to `target` (normally an odd number,
// 3) Active nodes spread across as many DISTINCT failure domains as possible, so
// losing one domain never takes the meta quorum. Deterministic (a pure function
// of its inputs, canonical sorted output) so every node computes the same set,
// and churn-minimizing (existing voters and their domains are preferred) so a
// membership change only happens when the set genuinely must change.
//
// Fewer eligible nodes/domains than `target` returns the best available smaller
// set (the caller reports reduced HA). `current` is the present voter set.
std::vector<NodeId> selectMetaVoters(const std::map<NodeId, NodeRecord>& nodes, const std::vector<NodeId>& current,
                                     unsigned target);

// True iff `desired` differs from `current` as a SET (order-independent), i.e. a
// joint-consensus membership change is warranted.
bool metaVotersDiffer(const std::vector<NodeId>& current, const std::vector<NodeId>& desired);

}  // namespace timestar::control
