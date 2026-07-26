#pragma once

#include "vshard_directory.hpp"

#include <string>

namespace timestar::data {

// A live subscription (SSE /subscribe) only ever sees data that lives on the node
// serving it. In a multi-node cluster that is a NODE-LOCAL view -- it silently
// misses every series owned by another node. The streaming contract forbids that
// silent degradation, so the moment the deployment is clustered a subscription
// must be either rejected or EXPLICITLY marked node-local; it must never be
// served as if it were cluster-wide.

// True if this deployment is multi-node: some VShard is owned by a node other
// than self (a single-node dev deployment owns everything locally).
inline bool isClustered(const VShardDirectory& dir) {
    for (NodeId owner : dir.ownerNodes())
        if (owner != dir.self())
            return true;
    return false;
}

struct SubscribeDecision {
    bool rejected = false;       // return an error instead of streaming
    bool nodeLocalOnly = false;  // stream, but the response MUST advertise node-local scope
    std::string reason;
};

// Decide how to handle a /subscribe in the current deployment. `rejectInCluster`
// is the operator policy: reject outright, or allow-but-mark. The one outcome
// this function can NEVER return is a clustered, non-rejected, non-marked
// subscription -- the forbidden silent degradation.
inline SubscribeDecision evaluateSubscribe(bool clustered, bool rejectInCluster) {
    if (!clustered)
        return {};  // single-node: an ordinary full-scope subscription
    if (rejectInCluster)
        return {.rejected = true,
                .nodeLocalOnly = false,
                .reason =
                    "subscriptions are node-local only in cluster mode; use /query for "
                    "cluster-wide reads"};
    return {.rejected = false,
            .nodeLocalOnly = true,
            .reason = "node-local subscription: this stream covers only series owned by this node"};
}

}  // namespace timestar::data
