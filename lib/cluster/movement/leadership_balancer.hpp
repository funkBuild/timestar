#pragma once

#include "../raft/raft_types.hpp"  // NodeId

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace timestar::movement {

using timestar::raft::NodeId;

// A leadership transfer: hand VShard `vshard`'s leadership to `target` (a current
// replica). Leadership balancing is also v1's read balancing -- balancing leaders
// balances write load and, since reads go to leaders, read load too.
struct LeaderTransfer {
    uint16_t vshard = 0;
    NodeId target = 0;
    friend bool operator==(const LeaderTransfer&, const LeaderTransfer&) = default;
};

// Picks ONE leadership transfer that reduces write-load imbalance, moving a
// leadership from the busiest leader-node to a lighter REPLICA of one of its
// groups (moving leadership is preferred over copying data when only ingest load
// is uneven). Honors hysteresis so minor noise does not churn leadership. Returns
// nullopt when no beneficial transfer exists.
class LeadershipBalancer {
public:
    static std::optional<LeaderTransfer> planOneTransfer(
        const std::map<uint16_t, NodeId>& leaders,                // vshard -> current leader
        const std::map<uint16_t, std::vector<NodeId>>& replicas,  // vshard -> replica set
        const std::map<NodeId, double>& writeLoad,                // per-node write load
        double hysteresis) {
        if (leaders.empty())
            return std::nullopt;
        auto load = [&](NodeId n) -> double {
            auto it = writeLoad.find(n);
            return it == writeLoad.end() ? 0.0 : it->second;
        };
        // Busiest and lightest node by write load.
        NodeId busy = 0, light = 0;
        double busyLoad = -1, lightLoad = 1e18;
        for (const auto& [n, l] : writeLoad) {
            if (l > busyLoad) {
                busyLoad = l;
                busy = n;
            }
            if (l < lightLoad) {
                lightLoad = l;
                light = n;
            }
        }
        if (busy == 0 || light == 0 || busy == light)
            return std::nullopt;
        if (busyLoad <= 0 || (busyLoad - lightLoad) / busyLoad < hysteresis)
            return std::nullopt;

        // Move leadership of a group the busy node leads to `light`, if `light` is a
        // replica of it (leadership can only move to an existing replica).
        for (const auto& [vshard, leader] : leaders) {
            if (leader != busy)
                continue;
            auto it = replicas.find(vshard);
            if (it == replicas.end())
                continue;
            if (std::find(it->second.begin(), it->second.end(), light) != it->second.end())
                return LeaderTransfer{vshard, light};
        }
        return std::nullopt;
    }
};

}  // namespace timestar::movement
