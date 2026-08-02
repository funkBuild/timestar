#pragma once

#include "move_job.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace timestar::movement {

// A node's capacity/load telemetry for placement decisions.
struct NodeLoad {
    NodeId node = 0;
    std::string failureDomain;
    double weight = 1.0;         // relative capacity (higher = can hold more)
    double diskFreePct = 100.0;  // free-space headroom
    uint32_t replicaCount = 0;   // replicas currently hosted
};

struct BalancerConfig {
    double minFreePct = 20.0;  // HARD: never place a replica on a node below this
    double hysteresis = 0.15;  // don't move unless the load gap exceeds this fraction
};

// Computes ONE bounded move toward balanced replica placement. HARD constraints
// (failure-domain anti-affinity: no two replicas of a VShard in the same domain;
// disk watermark: never target a node below minFreePct) take priority over load
// optimisation, and a move is only proposed when it strictly reduces the
// load gap by more than the hysteresis threshold (so minor telemetry noise does
// not churn placement). Returns nullopt when no safe, beneficial move exists.
class PlacementBalancer {
public:
    static std::optional<MovePlan> planOneMove(uint64_t mapEpoch,
                                               const std::map<uint16_t, std::vector<NodeId>>& placement,
                                               const std::map<NodeId, NodeLoad>& loads, const BalancerConfig& cfg) {
        if (mapEpoch == 0)
            return std::nullopt;
        auto normLoad = [&](NodeId n) -> double {
            auto it = loads.find(n);
            if (it == loads.end() || it->second.weight <= 0)
                return 0.0;
            return static_cast<double>(it->second.replicaCount) / it->second.weight;
        };
        auto domainOf = [&](NodeId n) -> std::string {
            auto it = loads.find(n);
            return it == loads.end() ? std::string{} : it->second.failureDomain;
        };
        auto eligibleTarget = [&](NodeId n) -> bool {
            auto it = loads.find(n);
            return it != loads.end() && it->second.diskFreePct >= cfg.minFreePct;
        };

        // Most-loaded (source) and least-loaded eligible (destination) nodes.
        NodeId src = 0, dst = 0;
        double srcLoad = -1, dstLoad = 1e18;
        for (const auto& [n, l] : loads) {
            double v = normLoad(n);
            if (v > srcLoad) {
                srcLoad = v;
                src = n;
            }
            if (eligibleTarget(n) && v < dstLoad) {
                dstLoad = v;
                dst = n;
            }
        }
        if (src == 0 || dst == 0 || src == dst)
            return std::nullopt;

        // Hysteresis: only move if the normalized load gap is meaningful. Compare
        // to the source load so the threshold scales with cluster size.
        if (srcLoad <= 0 || (srcLoad - dstLoad) / srcLoad < cfg.hysteresis)
            return std::nullopt;

        // Find a VShard replica on `src` that can move to `dst` without putting two
        // replicas of that VShard in the same failure domain, and where `dst` does
        // not already hold that VShard.
        for (const auto& [vshard, reps] : placement) {
            bool onSrc = std::find(reps.begin(), reps.end(), src) != reps.end();
            bool onDst = std::find(reps.begin(), reps.end(), dst) != reps.end();
            if (!onSrc || onDst)
                continue;
            const std::string dstDomain = domainOf(dst);
            bool domainClash = false;
            for (NodeId r : reps) {
                if (r == src)
                    continue;
                const std::string rd = domainOf(r);
                // Anti-affinity is a HARD constraint. Only allow the move when the
                // destination is PROVABLY in a distinct failure domain from every
                // co-replica: both labels present and different. An unknown (empty)
                // label on EITHER side is not proof of distinctness -- two unlabeled
                // nodes might share a rack -- so it is treated as a clash and the
                // move is refused. (Label failure domains to enable anti-affinity-
                // constrained balancing.)
                if (dstDomain.empty() || rd.empty() || rd == dstDomain) {
                    domainClash = true;
                    break;
                }
            }
            if (domainClash)
                continue;
            return MovePlan{vshard, /*dest=*/dst, /*victim=*/src, mapEpoch, /*sourceVoters=*/reps};
        }
        return std::nullopt;
    }
};

}  // namespace timestar::movement
