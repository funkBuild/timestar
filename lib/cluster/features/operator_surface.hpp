#pragma once

#include "../movement/movement_throttle.hpp"
#include "../movement/scrubber.hpp"
#include "../raft/raft_types.hpp"  // NodeId

#include <string>
#include <vector>

namespace timestar::features {

using timestar::raft::NodeId;

// The remaining operator surface (docs/clustering.md §"Observability and
// administration") beyond the Phase-4 minimum (cluster status / vshard describe /
// placement explain, in ClusterInspector). Thin, testable control+status views
// over the Phase-7 movement throttle, scrubber, and read-routing selection.

// `rebalance status / pause / resume`.
struct RebalanceStatus {
    bool running = false;  // movement may proceed right now
    bool autoPaused = false;
    bool cancelled = false;
};
class RebalanceOps {
public:
    explicit RebalanceOps(movement::MovementThrottle& t) : t_(t) {}
    RebalanceStatus status() const { return {t_.mayProceed(), t_.autoPaused(), t_.cancelled()}; }
    void pause() { t_.manualPause(); }
    void resume() { t_.manualResume(); }
    void cancel() { t_.cancel(); }

private:
    movement::MovementThrottle& t_;
};

// `repair status / scrub`.
struct RepairStatus {
    size_t quarantinedCount = 0;
    std::vector<std::string> quarantined;
};
inline RepairStatus repairStatus(const movement::Scrubber& s) {
    RepairStatus r;
    r.quarantined.assign(s.quarantined().begin(), s.quarantined().end());
    r.quarantinedCount = r.quarantined.size();
    return r;
}

// `query replica decision trace`: why a given VShard read was routed to a given
// replica -- the consistency mode, the eligible candidates (post-selection), and
// the chosen replica. A read-routing debugging surface (Phase 6 selection).
struct ReplicaDecision {
    uint16_t vshard = 0;
    std::string mode;              // "linearizable" | "session" | "bounded_staleness"
    std::vector<NodeId> eligible;  // candidates that passed eligibility, in preference order
    NodeId chosen = 0;             // the replica actually read (front of eligible), 0 if none
    std::string reason;
};

// Build a decision trace from a resolved eligibility-ordered candidate list.
inline ReplicaDecision traceReplicaDecision(uint16_t vshard, const std::string& mode,
                                            const std::vector<NodeId>& eligibleInPreferenceOrder) {
    ReplicaDecision d;
    d.vshard = vshard;
    d.mode = mode;
    d.eligible = eligibleInPreferenceOrder;
    if (eligibleInPreferenceOrder.empty()) {
        d.chosen = 0;
        d.reason = "no eligible replica (all unreachable or too stale) -- read fails closed";
    } else {
        d.chosen = eligibleInPreferenceOrder.front();
        d.reason =
            "chose the front of the eligible preference order (locality, then lag, "
            "then queue depth, then error rate)";
    }
    return d;
}

}  // namespace timestar::features
