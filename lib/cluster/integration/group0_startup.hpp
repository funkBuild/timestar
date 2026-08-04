#pragma once

#include "../control/control_map_cache.hpp"
#include "../raft/raft_types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::cluster {

enum class Group0StartMode {
    Disabled,
    AwaitExplicitBootstrap,
    Bootstrap,
    Recover,
    Observe,
};

struct Group0StartupDecision {
    Group0StartMode mode = Group0StartMode::Disabled;
    std::vector<raft::NodeId> initialVoters;

    bool host() const {
        return mode == Group0StartMode::Bootstrap || mode == Group0StartMode::Recover ||
               mode == Group0StartMode::Observe;
    }
    bool bootstrap() const { return mode == Group0StartMode::Bootstrap; }
};

// Pure startup policy used by the production composition. In particular, the
// configured seed on a fresh directory remains inert until the operator asks
// for bootstrap; simply enabling/restarting a server can never mint a cluster.
inline Group0StartupDecision decideGroup0Startup(bool enabled, raft::NodeId self, raft::NodeId seed,
                                                 bool bootstrapRequested, bool journalExists) {
    if (!enabled) {
        if (bootstrapRequested)
            throw std::invalid_argument("--cluster-init requires cluster.control_enabled=true");
        return {};
    }
    if (self == raft::kNoNode || seed == raft::kNoNode)
        throw std::invalid_argument("group0 startup requires non-zero self and seed node ids");
    if (bootstrapRequested && self != seed)
        throw std::invalid_argument("--cluster-init may run only on cluster.control_seed_node_id");
    if (bootstrapRequested)
        return {Group0StartMode::Bootstrap, {seed}};
    if (journalExists)
        return {Group0StartMode::Recover, {seed}};
    if (self == seed)
        return {Group0StartMode::AwaitExplicitBootstrap, {}};
    return {Group0StartMode::Observe, {seed}};
}

// The static map is only the epoch-1 bootstrap seed. After Group 0 completes a
// movement, the durable cache is the restart routing high-water mark and must
// win over that seed. Equal epochs remain immutable: two placements claiming
// the same epoch are irreconcilable and fail startup closed.
inline control::ControlMap selectServingMapForStartup(control::ControlMap configured,
                                                      std::optional<control::ControlMap> cached) {
    if (!control::isCompleteControlMap(configured))
        throw std::invalid_argument("group0 startup requires a complete configured serving map");
    if (!cached)
        return configured;
    if (!control::isCompleteControlMap(*cached))
        throw std::runtime_error("durable serving map is incomplete or invalid");
    if (cached->epoch < configured.epoch)
        throw std::runtime_error("durable serving-map epoch regresses the configured bootstrap epoch");
    if (cached->epoch == configured.epoch && *cached != configured)
        throw std::runtime_error("durable and configured serving maps conflict at the same epoch");
    return std::move(*cached);
}

}  // namespace timestar::cluster
