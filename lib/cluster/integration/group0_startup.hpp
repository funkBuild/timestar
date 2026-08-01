#pragma once

#include "../control/control_map_cache.hpp"
#include "../raft/raft_types.hpp"

#include <optional>
#include <stdexcept>
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

// Until resumable VShard movement exists, the only serving map production may
// activate is the immutable epoch-1 map derived from the bound static topology.
// A durable cache is still the restart source of truth, but it must be exactly
// that committed initial map; silently adopting a different map would start the
// wrong Raft groups without performing membership changes or data catch-up.
inline control::ControlMap selectServingMapForStartup(control::ControlMap configured,
                                                       std::optional<control::ControlMap> cached) {
    if (!control::isCompleteControlMap(configured))
        throw std::invalid_argument("group0 startup requires a complete configured serving map");
    if (!cached)
        return configured;
    if (!control::isCompleteControlMap(*cached) || *cached != configured)
        throw std::runtime_error(
            "durable serving map differs from static bootstrap placement; dynamic control-map cutover is not "
            "implemented");
    return std::move(*cached);
}

}  // namespace timestar::cluster
