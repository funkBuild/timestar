#pragma once

#include "../control/control_map_cache.hpp"
#include "../control/control_command.hpp"
#include "../raft/raft_types.hpp"

#include <map>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

namespace timestar::cluster {

class NodeCapabilityValidationError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

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

// Validate every reply that was observed, even when some other peer timed out.
// A down node must not hide a permanent wrong-cluster/address/UUID conflict in
// another reply merely because the complete collection could not be assembled.
inline void validateObservedNodeCapabilities(
    const std::string& clusterUuid, const std::map<raft::NodeId, std::string>& expectedPeers,
    const std::map<raft::NodeId, control::NodeCapabilityAdvertisement>& capabilities) {
    if (clusterUuid.empty())
        throw NodeCapabilityValidationError("control cluster identity is empty");
    std::set<std::string> persistentUuids;
    for (const auto& [id, capability] : capabilities) {
        auto expected = expectedPeers.find(id);
        if (expected == expectedPeers.end())
            throw NodeCapabilityValidationError("control capability collection contains an unconfigured node");
        if (capability.clusterUuid != clusterUuid || capability.record.raftId != id ||
            capability.record.address != expected->second)
            throw NodeCapabilityValidationError("control capability identity does not match configured node " +
                                                std::to_string(id));
        if (!persistentUuids.insert(capability.record.uuid).second)
            throw NodeCapabilityValidationError("duplicate persistent node identity in control capability collection");
    }
}

// Validate one complete capability collection against the operator-bound static
// topology. The authenticated RPC protects the frame in transit; these checks
// bind its claims to the node id/address we dialled and reject a copied data
// directory (one persistent UUID answering for multiple Raft ids).
inline std::map<raft::NodeId, features::VersionRange> validateNodeCapabilities(
    const std::string& clusterUuid, const std::map<raft::NodeId, std::string>& expectedPeers,
    const std::map<raft::NodeId, control::NodeCapabilityAdvertisement>& capabilities) {
    validateObservedNodeCapabilities(clusterUuid, expectedPeers, capabilities);
    if (capabilities.size() != expectedPeers.size())
        throw NodeCapabilityValidationError("control capability collection is incomplete");
    std::map<raft::NodeId, features::VersionRange> versions;
    for (const auto& peer : expectedPeers) {
        auto found = capabilities.find(peer.first);
        if (found == capabilities.end())
            throw NodeCapabilityValidationError("control capability collection is missing node " +
                                                std::to_string(peer.first));
        versions.emplace(peer.first, found->second.formats);
    }
    return versions;
}

}  // namespace timestar::cluster
