#pragma once

#include "../control/control_map_cache.hpp"
#include "../control/control_command.hpp"
#include "../data/replicated_command.hpp"
#include "../raft/raft_types.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
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

struct LegacyReceiptPreflightSummary {
    size_t vshards = 0;
    uint64_t maxLegacyReceipts = 0;
    uint64_t maxTotalReceipts = 0;
};

// Validate the v9 inventory against every replica in the immutable serving map.
// Requiring equal counts across replicas makes apply lag a retryable preflight
// failure instead of allowing a stale follower to hide legacy state. A VShard
// whose entire 1,024-entry capacity is legacy can never admit the first bounded
// receipt, so activation is refused rather than enabling a permanently unusable
// command format.
inline LegacyReceiptPreflightSummary validateLegacyReceiptInventories(
    const std::string& clusterUuid, const std::map<raft::NodeId, std::string>& expectedPeers,
    const control::ControlMap& servingMap,
    const std::map<raft::NodeId, control::LegacyReceiptInventoryAdvertisement>& inventories) {
    if (!control::isCompleteControlMap(servingMap))
        throw NodeCapabilityValidationError("legacy receipt preflight has no complete serving map");
    if (inventories.size() != expectedPeers.size())
        throw NodeCapabilityValidationError("legacy receipt inventory collection is incomplete");

    std::set<std::string> persistentUuids;
    std::map<raft::NodeId, std::map<uint16_t, control::LegacyReceiptInventoryEntry>> byNode;
    for (const auto& [id, inventory] : inventories) {
        auto expected = expectedPeers.find(id);
        if (expected == expectedPeers.end())
            throw NodeCapabilityValidationError("legacy receipt inventory contains an unconfigured node");
        if (inventory.clusterUuid != clusterUuid || inventory.record.raftId != id ||
            inventory.record.address != expected->second)
            throw NodeCapabilityValidationError("legacy receipt inventory identity does not match configured node " +
                                                std::to_string(id));
        if (!persistentUuids.insert(inventory.record.uuid).second)
            throw NodeCapabilityValidationError("duplicate persistent node identity in legacy receipt inventory");
        auto& nodeEntries = byNode[id];
        for (const auto& entry : inventory.entries) {
            auto placement = servingMap.placement.find(entry.vshard);
            if (placement == servingMap.placement.end() ||
                std::find(placement->second.begin(), placement->second.end(), id) == placement->second.end())
                throw NodeCapabilityValidationError("legacy receipt inventory advertises an unowned VShard");
            if (entry.legacyReceipts > entry.totalReceipts ||
                entry.totalReceipts > data::kMaxDeleteReceiptsPerVShard)
                throw NodeCapabilityValidationError("legacy receipt inventory exceeds the per-VShard hard cap");
            if (entry.hasUnappliedEntries)
                throw NodeCapabilityValidationError(
                    "legacy receipt inventory has an unapplied data-group log tail");
            if (!nodeEntries.emplace(entry.vshard, entry).second)
                throw NodeCapabilityValidationError("legacy receipt inventory repeats a VShard");
        }
    }

    LegacyReceiptPreflightSummary summary;
    for (const auto& [vshard, replicas] : servingMap.placement) {
        std::optional<control::LegacyReceiptInventoryEntry> expectedCounts;
        for (raft::NodeId replica : replicas) {
            auto node = byNode.find(replica);
            if (node == byNode.end())
                throw NodeCapabilityValidationError("serving map names an unconfigured receipt-inventory node");
            auto entry = node->second.find(vshard);
            if (entry == node->second.end())
                throw NodeCapabilityValidationError("legacy receipt inventory is missing a serving replica");
            if (expectedCounts && (entry->second.legacyReceipts != expectedCounts->legacyReceipts ||
                                   entry->second.totalReceipts != expectedCounts->totalReceipts))
                throw NodeCapabilityValidationError("legacy receipt counts disagree across serving replicas");
            expectedCounts = entry->second;
        }
        if (!expectedCounts)
            throw NodeCapabilityValidationError("serving VShard has no legacy receipt inventory");
        if (expectedCounts->legacyReceipts >= data::kMaxDeleteReceiptsPerVShard)
            throw NodeCapabilityValidationError("legacy receipts exhaust a VShard's bounded receipt capacity");
        ++summary.vshards;
        summary.maxLegacyReceipts = std::max(summary.maxLegacyReceipts, expectedCounts->legacyReceipts);
        summary.maxTotalReceipts = std::max(summary.maxTotalReceipts, expectedCounts->totalReceipts);
    }
    return summary;
}

}  // namespace timestar::cluster
