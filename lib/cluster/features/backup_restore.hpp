#pragma once

#include "../../core/vshard.hpp"
#include "../control/group0_state.hpp"

#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::features {

// Portable Group-0 state. Topology, node identities, Raft membership, movement
// jobs, controller ownership and join tokens are deliberately absent: restore
// creates a new cluster and must never resurrect any of those authorities.
struct PortableControlBackup {
    std::map<std::string, control::PolicyCell> policies;
    uint64_t lastRetentionSweepId = 0;
    std::map<std::string, control::RetentionCutoffRecord> retentionCutoffs;
    std::map<std::string, control::FrozenDeletePlan> frozenDeletePlans;

    friend bool operator==(const PortableControlBackup&, const PortableControlBackup&) = default;
    [[nodiscard]] bool valid() const;
};

// One immutable TSP1 file in a completed archive. The filename is derived from
// vshard (vshards/NNNN.tsp1), never supplied by the archive. encodedHash is the
// FNV-1a hash over the complete file; the TSP1 trailer independently protects
// and validates its body, catalog and logical verification hash.
struct VShardBackupUnit {
    uint16_t vshard = 0;
    uint64_t snapshotRevision = 0;
    std::string verificationHash;
    std::string catalogHash;
    uint64_t encodedSize = 0;
    uint64_t encodedHash = 0;

    friend bool operator==(const VShardBackupUnit&, const VShardBackupUnit&) = default;
    [[nodiscard]] bool valid() const;
};

// TSBK v1: a complete cluster backup descriptor. It is published only after
// all exact-v1 TSP1 units are durable. There is intentionally no configurable
// expected count: this build has exactly VIRTUAL_SHARD_COUNT VShards and a
// shorter archive is incomplete, not a smaller valid cluster.
struct ClusterBackupManifest {
    std::string sourceClusterUuid;
    PortableControlBackup control;
    std::vector<VShardBackupUnit> vshards;

    friend bool operator==(const ClusterBackupManifest&, const ClusterBackupManifest&) = default;
    [[nodiscard]] bool valid() const;
    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<ClusterBackupManifest> decode(std::string_view bytes);
};

struct RestorePlan {
    std::string newClusterUuid;
    PortableControlBackup control;
    std::vector<VShardBackupUnit> vshards;
    bool ok = false;
    std::string error;
};

class BackupRestore {
public:
    // Capture only state which is meaningful in a new cluster. An active
    // retention sweep is refused: different per-VShard archive boundaries could
    // otherwise preserve an unrepresentable partially-fanned-out operation.
    static std::optional<PortableControlBackup> capturePortableControl(const control::Group0State& state);

    // Validate the complete manifest before exposing a restore plan. The new
    // UUID is canonical lowercase 128-bit hex and must differ from the source.
    // No snapshot is installed by this pure planning step.
    static RestorePlan planRestore(const ClusterBackupManifest& backup, std::string newClusterUuid);

    [[nodiscard]] static bool canonicalClusterUuid(std::string_view uuid);
    [[nodiscard]] static std::string unitRelativePath(uint16_t vshard);
};

// Crash-safe exact-v1 archive assembly. VShard units are copied through a
// fixed buffer, re-inspected at their destination, fsync'd, and atomically
// linked without replacement. The manifest is written last and is the sole completeness marker.
// Validation rejects aliases, symlinks, extra files, incomplete unit sets, and
// any disagreement between TSBK metadata and the embedded TSP1 payloads.
class ClusterBackupArchive {
public:
    [[nodiscard]] static std::string manifestRelativePath();

    // Stage one immutable source TSP1 as vshards/NNNN.tsp1. A published
    // archive is immutable and refuses further staging. nullopt means the
    // source or copied payload is invalid; filesystem failures throw.
    static seastar::future<std::optional<VShardBackupUnit>> stageVShard(const std::filesystem::path& archiveDirectory,
                                                                        uint16_t vshard,
                                                                        const std::filesystem::path& sourceTsp1);

    // Verify all 4,096 staged units against `manifest`, then durably publish
    // manifest.tsbk1. Returns false for an incomplete or mismatched archive.
    static seastar::future<bool> publish(const std::filesystem::path& archiveDirectory,
                                         const ClusterBackupManifest& manifest);

    // Validate a published archive in bounded memory. No unit is extracted.
    static seastar::future<std::optional<ClusterBackupManifest>> validate(
        const std::filesystem::path& archiveDirectory);
};

}  // namespace timestar::features
