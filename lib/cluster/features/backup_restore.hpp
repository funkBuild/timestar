#pragma once

#include "../../core/vshard.hpp"
#include "../control/group0_state.hpp"

#include <array>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::features {

// One 256-bit secret used only to authenticate exact-v1 cluster backup
// manifests. The key identifier is SHA-256(key); the manifest tag is
// HMAC-SHA-256 over every manifest field and therefore transitively binds the
// hashes and sizes of all 4,096 immutable TSP1 units. Key files are deliberately
// small, owner-only files rather than values accepted over HTTP.
class ClusterBackupAuthenticationKey {
public:
    [[nodiscard]] static ClusterBackupAuthenticationKey load(const std::filesystem::path& path);
    [[nodiscard]] static ClusterBackupAuthenticationKey fromHex(std::string_view hex);

    [[nodiscard]] std::string keyId() const;
    [[nodiscard]] std::string authenticate(std::string_view bytes) const;
    [[nodiscard]] bool verifies(std::string_view bytes, std::string_view lowercaseHexTag) const;

    friend bool operator==(const ClusterBackupAuthenticationKey&, const ClusterBackupAuthenticationKey&) = default;

private:
    explicit ClusterBackupAuthenticationKey(std::array<unsigned char, 32> bytes) : bytes_(bytes) {}
    std::array<unsigned char, 32> bytes_{};
};

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
    // Checksummed exact-v1 encoding used by the durable export checkpoint. It
    // contains only portable authority-free state, just like the manifest.
    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<PortableControlBackup> decode(std::string_view bytes);
};

// One immutable TSP1 file in a completed archive. The filename is derived from
// vshard (vshards/NNNN.tsp1), never supplied by the archive. encodedSha256 is
// over the complete file; the TSP1 trailer independently protects
// and validates its body, catalog and logical verification hash. The SHA-256
// digest is authenticated by the cluster manifest HMAC.
struct VShardBackupUnit {
    uint16_t vshard = 0;
    uint64_t snapshotRevision = 0;
    std::string verificationHash;
    std::string catalogHash;
    uint64_t encodedSize = 0;
    std::string encodedSha256;

    friend bool operator==(const VShardBackupUnit&, const VShardBackupUnit&) = default;
    [[nodiscard]] bool valid() const;
};

// TSBK v1: a complete cluster backup descriptor. It is published only after
// all exact-v1 TSP1 units are durable. There is intentionally no configurable
// expected count: this build has exactly VIRTUAL_SHARD_COUNT VShards and a
// shorter archive is incomplete, not a smaller valid cluster.
struct ClusterBackupManifest {
    std::string authenticationKeyId;
    std::string sourceClusterUuid;
    PortableControlBackup control;
    std::vector<VShardBackupUnit> vshards;
    std::string authenticationTag;

    friend bool operator==(const ClusterBackupManifest&, const ClusterBackupManifest&) = default;
    [[nodiscard]] bool valid() const;
    void authenticate(const ClusterBackupAuthenticationKey& key);
    [[nodiscard]] bool authenticatedBy(const ClusterBackupAuthenticationKey& key) const;
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
    static RestorePlan planRestore(const ClusterBackupManifest& backup, std::string newClusterUuid,
                                   const ClusterBackupAuthenticationKey& authenticationKey);

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
                                         const ClusterBackupManifest& manifest,
                                         const ClusterBackupAuthenticationKey& authenticationKey);

    // Validate a published archive in bounded memory. No unit is extracted.
    static seastar::future<std::optional<ClusterBackupManifest>> validate(
        const std::filesystem::path& archiveDirectory, const ClusterBackupAuthenticationKey& authenticationKey);
};

}  // namespace timestar::features
