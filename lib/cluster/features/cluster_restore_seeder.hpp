#pragma once

#include "backup_restore.hpp"
#include "../control/group0_state.hpp"
#include "../raft/raft_types.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <seastar/core/future.hh>
#include <string>
#include <vector>

namespace timestar::features {

// The restore marker is outside cluster_raft so ordinary Raft recovery cannot
// accidentally treat a partial all-group import as a complete cluster.
enum class ClusterRestoreTargetState : uint8_t { Absent, InProgress, Prepared, Activated, Invalid };

struct ClusterRestoreSeedRequest {
    std::filesystem::path archiveDirectory;
    std::filesystem::path dataDirectory;
    std::optional<ClusterBackupAuthenticationKey> authenticationKey;
    std::string newClusterUuid;
    std::array<uint8_t, 16> clusterUuidBytes{};
    std::array<uint8_t, 16> bootId{};
    raft::NodeId self = raft::kNoNode;
    raft::NodeId controlSeed = raft::kNoNode;
    unsigned coreCount = 0;
    control::ControlMap servingMap;

    // Present only on the configured control seed. Restore creates that one
    // node as Active and leaves every other old/new identity absent; other
    // nodes must join with newly minted authority after recovery.
    std::optional<control::NodeRecord> controlSeedRecord;

    // Test-only crash seam. It runs after one VShard journal is durable but
    // before the batched progress marker necessarily advances.
    std::function<void(uint16_t)> vshardDurableForTesting;
};

struct ClusterRestoreSeedResult {
    size_t localVShards = 0;
    size_t seededThisRun = 0;
    bool resumed = false;
};

// Offline exact-v1 restore preparation. It writes only fresh Raft journals and
// a durable progress marker; the normal startup path subsequently replays each
// imported snapshot through the real state machine/Engine install path.
class ClusterRestoreSeeder {
public:
    static std::filesystem::path markerPath(const std::filesystem::path& dataDirectory);
    static std::filesystem::path releaseReceiptPath(const std::filesystem::path& dataDirectory);
    static ClusterRestoreTargetState inspectTarget(const std::filesystem::path& dataDirectory);
    static seastar::future<ClusterRestoreSeedResult> seed(ClusterRestoreSeedRequest request);

    // Offline ceremony: collect the completed marker from every participant,
    // finalize one exact-v1 release, distribute that release back to the same
    // nodes, then activate each data root. No node may open Raft networking from
    // a merely Prepared marker.
    static void finalizeRelease(const std::vector<std::filesystem::path>& markerFiles,
                                const std::filesystem::path& output);
    static void activate(const std::filesystem::path& dataDirectory, const std::filesystem::path& releaseFile);
};

}  // namespace timestar::features
