#pragma once

#include "../control/control_map_cache.hpp"
#include "backup_restore.hpp"

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>

namespace timestar::features {

// Immutable exact-v1 resume fence stored beside (never inside) an archive.
// Completed VShard files in the archive are the progress journal; this record
// prevents a resume from mixing a different operation, source cluster,
// serving map, or portable Group-0 recovery state with those files.
struct ClusterBackupExportCheckpoint {
    std::string operationId;
    std::string sourceClusterUuid;
    control::ControlMap servingMap;
    PortableControlBackup control;

    friend bool operator==(const ClusterBackupExportCheckpoint&, const ClusterBackupExportCheckpoint&) = default;
    [[nodiscard]] static bool canonicalOperationId(std::string_view value);
    [[nodiscard]] bool valid() const;
    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<ClusterBackupExportCheckpoint> decode(std::string_view bytes);
};

class DurableClusterBackupExport {
public:
    explicit DurableClusterBackupExport(std::filesystem::path archiveDirectory);

    // Create the immutable checkpoint without replacement, or return the
    // existing exact checkpoint. Corruption and conflicts fail closed.
    ClusterBackupExportCheckpoint createOrLoad(const ClusterBackupExportCheckpoint& desired) const;
    std::optional<ClusterBackupExportCheckpoint> load() const;

    // One transfer is active at a time, so one bounded partial is sufficient.
    // Its canonical VShard name makes interrupted state diagnosable. A resume
    // removes only this partial; completed archive units are never deleted.
    std::filesystem::path prepareDownload(uint16_t vshard) const;
    void removeDownload(const std::filesystem::path& path) const;

    const std::filesystem::path& stateDirectory() const { return stateDirectory_; }
    const std::filesystem::path& checkpointPath() const { return checkpointPath_; }

private:
    void ensureStateDirectory() const;

    std::filesystem::path archiveDirectory_;
    std::filesystem::path stateDirectory_;
    std::filesystem::path checkpointPath_;
};

}  // namespace timestar::features
