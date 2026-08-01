#pragma once

#include "control_map_cache.hpp"

#include <filesystem>
#include <optional>

namespace timestar::control {

// Crash-safe storage for the last COMPLETE committed serving map. The group-0
// journal remains authoritative; this file is the restart/outage fallback that
// lets a node keep routing from the last known committed epoch while group 0 is
// temporarily unavailable.
//
// Calls are synchronous and must be serialized by the shard-0 control-plane
// owner (and moved off the reactor with seastar::async by that owner). persist()
// writes a checksummed envelope to a temporary file, fsyncs it, atomically
// renames it, and fsyncs the parent directory before returning.
class DurableControlMapStore {
public:
    explicit DurableControlMapStore(std::filesystem::path dataDir) : path_(pathIn(dataDir)) {}

    // Missing means this node has never published a serving map. A present but
    // corrupt, incomplete, or semantically invalid file throws: startup must not
    // silently fall back to static placement in that case.
    std::optional<ControlMap> load() const;

    // Persist a strictly newer complete map. An exact same-epoch replay is an
    // idempotent success; a lower epoch or conflicting content at the same epoch
    // throws and leaves the existing durable map untouched.
    void persist(const ControlMap& map) const;

    const std::filesystem::path& path() const { return path_; }
    static std::filesystem::path pathIn(const std::filesystem::path& dataDir) {
        return dataDir / "control_map.cache";
    }

private:
    std::filesystem::path path_;
};

}  // namespace timestar::control
