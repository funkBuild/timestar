#pragma once

#include "../core/vshard.hpp"
#include "point_revision.hpp"

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace timestar {

// One VShard's data footprint in a single TSM file: which file, and the
// revision range of that VShard's points within it (ADR 0002/0003).
struct VShardExtent {
    uint64_t fileId = 0;  // TSM file sequence id
    RevisionRange revRange;

    friend bool operator==(const VShardExtent&, const VShardExtent&) = default;
};

// Per-core "VShard extents in the tier-0 manifest" (Task 4c): the set of TSM
// files covering each VShard, with per-VShard revision ranges. It is the
// metadata a VShard snapshot (Task 4d) reads to find exactly which files hold a
// VShard's data without scanning every file.
//
// Because tier-0 TSM files are (for now) multiplexed across VShards, one file
// appears as an extent under every VShard whose series it contains. Extraction
// of one VShard is then "iterate its extents", and its overall revision range is
// the union across them.
//
// Serialisation is deterministic (entries ordered by (vshard, fileId)) with a
// trailing CRC; decode() fails closed on any corruption.
class VShardExtentMap {
public:
    // Record that `file` (with the given per-VShard revision range) covers
    // `vshard`. Adding the same (vshard, fileId) twice merges the revision range.
    void add(VShardId vshard, const VShardExtent& extent);

    // Extents for a VShard, ordered by fileId (empty if none).
    [[nodiscard]] std::vector<VShardExtent> extents(VShardId vshard) const;
    // Union revision range of a VShard across all its extents (empty if none).
    [[nodiscard]] RevisionRange revRange(VShardId vshard) const;
    // All covered VShards, ascending.
    [[nodiscard]] std::vector<VShardId> vshards() const;
    // Total number of (vshard, file) extents.
    [[nodiscard]] size_t extentCount() const { return countExtents(); }

    [[nodiscard]] std::string encode() const;
    [[nodiscard]] static std::optional<VShardExtentMap> decode(std::string_view bytes);

private:
    [[nodiscard]] size_t countExtents() const;

    // vshard -> (fileId -> revRange), both ordered for deterministic encode.
    std::map<uint16_t, std::map<uint64_t, RevisionRange>> byVShard_;
};

}  // namespace timestar
