#pragma once

#include "../core/series_id.hpp"
#include "../core/vshard.hpp"

#include <cstdint>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

namespace timestar {

// The offline shard_N -> VShard migration plan (Task 6). Migration is offline and
// exclusive under the root lock; it emits every legacy point at revision 0 (the
// migrated-floor, so any replicated write later beats imported data -- ADR 0003).
// This models the plan's decisions and its two fail-safe policies:
//
//   - ORPHAN QUARANTINE: a series with data but no discoverable metadata is
//     quarantined and COUNTED, never silently dropped (the Task 6 gate). It is
//     recorded here with a reason rather than assigned a VShard.
//   - FREE-SPACE PRECONDITION: migration rewrites data into the VShard layout
//     before removing the legacy copy, so it must first confirm enough free
//     space (a generation of both layouts must coexist for crash-atomicity).
//
// A migratable series is assigned to its target VShard = virtualShard(series);
// the mapping is derived, never stored, so it is stable across core counts.
class MigrationPlan {
public:
    // Assign a discoverable series to its target VShard (revision 0 implied).
    // Idempotent for a given series.
    void assign(const SeriesId128& series);

    // Quarantine an orphan series (data present, metadata missing/unreadable).
    // Recorded with a reason and counted; never assigned or dropped.
    struct Orphan {
        SeriesId128 series;
        std::string reason;
        friend bool operator==(const Orphan&, const Orphan&) = default;
    };
    void quarantineOrphan(const SeriesId128& series, std::string reason);

    // The target VShards this migration will produce, ascending.
    [[nodiscard]] std::vector<VShardId> targetVShards() const;
    // The series assigned to a VShard, in the order assigned.
    [[nodiscard]] std::vector<SeriesId128> seriesForVShard(VShardId vshard) const;
    [[nodiscard]] size_t assignedCount() const noexcept { return assignedCount_; }

    [[nodiscard]] const std::vector<Orphan>& orphans() const noexcept { return orphans_; }
    [[nodiscard]] size_t orphanCount() const noexcept { return orphans_.size(); }

    // Free-space precondition: migration must fit BOTH the legacy bytes and the
    // rewritten VShard copy simultaneously (one complete generation on a crash),
    // plus a safety headroom. Returns true iff available >= legacy + rewritten +
    // headroom (all overflow-safe).
    [[nodiscard]] static bool freeSpaceSufficient(uint64_t legacyBytes, uint64_t rewrittenBytes,
                                                  uint64_t availableBytes, uint64_t headroomBytes);

private:
    std::map<uint16_t, std::vector<SeriesId128>> byVShard_;        // target vshard -> assigned series (insertion order)
    std::unordered_set<SeriesId128, SeriesId128::Hash> assigned_;  // dedup: assign() is idempotent
    std::vector<Orphan> orphans_;
    size_t assignedCount_ = 0;
};

}  // namespace timestar
