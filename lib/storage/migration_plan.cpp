#include "migration_plan.hpp"

#include "../core/placement_table.hpp"  // virtualShard

#include <limits>

namespace timestar {

void MigrationPlan::assign(const SeriesId128& series) {
    if (!seen_.insert(series).second)
        return;  // already classified (assigned or quarantined) -- first wins
    const uint16_t vs = timestar::virtualShard(series);
    byVShard_[vs].push_back(series);
    ++assignedCount_;
}

void MigrationPlan::quarantineOrphan(const SeriesId128& series, std::string reason) {
    if (!seen_.insert(series).second)
        return;  // already classified -- never both assigned and quarantined, no dup
    orphans_.push_back(Orphan{series, std::move(reason)});
}

std::vector<VShardId> MigrationPlan::targetVShards() const {
    std::vector<VShardId> out;
    out.reserve(byVShard_.size());
    for (const auto& [vs, series] : byVShard_)  // std::map -> ascending
        out.push_back(VShardId{vs});
    return out;
}

std::vector<SeriesId128> MigrationPlan::seriesForVShard(VShardId vshard) const {
    auto it = byVShard_.find(vshard.value());
    return it == byVShard_.end() ? std::vector<SeriesId128>{} : it->second;
}

bool MigrationPlan::freeSpaceSufficient(uint64_t legacyBytes, uint64_t rewrittenBytes, uint64_t availableBytes,
                                        uint64_t headroomBytes) {
    // required = legacy + rewritten + headroom, computed with explicit overflow
    // guards so a saturating sum can never wrap and falsely pass the check.
    constexpr uint64_t kMax = std::numeric_limits<uint64_t>::max();
    if (legacyBytes > kMax - rewrittenBytes)
        return false;
    uint64_t required = legacyBytes + rewrittenBytes;
    if (required > kMax - headroomBytes)
        return false;
    required += headroomBytes;
    return availableBytes >= required;
}

}  // namespace timestar
