#include "vshard_extent_map.hpp"

namespace timestar {

void VShardExtentMap::add(VShardId vshard, const VShardExtent& extent) {
    RevisionRange& range = byVShard_[vshard.value()][extent.fileId];
    range.merge(extent.revRange);  // same (vshard, file) added twice -> union
}

std::vector<VShardExtent> VShardExtentMap::extents(VShardId vshard) const {
    std::vector<VShardExtent> out;
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return out;
    out.reserve(it->second.size());
    for (const auto& [fileId, range] : it->second)  // std::map -> ascending fileId
        out.push_back(VShardExtent{fileId, range});
    return out;
}

RevisionRange VShardExtentMap::revRange(VShardId vshard) const {
    RevisionRange range;
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return range;
    for (const auto& [fileId, r] : it->second)
        range.merge(r);
    return range;
}

}  // namespace timestar
