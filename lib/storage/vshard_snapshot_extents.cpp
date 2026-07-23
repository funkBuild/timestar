#include "vshard_snapshot_extents.hpp"

#include "../core/placement_table.hpp"  // virtualShard

#include <map>
#include <seastar/core/coroutine.hh>
#include <vector>

namespace timestar {

seastar::future<> addTsmFileExtents(VShardExtentMap& map, uint64_t fileId, ::TSM& tsm) {
    // Snapshot the file's series ids first: getFullIndexEntry() may mutate the
    // full-index cache, so we do not hold an iterator across the await.
    const std::vector<SeriesId128> seriesIds = tsm.getSeriesIds();

    // Accumulate each VShard's revision range across all its series' blocks in
    // this file, then emit one extent per VShard.
    std::map<uint16_t, RevisionRange> perVShard;
    for (const auto& seriesId : seriesIds) {
        const uint16_t vs = timestar::virtualShard(seriesId);
        ::TSMIndexEntry* entry = co_await tsm.getFullIndexEntry(seriesId);
        if (entry == nullptr)
            continue;
        RevisionRange& range = perVShard[vs];
        for (const auto& block : entry->indexBlocks)
            range.merge(RevisionRange{block.blockMinRev, block.blockMaxRev, /*empty=*/false});
    }

    for (const auto& [vs, range] : perVShard)
        map.add(VShardId{vs}, VShardExtent{fileId, range});

    co_return;
}

}  // namespace timestar
