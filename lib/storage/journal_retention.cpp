#include "journal_retention.hpp"

#include <algorithm>
#include <cstddef>

namespace timestar {

void JournalRetention::setReleased(VShardId vshard, uint64_t releasedSeq) {
    auto& current = released_[vshard.value()];
    current = std::max(current, releasedSeq);  // monotonic; ignore a regression
}

void JournalRetention::clearReleased(VShardId vshard) {
    released_.erase(vshard.value());  // see the header: movement MUST call this on teardown
}

uint64_t JournalRetention::released(VShardId vshard) const {
    auto it = released_.find(vshard.value());
    return it == released_.end() ? 0 : it->second;
}

JournalRetention::SegmentGc JournalRetention::planSegment(const std::vector<JournalRecord>& segmentRecords) const {
    SegmentGc gc;
    gc.reclaimable = true;
    for (size_t i = 0; i < segmentRecords.size(); ++i) {
        const auto& record = segmentRecords[i];
        // seq > released => still needed. released defaults to 0, so with nothing
        // released every record (seq >= 1) is live.
        if (record.vshardSeq > released(record.vshard)) {
            gc.reclaimable = false;
            gc.liveRecordIndices.push_back(i);
        }
    }
    return gc;
}

}  // namespace timestar
