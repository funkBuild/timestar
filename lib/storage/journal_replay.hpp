#pragma once

#include "../core/vshard.hpp"
#include "journal_record.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

namespace timestar {

// Routes a multiplexed journal's records to their boot-derived owning cores and
// validates per-VShard sequence integrity. This is the in-memory heart of the
// replay path (Task 4b): the segment/barrier I/O layer decodes records and feeds
// them here in append order.
//
// The routing is a pure function of (vshard, THIS process's core count), not the
// core count the record was written under -- assignCore(vshard, coreCount). So a
// stream written under core count N replays correctly under core count M: every
// record of a VShard lands on one core, in sequence order (the Task 1
// replay-routing contract).
class JournalReplay {
public:
    // coreCount must be >= 1.
    explicit JournalReplay(unsigned coreCount);

    // Ingest one record in append order. Returns false and latches a failure if
    // the record's VShard id is out of range, its VShard sequence is not the
    // gap-free monotonic successor of the previous record for that VShard
    // (loss/corruption), or ingest is called after a prior failure.
    bool ingest(const JournalRecord& record);

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] const std::string& failureDetail() const noexcept { return failureDetail_; }

    // The core that owns a VShard under this replay's core count.
    [[nodiscard]] unsigned ownerCore(VShardId vshard) const { return assignCore(vshard, coreCount_); }

    // Records routed to `core`, in append order (which preserves each VShard's
    // sequence order, since all of a VShard's records route to one core). An
    // out-of-range core returns an empty list.
    [[nodiscard]] const std::vector<JournalRecord>& recordsForCore(unsigned core) const;

    [[nodiscard]] unsigned coreCount() const noexcept { return coreCount_; }

private:
    void fail(std::string detail);

    unsigned coreCount_;
    bool failed_ = false;
    std::string failureDetail_;
    std::vector<std::vector<JournalRecord>> byCore_;          // [core] -> owned records, append order
    std::unordered_map<uint16_t, uint64_t> lastSeqByVShard_;  // vshard -> last ingested sequence
};

}  // namespace timestar
