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
// them here, then calls finalize().
//
// The routing is a pure function of (vshard, THIS process's core count), not the
// core count the record was written under -- assignCore(vshard, coreCount). So a
// stream written under core count N replays correctly under core count M.
//
// Records are ordered by their per-VShard `vshard_seq`, NOT by physical append
// order (ADR 0001 sec 6.3: "apply in vshard_seq order"). This is required because
// segment-GC copy-forward relocates a laggard VShard's older records to the
// physical tail (behind its newer records already in the active segment), and a
// crash in the GC barrier->delete window leaves an identical duplicate. finalize()
// therefore sorts each VShard's records by sequence and drops exact duplicates;
// only a genuine gap or a CONFLICTING duplicate (same seq, different bytes) fails
// closed.
class JournalReplay {
public:
    // coreCount must be >= 1.
    explicit JournalReplay(unsigned coreCount);

    // Buffer one record. Returns false and latches a failure only if the record's
    // VShard id is out of range, or ingest is called after a prior failure.
    // Sequence integrity is checked in finalize(), not here (physical order is not
    // authoritative once copy-forward can reorder records).
    bool ingest(const JournalRecord& record);

    // Sort each VShard's buffered records by sequence, drop exact duplicates, and
    // validate that each VShard's surviving sequence is gap-free (starting at
    // whatever it starts at -- a retained log may begin mid-sequence after GC).
    // Populates the per-core routing. Fails closed on a gap or a conflicting
    // duplicate. Returns true iff replay is valid. Idempotent.
    bool finalize();

    [[nodiscard]] bool failed() const noexcept { return failed_; }
    [[nodiscard]] const std::string& failureDetail() const noexcept { return failureDetail_; }

    // The core that owns a VShard under this replay's core count.
    [[nodiscard]] unsigned ownerCore(VShardId vshard) const { return assignCore(vshard, coreCount_); }

    // Records routed to `core` after finalize(), each VShard's records contiguous
    // and in ascending sequence order (VShards ordered by id). Empty before
    // finalize(), on failure, or for an out-of-range core.
    [[nodiscard]] const std::vector<JournalRecord>& recordsForCore(unsigned core) const;

    [[nodiscard]] unsigned coreCount() const noexcept { return coreCount_; }

private:
    void fail(std::string detail);

    unsigned coreCount_;
    bool failed_ = false;
    bool finalized_ = false;
    std::string failureDetail_;
    std::vector<std::vector<JournalRecord>> byCore_;                     // [core] -> routed records (post-finalize)
    std::unordered_map<uint16_t, std::vector<JournalRecord>> buffered_;  // vshard -> buffered records
};

}  // namespace timestar
