#include "journal_gc.hpp"

#include "journal_segment.hpp"
#include "journal_sink.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>
#include <span>
#include <stdexcept>

namespace fs = std::filesystem;

namespace timestar {

seastar::future<> JournalGc::copyForward(JournalWriter& writer, uint64_t reclaimedSegment,
                                         std::vector<JournalRecord> records, Result* out) {
    // RE-VALIDATE INSIDE THE EXCLUSIVE SECTION. The plan above was made without the lock,
    // so the writer may have rotated since. What must still hold is that the segment being
    // COPIED INTO is strictly newer than `reclaimedSegment` -- the file these records came
    // from and the one the caller is about to UNLINK. Copying into it would destroy the
    // rescue along with the source.
    if (writer.currentSegmentNumber() <= reclaimedSegment)
        throw std::runtime_error("JournalGc: the writer's active segment is not above the segment being reclaimed");
    if (writer.fenced())
        throw std::runtime_error("JournalGc: the journal writer is fenced; refusing to copy records forward");
    for (const auto& record : records) {
        co_await writer.append(record);
        out->copiedRecords += 1;
        out->copiedBytes += record.payload.size();
    }
    co_await writer.barrier();
}

seastar::future<JournalGc::Result> JournalGc::collect(fs::path dir, uint64_t activeSegment, JournalWriter& writer,
                                                      const JournalRetention& retention, Options opts,
                                                      SharedShardJournal* exclusive) {
    Result result;

    // Enumerate sealed segments (strictly older than the active one), oldest
    // first. Blocking fs calls run off the reactor.
    std::vector<uint64_t> segments = co_await seastar::async([&dir, activeSegment] {
        std::vector<uint64_t> found;
        if (fs::exists(dir)) {
            for (const auto& entry : fs::directory_iterator(dir)) {
                if (auto n = JournalWriter::parseSegmentFilename(entry.path().filename().string())) {
                    if (*n < activeSegment)
                        found.push_back(*n);
                }
            }
        }
        std::sort(found.begin(), found.end());
        return found;
    });

    for (uint64_t seg : segments) {
        // READ AND PLAN OUTSIDE ANY EXCLUSION. A sealed segment is immutable -- the writer
        // never touches a segment below its active one -- so this needs no lock, and it is
        // the expensive part (a whole file read plus a full record scan). Only the copy
        // below is a write into the shared buffer, and only that takes the lock.
        const auto path = dir / JournalWriter::segmentFilename(seg);
        const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(path);
        auto scan = scanJournalSegment(std::span<const char>(bytes.data(), bytes.size()));

        // A sealed segment must have a valid header and no torn tail: the writer
        // only seals a segment by rotating past a barrier. A failure here is real
        // corruption, not a recoverable crash tail -- fail closed.
        if (!scan)
            throw std::runtime_error("JournalGc: corrupt sealed journal segment header: " + path.string());
        if (scan->torn)
            throw std::runtime_error("JournalGc: torn tail on a sealed journal segment: " + path.string());

        const auto gc = retention.planSegment(scan->records);

        // ------------------------------------------------------------------
        // HANDLE A SEGMENT THAT CANNOT BE RECLAIMED.
        //
        // A segment is unreclaimable when it still holds live records and they cannot (or
        // should not) be copied forward: copy-forward is off (the per-VShard layout, where
        // GC must never touch the writer), or the live set exceeds the budget, or the
        // segment is entirely live and copying it would churn every byte for no gain.
        //
        // The private layout stops here, preserving a physical suffix. That costs only
        // this VShard's own directory. The shared layout MUST continue: one idle group's
        // live HardState in an old segment would otherwise retain every later 64 MiB
        // segment on the reactor forever, even when those later segments are entirely
        // released. Deleting a later fully-released segment is safe by definition; any
        // per-VShard sequence hole it exposes is below a retained Snapshot boundary.
        // Production recoverRaftState is snapshot-aware, and JournalReplay now accepts
        // only that precise class of covered gap while continuing to reject gaps after
        // the latest retained Snapshot.
        // ------------------------------------------------------------------
        const bool wholeSegmentLive = gc.liveRecordIndices.size() == scan->records.size();
        const bool overBudget = gc.liveRecordIndices.size() > opts.maxCopyForwardRecords;
        if (!gc.reclaimable && (wholeSegmentLive || !opts.copyForward || overBudget)) {
            if (opts.continuePastPinned) {
                // Shared mode: this segment is genuinely pinned, but it has no authority
                // over the independently releasable segments after it.
                result.pinnedSegments.push_back(seg);
                continue;
            }
            // Private mode: `pinnedSegments` is the census of the physical suffix this
            // pass leaves behind, not just the record of the segment that halted it.
            for (auto it = std::find(segments.begin(), segments.end(), seg); it != segments.end(); ++it)
                result.pinnedSegments.push_back(*it);
            break;
        }

        if (!gc.reclaimable) {
            // Copy every live record forward into the active segment, make it
            // durable, THEN delete the old segment. Order is the crash-atomicity
            // guarantee: a crash before the barrier keeps the old segment; a
            // crash after the barrier but before the delete leaves a durable
            // BYTE-IDENTICAL duplicate, which recovery absorbs -- see the header.
            std::vector<JournalRecord> live;
            live.reserve(gc.liveRecordIndices.size());
            for (size_t idx : gc.liveRecordIndices)
                live.push_back(scan->records[idx]);
            // THE ONLY PART THAT NEEDS EXCLUSION, and it is bounded by
            // maxCopyForwardRecords rather than by the size of the directory.
            if (exclusive) {
                co_await exclusive->runExclusive(
                    [&writer, seg, recs = std::move(live), out = &result]() mutable -> seastar::future<> {
                        return copyForward(writer, seg, std::move(recs), out);
                    });
            } else {
                co_await copyForward(writer, seg, std::move(live), &result);
            }
            result.copyForwardSegments.push_back(seg);
        }

        co_await seastar::remove_file(path.string());
        // Make the deletion durable: an un-synced directory entry could resurrect
        // a "deleted" segment after a crash, reintroducing a duplicate record.
        co_await seastar::sync_directory(dir.string());
        result.deletedSegments.push_back(seg);
    }

    co_return result;
}

}  // namespace timestar
