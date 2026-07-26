#include "journal_gc.hpp"

#include "journal_segment.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>
#include <span>
#include <stdexcept>

namespace fs = std::filesystem;

namespace timestar {

seastar::future<JournalGc::Result> JournalGc::collect(fs::path dir, uint64_t activeSegment, JournalWriter& writer,
                                                      const JournalRetention& retention, Options opts) {
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

        // Nothing reclaimable in this segment (every record still live): leave it
        // untouched. Copying a fully-live segment forward would only churn bytes
        // and reorder records for no retention gain.
        if (!gc.reclaimable && gc.liveRecordIndices.size() == scan->records.size()) {
            result.pinnedSegments.push_back(seg);
            continue;
        }

        // PIN rather than copy when copy-forward is off, or when the live set is too
        // big to be worth (or to bound) the rewrite. Pinning is always safe: the
        // segment keeps the only copy of records somebody still needs, which is
        // precisely why it must not be deleted. Note that a LATER segment may still
        // be fully released and deleted -- that removes only records at or below some
        // VShard's released watermark, so what survives is still a prefix-free,
        // gap-free sequence per VShard (JournalReplay::finalize validates exactly
        // that).
        if (!gc.reclaimable && (!opts.copyForward || gc.liveRecordIndices.size() > opts.maxCopyForwardRecords)) {
            result.pinnedSegments.push_back(seg);
            continue;
        }

        if (!gc.reclaimable) {
            // Copy every live record forward into the active segment, make it
            // durable, THEN delete the old segment. Order is the crash-atomicity
            // guarantee: a crash before the barrier keeps the old segment; a
            // crash after the barrier but before the delete leaves a durable
            // duplicate that the recovery apply layer resolves by vshard_seq.
            for (size_t idx : gc.liveRecordIndices) {
                const auto& record = scan->records[idx];
                co_await writer.append(record);
                result.copiedRecords += 1;
                result.copiedBytes += record.payload.size();
            }
            co_await writer.barrier();
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
