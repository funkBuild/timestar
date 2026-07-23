#pragma once

#include "journal_record.hpp"
#include "journal_retention.hpp"
#include "journal_writer.hpp"

#include <cstdint>
#include <filesystem>
#include <seastar/core/future.hh>
#include <vector>

namespace timestar {

// Segment-GC I/O executor (ADR 0001 sec 5). Given a core's released watermarks
// (JournalRetention), it reclaims every SEALED segment older than the active
// one:
//
//   - A fully-released segment (no live records) is simply DELETED.
//   - A segment that still holds live records for a laggard VShard has those
//     records COPIED FORWARD into the active segment (re-framed and re-CRC'd via
//     the writer), made durable with a barrier, and only THEN is the old segment
//     deleted. A crash at any point leaves one complete generation: either the
//     old segment (copy not yet durable) or the new copy (old not yet deleted).
//
// GC never touches the active segment (the one the writer is appending to) or
// any segment >= it, so copy-forward targets never alias reclaim targets.
class JournalGc {
public:
    struct Result {
        std::vector<uint64_t> deletedSegments;      // segment numbers removed
        std::vector<uint64_t> copyForwardSegments;  // sealed segments whose live records were moved
        uint64_t copiedRecords = 0;
        uint64_t copiedBytes = 0;  // encoded body bytes copied forward
    };

    // Reclaim sealed segments strictly older than `activeSegment`, oldest first.
    // `writer` must be open (its current segment is the copy-forward target);
    // `retention` supplies the released watermarks. Throws on a corrupt/torn
    // sealed segment (that is not a recoverable tail on a sealed segment) or on
    // any writer I/O error (which also fences the writer).
    static seastar::future<Result> collect(std::filesystem::path dir, uint64_t activeSegment, JournalWriter& writer,
                                           const JournalRetention& retention);
};

}  // namespace timestar
