#pragma once

#include "journal_record.hpp"
#include "journal_retention.hpp"
#include "journal_writer.hpp"

#include <cstdint>
#include <filesystem>
#include <seastar/core/future.hh>
#include <vector>

namespace timestar {

class SharedShardJournal;

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
//   - A segment that can be neither is PINNED, and GC stops there, so what
//     survives is always a physical SUFFIX of the segment sequence (see collect()).
//
// GC never touches the active segment (the one the writer is appending to) or
// any segment >= it, so copy-forward targets never alias reclaim targets.
//
// WHY THE CRASH WINDOW BETWEEN THE COPY BARRIER AND THE UNLINK IS SAFE. It leaves a
// BYTE-IDENTICAL duplicate of each relocated record. Production recovery is
// `JournalWriter::open()` handing its record set to `recoverRaftState`, which sorts one
// VShard's records by `vshard_seq` and replays them: a repeated record re-applies the same
// HardState, re-places the same entry at the same index, or re-decodes the same Snapshot,
// so the reconstructed state is identical whether the duplicate is there or not. (The
// ADR's `JournalReplay` is stricter still -- it drops exact duplicates and fails closed on
// a CONFLICTING one -- but it has no production caller today, so it is not what makes this
// safe. Do not cite it as though it were.)

struct JournalGcOptions {
    // Copy a partially-released segment's still-live records into the ACTIVE
    // segment so the old one can be deleted (the laggard rule this brick was
    // written for). REQUIRES EXCLUSIVE ACCESS TO `writer` for the whole call:
    // the copy appends and barriers directly against it, and an append landing
    // inside a concurrent barrier is written at an offset computed before it
    // existed and then erased as though it had been (see the ordering contract
    // on SharedShardJournal, which is why its caller wraps this in
    // runExclusive()). With `false`, a partially-released segment is simply
    // PINNED -- left untouched -- and the writer is never touched at all, which
    // is what makes delete-only GC safe to run alongside live appends.
    bool copyForward = true;
    // Never copy more than this many live records out of one segment. A segment
    // that is mostly live is pinned instead: copy-forward is a WRITE, and
    // rewriting most of a segment to reclaim the rest of it costs more disk
    // traffic than the reclaim is worth. It also bounds the exclusive section.
    size_t maxCopyForwardRecords = 512;
};

class JournalGc {
public:
    struct Result {
        std::vector<uint64_t> deletedSegments;      // segment numbers removed
        std::vector<uint64_t> copyForwardSegments;  // sealed segments whose live records were moved
        std::vector<uint64_t> pinnedSegments;       // left in place: still hold live records
        uint64_t copiedRecords = 0;
        uint64_t copiedBytes = 0;  // encoded body bytes copied forward
    };

    // GC policy; a namespace-scope type so it can be a defaulted parameter here
    // (a nested class's default member initializers are not usable in the enclosing
    // class's own member declarations).
    using Options = JournalGcOptions;

    // Reclaim sealed segments strictly older than `activeSegment`, oldest first.
    // `writer` must be open (its current segment is the copy-forward target -- and it
    // is not touched at all when `opts.copyForward` is false); `retention` supplies
    // the released watermarks. Throws on a corrupt/torn sealed segment (that is not a
    // recoverable tail on a sealed segment) or on any writer I/O error (which also
    // fences the writer).
    // `exclusive`, when non-null, is the shared per-shard sink whose group-commit rounds
    // must not interleave with the copy-forward WRITES. It is taken for the copy ONLY --
    // reading and planning a sealed segment is done outside it, because a sealed segment
    // is immutable and the reads are the expensive part. Null for a private writer (no
    // other appender) and for delete-only collection (no writes at all).
    static seastar::future<Result> collect(std::filesystem::path dir, uint64_t activeSegment, JournalWriter& writer,
                                           const JournalRetention& retention, Options opts = Options{},
                                           SharedShardJournal* exclusive = nullptr);

private:
    // The bounded write half: append `records` to the writer and barrier. Runs inside the
    // exclusive section when there is one. A NAMED static coroutine, never a coroutine
    // lambda handed to runExclusive -- such a lambda's frame points at a closure the helper
    // owns.
    static seastar::future<> copyForward(JournalWriter& writer, uint64_t oldestKeptSegment,
                                         std::vector<JournalRecord> records, Result* out);
};

}  // namespace timestar
