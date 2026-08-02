#pragma once

#include "journal_record.hpp"
#include "journal_segment.hpp"

#include <cstdint>
#include <filesystem>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>
#include <string>
#include <vector>

namespace timestar {

// Append-only per-core journal writer over DMA segment files (Task 4b I/O, on
// top of the ADR 0001 segment format). Records are appended (buffered) and made
// durable in group-commit barriers (stream flush + fdatasync). Segments rotate
// at a target size and a record never straddles a segment boundary.
//
// Recovery (open()) reads existing segments in order, validates cluster/core/
// segment identity, and cleanly drops a torn record tail on the last one. The
// journal directory contains canonical segments plus the exact
// `snapshot_sidecars` auxiliary directory; any other non-canonical or
// non-regular entry fences recovery because it may be a damaged durable segment.
// Only a zero-byte final segment (crash before its buffered header was written)
// is deleted; any non-empty invalid segment is preserved and fences recovery.
//
// Any append/barrier I/O error FENCES the writer: it rejects all further appends
// and reports unhealthy (the journal safety contract -- no catch-and-continue on
// the durability path).
class JournalWriter {
public:
    // `headerTemplate` supplies clusterUuid/coreNumber/bootId; the segment number
    // is assigned per segment. `segmentBytes` is the rotation target (> header).
    JournalWriter(std::filesystem::path directory, JournalSegmentHeader headerTemplate, size_t segmentBytes);

    // Recover existing segments and open a fresh segment for append. Returns all
    // recovered records across segments, in append order. Throws on a corrupt
    // (bad/foreign/misnamed header, or torn-non-last) segment -- that is not a
    // recoverable tail. Also throws and fences if the directory contains an
    // unrecognised entry or the uint64 segment identity space is exhausted.
    seastar::future<std::vector<JournalRecord>> open();

    // Append a record (buffered, not yet durable). Rotates first if the current
    // segment is at the size target. Throws if the writer is fenced.
    seastar::future<> append(const JournalRecord& record);

    // Group-commit barrier: make every append so far durable. Fences on I/O error.
    seastar::future<> barrier();

    seastar::future<> close();

    // fdatasync calls issued by this writer (barriers, including the ones a
    // rotation or a seal performs internally). The DISK win of a shared per-shard
    // journal (debt D-10) is invisible on a tmpfs bench, so this ratio -- sync
    // REQUESTS per fsync, see JournalSink -- is the only honest evidence of
    // coalescing available on this box.
    [[nodiscard]] uint64_t fsyncs() const noexcept { return fsyncs_; }

    [[nodiscard]] bool fenced() const noexcept { return fenced_; }
    [[nodiscard]] uint64_t currentSegmentNumber() const noexcept { return currentSegment_; }

    // The per-core journal segment filename for a segment number.
    static std::string segmentFilename(uint64_t segmentNumber);
    // Parse a segment number from a filename; nullopt if not a canonical seg name.
    static std::optional<uint64_t> parseSegmentFilename(std::string_view name);

private:
    seastar::future<> startSegment(uint64_t segmentNumber);
    seastar::future<> sealCurrent();  // flush the tail, truncate to logical size, close the file
    // Write a whole aligned buffer at an aligned offset, keeping the source memory
    // aligned across short writes (consumes the buffer).
    seastar::future<> dmaWriteBuffer(uint64_t offset, seastar::temporary_buffer<char> buf);
    // Write `len` bytes from `src` at an aligned file offset (bounce through an
    // aligned buffer; `len` and `offset` must be multiples of alignment_).
    seastar::future<> dmaWriteAligned(uint64_t offset, const char* src, size_t len);
    // Write one padded block: `rem` (< alignment_) real bytes then zero fill.
    seastar::future<> writePaddedBlock(uint64_t offset, const char* src, size_t rem);
    void fence(std::string why);

    [[nodiscard]] uint64_t logicalLen() const noexcept { return alignedLen_ + tail_.size(); }

    std::filesystem::path dir_;
    JournalSegmentHeader headerTemplate_;
    size_t segmentBytes_;

    seastar::file file_;
    uint64_t currentSegment_ = 0;
    // O_DIRECT forbids flush-then-continue on an output_stream (the sink asserts
    // on an unaligned resumed offset), so the segment is written by hand: full
    // aligned blocks are final once written; the sub-block remainder is rewritten
    // (padded) on every barrier until it fills. logical size = alignedLen_ + tail_.
    size_t alignment_ = 0;     // disk write DMA alignment of the current file
    uint64_t alignedLen_ = 0;  // durable full-block prefix length (multiple of alignment_)
    std::string tail_;         // logical bytes after alignedLen_ (unpadded; grows past a block between barriers)
    uint64_t fsyncs_ = 0;      // fdatasyncs issued (see fsyncs())
    bool opened_ = false;
    bool fenced_ = false;
    std::string fenceReason_;
};

}  // namespace timestar
