#include "journal_writer.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <exception>
#include <format>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/file.hh>
#include <span>
#include <stdexcept>

namespace fs = std::filesystem;

namespace timestar {

namespace {
constexpr std::string_view kSegmentPrefix = "seg_";
constexpr std::string_view kSegmentSuffix = ".jnl";
}  // namespace

JournalWriter::JournalWriter(fs::path directory, JournalSegmentHeader headerTemplate, size_t segmentBytes)
    : dir_(std::move(directory)), headerTemplate_(headerTemplate), segmentBytes_(segmentBytes) {
    if (segmentBytes_ <= JournalSegmentHeader::kEncodedBytes)
        throw std::invalid_argument("JournalWriter: segmentBytes must exceed the segment header size");
}

std::string JournalWriter::segmentFilename(uint64_t segmentNumber) {
    return std::format("{}{:020}{}", kSegmentPrefix, segmentNumber, kSegmentSuffix);
}

std::optional<uint64_t> JournalWriter::parseSegmentFilename(std::string_view name) {
    if (!name.starts_with(kSegmentPrefix) || !name.ends_with(kSegmentSuffix))
        return std::nullopt;
    const auto digits = name.substr(kSegmentPrefix.size(), name.size() - kSegmentPrefix.size() - kSegmentSuffix.size());
    uint64_t value = 0;
    const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (digits.empty() || result.ec != std::errc{} || result.ptr != digits.data() + digits.size() ||
        name != segmentFilename(value)) {
        return std::nullopt;  // canonical zero-padded spelling only
    }
    return value;
}

void JournalWriter::fence(std::string why) {
    fenced_ = true;
    fenceReason_ = std::move(why);
}

seastar::future<std::vector<JournalRecord>> JournalWriter::open() {
    if (opened_)
        throw std::logic_error("JournalWriter::open called twice");

    // Enumerate existing segments (blocking fs calls off the reactor). This
    // directory is owned exclusively by the journal and has no metadata or
    // temporary-file namespace. Silently ignoring an unrecognised entry is not
    // safe: it may be an acknowledged segment whose filename was damaged or
    // only partially renamed. Preserve every suspicious entry and fence.
    std::vector<uint64_t> segments;
    try {
        segments = co_await seastar::async([this] {
            std::vector<uint64_t> found;
            fs::create_directories(dir_);
            for (const auto& entry : fs::directory_iterator(dir_)) {
                const auto path = entry.path();
                if (!fs::is_regular_file(entry.symlink_status()))
                    throw std::runtime_error("non-regular journal directory entry: " + path.string());
                auto n = parseSegmentFilename(path.filename().string());
                if (!n)
                    throw std::runtime_error("unrecognised journal directory entry: " + path.string());
                found.push_back(*n);
            }
            std::sort(found.begin(), found.end());
            return found;
        });
    } catch (const std::exception& e) {
        fence(std::string("journal directory discovery failed: ") + e.what());
        throw std::runtime_error(fenceReason_);
    }

    // Segment numbers are durable identities, not a wrapping counter. Allowing
    // max+1 to become zero would make startSegment() target an old generation.
    if (!segments.empty() && segments.back() == std::numeric_limits<uint64_t>::max()) {
        fence("journal segment number space exhausted in " + dir_.string());
        throw std::runtime_error(fenceReason_);
    }

    std::vector<JournalRecord> recovered;
    for (size_t i = 0; i < segments.size(); ++i) {
        const bool isFinal = (i + 1 == segments.size());
        const auto path = dir_ / segmentFilename(segments[i]);
        const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(path);
        auto scan = scanJournalSegment(std::span<const char>(bytes.data(), bytes.size()));

        if (!scan) {
            // The only safe header-less segment is a zero-byte FINAL file: that
            // is exactly what startSegment() leaves if the process dies before
            // the first barrier writes its buffered header. A non-empty file may
            // contain a partially-written header or durable records whose header
            // was damaged. Preserve it and fence instead of silently deleting
            // potentially-authoritative bytes.
            if (isFinal && bytes.empty()) {
                co_await seastar::remove_file(path.string());
                co_await seastar::sync_directory(dir_.string());  // make the deletion durable
                continue;
            }
            fence("corrupt journal segment header: " + path.string());
            throw std::runtime_error(fenceReason_);
        }

        // The decoder establishes format integrity; recovery must also establish
        // that the segment belongs to THIS cluster/core and that its embedded
        // sequence agrees with its canonical filename. bootId is intentionally
        // not compared with headerTemplate_: a clean restart creates a new boot
        // id while all prior-boot segments remain authoritative and must replay.
        // It remains part of the on-disk provenance for diagnostics and future
        // manifest-backed boot-transition validation.
        if (scan->header.clusterUuid != headerTemplate_.clusterUuid) {
            fence("journal segment belongs to a different cluster: " + path.string());
            throw std::runtime_error(fenceReason_);
        }
        if (scan->header.coreNumber != headerTemplate_.coreNumber) {
            fence("journal segment belongs to a different core: " + path.string());
            throw std::runtime_error(fenceReason_);
        }
        if (scan->header.segmentNumber != segments[i]) {
            fence("journal segment number does not match filename: " + path.string());
            throw std::runtime_error(fenceReason_);
        }

        if (scan->torn) {
            // A torn tail is only legitimate on the final segment (crash
            // mid-append). REPAIR it by truncating to the durable prefix, so
            // recovery is idempotent: the segment is clean on the next open even
            // after newer segments are created.
            if (!isFinal) {
                fence("torn tail on a non-final journal segment: " + path.string());
                throw std::runtime_error(fenceReason_);
            }
            seastar::file f = co_await seastar::open_file_dma(path.string(), seastar::open_flags::rw);
            co_await f.truncate(scan->durableBytes);
            co_await f.flush();
            co_await f.close();
        }

        for (auto& record : scan->records)
            recovered.push_back(std::move(record));
    }

    // New appends go to a fresh segment (segment numbers are never reused, even
    // if the final old one was deleted above -- back() is the max seen).
    const uint64_t next = segments.empty() ? 0 : segments.back() + 1;
    try {
        co_await startSegment(next);
    } catch (const std::exception& e) {
        if (!fenced_)
            fence(std::string("journal segment creation failed: ") + e.what());
        throw;
    }
    opened_ = true;
    co_return recovered;
}

seastar::future<> JournalWriter::dmaWriteBuffer(uint64_t offset, seastar::temporary_buffer<char> buf) {
    // offset and buf.size() are multiples of alignment_, and buf is allocated at
    // memory_dma_alignment(). On a short write we memmove the remainder to the
    // FRONT of the aligned buffer (rather than advancing the pointer) so the
    // source memory stays aligned -- required when memory alignment exceeds disk
    // alignment (a legal Seastar combination that would otherwise EINVAL).
    uint64_t pos = offset;
    size_t remaining = buf.size();
    while (remaining > 0) {
        const size_t n = co_await file_.dma_write(pos, buf.get(), remaining);
        if (n == 0 || n > remaining || (n & (alignment_ - 1)) != 0)
            throw std::runtime_error("journal dma_write returned an unaligned/short count");
        pos += n;
        remaining -= n;
        if (remaining > 0)
            std::memmove(buf.get_write(), buf.get() + n, remaining);
    }
}

seastar::future<> JournalWriter::dmaWriteAligned(uint64_t offset, const char* src, size_t len) {
    // offset and len are multiples of alignment_; bounce through aligned memory.
    auto buf = seastar::temporary_buffer<char>::aligned(file_.memory_dma_alignment(), len);
    std::memcpy(buf.get_write(), src, len);
    co_await dmaWriteBuffer(offset, std::move(buf));
}

seastar::future<> JournalWriter::writePaddedBlock(uint64_t offset, const char* src, size_t rem) {
    // One block: `rem` (< alignment_) real bytes, zero-padded to alignment_. The
    // padding is transient -- a later full/partial write at `offset` overwrites
    // it -- except on the final block before a seal, which truncate() trims off.
    auto buf = seastar::temporary_buffer<char>::aligned(file_.memory_dma_alignment(), alignment_);
    std::memcpy(buf.get_write(), src, rem);
    std::memset(buf.get_write() + rem, 0, alignment_ - rem);
    co_await dmaWriteBuffer(offset, std::move(buf));
}

seastar::future<> JournalWriter::startSegment(uint64_t segmentNumber) {
    const auto path = dir_ / segmentFilename(segmentNumber);
    // A fresh segment identity must never alias an existing directory entry.
    // In particular, do not use truncate-on-open here: a collision may be the
    // only durable copy of acknowledged records.
    seastar::file file = co_await seastar::open_file_dma(
        path.string(), seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::exclusive);

    // fsync the parent directory so the new segment's directory entry is durable
    // before any barrier promises its records are (fdatasync alone syncs file
    // contents, not the directory that names the file). Close the descriptor on
    // failure; recovery will recognise and remove the resulting empty final
    // segment before advancing past its identity.
    std::exception_ptr directorySyncError;
    try {
        co_await seastar::sync_directory(dir_.string());
    } catch (...) {
        directorySyncError = std::current_exception();
    }
    if (directorySyncError) {
        try {
            co_await file.close();
        } catch (...) {}
        std::rethrow_exception(directorySyncError);
    }

    file_ = std::move(file);
    alignment_ = file_.disk_write_dma_alignment();
    currentSegment_ = segmentNumber;
    alignedLen_ = 0;

    JournalSegmentHeader header = headerTemplate_;
    header.segmentNumber = segmentNumber;
    tail_ = header.encode();
    // NOTE: the header is buffered in tail_, not yet on disk -- the first barrier
    // makes it durable. A crash before that leaves an un-headed (empty) segment;
    // recovery discards an un-headed FINAL segment (see open()).
}

seastar::future<> JournalWriter::append(const JournalRecord& record) {
    if (fenced_)
        throw std::runtime_error("JournalWriter is fenced: " + fenceReason_);
    if (!opened_)
        throw std::logic_error("JournalWriter::append before open");

    // The SIZE without the bytes (debt D-32): this used to encode the record into a
    // temporary and then copy that temporary into `tail_`, i.e. one whole extra copy of
    // every payload -- negligible for a write batch, a second copy of a whole VShard
    // snapshot for a Snapshot record. `encodedBytes()` is the arithmetic the rotation
    // decision actually needs, and the record then streams straight into `tail_`.
    const size_t bytes = record.encodedBytes();
    // Rotate before writing if this record would push the current segment past
    // its target (a record never straddles a segment). A fresh segment holds
    // only the header, so a single record always fits (payloads are bounded well
    // below segmentBytes by the write-batch limit).
    if (logicalLen() + bytes > segmentBytes_ && logicalLen() > JournalSegmentHeader::kEncodedBytes) {
        try {
            if (currentSegment_ == std::numeric_limits<uint64_t>::max())
                throw std::runtime_error("journal segment number space exhausted during rotation");
            co_await sealCurrent();  // flush + truncate + close the full segment
            co_await startSegment(currentSegment_ + 1);
        } catch (const std::exception& e) {
            if (!fenced_)
                fence(std::string("journal rotation failed: ") + e.what());
            throw;
        }
    }

    record.encodeInto(tail_);  // buffered; made durable (and full blocks finalised) at the next barrier
}

seastar::future<> JournalWriter::barrier() {
    if (fenced_)
        throw std::runtime_error("JournalWriter is fenced: " + fenceReason_);
    if (!opened_)
        throw std::logic_error("JournalWriter::barrier before open");
    try {
        if (!tail_.empty()) {
            const size_t full = (tail_.size() / alignment_) * alignment_;
            const size_t rem = tail_.size() - full;
            if (full > 0)
                co_await dmaWriteAligned(alignedLen_, tail_.data(), full);  // these blocks are now final
            if (rem > 0)
                co_await writePaddedBlock(alignedLen_ + full, tail_.data() + full, rem);
            alignedLen_ += full;
            tail_.erase(0, full);  // keep only the unpadded sub-block remainder
        }
        ++fsyncs_;
        co_await file_.flush();  // fdatasync: durable through logicalLen()
    } catch (const std::exception& e) {
        fence(std::string("journal barrier failed: ") + e.what());
        throw;
    }
}

seastar::future<> JournalWriter::sealCurrent() {
    const uint64_t logical = logicalLen();
    co_await barrier();                // flush the tail (as a padded block) durably
    co_await file_.truncate(logical);  // trim the seal padding: sealed segments end at a record boundary
    ++fsyncs_;
    co_await file_.flush();
    co_await file_.close();
}

seastar::future<> JournalWriter::close() {
    if (!opened_)
        co_return;
    // Best-effort durable seal; a fenced writer still releases its fd.
    if (!fenced_) {
        try {
            co_await sealCurrent();
            opened_ = false;
            co_return;
        } catch (...) {
            // fall through to release the fd
        }
    }
    co_await file_.close();
    opened_ = false;
}

}  // namespace timestar
