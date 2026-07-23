#include "journal_writer.hpp"

#include <algorithm>
#include <charconv>
#include <cstring>
#include <exception>
#include <format>
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

    // Enumerate existing segments (blocking fs calls off the reactor).
    std::vector<uint64_t> segments = co_await seastar::async([this] {
        std::vector<uint64_t> found;
        fs::create_directories(dir_);
        for (const auto& entry : fs::directory_iterator(dir_)) {
            if (auto n = parseSegmentFilename(entry.path().filename().string()))
                found.push_back(*n);
        }
        std::sort(found.begin(), found.end());
        return found;
    });

    std::vector<JournalRecord> recovered;
    for (size_t i = 0; i < segments.size(); ++i) {
        const bool isFinal = (i + 1 == segments.size());
        const auto path = dir_ / segmentFilename(segments[i]);
        const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(path);
        auto scan = scanJournalSegment(std::span<const char>(bytes.data(), bytes.size()));

        if (!scan) {
            // No valid header. On the FINAL segment this is a crash before the
            // header became durable -- it holds no records. DELETE it so a later
            // recovery (once newer segments exist) does not see it as a fatal
            // non-final corrupt segment. On a non-final segment it is genuine
            // corruption: fail closed.
            if (isFinal) {
                co_await seastar::remove_file(path.string());
                co_await seastar::sync_directory(dir_.string());  // make the deletion durable
                continue;
            }
            fence("corrupt journal segment header: " + path.string());
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
    co_await startSegment(next);
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
    seastar::file file = co_await seastar::open_file_dma(
        path.string(), seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate);

    file_ = std::move(file);
    alignment_ = file_.disk_write_dma_alignment();
    currentSegment_ = segmentNumber;
    alignedLen_ = 0;
    // fsync the parent directory so the new segment's directory entry is durable
    // before any barrier promises its records are (fdatasync alone syncs file
    // contents, not the directory that names the file).
    co_await seastar::sync_directory(dir_.string());

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

    const std::string bytes = record.encode();
    // Rotate before writing if this record would push the current segment past
    // its target (a record never straddles a segment). A fresh segment holds
    // only the header, so a single record always fits (payloads are bounded well
    // below segmentBytes by the write-batch limit).
    if (logicalLen() + bytes.size() > segmentBytes_ && logicalLen() > JournalSegmentHeader::kEncodedBytes) {
        try {
            co_await sealCurrent();  // flush + truncate + close the full segment
            co_await startSegment(currentSegment_ + 1);
        } catch (const std::exception& e) {
            if (!fenced_)
                fence(std::string("journal rotation failed: ") + e.what());
            throw;
        }
    }

    tail_.append(bytes);  // buffered; made durable (and full blocks finalised) at the next barrier
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
