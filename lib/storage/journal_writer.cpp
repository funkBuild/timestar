#include "journal_writer.hpp"

#include <algorithm>
#include <charconv>
#include <format>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
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
        const auto path = dir_ / segmentFilename(segments[i]);
        const seastar::sstring bytes = co_await seastar::util::read_entire_file_contiguous(path);
        auto scan = scanJournalSegment(std::span<const char>(bytes.data(), bytes.size()));
        if (!scan) {
            fence("corrupt journal segment header: " + path.string());
            throw std::runtime_error(fenceReason_);
        }
        // Only the final (highest-numbered) segment may be torn -- that is a
        // crash mid-append. A torn tail on an earlier segment means a later
        // segment was created before the earlier one finished: corruption.
        if (scan->torn && i + 1 != segments.size()) {
            fence("torn tail on a non-final journal segment: " + path.string());
            throw std::runtime_error(fenceReason_);
        }
        for (auto& record : scan->records)
            recovered.push_back(std::move(record));
    }

    // New appends go to a fresh segment (recovery stays read-only; a torn tail on
    // the last old segment is simply never appended to and is re-skipped on any
    // future recovery).
    const uint64_t next = segments.empty() ? 0 : segments.back() + 1;
    co_await startSegment(next);
    opened_ = true;
    co_return recovered;
}

seastar::future<> JournalWriter::startSegment(uint64_t segmentNumber) {
    const auto path = dir_ / segmentFilename(segmentNumber);
    seastar::file file = co_await seastar::open_file_dma(
        path.string(), seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate);

    seastar::file_output_stream_options opts;
    opts.buffer_size = 131072;  // 128 KiB, a multiple of the DMA alignment
    opts.preallocation_size = segmentBytes_;
    auto stream = co_await seastar::make_file_output_stream(file, opts);

    JournalSegmentHeader header = headerTemplate_;
    header.segmentNumber = segmentNumber;
    const std::string headerBytes = header.encode();

    file_ = std::move(file);
    out_.emplace(std::move(stream));
    currentSegment_ = segmentNumber;
    currentBytes_ = 0;

    co_await out_->write(headerBytes);
    currentBytes_ += headerBytes.size();
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
    if (currentBytes_ + bytes.size() > segmentBytes_ && currentBytes_ > JournalSegmentHeader::kEncodedBytes) {
        co_await barrier();  // make the closing segment durable before rotating
        if (fenced_)
            throw std::runtime_error("JournalWriter is fenced: " + fenceReason_);
        co_await out_->close();
        co_await startSegment(currentSegment_ + 1);
    }

    try {
        co_await out_->write(bytes);
    } catch (const std::exception& e) {
        fence(std::string("journal append failed: ") + e.what());
        throw;
    }
    currentBytes_ += bytes.size();
}

seastar::future<> JournalWriter::barrier() {
    if (fenced_)
        throw std::runtime_error("JournalWriter is fenced: " + fenceReason_);
    if (!opened_)
        throw std::logic_error("JournalWriter::barrier before open");
    try {
        co_await out_->flush();
        co_await file_.flush();
    } catch (const std::exception& e) {
        fence(std::string("journal barrier failed: ") + e.what());
        throw;
    }
}

seastar::future<> JournalWriter::close() {
    if (!opened_ || !out_)
        co_return;
    // Best-effort durable close; a fenced writer still releases its fd.
    if (!fenced_) {
        try {
            co_await out_->flush();
            co_await file_.flush();
        } catch (...) {
            // fall through to close
        }
    }
    co_await out_->close();
    out_.reset();
    opened_ = false;
}

}  // namespace timestar
