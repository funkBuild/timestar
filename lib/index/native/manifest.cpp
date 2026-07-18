#include "manifest.hpp"

#include "crc32.hpp"

#include <algorithm>
#include <cstring>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/log.hh>
#include <stdexcept>
#include <unordered_set>

namespace timestar::index {

static seastar::logger manifest_log("timestar.manifest");

static void encodeFixed32(std::string& out, uint32_t v) {
    char buf[4];
    buf[0] = static_cast<char>(v & 0xff);
    buf[1] = static_cast<char>((v >> 8) & 0xff);
    buf[2] = static_cast<char>((v >> 16) & 0xff);
    buf[3] = static_cast<char>((v >> 24) & 0xff);
    out.append(buf, 4);
}

static void encodeFixed64(std::string& out, uint64_t v) {
    char buf[8];
    for (int i = 0; i < 8; ++i)
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    out.append(buf, 8);
}

seastar::future<> Manifest::openFileForAppend() {
    if (fileOpen_) {
        co_await file_.flush();
        co_await file_.close();
        fileOpen_ = false;
    }

    file_ = co_await seastar::open_file_dma(manifestPath_, seastar::open_flags::rw | seastar::open_flags::create);
    dmaAlign_ = file_.disk_write_dma_alignment();
    fileOpen_ = true;

    // Determine the current file size so we append at the right offset
    writeOffset_ = co_await file_.size();
}

seastar::future<Manifest> Manifest::open(std::string directory) {
    co_await seastar::recursive_touch_directory(directory);

    Manifest m;
    m.directory_ = directory;
    m.manifestPath_ = directory + "/MANIFEST";

    bool exists = co_await seastar::file_exists(m.manifestPath_);
    bool needsRewrite = false;
    if (exists) {
        co_await m.recover();
        // Rewrite a clean v2 snapshot when:
        //  - the manifest is legacy (v1, no CRC framing): upgrade in place, or
        //  - recovery stopped early (torn tail / corrupt record): discard the
        //    unreachable garbage so future appends stay recoverable.
        // Both use the atomic temp-file + rename path in writeSnapshot().
        needsRewrite = !m.crcFraming_ || m.recoveryTruncated_;
    }

    // Open the file handle for subsequent appends (rw mode for read-modify-write).
    // If the file didn't exist, open_flags::create will create it.
    co_await m.openFileForAppend();

    if (!exists || needsRewrite) {
        if (exists && !m.crcFraming_) {
            manifest_log.info("Upgrading legacy manifest to CRC-framed format (v2): {}", m.manifestPath_);
        }
        co_await m.writeSnapshot();
        m.recoveryTruncated_ = false;
    }

    co_return std::move(m);
}

std::vector<SSTableMetadata> Manifest::filesAtLevel(int level) const {
    std::vector<SSTableMetadata> result;
    for (const auto& f : files_) {
        if (f.level == level)
            result.push_back(f);
    }
    return result;
}

std::string Manifest::serializeSnapshot() const {
    std::string record;
    record.push_back(static_cast<char>(RecordType::Snapshot));
    encodeFixed64(record, nextFileNumber_);
    encodeFixed32(record, static_cast<uint32_t>(files_.size()));

    for (const auto& f : files_) {
        encodeFixed64(record, f.fileNumber);
        encodeFixed32(record, static_cast<uint32_t>(f.level));
        encodeFixed64(record, f.fileSize);
        encodeFixed64(record, f.entryCount);
        encodeFixed32(record, static_cast<uint32_t>(f.minKey.size()));
        record.append(f.minKey);
        encodeFixed32(record, static_cast<uint32_t>(f.maxKey.size()));
        record.append(f.maxKey);
        encodeFixed64(record, f.writeTimestamp);
    }

    return record;
}

std::string Manifest::serializeAddFile(const SSTableMetadata& info) const {
    std::string record;
    record.push_back(static_cast<char>(RecordType::AddFile));
    encodeFixed64(record, info.fileNumber);
    encodeFixed32(record, static_cast<uint32_t>(info.level));
    encodeFixed64(record, info.fileSize);
    encodeFixed64(record, info.entryCount);
    encodeFixed32(record, static_cast<uint32_t>(info.minKey.size()));
    record.append(info.minKey);
    encodeFixed32(record, static_cast<uint32_t>(info.maxKey.size()));
    record.append(info.maxKey);
    encodeFixed64(record, info.writeTimestamp);
    return record;
}

std::string Manifest::serializeRemoveFile(uint64_t fileNumber) const {
    std::string record;
    record.push_back(static_cast<char>(RecordType::RemoveFile));
    encodeFixed64(record, fileNumber);
    return record;
}

// Frame one record in the v2 format: [record_len(4)][record_crc(4)][record].
// The CRC covers the record payload only. Appends are always v2 — open()
// upgrades legacy manifests before any append can happen.
void Manifest::appendRecordFrame(std::string& out, const std::string& record) {
    encodeFixed32(out, static_cast<uint32_t>(record.size()));
    encodeFixed32(out, CRC32::compute(record.data(), record.size()));
    out.append(record);
}

seastar::future<> Manifest::appendFrame(const std::string& frame) {
    if (!fileOpen_) {
        co_await openFileForAppend();
    }

    // writeOffset_ is the logical end of data. It may not be DMA-aligned.
    // DMA writes require aligned offset, aligned buffer, and aligned size.
    // Strategy: read-modify-write the partial tail block if writeOffset_ is unaligned,
    // then append the new data, pad to alignment, and write the combined block.

    const uint64_t alignedStart = writeOffset_ & ~(static_cast<uint64_t>(dmaAlign_) - 1);
    const size_t tailBytes = static_cast<size_t>(writeOffset_ - alignedStart);
    const size_t totalBytes = tailBytes + frame.size();
    const size_t paddedSize = (totalBytes + dmaAlign_ - 1) & ~(dmaAlign_ - 1);

    auto buf = seastar::temporary_buffer<char>::aligned(dmaAlign_, paddedSize);
    std::memset(buf.get_write(), 0, paddedSize);

    // If there's a partial tail from a previous write, read it back
    if (tailBytes > 0) {
        auto tailBuf = co_await file_.dma_read<char>(alignedStart, dmaAlign_);
        std::memcpy(buf.get_write(), tailBuf.get(), tailBytes);
    }

    // Append the new frame data after the tail
    std::memcpy(buf.get_write() + tailBytes, frame.data(), frame.size());

    // DMA write the combined block
    size_t written = 0;
    while (written < paddedSize) {
        auto n = co_await file_.dma_write(alignedStart + written, buf.get() + written, paddedSize - written);
        if (n == 0)
            throw std::runtime_error("Manifest dma_write returned 0: " + manifestPath_);
        written += n;
    }

    // Advance logical offset by the actual data written (not padding)
    writeOffset_ += frame.size();

    // Truncate to the exact logical size so recovery doesn't see zero-pad bytes
    // as spurious records. truncate() does not require DMA alignment.
    co_await file_.truncate(writeOffset_);

    // Flush to ensure data reaches stable storage (equivalent to fsync)
    co_await file_.flush();
}

seastar::future<> Manifest::addFile(const SSTableMetadata& info) {
    std::string record = serializeAddFile(info);
    std::string frame;
    appendRecordFrame(frame, record);

    // Persist to disk FIRST, then update in-memory state.
    co_await appendFrame(frame);
    files_.push_back(info);
    if (info.fileNumber >= nextFileNumber_) {
        nextFileNumber_ = info.fileNumber + 1;
    }
}

seastar::future<> Manifest::removeFiles(const std::vector<uint64_t>& fileNumbers) {
    if (fileNumbers.empty())
        co_return;

    // Batch all removal records into a single write+fsync
    std::string batchFrame;
    for (uint64_t fn : fileNumbers) {
        appendRecordFrame(batchFrame, serializeRemoveFile(fn));
    }

    // Persist to disk FIRST, then update in-memory state.
    co_await appendFrame(batchFrame);

    // Update in-memory state AFTER successful persist
    std::unordered_set<uint64_t> toRemove(fileNumbers.begin(), fileNumbers.end());
    // Use explicit loop instead of std::erase_if with lambda to avoid
    // GCC 14 coroutine frame + std::reference_wrapper interaction bug.
    auto it = std::remove_if(files_.begin(), files_.end(),
                             [&toRemove](const SSTableMetadata& f) { return toRemove.contains(f.fileNumber); });
    files_.erase(it, files_.end());
}

seastar::future<> Manifest::atomicReplaceFiles(const SSTableMetadata& newFile,
                                               const std::vector<uint64_t>& removeFileNums) {
    // Build a single buffer containing the AddFile record followed by all
    // RemoveFile records. One write+fsync ensures crash atomicity: either
    // all records are persisted or none are.
    std::string combinedFrame;

    // AddFile record
    appendRecordFrame(combinedFrame, serializeAddFile(newFile));

    // RemoveFile records
    for (uint64_t fn : removeFileNums) {
        appendRecordFrame(combinedFrame, serializeRemoveFile(fn));
    }

    // Single write+fsync
    co_await appendFrame(combinedFrame);

    // Update in-memory state AFTER successful persist
    files_.push_back(newFile);
    if (newFile.fileNumber >= nextFileNumber_) {
        nextFileNumber_ = newFile.fileNumber + 1;
    }

    std::unordered_set<uint64_t> toRemove(removeFileNums.begin(), removeFileNums.end());
    auto newFn = newFile.fileNumber;
    auto it = std::remove_if(files_.begin(), files_.end(), [&toRemove, newFn](const SSTableMetadata& f) {
        return f.fileNumber != newFn && toRemove.contains(f.fileNumber);
    });
    files_.erase(it, files_.end());
}

seastar::future<> Manifest::writeSnapshot() {
    auto snapshot = serializeSnapshot();
    // Snapshot files are always written in the v2 CRC-framed format:
    // [magic][version] header followed by the CRC-framed snapshot record.
    std::string frame;
    encodeFixed32(frame, MANIFEST_MAGIC);
    encodeFixed32(frame, MANIFEST_VERSION);
    appendRecordFrame(frame, snapshot);
    crcFraming_ = true;

    // Write atomically: write to temp file via DMA, fsync, then rename.
    auto tmpPath = manifestPath_ + ".tmp";
    auto tmpFile = co_await seastar::open_file_dma(
        tmpPath, seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate);
    auto tmpAlign = tmpFile.disk_write_dma_alignment();

    std::exception_ptr err;
    try {
        const size_t dataSize = frame.size();
        const size_t paddedSize = (dataSize + tmpAlign - 1) & ~(tmpAlign - 1);

        auto buf = seastar::temporary_buffer<char>::aligned(tmpAlign, paddedSize);
        std::memset(buf.get_write(), 0, paddedSize);
        std::memcpy(buf.get_write(), frame.data(), dataSize);

        size_t written = 0;
        while (written < paddedSize) {
            auto n = co_await tmpFile.dma_write(written, buf.get() + written, paddedSize - written);
            if (n == 0)
                throw std::runtime_error("Manifest snapshot dma_write returned 0: " + tmpPath);
            written += n;
        }

        // Truncate to actual data size (remove DMA padding zeros)
        if (paddedSize != dataSize) {
            co_await tmpFile.truncate(dataSize);
        }
        co_await tmpFile.flush();
    } catch (...) {
        err = std::current_exception();
    }

    co_await tmpFile.close();
    if (err)
        std::rethrow_exception(err);

    // Atomic rename: temp -> manifest
    co_await seastar::rename_file(tmpPath, manifestPath_);

    // fsync parent directory so rename is durable
    co_await seastar::sync_directory(directory_);

    // Reopen the file handle since the old handle pointed to the pre-rename inode
    if (fileOpen_) {
        co_await file_.close();
        fileOpen_ = false;
    }
    co_await openFileForAppend();
}

seastar::future<> Manifest::recover() {
    files_.clear();
    crcFraming_ = false;
    recoveryTruncated_ = false;

    auto readFile = co_await seastar::open_file_dma(manifestPath_, seastar::open_flags::ro);
    auto fileSize = co_await readFile.size();
    if (fileSize == 0) {
        co_await readFile.close();
        co_return;
    }

    // Read the entire manifest file using DMA bulk read.
    // dma_read_bulk handles alignment internally and returns exactly fileSize bytes.
    auto fileBuf = co_await readFile.dma_read_bulk<char>(0, fileSize);
    co_await readFile.close();

    const auto decoded = decodeManifest(std::string_view(fileBuf.get(), static_cast<size_t>(fileSize)));
    if (decoded.status == ManifestDecodeStatus::Fatal) {
        throw std::runtime_error("Manifest is invalid at offset " + std::to_string(decoded.issueOffset) + ": " +
                                 decoded.issue + ": " + manifestPath_);
    }

    crcFraming_ = decoded.format == ManifestDiskFormat::CrcV2;
    recoveryTruncated_ = decoded.status == ManifestDecodeStatus::RecoverableTail;
    nextFileNumber_ = decoded.nextFileNumber;
    files_.reserve(decoded.files.size());
    for (const auto& decodedFile : decoded.files) {
        SSTableMetadata file;
        file.fileNumber = decodedFile.fileNumber;
        file.entryCount = decodedFile.entryCount;
        file.fileSize = decodedFile.fileSize;
        file.minKey = decodedFile.minKey;
        file.maxKey = decodedFile.maxKey;
        file.level = decodedFile.level;
        file.writeTimestamp = decodedFile.writeTimestamp;
        files_.push_back(std::move(file));
    }

    if (recoveryTruncated_) {
        manifest_log.error("Manifest recovery stopped at offset {} in {}: {}", decoded.issueOffset, manifestPath_,
                           decoded.issue);
    }
}

seastar::future<> Manifest::close() {
    if (fileOpen_) {
        co_await file_.flush();
        co_await file_.close();
        fileOpen_ = false;
    }
}

}  // namespace timestar::index
