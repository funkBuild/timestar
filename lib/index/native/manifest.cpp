#include "manifest.hpp"

#include <algorithm>
#include <cstring>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <stdexcept>
#include <unordered_set>

namespace timestar::index {

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

static uint32_t decodeFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<uint8_t>(p[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
}

static uint64_t decodeFixed64(const char* p) {
    uint64_t r = 0;
    for (int i = 0; i < 8; ++i)
        r |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
    return r;
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
    if (exists) {
        co_await m.recover();
    }

    // Open the file handle for subsequent appends (rw mode for read-modify-write).
    // If the file didn't exist, open_flags::create will create it.
    co_await m.openFileForAppend();

    if (!exists) {
        co_await m.writeSnapshot();
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
    encodeFixed32(frame, static_cast<uint32_t>(record.size()));
    frame.append(record);

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
        std::string record = serializeRemoveFile(fn);
        encodeFixed32(batchFrame, static_cast<uint32_t>(record.size()));
        batchFrame.append(record);
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
    std::string addRecord = serializeAddFile(newFile);
    encodeFixed32(combinedFrame, static_cast<uint32_t>(addRecord.size()));
    combinedFrame.append(addRecord);

    // RemoveFile records
    for (uint64_t fn : removeFileNums) {
        std::string rmRecord = serializeRemoveFile(fn);
        encodeFixed32(combinedFrame, static_cast<uint32_t>(rmRecord.size()));
        combinedFrame.append(rmRecord);
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
    std::string frame;
    encodeFixed32(frame, static_cast<uint32_t>(snapshot.size()));
    frame.append(snapshot);

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

    const char* p = fileBuf.get();
    const char* end = p + fileSize;

    while (p + 4 <= end) {
        uint32_t recordLen = decodeFixed32(p);
        p += 4;
        if (p + recordLen > end)
            break;

        const char* rp = p;
        const char* rend = p + recordLen;
        p += recordLen;

        if (rp >= rend)
            continue;
        auto type = static_cast<RecordType>(*rp);
        ++rp;

        if (type == RecordType::Snapshot) {
            files_.clear();
            if (rp + 12 > rend)
                continue;
            nextFileNumber_ = decodeFixed64(rp);
            rp += 8;
            uint32_t fileCount = decodeFixed32(rp);
            rp += 4;

            for (uint32_t i = 0; i < fileCount; ++i) {
                if (rp + 28 > rend)
                    break;
                SSTableMetadata f;
                f.fileNumber = decodeFixed64(rp);
                rp += 8;
                f.level = static_cast<int>(decodeFixed32(rp));
                rp += 4;
                f.fileSize = decodeFixed64(rp);
                rp += 8;
                f.entryCount = decodeFixed64(rp);
                rp += 8;

                if (rp + 4 > rend)
                    break;
                uint32_t minKeyLen = decodeFixed32(rp);
                rp += 4;
                if (rp + minKeyLen > rend)
                    break;
                f.minKey.assign(rp, minKeyLen);
                rp += minKeyLen;

                if (rp + 4 > rend)
                    break;
                uint32_t maxKeyLen = decodeFixed32(rp);
                rp += 4;
                if (rp + maxKeyLen > rend)
                    break;
                f.maxKey.assign(rp, maxKeyLen);
                rp += maxKeyLen;

                // writeTimestamp (present in all snapshots written by this codebase).
                if (rp + 8 <= rend) {
                    f.writeTimestamp = decodeFixed64(rp);
                    rp += 8;
                }

                files_.push_back(std::move(f));
            }
        } else if (type == RecordType::AddFile) {
            if (rp + 28 > rend)
                continue;
            SSTableMetadata f;
            f.fileNumber = decodeFixed64(rp);
            rp += 8;
            f.level = static_cast<int>(decodeFixed32(rp));
            rp += 4;
            f.fileSize = decodeFixed64(rp);
            rp += 8;
            f.entryCount = decodeFixed64(rp);
            rp += 8;

            if (rp + 4 > rend)
                continue;
            uint32_t minKeyLen = decodeFixed32(rp);
            rp += 4;
            if (rp + minKeyLen > rend)
                continue;
            f.minKey.assign(rp, minKeyLen);
            rp += minKeyLen;

            if (rp + 4 > rend)
                continue;
            uint32_t maxKeyLen = decodeFixed32(rp);
            rp += 4;
            if (rp + maxKeyLen > rend)
                continue;
            f.maxKey.assign(rp, maxKeyLen);
            rp += maxKeyLen;

            // writeTimestamp (v2 extension, optional for backward compat)
            if (rp + 8 <= rend) {
                f.writeTimestamp = decodeFixed64(rp);
                rp += 8;
            }

            uint64_t fn = f.fileNumber;
            files_.push_back(std::move(f));
            if (fn >= nextFileNumber_)
                nextFileNumber_ = fn + 1;
        } else if (type == RecordType::RemoveFile) {
            if (rp + 8 > rend)
                continue;
            uint64_t fn = decodeFixed64(rp);
            std::erase_if(files_, [fn](const SSTableMetadata& ff) { return ff.fileNumber == fn; });
        }
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
