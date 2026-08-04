#include "manifest.hpp"

#include "crc32.hpp"

#include <fcntl.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/log.hh>
#include <stdexcept>
#include <unordered_set>

namespace timestar::index {

static seastar::logger manifest_log("timestar.manifest");
static constexpr auto OPEN_NOFOLLOW = static_cast<seastar::open_flags>(O_NOFOLLOW);

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

static void requireBytes(const char* p, const char* end, size_t count, std::string_view field) {
    if (p > end || static_cast<size_t>(end - p) < count) {
        throw std::runtime_error("Malformed manifest record: truncated " + std::string(field));
    }
}

static SSTableMetadata decodeFileMetadata(const char*& p, const char* end, bool withTimestamp) {
    requireBytes(p, end, 28, "SSTable metadata");
    SSTableMetadata file;
    file.fileNumber = decodeFixed64(p);
    p += 8;
    const auto level = decodeFixed32(p);
    p += 4;
    file.fileSize = decodeFixed64(p);
    p += 8;
    file.entryCount = decodeFixed64(p);
    p += 8;

    requireBytes(p, end, 4, "minimum-key length");
    const auto minKeyLength = decodeFixed32(p);
    p += 4;
    requireBytes(p, end, minKeyLength, "minimum key");
    file.minKey.assign(p, minKeyLength);
    p += minKeyLength;

    requireBytes(p, end, 4, "maximum-key length");
    const auto maxKeyLength = decodeFixed32(p);
    p += 4;
    requireBytes(p, end, maxKeyLength, "maximum key");
    file.maxKey.assign(p, maxKeyLength);
    p += maxKeyLength;

    if (withTimestamp) {
        requireBytes(p, end, 8, "write timestamp");
        file.writeTimestamp = decodeFixed64(p);
        p += 8;
    }

    if (file.fileNumber == 0 || file.fileNumber == std::numeric_limits<uint64_t>::max()) {
        throw std::runtime_error("Malformed manifest record: invalid SSTable file number");
    }
    if (level > 3) {
        throw std::runtime_error("Malformed manifest record: invalid SSTable level " + std::to_string(level));
    }
    file.level = static_cast<int>(level);
    return file;
}

seastar::future<> Manifest::openFileForAppend() {
    if (fileOpen_) {
        co_await file_.flush();
        co_await file_.close();
        fileOpen_ = false;
    }

    auto [exists, regular] = co_await seastar::async([path = manifestPath_] {
        const auto status = std::filesystem::symlink_status(path);
        return std::make_pair(std::filesystem::exists(status), std::filesystem::is_regular_file(status));
    });
    if (exists && !regular) {
        throw std::runtime_error("Manifest path is not a regular file: " + manifestPath_);
    }

    const auto flags = exists ? seastar::open_flags::rw
                              : seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::exclusive;
    file_ = co_await seastar::open_file_dma(manifestPath_, flags | OPEN_NOFOLLOW);
    dmaAlign_ = file_.disk_write_dma_alignment();
    fileOpen_ = true;

    // Determine the current file size so we append at the right offset
    writeOffset_ = co_await file_.size();
}

seastar::future<Manifest> Manifest::open(std::string directory) {
    const auto createdDirectories = co_await seastar::async([directory] {
        std::vector<std::filesystem::path> missing;
        auto current = std::filesystem::path(directory);
        while (true) {
            const auto status = std::filesystem::symlink_status(current);
            if (std::filesystem::exists(status)) {
                if (!std::filesystem::is_directory(status)) {
                    throw std::runtime_error("Manifest directory ancestor is not a directory: " + current.string());
                }
                break;
            }
            missing.push_back(current);
            auto parent = current.parent_path();
            if (parent.empty())
                parent = ".";
            if (parent == current)
                break;
            current = std::move(parent);
        }
        if (!missing.empty()) {
            std::filesystem::create_directories(directory);
        }
        std::reverse(missing.begin(), missing.end());
        return missing;
    });
    for (const auto& createdDirectory : createdDirectories) {
        const auto parent = createdDirectory.parent_path();
        co_await seastar::sync_directory(parent.empty() ? "." : parent.string());
    }

    Manifest m;
    m.directory_ = directory;
    m.manifestPath_ = directory + "/MANIFEST";

    bool exists = co_await seastar::async([path = m.manifestPath_] {
        const auto status = std::filesystem::symlink_status(path);
        if (std::filesystem::exists(status) && !std::filesystem::is_regular_file(status)) {
            throw std::runtime_error("Manifest path is not a regular file: " + path);
        }
        return std::filesystem::exists(status);
    });
    bool needsRewrite = false;
    if (exists) {
        co_await m.recover();
        // Rewrite a clean snapshot when recovery stopped early at a physically
        // incomplete tail. This discards the
        //    unreachable garbage so future appends stay recoverable.
        needsRewrite = m.recoveryTruncated_;
    }

    // Open the file handle for subsequent appends (rw mode for read-modify-write).
    // If the file didn't exist, open_flags::create will create it.
    co_await m.openFileForAppend();

    if (!exists || needsRewrite) {
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

// Frame one record as [record_len(4)][record_crc(4)][record].
// The CRC covers the record payload only.
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
    if (info.fileNumber == 0 || info.fileNumber == std::numeric_limits<uint64_t>::max()) {
        throw std::overflow_error("Invalid or exhausted manifest SSTable file number");
    }
    if (std::ranges::any_of(files_, [&info](const auto& file) { return file.fileNumber == info.fileNumber; })) {
        throw std::runtime_error("Duplicate live SSTable file number " + std::to_string(info.fileNumber));
    }
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
    if (newFile.fileNumber == 0 || newFile.fileNumber == std::numeric_limits<uint64_t>::max()) {
        throw std::overflow_error("Invalid or exhausted manifest SSTable file number");
    }
    if (std::ranges::any_of(files_, [&newFile](const auto& file) { return file.fileNumber == newFile.fileNumber; })) {
        throw std::runtime_error("Duplicate live SSTable file number " + std::to_string(newFile.fileNumber));
    }
    // Build one append with AddFile first, followed by all RemoveFile records.
    // A torn append can expose only a safe prefix: either no change, the new
    // file plus all old files, or the new file plus a subset of old files.
    std::string combinedFrame;

    // AddFile record
    appendRecordFrame(combinedFrame, serializeAddFile(newFile));

    // RemoveFile records
    for (uint64_t fn : removeFileNums) {
        appendRecordFrame(combinedFrame, serializeRemoveFile(fn));
    }

    // Single append+fsync durability boundary.
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
    // Snapshot files use the v1 CRC-framed format:
    // [magic][version] header followed by the CRC-framed snapshot record.
    std::string frame;
    encodeFixed32(frame, MANIFEST_MAGIC);
    encodeFixed32(frame, MANIFEST_VERSION);
    appendRecordFrame(frame, snapshot);
    // Write atomically: write to temp file via DMA, fsync, then rename.
    auto tmpPath = manifestPath_ + ".tmp";
    const auto staleTempExists = co_await seastar::async([tmpPath] {
        const auto status = std::filesystem::symlink_status(tmpPath);
        if (std::filesystem::exists(status) && !std::filesystem::is_regular_file(status)) {
            throw std::runtime_error("Manifest temporary path is not a regular file: " + tmpPath);
        }
        return std::filesystem::exists(status);
    });
    if (staleTempExists) {
        co_await seastar::remove_file(tmpPath);
        co_await seastar::sync_directory(directory_);
    }
    auto tmpFile = co_await seastar::open_file_dma(tmpPath, seastar::open_flags::wo | seastar::open_flags::create |
                                                                seastar::open_flags::exclusive | OPEN_NOFOLLOW);
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
    recoveryTruncated_ = false;

    auto readFile = co_await seastar::open_file_dma(manifestPath_, seastar::open_flags::ro | OPEN_NOFOLLOW);
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

    if (static_cast<size_t>(end - p) < MANIFEST_HEADER_SIZE || decodeFixed32(p) != MANIFEST_MAGIC) {
        throw std::runtime_error("Manifest has invalid v1 header: " + manifestPath_);
    }
    const uint32_t version = decodeFixed32(p + 4);
    if (version != MANIFEST_VERSION) {
        throw std::runtime_error("Manifest unsupported version " + std::to_string(version) + ": " + manifestPath_);
    }
    p += MANIFEST_HEADER_SIZE;

    size_t completeRecords = 0;
    while (static_cast<size_t>(end - p) >= 8) {
        uint32_t recordLen = decodeFixed32(p);
        uint32_t storedCrc = decodeFixed32(p + 4);
        if (recordLen > static_cast<size_t>(end - p) - 8) {
            recoveryTruncated_ = true;
            break;
        }
        if (recordLen == 0) {
            throw std::runtime_error("Manifest contains an empty record: " + manifestPath_);
        }
        p += 8;
        if (CRC32::compute(p, recordLen) != storedCrc) {
            throw std::runtime_error("Manifest record CRC mismatch at offset " +
                                     std::to_string(static_cast<size_t>(p - 8 - fileBuf.get())) + " in " +
                                     manifestPath_);
        }

        const auto recordType = static_cast<uint8_t>(*p);
        if ((completeRecords == 0 && recordType != RecordType::Snapshot) ||
            (completeRecords != 0 && recordType == RecordType::Snapshot)) {
            throw std::runtime_error("Manifest record sequence does not begin with exactly one snapshot: " +
                                     manifestPath_);
        }
        applyRecord(p, p + recordLen);
        p += recordLen;
        ++completeRecords;
    }
    if (completeRecords == 0) {
        throw std::runtime_error("Manifest contains no complete snapshot: " + manifestPath_);
    }
    if (p < end && !recoveryTruncated_) {
        recoveryTruncated_ = true;
    }
}

// Apply one decoded record (type byte + payload) to the in-memory file set.
void Manifest::applyRecord(const char* rp, const char* rend) {
    requireBytes(rp, rend, 1, "record type");
    auto type = static_cast<RecordType>(*rp);
    ++rp;

    if (type == RecordType::Snapshot) {
        requireBytes(rp, rend, 12, "snapshot header");
        const auto recoveredNextFileNumber = decodeFixed64(rp);
        rp += 8;
        const auto fileCount = decodeFixed32(rp);
        rp += 4;

        auto parseFiles = [rp, rend, fileCount](bool withTimestamp) {
            auto cursor = rp;
            std::vector<SSTableMetadata> parsedFiles;
            parsedFiles.reserve(fileCount);
            std::unordered_set<uint64_t> seen;
            for (uint32_t i = 0; i < fileCount; ++i) {
                auto file = decodeFileMetadata(cursor, rend, withTimestamp);
                if (!seen.insert(file.fileNumber).second) {
                    throw std::runtime_error("Malformed manifest snapshot: duplicate SSTable file number " +
                                             std::to_string(file.fileNumber));
                }
                parsedFiles.push_back(std::move(file));
            }
            if (cursor != rend) {
                throw std::runtime_error("Malformed manifest snapshot: trailing record bytes");
            }
            return parsedFiles;
        };

        auto recoveredFiles = parseFiles(true);

        if (recoveredNextFileNumber == 0) {
            throw std::runtime_error("Malformed manifest snapshot: next file number is zero");
        }
        for (const auto& file : recoveredFiles) {
            if (file.fileNumber >= recoveredNextFileNumber) {
                throw std::runtime_error("Malformed manifest snapshot: next file number does not exceed live file " +
                                         std::to_string(file.fileNumber));
            }
        }
        files_ = std::move(recoveredFiles);
        nextFileNumber_ = recoveredNextFileNumber;
    } else if (type == RecordType::AddFile) {
        auto file = decodeFileMetadata(rp, rend, true);
        if (rp != rend) {
            throw std::runtime_error("Malformed manifest add-file record: trailing bytes");
        }
        if (file.fileNumber < nextFileNumber_) {
            throw std::runtime_error("Manifest reuses or duplicates SSTable file number " +
                                     std::to_string(file.fileNumber));
        }
        const auto fileNumber = file.fileNumber;
        files_.push_back(std::move(file));
        nextFileNumber_ = fileNumber + 1;
    } else if (type == RecordType::RemoveFile) {
        requireBytes(rp, rend, 8, "remove-file number");
        const auto fn = decodeFixed64(rp);
        rp += 8;
        if (rp != rend || fn == 0) {
            throw std::runtime_error("Malformed manifest remove-file record");
        }
        std::erase_if(files_, [fn](const SSTableMetadata& ff) { return ff.fileNumber == fn; });
    } else {
        throw std::runtime_error("Manifest contains unknown record type " +
                                 std::to_string(static_cast<unsigned char>(type)));
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
