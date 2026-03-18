#include "manifest.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>

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

seastar::future<Manifest> Manifest::open(std::string directory) {
    std::filesystem::create_directories(directory);

    Manifest m;
    m.directory_ = directory;
    m.manifestPath_ = directory + "/MANIFEST";

    if (std::filesystem::exists(m.manifestPath_)) {
        co_await m.recover();
    } else {
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

seastar::future<> Manifest::appendRecord(const std::string& record) {
    std::string frame;
    encodeFixed32(frame, static_cast<uint32_t>(record.size()));
    frame.append(record);

    auto path = manifestPath_;
    co_await seastar::async([path, frame = std::move(frame)] {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            throw std::runtime_error("Failed to open manifest for append: " + path);
        }
        ssize_t written = ::write(fd, frame.data(), frame.size());
        if (written < 0 || static_cast<size_t>(written) != frame.size()) {
            ::close(fd);
            throw std::runtime_error("Failed to write to manifest: " + path);
        }
        ::fsync(fd);
        ::close(fd);
    });
}

seastar::future<> Manifest::addFile(const SSTableMetadata& info) {
    // Persist to disk FIRST, then update in-memory state.
    co_await appendRecord(serializeAddFile(info));
    files_.push_back(info);
    if (info.fileNumber >= nextFileNumber_) {
        nextFileNumber_ = info.fileNumber + 1;
    }
}

seastar::future<> Manifest::removeFiles(const std::vector<uint64_t>& fileNumbers) {
    if (fileNumbers.empty())
        co_return;

    // Batch all removal records into a single write+fsync to avoid O(N) fsyncs.
    std::string batchFrame;
    for (uint64_t fn : fileNumbers) {
        std::string record = serializeRemoveFile(fn);
        encodeFixed32(batchFrame, static_cast<uint32_t>(record.size()));
        batchFrame.append(record);
    }

    // Persist to disk FIRST, then update in-memory state.
    auto path = manifestPath_;
    co_await seastar::async([path, batchFrame = std::move(batchFrame)] {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            throw std::runtime_error("Failed to open manifest for append: " + path);
        }
        ssize_t written = ::write(fd, batchFrame.data(), batchFrame.size());
        if (written < 0 || static_cast<size_t>(written) != batchFrame.size()) {
            ::close(fd);
            throw std::runtime_error("Failed to write to manifest: " + path);
        }
        ::fsync(fd);
        ::close(fd);
    });

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
    // RemoveFile records.  One write+fsync ensures crash atomicity: either
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
    auto path = manifestPath_;
    co_await seastar::async([path, combinedFrame = std::move(combinedFrame)] {
        int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            throw std::runtime_error("Failed to open manifest for atomic replace: " + path);
        }
        ssize_t written = ::write(fd, combinedFrame.data(), combinedFrame.size());
        if (written < 0 || static_cast<size_t>(written) != combinedFrame.size()) {
            ::close(fd);
            throw std::runtime_error("Failed to write atomic replace to manifest: " + path);
        }
        ::fsync(fd);
        ::close(fd);
    });

    // Update in-memory state AFTER successful persist
    files_.push_back(newFile);
    if (newFile.fileNumber >= nextFileNumber_) {
        nextFileNumber_ = newFile.fileNumber + 1;
    }

    std::unordered_set<uint64_t> toRemove(removeFileNums.begin(), removeFileNums.end());
    auto it = std::remove_if(files_.begin(), files_.end(),
                             [&toRemove](const SSTableMetadata& f) { return toRemove.contains(f.fileNumber); });
    files_.erase(it, files_.end());
}

seastar::future<> Manifest::writeSnapshot() {
    auto snapshot = serializeSnapshot();
    std::string frame;
    encodeFixed32(frame, static_cast<uint32_t>(snapshot.size()));
    frame.append(snapshot);

    auto path = manifestPath_;
    co_await seastar::async([path, frame = std::move(frame)] {
        // Write atomically: write to temp, fsync, then rename
        auto tmpPath = path + ".tmp";
        {
            int fd = ::open(tmpPath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (fd < 0) {
                throw std::runtime_error("Failed to open manifest temp: " + tmpPath);
            }
            ssize_t written = ::write(fd, frame.data(), frame.size());
            if (written < 0 || static_cast<size_t>(written) != frame.size()) {
                ::close(fd);
                throw std::runtime_error("Failed to write manifest snapshot: " + tmpPath);
            }
            ::fsync(fd);
            ::close(fd);
        }
        std::filesystem::rename(tmpPath, path);
        // fsync parent directory so rename is durable
        auto dir = std::filesystem::path(path).parent_path().string();
        int dirfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
        if (dirfd >= 0) {
            ::fsync(dirfd);
            ::close(dirfd);
        }
    });
}

seastar::future<> Manifest::recover() {
    files_.clear();
    auto fileSize = std::filesystem::file_size(manifestPath_);
    if (fileSize == 0)
        co_return;

    std::string data;
    co_await seastar::async([this, &data, fileSize] {
        data.resize(fileSize);
        std::ifstream ifs(manifestPath_, std::ios::binary);
        ifs.read(data.data(), static_cast<std::streamsize>(fileSize));
    });

    const char* p = data.data();
    const char* end = p + data.size();

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

                // writeTimestamp (v2 extension, optional for backward compat)
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
    // No file handles to close — we use open/write/close per operation
    co_return;
}

}  // namespace timestar::index
