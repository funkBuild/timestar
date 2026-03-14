#include "manifest.hpp"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <algorithm>
#include <cstring>
#include <filesystem>
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
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
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
    for (int i = 0; i < 8; ++i) r |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
    return r;
}

seastar::future<Manifest> Manifest::open(std::string directory) {
    std::filesystem::create_directories(directory);

    Manifest m;
    m.directory_ = directory;
    m.manifestPath_ = directory + "/MANIFEST";

    if (std::filesystem::exists(m.manifestPath_)) {
        m.file_ = co_await seastar::open_file_dma(m.manifestPath_, seastar::open_flags::rw);
        co_await m.recover();
    } else {
        m.file_ = co_await seastar::open_file_dma(m.manifestPath_,
                                                    seastar::open_flags::rw | seastar::open_flags::create);
        // Write initial empty snapshot
        co_await m.writeSnapshot();
    }

    co_return std::move(m);
}

std::string Manifest::serializeSnapshot() const {
    std::string record;
    record.push_back(static_cast<char>(RecordType::Snapshot));

    // next_file_number (8 bytes)
    encodeFixed64(record, nextFileNumber_);

    // file_count (4 bytes)
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
    return record;
}

std::string Manifest::serializeRemoveFile(uint64_t fileNumber) const {
    std::string record;
    record.push_back(static_cast<char>(RecordType::RemoveFile));
    encodeFixed64(record, fileNumber);
    return record;
}

seastar::future<> Manifest::appendRecord(const std::string& record) {
    // Length-prefix the record: [len (4 bytes)] [record]
    std::string frame;
    encodeFixed32(frame, static_cast<uint32_t>(record.size()));
    frame.append(record);

    auto buf = seastar::temporary_buffer<char>::aligned(4096, frame.size());
    std::memcpy(buf.get_write(), frame.data(), frame.size());
    co_await file_.dma_write(fileOffset_, buf.get(), buf.size());
    fileOffset_ += frame.size();
    co_await file_.flush();
}

std::vector<SSTableMetadata> Manifest::filesAtLevel(int level) const {
    std::vector<SSTableMetadata> result;
    for (const auto& f : files_) {
        if (f.level == level) result.push_back(f);
    }
    return result;
}

seastar::future<> Manifest::addFile(const SSTableMetadata& info) {
    files_.push_back(info);
    if (info.fileNumber >= nextFileNumber_) {
        nextFileNumber_ = info.fileNumber + 1;
    }
    co_await appendRecord(serializeAddFile(info));
}

seastar::future<> Manifest::removeFiles(const std::vector<uint64_t>& fileNumbers) {
    for (uint64_t fn : fileNumbers) {
        co_await appendRecord(serializeRemoveFile(fn));
        std::erase_if(files_, [fn](const SSTableMetadata& f) { return f.fileNumber == fn; });
    }
}

seastar::future<> Manifest::writeSnapshot() {
    // Rewrite the manifest file with a fresh snapshot
    co_await file_.close();

    // Write to a temp file, then rename for atomicity
    auto tmpPath = manifestPath_ + ".tmp";
    auto tmpFile =
        co_await seastar::open_file_dma(tmpPath, seastar::open_flags::wo | seastar::open_flags::create |
                                                      seastar::open_flags::truncate);

    auto snapshot = serializeSnapshot();
    std::string frame;
    encodeFixed32(frame, static_cast<uint32_t>(snapshot.size()));
    frame.append(snapshot);

    auto buf = seastar::temporary_buffer<char>::aligned(4096, frame.size());
    std::memcpy(buf.get_write(), frame.data(), frame.size());
    co_await tmpFile.dma_write(0, buf.get(), buf.size());
    co_await tmpFile.truncate(frame.size());
    co_await tmpFile.flush();
    co_await tmpFile.close();

    // Atomic rename
    std::filesystem::rename(tmpPath, manifestPath_);

    // Reopen
    file_ = co_await seastar::open_file_dma(manifestPath_, seastar::open_flags::rw);
    fileOffset_ = frame.size();
}

seastar::future<> Manifest::recover() {
    files_.clear();
    auto fileSize = co_await file_.size();
    if (fileSize == 0) co_return;

    auto buf = seastar::temporary_buffer<char>::aligned(4096, fileSize);
    co_await file_.dma_read(0, buf.get_write(), buf.size());

    const char* p = buf.get();
    const char* end = p + fileSize;

    while (p + 4 <= end) {
        uint32_t recordLen = decodeFixed32(p);
        p += 4;
        if (p + recordLen > end) break;

        const char* rp = p;
        const char* rend = p + recordLen;
        p += recordLen;

        if (rp >= rend) continue;
        auto type = static_cast<RecordType>(*rp);
        ++rp;

        if (type == RecordType::Snapshot) {
            files_.clear();
            if (rp + 12 > rend) continue;
            nextFileNumber_ = decodeFixed64(rp);
            rp += 8;
            uint32_t fileCount = decodeFixed32(rp);
            rp += 4;

            for (uint32_t i = 0; i < fileCount; ++i) {
                if (rp + 28 > rend) break;
                SSTableMetadata f;
                f.fileNumber = decodeFixed64(rp);
                rp += 8;
                f.level = static_cast<int>(decodeFixed32(rp));
                rp += 4;
                f.fileSize = decodeFixed64(rp);
                rp += 8;
                f.entryCount = decodeFixed64(rp);
                rp += 8;

                if (rp + 4 > rend) break;
                uint32_t minKeyLen = decodeFixed32(rp);
                rp += 4;
                if (rp + minKeyLen > rend) break;
                f.minKey.assign(rp, minKeyLen);
                rp += minKeyLen;

                if (rp + 4 > rend) break;
                uint32_t maxKeyLen = decodeFixed32(rp);
                rp += 4;
                if (rp + maxKeyLen > rend) break;
                f.maxKey.assign(rp, maxKeyLen);
                rp += maxKeyLen;

                files_.push_back(std::move(f));
            }
        } else if (type == RecordType::AddFile) {
            if (rp + 28 > rend) continue;
            SSTableMetadata f;
            f.fileNumber = decodeFixed64(rp);
            rp += 8;
            f.level = static_cast<int>(decodeFixed32(rp));
            rp += 4;
            f.fileSize = decodeFixed64(rp);
            rp += 8;
            f.entryCount = decodeFixed64(rp);
            rp += 8;

            if (rp + 4 > rend) continue;
            uint32_t minKeyLen = decodeFixed32(rp);
            rp += 4;
            if (rp + minKeyLen > rend) continue;
            f.minKey.assign(rp, minKeyLen);
            rp += minKeyLen;

            if (rp + 4 > rend) continue;
            uint32_t maxKeyLen = decodeFixed32(rp);
            rp += 4;
            if (rp + maxKeyLen > rend) continue;
            f.maxKey.assign(rp, maxKeyLen);
            rp += maxKeyLen;

            files_.push_back(std::move(f));
            if (f.fileNumber >= nextFileNumber_) nextFileNumber_ = f.fileNumber + 1;
        } else if (type == RecordType::RemoveFile) {
            if (rp + 8 > rend) continue;
            uint64_t fn = decodeFixed64(rp);
            std::erase_if(files_, [fn](const SSTableMetadata& ff) { return ff.fileNumber == fn; });
        }
    }

    fileOffset_ = fileSize;
}

seastar::future<> Manifest::close() {
    co_await file_.flush();
    co_await file_.close();
}

}  // namespace timestar::index
