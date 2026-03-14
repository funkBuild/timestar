#include "index_wal.hpp"

#include "memtable.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <stdexcept>

namespace timestar::index {

// Simple CRC32 (same polynomial as zlib)
static constexpr auto makeCrc32Table() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) {
            c = (c >> 1) ^ (0xEDB88320 & (-(c & 1)));
        }
        table[i] = c;
    }
    return table;
}

static constexpr auto crc32_table = makeCrc32Table();

uint32_t IndexWAL::computeCrc32(const char* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = crc32_table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}

static void encodeFixed32(char* buf, uint32_t v) {
    buf[0] = static_cast<char>(v & 0xff);
    buf[1] = static_cast<char>((v >> 8) & 0xff);
    buf[2] = static_cast<char>((v >> 16) & 0xff);
    buf[3] = static_cast<char>((v >> 24) & 0xff);
}

static void encodeFixed64(char* buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
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

std::string IndexWAL::walFileName(const std::string& dir, uint64_t generation) {
    char buf[32];
    snprintf(buf, sizeof(buf), "idx_%06lu.wal", generation);
    return dir + "/" + buf;
}

seastar::future<> IndexWAL::openFile(const std::string& path) {
    currentPath_ = path;
    // Use standard POSIX I/O (via seastar::async) for WAL files.
    // WAL writes are small and sequential — DMA alignment overhead is unnecessary.
    co_return;
}

seastar::future<IndexWAL> IndexWAL::open(std::string directory) {
    std::filesystem::create_directories(directory);

    IndexWAL wal;
    wal.directory_ = directory;

    // Find the highest existing WAL generation
    uint64_t maxGen = 0;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (entry.path().extension() == ".wal") {
            auto name = entry.path().stem().string();
            if (name.starts_with("idx_")) {
                try {
                    uint64_t gen = std::stoull(name.substr(4));
                    maxGen = std::max(maxGen, gen);
                } catch (...) {
                }
            }
        }
    }

    wal.walGeneration_ = maxGen;
    wal.currentPath_ = walFileName(directory, maxGen);
    co_return std::move(wal);
}

seastar::future<> IndexWAL::append(const IndexWriteBatch& batch) {
    // Serialize the batch
    std::string payload;
    batch.serializeTo(payload);

    // Build record: length(4) + crc32(4) + sequence(8) + payload
    size_t recordSize = 4 + 4 + 8 + payload.size();
    std::string record;
    record.resize(recordSize);

    char* p = record.data();
    uint32_t innerLen = static_cast<uint32_t>(4 + 8 + payload.size());
    encodeFixed32(p, innerLen);
    p += 4;

    char seqBuf[8];
    encodeFixed64(seqBuf, sequence_);

    std::string crcInput;
    crcInput.append(seqBuf, 8);
    crcInput.append(payload);
    uint32_t crc = computeCrc32(crcInput.data(), crcInput.size());
    encodeFixed32(p, crc);
    p += 4;

    std::memcpy(p, seqBuf, 8);
    p += 8;
    std::memcpy(p, payload.data(), payload.size());

    // Write using standard POSIX I/O in a Seastar thread
    auto path = currentPath_;
    co_await seastar::async([path, record = std::move(record)] {
        std::ofstream ofs(path, std::ios::binary | std::ios::app);
        ofs.write(record.data(), static_cast<std::streamsize>(record.size()));
        ofs.flush();
    });

    ++sequence_;
}

seastar::future<uint64_t> IndexWAL::replay(MemTable& target) {
    uint64_t recordsReplayed = 0;

    if (!std::filesystem::exists(currentPath_)) {
        co_return 0;
    }

    auto fileSize = std::filesystem::file_size(currentPath_);
    if (fileSize == 0) {
        co_return 0;
    }

    // Read WAL file using standard I/O
    std::string data;
    co_await seastar::async([this, &data, fileSize] {
        data.resize(fileSize);
        std::ifstream ifs(currentPath_, std::ios::binary);
        ifs.read(data.data(), static_cast<std::streamsize>(fileSize));
    });

    const char* p = data.data();
    const char* end = p + data.size();

    while (p + 4 <= end) {
        uint32_t innerLen = decodeFixed32(p);
        p += 4;

        if (p + innerLen > end) break;
        if (innerLen < 12) break;

        uint32_t storedCrc = decodeFixed32(p);
        p += 4;

        size_t crcDataLen = innerLen - 4;
        uint32_t computedCrc = computeCrc32(p, crcDataLen);
        if (storedCrc != computedCrc) break;

        uint64_t seq = decodeFixed64(p);
        p += 8;

        size_t payloadLen = crcDataLen - 8;
        std::string_view payload(p, payloadLen);
        p += payloadLen;

        try {
            auto batchData = IndexWriteBatch::deserializeFrom(payload);
            batchData.applyTo(target);
            sequence_ = seq + 1;
            ++recordsReplayed;
        } catch (...) {
            break;
        }
    }

    co_return recordsReplayed;
}

seastar::future<std::string> IndexWAL::rotate() {
    auto oldPath = currentPath_;
    ++walGeneration_;
    currentPath_ = walFileName(directory_, walGeneration_);
    co_return oldPath;
}

seastar::future<> IndexWAL::deleteFile(const std::string& path) {
    if (std::filesystem::exists(path)) {
        std::filesystem::remove(path);
    }
    co_return;
}

seastar::future<> IndexWAL::close() {
    // No file handles to close — we use open/write/close per append
    co_return;
}

}  // namespace timestar::index
