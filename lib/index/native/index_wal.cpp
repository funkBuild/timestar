#include "index_wal.hpp"

#include "memtable.hpp"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <array>
#include <cstring>
#include <filesystem>
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
    file_ = co_await seastar::open_file_dma(path, seastar::open_flags::rw | seastar::open_flags::create);
    fileOffset_ = co_await file_.size();
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
    co_await wal.openFile(walFileName(directory, maxGen));
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
    uint32_t innerLen = static_cast<uint32_t>(4 + 8 + payload.size());  // crc + seq + payload
    encodeFixed32(p, innerLen);
    p += 4;

    // Build crc input: sequence + payload
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

    // DMA write
    auto buf = seastar::temporary_buffer<char>::aligned(4096, record.size());
    std::memcpy(buf.get_write(), record.data(), record.size());
    co_await file_.dma_write(fileOffset_, buf.get(), buf.size());
    fileOffset_ += record.size();

    // fsync for durability
    co_await file_.flush();

    ++sequence_;
}

seastar::future<uint64_t> IndexWAL::replay(MemTable& target) {
    uint64_t recordsReplayed = 0;
    auto fileSize = co_await file_.size();

    if (fileSize == 0) {
        co_return 0;
    }

    // Read the entire WAL file
    auto buf = seastar::temporary_buffer<char>::aligned(4096, fileSize);
    auto bytesRead = co_await file_.dma_read(0, buf.get_write(), buf.size());

    const char* p = buf.get();
    const char* end = p + bytesRead;

    while (p + 4 <= end) {
        uint32_t innerLen = decodeFixed32(p);
        p += 4;

        if (p + innerLen > end) break;  // Truncated record (incomplete write)

        if (innerLen < 12) break;  // Too small: need crc(4) + seq(8) at minimum

        uint32_t storedCrc = decodeFixed32(p);
        p += 4;

        // Verify CRC over sequence + payload
        size_t crcDataLen = innerLen - 4;  // everything after crc
        uint32_t computedCrc = computeCrc32(p, crcDataLen);
        if (storedCrc != computedCrc) {
            break;  // Corrupt record — stop replay
        }

        uint64_t seq = decodeFixed64(p);
        p += 8;

        size_t payloadLen = crcDataLen - 8;
        std::string_view payload(p, payloadLen);
        p += payloadLen;

        try {
            auto batch = IndexWriteBatch::deserializeFrom(payload);
            batch.applyTo(target);
            sequence_ = seq + 1;
            ++recordsReplayed;
        } catch (...) {
            break;  // Corrupt batch — stop replay
        }
    }

    co_return recordsReplayed;
}

seastar::future<std::string> IndexWAL::rotate() {
    auto oldPath = currentPath_;
    co_await file_.flush();
    co_await file_.close();

    ++walGeneration_;
    co_await openFile(walFileName(directory_, walGeneration_));
    fileOffset_ = 0;

    co_return oldPath;
}

seastar::future<> IndexWAL::deleteFile(const std::string& path) {
    if (std::filesystem::exists(path)) {
        co_await seastar::remove_file(path);
    }
}

seastar::future<> IndexWAL::close() {
    co_await file_.flush();
    co_await file_.close();
}

}  // namespace timestar::index
