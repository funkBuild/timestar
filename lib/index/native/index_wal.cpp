#include "index_wal.hpp"

#include "memtable.hpp"

#include <fcntl.h>
#include <fmt/core.h>
#include <unistd.h>

#include <array>
#include <cinttypes>
#include <cstring>
#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>

namespace timestar::index {

// CRC32C using SSE 4.2 hardware intrinsics (Castagnoli polynomial).
// Falls back to software table for non-x86 platforms.
#if defined(__x86_64__) || defined(_M_X64)
    #include <nmmintrin.h>  // SSE 4.2 CRC32C intrinsics

__attribute__((target("sse4.2"))) uint32_t IndexWAL::computeCrc32(const char* data, size_t len) {
    uint64_t crc = 0xFFFFFFFF;
    const auto* p = reinterpret_cast<const uint8_t*>(data);

    // Process 8 bytes at a time
    while (len >= 8) {
        uint64_t val;
        std::memcpy(&val, p, 8);
        crc = _mm_crc32_u64(crc, val);
        p += 8;
        len -= 8;
    }

    // Process remaining bytes
    while (len-- > 0) {
        crc = _mm_crc32_u8(static_cast<uint32_t>(crc), *p++);
    }

    return static_cast<uint32_t>(crc ^ 0xFFFFFFFF);
}

#else
// Software fallback: CRC32C with Castagnoli polynomial (0x82F63B78 reflected)
// Must match the x86 hardware intrinsics (_mm_crc32) for WAL portability.
static constexpr auto makeCrc32cTable() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t c = i;
        for (int j = 0; j < 8; ++j) {
            c = (c >> 1) ^ (0x82F63B78 & (-(c & 1)));
        }
        table[i] = c;
    }
    return table;
}

static constexpr auto crc32c_table = makeCrc32cTable();

uint32_t IndexWAL::computeCrc32(const char* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < len; ++i) {
        crc = crc32c_table[(crc ^ static_cast<uint8_t>(data[i])) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;
}
#endif

static void encodeFixed32(char* buf, uint32_t v) {
    buf[0] = static_cast<char>(v & 0xff);
    buf[1] = static_cast<char>((v >> 8) & 0xff);
    buf[2] = static_cast<char>((v >> 16) & 0xff);
    buf[3] = static_cast<char>((v >> 24) & 0xff);
}

static void encodeFixed64(char* buf, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
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

std::string IndexWAL::walFileName(const std::string& dir, uint64_t generation) {
    char buf[32];
    snprintf(buf, sizeof(buf), "idx_%06" PRIu64 ".wal", generation);
    return dir + "/" + buf;
}

IndexWAL::~IndexWAL() {
    // Safety net: flush unflushed data via non-blocking POSIX write.
    // close() should be called before destruction in normal shutdown.
    // Avoiding ::fsync() here — it can block for seconds and stall the reactor.
    // The data is durable enough via write() + OS page cache; WAL replay will
    // handle any incomplete records on next startup.
    if ((!buffer_.empty() || !tailBuf_.empty()) && !currentPath_.empty()) {
        int fd = ::open(currentPath_.c_str(), O_WRONLY | O_CREAT, 0644);
        if (fd >= 0) {
            // Seek to dmaWritePos_ so we overwrite any zero-filled region
            // left by truncate, rather than appending after it (O_APPEND
            // would write past the zeros, creating a gap that breaks replay).
            ::lseek(fd, static_cast<off_t>(dmaWritePos_), SEEK_SET);
            if (!tailBuf_.empty()) {
                auto r = ::write(fd, tailBuf_.data(), tailBuf_.size());
                (void)r;
            }
            if (!buffer_.empty()) {
                auto r = ::write(fd, buffer_.data(), buffer_.size());
                (void)r;
            }
            ::close(fd);
        }
    }
    if (walFile_) {
        walFile_->close().handle_exception([](auto) {});
        walFile_.reset();
    }
}

// Open Seastar DMA file handle for the current WAL path.
// Truncates existing file — safe because replay() has already consumed data.
seastar::future<> IndexWAL::openFile() {
    walFile_.emplace(co_await seastar::open_file_dma(
        currentPath_, seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate));
    dmaAlignment_ = walFile_->disk_write_dma_alignment();
    writePos_ = 0;
    dmaWritePos_ = 0;
    buffer_.clear();
    buffer_.reserve(BUFFER_CAPACITY);
    tailBuf_.clear();
}

// Flush the write buffer to disk using DMA.
// Uses a tail buffer to avoid reading existing data back from disk.
// Only writes new aligned blocks; keeps the partial tail in memory.
seastar::future<> IndexWAL::flushBuffer() {
    if (buffer_.empty())
        co_return;

    // Prepend any existing tail (partial block from previous flush)
    std::string combined;
    if (!tailBuf_.empty()) {
        combined.reserve(tailBuf_.size() + buffer_.size());
        combined = std::move(tailBuf_);
        combined.append(buffer_);
    } else {
        combined = std::move(buffer_);
    }
    buffer_.clear();
    tailBuf_.clear();

    const size_t totalSize = combined.size();
    const size_t alignedSize = totalSize & ~(dmaAlignment_ - 1);  // round down
    const size_t tailSize = totalSize - alignedSize;

    // Write full aligned blocks
    if (alignedSize > 0) {
        const size_t paddedSize = alignedSize;  // already aligned
        auto buf = seastar::temporary_buffer<char>::aligned(dmaAlignment_, paddedSize);
        std::memcpy(buf.get_write(), combined.data(), alignedSize);

        // DMA write offset must be aligned — dmaWritePos_ is always aligned
        size_t written = 0;
        while (written < paddedSize) {
            auto n = co_await walFile_->dma_write(dmaWritePos_ + written, buf.get() + written, paddedSize - written);
            if (n == 0)
                throw std::runtime_error("IndexWAL dma_write returned 0");
            written += n;
        }
        dmaWritePos_ += alignedSize;
    }

    // Keep the partial tail in memory for the next flush
    if (tailSize > 0) {
        tailBuf_.assign(combined.data() + alignedSize, tailSize);
    }

    // writePos_ is the logical end of all data written so far.
    // It equals the DMA-written bytes + any pending tail bytes.
    // (Cannot use += totalSize because totalSize includes tail bytes
    // that were already counted in writePos_ during the previous flush.)
    writePos_ = dmaWritePos_ + tailSize;

    // Truncate to the actual DMA-written position (not writePos_, which
    // includes tail bytes still in memory — truncating to writePos_ would
    // create a zero gap from dmaWritePos_ to writePos_ that breaks replay).
    co_await walFile_->truncate(dmaWritePos_);
    co_await walFile_->flush();
}

seastar::future<IndexWAL> IndexWAL::open(std::string directory) {
    // Wrap blocking filesystem calls in seastar::async to avoid reactor stalls.
    auto [dir, allGens] = co_await seastar::async([directory] {
        std::filesystem::create_directories(directory);

        std::vector<uint64_t> gens;
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            if (entry.path().extension() == ".wal") {
                auto name = entry.path().stem().string();
                if (name.starts_with("idx_")) {
                    try {
                        uint64_t g = std::stoull(name.substr(4));
                        gens.push_back(g);
                    } catch (...) {}
                }
            }
        }
        std::sort(gens.begin(), gens.end());
        return std::make_pair(directory, std::move(gens));
    });

    IndexWAL wal;
    wal.directory_ = dir;

    if (allGens.empty()) {
        wal.walGeneration_ = 0;
        wal.currentPath_ = walFileName(dir, 0);
    } else {
        wal.walGeneration_ = allGens.back();
        wal.currentPath_ = walFileName(dir, allGens.back());
        // All generations except the latest are old WAL files to replay first
        for (size_t i = 0; i + 1 < allGens.size(); ++i) {
            wal.oldWalPaths_.push_back(walFileName(dir, allGens[i]));
        }
    }

    co_return std::move(wal);
}

seastar::future<> IndexWAL::append(const IndexWriteBatch& batch) {
    // Lazy file open — after replay() has consumed existing data
    if (!walFile_) {
        co_await openFile();
    }

    // Serialize batch directly into the WAL buffer — no intermediate payload string.
    // Record layout: length(4) + CRC(4) + sequence(8) + payload(variable)

    size_t headerOffset = buffer_.size();
    // Reserve space for header: length(4) + CRC(4) + sequence(8) = 16 bytes
    buffer_.resize(headerOffset + 16);

    // Serialize batch payload directly into buffer_ (appends after the header).
    // This may reallocate buffer_, so all header writes use offsets computed AFTER.
    batch.serializeTo(buffer_);

    // Fill in the header (all pointer arithmetic done after final buffer_ state)
    size_t payloadSize = buffer_.size() - headerOffset - 16;
    uint32_t innerLen = static_cast<uint32_t>(4 + 8 + payloadSize);
    char* hdr = buffer_.data() + headerOffset;

    encodeFixed32(hdr, innerLen);       // record length at offset 0
    encodeFixed64(hdr + 8, sequence_);  // sequence number at offset 8

    // Compute CRC over sequence(8) + payload (starts at offset 8)
    uint32_t crc = computeCrc32(hdr + 8, 8 + payloadSize);
    encodeFixed32(hdr + 4, crc);  // CRC at offset 4

    // Flush when buffer exceeds capacity
    if (buffer_.size() >= BUFFER_CAPACITY) {
        co_await flushBuffer();
    }

    ++sequence_;
}

seastar::future<uint64_t> IndexWAL::replay(MemTable& target) {
    uint64_t recordsReplayed = 0;

    // Flush any buffered writes (including tail) before reading
    if (walFile_) {
        co_await flushBuffer();
        co_await flushTail();
    }

    // Replay older WAL generations first (ascending order) — these are from
    // pre-crash rotations where deleteFile() never completed.
    for (const auto& oldPath : oldWalPaths_) {
        recordsReplayed += co_await replayOneFile(oldPath, target);
    }

    // Replay the current (latest) generation
    recordsReplayed += co_await replayOneFile(currentPath_, target);

    // Delete old WAL files only after ALL replays succeed — if we crash here,
    // the next open() will find them again and replay is idempotent.
    for (const auto& oldPath : oldWalPaths_) {
        co_await deleteFile(oldPath);
    }
    oldWalPaths_.clear();

    co_return recordsReplayed;
}

seastar::future<uint64_t> IndexWAL::replayOneFile(const std::string& path, MemTable& target) {
    uint64_t recordsReplayed = 0;

    if (!co_await seastar::file_exists(path)) {
        co_return 0;
    }

    auto fileSize = co_await seastar::file_size(path);
    if (fileSize == 0) {
        co_return 0;
    }

    // Read WAL file using Seastar DMA I/O
    auto f = co_await seastar::open_file_dma(path, seastar::open_flags::ro);
    auto is = seastar::make_file_input_stream(f);
    auto buf = co_await is.read_exactly(fileSize);
    co_await is.close();
    co_await f.close();

    std::string data(buf.get(), buf.size());
    const char* p = data.data();
    const char* end = p + data.size();

    while (p + 4 <= end) {
        uint32_t innerLen = decodeFixed32(p);
        p += 4;

        if (p + innerLen > end)
            break;
        if (innerLen < 12)
            break;

        uint32_t storedCrc = decodeFixed32(p);
        p += 4;

        size_t crcDataLen = innerLen - 4;
        uint32_t computedCrc = computeCrc32(p, crcDataLen);
        if (storedCrc != computedCrc)
            break;

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
    // Flush everything (including tail) and close current file
    if (walFile_) {
        co_await flushBuffer();
        co_await flushTail();
        co_await walFile_->close();
        walFile_.reset();
    }

    auto oldPath = currentPath_;
    ++walGeneration_;
    currentPath_ = walFileName(directory_, walGeneration_);

    // File opened lazily on next append()

    co_return oldPath;
}

seastar::future<> IndexWAL::deleteFile(const std::string& path) {
    if (co_await seastar::file_exists(path)) {
        co_await seastar::remove_file(path);
    }
}

// Write the remaining tail buffer (partial DMA block) to disk.
// Called before close/rotate to ensure all data is persisted.
seastar::future<> IndexWAL::flushTail() {
    if (tailBuf_.empty())
        co_return;

    const size_t tailSize = tailBuf_.size();
    const size_t paddedSize = (tailSize + dmaAlignment_ - 1) & ~(dmaAlignment_ - 1);

    auto buf = seastar::temporary_buffer<char>::aligned(dmaAlignment_, paddedSize);
    std::memset(buf.get_write(), 0, paddedSize);
    std::memcpy(buf.get_write(), tailBuf_.data(), tailSize);

    size_t written = 0;
    while (written < paddedSize) {
        auto n = co_await walFile_->dma_write(dmaWritePos_ + written, buf.get() + written, paddedSize - written);
        if (n == 0)
            throw std::runtime_error("IndexWAL flushTail dma_write returned 0");
        written += n;
    }
    dmaWritePos_ += paddedSize;
    tailBuf_.clear();

    // Truncate to logical size (remove DMA padding)
    co_await walFile_->truncate(writePos_);
    co_await walFile_->flush();
}

seastar::future<> IndexWAL::close() {
    if (walFile_) {
        co_await flushBuffer();
        co_await flushTail();
        co_await walFile_->close();
        walFile_.reset();
    }
}

}  // namespace timestar::index
