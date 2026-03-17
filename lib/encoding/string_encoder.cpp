#include "string_encoder.hpp"

#include <zstd.h>

#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

// Maximum decompressed size we'll allocate for string decoding (256MB).
// Prevents OOM from a crafted payload that claims a multi-GB uncompressed size.
static constexpr uint32_t MAX_UNCOMPRESSED_SIZE = 256 * 1024 * 1024;

// Thread-local zstd contexts — eliminates ~200KB alloc per compress and ~130KB
// per decompress.  Seastar's shard-per-core model means one thread per shard,
// so thread_local is safe with no synchronization overhead.
// RAII wrappers ensure cleanup at thread exit.
struct ZstdCCtxDeleter { void operator()(ZSTD_CCtx* p) const { ZSTD_freeCCtx(p); } };
struct ZstdDCtxDeleter { void operator()(ZSTD_DCtx* p) const { ZSTD_freeDCtx(p); } };

static ZSTD_CCtx* getThreadCCtx() {
    static thread_local std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter> ctx(ZSTD_createCCtx());
    return ctx.get();
}

static ZSTD_DCtx* getThreadDCtx() {
    static thread_local std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> ctx(ZSTD_createDCtx());
    return ctx.get();
}

void StringEncoder::writeVarInt(AlignedBuffer& buffer, uint32_t value) {
    while (value >= 0x80) {
        buffer.write(static_cast<uint8_t>((value & 0x7F) | 0x80));
        value >>= 7;
    }
    buffer.write(static_cast<uint8_t>(value & 0x7F));
}

uint32_t StringEncoder::readVarInt(Slice& slice) {
    uint32_t value = 0;
    uint32_t shift = 0;
    uint8_t byte;

    do {
        if (slice.offset >= slice.length_) {
            throw std::runtime_error("Unexpected end of data while reading varint");
        }
        byte = slice.data[slice.offset++];
        value |= (uint32_t(byte & 0x7F) << shift);
        shift += 7;
        if (shift > 28) {
            // 5th byte: only bits 28-31 fit in uint32_t. Reject if higher bits
            // are set (byte & 0x70 contributes bits 32-34) or if continuation bit
            // indicates more bytes.
            if ((byte & 0x80) || (byte & 0x70)) {
                throw std::runtime_error("VarInt too large for uint32_t");
            }
            break;
        }
    } while (byte & 0x80);

    return value;
}

// Shared encode implementation: validates input, builds varint-prefixed
// uncompressed buffer, compresses with zstd, and returns the compressed
// payload with metadata. Both encode() and encodeInto() delegate here.
StringEncoder::CompressedPayload StringEncoder::compressStrings(
    std::span<const std::string> values, int compressionLevel) {
    if (values.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: count " + std::to_string(values.size()) +
                                  " exceeds uint32_t maximum");
    }

    // Calculate uncompressed size with varint length prefixes
    size_t uncompSize = 0;
    for (const auto& str : values) {
        if (str.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::overflow_error("String encoder: individual string length " + std::to_string(str.size()) +
                                      " exceeds uint32_t maximum");
        }
        uint32_t len = static_cast<uint32_t>(str.size());
        size_t varintSize = 1;
        while (len >= 0x80) { varintSize++; len >>= 7; }
        uncompSize += varintSize + str.size();
    }

    if (uncompSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: total uncompressed size " + std::to_string(uncompSize) +
                                  " exceeds uint32_t maximum");
    }

    // Build uncompressed buffer (varint-prefixed strings)
    AlignedBuffer uncompressed(uncompSize);
    size_t writePos = 0;
    for (const auto& str : values) {
        uint32_t len = static_cast<uint32_t>(str.size());
        while (len >= 0x80) {
            uncompressed.data[writePos++] = static_cast<uint8_t>((len & 0x7F) | 0x80);
            len >>= 7;
        }
        uncompressed.data[writePos++] = static_cast<uint8_t>(len & 0x7F);
        std::memcpy(uncompressed.data.data() + writePos, str.data(), str.size());
        writePos += str.size();
    }
    uncompressed.data.resize(writePos);

    // Compress with zstd — reuse thread-local buffer to avoid per-call allocation
    size_t compressedMaxSize = ZSTD_compressBound(uncompressed.data.size());
    static thread_local std::vector<char> tlCompBuf;
    tlCompBuf.resize(compressedMaxSize);
    auto& compressed = tlCompBuf;
    size_t compressedSize = ZSTD_compressCCtx(getThreadCCtx(), compressed.data(), compressedMaxSize,
                                               reinterpret_cast<const char*>(uncompressed.data.data()),
                                               uncompressed.data.size(), compressionLevel);
    if (ZSTD_isError(compressedSize)) {
        throw std::runtime_error(std::string("String encoder: zstd compression failed: ") +
                                 ZSTD_getErrorName(compressedSize));
    }
    if (compressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: compressed size " + std::to_string(compressedSize) +
                                  " exceeds uint32_t maximum");
    }

    compressed.resize(compressedSize);
    return {std::move(compressed),
            static_cast<uint32_t>(uncompressed.data.size()),
            static_cast<uint32_t>(compressedSize),
            static_cast<uint32_t>(values.size())};
}

// Write the standard STRG header into an AlignedBuffer.
static void writeStringHeader(AlignedBuffer& buf, uint32_t uncompSize, uint32_t compSize, uint32_t count) {
    static constexpr uint32_t STRG_MAGIC = 0x53545247;
    buf.write(STRG_MAGIC);
    buf.write(uncompSize);
    buf.write(compSize);
    buf.write(count);
}

AlignedBuffer StringEncoder::encode(std::span<const std::string> values, int compressionLevel) {
    AlignedBuffer result;
    auto payload = compressStrings(values, compressionLevel);
    writeStringHeader(result, payload.uncompressedSize, payload.compressedSize, payload.count);
    result.write_bytes(payload.data.data(), payload.data.size());
    return result;
}

size_t StringEncoder::encodeInto(std::span<const std::string> values, AlignedBuffer& target,
                                  int compressionLevel) {
    const size_t startPos = target.size();

    if (values.empty()) [[unlikely]] {
        writeStringHeader(target, 0, 0, 0);
        return target.size() - startPos;
    }

    auto payload = compressStrings(values, compressionLevel);
    writeStringHeader(target, payload.uncompressedSize, payload.compressedSize, payload.count);
    target.write_bytes(payload.data.data(), payload.data.size());
    return target.size() - startPos;
}

void StringEncoder::decode(AlignedBuffer& encoded, size_t count, std::vector<std::string>& out) {
    if (encoded.size() < 16) {
        throw std::runtime_error("Invalid encoded string buffer: too small for header");
    }

    // Read header
    uint32_t magic;
    std::memcpy(&magic, encoded.data.data(), 4);
    if (magic != 0x53545247) {
        throw std::runtime_error("Invalid magic number in string encoding");
    }

    uint32_t uncompressedSize;
    std::memcpy(&uncompressedSize, encoded.data.data() + 4, 4);

    uint32_t compressedSize;
    std::memcpy(&compressedSize, encoded.data.data() + 8, 4);

    uint32_t storedCount;
    std::memcpy(&storedCount, encoded.data.data() + 12, 4);

    if (storedCount < count) {
        throw std::runtime_error("String block has fewer entries than requested: header=" +
                                 std::to_string(storedCount) + " requested=" + std::to_string(count));
    }

    if (encoded.size() < 16 + compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }

    // Decompress (guard against crafted payloads claiming excessive uncompressed size)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    static thread_local std::vector<uint8_t> tlDecompBuf;
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                      reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize);
        if (ZSTD_isError(ret)) {
            throw std::runtime_error(std::string("Failed to decompress string data: ") + ZSTD_getErrorName(ret));
        }
    }

    // Decode strings
    Slice slice(uncompressed.data(), uncompressedSize);
    out.clear();
    out.reserve(count);

    for (size_t i = 0; i < count && slice.offset < slice.length_; i++) {
        uint32_t strLen = readVarInt(slice);

        if (strLen > slice.length_ - slice.offset) {
            throw std::runtime_error("Invalid string length in encoded data");
        }

        out.emplace_back(reinterpret_cast<const char*>(slice.data + slice.offset), strLen);
        slice.offset += strLen;
    }
}

void StringEncoder::decode(Slice& encoded, size_t count, std::vector<std::string>& out) {
    if (encoded.length_ - encoded.offset < 16) {
        throw std::runtime_error("Invalid encoded string buffer: too small for header");
    }

    // Read header
    uint32_t magic;
    std::memcpy(&magic, encoded.data + encoded.offset, 4);
    if (magic != 0x53545247) {
        throw std::runtime_error("Invalid magic number in string encoding");
    }
    encoded.offset += 4;

    uint32_t uncompressedSize;
    std::memcpy(&uncompressedSize, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    uint32_t compressedSize;
    std::memcpy(&compressedSize, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    uint32_t storedCount;
    std::memcpy(&storedCount, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    if (storedCount < count) {
        throw std::runtime_error("String block has fewer entries than requested: header=" +
                                 std::to_string(storedCount) + " requested=" + std::to_string(count));
    }

    if (encoded.length_ - encoded.offset < compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }

    // Decompress (guard against crafted payloads claiming excessive uncompressed size)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    static thread_local std::vector<uint8_t> tlDecompBuf;
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                      reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
        if (ZSTD_isError(ret)) {
            throw std::runtime_error(std::string("Failed to decompress string data: ") + ZSTD_getErrorName(ret));
        }
    }
    encoded.offset += compressedSize;

    // Decode strings
    Slice uncompSlice(uncompressed.data(), uncompressedSize);
    out.clear();
    out.reserve(count);

    for (size_t i = 0; i < count && uncompSlice.offset < uncompSlice.length_; i++) {
        uint32_t strLen = readVarInt(uncompSlice);

        if (strLen > uncompSlice.length_ - uncompSlice.offset) {
            throw std::runtime_error("Invalid string length in encoded data");
        }

        out.emplace_back(reinterpret_cast<const char*>(uncompSlice.data + uncompSlice.offset), strLen);
        uncompSlice.offset += strLen;
    }
}

void StringEncoder::decode(AlignedBuffer& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                           std::vector<std::string>& out) {
    if (encoded.size() < 16) {
        throw std::runtime_error("Invalid encoded string buffer: too small for header");
    }

    // Read header
    uint32_t magic;
    std::memcpy(&magic, encoded.data.data(), 4);
    if (magic != 0x53545247) {
        throw std::runtime_error("Invalid magic number in string encoding");
    }

    uint32_t uncompressedSize;
    std::memcpy(&uncompressedSize, encoded.data.data() + 4, 4);

    uint32_t compressedSize;
    std::memcpy(&compressedSize, encoded.data.data() + 8, 4);

    uint32_t storedCount;
    std::memcpy(&storedCount, encoded.data.data() + 12, 4);

    if (encoded.size() < 16 + compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }

    // Decompress (zstd doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    static thread_local std::vector<uint8_t> tlDecompBuf;
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                      reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize);
        if (ZSTD_isError(ret)) {
            throw std::runtime_error(std::string("Failed to decompress string data: ") + ZSTD_getErrorName(ret));
        }
    }

    // Decode strings with skip/limit: skip the first skipCount strings without allocating,
    // then collect the next limitCount strings.
    Slice slice(uncompressed.data(), uncompressedSize);
    out.clear();
    out.reserve(limitCount);

    size_t produced = 0;
    for (size_t i = 0; i < totalCount && slice.offset < slice.length_; i++) {
        uint32_t strLen = readVarInt(slice);

        if (strLen > slice.length_ - slice.offset) {
            throw std::runtime_error("Invalid string length in encoded data");
        }

        if (i < skipCount) {
            // Skip: advance pointer without allocating the string
            slice.offset += strLen;
        } else if (produced < limitCount) {
            out.emplace_back(reinterpret_cast<const char*>(slice.data + slice.offset), strLen);
            slice.offset += strLen;
            produced++;
        } else {
            // We have enough strings, stop early
            break;
        }
    }
}

void StringEncoder::decode(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                           std::vector<std::string>& out) {
    if (encoded.length_ - encoded.offset < 16) {
        throw std::runtime_error("Invalid encoded string buffer: too small for header");
    }

    // Read header
    uint32_t magic;
    std::memcpy(&magic, encoded.data + encoded.offset, 4);
    if (magic != 0x53545247) {
        throw std::runtime_error("Invalid magic number in string encoding");
    }
    encoded.offset += 4;

    uint32_t uncompressedSize;
    std::memcpy(&uncompressedSize, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    uint32_t compressedSize;
    std::memcpy(&compressedSize, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    uint32_t storedCount;
    std::memcpy(&storedCount, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    if (encoded.length_ - encoded.offset < compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }

    // Decompress (zstd doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    static thread_local std::vector<uint8_t> tlDecompBuf;
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                      reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
        if (ZSTD_isError(ret)) {
            throw std::runtime_error(std::string("Failed to decompress string data: ") + ZSTD_getErrorName(ret));
        }
    }
    encoded.offset += compressedSize;

    // Decode strings with skip/limit: skip the first skipCount strings without allocating,
    // then collect the next limitCount strings.
    Slice uncompSlice(uncompressed.data(), uncompressedSize);
    out.clear();
    out.reserve(limitCount);

    size_t produced = 0;
    for (size_t i = 0; i < totalCount && uncompSlice.offset < uncompSlice.length_; i++) {
        uint32_t strLen = readVarInt(uncompSlice);

        if (strLen > uncompSlice.length_ - uncompSlice.offset) {
            throw std::runtime_error("Invalid string length in encoded data");
        }

        if (i < skipCount) {
            // Skip: advance pointer without allocating the string
            uncompSlice.offset += strLen;
        } else if (produced < limitCount) {
            out.emplace_back(reinterpret_cast<const char*>(uncompSlice.data + uncompSlice.offset), strLen);
            uncompSlice.offset += strLen;
            produced++;
        } else {
            // We have enough strings, stop early
            break;
        }
    }
}