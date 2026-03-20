#include "string_encoder.hpp"

#include <zstd.h>

#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>

// Maximum decompressed size we'll allocate for string decoding (256MB).
// Prevents OOM from a crafted payload that claims a multi-GB uncompressed size.
static constexpr uint32_t MAX_UNCOMPRESSED_SIZE = 256 * 1024 * 1024;

// Single shared thread-local decompression buffer for all decode functions.
// Previously each function declared its own, consuming 6x memory. Safe because
// Seastar is single-threaded per shard and no function holds a reference across calls.
static thread_local std::vector<uint8_t> tlDecompBuf;

// Validate zstd decompression result: check for errors and size mismatch.
// A truncated or corrupted compressed stream may decompress fewer bytes than
// the header claims, leaving stale data in the reused thread-local buffer.
static void validateDecompress(size_t ret, size_t expectedSize) {
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(std::string("Failed to decompress string data: ") + ZSTD_getErrorName(ret));
    }
    if (ret != expectedSize) {
        throw std::runtime_error("String decoder: decompressed size mismatch (expected " +
                                 std::to_string(expectedSize) + ", got " + std::to_string(ret) + ")");
    }
}

// Thread-local zstd contexts — eliminates ~200KB alloc per compress and ~130KB
// per decompress.  Seastar's shard-per-core model means one thread per shard,
// so thread_local is safe with no synchronization overhead.
// RAII wrappers ensure cleanup at thread exit.
struct ZstdCCtxDeleter {
    void operator()(ZSTD_CCtx* p) const { ZSTD_freeCCtx(p); }
};
struct ZstdDCtxDeleter {
    void operator()(ZSTD_DCtx* p) const { ZSTD_freeDCtx(p); }
};

static ZSTD_CCtx* getThreadCCtx() {
    static thread_local std::unique_ptr<ZSTD_CCtx, ZstdCCtxDeleter> ctx(ZSTD_createCCtx());
    if (!ctx) [[unlikely]]
        throw std::runtime_error("Failed to allocate ZSTD compression context");
    return ctx.get();
}

static ZSTD_DCtx* getThreadDCtx() {
    static thread_local std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter> ctx(ZSTD_createDCtx());
    if (!ctx) [[unlikely]]
        throw std::runtime_error("Failed to allocate ZSTD decompression context");
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
StringEncoder::CompressedPayload StringEncoder::compressStrings(std::span<const std::string> values,
                                                                int compressionLevel) {
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
        while (len >= 0x80) {
            varintSize++;
            len >>= 7;
        }
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

    // Copy instead of move — `compressed` is a reference to thread-local `tlCompBuf`,
    // and moving it would leave the thread-local empty, defeating its reuse purpose.
    return {std::vector<char>(compressed.begin(), compressed.begin() + compressedSize),
            static_cast<uint32_t>(uncompressed.data.size()), static_cast<uint32_t>(compressedSize),
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

size_t StringEncoder::encodeInto(std::span<const std::string> values, AlignedBuffer& target, int compressionLevel) {
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

    // Empty block: nothing to decompress — return immediately.
    if (uncompressedSize == 0) {
        out.clear();
        return;
    }

    // Decompress (guard against crafted payloads claiming excessive uncompressed size)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret =
            ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize);
        validateDecompress(ret, uncompressedSize);
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

    // Empty block: nothing to decompress — advance past compressed data and return.
    if (uncompressedSize == 0) {
        encoded.offset += compressedSize;
        out.clear();
        return;
    }

    // Decompress (guard against crafted payloads claiming excessive uncompressed size)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret =
            ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
        validateDecompress(ret, uncompressedSize);
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

    // Empty block: nothing to decompress — return immediately.
    if (uncompressedSize == 0) {
        out.clear();
        return;
    }

    // Decompress (zstd doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret =
            ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize);
        validateDecompress(ret, uncompressedSize);
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

// ==================== Dictionary Encoding (Phase 3) ====================

static constexpr uint32_t STR2_MAGIC = 0x53545232;  // "STR2"

StringEncoder::Dictionary StringEncoder::buildDictionary(std::span<const std::string> values) {
    Dictionary dict;
    // Use an ordered map to assign stable IDs (insertion order)
    std::unordered_map<std::string_view, uint32_t> seen;
    seen.reserve(std::min(values.size(), MAX_DICT_ENTRIES + 1));

    size_t totalBytes = 4;  // count(4)
    for (const auto& s : values) {
        if (seen.find(s) != seen.end())
            continue;
        if (seen.size() >= MAX_DICT_ENTRIES) {
            return dict;  // valid=false
        }
        // varint size of string length + string data
        size_t varintSize = 1;
        uint32_t len = static_cast<uint32_t>(s.size());
        while (len >= 0x80) {
            varintSize++;
            len >>= 7;
        }
        totalBytes += varintSize + s.size();
        if (totalBytes > MAX_DICT_BYTES) {
            return dict;  // valid=false
        }
        seen[s] = static_cast<uint32_t>(dict.entries.size());
        dict.entries.push_back(s);
    }
    dict.totalBytes = totalBytes;
    dict.valid = true;
    return dict;
}

AlignedBuffer StringEncoder::serializeDictionary(const Dictionary& dict) {
    AlignedBuffer buf;
    buf.write(static_cast<uint32_t>(dict.entries.size()));
    for (const auto& s : dict.entries) {
        writeVarInt(buf, static_cast<uint32_t>(s.size()));
        buf.write_bytes(s.data(), s.size());
    }
    return buf;
}

StringEncoder::Dictionary StringEncoder::deserializeDictionary(Slice& encoded, size_t dictSize) {
    Dictionary dict;
    if (dictSize < 4)
        return dict;

    size_t startOffset = encoded.offset;
    uint32_t count;
    std::memcpy(&count, encoded.data + encoded.offset, 4);
    encoded.offset += 4;

    // Guard against crafted payloads with absurdly large count
    if (count > MAX_DICT_ENTRIES || count > dictSize) {
        throw std::runtime_error("Dictionary entry count too large: " + std::to_string(count));
    }

    dict.entries.reserve(count);
    for (uint32_t i = 0; i < count && encoded.offset < startOffset + dictSize; ++i) {
        uint32_t strLen = readVarInt(encoded);
        if (strLen > encoded.length_ - encoded.offset) {
            throw std::runtime_error("Invalid string length in dictionary");
        }
        dict.entries.emplace_back(reinterpret_cast<const char*>(encoded.data + encoded.offset), strLen);
        encoded.offset += strLen;
    }
    dict.totalBytes = dictSize;
    dict.valid = true;
    return dict;
}

AlignedBuffer StringEncoder::encodeDictionary(std::span<const std::string> values, const Dictionary& dict,
                                              int compressionLevel) {
    // Build string -> ID map from dictionary
    std::unordered_map<std::string_view, uint32_t> idMap;
    idMap.reserve(dict.entries.size());
    for (uint32_t i = 0; i < dict.entries.size(); ++i) {
        idMap[dict.entries[i]] = i;
    }

    // Encode values as varint IDs into an uncompressed buffer
    // Estimate: each ID is 1-3 bytes (varint), so values.size() * 3 is a safe upper bound
    AlignedBuffer uncompressed(values.size() * 3);
    size_t writePos = 0;
    for (const auto& s : values) {
        auto it = idMap.find(s);
        if (it == idMap.end()) {
            throw std::runtime_error("String not found in dictionary during encoding");
        }
        uint32_t id = it->second;
        // Inline varint write for speed
        while (id >= 0x80) {
            uncompressed.data[writePos++] = static_cast<uint8_t>((id & 0x7F) | 0x80);
            id >>= 7;
        }
        uncompressed.data[writePos++] = static_cast<uint8_t>(id & 0x7F);
    }
    uncompressed.data.resize(writePos);

    // Compress the ID stream with zstd
    size_t compressedMaxSize = ZSTD_compressBound(writePos);
    static thread_local std::vector<char> tlCompBuf;
    tlCompBuf.resize(compressedMaxSize);
    size_t compressedSize =
        ZSTD_compressCCtx(getThreadCCtx(), tlCompBuf.data(), compressedMaxSize,
                          reinterpret_cast<const char*>(uncompressed.data.data()), writePos, compressionLevel);
    if (ZSTD_isError(compressedSize)) {
        throw std::runtime_error(std::string("String dict encoder: zstd compression failed: ") +
                                 ZSTD_getErrorName(compressedSize));
    }

    // Write header + compressed data
    AlignedBuffer result;
    result.write(STR2_MAGIC);                             // magic
    result.write(static_cast<uint32_t>(writePos));        // uncompressed size
    result.write(static_cast<uint32_t>(compressedSize));  // compressed size
    result.write(static_cast<uint32_t>(values.size()));   // count
    result.write_bytes(tlCompBuf.data(), compressedSize);
    return result;
}

void StringEncoder::decodeDictionary(Slice& encoded, size_t count, const Dictionary& dict,
                                     std::vector<std::string>& out) {
    if (encoded.length_ - encoded.offset < 16) {
        throw std::runtime_error("Invalid dictionary-encoded string buffer: too small for header");
    }

    uint32_t magic;
    std::memcpy(&magic, encoded.data + encoded.offset, 4);
    if (magic != STR2_MAGIC) {
        throw std::runtime_error("Invalid magic in dictionary-encoded string block");
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
        throw std::runtime_error("Dictionary-encoded string buffer: size mismatch");
    }
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("Dictionary-encoded block uncompressedSize exceeds limit");
    }

    // Empty block: nothing to decompress — advance past compressed data and return.
    if (uncompressedSize == 0) {
        encoded.offset += compressedSize;
        out.clear();
        return;
    }

    // Decompress ID stream
    tlDecompBuf.resize(uncompressedSize);
    size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(tlDecompBuf.data()), uncompressedSize,
                                     reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
    validateDecompress(ret, uncompressedSize);
    encoded.offset += compressedSize;

    // Decode varint IDs and look up dictionary
    Slice idSlice(tlDecompBuf.data(), uncompressedSize);
    out.clear();
    out.reserve(count);
    for (size_t i = 0; i < count && idSlice.offset < idSlice.length_; ++i) {
        uint32_t id = readVarInt(idSlice);
        if (id >= dict.entries.size()) {
            throw std::runtime_error("Dictionary ID " + std::to_string(id) + " out of range (dict size " +
                                     std::to_string(dict.entries.size()) + ")");
        }
        out.push_back(dict.entries[id]);
    }
}

void StringEncoder::decodeDictionary(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                                     const Dictionary& dict, std::vector<std::string>& out) {
    if (encoded.length_ - encoded.offset < 16) {
        throw std::runtime_error("Invalid dictionary-encoded string buffer: too small for header");
    }

    uint32_t magic;
    std::memcpy(&magic, encoded.data + encoded.offset, 4);
    if (magic != STR2_MAGIC) {
        throw std::runtime_error("Invalid magic in dictionary-encoded string block");
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
        throw std::runtime_error("Dictionary-encoded string buffer: size mismatch");
    }
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("Dictionary-encoded block uncompressedSize exceeds limit");
    }

    // Empty block: nothing to decompress — advance past compressed data and return.
    if (uncompressedSize == 0) {
        encoded.offset += compressedSize;
        out.clear();
        return;
    }

    tlDecompBuf.resize(uncompressedSize);
    size_t ret = ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(tlDecompBuf.data()), uncompressedSize,
                                     reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
    validateDecompress(ret, uncompressedSize);
    encoded.offset += compressedSize;

    Slice idSlice(tlDecompBuf.data(), uncompressedSize);
    out.clear();
    out.reserve(limitCount);
    size_t produced = 0;
    for (size_t i = 0; i < totalCount && idSlice.offset < idSlice.length_; ++i) {
        uint32_t id = readVarInt(idSlice);
        if (id >= dict.entries.size()) {
            throw std::runtime_error("Dictionary ID out of range");
        }
        if (i < skipCount)
            continue;
        if (produced < limitCount) {
            out.push_back(dict.entries[id]);
            produced++;
        } else {
            break;
        }
    }
}

bool StringEncoder::isDictionaryEncoded(const uint8_t* data, size_t size) {
    if (size < 4)
        return false;
    uint32_t magic;
    std::memcpy(&magic, data, 4);
    return magic == STR2_MAGIC;
}

bool StringEncoder::isDictionaryEncoded(Slice& slice) {
    if (slice.length_ - slice.offset < 4)
        return false;
    uint32_t magic;
    std::memcpy(&magic, slice.data + slice.offset, 4);
    return magic == STR2_MAGIC;
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

    // Empty block: nothing to decompress — advance past compressed data and return.
    if (uncompressedSize == 0) {
        encoded.offset += compressedSize;
        out.clear();
        return;
    }

    // Decompress (zstd doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    tlDecompBuf.resize(uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret =
            ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), uncompressedSize,
                                reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize);
        validateDecompress(ret, uncompressedSize);
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