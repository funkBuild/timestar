#include "string_encoder.hpp"

#include <zstd.h>

#include <cassert>
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
// zstd compression output staging, shared by compressStrings() and
// encodeDictionaryInto() (never live simultaneously — a block is either raw or
// dict-encoded). Previously two separate function-local thread_locals.
static thread_local std::vector<char> tlCompBuf;
// Uncompressed varint-prefixed input staging for compressStrings(). Reused across
// calls to avoid a fresh 4KB-page-aligned AlignedBuffer per encode; zstd reads a
// raw pointer, so no DMA alignment is required.
static thread_local std::vector<uint8_t> tlUncompBuf;

// One greenfield v1 string-block format with two encoding variants. The final
// byte is the format version; the middle bytes identify the active encoding.
// Both variants have the same 16-byte header and are updated in place together.
static constexpr uint32_t kRawStringV1Magic = 0x31525453;         // "STR1"
static constexpr uint32_t kDictionaryStringV1Magic = 0x31445453;  // "STD1"
static constexpr size_t kStringHeaderBytes = 4 * sizeof(uint32_t);

struct StringBlockHeader {
    uint32_t uncompressedSize = 0;
    uint32_t compressedSize = 0;
    uint32_t count = 0;
};

static void writeStringHeader(AlignedBuffer& buffer, uint32_t magic, uint32_t uncompressedSize, uint32_t compressedSize,
                              uint32_t count) {
    buffer.write(magic);
    buffer.write(uncompressedSize);
    buffer.write(compressedSize);
    buffer.write(count);
}

static StringBlockHeader readStringHeader(Slice& encoded, uint32_t expectedMagic) {
    if (encoded.offset > encoded.length_)
        throw std::runtime_error("String decoder: slice offset past end");
    if (encoded.length_ - encoded.offset < kStringHeaderBytes)
        throw std::runtime_error("Invalid encoded string buffer: too small for v1 header");

    uint32_t magic = 0;
    StringBlockHeader header;
    std::memcpy(&magic, encoded.data + encoded.offset, sizeof(magic));
    std::memcpy(&header.uncompressedSize, encoded.data + encoded.offset + 4, sizeof(header.uncompressedSize));
    std::memcpy(&header.compressedSize, encoded.data + encoded.offset + 8, sizeof(header.compressedSize));
    std::memcpy(&header.count, encoded.data + encoded.offset + 12, sizeof(header.count));
    encoded.offset += kStringHeaderBytes;

    if (magic != expectedMagic) {
        if (expectedMagic == kRawStringV1Magic && magic == kDictionaryStringV1Magic) {
            throw std::runtime_error("Dictionary-encoded v1 string block has no dictionary in its TSM index entry");
        }
        throw std::runtime_error("Invalid or unsupported v1 string block marker");
    }
    if (header.compressedSize > encoded.length_ - encoded.offset)
        throw std::runtime_error("Invalid encoded string buffer: size mismatch");
    if (header.uncompressedSize > MAX_UNCOMPRESSED_SIZE)
        throw std::runtime_error("String block uncompressed size exceeds limit");
    return header;
}

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

    // Build uncompressed buffer (varint-prefixed strings) into reused thread-local
    // staging instead of a fresh page-aligned AlignedBuffer per call.
    std::vector<uint8_t>& uncompressed = tlUncompBuf;
    uncompressed.resize(uncompSize);
    size_t writePos = 0;
    for (const auto& str : values) {
        uint32_t len = static_cast<uint32_t>(str.size());
        while (len >= 0x80) {
            uncompressed[writePos++] = static_cast<uint8_t>((len & 0x7F) | 0x80);
            len >>= 7;
        }
        uncompressed[writePos++] = static_cast<uint8_t>(len & 0x7F);
        std::memcpy(uncompressed.data() + writePos, str.data(), str.size());
        writePos += str.size();
    }
    if (writePos != uncompSize) [[unlikely]] {
        throw std::runtime_error("String encoder: varint size mismatch (wrote " + std::to_string(writePos) +
                                 " expected " + std::to_string(uncompSize) + ")");
    }

    // Compress with zstd — reuse thread-local buffer to avoid per-call allocation
    size_t compressedMaxSize = ZSTD_compressBound(writePos);
    tlCompBuf.resize(compressedMaxSize);
    auto& compressed = tlCompBuf;
    size_t compressedSize =
        ZSTD_compressCCtx(getThreadCCtx(), compressed.data(), compressedMaxSize,
                          reinterpret_cast<const char*>(uncompressed.data()), writePos, compressionLevel);
    if (ZSTD_isError(compressedSize)) {
        throw std::runtime_error(std::string("String encoder: zstd compression failed: ") +
                                 ZSTD_getErrorName(compressedSize));
    }
    if (compressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: compressed size " + std::to_string(compressedSize) +
                                  " exceeds uint32_t maximum");
    }

    // Return a pointer into the thread-local `tlCompBuf` (no per-block heap
    // allocation or payload copy — callers write the bytes straight out).
    // Never std::move the thread-local: that would leave it empty and defeat
    // its reuse purpose.
    return {compressed.data(), static_cast<uint32_t>(writePos), static_cast<uint32_t>(compressedSize),
            static_cast<uint32_t>(values.size())};
}

AlignedBuffer StringEncoder::encode(std::span<const std::string> values, int compressionLevel) {
    AlignedBuffer result;
    if (values.empty()) [[unlikely]] {
        writeStringHeader(result, kRawStringV1Magic, 0, 0, 0);
        return result;
    }
    auto payload = compressStrings(values, compressionLevel);
    writeStringHeader(result, kRawStringV1Magic, payload.uncompressedSize, payload.compressedSize, payload.count);
    result.write_bytes(payload.data, payload.compressedSize);
    return result;
}

size_t StringEncoder::encodeInto(std::span<const std::string> values, AlignedBuffer& target, int compressionLevel) {
    const size_t startPos = target.size();

    if (values.empty()) [[unlikely]] {
        writeStringHeader(target, kRawStringV1Magic, 0, 0, 0);
        return target.size() - startPos;
    }

    auto payload = compressStrings(values, compressionLevel);
    writeStringHeader(target, kRawStringV1Magic, payload.uncompressedSize, payload.compressedSize, payload.count);
    target.write_bytes(payload.data, payload.compressedSize);
    return target.size() - startPos;
}

// Test-only convenience overload: wrap the buffer in a Slice and delegate.
void StringEncoder::decode(AlignedBuffer& encoded, size_t count, std::vector<std::string>& out) {
    Slice slice(encoded.data.data(), encoded.size());
    decode(slice, count, out);
}

void StringEncoder::decode(Slice& encoded, size_t count, std::vector<std::string>& out) {
    if (encoded.offset > encoded.length_ || encoded.length_ - encoded.offset < kStringHeaderBytes)
        throw std::runtime_error("Invalid encoded string buffer: too small for v1 header");
    uint32_t storedCount = 0;
    std::memcpy(&storedCount, encoded.data + encoded.offset + 12, sizeof(storedCount));
    if (count > storedCount)
        throw std::runtime_error("String block has fewer entries than requested");
    out.clear();
    if (decode(encoded, storedCount, 0, count, out) != count)
        throw std::runtime_error("String block contains fewer values than its v1 header declares");
}

// ==================== Dictionary Encoding (Phase 3) ====================

StringEncoder::Dictionary StringEncoder::buildDictionary(std::span<const std::string> values) {
    Dictionary dict;
    dict.entries.reserve(MAX_DICT_ENTRIES);
    // Use owned string keys to avoid dangling string_view pointers if entries were to reallocate.
    std::unordered_map<std::string, uint32_t> seen;
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
        dict.entries.push_back(s);
        seen[s] = static_cast<uint32_t>(dict.entries.size() - 1);
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
    if (dictSize > encoded.bytesLeft()) {
        throw std::runtime_error("Dictionary extends beyond its containing slice");
    }
    Slice dictSlice = encoded.getSlice(dictSize);
    if (dictSize < sizeof(uint32_t))
        return dict;

    uint32_t count = dictSlice.read<uint32_t>();

    // Guard against crafted payloads with absurdly large count
    if (count > MAX_DICT_ENTRIES || count > dictSize) {
        throw std::runtime_error("Dictionary entry count too large: " + std::to_string(count));
    }

    dict.entries.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t strLen = readVarInt(dictSlice);
        if (strLen > dictSlice.bytesLeft()) {
            throw std::runtime_error("Invalid string length in dictionary");
        }
        dict.entries.emplace_back(reinterpret_cast<const char*>(dictSlice.data + dictSlice.offset), strLen);
        dictSlice.offset += strLen;
    }
    if (dictSlice.bytesLeft() != 0) {
        throw std::runtime_error("Dictionary has trailing bytes");
    }
    dict.totalBytes = dictSize;
    dict.valid = true;
    return dict;
}

size_t StringEncoder::encodeDictionaryInto(std::span<const std::string> values, const Dictionary& dict,
                                           AlignedBuffer& target, int compressionLevel) {
    if (values.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::runtime_error("encodeDictionary: values.size() (" + std::to_string(values.size()) +
                                 ") exceeds uint32_t max");
    }
    const size_t startPos = target.size();

    // Build string -> ID map from dictionary
    std::unordered_map<std::string_view, uint32_t> idMap;
    idMap.reserve(dict.entries.size());
    for (uint32_t i = 0; i < dict.entries.size(); ++i) {
        idMap[dict.entries[i]] = i;
    }

    // Encode values as varint IDs into the reused thread-local staging buffer
    // (no fresh page-aligned AlignedBuffer per block).
    // A uint32_t varint is at most 5 bytes, so values.size() * 5 is the safe upper bound.
    const size_t maxBufSize = values.size() * 5;
    std::vector<uint8_t>& uncompressed = tlUncompBuf;
    uncompressed.resize(maxBufSize);
    size_t writePos = 0;
    for (const auto& s : values) {
        auto it = idMap.find(s);
        if (it == idMap.end()) {
            throw std::runtime_error("String not found in dictionary during encoding");
        }
        uint32_t id = it->second;
        // Inline varint write for speed
        while (id >= 0x80) {
            uncompressed[writePos++] = static_cast<uint8_t>((id & 0x7F) | 0x80);
            id >>= 7;
        }
        uncompressed[writePos++] = static_cast<uint8_t>(id & 0x7F);
    }
    assert(writePos <= maxBufSize);

    // Compress the ID stream with zstd
    size_t compressedMaxSize = ZSTD_compressBound(writePos);
    tlCompBuf.resize(compressedMaxSize);
    size_t compressedSize =
        ZSTD_compressCCtx(getThreadCCtx(), tlCompBuf.data(), compressedMaxSize,
                          reinterpret_cast<const char*>(uncompressed.data()), writePos, compressionLevel);
    if (ZSTD_isError(compressedSize)) {
        throw std::runtime_error(std::string("String dict encoder: zstd compression failed: ") +
                                 ZSTD_getErrorName(compressedSize));
    }

    // Write the dictionary variant of the shared v1 header and payload.
    writeStringHeader(target, kDictionaryStringV1Magic, static_cast<uint32_t>(writePos),
                      static_cast<uint32_t>(compressedSize), static_cast<uint32_t>(values.size()));
    target.write_bytes(tlCompBuf.data(), compressedSize);
    return target.size() - startPos;
}

size_t StringEncoder::decodeDictionary(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                                       const std::vector<std::string>& dictEntries, std::vector<std::string>& out) {
    const StringBlockHeader header = readStringHeader(encoded, kDictionaryStringV1Magic);
    if (header.count != totalCount)
        throw std::runtime_error("Dictionary string block count does not match its v1 header");

    // Empty block: nothing to decompress — advance past compressed data and
    // return. APPENDS, so `out` must be left exactly as it was found.
    if (header.uncompressedSize == 0) {
        encoded.offset += header.compressedSize;
        return 0;
    }

    tlDecompBuf.resize(header.uncompressedSize);
    size_t ret =
        ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(tlDecompBuf.data()), header.uncompressedSize,
                            reinterpret_cast<const char*>(encoded.data + encoded.offset), header.compressedSize);
    validateDecompress(ret, header.uncompressedSize);
    encoded.offset += header.compressedSize;

    Slice idSlice(tlDecompBuf.data(), header.uncompressedSize);
    out.reserve(out.size() + limitCount);
    size_t produced = 0;
    for (size_t i = 0; i < totalCount && idSlice.offset < idSlice.length_; ++i) {
        uint32_t id = readVarInt(idSlice);
        if (id >= dictEntries.size()) {
            throw std::runtime_error("Dictionary ID out of range");
        }
        if (i < skipCount)
            continue;
        if (produced < limitCount) {
            out.push_back(dictEntries[id]);
            produced++;
        } else {
            break;
        }
    }
    return produced;
}

bool StringEncoder::isDictionaryEncoded(Slice& slice) {
    if (slice.offset > slice.length_ || slice.length_ - slice.offset < 4)
        return false;
    uint32_t magic;
    std::memcpy(&magic, slice.data + slice.offset, 4);
    return magic == kDictionaryStringV1Magic;
}

size_t StringEncoder::decode(Slice& encoded, size_t totalCount, size_t skipCount, size_t limitCount,
                             std::vector<std::string>& out) {
    const StringBlockHeader header = readStringHeader(encoded, kRawStringV1Magic);
    if (header.count != totalCount)
        throw std::runtime_error("Raw string block count does not match its v1 header");

    // Empty block: nothing to decompress — advance past compressed data and
    // return. APPENDS, so `out` must be left exactly as it was found.
    if (header.uncompressedSize == 0) {
        encoded.offset += header.compressedSize;
        return 0;
    }

    // Decompress (zstd doesn't support random access, so we must decompress the full block)
    // Reuse thread-local buffer — grows to high-water mark, no alloc after warmup.
    tlDecompBuf.resize(header.uncompressedSize);
    auto& uncompressed = tlDecompBuf;

    {
        size_t ret =
            ZSTD_decompressDCtx(getThreadDCtx(), reinterpret_cast<char*>(uncompressed.data()), header.uncompressedSize,
                                reinterpret_cast<const char*>(encoded.data + encoded.offset), header.compressedSize);
        validateDecompress(ret, header.uncompressedSize);
    }
    encoded.offset += header.compressedSize;

    // Decode strings with skip/limit: skip the first skipCount strings without allocating,
    // then collect the next limitCount strings.
    // APPENDS to `out` -- it must NOT be cleared. decodeBlockFlat() decodes every
    // block of a series into ONE shared flat vector (exactly as the float, bool
    // and integer decoders do), so clearing here destroyed every previously
    // decoded block's values while their timestamps remained. TSM now rejects a
    // decoder that shrinks the output, so this would fail the query loudly rather
    // than corrupt memory -- but the contract still belongs here.
    Slice uncompSlice(uncompressed.data(), header.uncompressedSize);
    out.reserve(out.size() + limitCount);

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
    return produced;
}
