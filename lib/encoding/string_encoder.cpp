#include "string_encoder.hpp"

#include <snappy.h>

#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>

// Maximum decompressed size we'll allocate for string decoding (256MB).
// Prevents OOM from a crafted payload that claims a multi-GB uncompressed size.
static constexpr uint32_t MAX_UNCOMPRESSED_SIZE = 256 * 1024 * 1024;

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

AlignedBuffer StringEncoder::encode(std::span<const std::string> values) {
    AlignedBuffer result;

    // Validate count fits in uint32_t header field
    if (values.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: count " + std::to_string(values.size()) +
                                  " exceeds uint32_t maximum");
    }

    // Calculate uncompressed size and validate individual string lengths
    size_t uncompressedSize = 0;
    for (const auto& str : values) {
        if (str.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::overflow_error("String encoder: individual string length " + std::to_string(str.size()) +
                                      " exceeds uint32_t maximum");
        }
        // Calculate varint size for length
        uint32_t len = static_cast<uint32_t>(str.size());
        size_t varintSize = 1;
        while (len >= 0x80) {
            varintSize++;
            len >>= 7;
        }
        uncompressedSize += varintSize + str.size();
    }

    // Validate total uncompressed size fits in uint32_t header field
    if (uncompressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: total uncompressed size " + std::to_string(uncompressedSize) +
                                  " exceeds uint32_t maximum");
    }

    // Create uncompressed buffer with string data
    AlignedBuffer uncompressed(uncompressedSize);
    size_t writePos = 0;

    for (const auto& str : values) {
        if (str.size() > std::numeric_limits<uint32_t>::max()) {
            throw std::overflow_error("String encoder: individual string size " + std::to_string(str.size()) +
                                      " exceeds uint32_t maximum");
        }
        // Write varint length prefix
        uint32_t len = static_cast<uint32_t>(str.size());
        while (len >= 0x80) {
            uncompressed.data[writePos++] = static_cast<uint8_t>((len & 0x7F) | 0x80);
            len >>= 7;
        }
        uncompressed.data[writePos++] = static_cast<uint8_t>(len & 0x7F);

        // Write string data
        std::memcpy(uncompressed.data.data() + writePos, str.data(), str.size());
        writePos += str.size();
    }
    uncompressed.data.resize(writePos);

    // Compress with Snappy
    size_t compressedMaxSize = snappy::MaxCompressedLength(uncompressed.data.size());
    std::vector<char> compressed(compressedMaxSize);
    size_t compressedSize;

    snappy::RawCompress(reinterpret_cast<const char*>(uncompressed.data.data()), uncompressed.data.size(),
                        compressed.data(), &compressedSize);

    // Validate compressed size fits in uint32_t header field
    if (compressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: compressed size " + std::to_string(compressedSize) +
                                  " exceeds uint32_t maximum");
    }

    // Write header: magic_number(4) | uncompressed_size(4) | compressed_size(4) | count(4)

    // Magic number for string encoding
    uint32_t magic = 0x53545247;  // "STRG"
    result.write(magic);

    uint32_t uncompSize = static_cast<uint32_t>(uncompressed.data.size());
    result.write(uncompSize);

    uint32_t compSize = static_cast<uint32_t>(compressedSize);
    result.write(compSize);

    uint32_t count = static_cast<uint32_t>(values.size());
    result.write(count);

    // Write compressed data
    result.write_bytes(compressed.data(), compressedSize);

    return result;
}

size_t StringEncoder::encodeInto(std::span<const std::string> values, AlignedBuffer& target) {
    const size_t startPos = target.size();

    if (values.empty()) [[unlikely]] {
        // Write a valid empty header so decoders don't choke
        uint32_t magic = 0x53545247;  // "STRG"
        target.write(magic);
        target.write(static_cast<uint32_t>(0));  // uncompressed_size
        target.write(static_cast<uint32_t>(0));  // compressed_size
        target.write(static_cast<uint32_t>(0));  // count
        return target.size() - startPos;
    }

    // Validate count fits in uint32_t header field
    if (values.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: count " + std::to_string(values.size()) +
                                  " exceeds uint32_t maximum");
    }

    // Calculate uncompressed size and validate individual string lengths
    size_t uncompressedSize = 0;
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
        uncompressedSize += varintSize + str.size();
    }

    if (uncompressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: total uncompressed size " + std::to_string(uncompressedSize) +
                                  " exceeds uint32_t maximum");
    }

    // Build the uncompressed buffer (varint-prefixed strings)
    AlignedBuffer uncompressed(uncompressedSize);
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

    // Compress with Snappy into a temporary buffer.
    // We still need one temporary for Snappy's output since it requires a
    // contiguous writable region and we don't know the exact compressed size
    // upfront. This eliminates the 'result' AlignedBuffer copy from encode().
    size_t compressedMaxSize = snappy::MaxCompressedLength(uncompressed.data.size());
    std::vector<char> compressed(compressedMaxSize);
    size_t compressedSize;

    snappy::RawCompress(reinterpret_cast<const char*>(uncompressed.data.data()), uncompressed.data.size(),
                        compressed.data(), &compressedSize);

    if (compressedSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("String encoder: compressed size " + std::to_string(compressedSize) +
                                  " exceeds uint32_t maximum");
    }

    // Write header directly into the target buffer
    uint32_t magic = 0x53545247;  // "STRG"
    target.write(magic);
    target.write(static_cast<uint32_t>(uncompressed.data.size()));
    target.write(static_cast<uint32_t>(compressedSize));
    target.write(static_cast<uint32_t>(values.size()));

    // Write compressed data directly into the target buffer
    target.write_bytes(compressed.data(), compressedSize);

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
    std::vector<uint8_t> uncompressed(uncompressedSize);

    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize,
                               reinterpret_cast<char*>(uncompressed.data()))) {
        throw std::runtime_error("Failed to decompress string data");
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
    std::vector<uint8_t> uncompressed(uncompressedSize);

    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize,
                               reinterpret_cast<char*>(uncompressed.data()))) {
        throw std::runtime_error("Failed to decompress string data");
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

    // Decompress (Snappy doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    std::vector<uint8_t> uncompressed(uncompressedSize);

    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data.data() + 16), compressedSize,
                               reinterpret_cast<char*>(uncompressed.data()))) {
        throw std::runtime_error("Failed to decompress string data");
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

    // Decompress (Snappy doesn't support random access, so we must decompress the full block)
    if (uncompressedSize > MAX_UNCOMPRESSED_SIZE) {
        throw std::runtime_error("String block uncompressedSize (" + std::to_string(uncompressedSize) +
                                 ") exceeds limit");
    }
    std::vector<uint8_t> uncompressed(uncompressedSize);

    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data + encoded.offset), compressedSize,
                               reinterpret_cast<char*>(uncompressed.data()))) {
        throw std::runtime_error("Failed to decompress string data");
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