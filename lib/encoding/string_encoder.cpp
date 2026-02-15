#include "string_encoder.hpp"
#include <snappy.h>
#include <cstring>

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
            // We've read the 5th byte (bits 28-34), but uint32_t only holds 32 bits,
            // so bits 32-34 are silently truncated. If the continuation bit is still
            // set, the varint is definitely too large for uint32_t.
            if (byte & 0x80) {
                throw std::runtime_error("VarInt too large");
            }
            break;
        }
    } while (byte & 0x80);

    return value;
}

AlignedBuffer StringEncoder::encode(const std::vector<std::string>& values) {
    AlignedBuffer result;
    
    // Calculate uncompressed size
    size_t uncompressedSize = 0;
    for (const auto& str : values) {
        // Calculate varint size for length
        uint32_t len = str.size();
        size_t varintSize = 1;
        while (len >= 0x80) {
            varintSize++;
            len >>= 7;
        }
        uncompressedSize += varintSize + str.size();
    }
    
    // Create uncompressed buffer with string data
    AlignedBuffer uncompressed(uncompressedSize);
    size_t writePos = 0;
    
    for (const auto& str : values) {
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
    
    snappy::RawCompress(reinterpret_cast<const char*>(uncompressed.data.data()), 
                        uncompressed.data.size(),
                        compressed.data(),
                        &compressedSize);
    
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

    if (encoded.size() < 16 + compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }
    
    // Decompress
    std::vector<uint8_t> uncompressed(uncompressedSize);
    
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data.data() + 16),
                                compressedSize,
                                reinterpret_cast<char*>(uncompressed.data()))) {
        throw std::runtime_error("Failed to decompress string data");
    }
    
    // Decode strings
    Slice slice(uncompressed.data(), uncompressedSize);
    out.clear();
    out.reserve(count);
    
    for (size_t i = 0; i < count && slice.offset < slice.length_; i++) {
        uint32_t strLen = readVarInt(slice);
        
        if (slice.offset + strLen > slice.length_) {
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
    
    if (encoded.length_ - encoded.offset < compressedSize) {
        throw std::runtime_error("Invalid encoded buffer: size mismatch");
    }
    
    // Decompress
    std::vector<uint8_t> uncompressed(uncompressedSize);
    
    if (!snappy::RawUncompress(reinterpret_cast<const char*>(encoded.data + encoded.offset),
                                compressedSize,
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
        
        if (uncompSlice.offset + strLen > uncompSlice.length_) {
            throw std::runtime_error("Invalid string length in encoded data");
        }
        
        out.emplace_back(reinterpret_cast<const char*>(uncompSlice.data + uncompSlice.offset), strLen);
        uncompSlice.offset += strLen;
    }
}