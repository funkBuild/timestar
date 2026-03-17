#include "bool_encoder.hpp"

// Timestamp encoding - http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

AlignedBuffer BoolEncoder::encode(const std::vector<bool>& values) {
    AlignedBuffer buffer;

    const size_t valuesLength = values.size();
    size_t offset = 0;

    while (offset < valuesLength) {
        unsigned int numValuesLeft = valuesLength - offset;

        if (numValuesLeft >= 64) {
            encodeBool<uint64_t>(values, offset, buffer);
        } else if (numValuesLeft >= 32) {
            encodeBool<uint32_t>(values, offset, buffer);
        } else if (numValuesLeft >= 16) {
            encodeBool<uint16_t>(values, offset, buffer);
        } else if (numValuesLeft >= 8) {
            encodeBool<uint8_t>(values, offset, buffer);
        } else {
            encodeBool(numValuesLeft, values, offset, buffer);
        }
    }

    return buffer;
}

size_t BoolEncoder::encodeInto(const std::vector<bool>& values, AlignedBuffer& target) {
    const size_t startPos = target.size();
    const size_t valuesLength = values.size();
    size_t offset = 0;

    while (offset < valuesLength) {
        unsigned int numValuesLeft = valuesLength - offset;

        if (numValuesLeft >= 64) {
            encodeBool<uint64_t>(values, offset, target);
        } else if (numValuesLeft >= 32) {
            encodeBool<uint32_t>(values, offset, target);
        } else if (numValuesLeft >= 16) {
            encodeBool<uint16_t>(values, offset, target);
        } else if (numValuesLeft >= 8) {
            encodeBool<uint8_t>(values, offset, target);
        } else {
            encodeBool(numValuesLeft, values, offset, target);
        }
    }

    return target.size() - startPos;
}

void BoolEncoder::decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out) {
    if (length == 0) return;
    out.reserve(out.size() + length);

    // Phase 1: Skip bits — advance pointer directly (no per-byte read call)
    size_t skipBytes = nToSkip / 8;
    size_t skipBitsRem = nToSkip % 8;
    encoded.offset += skipBytes;

    size_t remaining = length;

    // Phase 2: Partial first byte after skip
    if (skipBitsRem > 0) {
        uint8_t byte = encoded.data[encoded.offset++];
        for (size_t i = skipBitsRem; i < 8 && remaining > 0; ++i) {
            out.push_back(((byte >> i) & 0x1) != 0);
            --remaining;
        }
    }

    // Phase 3: Bulk decode — read raw bytes directly from data pointer,
    // bypassing Slice::read<uint8_t>() per-byte bounds checks.
    const uint8_t* __restrict__ src = encoded.data + encoded.offset;

    // Process 8 bytes (64 values) at a time
    while (remaining >= 64) {
        for (int b = 0; b < 8; ++b) {
            uint8_t byte = src[b];
            out.push_back((byte & 0x01) != 0);
            out.push_back((byte & 0x02) != 0);
            out.push_back((byte & 0x04) != 0);
            out.push_back((byte & 0x08) != 0);
            out.push_back((byte & 0x10) != 0);
            out.push_back((byte & 0x20) != 0);
            out.push_back((byte & 0x40) != 0);
            out.push_back((byte & 0x80) != 0);
        }
        src += 8;
        remaining -= 64;
    }

    // Remaining full bytes
    while (remaining >= 8) {
        uint8_t byte = *src++;
        out.push_back((byte & 0x01) != 0);
        out.push_back((byte & 0x02) != 0);
        out.push_back((byte & 0x04) != 0);
        out.push_back((byte & 0x08) != 0);
        out.push_back((byte & 0x10) != 0);
        out.push_back((byte & 0x20) != 0);
        out.push_back((byte & 0x40) != 0);
        out.push_back((byte & 0x80) != 0);
        remaining -= 8;
    }

    encoded.offset += (src - (encoded.data + encoded.offset));

    // Phase 4: Tail — final partial byte
    if (remaining > 0) {
        uint8_t byte = encoded.data[encoded.offset++];
        for (size_t i = 0; i < remaining; ++i) {
            out.push_back(((byte >> i) & 0x1) != 0);
        }
    }
}

template <class T>
void BoolEncoder::encodeBool(const std::vector<bool>& values, size_t& offset, AlignedBuffer& buffer) {
    T value = 0;

    for (unsigned int i = 0; i < 8 * sizeof(T); i++) {
        value |= ((uint64_t)values[offset + i]) << i;
    }

    offset += 8 * sizeof(T);
    buffer.write(value);
}

void BoolEncoder::encodeBool(unsigned int length, const std::vector<bool>& values, size_t& offset,
                             AlignedBuffer& buffer) {
    uint8_t value = 0;

    for (unsigned int i = 0; i < length; i++) {
        value |= ((unsigned int)values[offset + i]) << i;
    }

    offset += length;
    buffer.write(value);
}