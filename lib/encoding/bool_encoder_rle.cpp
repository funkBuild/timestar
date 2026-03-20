#include "bool_encoder_rle.hpp"

// RLE boolean encoder.
//
// Format:
//   Byte 0: initial value (0 or 1)
//   Remaining bytes: varint-encoded run lengths (LEB128), alternating between
//   the initial value and its complement.
//
// Example: [true x 1000, false x 500, true x 200]
//   -> initial=1, runs=[1000, 500, 200]

void BoolEncoderRLE::writeVarint(AlignedBuffer& buf, uint64_t value) {
    if (value < 0x80) [[likely]] {
        buf.write<uint8_t>(static_cast<uint8_t>(value));
        return;
    }
    // Multi-byte path
    while (value >= 0x80) {
        buf.write<uint8_t>(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    buf.write<uint8_t>(static_cast<uint8_t>(value));
}

uint64_t BoolEncoderRLE::readVarint(Slice& slice) {
    if (slice.bytesLeft() == 0) [[unlikely]] {
        throw std::runtime_error("BoolEncoderRLE: unexpected end of data in varint");
    }
    // Fast path: single-byte varint (value < 128) — the common case.
    const uint8_t* ptr = slice.data + slice.offset;
    uint8_t first = *ptr;
    if ((first & 0x80) == 0) [[likely]] {
        slice.offset += 1;
        return first;
    }
    // Multi-byte: decode up to 10 bytes using direct pointer access.
    uint64_t result = first & 0x7F;
    int shift = 7;
    size_t pos = 1;
    const size_t remaining = slice.bytesLeft();
    while (pos < remaining) {
        uint8_t byte = ptr[pos++];
        result |= static_cast<uint64_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            // Complete varint — advance slice and return
            slice.offset += pos;
            return result;
        }
        shift += 7;
        if (shift >= 64) {
            throw std::runtime_error("BoolEncoderRLE: varint overflow (>64 bits)");
        }
    }
    // Loop exited because we ran out of data mid-varint (continuation bit set)
    throw std::runtime_error("BoolEncoderRLE: truncated varint");
}

AlignedBuffer BoolEncoderRLE::encode(const std::vector<bool>& values) {
    AlignedBuffer buffer;
    encodeInto(values, buffer);
    return buffer;
}

size_t BoolEncoderRLE::encodeInto(const std::vector<bool>& values, AlignedBuffer& target) {
    const size_t startPos = target.size();
    const size_t n = values.size();

    if (n == 0) [[unlikely]] {
        return 0;
    }

    // Task #3: Pre-allocate buffer space (conservative estimate).
    target.reserve(target.size() + 1 + n / 4);

#ifdef __GLIBCXX__
    // -----------------------------------------------------------------------
    // GCC libstdc++ fast path: access the packed word storage directly.
    //
    // In libstdc++, std::vector<bool>::iterator is a _Bit_iterator whose
    // members _M_p (pointer to unsigned long word) and _M_offset (bit index
    // within that word) give us direct access to the underlying storage.
    // Each word is an unsigned long (64 bits on x86-64).
    //
    // Algorithm: XOR each word with the "expected" pattern for the current
    // run value.  If XOR == 0 the entire word continues the run.  Otherwise
    // __builtin_ctzll finds the bit position of the first transition.
    // -----------------------------------------------------------------------
    static_assert(sizeof(unsigned long) == 8, "Word-level RLE encode assumes 64-bit unsigned long");

    auto it = values.begin();
    const unsigned long* wordPtr = it._M_p;
    const unsigned int bitOff = it._M_offset;  // 0 for a fresh vector

    // Write the initial value byte.
    bool currentValue = values[0];
    target.write<uint8_t>(currentValue ? 1 : 0);

    uint64_t runLength = 0;

    // Number of full 64-bit words that contain our bits (accounting for offset).
    const size_t totalBits = n + bitOff;  // bits from wordPtr[0] onward
    const size_t fullWords = totalBits / 64;
    const unsigned int tailBits = totalBits % 64;  // leftover bits in last word

    for (size_t w = 0; w < fullWords; ++w) {
        unsigned long word = wordPtr[w];

        // Mask out bits before bitOff in the very first word.
        unsigned long mask = ~0UL;
        unsigned int lowBit = 0;
        unsigned int highBit = 64;  // one-past-end within this word
        if (w == 0 && bitOff != 0) {
            lowBit = bitOff;
            mask = ~0UL << bitOff;  // zero out bits [0, bitOff)
        }

        // XOR the word against the current run value to find transitions.
        unsigned long xorWord = (word ^ (currentValue ? ~0UL : 0UL)) & mask;

        if (xorWord == 0) {
            // Entire (masked) word matches the current run value.
            runLength += (highBit - lowBit);
            continue;
        }

        // There are one or more transitions inside this word.
        // Walk through them using ctz (count trailing zeros).
        unsigned int pos = lowBit;
        while (xorWord != 0) {
            unsigned int tz = __builtin_ctzll(xorWord);
            unsigned int transitionBit = tz;  // absolute bit position in word

            // Bits [pos, transitionBit) continue the current run.
            runLength += (transitionBit - pos);
            writeVarint(target, runLength);
            currentValue = !currentValue;
            runLength = 0;
            pos = transitionBit;

            // Clear all bits up to and including transitionBit.
            // We need to clear the lowest set bit and also recompute xorWord
            // for the new run value from position pos onward.
            // Recompute: XOR of remaining bits with the NEW expected value.
            unsigned long remainMask = (pos < 63) ? (~0UL << pos) : (pos == 63 ? (1UL << 63) : 0UL);
            xorWord = (word ^ (currentValue ? ~0UL : 0UL)) & remainMask;
        }

        // Remaining bits [pos, highBit) are part of the current run.
        runLength += (highBit - pos);
    }

    // Handle the tail (partial last word).
    if (tailBits > 0) {
        unsigned long word = wordPtr[fullWords];

        // Only bits [0, tailBits) are valid in this word.
        // But if fullWords == 0 and bitOff != 0, we also need to skip [0, bitOff).
        unsigned int lowBit = 0;
        if (fullWords == 0 && bitOff != 0) {
            lowBit = bitOff;
        }

        // Mask: only bits [lowBit, tailBits) are valid.
        unsigned long mask = ((tailBits < 64) ? (1UL << tailBits) : 0UL) - 1UL;
        if (lowBit > 0) {
            mask &= ~((1UL << lowBit) - 1UL);
        }

        unsigned long xorWord = (word ^ (currentValue ? ~0UL : 0UL)) & mask;

        if (xorWord == 0) {
            runLength += (tailBits - lowBit);
        } else {
            unsigned int pos = lowBit;
            while (xorWord != 0) {
                unsigned int tz = __builtin_ctzll(xorWord);
                unsigned int transitionBit = tz;

                runLength += (transitionBit - pos);
                writeVarint(target, runLength);
                currentValue = !currentValue;
                runLength = 0;
                pos = transitionBit;

                unsigned long remainMask = mask & ((pos < 63) ? (~0UL << pos) : (pos == 63 ? (1UL << 63) : 0UL));
                xorWord = (word ^ (currentValue ? ~0UL : 0UL)) & remainMask;
            }
            runLength += (tailBits - pos);
        }
    }

    // Write the final run.
    writeVarint(target, runLength);

#else
    // -----------------------------------------------------------------------
    // Portable fallback: per-element proxy access.
    // -----------------------------------------------------------------------
    bool currentValue = values[0];
    target.write<uint8_t>(currentValue ? 1 : 0);

    uint64_t runLength = 1;
    for (size_t i = 1; i < n; ++i) {
        if (static_cast<bool>(values[i]) == currentValue) {
            ++runLength;
        } else {
            writeVarint(target, runLength);
            currentValue = !currentValue;
            runLength = 1;
        }
    }
    writeVarint(target, runLength);
#endif

    return target.size() - startPos;
}

void BoolEncoderRLE::decode(Slice& encoded, size_t nToSkip, size_t length, std::vector<bool>& out) {
    if (length == 0) [[unlikely]]
        return;

    bool currentValue = encoded.read<uint8_t>() != 0;

    // Track leftover from a run that straddles the skip/emit boundary.
    size_t leftover = 0;

    // Phase 1: Skip nToSkip values by consuming runs without emitting.
    while (nToSkip > 0 && encoded.bytesLeft() > 0) {
        uint64_t runLen = readVarint(encoded);
        if (runLen <= nToSkip) {
            // Entire run consumed by skip
            nToSkip -= runLen;
            currentValue = !currentValue;
        } else {
            // Run partially overlaps: skip the front, keep the tail for emit
            leftover = static_cast<size_t>(runLen) - nToSkip;
            nToSkip = 0;
        }
    }

    // Pre-size the output vector: reserve then resize in one shot.
    const size_t basePos = out.size();
    out.reserve(basePos + length);
    out.resize(basePos + length);
    size_t writePos = basePos;

    // Phase 2a: Emit leftover from the straddling run (if any).
    if (leftover > 0) {
        const size_t toEmit = std::min(leftover, length);
        std::fill(out.begin() + writePos, out.begin() + writePos + toEmit, currentValue);
        writePos += toEmit;
        length -= toEmit;
        currentValue = !currentValue;
    }

    // Phase 2b: Emit remaining runs using bulk std::fill.
    while (length > 0 && encoded.bytesLeft() > 0) {
        uint64_t runLen = readVarint(encoded);
        const size_t toEmit = std::min(static_cast<size_t>(runLen), length);
        std::fill(out.begin() + writePos, out.begin() + writePos + toEmit, currentValue);
        writePos += toEmit;
        length -= toEmit;
        currentValue = !currentValue;
    }

    if (writePos != out.size()) {
        throw std::runtime_error("BoolEncoderRLE::decode: emitted " + std::to_string(writePos - basePos) +
                                 " values, expected " + std::to_string(out.size() - basePos));
    }
}
