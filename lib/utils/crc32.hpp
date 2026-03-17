#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

// CRC32 (ISO 3309 / ITU-T V.42) using slicing-by-8.
// Polynomial: 0xEDB88320 (reflected representation of 0x04C11DB7).
// Same algorithm as zlib, gzip, PNG, etc.
//
// Slicing-by-8 processes 8 bytes per iteration using 8 interleaved lookup
// tables (8 KB total), achieving 4-8x throughput vs single-table lookup.
// Output is bit-identical to the original single-table implementation.

namespace detail {

// Generate 8 slicing tables at compile time.
// Table 0 is the standard CRC32 byte-at-a-time table.
// Tables 1-7 are derived by applying the polynomial recursively.
constexpr std::array<std::array<uint32_t, 256>, 8> generateCrc32SlicingTables() {
    std::array<std::array<uint32_t, 256>, 8> tables{};

    // Table 0: standard byte-at-a-time table
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = i;
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
        tables[0][i] = crc;
    }

    // Tables 1-7: each entry is derived by feeding the previous table's
    // entry through one more byte of CRC computation.
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t crc = tables[0][i];
        for (int t = 1; t < 8; t++) {
            crc = tables[0][crc & 0xFF] ^ (crc >> 8);
            tables[t][i] = crc;
        }
    }

    return tables;
}

}  // namespace detail

class CRC32 {
public:
    // Initial CRC state for incremental computation.
    static constexpr uint32_t INIT = 0xFFFFFFFF;

    // Compile-time slicing-by-8 CRC32 lookup tables (8 KB).
    static constexpr auto TABLES = detail::generateCrc32SlicingTables();

    // Single-table alias for backward compatibility (table 0 = standard table).
    static constexpr auto& TABLE = TABLES[0];

    // Finalize the running CRC state to produce the final checksum.
    static constexpr uint32_t finalize(uint32_t crc) { return crc ^ 0xFFFFFFFF; }

    // Incrementally feed bytes into a running CRC state.
    // Slicing-by-8: processes 8 bytes per iteration via 8 interleaved
    // table lookups XOR'd together, ~4-8x faster than byte-at-a-time.
    static uint32_t update(uint32_t crc, const uint8_t* data, size_t length) {
        const uint8_t* end = data + length;

        // Slicing-by-8 main loop: 8 bytes per iteration.
        // Read two 32-bit words, XOR the first with the running CRC,
        // then look up all 8 bytes across the 8 tables simultaneously.
        while (data + 8 <= end) {
            uint32_t a, b;
            std::memcpy(&a, data, sizeof(uint32_t));
            std::memcpy(&b, data + 4, sizeof(uint32_t));
            a ^= crc;

            crc = TABLES[7][(a) & 0xFF] ^ TABLES[6][(a >> 8) & 0xFF] ^ TABLES[5][(a >> 16) & 0xFF] ^
                  TABLES[4][(a >> 24) & 0xFF] ^ TABLES[3][(b) & 0xFF] ^ TABLES[2][(b >> 8) & 0xFF] ^
                  TABLES[1][(b >> 16) & 0xFF] ^ TABLES[0][(b >> 24) & 0xFF];
            data += 8;
        }

        // Tail: remaining 0-7 bytes, byte-at-a-time
        while (data < end) {
            crc = TABLES[0][(crc ^ *data++) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }

    // Convenience overload for char pointers.
    static uint32_t update(uint32_t crc, const char* data, size_t length) {
        return update(crc, reinterpret_cast<const uint8_t*>(data), length);
    }

    // Compute CRC32 over a byte buffer (single-shot).
    static uint32_t compute(const uint8_t* data, size_t length) { return finalize(update(INIT, data, length)); }

    // Convenience overload for char pointers.
    static uint32_t compute(const char* data, size_t length) {
        return compute(reinterpret_cast<const uint8_t*>(data), length);
    }
};
