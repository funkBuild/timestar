#ifndef CRC32_H_INCLUDED
#define CRC32_H_INCLUDED

#include <array>
#include <cstddef>
#include <cstdint>

// CRC32 (ISO 3309 / ITU-T V.42) implementation using a precomputed lookup table.
// Polynomial: 0xEDB88320 (reflected representation of 0x04C11DB7).
//
// This is the same algorithm used by zlib, gzip, PNG, etc.

class CRC32 {
public:
    // Initial CRC state for incremental computation.
    static constexpr uint32_t INIT = 0xFFFFFFFF;

    // Compute CRC32 over a byte buffer (single-shot).
    static uint32_t compute(const uint8_t *data, size_t length) {
        return finalize(update(INIT, data, length));
    }

    // Convenience overload for char pointers.
    static uint32_t compute(const char *data, size_t length) {
        return compute(reinterpret_cast<const uint8_t *>(data), length);
    }

    // Incrementally feed bytes into a running CRC state.
    // Usage:
    //   uint32_t crc = CRC32::INIT;
    //   crc = CRC32::update(crc, buf1, len1);
    //   crc = CRC32::update(crc, buf2, len2);
    //   uint32_t result = CRC32::finalize(crc);
    static uint32_t update(uint32_t crc, const uint8_t *data, size_t length) {
        static const auto tbl = generateTable();
        for (size_t i = 0; i < length; i++) {
            crc = tbl[(crc ^ data[i]) & 0xFF] ^ (crc >> 8);
        }
        return crc;
    }

    // Convenience overload for char pointers.
    static uint32_t update(uint32_t crc, const char *data, size_t length) {
        return update(crc, reinterpret_cast<const uint8_t *>(data), length);
    }

    // Finalize the running CRC state to produce the final checksum.
    static constexpr uint32_t finalize(uint32_t crc) {
        return crc ^ 0xFFFFFFFF;
    }

private:
    // Generate the CRC32 lookup table at startup from polynomial 0xEDB88320.
    static constexpr std::array<uint32_t, 256> generateTable() {
        std::array<uint32_t, 256> table{};
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                if (crc & 1) {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
            table[i] = crc;
        }
        return table;
    }
};

#endif // CRC32_H_INCLUDED
