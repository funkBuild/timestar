#include "float_decoder.hpp"

#include <bit>
#include <cstring>
#include <stdexcept>

void FloatDecoderBasic::decode(CompressedSlice& values, size_t nToSkip, size_t length, std::vector<double>& out) {
    if (length == 0) [[unlikely]] {
        return;
    }

    // Reserve exact space upfront to avoid reallocations
    const size_t current_size = out.size();
    const size_t required_capacity = current_size + length;

    out.resize(required_capacity);
    double* __restrict__ output_ptr = out.data() + current_size;

    uint64_t last_value = values.readFixed<uint64_t, 64>();
    uint64_t tzb = 0;
    uint64_t data_bits = 0;
    bool bounds_initialized = false;

    // Save original nToSkip for totalLength calculation
    const size_t originalSkip = nToSkip;

    // Handle first value
    if (nToSkip == 0) {
        *output_ptr++ = std::bit_cast<double>(last_value);
    } else {
        nToSkip--;  // Account for the first value if skipping
    }

    // Total values to process from delta stream
    const size_t totalLength = originalSkip + length;

    // Split the loop into skip phase and emit phase to eliminate the
    // nToSkip branch from the hot emit loop.

    // Phase 1: Skip remaining values (if any)
    size_t count = 0;
    while (nToSkip > 0 && ++count < totalLength) {
        if (values.readBit()) {
            if (values.readBit()) {
                // 0b11 prefix - new bounds
                const uint64_t control_data = values.read<uint64_t>(11);
                const auto lzb = control_data & 0x1F;
                data_bits = (control_data >> 5) & 0x3F;

                if (data_bits == 0 && lzb == 0) [[unlikely]] {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    if (lzb + data_bits > 64) [[unlikely]] {
                        throw std::runtime_error("Corrupt float block: lzb + data_bits > 64");
                    }
                    tzb = 64 - lzb - data_bits;
                }

                bounds_initialized = true;
                last_value ^= values.read<uint64_t>(data_bits) << tzb;
            } else {
                if (!bounds_initialized) [[unlikely]] {
                    throw std::runtime_error("Corrupt float block: reuse-bounds before any bounds established");
                }
                last_value ^= values.read<uint64_t>(data_bits) << tzb;
            }
        }
        nToSkip--;
    }

    // Phase 2: Emit values -- no skip check in the hot path
    while (++count < totalLength) {
        if (values.readBit()) {
            if (values.readBit()) {
                // 0b11 prefix - new bounds
                const uint64_t control_data = values.read<uint64_t>(11);
                const auto lzb = control_data & 0x1F;
                data_bits = (control_data >> 5) & 0x3F;

                if (data_bits == 0 && lzb == 0) [[unlikely]] {
                    data_bits = 64;
                    tzb = 0;
                } else {
                    if (lzb + data_bits > 64) [[unlikely]] {
                        throw std::runtime_error("Corrupt float block: lzb + data_bits > 64");
                    }
                    tzb = 64 - lzb - data_bits;
                }

                bounds_initialized = true;
                last_value ^= values.read<uint64_t>(data_bits) << tzb;
            } else {
                // 0b01 prefix - reusing previous bounds (common case in typical data)
                last_value ^= values.read<uint64_t>(data_bits) << tzb;
            }
        }
        // else: 0b0 prefix - value unchanged

        *output_ptr++ = std::bit_cast<double>(last_value);
    }
}
