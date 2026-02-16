#include "alp_decoder.hpp"
#include "alp_constants.hpp"
#include "alp_ffor.hpp"

#include <bit>
#include <cmath>
#include <stdexcept>
#include <cstring>

void ALPDecoder::decode(CompressedSlice& encoded, size_t nToSkip, size_t length,
                        std::vector<double>& out) {
    if (length == 0) return;

    // Pre-allocate output
    const size_t current_size = out.size();
    const size_t required = current_size + length;
    if (out.capacity() < required) {
        out.reserve(required + (required >> 3));
    }
    out.resize(required);
    double* output_ptr = out.data() + current_size;
    size_t output_remaining = length;

    // === Read Stream Header ===
    uint64_t header0 = encoded.readFixed<uint64_t, 64>();
    uint64_t header1 = encoded.readFixed<uint64_t, 64>();

    uint32_t magic = static_cast<uint32_t>(header0 & 0xFFFFFFFF);
    if (magic != alp::ALP_MAGIC) {
        throw std::runtime_error("ALPDecoder: invalid magic number");
    }

    uint32_t total_values = static_cast<uint32_t>(header0 >> 32);
    uint16_t num_blocks   = static_cast<uint16_t>(header1 & 0xFFFF);
    uint16_t tail_count   = static_cast<uint16_t>((header1 >> 16) & 0xFFFF);
    uint8_t  scheme       = static_cast<uint8_t>((header1 >> 32) & 0xFF);

    // Track global position for skip/limit
    size_t global_pos = 0;

    for (uint16_t block = 0; block < num_blocks && output_remaining > 0; ++block) {
        if (scheme == alp::SCHEME_ALP) {
            // === ALP Block Header ===
            uint64_t bh0 = encoded.readFixed<uint64_t, 64>();
            uint64_t bh1 = encoded.readFixed<uint64_t, 64>();

            uint8_t  exp             = static_cast<uint8_t>(bh0 & 0xFF);
            uint8_t  fac             = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            uint8_t  bw              = static_cast<uint8_t>((bh0 >> 16) & 0x7F);
            uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            uint16_t block_count     = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            int64_t  for_base        = std::bit_cast<int64_t>(bh1);

            // === Read FFOR Data ===
            size_t packed_words = alp::ffor_packed_words(block_count, bw);
            std::vector<uint64_t> packed_data(packed_words);
            for (size_t w = 0; w < packed_words; ++w) {
                packed_data[w] = encoded.readFixed<uint64_t, 64>();
            }

            // Unpack integers
            std::vector<int64_t> decoded_ints(block_count);
            alp::ffor_unpack(packed_data.data(), block_count, for_base, bw, decoded_ints.data());

            // === Read Exceptions ===
            std::vector<uint16_t> exc_positions;
            std::vector<uint64_t> exc_values;
            if (exception_count > 0) {
                // Read exception positions
                size_t pos_words = (exception_count * 2 + 7) / 8;
                exc_positions.resize(exception_count);
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = encoded.readFixed<uint64_t, 64>();
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) {
                            exc_positions[idx] = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                        }
                    }
                }

                // Read exception values
                exc_values.resize(exception_count);
                for (size_t i = 0; i < exception_count; ++i) {
                    exc_values[i] = encoded.readFixed<uint64_t, 64>();
                }
            }

            // === Convert to doubles and apply skip/limit ===
            // Build exception map for this block
            size_t exc_idx = 0;

            for (size_t i = 0; i < block_count; ++i) {
                double value;

                // Check if this position is an exception
                if (exc_idx < exception_count && exc_positions[exc_idx] == i) {
                    value = std::bit_cast<double>(exc_values[exc_idx]);
                    exc_idx++;
                } else {
                    // Reverse the scaling: decoded = encoded * 10^fac / 10^exp
                    value = static_cast<double>(decoded_ints[i])
                            * alp::FRAC_ARR[fac] / alp::FACT_ARR[exp];
                }

                if (global_pos >= nToSkip && output_remaining > 0) {
                    *output_ptr++ = value;
                    output_remaining--;
                }
                global_pos++;
            }

        } else {
            // === ALP_RD Block ===
            uint64_t bh0 = encoded.readFixed<uint64_t, 64>();
            uint64_t bh1 = encoded.readFixed<uint64_t, 64>();

            uint8_t  right_bw        = static_cast<uint8_t>(bh0 & 0xFF);
            uint8_t  left_bw         = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            uint8_t  dict_size       = static_cast<uint8_t>((bh0 >> 16) & 0xFF);
            uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            uint16_t block_count     = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            uint64_t right_for_base  = bh1;

            // === Read Dictionary ===
            std::vector<uint64_t> dictionary(dict_size);
            for (size_t i = 0; i < dict_size; ++i) {
                dictionary[i] = encoded.readFixed<uint64_t, 64>();
            }

            // === Read Left Indices (FFOR packed) ===
            std::vector<int64_t> left_indices_i64(block_count, 0);
            if (left_bw > 0) {
                size_t left_packed_words = alp::ffor_packed_words(block_count, left_bw);
                std::vector<uint64_t> left_packed(left_packed_words);
                for (size_t w = 0; w < left_packed_words; ++w) {
                    left_packed[w] = encoded.readFixed<uint64_t, 64>();
                }
                alp::ffor_unpack(left_packed.data(), block_count, 0, left_bw, left_indices_i64.data());
            }

            // === Read Right FFOR Data ===
            std::vector<uint64_t> right_parts(block_count, right_for_base);
            if (right_bw > 0) {
                size_t right_packed_words = alp::ffor_packed_words(block_count, right_bw);
                std::vector<uint64_t> right_packed(right_packed_words);
                for (size_t w = 0; w < right_packed_words; ++w) {
                    right_packed[w] = encoded.readFixed<uint64_t, 64>();
                }
                alp::ffor_unpack_u64(right_packed.data(), block_count, right_for_base,
                                     right_bw, right_parts.data());
            }

            // === Read Exceptions ===
            std::vector<uint16_t> exc_positions;
            std::vector<uint64_t> exc_values;
            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                exc_positions.resize(exception_count);
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = encoded.readFixed<uint64_t, 64>();
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) {
                            exc_positions[idx] = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                        }
                    }
                }
                exc_values.resize(exception_count);
                for (size_t i = 0; i < exception_count; ++i) {
                    exc_values[i] = encoded.readFixed<uint64_t, 64>();
                }
            }

            // === Reconstruct doubles ===
            // Determine right_bit_count from block context
            // right_bit_count is encoded implicitly: we stored right parts with FFOR,
            // and left parts as dictionary indices. We need the split position.
            // It's recoverable: dictionary values are the left parts (upper bits).
            // We need to figure out how many bits were in the right part.
            // This is stored implicitly by the range of right values + right_for_base.
            // Actually we need to store it - let's compute from dictionary and right parts.
            // The split position is determined by finding where left part bits end.
            // Since we have dict entries as left-part values and right parts,
            // we reconstruct: bits = (left << right_bit_count) | right
            // We need right_bit_count. Let's derive from the reserved field.
            // NOTE: We repurpose reserved [24:31] to store right_bit_count.
            uint8_t right_bit_count = static_cast<uint8_t>((bh0 >> 24) & 0xFF);

            const uint64_t right_mask = (right_bit_count == 64) ? ~0ULL
                                        : ((1ULL << right_bit_count) - 1);

            size_t exc_idx = 0;
            for (size_t i = 0; i < block_count; ++i) {
                double value;

                if (exc_idx < exception_count && exc_positions[exc_idx] == i) {
                    value = std::bit_cast<double>(exc_values[exc_idx]);
                    exc_idx++;
                } else {
                    uint64_t left = dictionary[static_cast<uint8_t>(left_indices_i64[i])];
                    uint64_t right = right_parts[i] & right_mask;
                    uint64_t combined = (left << right_bit_count) | right;
                    value = std::bit_cast<double>(combined);
                }

                if (global_pos >= nToSkip && output_remaining > 0) {
                    *output_ptr++ = value;
                    output_remaining--;
                }
                global_pos++;
            }
        }
    }

    // If we decoded fewer than requested (shouldn't happen with valid data), trim
    if (output_remaining > 0) {
        out.resize(required - output_remaining);
    }
}
