#include "alp_decoder.hpp"

#include "alp_constants.hpp"
#include "alp_ffor.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <stdexcept>

namespace {

// Thread-local scratch buffers for ALP decoding.
// Since Seastar uses a shard-per-core model (one thread per shard, no sharing),
// thread_local is safe and avoids any synchronization overhead.
// After the first few decode calls, all buffers reach their maximum needed capacity
// and subsequent calls are allocation-free.
struct ALPScratchBuffers {
    // Reusable per-block decode buffers. Allocated once per thread, capacity
    // persists across calls (no per-block heap allocation after warm-up).
    // Replaces large stack arrays (~28-46KB) that risked overflow on Seastar's
    // 128KB fiber stacks.
    std::vector<uint64_t> packed_data{1024};
    std::vector<int64_t> decoded_ints{1024};
    std::vector<uint16_t> exc_positions{1024};
    std::vector<uint64_t> exc_values{1024};
    std::vector<uint64_t> pos_word_buf{256};

    // ALP_RD scheme buffers
    std::vector<uint64_t> dictionary;
};

static ALPScratchBuffers& getScratch() {
    static thread_local ALPScratchBuffers scratch;
    return scratch;
}

}  // anonymous namespace

void ALPDecoder::decode(CompressedSlice& encoded, size_t nToSkip, size_t length, std::vector<double>& out) {
    if (length == 0) [[unlikely]]
        return;

    // Pre-allocate output -- exact size, no over-reserve since length is known
    const size_t current_size = out.size();
    const size_t required = current_size + length;
    out.resize(required);
    double* __restrict__ output_ptr = out.data() + current_size;
    size_t output_remaining = length;

    // === Read Stream Header ===
    uint64_t header0 = encoded.readFixed<uint64_t, 64>();
    uint64_t header1 = encoded.readFixed<uint64_t, 64>();

    uint32_t magic = static_cast<uint32_t>(header0 & 0xFFFFFFFF);
    if (magic != alp::ALP_MAGIC) [[unlikely]] {
        throw std::runtime_error("ALPDecoder: invalid magic number");
    }

    const uint16_t num_blocks = static_cast<uint16_t>(header1 & 0xFFFF);
    const uint8_t scheme = static_cast<uint8_t>((header1 >> 32) & 0xFF);

    // Get thread-local scratch buffers (reuses capacity across calls)
    auto& scratch = getScratch();

    // Track global position for skip/limit
    size_t global_pos = 0;

    for (uint16_t block = 0; block < num_blocks && output_remaining > 0; ++block) {
        if (scheme == alp::SCHEME_ALP || scheme == alp::SCHEME_ALP_DELTA) [[likely]] {
            // === ALP Block Header ===
            uint64_t bh0 = encoded.readFixed<uint64_t, 64>();
            uint64_t bh1 = encoded.readFixed<uint64_t, 64>();

            const uint8_t exp = static_cast<uint8_t>(bh0 & 0xFF);
            const uint8_t fac = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            const uint8_t bw = static_cast<uint8_t>((bh0 >> 16) & 0x7F);
            const uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            const uint16_t block_count = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            if (exp >= 19 || fac >= 19) throw std::runtime_error("ALP: invalid exp/fac");
            if (bw > 64) throw std::runtime_error("ALP: invalid bit width");
            if (block_count > 1024) throw std::runtime_error("ALP: block_count exceeds limit");
            if (exception_count > block_count) throw std::runtime_error("ALP: exception_count exceeds block_count");
            const int64_t for_base = std::bit_cast<int64_t>(bh1);

            // Read first_value for delta scheme (part of block header)
            int64_t first_value = 0;
            if (scheme == alp::SCHEME_ALP_DELTA) {
                first_value = std::bit_cast<int64_t>(encoded.readFixed<uint64_t, 64>());
            }

            // === Read FFOR Data ===
            const size_t packed_words = alp::ffor_packed_words(block_count, bw);

            // Fast path: if this entire sub-block is in the skip range,
            // advance the stream past the packed data and exceptions without unpacking.
            if (global_pos + block_count <= nToSkip) {
                // Skip FFOR packed words + exception data in bulk
                size_t skip_words = packed_words;
                if (exception_count > 0) [[unlikely]] {
                    skip_words += (exception_count * 2 + 7) / 8;  // position words
                    skip_words += exception_count;                // value words
                }
                encoded.skipWords(skip_words);
                global_pos += block_count;
                continue;
            }

            // Bulk read FFOR packed data into thread-local buffer, bypassing the
            // per-word readFixed state machine (~6 ops/word → single memcpy).
            auto& scratch = getScratch();
            scratch.packed_data.resize(packed_words);
            encoded.readAlignedWords(scratch.packed_data.data(), packed_words);

            // Unpack integers into thread-local buffer (capacity persists)
            scratch.decoded_ints.resize(block_count);
            alp::ffor_unpack(scratch.packed_data.data(), block_count, for_base, bw, scratch.decoded_ints.data());

            // Convenient aliases for the per-block data
            auto* decoded_stack = scratch.decoded_ints.data();
            auto* exc_positions_stack = scratch.exc_positions.data();
            auto* exc_values_stack = scratch.exc_values.data();

            // === Read Exceptions ===
            if (exception_count > 0) [[unlikely]] {
                scratch.exc_positions.resize(exception_count);
                scratch.exc_values.resize(exception_count);
                exc_positions_stack = scratch.exc_positions.data();
                exc_values_stack = scratch.exc_values.data();

                const size_t pos_words = (exception_count * 2 + 7) / 8;
                scratch.pos_word_buf.resize(pos_words);
                encoded.readAlignedWords(scratch.pos_word_buf.data(), pos_words);

                // Unpack positions from words
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = scratch.pos_word_buf[w];
                    const size_t base_idx = w * 4;
                    const size_t remaining = exception_count - base_idx;
                    const size_t count = remaining < 4 ? remaining : 4;
                    for (size_t j = 0; j < count; ++j) {
                        exc_positions_stack[base_idx + j] = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                    }
                }

                // Bulk read exception values
                encoded.readAlignedWords(exc_values_stack, exception_count);
            }

            // === Prefix-Sum Reconstruction (SCHEME_ALP_DELTA only) ===
            if (scheme == alp::SCHEME_ALP_DELTA) {
                int64_t running = first_value;
                size_t exc_scan = 0;
                bool is_first = true;

                for (size_t i = 0; i < block_count; ++i) {
                    if (exc_scan < exception_count && exc_positions_stack[exc_scan] == i) [[unlikely]] {
                        exc_scan++;
                        continue;
                    }
                    if (is_first) [[unlikely]] {
                        decoded_stack[i] = first_value;
                        running = first_value;
                        is_first = false;
                    } else {
                        uint64_t zz = static_cast<uint64_t>(decoded_stack[i]);
                        int64_t delta = static_cast<int64_t>((zz >> 1) ^ -(zz & 1));
                        running += delta;
                        decoded_stack[i] = running;
                    }
                }
            }

            // === Convert to doubles and apply skip/limit ===
            // Cache the scaling constants (avoids repeated array lookups)
            const double frac_val = alp::FRAC_ARR[fac];
            const double fact_val = alp::FACT_ARR[exp];

            if (exception_count == 0) [[likely]] {
                // Fast path: no exceptions -- tight loop, no exception checking
                const int64_t* __restrict__ decoded = decoded_stack;

                // Skip values before nToSkip
                size_t i = 0;
                if (global_pos < nToSkip) {
                    i = nToSkip - global_pos;
                }

                // Emit values until block ends or output is full
                const size_t emit_end = std::min(static_cast<size_t>(block_count), i + output_remaining);
                for (; i < emit_end; ++i) {
                    // Must preserve original evaluation order: (int * frac) / fact
                    // to maintain bit-exact round-trip with the encoder
                    *output_ptr++ = static_cast<double>(decoded[i]) * frac_val / fact_val;
                }
                output_remaining = length - static_cast<size_t>(output_ptr - (out.data() + current_size));
                global_pos += block_count;
            } else {
                // Slow path: has exceptions
                size_t exc_idx = 0;

                for (size_t i = 0; i < block_count; ++i) {
                    double value;

                    // Check if this position is an exception
                    if (exc_idx < exception_count && exc_positions_stack[exc_idx] == i) [[unlikely]] {
                        value = std::bit_cast<double>(exc_values_stack[exc_idx]);
                        exc_idx++;
                    } else {
                        value = static_cast<double>(decoded_stack[i]) * frac_val / fact_val;
                    }

                    if (global_pos >= nToSkip) [[likely]] {
                        *output_ptr++ = value;
                        output_remaining--;
                        if (output_remaining == 0) [[unlikely]] {
                            break;
                        }
                    }
                    global_pos++;
                }
            }

        } else if (scheme == alp::SCHEME_ALP_RD) {
            // === ALP_RD Block ===
            uint64_t bh0 = encoded.readFixed<uint64_t, 64>();
            uint64_t bh1 = encoded.readFixed<uint64_t, 64>();

            const uint8_t right_bw = static_cast<uint8_t>(bh0 & 0xFF);
            const uint8_t left_bw = static_cast<uint8_t>((bh0 >> 8) & 0xFF);
            const uint8_t dict_size = static_cast<uint8_t>((bh0 >> 16) & 0xFF);
            const uint16_t exception_count = static_cast<uint16_t>((bh0 >> 32) & 0xFFFF);
            const uint16_t block_count = static_cast<uint16_t>((bh0 >> 48) & 0xFFFF);
            if (dict_size > 8) throw std::runtime_error("ALP_RD: dict_size exceeds limit");
            if (right_bw > 64) throw std::runtime_error("ALP_RD: invalid right bit width");
            if (left_bw > 64) throw std::runtime_error("ALP_RD: invalid left bit width");
            if (block_count > 1024) throw std::runtime_error("ALP_RD: block_count exceeds limit");
            if (exception_count > block_count) throw std::runtime_error("ALP: exception_count exceeds block_count");
            const uint64_t right_for_base = bh1;

            // === Read Dictionary ===
            // (Must always read to advance stream position)
            scratch.dictionary.resize(dict_size);
            for (size_t i = 0; i < dict_size; ++i) {
                scratch.dictionary[i] = encoded.readFixed<uint64_t, 64>();
            }

            // Fast path: if this entire sub-block is in the skip range,
            // advance the stream past the packed data and exceptions without unpacking.
            if (global_pos + block_count <= nToSkip) {
                size_t skip_words = 0;
                if (left_bw > 0)
                    skip_words += alp::ffor_packed_words(block_count, left_bw);
                if (right_bw > 0)
                    skip_words += alp::ffor_packed_words(block_count, right_bw);
                if (exception_count > 0) [[unlikely]] {
                    skip_words += (exception_count * 2 + 7) / 8;
                    skip_words += exception_count;
                }
                encoded.skipWords(skip_words);
                global_pos += block_count;
                continue;
            }

            // === Read Left Indices (FFOR packed) ===
            alignas(8) int64_t left_indices_stack[1024];
            std::fill_n(left_indices_stack, block_count, int64_t{0});
            if (left_bw > 0) {
                size_t left_packed_words = alp::ffor_packed_words(block_count, left_bw);
                alignas(8) uint64_t left_packed_stack[1024];
                encoded.readAlignedWords(left_packed_stack, left_packed_words);
                alp::ffor_unpack(left_packed_stack, block_count, 0, left_bw, left_indices_stack);
            }

            // === Read Right FFOR Data ===
            alignas(8) uint64_t right_parts_stack[1024];
            std::fill_n(right_parts_stack, block_count, right_for_base);
            if (right_bw > 0) {
                size_t right_packed_words = alp::ffor_packed_words(block_count, right_bw);
                alignas(8) uint64_t right_packed_stack[1024];
                encoded.readAlignedWords(right_packed_stack, right_packed_words);
                alp::ffor_unpack_u64(right_packed_stack, block_count, right_for_base, right_bw, right_parts_stack);
            }

            // === Read Exceptions ===
            alignas(8) uint16_t rd_exc_positions_stack[1024];
            alignas(8) uint64_t rd_exc_values_stack[1024];
            if (exception_count > 0) [[unlikely]] {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                alignas(8) uint64_t pos_word_buf[256];
                encoded.readAlignedWords(pos_word_buf, pos_words);
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = pos_word_buf[w];
                    const size_t base_idx = w * 4;
                    const size_t remaining = exception_count - base_idx;
                    const size_t cnt = remaining < 4 ? remaining : 4;
                    for (size_t j = 0; j < cnt; ++j) {
                        rd_exc_positions_stack[base_idx + j] = static_cast<uint16_t>((word >> (j * 16)) & 0xFFFF);
                    }
                }
                encoded.readAlignedWords(rd_exc_values_stack, exception_count);
            }

            // === Reconstruct doubles ===
            const uint8_t right_bit_count = static_cast<uint8_t>((bh0 >> 24) & 0xFF);

            const uint64_t right_mask = (right_bit_count == 64) ? ~0ULL : ((1ULL << right_bit_count) - 1);

            if (exception_count == 0) [[likely]] {
                // Fast path: no exceptions -- tight reconstruction loop
                const int64_t* __restrict__ left_idx = left_indices_stack;
                const uint64_t* __restrict__ right = right_parts_stack;
                const uint64_t* __restrict__ dict = scratch.dictionary.data();

                // Skip values before nToSkip
                size_t i = 0;
                if (global_pos < nToSkip) {
                    i = nToSkip - global_pos;
                }

                // Emit values until block ends or output is full
                const size_t emit_end = std::min(static_cast<size_t>(block_count), i + output_remaining);
                for (; i < emit_end; ++i) {
                    auto lidx = static_cast<uint8_t>(left_idx[i]);
                    if (lidx >= dict_size) [[unlikely]]
                        throw std::runtime_error("ALP_RD decoder: dictionary index out of range");
                    uint64_t left = dict[lidx];
                    uint64_t r = right[i] & right_mask;
                    uint64_t combined = (left << right_bit_count) | r;
                    *output_ptr++ = std::bit_cast<double>(combined);
                }
                output_remaining = length - static_cast<size_t>(output_ptr - (out.data() + current_size));
                global_pos += block_count;
            } else {
                // Slow path: has exceptions
                size_t exc_idx = 0;
                for (size_t i = 0; i < block_count; ++i) {
                    double value;

                    if (exc_idx < exception_count && rd_exc_positions_stack[exc_idx] == i) [[unlikely]] {
                        value = std::bit_cast<double>(rd_exc_values_stack[exc_idx]);
                        exc_idx++;
                    } else {
                        auto lidx = static_cast<uint8_t>(left_indices_stack[i]);
                        if (lidx >= dict_size) [[unlikely]]
                            throw std::runtime_error("ALP_RD decoder: dictionary index out of range");
                        uint64_t left = scratch.dictionary[lidx];
                        uint64_t right = right_parts_stack[i] & right_mask;
                        uint64_t combined = (left << right_bit_count) | right;
                        value = std::bit_cast<double>(combined);
                    }

                    if (global_pos >= nToSkip) [[likely]] {
                        *output_ptr++ = value;
                        output_remaining--;
                        if (output_remaining == 0) [[unlikely]] {
                            break;
                        }
                    }
                    global_pos++;
                }
            }
        } else [[unlikely]] {
            throw std::runtime_error("ALPDecoder: unknown ALP scheme");
        }
    }

    // If we decoded fewer than requested (shouldn't happen with valid data), trim
    if (output_remaining > 0) [[unlikely]] {
        out.resize(required - output_remaining);
    }
}
