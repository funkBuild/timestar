#include "alp_encoder.hpp"

#include "../../storage/aligned_buffer.hpp"
#include "alp_constants.hpp"
#include "alp_ffor.hpp"
#include "alp_rd.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <vector>

namespace {

// Result of encoding a single value with a given (exp, fac) pair
struct ScaleResult {
    int64_t encoded;
    bool exact;  // true if round-trip is lossless
};

// Scale a double to int64 using: round(value * 10^exp) then integer-divide by 10^fac
// Verify round-trip: (encoded * 10^fac) / 10^exp == original value
inline ScaleResult scaleValue(double value, uint8_t exp, uint8_t fac) {
    // Multiply by 10^exp
    double scaled = value * alp::FACT_ARR[exp];

    // Round to nearest integer
    double rounded = std::round(scaled);

    // Check for overflow before casting
    if (rounded > static_cast<double>(alp::MAX_SAFE_INT) || rounded < static_cast<double>(alp::MIN_SAFE_INT)) {
        return {0, false};
    }

    int64_t encoded = static_cast<int64_t>(rounded);

    // Apply factoring: divide by 10^fac (integer division)
    if (fac > 0) {
        int64_t factor = static_cast<int64_t>(alp::FACT_ARR[fac]);
        encoded = encoded / factor;
    }

    // Verify round-trip
    double decoded = static_cast<double>(encoded) * alp::FRAC_ARR[fac] / alp::FACT_ARR[exp];
    bool exact = (decoded == value);

    return {encoded, exact};
}

// Find the best (exp, fac) pair by sampling values
struct BestPair {
    uint8_t exp = 0;
    uint8_t fac = 0;
    size_t exceptions = 0;  // number of non-roundtrippable values in sample
};

BestPair findBestExpFac(const double* values, size_t count) {
    const size_t sample_size = std::min(count, alp::ALP_SAMPLE_SIZE);

    BestPair best;
    best.exceptions = sample_size + 1;

    // Build sample indices (evenly spaced)
    std::vector<size_t> sample_indices(sample_size);
    if (sample_size == count) {
        for (size_t i = 0; i < count; ++i)
            sample_indices[i] = i;
    } else {
        for (size_t i = 0; i < sample_size; ++i) {
            sample_indices[i] = (i * count) / sample_size;
        }
    }

    // exp >= fac is required (exp is the total decimal digits, fac is the integer part)
    for (uint8_t exp = 0; exp < alp::EXP_COUNT; ++exp) {
        for (uint8_t fac = 0; fac <= exp; ++fac) {
            size_t exceptions = 0;

            for (size_t idx : sample_indices) {
                double v = values[idx];
                // Skip special values - they'll always be exceptions
                if (std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v))) {
                    exceptions++;
                    continue;
                }
                auto result = scaleValue(v, exp, fac);
                if (!result.exact) {
                    exceptions++;
                }
            }

            if (exceptions < best.exceptions) {
                best.exp = exp;
                best.fac = fac;
                best.exceptions = exceptions;
            }

            // Perfect match - no need to keep searching
            if (exceptions == 0) {
                return best;
            }
        }
    }

    return best;
}

// Calculate required bit width for a range of int64 values
uint8_t requiredBitWidth(int64_t min_val, int64_t max_val) {
    if (min_val == max_val)
        return 0;
    // Use unsigned arithmetic to avoid signed overflow UB when range exceeds INT64_MAX
    uint64_t range = static_cast<uint64_t>(max_val) - static_cast<uint64_t>(min_val);
    if (range == 0)
        return 0;
    return static_cast<uint8_t>(64 - __builtin_clzll(range));
}

}  // anonymous namespace

CompressedBuffer ALPEncoder::encode(std::span<const double> values) {
    // Delegate to encodeInto() to avoid maintaining two near-identical ~300-line
    // encoding implementations. The AlignedBuffer→CompressedBuffer conversion
    // copies the raw words, negligible compared to the encoding work.
    AlignedBuffer aligned;
    encodeInto(values, aligned);

    CompressedBuffer buffer;
    const size_t numBytes = aligned.size();
    if (numBytes > 0) {
        // Copy AlignedBuffer's raw bytes into CompressedBuffer as 64-bit words.
        const size_t fullWords = numBytes / 8;
        const size_t tailBytes = numBytes % 8;
        buffer.reserve(fullWords + (tailBytes > 0 ? 1 : 0));

        const uint8_t* src = aligned.data.data();
        for (size_t i = 0; i < fullWords; ++i) {
            uint64_t word;
            std::memcpy(&word, src + i * 8, 8);
            buffer.write<64>(word);
        }
        if (tailBytes > 0) {
            uint64_t word = 0;
            std::memcpy(&word, src + fullWords * 8, tailBytes);
            buffer.write(word, static_cast<int>(tailBytes * 8));
        }
    }
    buffer.shrink_to_fit();
    return buffer;
}

// Legacy encode() removed -- see git history for reference implementation.

size_t ALPEncoder::encodeInto(std::span<const double> values, AlignedBuffer& target) {
    if (values.empty()) {
        return 0;
    }

    const size_t startPos = target.size();

    const size_t total_values = values.size();
    const size_t num_blocks = (total_values + alp::ALP_VECTOR_SIZE - 1) / alp::ALP_VECTOR_SIZE;
    if (num_blocks > UINT16_MAX)
        throw std::overflow_error("ALP: too many blocks for 16-bit header field");
    const size_t tail_count = total_values % alp::ALP_VECTOR_SIZE;

    // Determine scheme: try ALP first, fall back to ALP_RD
    auto best = findBestExpFac(values.data(), total_values);
    double exception_rate = static_cast<double>(best.exceptions) / std::min(total_values, alp::ALP_SAMPLE_SIZE);

    uint8_t scheme = (exception_rate > alp::ALP_RD_EXCEPTION_THRESHOLD) ? alp::SCHEME_ALP_RD : alp::SCHEME_ALP;

    // Delta-benefit heuristic: sample first block to check if delta reduces bit width
    if (scheme == alp::SCHEME_ALP) {
        const size_t sample_count = std::min(total_values, alp::ALP_VECTOR_SIZE);
        int64_t abs_min = std::numeric_limits<int64_t>::max();
        int64_t abs_max = std::numeric_limits<int64_t>::min();
        uint64_t zz_min = UINT64_MAX;
        uint64_t zz_max = 0;
        int64_t prev = 0;
        bool first_valid = true;

        for (size_t i = 0; i < sample_count; ++i) {
            double v = values[i];
            if (std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v)))
                continue;
            auto result = scaleValue(v, best.exp, best.fac);
            if (!result.exact)
                continue;

            if (result.encoded < abs_min)
                abs_min = result.encoded;
            if (result.encoded > abs_max)
                abs_max = result.encoded;

            if (!first_valid) {
                int64_t delta = result.encoded - prev;
                uint64_t zz = (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
                if (zz < zz_min)
                    zz_min = zz;
                if (zz > zz_max)
                    zz_max = zz;
            }
            first_valid = false;
            prev = result.encoded;
        }

        uint8_t abs_bw = requiredBitWidth(abs_min, abs_max);
        uint8_t delta_bw;
        if (zz_min <= zz_max) {
            uint64_t zz_range = zz_max - zz_min;
            delta_bw = (zz_range == 0) ? 0 : static_cast<uint8_t>(64 - __builtin_clzll(zz_range));
        } else {
            delta_bw = abs_bw;
        }
        if (delta_bw < abs_bw) {
            scheme = alp::SCHEME_ALP_DELTA;
        }
    }

    // Pre-allocate estimated space in the target buffer (rough upper bound in bytes)
    const size_t est_bytes = (2 + num_blocks * (2 + alp::ALP_VECTOR_SIZE + 128)) * sizeof(uint64_t);
    target.reserve(startPos + est_bytes);

    // === Stream Header (2 x uint64_t) ===
    // Word 0: [0:31] magic, [32:63] total_values (max 2^32-1)
    if (total_values > UINT32_MAX) {
        throw std::overflow_error("ALP encoder: total_values " + std::to_string(total_values) +
                                  " exceeds 32-bit header capacity");
    }
    uint64_t header0 = static_cast<uint64_t>(alp::ALP_MAGIC) | (static_cast<uint64_t>(total_values) << 32);
    target.write(header0);

    // Word 1: [0:15] num_blocks, [16:31] tail_count, [32:39] scheme
    uint64_t header1 = static_cast<uint64_t>(num_blocks) | (static_cast<uint64_t>(tail_count) << 16) |
                       (static_cast<uint64_t>(scheme) << 32);
    target.write(header1);

    if (scheme == alp::SCHEME_ALP || scheme == alp::SCHEME_ALP_DELTA) {
        // === ALP Encoding (with optional delta) ===
        const uint8_t exp = best.exp;
        const uint8_t fac = best.fac;

        // Scratch buffers hoisted out of the block loop to avoid per-block heap allocations.
        std::vector<int64_t> encoded(alp::ALP_VECTOR_SIZE);
        std::vector<uint16_t> exc_positions;
        std::vector<uint64_t> exc_values;
        std::vector<uint64_t> packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * alp::ALP_VECTOR_SIZE;
            const size_t block_count = (block == num_blocks - 1 && tail_count > 0) ? tail_count : alp::ALP_VECTOR_SIZE;

            // Reuse scratch buffers (resize is no-op when <= capacity)
            encoded.resize(block_count);
            exc_positions.clear();
            exc_values.clear();

            int64_t min_val = std::numeric_limits<int64_t>::max();
            int64_t max_val = std::numeric_limits<int64_t>::min();
            int64_t first_value = 0;  // absolute value of first non-exception (for delta)

            for (size_t i = 0; i < block_count; ++i) {
                double v = values[block_start + i];
                bool is_special = std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v));  // -0.0

                if (!is_special) {
                    auto result = scaleValue(v, exp, fac);
                    if (result.exact) {
                        encoded[i] = result.encoded;
                        if (result.encoded < min_val)
                            min_val = result.encoded;
                        if (result.encoded > max_val)
                            max_val = result.encoded;
                        continue;
                    }
                }

                // Exception: store as raw bits
                exc_positions.push_back(static_cast<uint16_t>(i));
                exc_values.push_back(std::bit_cast<uint64_t>(v));
                encoded[i] = 0;  // placeholder (will use base after FFOR)
            }

            // === Delta + Zigzag Transform (SCHEME_ALP_DELTA only) ===
            if (scheme == alp::SCHEME_ALP_DELTA) {
                // Find first non-exception and save its absolute value
                size_t exc_scan = 0;
                size_t first_non_exc = block_count;
                for (size_t i = 0; i < block_count; ++i) {
                    if (exc_scan < exc_positions.size() && exc_positions[exc_scan] == i) {
                        exc_scan++;
                        continue;
                    }
                    first_non_exc = i;
                    first_value = encoded[i];
                    break;
                }

                // Forward scan: compute deltas and zigzag encode
                if (first_non_exc < block_count) {
                    int64_t prev_abs = encoded[first_non_exc];
                    exc_scan = 0;
                    while (exc_scan < exc_positions.size() && exc_positions[exc_scan] <= first_non_exc) {
                        exc_scan++;
                    }
                    for (size_t i = first_non_exc + 1; i < block_count; ++i) {
                        if (exc_scan < exc_positions.size() && exc_positions[exc_scan] == i) {
                            exc_scan++;
                            continue;
                        }
                        int64_t cur = encoded[i];
                        int64_t delta = cur - prev_abs;
                        prev_abs = cur;
                        uint64_t zz = (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
                        encoded[i] = static_cast<int64_t>(zz);
                    }
                    encoded[first_non_exc] = 0;  // zigzag(0) = 0
                }

                // Recalculate min/max from zigzag values
                min_val = std::numeric_limits<int64_t>::max();
                max_val = std::numeric_limits<int64_t>::min();
                exc_scan = 0;
                for (size_t i = 0; i < block_count; ++i) {
                    if (exc_scan < exc_positions.size() && exc_positions[exc_scan] == i) {
                        exc_scan++;
                        continue;
                    }
                    if (encoded[i] < min_val)
                        min_val = encoded[i];
                    if (encoded[i] > max_val)
                        max_val = encoded[i];
                }
            }

            // If all values are exceptions, set a safe base
            if (min_val > max_val) {
                min_val = 0;
                max_val = 0;
            }

            // Set exception placeholders to base so they don't affect bit width
            for (auto pos : exc_positions) {
                encoded[pos] = min_val;
            }

            const uint8_t bw = requiredBitWidth(min_val, max_val);
            const uint16_t exception_count = static_cast<uint16_t>(exc_positions.size());

            // === Block Header (2 x uint64_t) ===
            uint64_t bh0 = static_cast<uint64_t>(exp) | (static_cast<uint64_t>(fac) << 8) |
                           (static_cast<uint64_t>(bw) << 16) | (static_cast<uint64_t>(exception_count) << 32) |
                           (static_cast<uint64_t>(block_count) << 48);
            target.write(bh0);

            // Word 1: FOR base
            target.write(std::bit_cast<uint64_t>(min_val));

            // Word 2: first_value (SCHEME_ALP_DELTA only)
            if (scheme == alp::SCHEME_ALP_DELTA) {
                target.write(std::bit_cast<uint64_t>(first_value));
            }

            // === FFOR Data ===
            size_t packed_words = alp::ffor_packed_words(block_count, bw);
            if (packed_words > 0) {
                packed.assign(packed_words, 0);
                alp::ffor_pack(encoded.data(), block_count, min_val, bw, packed.data());
                target.write_array(packed.data(), packed_words);
            }

            // === Exception Positions (packed to word boundary) ===
            if (exception_count > 0) {
                // Pack uint16 positions into uint64 words (4 per word)
                size_t pos_words = (exception_count * 2 + 7) / 8;
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = 0;
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) {
                            word |= static_cast<uint64_t>(exc_positions[idx]) << (j * 16);
                        }
                    }
                    target.write(word);
                }

                // === Exception Values (one word each) ===
                target.write_array(exc_values.data(), exception_count);
            }
        }
    } else {
        // === ALP_RD Encoding ===
        uint8_t right_bit_count = alp::ALPRD::findBestSplit(values.data(), total_values);

        // Scratch buffers hoisted out of the block loop
        std::vector<int64_t> left_as_i64(alp::ALP_VECTOR_SIZE);
        std::vector<uint64_t> left_packed;
        std::vector<uint64_t> right_packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * alp::ALP_VECTOR_SIZE;
            const size_t block_count = (block == num_blocks - 1 && tail_count > 0) ? tail_count : alp::ALP_VECTOR_SIZE;

            auto rd = alp::ALPRD::encodeBlock(values.data() + block_start, block_count, right_bit_count);
            const uint16_t exception_count = static_cast<uint16_t>(rd.exception_positions.size());

            // === Block Header (2 x uint64_t) ===
            uint64_t bh0 = static_cast<uint64_t>(rd.right_bw) | (static_cast<uint64_t>(rd.left_bw) << 8) |
                           (static_cast<uint64_t>(rd.dictionary.size()) << 16) |
                           (static_cast<uint64_t>(right_bit_count) << 24) |
                           (static_cast<uint64_t>(exception_count) << 32) | (static_cast<uint64_t>(block_count) << 48);
            target.write(bh0);

            // Word 1: right FOR base
            target.write(rd.right_for_base);

            // === Dictionary ===
            if (!rd.dictionary.empty()) {
                target.write_array(rd.dictionary.data(), rd.dictionary.size());
            }

            // === Left Indices (FFOR packed) ===
            if (rd.left_bw > 0) {
                left_as_i64.resize(block_count);
                for (size_t i = 0; i < block_count; ++i) {
                    left_as_i64[i] = static_cast<int64_t>(rd.left_indices[i]);
                }
                size_t left_packed_words = alp::ffor_packed_words(block_count, rd.left_bw);
                left_packed.assign(left_packed_words, 0);
                alp::ffor_pack(left_as_i64.data(), block_count, 0, rd.left_bw, left_packed.data());
                target.write_array(left_packed.data(), left_packed_words);
            }

            // === Right FFOR Data ===
            if (rd.right_bw > 0) {
                size_t right_packed_words = alp::ffor_packed_words(block_count, rd.right_bw);
                right_packed.assign(right_packed_words, 0);
                alp::ffor_pack_u64(rd.right_parts.data(), block_count, rd.right_for_base, rd.right_bw,
                                   right_packed.data());
                target.write_array(right_packed.data(), right_packed_words);
            }

            // === Exception Positions ===
            if (exception_count > 0) {
                size_t pos_words = (exception_count * 2 + 7) / 8;
                for (size_t w = 0; w < pos_words; ++w) {
                    uint64_t word = 0;
                    for (size_t j = 0; j < 4; ++j) {
                        size_t idx = w * 4 + j;
                        if (idx < exception_count) {
                            word |= static_cast<uint64_t>(rd.exception_positions[idx]) << (j * 16);
                        }
                    }
                    target.write(word);
                }

                // === Exception Values ===
                target.write_array(rd.exception_values.data(), exception_count);
            }
        }
    }

    return target.size() - startPos;
}
