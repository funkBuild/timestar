#include "alp_encoder.hpp"

#include "../../storage/aligned_buffer.hpp"
#include "alp_constants.hpp"
#include "alp_ffor.hpp"
#include "alp_param_search.hpp"
#include "alp_rd.hpp"
#include "alp_simd.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstring>
#include <limits>
#include <optional>
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
    double scaled = value * timestar::alp::FACT_ARR[exp];

    // Round to nearest integer
    double rounded = std::round(scaled);

    // Check for overflow before casting
    if (rounded > static_cast<double>(timestar::alp::MAX_SAFE_INT) ||
        rounded < static_cast<double>(timestar::alp::MIN_SAFE_INT)) {
        return {0, false};
    }

    int64_t encoded = static_cast<int64_t>(rounded);

    // Apply factoring: divide by 10^fac (integer division)
    if (fac > 0) {
        int64_t factor = static_cast<int64_t>(timestar::alp::FACT_ARR[fac]);
        encoded = encoded / factor;
    }

    // Verify round-trip
    double decoded = static_cast<double>(encoded) * timestar::alp::FRAC_ARR[fac] / timestar::alp::FACT_ARR[exp];
    bool exact = (decoded == value);

    return {encoded, exact};
}

// Largest |round(value * 10^exp)| for which the reasoning in findBestExpFac
// holds; at or above it a sample is evaluated candidate by candidate instead.
constexpr double PROVABLE_SCALE_LIMIT = static_cast<double>(1LL << 50);

struct Fac0Result {
    bool exact;  // round-trips under (exp, fac = 0)
    bool gray;   // in range, but too large for the fac > 0 shortcut
};

// scaleValue(value, exp, 0) reduced to what the search needs. The arithmetic
// is kept identical so the search and the encoder never disagree.
inline Fac0Result scaleFac0(double value, uint8_t exp) {
    const double rounded = std::round(value * timestar::alp::FACT_ARR[exp]);
    if (rounded > static_cast<double>(timestar::alp::MAX_SAFE_INT) ||
        rounded < static_cast<double>(timestar::alp::MIN_SAFE_INT)) {
        return {false, false};  // out of range: an exception for every fac
    }
    const double decoded =
        static_cast<double>(static_cast<int64_t>(rounded)) * timestar::alp::FRAC_ARR[0] / timestar::alp::FACT_ARR[exp];
    return {decoded == value, std::abs(rounded) >= PROVABLE_SCALE_LIMIT};
}

inline bool isAlpSpecial(double v) {
    return std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v));
}

}  // anonymous namespace

namespace timestar::alp {

// The reference behaviour is a brute-force scan: for each exp ascending, for
// each fac in [0, exp], count the sampled values that fail to round-trip, and
// keep the first pair with the fewest failures. That is up to 190 pairs x 256
// samples of scale-round-divide-compare per TSM block and per WAL entry, and
// it only stops early when a pair has NO failures. Clean decimal data stops
// after a handful of pairs; anything else — full-precision doubles, float32
// values widened to double, decimals with a few outliers — pays for the whole
// grid, which made the search an order of magnitude more expensive than the
// encode it configures.
//
// This search returns the same pair with a fraction of the work, using three
// facts:
//
// 1. NaN, +-Inf and -0.0 fail under every pair, so they are counted once and
//    dropped from the sample.
//
// 2. A stream is only ALP-encoded when at most half the sample fails. A pair
//    past that budget can never be used (ALP_RD ignores exp/fac), so it is
//    abandoned there rather than counted to the end.
//
// 3. fac > 0 cannot rescue a value that fails at fac == 0. Write R for
//    round(value * 10^exp). With fac = f the encoder stores trunc(R / 10^f)
//    and decodes fl(M / 10^exp), M = trunc(R / 10^f) * 10^f. If M == R this is
//    the fac == 0 computation. If M != R and the decode still returned value,
//    then value = fl(M / 10^exp), so fl(value * 10^exp) is within
//    |M| * 2^-52 of M — under 0.25 while |M| <= |R| < 2^50 — and rounds to M,
//    contradicting R != M. So for |R| < 2^50 every failure at (exp, 0) is a
//    failure at (exp, f), and (exp, f) can never beat (exp, 0), which is tried
//    first. A whole exp is dismissed on its fac == 0 failures alone; fac > 0
//    is only evaluated for an exp where some sample has |R| >= 2^50 ("gray"),
//    and there it is evaluated directly, exactly as before.
//
// Samples that failed are moved to the front as they are found, so the next
// candidate meets its likeliest failures first and is dismissed quickly.
AlpParams findBestExpFac(const double* values, size_t count) {
    const size_t sample_size = std::min(count, ALP_SAMPLE_SIZE);
    static_assert(ALP_SAMPLE_SIZE <= 256, "sample stack buffer sized for ALP_SAMPLE_SIZE");

    // Evenly spaced sample, special values removed (fact 1).
    std::array<double, 256> sample;
    size_t n = 0;
    for (size_t i = 0; i < sample_size; ++i) {
        const double v = values[sample_size == count ? i : (i * count) / sample_size];
        if (!isAlpSpecial(v)) {
            sample[n++] = v;
        }
    }
    const size_t specials = sample_size - n;

    // Fact 2: non-special failures a pair may have and still be used.
    const size_t max_total = sample_size / 2;
    if (specials > max_total) {
        return {0, 0, sample_size};
    }
    const size_t budget = max_total - specials;

    auto pairIndex = [](uint8_t exp, uint8_t fac) { return static_cast<size_t>(exp) * (exp + 1) / 2 + fac; };

    // Count failures of (exp, fac) over the whole sample, stopping once
    // `giveUp(failures)` says the pair has lost.
    auto countDirect = [&](uint8_t exp, uint8_t fac, auto&& giveUp) {
        size_t failures = 0;
        for (size_t i = 0; i < n && !giveUp(failures); ++i) {
            if (!scaleValue(sample[i], exp, fac).exact) {
                std::swap(sample[i], sample[failures++]);
            }
        }
        return failures;
    };

    // ---- Pass 1: the first pair that fails on nothing ----
    // Each exp is dropped at its first provable failure, so data that scales
    // cleanly costs one sweep of the sample plus a probe per rejected exp.
    std::array<uint16_t, EXP_COUNT> clean_run{};
    for (uint8_t exp = 0; exp < EXP_COUNT; ++exp) {
        bool gray_failure = false;
        size_t i = 0;
        for (; i < n; ++i) {
            const auto r = scaleFac0(sample[i], exp);
            if (r.exact) {
                continue;
            }
            std::swap(sample[i], sample[0]);
            if (!r.gray) {
                break;  // fact 3: no fac can fix this exp
            }
            gray_failure = true;
        }
        clean_run[exp] = static_cast<uint16_t>(i);
        if (i < n) {
            continue;
        }
        if (!gray_failure) {
            return {exp, 0, specials};
        }
        for (uint8_t fac = 1; fac <= exp; ++fac) {
            if (countDirect(exp, fac, [](size_t failures) { return failures > 0; }) == 0) {
                return {exp, fac, specials};
            }
        }
    }

    // ---- Pass 2: fewest failures, earliest pair on ties ----
    // No pair exists yet, so the incumbent is "one over budget, ordered before
    // everything": only a pair within budget can replace it.
    size_t best_failures = budget + 1;
    size_t best_index = 0;
    AlpParams best{0, 0, sample_size};
    bool found = false;

    auto cannotWin = [&](size_t failures, size_t index) {
        return failures > best_failures || (failures == best_failures && (!found || index > best_index));
    };
    auto offer = [&](uint8_t exp, uint8_t fac, size_t failures) {
        if (!cannotWin(failures, pairIndex(exp, fac))) {
            best_failures = failures;
            best_index = pairIndex(exp, fac);
            best = {exp, fac, specials + failures};
            found = true;
        }
    };

    auto evaluateExp = [&](uint8_t exp) {
        const size_t index0 = pairIndex(exp, 0);
        size_t provable = 0;  // failures every fac shares (fact 3)
        size_t gray_failures = 0;
        size_t failures = 0;
        bool any_gray = false;
        for (size_t i = 0; i < n; ++i) {
            const auto r = scaleFac0(sample[i], exp);
            any_gray |= r.gray;
            if (r.exact) {
                continue;
            }
            std::swap(sample[i], sample[failures++]);
            if (r.gray) {
                ++gray_failures;
            } else if (cannotWin(++provable, index0)) {
                return;  // every pair of this exp has at least `provable` failures
            }
        }
        offer(exp, 0, provable + gray_failures);
        if (any_gray) {
            for (uint8_t fac = 1; fac <= exp; ++fac) {
                const size_t index = pairIndex(exp, fac);
                const size_t f = countDirect(exp, fac, [&](size_t so_far) { return cannotWin(so_far, index); });
                offer(exp, fac, f);
            }
        }
    };

    // Start with the exp that got furthest in pass 1: it is the likeliest
    // winner, and a tight incumbent lets every other exp be dismissed after a
    // few samples. The order pairs are tried in does not change the result.
    const uint8_t first =
        static_cast<uint8_t>(std::max_element(clean_run.begin(), clean_run.end()) - clean_run.begin());
    evaluateExp(first);
    for (uint8_t exp = 0; exp < EXP_COUNT; ++exp) {
        if (exp != first) {
            evaluateExp(exp);
        }
    }
    return best;
}

}  // namespace timestar::alp

namespace {

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
        // Bulk-copy AlignedBuffer's raw bytes into the word vector in one
        // memcpy (zero-padded tail word). The old word-by-word write<64>()
        // loop cost ~5ms per 1M values — comparable to the encode itself.
        // Range-assign copies the full words in a single pass with no
        // preparatory zero-fill (assign(n, 0) memset several MB per call);
        // only a partial tail word needs explicit zero padding.
        // CompressedBuffer::size() derives from data.size(), and consumers
        // read via CompressedSlice / serialize the bytes, so filling `data`
        // directly is equivalent.
        const size_t fullWords = numBytes / 8;
        const uint64_t* words = reinterpret_cast<const uint64_t*>(aligned.data.data());
        buffer.data.assign(words, words + fullWords);
        if (numBytes % 8 != 0) {
            uint64_t last = 0;
            std::memcpy(&last, aligned.data.data() + fullWords * 8, numBytes % 8);
            buffer.data.push_back(last);
        }
    }
    return buffer;
}

// Legacy encode() removed -- see git history for reference implementation.

size_t ALPEncoder::encodeInto(std::span<const double> values, AlignedBuffer& target) {
    if (values.empty()) {
        return 0;
    }

    const size_t startPos = target.size();

    const size_t total_values = values.size();
    const size_t num_blocks = (total_values + timestar::alp::ALP_VECTOR_SIZE - 1) / timestar::alp::ALP_VECTOR_SIZE;
    if (num_blocks > UINT16_MAX)
        throw std::overflow_error("ALP: too many blocks for 16-bit header field");
    const size_t tail_count = total_values % timestar::alp::ALP_VECTOR_SIZE;

    // Determine scheme: try ALP first, fall back to ALP_RD
    const auto best = timestar::alp::findBestExpFac(values.data(), total_values);
    double exception_rate =
        static_cast<double>(best.exceptions) / std::min(total_values, timestar::alp::ALP_SAMPLE_SIZE);

    uint8_t scheme = (exception_rate > timestar::alp::ALP_RD_EXCEPTION_THRESHOLD) ? timestar::alp::SCHEME_ALP_RD
                                                                                  : timestar::alp::SCHEME_ALP;

    const uint8_t exp = best.exp;
    const uint8_t fac = best.fac;

    // Per-block scale state, reused across blocks (resize is a no-op once the
    // vectors have grown). Unused — and never allocated — under ALP_RD.
    std::vector<int64_t> encoded;
    std::vector<uint16_t> exc_positions;
    std::vector<uint64_t> exc_values;
    int64_t min_val = 0;
    int64_t max_val = 0;

    // Single-pass masked SIMD scale loop: only for fac == 0 (integer
    // divide by 10^fac has no matching SIMD equivalent) and only on
    // targets with native i64<->f64 conversion (AVX-512 DQ).
    const bool use_simd_scale = (fac == 0) && timestar::alp::simd::alpScaleSimdAvailable();

    // Scale one block: encoded[] holds the integers (placeholders in exception
    // slots), exc_* the exceptions in ascending position, min_val/max_val the
    // range of the exact values (INT64_MAX/INT64_MIN when there are none).
    auto scaleBlock = [&](size_t block_start, size_t block_count) {
        encoded.resize(block_count);
        min_val = std::numeric_limits<int64_t>::max();
        max_val = std::numeric_limits<int64_t>::min();
        if (use_simd_scale) {
            exc_positions.resize(block_count);
            exc_values.resize(block_count);
            const size_t n_exc = timestar::alp::simd::alpScaleF0(values.data() + block_start, block_count,
                                                                 timestar::alp::FACT_ARR[exp], encoded.data(), &min_val,
                                                                 &max_val, exc_positions.data(), exc_values.data());
            exc_positions.resize(n_exc);
            exc_values.resize(n_exc);
            return;
        }
        exc_positions.clear();
        exc_values.clear();
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
    };

    const size_t first_block_count = std::min(total_values, timestar::alp::ALP_VECTOR_SIZE);

    // ALP_RD state. Usually only touched in the ALP_RD branch below; the size
    // check may fill it early for the first block, which that branch reuses.
    timestar::alp::ALPRDBlockResult rd;
    std::optional<uint8_t> rd_right_bits;
    bool rd_first_block_ready = false;

    // Delta-benefit heuristic: does delta + zigzag narrow the first block?
    //
    // The answer comes from the first block's scaled integers. The encode loop
    // needs those same integers, so the block is scaled once here and reused
    // below — scaling it a second time, value by value, just to answer this
    // used to cost as much as encoding the block.
    //
    // One caveat keeps the decision identical to the value-by-value scan it
    // replaces. The SIMD kernel rounds halves to even; scaleValue rounds them
    // away from zero. Below 2^50 that cannot matter: a product that lands on a
    // half is at least 0.5 from either integer, and (as in findBestExpFac) a
    // value only round-trips when its product is within 0.25 of the integer,
    // so both kernels call it an exception and agree everywhere else. At or
    // above 2^50 they can store different integers, so those blocks take the
    // scalar scan.
    if (scheme == timestar::alp::SCHEME_ALP) {
        scaleBlock(0, first_block_count);

        uint64_t zz_min = UINT64_MAX;
        uint64_t zz_max = 0;
        int64_t abs_min = min_val;
        int64_t abs_max = max_val;
        int64_t prev = 0;
        bool first_valid = true;
        auto observe = [&](int64_t value) {
            if (!first_valid) {
                int64_t delta = value - prev;
                uint64_t zz = (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
                if (zz < zz_min)
                    zz_min = zz;
                if (zz > zz_max)
                    zz_max = zz;
            }
            first_valid = false;
            prev = value;
        };

        bool block_matches_scalar = !use_simd_scale;
        if (use_simd_scale) {
            double largest = 0.0;
            for (size_t i = 0; i < first_block_count; ++i) {
                const double magnitude = std::abs(values[i]);
                // NaN compares false and Inf is an exception in both kernels.
                if (magnitude > largest && magnitude <= std::numeric_limits<double>::max())
                    largest = magnitude;
            }
            block_matches_scalar = largest * timestar::alp::FACT_ARR[exp] < PROVABLE_SCALE_LIMIT - 2.0;
        }

        if (block_matches_scalar) {
            size_t exc_scan = 0;
            for (size_t i = 0; i < first_block_count; ++i) {
                if (exc_scan < exc_positions.size() && exc_positions[exc_scan] == i) {
                    ++exc_scan;
                    continue;
                }
                observe(encoded[i]);
            }
        } else {
            abs_min = std::numeric_limits<int64_t>::max();
            abs_max = std::numeric_limits<int64_t>::min();
            for (size_t i = 0; i < first_block_count; ++i) {
                double v = values[i];
                if (std::isnan(v) || std::isinf(v) || (v == 0.0 && std::signbit(v)))
                    continue;
                auto result = scaleValue(v, exp, fac);
                if (!result.exact)
                    continue;
                if (result.encoded < abs_min)
                    abs_min = result.encoded;
                if (result.encoded > abs_max)
                    abs_max = result.encoded;
                observe(result.encoded);
            }
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
            scheme = timestar::alp::SCHEME_ALP_DELTA;
        }

        // Size check. The parameter search minimises exceptions and nothing
        // else, so it will happily pick a 15-digit exponent under which about
        // half of a full-precision stream "round-trips" — as 53-bit integers,
        // with the other half stored as 10-byte exceptions. That is ~11 bytes
        // for an 8-byte double (float32 readings widened to double fare little
        // better). ALP_RD exists for exactly this data, so when ALP's first
        // block comes out wider than ALP_SUSPECT_BITS_PER_VALUE, encode the
        // same block with ALP_RD and keep whichever is smaller. Streams ALP
        // handles well never reach this branch and are encoded as before.
        const uint8_t alp_bw = std::min(abs_bw, delta_bw);
        const size_t exceptions = exc_positions.size();
        const size_t alp_words = 2 + timestar::alp::ffor_packed_words(first_block_count, alp_bw) +
                                 (exceptions ? (exceptions * 2 + 7) / 8 + exceptions : 0);
        if (alp_words * 64 > timestar::alp::ALP_SUSPECT_BITS_PER_VALUE * first_block_count) {
            rd_right_bits = timestar::alp::ALPRD::findBestSplit(values.data(), total_values);
            timestar::alp::ALPRD::encodeBlock(values.data(), first_block_count, *rd_right_bits, rd);
            const size_t rd_exceptions = rd.exception_positions.size();
            const size_t rd_words =
                2 + rd.dictionary.size() +
                (rd.left_bw ? timestar::alp::ffor_packed_words(first_block_count, rd.left_bw) : 0) +
                (rd.right_bw ? timestar::alp::ffor_packed_words(first_block_count, rd.right_bw) : 0) +
                (rd_exceptions ? (rd_exceptions * 2 + 7) / 8 + rd_exceptions : 0);
            if (rd_words < alp_words) {
                scheme = timestar::alp::SCHEME_ALP_RD;
                rd_first_block_ready = true;
            }
        }
    }

    // Pre-allocate estimated space in the target buffer (rough upper bound in bytes)
    const size_t est_bytes = (2 + num_blocks * (2 + timestar::alp::ALP_VECTOR_SIZE + 128)) * sizeof(uint64_t);
    target.reserve(startPos + est_bytes);

    // === Stream Header (2 x uint64_t) ===
    // Word 0: [0:31] magic, [32:63] total_values (max 2^32-1)
    if (total_values > UINT32_MAX) {
        throw std::overflow_error("ALP encoder: total_values " + std::to_string(total_values) +
                                  " exceeds 32-bit header capacity");
    }
    uint64_t header0 = static_cast<uint64_t>(timestar::alp::ALP_MAGIC) | (static_cast<uint64_t>(total_values) << 32);
    target.write(header0);

    // Word 1: [0:15] num_blocks, [16:31] tail_count, [32:39] scheme
    uint64_t header1 = static_cast<uint64_t>(num_blocks) | (static_cast<uint64_t>(tail_count) << 16) |
                       (static_cast<uint64_t>(scheme) << 32);
    target.write(header1);

    if (scheme == timestar::alp::SCHEME_ALP || scheme == timestar::alp::SCHEME_ALP_DELTA) {
        // === ALP Encoding (with optional delta) ===
        std::vector<uint64_t> packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * timestar::alp::ALP_VECTOR_SIZE;
            const size_t block_count =
                (block == num_blocks - 1 && tail_count > 0) ? tail_count : timestar::alp::ALP_VECTOR_SIZE;

            // The first block was already scaled for the delta heuristic.
            if (block > 0) {
                scaleBlock(block_start, block_count);
            }
            int64_t first_value = 0;  // absolute value of first non-exception (for delta)

            // === Delta + Zigzag Transform (SCHEME_ALP_DELTA only) ===
            if (scheme == timestar::alp::SCHEME_ALP_DELTA) {
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
            if (scheme == timestar::alp::SCHEME_ALP_DELTA) {
                target.write(std::bit_cast<uint64_t>(first_value));
            }

            // === FFOR Data ===
            // ffor_pack writes every output word, so resize() (a no-op after the
            // first block) replaces the per-block zero-fill assign().
            size_t packed_words = timestar::alp::ffor_packed_words(block_count, bw);
            if (packed_words > 0) {
                packed.resize(packed_words);
                timestar::alp::ffor_pack(encoded.data(), block_count, min_val, bw, packed.data());
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
        const uint8_t right_bit_count =
            rd_right_bits ? *rd_right_bits : timestar::alp::ALPRD::findBestSplit(values.data(), total_values);

        // Scratch buffers hoisted out of the block loop
        std::vector<uint64_t> left_packed;
        std::vector<uint64_t> right_packed;

        for (size_t block = 0; block < num_blocks; ++block) {
            const size_t block_start = block * timestar::alp::ALP_VECTOR_SIZE;
            const size_t block_count =
                (block == num_blocks - 1 && tail_count > 0) ? tail_count : timestar::alp::ALP_VECTOR_SIZE;

            if (block > 0 || !rd_first_block_ready) {
                timestar::alp::ALPRD::encodeBlock(values.data() + block_start, block_count, right_bit_count, rd);
            }
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
            // ffor_pack_u8 packs straight from the uint8 dictionary indices —
            // no widen-to-int64 pass. The packers write every output word, so
            // resize() (a no-op after the first block) replaces the per-block
            // zero-fill assign().
            if (rd.left_bw > 0) {
                size_t left_packed_words = timestar::alp::ffor_packed_words(block_count, rd.left_bw);
                left_packed.resize(left_packed_words);
                timestar::alp::ffor_pack_u8(rd.left_indices.data(), block_count, rd.left_bw, left_packed.data());
                target.write_array(left_packed.data(), left_packed_words);
            }

            // === Right FFOR Data ===
            if (rd.right_bw > 0) {
                size_t right_packed_words = timestar::alp::ffor_packed_words(block_count, rd.right_bw);
                right_packed.resize(right_packed_words);
                timestar::alp::ffor_pack_u64(rd.right_parts.data(), block_count, rd.right_for_base, rd.right_bw,
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
