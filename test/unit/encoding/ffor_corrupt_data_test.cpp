#include "../../../lib/encoding/integer/integer_encoder_ffor.hpp"
#include "../../../lib/encoding/alp/alp_ffor.hpp"
#include "../../../lib/storage/aligned_buffer.hpp"
#include "../../../lib/storage/slice_buffer.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

// Helper: build a raw FFOR block with manually crafted header values.
// Block header format (from integer_encoder_ffor.cpp):
//   Word 0: [0:10] block_count, [11:17] bit_width, [18:27] exception_count
//   Word 1: base (FOR reference = min zigzag value in block)
static AlignedBuffer makeFforBlock(uint16_t block_count, uint8_t bw, uint16_t exc_count, uint64_t base,
                                   const std::vector<uint64_t>& packed_words,
                                   const std::vector<uint16_t>& exc_positions,
                                   const std::vector<uint64_t>& exc_values) {
    AlignedBuffer buf;

    uint64_t w0 = static_cast<uint64_t>(block_count) | (static_cast<uint64_t>(bw) << 11) |
                  (static_cast<uint64_t>(exc_count) << 18);
    buf.write(w0);
    buf.write(base);

    for (uint64_t w : packed_words) {
        buf.write(w);
    }

    // Exception positions: 4 x uint16_t per uint64_t word
    if (!exc_positions.empty()) {
        size_t pos_words = (exc_positions.size() + 3) / 4;
        for (size_t i = 0; i < pos_words; ++i) {
            uint64_t word = 0;
            for (size_t j = 0; j < 4; ++j) {
                size_t idx = i * 4 + j;
                if (idx < exc_positions.size()) {
                    word |= static_cast<uint64_t>(exc_positions[idx]) << (j * 16);
                }
            }
            buf.write(word);
        }
        for (uint64_t v : exc_values) {
            buf.write(v);
        }
    }

    return buf;
}

TEST(FFORCorruptData, SignedRangeValidatesUnselectedSuffix) {
    std::vector<AlignedBuffer> invalid;
    invalid.push_back(makeFforBlock(8, 65, 0, 0, {}, {}, {}));
    invalid.push_back(makeFforBlock(2047, 0, 0, 0, {}, {}, {}));
    invalid.push_back(makeFforBlock(8, 0, 9, 0, {}, {}, {}));
    invalid.push_back(makeFforBlock(8, 32, 0, 0, {0}, {}, {}));
    invalid.push_back(makeFforBlock(8, 0, 1, 0, {}, {8}, {123}));
    invalid.push_back(makeFforBlock(8, 0, 1, 0, {}, {0}, {}));
    for (const auto& suffix : invalid) {
        auto data = makeFforBlock(1, 0, 0, 84, {}, {}, {});
        data.write(suffix);
        for (size_t count : {0, 1}) {
            Slice slice(data.data.data(), data.size());
            std::vector<int64_t> out;
            EXPECT_THROW(IntegerEncoderFFOR::decodeSigned(slice, 9, 0, count, out), std::runtime_error);
            Slice doubles(data.data.data(), data.size());
            std::vector<double> doubleOut;
            EXPECT_THROW(IntegerEncoderFFOR::decodeSigned(doubles, 9, 0, count, doubleOut), std::runtime_error);
        }
    }
}

TEST(FFORCorruptData, SignedRangeCarriesStateAcrossEmptyAndSingleValueGroups) {
    AlignedBuffer data;
    // ZigZag integer values 0, 0, 0, 0, 2, 4, 6 become signed 0, 0, 0, 0, 1, 2, 3.
    for (auto [count, base] : std::vector<std::pair<uint16_t, uint64_t>>{
             {0, 0}, {1, 0}, {0, 0}, {1, 0}, {2, 0}, {1, 4}, {0, 0}, {2, 0}})
        data.write(makeFforBlock(count, 0, 0, base, {}, {}, {}));
    std::vector<int64_t> expected{0, 0, 0, 0, 1, 2, 3};
    for (size_t first = 0; first <= expected.size(); ++first) {
        Slice slice(data.data.data(), data.size());
        std::vector<int64_t> out;
        EXPECT_EQ(IntegerEncoderFFOR::decodeSigned(slice, expected.size(), first, 3, out),
                  std::min(size_t{3}, expected.size() - first));
        EXPECT_EQ(out, std::vector<int64_t>(expected.begin() + first,
                                          expected.begin() + first + std::min(size_t{3}, expected.size() - first)));
    }
}

// --- Test 1: Corrupt block where bw=64 implies max < min (enormous range) ---
//
// A legitimate encoder always stores bw = bitsForRange(max - min), where
// bitsForRange(0) = 0.  A corrupt block with bw=64 implies a range of
// (1<<64)-1, i.e. the distance from base (min) to the notional max would
// wrap around the entire uint64 space -- equivalent to max < min semantically.
// The decoder must reject this rather than over-reading past the end of the slice.
TEST(FFORCorruptData, MaxLessThanMinThrows) {
    // Craft a block claiming bw=64 (implies range = UINT64_MAX, i.e. max < min
    // in the sense that base + UINT64_MAX wraps around to base - 1).
    // block_count=4, exc_count=0, base=100.
    // We provide ZERO packed words (the corrupt block is truncated after the
    // header) so the decoder would be forced to read 4*64/64 = 4 words beyond
    // the end of the buffer if it doesn't validate first.
    const uint16_t block_count = 4;
    const uint8_t bw = 64;  // <-- corrupt: implies max = base + UINT64_MAX
    const uint16_t exc_count = 0;
    const uint64_t base = 100;

    // No packed data words supplied -- decoder would over-read without the check
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base, {}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW(
        {
            // decode() iterates blocks; decodeBlockInto() is the first point of
            // validation.  Pass timestampSize=block_count so the decoder attempts
            // to read the block.
            IntegerEncoderFFOR::decode(s, block_count, out);
        },
        std::runtime_error)
        << "Decoder must throw on corrupt bw=64 (max < min)";
}

// --- Test 2: Corrupt block where bw implies more packed bytes than remain ---
//
// Here we use bw=32 with block_count=8 but supply only 1 packed word instead
// of the 4 that ffor_packed_words(8, 32) would require.  The decoder should
// detect the buffer under-run and throw.
TEST(FFORCorruptData, PackedDataTruncatedThrows) {
    const uint16_t block_count = 8;
    const uint8_t bw = 32;  // needs 8*32/64 = 4 words
    const uint16_t exc_count = 0;
    const uint64_t base = 0;

    // Provide only 1 packed word (3 are missing)
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base, {0xDEADBEEFCAFEBABEULL}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW(
        { IntegerEncoderFFOR::decode(s, block_count, out); }, std::runtime_error)
        << "Decoder must throw when packed data is truncated";
}

// --- Test 3: Corrupt block where exception positions extend past the slice ---
//
// A valid block (bw=1, block_count=4) but exc_count is inflated so the decoder
// tries to read exception position words beyond the end of the buffer.
TEST(FFORCorruptData, ExceptionCountInflatedThrows) {
    const uint16_t block_count = 4;
    const uint8_t bw = 1;            // ffor_packed_words(4, 1) = 1 word
    const uint16_t exc_count = 200;  // <-- corrupt: far more than block_count
    const uint64_t base = 0;

    // 1 valid packed word for bw=1, block_count=4; no actual exception data
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base, {0xFULL}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW(
        { IntegerEncoderFFOR::decode(s, block_count, out); }, std::runtime_error)
        << "Decoder must throw when exception count exceeds block_count";
}

// --- Test: bw > 64 must not cause undefined behavior (shift UB) ---
// The ffor_pack/unpack functions use (1ULL << bw) which is UB when bw >= 64.
// The fix changes `bw == 64` checks to `bw >= 64`, treating any bw > 64 the
// same as bw == 64 (full-width copy).
TEST(FFORCorruptData, BitWidthAbove64DoesNotCrash) {
    constexpr size_t N = 4;
    int64_t values[N] = {100, 200, 300, 400};
    int64_t base = 0;
    // bw=65 would cause UB without the fix (1ULL << 65 is undefined)
    uint64_t packed[N] = {};

    // Should not crash — treated as full 64-bit copy
    timestar::alp::ffor_pack(values, N, base, 65, packed);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(packed[i], static_cast<uint64_t>(values[i] - base));
    }

    // Unpack should round-trip
    int64_t unpacked[N] = {};
    timestar::alp::ffor_unpack(packed, N, base, 65, unpacked);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(unpacked[i], values[i]);
    }

    // Same for unsigned variant
    uint64_t uvals[N] = {10, 20, 30, 40};
    uint64_t ubase = 0;
    uint64_t upacked[N] = {};
    timestar::alp::ffor_pack_u64(uvals, N, ubase, 200, upacked);  // bw=200
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(upacked[i], uvals[i] - ubase);
    }

    uint64_t uunpacked[N] = {};
    timestar::alp::ffor_unpack_u64(upacked, N, ubase, 200, uunpacked);
    for (size_t i = 0; i < N; ++i) {
        EXPECT_EQ(uunpacked[i], uvals[i]);
    }
}

// --- Test 4: Regression -- normal encode/decode still works after the fix ---
TEST(FFORCorruptData, NormalRoundTripUnaffected) {
    std::vector<uint64_t> values;
    // Monotonically increasing timestamps at 1-second intervals
    for (size_t i = 0; i < 50; ++i) {
        values.push_back(1000000000ULL * (1700000000ULL + i));
    }

    AlignedBuffer encoded = IntegerEncoderFFOR::encode(values);
    Slice s(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] = IntegerEncoderFFOR::decode(s, values.size(), decoded);

    ASSERT_EQ(added, values.size());
    ASSERT_EQ(decoded.size(), values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(decoded[i], values[i]) << "Mismatch at index " << i;
    }
}

TEST(FFORCorruptData, UnsignedPackingSupportsEveryByteAlignment) {
    for (size_t count : {1, 3, 65, 1024}) {
        for (uint8_t bw = 0; bw <= 64; ++bw) {
            const uint64_t mask = bw == 64 ? UINT64_MAX : (uint64_t{1} << bw) - 1;
            std::vector<uint64_t> values(count);
            for (size_t i = 0; i < count; ++i)
                values[i] = ((i + 1) * 0x9e3779b97f4a7c15ULL) & mask;
            const auto words = timestar::alp::ffor_packed_words(count, bw);
            std::vector<uint64_t> aligned(words);
            timestar::alp::ffor_pack_u64(values.data(), count, 0, bw, aligned.data());
            std::vector<uint64_t> recovered(count);
            timestar::alp::ffor_unpack_u64(aligned.data(), count, 0, bw, recovered.data());
            ASSERT_EQ(recovered, values);
            for (size_t offset = 0; offset < 8; ++offset) {
                std::vector<uint8_t> bytes(words * 8 + 16, 0xcd);
                timestar::alp::ffor_pack_u64(values.data(), count, 0, bw, bytes.data() + offset);
                if (words)
                    EXPECT_EQ(std::memcmp(bytes.data() + offset, aligned.data(), words * 8), 0);
                for (size_t i = 0; i < offset; ++i) EXPECT_EQ(bytes[i], 0xcd);
                for (size_t i = offset + words * 8; i < bytes.size(); ++i) EXPECT_EQ(bytes[i], 0xcd);
            }
        }
    }
}

TEST(FFORCorruptData, ByteAlignedUnsignedDecodeMatchesIndependentPackedWords) {
    for (size_t count : {0, 1, 3, 63, 64, 65, 1023, 1024}) {
        for (uint8_t bw = 0; bw <= 64; ++bw) {
            const uint64_t mask = bw == 64 ? UINT64_MAX : (uint64_t{1} << bw) - 1;
            const uint64_t base = UINT64_MAX - 17;
            const auto wordCount = timestar::alp::ffor_packed_words(count, bw);
            std::vector<uint64_t> words(wordCount, 0), expected(count);
            // Construct packed bits independently of the production packer.
            for (size_t i = 0; i < count; ++i) {
                const uint64_t delta = ((i + 1) * 0x9e3779b97f4a7c15ULL) & mask;
                expected[i] = base + delta;
                for (unsigned bit = 0; bit < bw; ++bit) {
                    const size_t pos = i * bw + bit;
                    words[pos / 64] |= ((delta >> bit) & 1) << (pos % 64);
                }
            }
            for (size_t offset = 0; offset < 8; ++offset) {
                SCOPED_TRACE(::testing::Message() << "count=" << count << " bw=" << unsigned(bw) << " offset=" << offset);
                // Exact allocation end lets ASan detect any trailing word read.
                const size_t byteCount = wordCount * 8;
                auto bytes = std::make_unique<uint8_t[]>(byteCount + offset);
                if (byteCount) std::memcpy(bytes.get() + offset, words.data(), byteCount);
                std::vector<uint64_t> output(count + 2, 0xbadc0ffeeULL);
                timestar::alp::ffor_unpack_u64(bytes.get() + offset, count, base, bw, output.data() + 1);
                EXPECT_EQ(output.front(), 0xbadc0ffeeULL);
                EXPECT_EQ(output.back(), 0xbadc0ffeeULL);
                EXPECT_TRUE(std::equal(expected.begin(), expected.end(), output.begin() + 1));
            }
        }
    }
}

TEST(FFORCorruptData, UnalignedTimestampDecodePreservesFiltersAndAppendedOutput) {
    std::vector<uint64_t> input;
    uint64_t ts = 1700000000000000000ULL;
    for (size_t i = 0; i < 2051; ++i) {
        input.push_back(ts);
        ts += 1000000 + (i * 7919) % 100000 + (i % 101 == 0 ? uint64_t{1} << 40 : 0);
    }
    auto encoded = IntegerEncoderFFOR::encode(input);
    for (size_t offset = 0; offset < 8; ++offset) {
        auto bytes = std::make_unique<uint8_t[]>(encoded.size() + offset);
        std::memcpy(bytes.get() + offset, encoded.data.data(), encoded.size());
        for (bool filtered : {false, true}) {
            Slice slice(bytes.get() + offset, encoded.size());
            std::vector<uint64_t> output{123};
            const auto [skipped, added] = IntegerEncoderFFOR::decode(slice, input.size(), output,
                filtered ? input[1023] : 0, filtered ? input[1025] : UINT64_MAX);
            const size_t start = filtered ? 1023 : 0, count = filtered ? 3 : input.size();
            EXPECT_EQ(skipped, start);
            EXPECT_EQ(added, count);
            ASSERT_EQ(output.size(), count + 1);
            EXPECT_EQ(output.front(), 123);
            EXPECT_TRUE(std::equal(input.begin() + start, input.begin() + start + count, output.begin() + 1));
        }
    }
}

namespace {
void expectFilteredPrefix(const AlignedBuffer& encoded, const std::vector<uint64_t>& input,
                          unsigned requested, uint64_t lo, uint64_t hi, size_t offset) {
    std::vector<uint8_t> bytes(encoded.size() + offset);
    std::memcpy(bytes.data() + offset, encoded.data.data(), encoded.size());
    Slice slice(bytes.data() + offset, encoded.size());
    std::vector<uint64_t> expected{123}, actual{123};
    size_t skipped = 0;
    for (size_t i = 0; i < std::min(size_t(requested), input.size()); ++i) {
        if (input[i] < lo) ++skipped;
        else if (input[i] > hi) break;
        else expected.push_back(input[i]);
    }
    const auto result = IntegerEncoderFFOR::decode(slice, requested, actual, lo, hi);
    EXPECT_EQ(result.first, skipped);
    EXPECT_EQ(result.second, expected.size() - 1);
    EXPECT_EQ(actual, expected);
}
}

TEST(FFORCorruptData, ConstantDeltaRunsPreserveModularValuesFiltersAndStateTransitions) {
    for (uint64_t first : {uint64_t{0}, uint64_t{10000}, (uint64_t{1} << 63) - 7, UINT64_MAX - 31}) {
        for (uint64_t step : {uint64_t{0}, uint64_t{1}, uint64_t{7}, uint64_t{1} << 63, UINT64_MAX - 2}) {
            SCOPED_TRACE(::testing::Message() << "first=" << first << " step=" << step);
            const auto zigzagBits = [](uint64_t v) { return (v << 1) ^ (uint64_t{0} - (v >> 63)); };
            AlignedBuffer encoded;
            // Empty groups must not consume the stream's two special values.
            encoded.write(makeFforBlock(0, 0, 0, 0, {}, {}, {}));
            encoded.write(makeFforBlock(1, 0, 0, first, {}, {}, {}));
            encoded.write(makeFforBlock(0, 0, 0, 0, {}, {}, {}));
            encoded.write(makeFforBlock(1, 0, 0, zigzagBits(step), {}, {}, {}));
            std::vector<uint64_t> input{first, first + step};
            uint64_t last = input.back();
            for (size_t count : {0, 1, 7, 1024, 1023}) {
                encoded.write(makeFforBlock(count, 0, 0, 0, {}, {}, {}));
                for (size_t i = 0; i < count; ++i) input.push_back(last += step);
            }
            // A zero-bit group with an exception is not a constant-delta run.
            encoded.write(makeFforBlock(8, 0, 1, 0, {}, {3}, {zigzagBits(3)}));
            auto changedStep = step;
            for (size_t i = 0; i < 8; ++i) {
                if (i == 3) changedStep += 3;
                input.push_back(last += changedStep);
            }
            encoded.write(makeFforBlock(17, 0, 0, 0, {}, {}, {}));
            for (size_t i = 0; i < 17; ++i) input.push_back(last += changedStep);
            for (unsigned requested : {0u, 1u, 2u, 3u, 1023u, 1024u, 1025u, 5000u}) {
                for (auto [lo, hi] : {std::pair<uint64_t,uint64_t>{0, UINT64_MAX}, {0, 0}, {1, UINT64_MAX},
                                      {first, first}, {input[1023], input[1025]}, {10007, 10103},
                                      {UINT64_MAX - 10, UINT64_MAX}, {100, 50}}) {
                    for (size_t offset : {0, 3})
                        expectFilteredPrefix(encoded, input, requested, lo, hi, offset);
                }
            }
        }
    }
}

TEST(FFORCorruptData, AllZeroFirstBlockAndTruncatedConstantRuns) {
    for (size_t n : {1, 2, 3, 1023, 1024, 1025, 2051}) {
        std::vector<uint64_t> values(n, 0);
        auto encoded = IntegerEncoderFFOR::encode(values);
        expectFilteredPrefix(encoded, values, n, 0, UINT64_MAX, 7);
        expectFilteredPrefix(encoded, values, n, 1, UINT64_MAX, 7);
        expectFilteredPrefix(encoded, values, n / 2, 0, 0, 7);
    }
}

TEST(FFORCorruptData, SelectiveDecodeAllocatesForSurvivorsOnly) {
    std::vector<uint64_t> input(65537);
    for (size_t i = 0; i < input.size(); ++i) input[i] = 1700000000000000000ULL + i * 1000;
    auto encoded = IntegerEncoderFFOR::encode(input);
    for (bool empty : {false, true}) {
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> output;
        const auto [skipped, added] = IntegerEncoderFFOR::decode(slice, input.size(), output,
            empty ? input.back() + 1 : input[input.size() - 128], UINT64_MAX);
        EXPECT_EQ(skipped, input.size() - (empty ? 0 : 128));
        EXPECT_EQ(added, empty ? 0 : 128);
        EXPECT_LE(output.capacity(), empty ? 0 : 256);
        EXPECT_TRUE(std::equal(output.begin(), output.end(), input.end() - output.size()));
    }
    std::vector<uint64_t> prefix{123};
    auto* storage = prefix.data();
    const auto capacity = prefix.capacity();
    Slice slice(encoded.data.data(), encoded.size());
    IntegerEncoderFFOR::decode(slice, input.size(), prefix, input.back() + 1, UINT64_MAX);
    EXPECT_EQ(prefix, (std::vector<uint64_t>{123}));
    EXPECT_EQ(prefix.data(), storage);
    EXPECT_EQ(prefix.capacity(), capacity);
}

TEST(FFORCorruptData, FullBlockAppendsUseAmortizedCapacityGrowth) {
    std::vector<uint64_t> input(3000);
    for (size_t i = 0; i < input.size(); ++i) input[i] = 1700000000000000000ULL + i * 1000;
    auto encoded = IntegerEncoderFFOR::encode(input);
    std::vector<uint64_t> output;
    size_t growths = 0, capacity = 0;
    for (size_t block = 0; block < 128; ++block) {
        Slice slice(encoded.data.data(), encoded.size());
        const auto result = IntegerEncoderFFOR::decode(slice, input.size(), output);
        ASSERT_EQ(result.second, input.size());
        EXPECT_TRUE(std::equal(input.begin(), input.end(), output.end() - input.size()));
        if (output.capacity() != capacity) ++growths;
        capacity = output.capacity();
    }
    EXPECT_EQ(output.size(), 128 * input.size());
    EXPECT_LT(growths, 24u); // A fixed increment reallocates for nearly every block.
    EXPECT_LE(output.capacity(), 2 * output.size());
}

TEST(FFORCorruptData, AllocationLookaheadDoesNotConsumeOrValidateUnreachedGroups) {
    AlignedBuffer encoded;
    encoded.write(makeFforBlock(1, 0, 0, 100, {}, {}, {}));
    encoded.write(makeFforBlock(1, 0, 0, 2, {}, {}, {})); // first delta = 1
    encoded.write(makeFforBlock(1024, 0, 0, 0, {}, {}, {}));
    encoded.write(makeFforBlock(1024, 0, 0, 0, {}, {}, {}));
    encoded.write(makeFforBlock(2047, 0, 0, 0, {}, {}, {})); // invalid but unreached
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> output;
    const auto result = IntegerEncoderFFOR::decode(slice, 6000, output, 100, 120);
    EXPECT_EQ(result.first, 0);
    EXPECT_EQ(result.second, 21);
    ASSERT_EQ(output.size(), 21);
    for (size_t i = 0; i < output.size(); ++i) EXPECT_EQ(output[i], 100 + i);
    EXPECT_EQ(slice.offset, 48); // exactly the three groups needed by this filter
    EXPECT_LE(output.capacity(), 42);
}
