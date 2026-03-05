#include <gtest/gtest.h>
#include "../../../lib/encoding/integer/integer_encoder_ffor.hpp"
#include "../../../lib/storage/aligned_buffer.hpp"
#include "../../../lib/storage/slice_buffer.hpp"

#include <vector>
#include <cstdint>
#include <stdexcept>
#include <cstring>

// Helper: build a raw FFOR block with manually crafted header values.
// Block header format (from integer_encoder_ffor.cpp):
//   Word 0: [0:10] block_count, [11:17] bit_width, [18:27] exception_count
//   Word 1: base (FOR reference = min zigzag value in block)
static AlignedBuffer makeFforBlock(uint16_t block_count, uint8_t bw,
                                   uint16_t exc_count, uint64_t base,
                                   const std::vector<uint64_t>& packed_words,
                                   const std::vector<uint16_t>& exc_positions,
                                   const std::vector<uint64_t>& exc_values) {
    AlignedBuffer buf;

    uint64_t w0 = static_cast<uint64_t>(block_count)
                | (static_cast<uint64_t>(bw) << 11)
                | (static_cast<uint64_t>(exc_count) << 18);
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
    const uint8_t  bw          = 64;   // <-- corrupt: implies max = base + UINT64_MAX
    const uint16_t exc_count   = 0;
    const uint64_t base        = 100;

    // No packed data words supplied -- decoder would over-read without the check
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base, {}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW({
        // decode() iterates blocks; decodeBlockInto() is the first point of
        // validation.  Pass timestampSize=block_count so the decoder attempts
        // to read the block.
        IntegerEncoderFFOR::decode(s, block_count, out);
    }, std::runtime_error) << "Decoder must throw on corrupt bw=64 (max < min)";
}

// --- Test 2: Corrupt block where bw implies more packed bytes than remain ---
//
// Here we use bw=32 with block_count=8 but supply only 1 packed word instead
// of the 4 that ffor_packed_words(8, 32) would require.  The decoder should
// detect the buffer under-run and throw.
TEST(FFORCorruptData, PackedDataTruncatedThrows) {
    const uint16_t block_count = 8;
    const uint8_t  bw          = 32;   // needs 8*32/64 = 4 words
    const uint16_t exc_count   = 0;
    const uint64_t base        = 0;

    // Provide only 1 packed word (3 are missing)
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base,
                                     {0xDEADBEEFCAFEBABEULL}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW({
        IntegerEncoderFFOR::decode(s, block_count, out);
    }, std::runtime_error) << "Decoder must throw when packed data is truncated";
}

// --- Test 3: Corrupt block where exception positions extend past the slice ---
//
// A valid block (bw=1, block_count=4) but exc_count is inflated so the decoder
// tries to read exception position words beyond the end of the buffer.
TEST(FFORCorruptData, ExceptionCountInflatedThrows) {
    const uint16_t block_count = 4;
    const uint8_t  bw          = 1;    // ffor_packed_words(4, 1) = 1 word
    const uint16_t exc_count   = 200;  // <-- corrupt: far more than block_count
    const uint64_t base        = 0;

    // 1 valid packed word for bw=1, block_count=4; no actual exception data
    AlignedBuffer buf = makeFforBlock(block_count, bw, exc_count, base,
                                     {0xFULL}, {}, {});

    Slice s(buf.data.data(), buf.size());
    std::vector<uint64_t> out;
    out.reserve(1024);

    EXPECT_THROW({
        IntegerEncoderFFOR::decode(s, block_count, out);
    }, std::runtime_error) << "Decoder must throw when exception count exceeds block_count";
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
