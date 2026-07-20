// The decode count contract, pinned at the decoder boundary.
//
// A TSM block declares how many points it holds. The timestamp decoder is
// trimmed to that count, and every value decoder is then asked for exactly as
// many values as timestamps survived the time filter. The block-level caller
// (TSM::decodeBlockFlat and friends) enforces:
//
//   produced > expected : benign -- a decoder working in fixed-size groups can
//                         overshoot the tail. Truncate.
//   produced < expected : the block is corrupt or a decoder regressed. Raise,
//                         because pairing values[i] with timestamps[i] past the
//                         shortfall MISPAIRS real data.
//
// For that to work, a decoder must report what it ACTUALLY produced. This file
// pins that report for every value type, because the decoders used to disagree
// about how a shortfall even manifests -- ALP trimmed, bool threw, string and
// integer silently under-produced -- and that divergence is what allowed a
// desynced timestamp/value pair to reach the query at all.
//
// TWO PROPERTIES PER DECODER
//   1. Exact stream: produced == requested, values are the requested window.
//   2. APPEND, never replace: a decoder handed a non-empty `out` must leave the
//      existing contents alone. TSM decodes every block of a series into ONE
//      shared vector, so a decoder that clears destroys earlier blocks' values
//      while their timestamps remain -- the exact shape of the string-decoder
//      bug this contract exists to prevent.

#include "../../../lib/encoding/bool_encoder_rle.hpp"
#include "../../../lib/encoding/float_encoder.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/encoding/string_encoder.hpp"
#include "../../../lib/storage/compressed_buffer.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

class DecoderProducedCountTest : public ::testing::Test {};

// ---------------------------------------------------------------------------
// String (raw STRG)
// ---------------------------------------------------------------------------

TEST_F(DecoderProducedCountTest, StringDecodeReportsExactCount) {
    const std::vector<std::string> values = {"a", "b", "c", "d"};
    auto encoded = StringEncoder::encode(values);

    std::vector<std::string> out;
    Slice slice(encoded.data.data(), encoded.size());
    const size_t produced = StringEncoder::decode(slice, values.size(), 0, values.size(), out);

    EXPECT_EQ(produced, values.size());
    EXPECT_EQ(out.size(), values.size()) << "reported count must match what was appended";
}

TEST_F(DecoderProducedCountTest, StringDecodeAppendsAndReportsOnlyItsOwnValues) {
    const std::vector<std::string> values = {"x", "y"};
    auto encoded = StringEncoder::encode(values);

    std::vector<std::string> out = {"earlier0", "earlier1", "earlier2"};
    Slice slice(encoded.data.data(), encoded.size());
    const size_t produced = StringEncoder::decode(slice, values.size(), 0, values.size(), out);

    EXPECT_EQ(produced, 2u) << "produced counts THIS call's values, not the vector size";
    ASSERT_EQ(out.size(), 5u) << "decoder clobbered values decoded by an earlier block";
    EXPECT_EQ(out[0], "earlier0");
    EXPECT_EQ(out[2], "earlier2");
    EXPECT_EQ(out[3], "x");
}

// A window shorter than the block is the normal skip/limit case, not a fault.
TEST_F(DecoderProducedCountTest, StringDecodeReportsTheWindowNotTheBlock) {
    const std::vector<std::string> values = {"v0", "v1", "v2", "v3", "v4"};
    auto encoded = StringEncoder::encode(values);

    std::vector<std::string> out;
    Slice slice(encoded.data.data(), encoded.size());
    const size_t produced = StringEncoder::decode(slice, values.size(), 1, 2, out);

    EXPECT_EQ(produced, 2u);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], "v1");
    EXPECT_EQ(out[1], "v2");
}

// An empty block produces nothing and must not disturb `out`.
TEST_F(DecoderProducedCountTest, StringDecodeOfEmptyBlockReportsZeroAndKeepsOutput) {
    const std::vector<std::string> empty;
    auto encoded = StringEncoder::encode(empty);

    std::vector<std::string> out = {"kept"};
    Slice slice(encoded.data.data(), encoded.size());
    const size_t produced = StringEncoder::decode(slice, 0, 0, 0, out);

    EXPECT_EQ(produced, 0u);
    ASSERT_EQ(out.size(), 1u) << "an empty block cleared previously decoded values";
    EXPECT_EQ(out[0], "kept");
}

// ---------------------------------------------------------------------------
// Float (ALP)
// ---------------------------------------------------------------------------

TEST_F(DecoderProducedCountTest, FloatDecodeReportsExactCountAndAppends) {
    const std::vector<double> values = {1.5, -2.25, 3.0, 4.75};
    auto encoded = FloatEncoder::encode(values);

    std::vector<double> out = {99.0};
    // ALP writes packed uint64 words, so the zero-copy slice needs a multiple
    // of 8 (same helper pattern as encode_into_test.cpp).
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(encoded.data.data());
    const size_t byteLen = encoded.size();
    CompressedSlice slice =
        (byteLen % 8 == 0) ? CompressedSlice(CompressedSlice::ZeroCopy{}, ptr, byteLen) : CompressedSlice(ptr, byteLen);
    const size_t produced = FloatDecoder::decode(slice, 0, values.size(), out);

    EXPECT_EQ(produced, values.size());
    ASSERT_EQ(out.size(), values.size() + 1) << "float decoder clobbered an earlier block's values";
    EXPECT_DOUBLE_EQ(out[0], 99.0);
    EXPECT_DOUBLE_EQ(out[1], 1.5);
    EXPECT_DOUBLE_EQ(out[4], 4.75);
}

// ---------------------------------------------------------------------------
// Bool (RLE)
// ---------------------------------------------------------------------------

TEST_F(DecoderProducedCountTest, BoolDecodeReportsExactCountAndAppends) {
    const std::vector<bool> values = {true, true, false, true};
    auto encoded = BoolEncoderRLE::encode(values);

    std::vector<bool> out = {false};
    Slice slice(encoded.data.data(), encoded.size());
    const size_t produced = BoolEncoderRLE::decode(slice, 0, values.size(), out);

    EXPECT_EQ(produced, values.size());
    ASSERT_EQ(out.size(), values.size() + 1) << "bool decoder clobbered an earlier block's values";
    EXPECT_EQ(out[1], true);
    EXPECT_EQ(out[3], false);
}

// ---------------------------------------------------------------------------
// Timestamps (FFOR integer)
// ---------------------------------------------------------------------------
// The timestamp side of the contract: the decoder must never emit more than the
// caller asked for, even though it decodes in whole groups of kBlockSize. This
// is the ONLY thing standing between a timestamp over-read and values being
// fabricated to match it.

TEST_F(DecoderProducedCountTest, TimestampDecodeNeverExceedsTheRequestedCount) {
    std::vector<uint64_t> src;
    for (uint64_t i = 0; i < 1500; ++i) {
        src.push_back(1700000000000000000ULL + i * 1000000ULL);
    }
    auto encoded = IntegerEncoder::encode(src);

    for (unsigned requested : {1u, 1023u, 1024u, 1025u, 1200u}) {
        // Filtered path (a window inside the sentinels) is the one that used to
        // run to the end of the FFOR group instead of stopping at the request.
        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> out;
        auto [skipped, added] = IntegerEncoder::decode(slice, requested, out, 1ULL, UINT64_MAX - 1);

        EXPECT_EQ(out.size(), added) << "requested " << requested;
        ASSERT_LE(out.size(), requested) << "decoder emitted more timestamps than requested (" << requested << ")";
        for (size_t i = 0; i < out.size(); ++i) {
            ASSERT_EQ(out[i], src[i]) << "requested " << requested << ", value " << i;
        }
    }
}
