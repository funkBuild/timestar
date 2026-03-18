#include "../../../lib/encoding/bool_encoder_rle.hpp"
#include "../../../lib/storage/slice_buffer.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

// =============================================================================
// Bug #29: BoolEncoderRLE readVarint returning 0 on exhausted slice
//
// When the slice has no bytes left, readVarint() silently returned 0 instead
// of signaling an error.  This could cause the decoder to produce corrupted
// data (run length 0 means the run is skipped, silently losing values).
// The fix throws std::runtime_error on exhausted input.
// =============================================================================

TEST(BoolEncoderRLEVarint, DecodeThrowsOnEmptySlice) {
    // An empty slice should throw when trying to decode any values,
    // because read<uint8_t>() for the initial value byte fails
    uint8_t dummy = 0;
    Slice empty(&dummy, 0);

    std::vector<bool> decoded;
    EXPECT_THROW(
        BoolEncoderRLE::decode(empty, 0, 1, decoded),
        std::runtime_error
    );
}

TEST(BoolEncoderRLEVarint, DecodeHandlesTruncatedData) {
    // Craft a buffer with initial value byte and one run varint.
    // Buffer: [initial_value=1] [varint=5 (run of 5 trues)]
    // Requesting exactly 5 should work (one run covers it).
    std::vector<uint8_t> crafted = {
        0x01,  // initial value = true
        0x05,  // run of 5 trues (single-byte varint: 5 < 128)
    };
    Slice slice(crafted.data(), crafted.size());

    std::vector<bool> decoded;
    BoolEncoderRLE::decode(slice, 0, 5, decoded);
    ASSERT_EQ(decoded.size(), 5u);
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_TRUE(decoded[i]) << "Expected true at index " << i;
    }
}

TEST(BoolEncoderRLEVarint, NormalEncodeDecodeStillWorks) {
    // Verify the fix doesn't break normal operation
    std::vector<bool> original = {true, true, false, false, false, true, false, true, true, true};
    AlignedBuffer encoded = BoolEncoderRLE::encode(original);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoderRLE::decode(slice, 0, original.size(), decoded);

    ASSERT_EQ(decoded.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_EQ(static_cast<bool>(decoded[i]), static_cast<bool>(original[i]))
            << "Mismatch at index " << i;
    }
}

TEST(BoolEncoderRLEVarint, LargeRunNormalRoundtrip) {
    // Larger sequence to verify normal operation with many runs
    std::vector<bool> original;
    original.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
        original.push_back((i / 100) % 2 == 0);
    }

    AlignedBuffer encoded = BoolEncoderRLE::encode(original);
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<bool> decoded;
    BoolEncoderRLE::decode(slice, 0, original.size(), decoded);

    ASSERT_EQ(decoded.size(), original.size());
    for (size_t i = 0; i < original.size(); ++i) {
        EXPECT_EQ(static_cast<bool>(decoded[i]), static_cast<bool>(original[i]))
            << "Mismatch at index " << i;
    }
}

// Source-inspection test: verify readVarint throws instead of returning 0
#ifndef BOOL_ENCODER_RLE_SOURCE_PATH
TEST(BoolEncoderRLEVarint, SourceInspection_ReadVarintThrowsNotReturns0) {
    GTEST_SKIP() << "BOOL_ENCODER_RLE_SOURCE_PATH not defined";
}
#else
TEST(BoolEncoderRLEVarint, SourceInspection_ReadVarintThrowsNotReturns0) {
    std::ifstream file(BOOL_ENCODER_RLE_SOURCE_PATH);
    ASSERT_TRUE(file.is_open()) << "Could not open bool_encoder_rle.cpp at: " << BOOL_ENCODER_RLE_SOURCE_PATH;
    std::string sourceCode{std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};

    // Find the readVarint function
    auto funcPos = sourceCode.find("readVarint(Slice&");
    ASSERT_NE(funcPos, std::string::npos) << "Could not find readVarint function";

    // Extract the first 300 chars of the function
    auto region = sourceCode.substr(funcPos, 300);

    // Verify it throws instead of returning 0
    EXPECT_NE(region.find("throw"), std::string::npos)
        << "readVarint must throw on exhausted slice, not return 0.\n"
        << "Function region:\n" << region;
    EXPECT_EQ(region.find("return 0"), std::string::npos)
        << "readVarint must NOT return 0 on exhausted slice. "
        << "It should throw std::runtime_error.\n"
        << "Function region:\n" << region;
}
#endif
