#include "alp/alp_constants.hpp"
#include "alp/alp_encoder.hpp"

#include <gtest/gtest.h>

#include <climits>
#include <cstdint>
#include <fstream>
#include <string>

// =============================================================================
// Bug #9: ALP encoder num_blocks overflow
//
// The num_blocks variable is stored in 16 bits in the stream header, but was
// not validated after computation.  With >67M float values the block count
// overflows UINT16_MAX, silently truncating data.  The fix adds an
// overflow_error throw in both encode() and encodeInto().
// =============================================================================

// Verify the math: num_blocks = ceil(total / ALP_VECTOR_SIZE) overflows at
// UINT16_MAX * ALP_VECTOR_SIZE + 1 values.
TEST(ALPEncoderOverflow, NumBlocksOverflowThreshold) {
    constexpr size_t ALP_VECTOR_SIZE = alp::ALP_VECTOR_SIZE;  // 1024
    constexpr size_t maxSafe = static_cast<size_t>(UINT16_MAX) * ALP_VECTOR_SIZE;
    constexpr size_t overflow = maxSafe + 1;

    // Verify the computation independently
    size_t safeBlocks = (maxSafe + ALP_VECTOR_SIZE - 1) / ALP_VECTOR_SIZE;
    EXPECT_EQ(safeBlocks, UINT16_MAX) << "UINT16_MAX * ALP_VECTOR_SIZE should yield exactly UINT16_MAX blocks";

    size_t overflowBlocks = (overflow + ALP_VECTOR_SIZE - 1) / ALP_VECTOR_SIZE;
    EXPECT_GT(overflowBlocks, static_cast<size_t>(UINT16_MAX))
        << "One more value should push num_blocks past UINT16_MAX";
}

// Source-inspection: verify both encode() and encodeInto() contain the guard
#ifndef ALP_ENCODER_SOURCE_PATH
TEST(ALPEncoderOverflow, SourceInspection_EncodeGuardExists) {
    GTEST_SKIP() << "ALP_ENCODER_SOURCE_PATH not defined";
}
TEST(ALPEncoderOverflow, SourceInspection_EncodeIntoGuardExists) {
    GTEST_SKIP() << "ALP_ENCODER_SOURCE_PATH not defined";
}
#else
class ALPEncoderOverflowSourceTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(ALP_ENCODER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open alp_encoder.cpp at: " << ALP_ENCODER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

TEST_F(ALPEncoderOverflowSourceTest, SourceInspection_EncodeGuardExists) {
    // Find encode() function (not encodeInto)
    auto pos = sourceCode.find("ALPEncoder::encode(");
    ASSERT_NE(pos, std::string::npos) << "Could not find ALPEncoder::encode()";

    // Extract a region after the encode() signature to check for the guard
    auto region = sourceCode.substr(pos, 500);
    EXPECT_NE(region.find("UINT16_MAX"), std::string::npos)
        << "encode() must contain a UINT16_MAX overflow check for num_blocks";
    EXPECT_NE(region.find("overflow_error"), std::string::npos)
        << "encode() must throw std::overflow_error on num_blocks overflow";
}

TEST_F(ALPEncoderOverflowSourceTest, SourceInspection_EncodeIntoGuardExists) {
    auto pos = sourceCode.find("ALPEncoder::encodeInto(");
    ASSERT_NE(pos, std::string::npos) << "Could not find ALPEncoder::encodeInto()";

    auto region = sourceCode.substr(pos, 500);
    EXPECT_NE(region.find("UINT16_MAX"), std::string::npos)
        << "encodeInto() must contain a UINT16_MAX overflow check for num_blocks";
    EXPECT_NE(region.find("overflow_error"), std::string::npos)
        << "encodeInto() must throw std::overflow_error on num_blocks overflow";
}
#endif
