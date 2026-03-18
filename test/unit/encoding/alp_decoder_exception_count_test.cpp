#include "alp/alp_decoder.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Bug #10: ALP decoder exception_count overflow
//
// After reading exception_count from the block header, the decoder used it
// to size stack arrays without validating that it does not exceed block_count.
// A corrupted stream could cause stack buffer overflow.  The fix adds:
//   if (exception_count > block_count) throw std::runtime_error(...)
// =============================================================================

#ifndef ALP_DECODER_SOURCE_PATH
TEST(ALPDecoderExceptionCount, SourceInspection_BoundsCheckExists) {
    GTEST_SKIP() << "ALP_DECODER_SOURCE_PATH not defined";
}
#else
class ALPDecoderExceptionCountTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(ALP_DECODER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open alp_decoder.cpp at: " << ALP_DECODER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

TEST_F(ALPDecoderExceptionCountTest, SourceInspection_BoundsCheckExists) {
    // The bounds check should appear after exception_count is read
    EXPECT_NE(sourceCode.find("exception_count > block_count"), std::string::npos)
        << "alp_decoder.cpp must contain a bounds check: exception_count > block_count";
}

TEST_F(ALPDecoderExceptionCountTest, SourceInspection_ThrowsOnViolation) {
    // Verify it throws a runtime_error (not silently continues)
    auto pos = sourceCode.find("exception_count > block_count");
    ASSERT_NE(pos, std::string::npos);

    // Check that the throw is nearby (within 200 chars)
    auto region = sourceCode.substr(pos, 200);
    EXPECT_NE(region.find("throw"), std::string::npos)
        << "The exception_count > block_count check must throw an exception";
    EXPECT_NE(region.find("runtime_error"), std::string::npos)
        << "The exception_count > block_count check must throw a std::runtime_error";
}

TEST_F(ALPDecoderExceptionCountTest, SourceInspection_BothPathsProtected) {
    // There should be at least 2 occurrences of the check (ALP and ALP_RD paths)
    size_t count = 0;
    size_t pos = 0;
    while ((pos = sourceCode.find("exception_count > block_count", pos)) != std::string::npos) {
        ++count;
        pos += 10;
    }
    EXPECT_GE(count, 2u)
        << "Both ALP and ALP_RD decode paths must have the exception_count bounds check";
}
#endif
