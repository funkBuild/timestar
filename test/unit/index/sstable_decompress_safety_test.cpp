#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Bug #25: SSTable missing decompressed size verification after ZSTD
// Bug #26: SSTable unbounded uncompressedSize allocation from disk data
//
// After ZSTD decompression, the actual decompressed size was not compared
// against the expected uncompressedSize from the block header.  A mismatch
// could lead to silent data corruption.
//
// Additionally, the uncompressedSize from disk was used directly to allocate
// a string without any sanity bound.  A corrupted/malicious file could claim
// a multi-GB block, causing OOM.  The fix adds a 64MB cap.
// =============================================================================

#ifndef SSTABLE_SOURCE_PATH
TEST(SSTableDecompressSafety, SourceInspection_SizeCheckExists) {
    GTEST_SKIP() << "SSTABLE_SOURCE_PATH not defined";
}
TEST(SSTableDecompressSafety, SourceInspection_SizeLimitExists) {
    GTEST_SKIP() << "SSTABLE_SOURCE_PATH not defined";
}
#else
class SSTableDecompressSafetyTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(SSTABLE_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open sstable.cpp at: " << SSTABLE_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

TEST_F(SSTableDecompressSafetyTest, SourceInspection_SizeCheckExists) {
    // After decompression, there should be a check: decompSize != uncompressedSize
    EXPECT_NE(sourceCode.find("decompSize != uncompressedSize"), std::string::npos)
        << "sstable.cpp must verify that decompressed size matches the expected "
        << "uncompressedSize from the block header";
}

TEST_F(SSTableDecompressSafetyTest, SourceInspection_SizeCheckThrows) {
    auto pos = sourceCode.find("decompSize != uncompressedSize");
    ASSERT_NE(pos, std::string::npos);

    // Check that there's a throw nearby
    auto region = sourceCode.substr(pos, 300);
    EXPECT_NE(region.find("throw"), std::string::npos)
        << "The decompressed size mismatch check must throw an exception";
    EXPECT_NE(region.find("size mismatch"), std::string::npos)
        << "The error message should mention 'size mismatch'";
}

TEST_F(SSTableDecompressSafetyTest, SourceInspection_SizeLimitExists) {
    // There should be a MAX_BLOCK_SIZE constant
    EXPECT_NE(sourceCode.find("MAX_BLOCK_SIZE"), std::string::npos)
        << "sstable.cpp must define a MAX_BLOCK_SIZE sanity limit for decompression";
}

TEST_F(SSTableDecompressSafetyTest, SourceInspection_SizeLimitIs64MB) {
    // 64MB = 64 * 1024 * 1024
    EXPECT_NE(sourceCode.find("64 * 1024 * 1024"), std::string::npos)
        << "MAX_BLOCK_SIZE should be 64 * 1024 * 1024 (64MB)";
}

TEST_F(SSTableDecompressSafetyTest, SourceInspection_SizeLimitCheckedBeforeAllocation) {
    // The MAX_BLOCK_SIZE check must appear BEFORE the string allocation
    auto limitPos = sourceCode.find("MAX_BLOCK_SIZE");
    auto allocPos = sourceCode.find("std::string result(uncompressedSize");

    ASSERT_NE(limitPos, std::string::npos);
    ASSERT_NE(allocPos, std::string::npos);

    EXPECT_LT(limitPos, allocPos)
        << "MAX_BLOCK_SIZE check must appear before the string allocation "
        << "to prevent unbounded memory allocation from corrupted data";
}
#endif
