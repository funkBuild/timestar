#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Bug #24: TSM writer int64 sum overflow
//
// When computing block stats for Integer series, the sum was accumulated as
// int64_t.  Large values (e.g. INT64_MAX/2 + INT64_MAX/2) cause signed
// integer overflow, which is undefined behavior in C++.  The fix accumulates
// as double from the start (the result is immediately stored as double anyway).
// =============================================================================

#ifndef TSM_WRITER_SOURCE_PATH
TEST(TSMWriterIntSumOverflow, SourceInspection_NoInt64Sum) {
    GTEST_SKIP() << "TSM_WRITER_SOURCE_PATH not defined";
}
#else
class TSMWriterIntSumTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(TSM_WRITER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open tsm_writer.cpp at: " << TSM_WRITER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Find the integer writeIndexBlock method body
    std::string getIntegerWriteIndexBlockBody() {
        // Find the Integer-specific writeIndexBlock (takes int64_t values)
        auto pos = sourceCode.find("writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const int64_t> values");
        if (pos == std::string::npos)
            return "";

        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos)
            return "";

        int depth = 1;
        size_t i = bracePos + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }
        return sourceCode.substr(pos, i - pos);
    }
};

TEST_F(TSMWriterIntSumTest, SourceInspection_NoInt64Sum) {
    std::string body = getIntegerWriteIndexBlockBody();
    ASSERT_FALSE(body.empty()) << "Could not find Integer writeIndexBlock method";

    // The bug pattern: "int64_t sum = 0" accumulating as int64_t
    EXPECT_EQ(body.find("int64_t sum"), std::string::npos)
        << "Found 'int64_t sum' in Integer writeIndexBlock. "
        << "This causes signed integer overflow UB for large values. "
        << "Must use 'double sum' instead.\n"
        << "Method body:\n" << body;
}

TEST_F(TSMWriterIntSumTest, SourceInspection_UsesDoubleSum) {
    std::string body = getIntegerWriteIndexBlockBody();
    ASSERT_FALSE(body.empty()) << "Could not find Integer writeIndexBlock method";

    // The fix: "double sum = 0.0"
    EXPECT_NE(body.find("double sum"), std::string::npos)
        << "Integer writeIndexBlock should use 'double sum' to avoid overflow.\n"
        << "Method body:\n" << body;
}
#endif
