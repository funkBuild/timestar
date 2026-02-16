#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <regex>

// =============================================================================
// Source code inspection tests for TSMCompactor::compact() series type lookup
//
// These tests verify that the compact() method looks up series types from ALL
// files, not just files[0]. The original bug caused silent data loss when a
// series existed only in files[1..N] because getSeriesType() on files[0]
// returned empty and the series was skipped entirely.
// =============================================================================

class TSMCompactorTypeLookupTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(TSM_COMPACTOR_SOURCE_PATH);
        ASSERT_TRUE(file.is_open())
            << "Could not open tsm_compactor.cpp at: " << TSM_COMPACTOR_SOURCE_PATH;
        sourceCode.assign(
            std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Extract the compact() method body from the source code
    std::string getCompactMethodBody() {
        // Find the compact() method signature
        auto pos = sourceCode.find("TSMCompactor::compact(");
        EXPECT_NE(pos, std::string::npos) << "Could not find TSMCompactor::compact()";
        if (pos == std::string::npos) return "";

        // Find the opening brace of the method
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos) return "";

        // Track brace depth to find the end of the method
        int depth = 1;
        size_t i = bracePos + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }

        return sourceCode.substr(pos, i - pos);
    }

    // Extract the section of compact() that categorizes series by type
    std::string getSeriesCategorizationSection() {
        std::string compactBody = getCompactMethodBody();
        if (compactBody.empty()) return "";

        // Find the loop over allSeries that categorizes by type
        auto pos = compactBody.find("for (const auto& seriesId : allSeries)");
        if (pos == std::string::npos) return "";

        // Find the matching closing brace for this for loop
        auto bracePos = compactBody.find('{', pos);
        if (bracePos == std::string::npos) return "";

        int depth = 1;
        size_t i = bracePos + 1;
        while (i < compactBody.size() && depth > 0) {
            if (compactBody[i] == '{') depth++;
            else if (compactBody[i] == '}') depth--;
            i++;
        }

        return compactBody.substr(pos, i - pos);
    }
};

// Test 1: Verify the series type lookup iterates over ALL files, not just files[0]
TEST_F(TSMCompactorTypeLookupTest, TypeLookupChecksAllFiles) {
    std::string section = getSeriesCategorizationSection();
    ASSERT_FALSE(section.empty()) << "Could not extract series categorization section";

    // There should be a loop over files when looking up series type
    // e.g., "for (const auto& file : files)"
    bool hasFileLoop = (section.find("for (const auto& file : files)") != std::string::npos);

    EXPECT_TRUE(hasFileLoop)
        << "The series type lookup in compact() must iterate over all files, "
        << "not just files[0]. A 'for (const auto& file : files)' loop should "
        << "be present in the series categorization section.\n"
        << "Section contents:\n" << section;
}

// Test 2: Verify that files[0]->getSeriesType is NOT the sole type lookup
TEST_F(TSMCompactorTypeLookupTest, NoSingleFileTypeLookup) {
    std::string section = getSeriesCategorizationSection();
    ASSERT_FALSE(section.empty()) << "Could not extract series categorization section";

    // The bug pattern: "files[0]->getSeriesType(seriesId)" as the only lookup
    bool hasSingleFileLookup = (section.find("files[0]->getSeriesType") != std::string::npos);

    EXPECT_FALSE(hasSingleFileLookup)
        << "Found 'files[0]->getSeriesType' in compact() series categorization. "
        << "This only checks the first file, causing data loss when a series "
        << "exists only in later files. Must iterate over all files instead.\n"
        << "Section contents:\n" << section;

    // Instead, the pattern should use file->getSeriesType (from the loop variable)
    bool hasLoopVariableLookup = (section.find("file->getSeriesType") != std::string::npos);

    EXPECT_TRUE(hasLoopVariableLookup)
        << "Expected 'file->getSeriesType' (using loop variable) in the series "
        << "categorization section of compact().\n"
        << "Section contents:\n" << section;
}

// Test 3: Verify there is a break after finding a type (efficiency optimization)
TEST_F(TSMCompactorTypeLookupTest, TypeLookupBreaksOnFirstMatch) {
    std::string section = getSeriesCategorizationSection();
    ASSERT_FALSE(section.empty()) << "Could not extract series categorization section";

    // Find the inner file loop
    auto fileLoopPos = section.find("for (const auto& file : files)");
    if (fileLoopPos == std::string::npos) {
        FAIL() << "Could not find 'for (const auto& file : files)' loop";
        return;
    }

    // Extract the inner loop body
    auto bracePos = section.find('{', fileLoopPos);
    ASSERT_NE(bracePos, std::string::npos);

    int depth = 1;
    size_t i = bracePos + 1;
    while (i < section.size() && depth > 0) {
        if (section[i] == '{') depth++;
        else if (section[i] == '}') depth--;
        i++;
    }
    std::string innerLoop = section.substr(fileLoopPos, i - fileLoopPos);

    // There should be a break after finding a type
    bool hasBreak = (innerLoop.find("break") != std::string::npos);

    EXPECT_TRUE(hasBreak)
        << "The inner file loop should contain a 'break' statement after finding "
        << "a series type. Once we find which file contains the series, there is "
        << "no need to keep checking remaining files.\n"
        << "Inner loop contents:\n" << innerLoop;

    // The break should be after/inside a has_value() check
    bool hasValueCheck = (innerLoop.find("has_value()") != std::string::npos);

    EXPECT_TRUE(hasValueCheck)
        << "The inner file loop should check has_value() before breaking.\n"
        << "Inner loop contents:\n" << innerLoop;
}

// Test 4: Verify getAllSeriesIds iterates all files (regression prevention)
TEST_F(TSMCompactorTypeLookupTest, AllSeriesIdsFromAllFiles) {
    // Find the getAllSeriesIds method
    auto pos = sourceCode.find("TSMCompactor::getAllSeriesIds(");
    ASSERT_NE(pos, std::string::npos) << "Could not find TSMCompactor::getAllSeriesIds()";

    // Extract the method body
    auto bracePos = sourceCode.find('{', pos);
    ASSERT_NE(bracePos, std::string::npos);

    int depth = 1;
    size_t i = bracePos + 1;
    while (i < sourceCode.size() && depth > 0) {
        if (sourceCode[i] == '{') depth++;
        else if (sourceCode[i] == '}') depth--;
        i++;
    }
    std::string methodBody = sourceCode.substr(pos, i - pos);

    // Should iterate over all files
    bool hasFileLoop = (methodBody.find("for (const auto& file : files)") != std::string::npos);
    EXPECT_TRUE(hasFileLoop)
        << "getAllSeriesIds() must iterate over all files to collect series IDs.\n"
        << "Method contents:\n" << methodBody;

    // Should use getSeriesIds()
    bool getsIds = (methodBody.find("getSeriesIds()") != std::string::npos);
    EXPECT_TRUE(getsIds)
        << "getAllSeriesIds() must call getSeriesIds() on each file.\n"
        << "Method contents:\n" << methodBody;

    // Should use a set or similar to deduplicate
    bool usesSet = (methodBody.find("set<") != std::string::npos ||
                    methodBody.find("unordered_set<") != std::string::npos);
    EXPECT_TRUE(usesSet)
        << "getAllSeriesIds() should use a set to deduplicate series IDs.\n"
        << "Method contents:\n" << methodBody;
}
