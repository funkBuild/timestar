// Source-inspection tests for the deleteRangeBySeries phantom metadata bug fix.
//
// Bug: deleteRangeBySeries() called getOrCreateSeriesId() which creates
// metadata for a series that doesn't exist. Deleting a non-existent series
// should be a no-op, not create phantom metadata entries.
//
// Fix: Use getSeriesId() (lookup-only) instead, and return false early if the
// series doesn't exist. Since metadata lives on shard 0, the lookup must be
// routed to shard 0 via shardedRef when called on a non-zero shard.
//
// These are source-inspection tests because deleteRangeBySeries is a Seastar
// coroutine that requires the full Seastar runtime. We verify the fix by
// reading and parsing the source code.

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

class EngineDeletePhantomTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef ENGINE_SOURCE_PATH
        std::ifstream file(ENGINE_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif

        std::vector<std::string> paths = {
            "../lib/core/engine.cpp",
            "../../lib/core/engine.cpp",
        };

        for (const auto& path : paths) {
            std::ifstream file(path);
            if (file.is_open()) {
                std::stringstream ss;
                ss << file.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }

    // Extract the deleteRangeBySeries function body from source
    std::string extractDeleteRangeBySeriesFunction() const {
        size_t start = sourceCode.find("Engine::deleteRangeBySeries");
        if (start == std::string::npos)
            return "";

        // Find the opening brace of the function
        size_t braceStart = sourceCode.find('{', start);
        if (braceStart == std::string::npos)
            return "";

        // Find matching closing brace (simple brace counting)
        int braceCount = 1;
        size_t pos = braceStart + 1;
        while (pos < sourceCode.size() && braceCount > 0) {
            if (sourceCode[pos] == '{')
                braceCount++;
            else if (sourceCode[pos] == '}')
                braceCount--;
            pos++;
        }

        return sourceCode.substr(start, pos - start);
    }
};

// Verify that the source file was successfully loaded
TEST_F(EngineDeletePhantomTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load engine.cpp source file";
    ASSERT_NE(sourceCode.find("Engine::deleteRangeBySeries"), std::string::npos)
        << "Source file does not contain Engine::deleteRangeBySeries()";
}

// Core test: deleteRangeBySeries must NOT use getOrCreateSeriesId
TEST_F(EngineDeletePhantomTest, DoesNotUseGetOrCreateSeriesId) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty()) << "Could not extract deleteRangeBySeries() function body";

    EXPECT_EQ(funcBody.find("getOrCreateSeriesId"), std::string::npos)
        << "BUG: deleteRangeBySeries() still uses getOrCreateSeriesId(). "
           "This creates phantom metadata entries when deleting a non-existent "
           "series. Use getSeriesId() (lookup-only) instead.";
}

// Core test: deleteRangeBySeries must use getSeriesId (lookup-only)
TEST_F(EngineDeletePhantomTest, UsesGetSeriesIdLookupOnly) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty());

    EXPECT_NE(funcBody.find("getSeriesId"), std::string::npos)
        << "deleteRangeBySeries() should use getSeriesId() to look up the "
           "series without creating it.";
}

// Core test: deleteRangeBySeries must handle the case where getSeriesId
// returns std::nullopt (series doesn't exist) by returning false early
TEST_F(EngineDeletePhantomTest, HandlesNulloptByReturningFalse) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty());

    // Should check has_value() or !seriesIdOpt or similar
    bool checksOptional =
        funcBody.find("has_value") != std::string::npos || funcBody.find("!seriesId") != std::string::npos ||
        funcBody.find("nullopt") != std::string::npos || funcBody.find("std::nullopt") != std::string::npos;

    EXPECT_TRUE(checksOptional) << "BUG: deleteRangeBySeries() does not check whether getSeriesId() "
                                   "returned std::nullopt. If the series doesn't exist, it should "
                                   "return false early (no-op).";

    // Should co_return false when series doesn't exist
    EXPECT_NE(funcBody.find("co_return false"), std::string::npos)
        << "deleteRangeBySeries() should co_return false when the series "
           "doesn't exist (getSeriesId returns nullopt).";
}

// Verify that the function still calls deleteRange for existing series
TEST_F(EngineDeletePhantomTest, StillCallsDeleteRangeForExistingSeries) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty());

    EXPECT_NE(funcBody.find("deleteRange"), std::string::npos)
        << "deleteRangeBySeries() should still call deleteRange() for "
           "existing series that pass the existence check.";
}

// Verify the function still constructs the series key for deleteRange
TEST_F(EngineDeletePhantomTest, ConstructsSeriesKeyForDeleteRange) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty());

    EXPECT_NE(funcBody.find("seriesKey"), std::string::npos)
        << "deleteRangeBySeries() should construct a series key to pass "
           "to deleteRange().";
}

// Verify the existence check uses local index lookup.
// With the distributed index, each shard has its own NativeIndex containing
// metadata for its series. The getSeriesId call uses the local index directly.
TEST_F(EngineDeletePhantomTest, RoutesMetadataCheckToShard0) {
    std::string funcBody = extractDeleteRangeBySeriesFunction();
    ASSERT_FALSE(funcBody.empty());

    // With distributed index, deleteRangeBySeries uses local index.getSeriesId()
    bool usesLocalIndex = funcBody.find("index.getSeriesId") != std::string::npos;

    EXPECT_TRUE(usesLocalIndex) << "deleteRangeBySeries() must use the local index.getSeriesId() "
                                   "for metadata lookup in the distributed index model.";
}
