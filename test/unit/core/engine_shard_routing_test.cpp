#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection tests: verify that executeLocalQuery and deleteByPattern
// use the local NativeIndex directly (distributed index model).
//
// With the distributed index, each shard has its own NativeIndex containing
// metadata only for series whose data lives on that shard. There is no need
// to route lookups to shard 0 — all index access is local.
// =============================================================================

class EngineShardRoutingTest : public ::testing::Test {
protected:
    std::string sourceCode;

    std::string extractFunctionBody(const std::string& funcName) const {
        std::string pattern = "Engine::" + funcName + "(";
        auto pos = sourceCode.find(pattern);
        if (pos == std::string::npos)
            return "";
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos)
            return "";
        int depth = 1;
        size_t cur = bracePos + 1;
        while (cur < sourceCode.size() && depth > 0) {
            if (sourceCode[cur] == '{')
                depth++;
            else if (sourceCode[cur] == '}')
                depth--;
            cur++;
        }
        if (depth != 0)
            return "";
        return sourceCode.substr(bracePos, cur - bracePos);
    }

    void SetUp() override {
        std::ifstream file(ENGINE_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open engine.cpp at: " << ENGINE_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

// ---------------------------------------------------------------------------
// Test 1: executeLocalQuery uses local index (no shard-0 routing)
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, ExecuteLocalQueryRoutesToShard0) {
    std::string body = extractFunctionBody("executeLocalQuery");
    ASSERT_FALSE(body.empty()) << "Could not extract executeLocalQuery function body";

    // With distributed index, metadata lookups use local index directly
    EXPECT_NE(body.find("getSeriesMetadataBatch"), std::string::npos)
        << "executeLocalQuery must call getSeriesMetadataBatch on local index.";

    // Should NOT route through shard 0 — all index access is local
    EXPECT_EQ(body.find("invoke_on(0"), std::string::npos)
        << "executeLocalQuery should not route to shard 0 in distributed index model.";
}

// ---------------------------------------------------------------------------
// Test 2: deleteByPattern uses local index methods
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, DeleteByPatternRoutesToShard0) {
    std::string body = extractFunctionBody("deleteByPattern");
    ASSERT_FALSE(body.empty()) << "Could not extract deleteByPattern function body";

    // Must use local index for series lookup
    EXPECT_NE(body.find("getAllSeriesForMeasurement"), std::string::npos)
        << "deleteByPattern must call getAllSeriesForMeasurement on local index.";

    EXPECT_NE(body.find("findSeries"), std::string::npos)
        << "deleteByPattern must call findSeries on local index.";

    EXPECT_NE(body.find("getSeriesMetadata"), std::string::npos)
        << "deleteByPattern must call getSeriesMetadata on local index.";
}

// ---------------------------------------------------------------------------
// Test 3: executeLocalQuery uses direct local index access
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, NoDirectIndexAccessInExecuteLocalQuery) {
    std::string body = extractFunctionBody("executeLocalQuery");
    ASSERT_FALSE(body.empty()) << "Could not extract executeLocalQuery function body";

    // With distributed index, direct local index access is the correct pattern
    EXPECT_NE(body.find("index.getSeriesMetadataBatch"), std::string::npos)
        << "executeLocalQuery must use local index.getSeriesMetadataBatch directly.";
}

// ---------------------------------------------------------------------------
// Test 4: deleteByPattern uses direct local index access
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, NoDirectIndexAccessInDeleteByPattern) {
    std::string body = extractFunctionBody("deleteByPattern");
    ASSERT_FALSE(body.empty()) << "Could not extract deleteByPattern function body";

    // With distributed index, local index access is the correct pattern
    EXPECT_NE(body.find("index.getAllSeriesForMeasurement"), std::string::npos)
        << "deleteByPattern must use local index.getAllSeriesForMeasurement directly.";

    EXPECT_NE(body.find("index.findSeries"), std::string::npos)
        << "deleteByPattern must use local index.findSeries directly.";
}
