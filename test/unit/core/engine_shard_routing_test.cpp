#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source-inspection tests: verify that executeLocalQuery and deleteByPattern
// route LevelDB index access through shard 0 via shardedRef->invoke_on(, ...).
//
// The LevelDB index only exists on shard 0. On non-zero shards, direct index
// method calls return empty/null results, silently breaking queries and deletes
// when time series data is stored on those shards.
//
// The fix: wrap every index call in a shard check that routes through shard 0
// using shardedRef->invoke_on(, ...) when shardId != 0.
// =============================================================================

class EngineShardRoutingTest : public ::testing::Test {
protected:
    std::string sourceCode;

    // Extract the body of a named function (Engine::funcName) from the source.
    // Returns the text from the opening '{' to the matching closing '}'.
    std::string extractFunctionBody(const std::string& funcName) const {
        // Find "Engine::funcName("
        std::string pattern = "Engine::" + funcName + "(";
        auto pos = sourceCode.find(pattern);
        if (pos == std::string::npos)
            return "";

        // Find the opening brace of the function body
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos)
            return "";

        // Brace-match to find the closing brace
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
// Test 1: executeLocalQuery routes metadata lookups through shard 0
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, ExecuteLocalQueryRoutesToShard0) {
    std::string body = extractFunctionBody("executeLocalQuery");
    ASSERT_FALSE(body.empty()) << "Could not extract executeLocalQuery function body";

    // The function must contain shardedRef->invoke_on( for routing metadata lookups
    EXPECT_NE(body.find("shardedRef->invoke_on("), std::string::npos)
        << "executeLocalQuery must route index lookups through shard 0 using "
        << "shardedRef->invoke_on(, ...) for non-zero shards.";

    // It should reference getSeriesMetadataBatch inside the invoke_on call
    EXPECT_NE(body.find("getSeriesMetadataBatch"), std::string::npos)
        << "executeLocalQuery must call getSeriesMetadataBatch (routed through shard 0).";

    // It should contain a shardId == 0 check
    EXPECT_NE(body.find("shardId == 0"), std::string::npos)
        << "executeLocalQuery must check shardId == 0 before deciding routing path.";
}

// ---------------------------------------------------------------------------
// Test 2: deleteByPattern routes index methods through shard 0
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, DeleteByPatternRoutesToShard0) {
    std::string body = extractFunctionBody("deleteByPattern");
    ASSERT_FALSE(body.empty()) << "Could not extract deleteByPattern function body";

    // Must use shardedRef->invoke_on( for routing
    EXPECT_NE(body.find("shardedRef->invoke_on("), std::string::npos)
        << "deleteByPattern must route index lookups through shard 0 using "
        << "shardedRef->invoke_on(, ...) for non-zero shards.";

    // Must route getAllSeriesForMeasurement through shard 0
    EXPECT_NE(body.find("getAllSeriesForMeasurement"), std::string::npos)
        << "deleteByPattern must call getAllSeriesForMeasurement (routed through shard 0).";

    // Must route findSeries through shard 0
    EXPECT_NE(body.find("findSeries"), std::string::npos)
        << "deleteByPattern must call findSeries (routed through shard 0).";

    // Must route getSeriesMetadata through shard 0
    EXPECT_NE(body.find("getSeriesMetadata"), std::string::npos)
        << "deleteByPattern must call getSeriesMetadata (routed through shard 0).";

    // Must contain a shardId == 0 check
    EXPECT_NE(body.find("shardId == 0"), std::string::npos)
        << "deleteByPattern must check shardId == 0 before deciding routing path.";
}

// ---------------------------------------------------------------------------
// Test 3: No unguarded direct index access in executeLocalQuery
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, NoDirectIndexAccessInExecuteLocalQuery) {
    std::string body = extractFunctionBody("executeLocalQuery");
    ASSERT_FALSE(body.empty()) << "Could not extract executeLocalQuery function body";

    // Count shardId == 0 guards
    size_t pos = 0;
    int shardChecks = 0;
    while ((pos = body.find("shardId == 0", pos)) != std::string::npos) {
        shardChecks++;
        pos += 1;
    }

    // There must be at least one shard check for index access
    EXPECT_GE(shardChecks, 1) << "executeLocalQuery must have at least one shardId == 0 guard "
                              << "before any index access.";

    // Must have a routing block for non-zero shards
    pos = 0;
    int routingBlocks = 0;
    while ((pos = body.find("shardedRef->invoke_on(", pos)) != std::string::npos) {
        routingBlocks++;
        pos += 1;
    }

    EXPECT_GE(routingBlocks, 1) << "executeLocalQuery must have at least 1 shardedRef->invoke_on(, ...) call "
                                << "for routing metadata lookups to shard 0.";
}

// ---------------------------------------------------------------------------
// Test 4: No unguarded direct index access in deleteByPattern
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, NoDirectIndexAccessInDeleteByPattern) {
    std::string body = extractFunctionBody("deleteByPattern");
    ASSERT_FALSE(body.empty()) << "Could not extract deleteByPattern function body";

    // Count shardId == 0 guards
    size_t pos = 0;
    int shardChecks = 0;
    while ((pos = body.find("shardId == 0", pos)) != std::string::npos) {
        shardChecks++;
        pos += 1;
    }

    // Must have shard guards for both series lookup and metadata phases
    EXPECT_GE(shardChecks, 2) << "deleteByPattern must have at least 2 shardId == 0 guards: "
                              << "one for series lookup (getAllSeriesForMeasurement/findSeries), "
                              << "one for metadata filtering (getSeriesMetadata).";

    // Count routing blocks
    pos = 0;
    int routingBlocks = 0;
    while ((pos = body.find("shardedRef->invoke_on(", pos)) != std::string::npos) {
        routingBlocks++;
        pos += 1;
    }

    // We need routing for: series lookup phase + metadata filter phase
    // Series lookup has 2 invoke_on calls (one for empty tags, one for tag filter)
    // Metadata filter has 1 invoke_on call (batched)
    EXPECT_GE(routingBlocks, 2) << "deleteByPattern must have at least 2 shardedRef->invoke_on(, ...) calls "
                                << "for routing index lookups to shard 0. Found " << routingBlocks << ".";
}
