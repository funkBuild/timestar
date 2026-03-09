#include <gtest/gtest.h>
#include <fstream>
#include <string>

// =============================================================================
// Source-inspection tests: verify that the HTTP server does NOT use global
// handler pointers that would be allocated on shard 0 but accessed from all
// shards via set_routes().
//
// In Seastar's shard-per-core model, objects allocated on one shard must not
// be accessed from other shards. The original code had global unique_ptrs for
// handlers that were created on shard 0 in the async block, then used from
// every shard when set_routes() registered lambdas that captured `this`.
//
// The fix: create handler instances inside set_routes() so each shard gets
// its own handler instance allocated on the correct shard's heap.
// =============================================================================

class HttpServerShardSafetyTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(HTTP_SERVER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open())
            << "Could not open timestar_http_server.cpp at: " << HTTP_SERVER_SOURCE_PATH;
        sourceCode.assign(
            std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Extract the body of a named free function from the source.
    std::string extractFunctionBody(const std::string& funcSignature) const {
        auto pos = sourceCode.find(funcSignature);
        if (pos == std::string::npos) return "";

        // Find the opening brace of the function body
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos) return "";

        // Brace-match to find the closing brace
        int depth = 1;
        size_t cur = bracePos + 1;
        while (cur < sourceCode.size() && depth > 0) {
            if (sourceCode[cur] == '{') depth++;
            else if (sourceCode[cur] == '}') depth--;
            cur++;
        }
        if (depth != 0) return "";

        return sourceCode.substr(bracePos, cur - bracePos);
    }

    // Count occurrences of a pattern in a string.
    int countOccurrences(const std::string& text, const std::string& pattern) const {
        int count = 0;
        size_t pos = 0;
        while ((pos = text.find(pattern, pos)) != std::string::npos) {
            count++;
            pos += 1;
        }
        return count;
    }
};

// ---------------------------------------------------------------------------
// Test 1: No global unique_ptr handler declarations
//
// The original bug had global std::unique_ptr<HandlerType> variables that were
// created on shard 0 but accessed from all shards.
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, NoGlobalHandlerUniquePointers) {
    // Check that no global unique_ptr handler variables exist
    EXPECT_EQ(sourceCode.find("std::unique_ptr<HttpWriteHandler>"), std::string::npos)
        << "Found global std::unique_ptr<HttpWriteHandler>. "
        << "Handlers must not be stored in global unique_ptrs that are allocated "
        << "on shard 0 but accessed from all shards.";

    EXPECT_EQ(sourceCode.find("std::unique_ptr<timestar::HttpQueryHandler>"), std::string::npos)
        << "Found global std::unique_ptr<timestar::HttpQueryHandler>. "
        << "Handlers must not be stored in global unique_ptrs.";

    // Also check for HttpQueryHandler without namespace
    EXPECT_EQ(sourceCode.find("std::unique_ptr<HttpQueryHandler>"), std::string::npos)
        << "Found global std::unique_ptr<HttpQueryHandler>. "
        << "Handlers must not be stored in global unique_ptrs.";

    EXPECT_EQ(sourceCode.find("std::unique_ptr<HttpDeleteHandler>"), std::string::npos)
        << "Found global std::unique_ptr<HttpDeleteHandler>. "
        << "Handlers must not be stored in global unique_ptrs.";

    EXPECT_EQ(sourceCode.find("std::unique_ptr<HttpMetadataHandler>"), std::string::npos)
        << "Found global std::unique_ptr<HttpMetadataHandler>. "
        << "Handlers must not be stored in global unique_ptrs.";
}

// ---------------------------------------------------------------------------
// Test 2: No g_writeHandler, g_queryHandler, g_deleteHandler, g_metadataHandler
//         global variables
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, NoGlobalHandlerVariables) {
    EXPECT_EQ(sourceCode.find("g_writeHandler"), std::string::npos)
        << "Found 'g_writeHandler' global variable. "
        << "Handler globals cause cross-shard memory access.";

    EXPECT_EQ(sourceCode.find("g_queryHandler"), std::string::npos)
        << "Found 'g_queryHandler' global variable. "
        << "Handler globals cause cross-shard memory access.";

    EXPECT_EQ(sourceCode.find("g_deleteHandler"), std::string::npos)
        << "Found 'g_deleteHandler' global variable. "
        << "Handler globals cause cross-shard memory access.";

    EXPECT_EQ(sourceCode.find("g_metadataHandler"), std::string::npos)
        << "Found 'g_metadataHandler' global variable. "
        << "Handler globals cause cross-shard memory access.";
}

// ---------------------------------------------------------------------------
// Test 3: Handlers are created inside set_routes()
//
// Each shard must create its own handler instances inside set_routes() so
// they are allocated on the correct shard's heap.
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, HandlersCreatedInsideSetRoutes) {
    std::string setRoutesBody = extractFunctionBody("void set_routes(");
    ASSERT_FALSE(setRoutesBody.empty())
        << "Could not extract set_routes function body";

    // Handlers must be created with new inside set_routes
    EXPECT_NE(setRoutesBody.find("HttpWriteHandler"), std::string::npos)
        << "set_routes must create an HttpWriteHandler instance per shard.";

    EXPECT_NE(setRoutesBody.find("HttpQueryHandler"), std::string::npos)
        << "set_routes must create an HttpQueryHandler instance per shard.";

    EXPECT_NE(setRoutesBody.find("HttpDeleteHandler"), std::string::npos)
        << "set_routes must create an HttpDeleteHandler instance per shard.";

    EXPECT_NE(setRoutesBody.find("HttpMetadataHandler"), std::string::npos)
        << "set_routes must create an HttpMetadataHandler instance per shard.";
}

// ---------------------------------------------------------------------------
// Test 4: Handlers are heap-allocated (new) inside set_routes()
//
// Since registerRoutes() captures `this` in lambdas, the handler objects must
// outlive the set_routes() call. They must be heap-allocated with new.
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, HandlersAreHeapAllocated) {
    std::string setRoutesBody = extractFunctionBody("void set_routes(");
    ASSERT_FALSE(setRoutesBody.empty())
        << "Could not extract set_routes function body";

    // Check for heap allocation of handlers using new
    EXPECT_NE(setRoutesBody.find("new HttpWriteHandler"), std::string::npos)
        << "HttpWriteHandler must be heap-allocated with new inside set_routes "
        << "because registerRoutes() captures `this` in route lambdas.";

    // HttpQueryHandler might be in timestar:: namespace
    bool hasQueryHandler = setRoutesBody.find("new timestar::HttpQueryHandler") != std::string::npos
                        || setRoutesBody.find("new HttpQueryHandler") != std::string::npos;
    EXPECT_TRUE(hasQueryHandler)
        << "HttpQueryHandler must be heap-allocated with new inside set_routes.";

    EXPECT_NE(setRoutesBody.find("new HttpDeleteHandler"), std::string::npos)
        << "HttpDeleteHandler must be heap-allocated with new inside set_routes.";

    EXPECT_NE(setRoutesBody.find("new HttpMetadataHandler"), std::string::npos)
        << "HttpMetadataHandler must be heap-allocated with new inside set_routes.";
}

// ---------------------------------------------------------------------------
// Test 5: registerRoutes is called inside set_routes()
//
// Each handler's registerRoutes must be called within set_routes().
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, RegisterRoutesCalledInSetRoutes) {
    std::string setRoutesBody = extractFunctionBody("void set_routes(");
    ASSERT_FALSE(setRoutesBody.empty())
        << "Could not extract set_routes function body";

    // Count registerRoutes calls - should be at least 4 (one per handler)
    int registerCalls = countOccurrences(setRoutesBody, "registerRoutes");
    EXPECT_GE(registerCalls, 4)
        << "set_routes must call registerRoutes() for all 4 handlers "
        << "(write, query, delete, metadata). Found " << registerCalls << " calls.";
}

// ---------------------------------------------------------------------------
// Test 6: No handler creation in the main() / seastar::async block
//
// Handler creation must NOT happen in the main async block where it would
// be on shard 0's heap.
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, NoHandlerCreationInMain) {
    // Find the main function body
    std::string mainBody = extractFunctionBody("int main(");
    ASSERT_FALSE(mainBody.empty())
        << "Could not extract main function body";

    // The main body should not contain make_unique for any handler type
    EXPECT_EQ(mainBody.find("make_unique<HttpWriteHandler>"), std::string::npos)
        << "Found make_unique<HttpWriteHandler> in main(). "
        << "Handlers must be created per-shard in set_routes(), not in main().";

    EXPECT_EQ(mainBody.find("make_unique<timestar::HttpQueryHandler>"), std::string::npos)
        << "Found make_unique<timestar::HttpQueryHandler> in main(). "
        << "Handlers must be created per-shard in set_routes(), not in main().";

    EXPECT_EQ(mainBody.find("make_unique<HttpDeleteHandler>"), std::string::npos)
        << "Found make_unique<HttpDeleteHandler> in main(). "
        << "Handlers must be created per-shard in set_routes(), not in main().";

    EXPECT_EQ(mainBody.find("make_unique<HttpMetadataHandler>"), std::string::npos)
        << "Found make_unique<HttpMetadataHandler> in main(). "
        << "Handlers must be created per-shard in set_routes(), not in main().";
}

// ---------------------------------------------------------------------------
// Test 7: g_engine is passed to handlers in set_routes()
//
// The handlers need the sharded<Engine>* pointer to do their work.
// ---------------------------------------------------------------------------
TEST_F(HttpServerShardSafetyTest, EnginePointerPassedToHandlers) {
    std::string setRoutesBody = extractFunctionBody("void set_routes(");
    ASSERT_FALSE(setRoutesBody.empty())
        << "Could not extract set_routes function body";

    // g_engine must be referenced in set_routes for passing to handlers
    EXPECT_NE(setRoutesBody.find("g_engine"), std::string::npos)
        << "set_routes must reference g_engine to pass it to handler constructors.";
}
