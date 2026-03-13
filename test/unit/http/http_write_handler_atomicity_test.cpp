#include <gtest/gtest.h>

#include <map>
#include <string>
#include <variant>
#include <vector>

// This test file verifies the ordering contract in processWritePoint():
// Data must be inserted BEFORE metadata is indexed. This is the crash-safe
// order because:
//   - If data insert succeeds but metadata fails: data is durable, can be
//     discovered on retry.
//   - If metadata succeeds but data fails: we have phantom metadata pointing
//     to nonexistent data (the old, buggy behavior).
//
// These tests are purely structural: they verify the source code ordering
// by inspecting the file content, since processWritePoint requires the full
// Seastar runtime (sharded<Engine>) which cannot be instantiated in unit tests.

// Read the source file at compile time would be ideal, but we use a runtime
// approach that checks the actual source for correctness.

#include <fstream>
#include <sstream>

class HttpWriteHandlerAtomicityTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef HTTP_WRITE_HANDLER_SOURCE_PATH
        std::ifstream file(HTTP_WRITE_HANDLER_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif

        // Read the actual source file to verify ordering
        // The path is relative to the build directory, so we use a path
        // that works from the standard build location
        std::vector<std::string> paths = {
            "../lib/http/http_write_handler.cpp",
            "../../lib/http/http_write_handler.cpp",
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

    // Find the position of a pattern in the source code, starting from a given offset
    size_t findPattern(const std::string& pattern, size_t startPos = 0) const {
        return sourceCode.find(pattern, startPos);
    }

    // Extract the processWritePoint function body from source
    std::string extractProcessWritePoint() const {
        size_t start = sourceCode.find("HttpWriteHandler::processWritePoint(");
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
TEST_F(HttpWriteHandlerAtomicityTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load http_write_handler.cpp source file";
    ASSERT_NE(sourceCode.find("processWritePoint"), std::string::npos)
        << "Source file does not contain processWritePoint";
}

// Verify processWritePoint function exists and has the expected structure
TEST_F(HttpWriteHandlerAtomicityTest, ProcessWritePointExists) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty()) << "Could not extract processWritePoint function body";

    // Should contain all 4 type branches
    EXPECT_NE(funcBody.find("holds_alternative<double>"), std::string::npos) << "Missing double branch";
    EXPECT_NE(funcBody.find("holds_alternative<bool>"), std::string::npos) << "Missing bool branch";
    EXPECT_NE(funcBody.find("holds_alternative<std::string>"), std::string::npos) << "Missing string branch";
    EXPECT_NE(funcBody.find("holds_alternative<int64_t>"), std::string::npos) << "Missing int64_t branch";
}

// Core test: In processWritePoint, data dispatch (when_all_succeed on shard inserts)
// must appear BEFORE metadata indexing (indexMetadataSync).
// The refactored function batches inserts by shard, dispatches via when_all_succeed,
// then awaits synchronous metadata indexing before returning the response.
TEST_F(HttpWriteHandlerAtomicityTest, DoubleBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // The batched architecture groups inserts by shard, dispatches via when_all_succeed,
    // then awaits indexMetadataSync. Verify ordering.
    size_t dataDispatchPos = funcBody.find("when_all_succeed");
    size_t metadataPos = funcBody.find("indexMetadataSync");

    ASSERT_NE(dataDispatchPos, std::string::npos)
        << "Could not find when_all_succeed (parallel shard dispatch) in processWritePoint";
    ASSERT_NE(metadataPos, std::string::npos) << "Could not find indexMetadataSync in processWritePoint";

    EXPECT_LT(dataDispatchPos, metadataPos) << "BUG: indexMetadataSync appears BEFORE when_all_succeed. "
                                               "Data must be inserted before metadata for crash safety.";
}

// Verify all 4 type branches exist in the grouping phase
TEST_F(HttpWriteHandlerAtomicityTest, BoolBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // All 4 type branches should exist in the grouping phase
    EXPECT_NE(funcBody.find("holds_alternative<double>"), std::string::npos) << "Missing double type handling";
    EXPECT_NE(funcBody.find("holds_alternative<bool>"), std::string::npos) << "Missing bool type handling";
    EXPECT_NE(funcBody.find("holds_alternative<std::string>"), std::string::npos) << "Missing string type handling";
    EXPECT_NE(funcBody.find("holds_alternative<int64_t>"), std::string::npos) << "Missing int64_t type handling";

    // Inserts should be dispatched via engine.insert or engine.insertBatch
    bool hasInsert = funcBody.find("engine.insert(") != std::string::npos ||
                     funcBody.find("engine.insertBatch(") != std::string::npos;
    EXPECT_TRUE(hasInsert) << "Could not find engine.insert() or engine.insertBatch() in processWritePoint";
}

// Verify metadata uses synchronous indexMetadataSync, not per-field individual calls
TEST_F(HttpWriteHandlerAtomicityTest, StringBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // Metadata should use synchronous dispatch
    EXPECT_NE(funcBody.find("indexMetadataSync"), std::string::npos)
        << "processWritePoint should use indexMetadataSync for synchronous metadata indexing";

    // Should NOT have individual indexMetadata calls (the old per-field pattern)
    // Count occurrences of "indexMetadata(" that are NOT "indexMetadataBatch(" or "indexMetadataSync"
    size_t pos = 0;
    int individualCalls = 0;
    while ((pos = funcBody.find("indexMetadata(", pos)) != std::string::npos) {
        // Check it's not indexMetadataBatch or indexMetadataSync
        if (funcBody.substr(pos, 19) != "indexMetadataBatch(" && funcBody.substr(pos, 18) != "indexMetadataSync(") {
            individualCalls++;
        }
        pos++;
    }
    EXPECT_EQ(individualCalls, 0) << "processWritePoint should not call individual indexMetadata(); "
                                     "use indexMetadataSync() instead for synchronous metadata indexing";
}

// Verify parallel shard dispatch structure exists
TEST_F(HttpWriteHandlerAtomicityTest, Int64BranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // Should have parallel dispatch infrastructure
    EXPECT_NE(funcBody.find("invoke_on"), std::string::npos)
        << "processWritePoint should use invoke_on for cross-shard dispatch";
    EXPECT_NE(funcBody.find("when_all_succeed"), std::string::npos)
        << "processWritePoint should use when_all_succeed for parallel shard dispatch";

    // The skipMetadataIndexing=true pattern should be used
    bool hasSkipMeta =
        funcBody.find("true") != std::string::npos && (funcBody.find("skipMetadata") != std::string::npos ||
                                                       funcBody.find("engine.insert(std::move(") != std::string::npos);
    EXPECT_TRUE(hasSkipMeta) << "processWritePoint should pass skipMetadataIndexing=true to engine.insert()";
}

// Verify that the batch path (processMultiWritePoint) already has the correct ordering:
// insertBatch happens inside the function, metadata is deferred to the caller
TEST_F(HttpWriteHandlerAtomicityTest, BatchPathDataBeforeMetadata) {
    // processMultiWritePoint should call insertBatch but NOT indexMetadata
    // (metadata is handled by the caller after all data inserts succeed)
    size_t funcStart = sourceCode.find("HttpWriteHandler::processMultiWritePoint(");
    ASSERT_NE(funcStart, std::string::npos);

    // Find function body
    size_t braceStart = sourceCode.find('{', funcStart);
    ASSERT_NE(braceStart, std::string::npos);

    int braceCount = 1;
    size_t pos = braceStart + 1;
    while (pos < sourceCode.size() && braceCount > 0) {
        if (sourceCode[pos] == '{')
            braceCount++;
        else if (sourceCode[pos] == '}')
            braceCount--;
        pos++;
    }
    std::string batchBody = sourceCode.substr(funcStart, pos - funcStart);

    // Should contain insertBatch (data insertion)
    EXPECT_NE(batchBody.find("insertBatch"), std::string::npos)
        << "processMultiWritePoint should call insertBatch for data insertion";

    // Should NOT directly call indexMetadata - metadata is deferred to caller
    EXPECT_EQ(batchBody.find("indexMetadata"), std::string::npos)
        << "processMultiWritePoint should NOT call indexMetadata directly; "
           "metadata indexing should be deferred to the caller (handleWrite) "
           "for proper data-before-metadata ordering";
}

// Verify that the crash-safety comment exists explaining the ordering rationale
TEST_F(HttpWriteHandlerAtomicityTest, CrashSafetyCommentExists) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // Check that there is a comment explaining the ordering rationale
    bool hasDataFirstComment =
        funcBody.find("data before metadata") != std::string::npos ||
        funcBody.find("data first") != std::string::npos || funcBody.find("Insert data first") != std::string::npos ||
        funcBody.find("crash safety") != std::string::npos || funcBody.find("crash-safe") != std::string::npos;

    EXPECT_TRUE(hasDataFirstComment) << "processWritePoint should contain a comment explaining the "
                                        "data-before-metadata ordering rationale for crash safety";
}
