#include <gtest/gtest.h>

#include <map>
#include <string>
#include <variant>
#include <vector>

// This test file verifies the ordering contract in the write handler:
// Data must be inserted BEFORE metadata is indexed. This is the crash-safe
// order because:
//   - If data insert succeeds but metadata fails: data is durable, can be
//     discovered on retry.
//   - If metadata succeeds but data fails: we have phantom metadata pointing
//     to nonexistent data (the old, buggy behavior).
//
// These tests are purely structural: they verify the source code ordering
// by inspecting the file content, since the write handler requires the full
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
};

// Verify that the source file was successfully loaded
TEST_F(HttpWriteHandlerAtomicityTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load http_write_handler.cpp source file";
    ASSERT_NE(sourceCode.find("processMultiWritePoint"), std::string::npos)
        << "Source file does not contain processMultiWritePoint";
}

// Verify that processMultiWritePoint has the correct ordering:
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

// =================== Bug fix regression tests ===================
// These tests verify structural fixes in the write handler by inspecting
// the source code, since the handler requires the full Seastar runtime.

// Helper: extract a named function body from the full source code.
static std::string extractFunctionBody(const std::string& sourceCode, const std::string& funcSignature) {
    size_t start = sourceCode.find(funcSignature);
    if (start == std::string::npos)
        return "";

    size_t braceStart = sourceCode.find('{', start);
    if (braceStart == std::string::npos)
        return "";

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

// Bug 1: Batch write failures must decrement pointsWritten.
// Previously, processMultiWritePoint failures were caught and logged but
// pointsWritten was never decremented, causing the response to report
// more points written than actually persisted (silent data loss).
TEST_F(HttpWriteHandlerAtomicityTest, BatchFailuresDecrementPointsWritten) {
    std::string handleBody = extractFunctionBody(sourceCode, "HttpWriteHandler::handleWrite(");
    ASSERT_FALSE(handleBody.empty()) << "Could not extract handleWrite function body";

    // The batch result loop must subtract failed points from pointsWritten.
    // Look for the decrement in the catch block of the mwpResults loop.
    EXPECT_NE(handleBody.find("pointsWritten -= perMwpPoints["), std::string::npos)
        << "Batch write catch block must decrement pointsWritten by the "
           "per-MWP point count when a write fails";

    // There should be a failedWrites counter
    EXPECT_NE(handleBody.find("failedWrites"), std::string::npos)
        << "Batch write path must track the number of failed writes";

    // There should be a partial failure response when some writes fail
    EXPECT_NE(handleBody.find("createPartialFailureResponse"), std::string::npos)
        << "Batch write path must return a partial failure response "
           "when some writes fail, not a success response";
}

// Bug 2: pointsWritten must be int64_t to avoid integer overflow.
// size_t * size_t (timestamps.size() * fields.size()) can exceed INT_MAX
// for large batch writes. Using int would cause undefined behavior.
TEST_F(HttpWriteHandlerAtomicityTest, PointsWrittenIsInt64) {
    std::string handleBody = extractFunctionBody(sourceCode, "HttpWriteHandler::handleWrite(");
    ASSERT_FALSE(handleBody.empty()) << "Could not extract handleWrite function body";

    // pointsWritten must be declared as int64_t, not int
    EXPECT_NE(handleBody.find("int64_t pointsWritten"), std::string::npos)
        << "pointsWritten must be int64_t to prevent overflow from "
           "size_t * size_t multiplication";

    // The int declaration should NOT be present (would indicate the bug is back)
    // Search for "int pointsWritten" that is NOT preceded by "int64_t"
    auto pos = handleBody.find("int pointsWritten");
    if (pos != std::string::npos) {
        // Make sure this is actually "int64_t pointsWritten", not bare "int pointsWritten"
        bool isInt64 = (pos >= 4 && handleBody.substr(pos - 4, 4) == "int6");
        EXPECT_TRUE(isInt64) << "Found bare 'int pointsWritten' declaration instead of 'int64_t pointsWritten'";
    }
}

// Also verify that the fast-path and single-write paths use int64_t casts
// to prevent overflow when computing pointsWritten from size_t multiplication.
TEST_F(HttpWriteHandlerAtomicityTest, PointsWrittenCastsPreventOverflow) {
    std::string handleBody = extractFunctionBody(sourceCode, "HttpWriteHandler::handleWrite(");
    ASSERT_FALSE(handleBody.empty()) << "Could not extract handleWrite function body";

    // The fast-path and single-write paths should cast to int64_t before multiplying
    EXPECT_NE(handleBody.find("static_cast<int64_t>(mwp.timestamps.size())"), std::string::npos)
        << "pointsWritten computation must cast size_t to int64_t before "
           "multiplication to prevent overflow";
}

// Bug 3: Fast-path must not handle batch writes with "writes" key after byte 64.
// Previously, the fast-path only checked the first 64 bytes for "writes" key.
// If "writes" appeared later (e.g., JSON with many tags), the fast-path would
// parse only measurement/tags/fields as a single write, silently dropping the
// entire "writes" array.
TEST_F(HttpWriteHandlerAtomicityTest, FastPathChecksEntireBodyForWritesKey) {
    std::string handleBody = extractFunctionBody(sourceCode, "HttpWriteHandler::handleWrite(");
    ASSERT_FALSE(handleBody.empty()) << "Could not extract handleWrite function body";

    // The fast-path condition must check writesPos against npos only,
    // not writesPos > 64 (which was the bug).
    bool hasNposCheck = (handleBody.find("writesPos == std::string::npos") != std::string::npos) ||
                        (handleBody.find("writesPos == std::string_view::npos") != std::string::npos);
    EXPECT_TRUE(hasNposCheck)
        << "Fast-path must only skip batch detection when \"writes\" key is "
           "completely absent (npos), not when it appears after byte 64";

    // The buggy condition "writesPos > 64" must NOT be present
    EXPECT_EQ(handleBody.find("writesPos > 64"), std::string::npos)
        << "Fast-path must not use 'writesPos > 64' condition -- this causes "
           "batch writes to be silently dropped when \"writes\" key appears "
           "after byte 64 in the JSON body";
}

// Verify the createPartialFailureResponse method exists in the source
TEST_F(HttpWriteHandlerAtomicityTest, PartialFailureResponseMethodExists) {
    EXPECT_NE(sourceCode.find("HttpWriteHandler::createPartialFailureResponse"), std::string::npos)
        << "createPartialFailureResponse method must exist to report partial "
           "batch write failures to clients";

    // The partial response should include status, points_written, failed_writes, and errors
    std::string partialBody = extractFunctionBody(sourceCode, "HttpWriteHandler::createPartialFailureResponse(");
    ASSERT_FALSE(partialBody.empty()) << "Could not extract createPartialFailureResponse function body";

    EXPECT_NE(partialBody.find("\"partial\""), std::string::npos)
        << "Partial failure response must have status \"partial\"";
    EXPECT_NE(partialBody.find("points_written"), std::string::npos)
        << "Partial failure response must include points_written count";
    EXPECT_NE(partialBody.find("failed_writes"), std::string::npos)
        << "Partial failure response must include failed_writes count";
    EXPECT_NE(partialBody.find("errors"), std::string::npos) << "Partial failure response must include error messages";
}

// Verify the createSuccessResponse signature uses int64_t
TEST_F(HttpWriteHandlerAtomicityTest, SuccessResponseUsesInt64) {
    EXPECT_NE(sourceCode.find("createSuccessResponse(int64_t pointsWritten)"), std::string::npos)
        << "createSuccessResponse must accept int64_t, not int, to match "
           "the int64_t pointsWritten variable in handleWrite";
}
