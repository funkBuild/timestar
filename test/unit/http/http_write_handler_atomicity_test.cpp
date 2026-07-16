#include <gtest/gtest.h>

#include <map>
#include <string>
#include <variant>
#include <vector>

// This test file verifies structural properties of the write handler that
// cannot be observed behaviorally within a unit-test budget:
//   - Data must be inserted BEFORE metadata is indexed (crash-safe ordering:
//     observing the crash window would require killing the process mid-write).
//   - pointsWritten must be int64_t (overflow needs >2^31 points to observe).
//   - The fast-path "writes"-key scan must use an npos check, not a byte-64
//     cutoff (with the current strict fast-path parser the buggy cutoff is
//     masked by a parse failure fallback, so it is not behaviorally visible).
//
// The behaviorally observable regressions formerly asserted here (batch
// failure accounting / partial-failure responses / late-"writes" batch
// handling) are now covered by real handler-driving tests in
// http_write_handler_atomicity_behavioral_test.cpp.

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

    // Should perform data insertion. The per-shard insert dispatch (and the
    // Engine::insertBatch calls) was extracted into the dispatchShardInserts()
    // helper; accept either the helper call or a direct insertBatch.
    EXPECT_TRUE(batchBody.find("dispatchShardInserts") != std::string::npos ||
                batchBody.find("insertBatch") != std::string::npos)
        << "processMultiWritePoint should dispatch data inserts (dispatchShardInserts/insertBatch)";

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

// Bug 1 (batch write failures must decrement pointsWritten) is now covered
// behaviorally in http_write_handler_atomicity_behavioral_test.cpp, which
// drives handleWrite() with a batch containing a shard insert that fails and
// asserts the partial response accounting (points_written + failed_writes ==
// total points). The former source-inspection variant was removed.

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
    // The fast path lives in the tryFastDoubleWrite phase method of handleWrite.
    std::string handleBody = extractFunctionBody(sourceCode, "HttpWriteHandler::tryFastDoubleWrite(");
    ASSERT_FALSE(handleBody.empty()) << "Could not extract tryFastDoubleWrite function body";

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

// The partial-failure response shape (status "partial", points_written,
// failed_writes, errors) is now asserted behaviorally on a real handler
// response in http_write_handler_atomicity_behavioral_test.cpp; the former
// source-inspection variant was removed.

// Verify the createSuccessResponse signature uses int64_t
TEST_F(HttpWriteHandlerAtomicityTest, SuccessResponseUsesInt64) {
    EXPECT_NE(sourceCode.find("createSuccessResponse(int64_t pointsWritten)"), std::string::npos)
        << "createSuccessResponse must accept int64_t, not int, to match "
           "the int64_t pointsWritten variable in handleWrite";
}
