#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <sstream>

// =============================================================================
// Source code inspection tests for compaction error handling
//
// These tests verify that both TSMCompactor::runCompactionLoop() and
// TSMFileManager::checkAndTriggerCompaction() wrap their executeCompaction()
// calls in try/catch blocks. Without error handling, a single compaction
// failure (e.g., disk full, corrupt TSM file, I/O error) permanently stops
// all future compaction, causing unbounded file accumulation.
// =============================================================================

class CompactionErrorHandlingTest : public ::testing::Test {
protected:
    std::string compactorSource;
    std::string fileManagerSource;

    void SetUp() override {
        // Load tsm_compactor.cpp
        std::ifstream compactorFile(TSM_COMPACTOR_SOURCE_PATH);
        ASSERT_TRUE(compactorFile.is_open())
            << "Could not open tsm_compactor.cpp at: " << TSM_COMPACTOR_SOURCE_PATH;
        compactorSource.assign(
            std::istreambuf_iterator<char>(compactorFile),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(compactorSource.empty());

        // Load tsm_file_manager.cpp
        std::ifstream fileManagerFile(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
        ASSERT_TRUE(fileManagerFile.is_open())
            << "Could not open tsm_file_manager.cpp at: " << TSM_FILE_MANAGER_CPP_SOURCE_PATH;
        fileManagerSource.assign(
            std::istreambuf_iterator<char>(fileManagerFile),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(fileManagerSource.empty());
    }

    // Extract a method body from source code given the method signature pattern
    std::string extractMethodBody(const std::string& source, const std::string& signature) {
        auto pos = source.find(signature);
        if (pos == std::string::npos) return "";

        // Find the opening brace of the method
        auto bracePos = source.find('{', pos);
        if (bracePos == std::string::npos) return "";

        // Track brace depth to find the end of the method
        int depth = 1;
        size_t i = bracePos + 1;
        while (i < source.size() && depth > 0) {
            if (source[i] == '{') depth++;
            else if (source[i] == '}') depth--;
            i++;
        }

        return source.substr(pos, i - pos);
    }

    // Check if a try/catch block wraps a specific call within a method body
    // This verifies the call appears inside a try block (between try{ and catch)
    bool hasTrappedCall(const std::string& methodBody, const std::string& call) {
        // Find the call
        auto callPos = methodBody.find(call);
        if (callPos == std::string::npos) return false;

        // Look backwards from the call for a 'try' keyword
        // The try must appear before the call and its matching '{' must
        // still be open (i.e., the call is inside the try block)
        std::string beforeCall = methodBody.substr(0, callPos);

        // Find the last 'try' before the call
        auto tryPos = beforeCall.rfind("try");
        if (tryPos == std::string::npos) return false;

        // Find the opening brace after 'try'
        auto tryBrace = beforeCall.find('{', tryPos);
        if (tryBrace == std::string::npos) return false;

        // Verify the try block's opening brace is still open at the call site
        // (count braces between try's '{' and the call)
        int depth = 1;
        for (size_t j = tryBrace + 1; j < callPos && depth > 0; j++) {
            if (beforeCall[j] == '{') depth++;
            else if (beforeCall[j] == '}') depth--;
        }

        // depth > 0 means the try block is still open at the call site
        return depth > 0;
    }

    // Check that a catch block does NOT contain 'throw' or 'rethrow'
    bool catchBlockDoesNotRethrow(const std::string& methodBody, const std::string& call) {
        // Find the call
        auto callPos = methodBody.find(call);
        if (callPos == std::string::npos) return false;

        // Find the catch block after the call
        auto catchPos = methodBody.find("catch", callPos);
        if (catchPos == std::string::npos) return false;

        // Find the opening brace of the catch block
        auto catchBrace = methodBody.find('{', catchPos);
        if (catchBrace == std::string::npos) return false;

        // Find the closing brace of the catch block
        int depth = 1;
        size_t i = catchBrace + 1;
        while (i < methodBody.size() && depth > 0) {
            if (methodBody[i] == '{') depth++;
            else if (methodBody[i] == '}') depth--;
            i++;
        }

        std::string catchBody = methodBody.substr(catchBrace, i - catchBrace);

        // Check that the catch block does NOT contain 'throw;' or 'throw '
        // (which would rethrow the exception)
        bool hasRethrow = (catchBody.find("throw;") != std::string::npos) ||
                          (catchBody.find("throw ") != std::string::npos);

        return !hasRethrow;
    }
};

// Test 1: runCompactionLoop has try/catch around executeCompaction
TEST_F(CompactionErrorHandlingTest, RunCompactionLoopHasTryCatchAroundExecuteCompaction) {
    std::string loopBody = extractMethodBody(compactorSource, "TSMCompactor::runCompactionLoop()");
    ASSERT_FALSE(loopBody.empty())
        << "Could not find TSMCompactor::runCompactionLoop() in source";

    // Verify executeCompaction is called
    ASSERT_NE(loopBody.find("executeCompaction"), std::string::npos)
        << "runCompactionLoop() does not call executeCompaction()";

    // Verify the executeCompaction call is inside a try block
    EXPECT_TRUE(hasTrappedCall(loopBody, "executeCompaction"))
        << "executeCompaction() in runCompactionLoop() is NOT wrapped in a try/catch block. "
        << "If executeCompaction() throws (e.g., disk full, corrupt file, I/O error), "
        << "the exception will propagate up and permanently terminate the compaction loop, "
        << "causing unbounded file accumulation.";
}

// Test 2: runCompactionLoop catch block logs an error
TEST_F(CompactionErrorHandlingTest, RunCompactionLoopCatchBlockLogsError) {
    std::string loopBody = extractMethodBody(compactorSource, "TSMCompactor::runCompactionLoop()");
    ASSERT_FALSE(loopBody.empty());

    // Find the catch block after executeCompaction
    auto execPos = loopBody.find("executeCompaction");
    ASSERT_NE(execPos, std::string::npos);

    auto catchPos = loopBody.find("catch", execPos);
    ASSERT_NE(catchPos, std::string::npos)
        << "No catch block found after executeCompaction in runCompactionLoop()";

    // Find the catch body
    auto catchBrace = loopBody.find('{', catchPos);
    ASSERT_NE(catchBrace, std::string::npos);

    int depth = 1;
    size_t i = catchBrace + 1;
    while (i < loopBody.size() && depth > 0) {
        if (loopBody[i] == '{') depth++;
        else if (loopBody[i] == '}') depth--;
        i++;
    }

    std::string catchBody = loopBody.substr(catchBrace, i - catchBrace);

    // Verify the catch block logs an error
    bool logsError = (catchBody.find("compactor_log.error") != std::string::npos) ||
                     (catchBody.find("log.error") != std::string::npos);

    EXPECT_TRUE(logsError)
        << "The catch block in runCompactionLoop() does not log an error. "
        << "Compaction failures should be logged so operators can diagnose issues. "
        << "Catch body:\n" << catchBody;
}

// Test 3: checkAndTriggerCompaction has try/catch around executeCompaction
TEST_F(CompactionErrorHandlingTest, CheckAndTriggerCompactionHasTryCatchAroundExecuteCompaction) {
    std::string methodBody = extractMethodBody(fileManagerSource,
        "TSMFileManager::checkAndTriggerCompaction()");
    ASSERT_FALSE(methodBody.empty())
        << "Could not find TSMFileManager::checkAndTriggerCompaction() in source";

    // Verify executeCompaction is called
    ASSERT_NE(methodBody.find("executeCompaction"), std::string::npos)
        << "checkAndTriggerCompaction() does not call executeCompaction()";

    // Verify the executeCompaction call is inside a try block
    EXPECT_TRUE(hasTrappedCall(methodBody, "executeCompaction"))
        << "executeCompaction() in checkAndTriggerCompaction() is NOT wrapped in a try/catch. "
        << "A compaction failure during a write would cause the write endpoint to return an error "
        << "to the client, even though the write itself succeeded.";
}

// Test 4: The catch blocks do NOT rethrow (loop continues after error)
TEST_F(CompactionErrorHandlingTest, RunCompactionLoopContinuesAfterError) {
    std::string loopBody = extractMethodBody(compactorSource, "TSMCompactor::runCompactionLoop()");
    ASSERT_FALSE(loopBody.empty());

    EXPECT_TRUE(catchBlockDoesNotRethrow(loopBody, "executeCompaction"))
        << "The catch block in runCompactionLoop() rethrows the exception. "
        << "This defeats the purpose of error handling -- the loop must continue "
        << "so compaction can be retried on the next cycle.";
}

// Test 5: checkAndTriggerCompaction catch block does NOT rethrow
TEST_F(CompactionErrorHandlingTest, CheckAndTriggerCompactionContinuesAfterError) {
    std::string methodBody = extractMethodBody(fileManagerSource,
        "TSMFileManager::checkAndTriggerCompaction()");
    ASSERT_FALSE(methodBody.empty());

    EXPECT_TRUE(catchBlockDoesNotRethrow(methodBody, "executeCompaction"))
        << "The catch block in checkAndTriggerCompaction() rethrows the exception. "
        << "This would propagate the error to the write path caller, failing a write "
        << "that already succeeded just because background compaction failed.";
}
