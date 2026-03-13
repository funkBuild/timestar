#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// Source-inspection tests for WAL::init error handling.
//
// WAL::init() requires the full Seastar runtime (open_file_dma, coroutines)
// which cannot be instantiated in plain GTest unit tests.  Instead, we read
// the wal.cpp source and verify that the error path throws rather than
// silently returning.  This is the same approach used by
// HttpWriteHandlerAtomicityTest for verifying crash-safety ordering.

class WALInitFailureTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef WAL_SOURCE_PATH
        std::ifstream file(WAL_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif

        std::vector<std::string> paths = {
            "../lib/storage/wal.cpp",
            "../../lib/storage/wal.cpp",
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

    // Extract the WAL::init function body from source
    std::string extractInitFunction() const {
        size_t start = sourceCode.find("WAL::init(");
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
TEST_F(WALInitFailureTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load wal.cpp source file";
    ASSERT_NE(sourceCode.find("WAL::init("), std::string::npos) << "Source file does not contain WAL::init";
}

// Verify WAL::init function exists and has the expected structure
TEST_F(WALInitFailureTest, InitFunctionExists) {
    std::string funcBody = extractInitFunction();
    ASSERT_FALSE(funcBody.empty()) << "Could not extract WAL::init function body";

    // Should contain the file open call
    EXPECT_NE(funcBody.find("open_file_dma"), std::string::npos) << "WAL::init should call open_file_dma";

    // Should check the walFile result
    EXPECT_NE(funcBody.find("!walFile"), std::string::npos) << "WAL::init should check if walFile is falsy after open";
}

// Core test: WAL::init must throw on file open failure, not silently return.
// The old buggy code had:
//   if (!walFile) { co_return; }
// which let callers believe init succeeded.
TEST_F(WALInitFailureTest, ThrowsOnFileOpenFailure) {
    std::string funcBody = extractInitFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the walFile check
    size_t checkPos = funcBody.find("!walFile");
    ASSERT_NE(checkPos, std::string::npos) << "Could not find !walFile check in WAL::init";

    // Look at the code between the !walFile check and the next meaningful
    // statement (the "Get current file size" section or end of function).
    // We search a reasonable window after the check.
    std::string afterCheck = funcBody.substr(checkPos, 300);

    // There MUST be a throw in this section
    EXPECT_NE(afterCheck.find("throw"), std::string::npos)
        << "BUG: WAL::init does not throw when walFile is falsy after open_file_dma. "
           "A silent co_return leaves the caller unaware that initialization failed, "
           "causing all subsequent WAL operations to behave inconsistently.";

    // The throw should mention the filename for debuggability
    size_t throwPos = afterCheck.find("throw");
    if (throwPos != std::string::npos) {
        std::string throwSection = afterCheck.substr(throwPos, 200);
        EXPECT_NE(throwSection.find("filename"), std::string::npos)
            << "The exception message should include the filename for debuggability";
    }
}

// Verify there is NO silent co_return in the error path.
// This is the inverse check: the old buggy pattern was "co_return;" right
// after the error log.
TEST_F(WALInitFailureTest, NoSilentCoReturnOnFailure) {
    std::string funcBody = extractInitFunction();
    ASSERT_FALSE(funcBody.empty());

    size_t checkPos = funcBody.find("!walFile");
    ASSERT_NE(checkPos, std::string::npos);

    // Look at the section between the !walFile check and the next major
    // code block (walFile.size() call).
    size_t nextSection = funcBody.find("walFile.size()", checkPos);
    std::string errorBlock;
    if (nextSection != std::string::npos) {
        errorBlock = funcBody.substr(checkPos, nextSection - checkPos);
    } else {
        errorBlock = funcBody.substr(checkPos, 300);
    }

    // There must NOT be a bare "co_return;" in the error path
    // (co_return with a value would be fine for other patterns, but
    //  "co_return;" means silent void return)
    size_t coReturnPos = errorBlock.find("co_return;");
    EXPECT_EQ(coReturnPos, std::string::npos) << "BUG: WAL::init has a silent 'co_return;' in the !walFile error path. "
                                                 "This means callers have no way to know init failed. "
                                                 "It should throw std::runtime_error instead.";
}

// Baseline: verify that the recovery path (file not found for recovery)
// already throws, establishing the expected pattern for error handling.
TEST_F(WALInitFailureTest, RecoveryPathThrowsBaseline) {
    std::string funcBody = extractInitFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the recovery check ("does not exist for recovery")
    size_t recoveryCheck = funcBody.find("does not exist for recovery");
    ASSERT_NE(recoveryCheck, std::string::npos) << "Could not find recovery path error message";

    // There should be a throw near this error message
    std::string recoverySection = funcBody.substr(recoveryCheck > 100 ? recoveryCheck - 100 : 0, 300);

    EXPECT_NE(recoverySection.find("throw"), std::string::npos)
        << "The recovery path should throw on missing WAL file (baseline check)";
}

// Verify both error paths use consistent error handling (both throw).
TEST_F(WALInitFailureTest, ConsistentErrorHandling) {
    std::string funcBody = extractInitFunction();
    ASSERT_FALSE(funcBody.empty());

    // Count throw statements in the init function.
    // There should be at least 2: one for recovery-file-not-found,
    // and one for open_file_dma failure.
    size_t throwCount = 0;
    size_t searchPos = 0;
    while ((searchPos = funcBody.find("throw std::runtime_error", searchPos)) != std::string::npos) {
        throwCount++;
        searchPos += 5;
    }

    EXPECT_GE(throwCount, 2u) << "WAL::init should have at least 2 throw statements: "
                                 "one for recovery-file-not-found and one for open_file_dma failure. "
                                 "Found "
                              << throwCount
                              << " throw(s). Both error paths should use "
                                 "consistent error handling (throw, not silent return).";
}
