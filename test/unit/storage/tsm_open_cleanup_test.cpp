#include <gtest/gtest.h>
#include <string>
#include <fstream>
#include <sstream>

// Source-inspection tests for TSM::open() file handle cleanup.
//
// TSM::open() requires the Seastar runtime (open_file_dma, coroutines),
// so we cannot call it directly in a Google Test process. Instead, we
// verify the structural correctness of the source code: that a try/catch
// block surrounds the post-open operations, that the file handle is closed
// on failure, and that the original exception is re-thrown.
//
// Implementation note: GCC 14 does not support co_await in catch blocks,
// so the pattern uses std::exception_ptr to capture the exception in the
// catch block, then closes and rethrows outside the handler.
//
// Pattern borrowed from http_write_handler_atomicity_test.cpp.

class TSMOpenCleanupTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::vector<std::string> paths = {
            "../lib/storage/tsm.cpp",
            "../../lib/storage/tsm.cpp",
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

        // Fallback to absolute path
        std::ifstream file("/home/matt/Desktop/source/timestar/lib/storage/tsm.cpp");
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
        }
    }

    // Extract the TSM::open() function body from source
    std::string extractOpenFunction() const {
        size_t start = sourceCode.find("TSM::open()");
        if (start == std::string::npos) return "";

        // Find the opening brace of the function
        size_t braceStart = sourceCode.find('{', start);
        if (braceStart == std::string::npos) return "";

        // Find matching closing brace (simple brace counting)
        int braceCount = 1;
        size_t pos = braceStart + 1;
        while (pos < sourceCode.size() && braceCount > 0) {
            if (sourceCode[pos] == '{') braceCount++;
            else if (sourceCode[pos] == '}') braceCount--;
            pos++;
        }

        return sourceCode.substr(start, pos - start);
    }
};

// Verify that the source file was successfully loaded
TEST_F(TSMOpenCleanupTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty())
        << "Could not load tsm.cpp source file";
    ASSERT_NE(sourceCode.find("TSM::open()"), std::string::npos)
        << "Source file does not contain TSM::open()";
}

// Verify TSM::open() function exists and has the expected structure
TEST_F(TSMOpenCleanupTest, OpenFunctionHasExpectedStructure) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty())
        << "Could not extract TSM::open() function body";

    // Should contain the file open call
    EXPECT_NE(funcBody.find("open_file_dma"), std::string::npos)
        << "TSM::open() should call open_file_dma";

    // Should contain readSparseIndex
    EXPECT_NE(funcBody.find("readSparseIndex"), std::string::npos)
        << "TSM::open() should call readSparseIndex()";

    // Should contain loadTombstones
    EXPECT_NE(funcBody.find("loadTombstones"), std::string::npos)
        << "TSM::open() should call loadTombstones()";
}

// Core test: TSM::open() must have a try/catch block for cleanup
TEST_F(TSMOpenCleanupTest, HasTryCatchForCleanup) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // After open_file_dma succeeds, there must be a try block wrapping
    // the subsequent operations
    size_t openPos = funcBody.find("open_file_dma");
    ASSERT_NE(openPos, std::string::npos);

    // There should be a try block after the file open
    size_t tryPos = funcBody.find("try", openPos);
    ASSERT_NE(tryPos, std::string::npos)
        << "BUG: TSM::open() has no try block after open_file_dma. "
           "If readSparseIndex() or loadTombstones() throws, the file handle leaks.";

    // There should be a catch block
    size_t catchPos = funcBody.find("catch", tryPos);
    ASSERT_NE(catchPos, std::string::npos)
        << "BUG: TSM::open() has a try block but no catch block. "
           "The file handle will leak on exception.";
}

// Core test: On failure, the file handle must be closed.
// Because GCC 14 does not support co_await in catch blocks, the close
// happens after the catch block, guarded by an if(openError) check.
TEST_F(TSMOpenCleanupTest, FileHandleClosedOnFailure) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // The function should close the file handle on the error path.
    // With the exception_ptr pattern, this is tsmFile.close() after the catch block.
    EXPECT_NE(funcBody.find("tsmFile.close()"), std::string::npos)
        << "BUG: TSM::open() does not close the file handle on failure. "
           "This causes a file descriptor leak when readSparseIndex() or "
           "loadTombstones() throws.";

    // The close must appear AFTER the catch block (in the error-path cleanup)
    size_t catchPos = funcBody.find("catch");
    ASSERT_NE(catchPos, std::string::npos);

    size_t closePos = funcBody.find("tsmFile.close()", catchPos);
    EXPECT_NE(closePos, std::string::npos)
        << "tsmFile.close() should appear after the catch block for cleanup";
}

// Core test: The original exception must be re-thrown after cleanup.
// With the exception_ptr pattern this is std::rethrow_exception().
TEST_F(TSMOpenCleanupTest, OriginalExceptionRethrown) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // Must use exception_ptr to capture and rethrow
    bool hasExceptionCapture =
        funcBody.find("std::current_exception()") != std::string::npos;
    EXPECT_TRUE(hasExceptionCapture)
        << "BUG: TSM::open() does not capture the exception with "
           "std::current_exception(). The caller must know that open() failed.";

    bool hasRethrow =
        funcBody.find("std::rethrow_exception") != std::string::npos ||
        funcBody.find("rethrow_exception") != std::string::npos;
    EXPECT_TRUE(hasRethrow)
        << "BUG: TSM::open() does not rethrow the captured exception. "
           "The caller must know that open() failed.";
}

// Core test: The close() call must be guarded by its own try/catch
// so a close error doesn't mask the original exception
TEST_F(TSMOpenCleanupTest, CloseIsGuardedByNestedTryCatch) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the error-path cleanup section (after the main catch block).
    // The close should be wrapped in its own try/catch.
    size_t mainCatchPos = funcBody.find("catch");
    ASSERT_NE(mainCatchPos, std::string::npos);

    // Find the close call in the error path
    size_t closePos = funcBody.find("tsmFile.close()", mainCatchPos);
    ASSERT_NE(closePos, std::string::npos);

    // There should be a try block before the close (within the error-path section)
    // Look for a try block between the main catch and the close
    std::string errorPath = funcBody.substr(mainCatchPos);
    size_t tryInErrorPath = errorPath.find("try");
    size_t closeInErrorPath = errorPath.find("tsmFile.close()");
    ASSERT_NE(tryInErrorPath, std::string::npos)
        << "BUG: The close() call in the error path is not wrapped "
           "in a try block. If close() throws, the original exception "
           "will be lost.";

    EXPECT_LT(tryInErrorPath, closeInErrorPath)
        << "The try block should appear before tsmFile.close()";

    // There should also be a catch block for the close
    size_t catchInErrorPath = errorPath.find("catch", tryInErrorPath + 3);
    ASSERT_NE(catchInErrorPath, std::string::npos)
        << "BUG: The close() call has a try but no catch. "
           "Close errors must be silently ignored.";
}

// Verify the try block wraps readSparseIndex and loadTombstones
TEST_F(TSMOpenCleanupTest, TryBlockWrapsPostOpenOperations) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the try block after open_file_dma
    size_t openPos = funcBody.find("open_file_dma");
    ASSERT_NE(openPos, std::string::npos);

    size_t tryPos = funcBody.find("try", openPos);
    ASSERT_NE(tryPos, std::string::npos);

    // Extract the try block body
    size_t tryBrace = funcBody.find('{', tryPos);
    ASSERT_NE(tryBrace, std::string::npos);

    int braceCount = 1;
    size_t pos = tryBrace + 1;
    while (pos < funcBody.size() && braceCount > 0) {
        if (funcBody[pos] == '{') braceCount++;
        else if (funcBody[pos] == '}') braceCount--;
        pos++;
    }
    std::string tryBody = funcBody.substr(tryPos, pos - tryPos);

    // The try block should contain both readSparseIndex and loadTombstones
    EXPECT_NE(tryBody.find("readSparseIndex"), std::string::npos)
        << "readSparseIndex() should be inside the try block for cleanup";
    EXPECT_NE(tryBody.find("loadTombstones"), std::string::npos)
        << "loadTombstones() should be inside the try block for cleanup";
}

// Verify the catch block uses catch(...) to handle all exception types
TEST_F(TSMOpenCleanupTest, CatchBlockHandlesAllExceptions) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    size_t tryPos = funcBody.find("try");
    ASSERT_NE(tryPos, std::string::npos);

    // Find the catch clause after the try block
    // It should be catch(...) to catch all exceptions
    size_t catchPos = funcBody.find("catch", tryPos);
    ASSERT_NE(catchPos, std::string::npos);

    // Extract text between "catch" and the opening brace
    size_t catchBrace = funcBody.find('{', catchPos);
    ASSERT_NE(catchBrace, std::string::npos);

    std::string catchClause = funcBody.substr(catchPos, catchBrace - catchPos);

    EXPECT_NE(catchClause.find("..."), std::string::npos)
        << "The outer catch should use catch(...) to handle all exception types, "
           "not just specific ones. Seastar exceptions and std::exception "
           "subtypes all need cleanup.";
}

// Verify the exception_ptr variable is declared before the try block
TEST_F(TSMOpenCleanupTest, ExceptionPtrDeclaredBeforeTry) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    size_t exPtrPos = funcBody.find("exception_ptr");
    ASSERT_NE(exPtrPos, std::string::npos)
        << "TSM::open() should declare a std::exception_ptr variable";

    size_t tryPos = funcBody.find("try {");
    ASSERT_NE(tryPos, std::string::npos);

    EXPECT_LT(exPtrPos, tryPos)
        << "The exception_ptr variable must be declared before the try block "
           "so it can be set in the catch block and checked afterwards.";
}

// Regression test: The normal open path should not be broken
TEST_F(TSMOpenCleanupTest, NormalPathStillCallsAllOperations) {
    std::string funcBody = extractOpenFunction();
    ASSERT_FALSE(funcBody.empty());

    // All these operations should still be present in the function
    EXPECT_NE(funcBody.find("open_file_dma"), std::string::npos)
        << "open_file_dma should still be called";
    EXPECT_NE(funcBody.find("tsmFile.size()"), std::string::npos)
        << "tsmFile.size() should still be called";
    EXPECT_NE(funcBody.find("readSparseIndex"), std::string::npos)
        << "readSparseIndex() should still be called";
    EXPECT_NE(funcBody.find("loadTombstones"), std::string::npos)
        << "loadTombstones() should still be called";
}
