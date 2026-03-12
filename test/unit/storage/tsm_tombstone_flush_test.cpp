#include <gtest/gtest.h>
#include <string>
#include <fstream>
#include <sstream>

// Source-inspection tests for TSMTombstone::flush() file handle cleanup.
//
// TSMTombstone::flush() requires the Seastar runtime (open_file_dma,
// coroutines), so we cannot call it directly in a Google Test process.
// Instead, we verify the structural correctness of the source code:
// that the output stream is always closed even when writes throw, and
// that the original exception is properly captured and re-thrown.
//
// Implementation note: GCC 14 does not support co_await in catch blocks,
// so the pattern uses std::exception_ptr to capture the exception in the
// catch block, then closes and rethrows outside the handler.
//
// Pattern borrowed from tsm_open_cleanup_test.cpp.

class TombstoneFlushCleanupTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef TSM_TOMBSTONE_SOURCE_PATH
        std::ifstream file(TSM_TOMBSTONE_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif

        std::vector<std::string> paths = {
            "../lib/storage/tsm_tombstone.cpp",
            "../../lib/storage/tsm_tombstone.cpp",
        };

        for (const auto& path : paths) {
            std::ifstream f(path);
            if (f.is_open()) {
                std::stringstream ss;
                ss << f.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }

    // Extract the TSMTombstone::flush() function body from source
    std::string extractFlushFunction() const {
        size_t start = sourceCode.find("TSMTombstone::flush()");
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
TEST_F(TombstoneFlushCleanupTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty())
        << "Could not load tsm_tombstone.cpp source file";
    ASSERT_NE(sourceCode.find("TSMTombstone::flush()"), std::string::npos)
        << "Source file does not contain TSMTombstone::flush()";
}

// Verify flush() function exists and has the expected structure
TEST_F(TombstoneFlushCleanupTest, FlushFunctionHasExpectedStructure) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty())
        << "Could not extract TSMTombstone::flush() function body";

    // Should contain the file open call
    EXPECT_NE(funcBody.find("open_file_dma"), std::string::npos)
        << "flush() should call open_file_dma to open the tombstone file";

    // Should create an output stream
    EXPECT_NE(funcBody.find("output_stream"), std::string::npos)
        << "flush() should create an output_stream for writing";

    // Should write header, entries, and checksum
    EXPECT_NE(funcBody.find("output_stream.write"), std::string::npos)
        << "flush() should write data via the output stream";

    // Should flush the stream
    EXPECT_NE(funcBody.find("output_stream.flush()"), std::string::npos)
        << "flush() should flush the output stream before closing";
}

// Core test: flush() must use exception_ptr pattern for cleanup (not throw in catch)
TEST_F(TombstoneFlushCleanupTest, UsesExceptionPtrPatternNotThrowInCatch) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // Must use exception_ptr to capture exceptions
    EXPECT_NE(funcBody.find("exception_ptr"), std::string::npos)
        << "BUG: flush() should use std::exception_ptr to capture exceptions. "
           "Without this, the output stream cannot be closed in the error path "
           "because GCC 14 does not support co_await in catch blocks.";

    EXPECT_NE(funcBody.find("std::current_exception()"), std::string::npos)
        << "BUG: flush() should capture the exception with "
           "std::current_exception() so cleanup can happen outside the catch.";
}

// Core test: The output stream must be closed unconditionally (not just on success)
TEST_F(TombstoneFlushCleanupTest, OutputStreamClosedUnconditionally) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // The output_stream.close() should appear after the catch block,
    // meaning it executes regardless of whether an exception was thrown.
    size_t catchPos = funcBody.find("catch");
    ASSERT_NE(catchPos, std::string::npos)
        << "flush() should have a catch block";

    size_t closePos = funcBody.find("output_stream.close()", catchPos);
    EXPECT_NE(closePos, std::string::npos)
        << "BUG: output_stream.close() must appear after the catch block so "
           "the file descriptor is closed on both success and error paths. "
           "Without this, the underlying file descriptor leaks on write failures.";
}

// Core test: The close call must NOT be only inside the try block
TEST_F(TombstoneFlushCleanupTest, CloseNotOnlyInsideTryBlock) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find all occurrences of output_stream.close()
    // At least one must be outside (after) the main catch block
    size_t mainCatchPos = funcBody.find("catch");
    ASSERT_NE(mainCatchPos, std::string::npos);

    // Find close after the main catch - this is the unconditional cleanup
    size_t closeAfterCatch = funcBody.find("output_stream.close()", mainCatchPos);
    EXPECT_NE(closeAfterCatch, std::string::npos)
        << "BUG: output_stream.close() only appears inside the try block. "
           "If any write throws, the stream is never closed and the file "
           "descriptor leaks. Move close() outside the try/catch for "
           "unconditional cleanup.";
}

// Core test: The close call should be wrapped in its own try/catch
// so close errors don't mask the original exception
TEST_F(TombstoneFlushCleanupTest, CloseIsGuardedByNestedTryCatch) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the unconditional close (after the main catch)
    size_t mainCatchPos = funcBody.find("catch");
    ASSERT_NE(mainCatchPos, std::string::npos);

    size_t closePos = funcBody.find("output_stream.close()", mainCatchPos);
    ASSERT_NE(closePos, std::string::npos);

    // The close should be inside its own try block
    // Look for a try block between the main catch and the close
    std::string afterCatch = funcBody.substr(mainCatchPos);
    size_t tryInCleanup = afterCatch.find("try");
    size_t closeInCleanup = afterCatch.find("output_stream.close()");
    ASSERT_NE(tryInCleanup, std::string::npos)
        << "BUG: The close() call in the cleanup path is not wrapped in a "
           "try block. If close() throws, the original exception will be lost.";

    EXPECT_LT(tryInCleanup, closeInCleanup)
        << "The try block should appear before output_stream.close()";

    // There should also be a catch block for the close
    size_t catchInCleanup = afterCatch.find("catch", tryInCleanup + 3);
    ASSERT_NE(catchInCleanup, std::string::npos)
        << "BUG: The close() call has a try but no catch. "
           "Close errors must be silently ignored to avoid masking "
           "the original exception.";
}

// Core test: The original exception must be re-thrown after cleanup
TEST_F(TombstoneFlushCleanupTest, OriginalExceptionRethrown) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    bool hasRethrow =
        funcBody.find("std::rethrow_exception") != std::string::npos ||
        funcBody.find("rethrow_exception") != std::string::npos;
    EXPECT_TRUE(hasRethrow)
        << "BUG: flush() does not rethrow the captured exception after "
           "cleanup. The caller must know that flush() failed.";
}

// Verify the exception_ptr variable is declared before the try block
TEST_F(TombstoneFlushCleanupTest, ExceptionPtrDeclaredBeforeTry) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    size_t exPtrPos = funcBody.find("exception_ptr");
    ASSERT_NE(exPtrPos, std::string::npos)
        << "flush() should declare a std::exception_ptr variable";

    // Find the try block that wraps the write operations (after open_file_dma)
    size_t openPos = funcBody.find("open_file_dma");
    ASSERT_NE(openPos, std::string::npos);

    size_t tryPos = funcBody.find("try", openPos);
    ASSERT_NE(tryPos, std::string::npos);

    EXPECT_LT(exPtrPos, tryPos)
        << "The exception_ptr variable must be declared before the try block "
           "so it can be set in the catch block and checked afterwards.";
}

// Verify the catch block uses catch(...) to handle all exception types
TEST_F(TombstoneFlushCleanupTest, CatchBlockHandlesAllExceptions) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // Find the main catch block (after open_file_dma and try)
    size_t openPos = funcBody.find("open_file_dma");
    ASSERT_NE(openPos, std::string::npos);

    size_t tryPos = funcBody.find("try", openPos);
    ASSERT_NE(tryPos, std::string::npos);

    size_t catchPos = funcBody.find("catch", tryPos);
    ASSERT_NE(catchPos, std::string::npos);

    // Extract text between "catch" and the opening brace
    size_t catchBrace = funcBody.find('{', catchPos);
    ASSERT_NE(catchBrace, std::string::npos);

    std::string catchClause = funcBody.substr(catchPos, catchBrace - catchPos);

    EXPECT_NE(catchClause.find("..."), std::string::npos)
        << "The catch block should use catch(...) to handle all exception "
           "types, not just std::exception. Seastar exceptions and other "
           "types all need cleanup.";
}

// Verify that flush() does NOT have a catch block that throws without closing
TEST_F(TombstoneFlushCleanupTest, NoCatchBlockThrowsWithoutClosing) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // The old buggy pattern was: catch block with throw_with_nested or throw
    // but no close() before it. Verify this pattern is gone.
    EXPECT_EQ(funcBody.find("throw_with_nested"), std::string::npos)
        << "BUG: flush() still uses throw_with_nested in a catch block. "
           "This pattern throws without closing the output stream, causing "
           "a file descriptor leak. Use the exception_ptr pattern instead.";
}

// Regression test: The normal flush path should still write all components
TEST_F(TombstoneFlushCleanupTest, NormalPathStillWritesAllComponents) {
    std::string funcBody = extractFlushFunction();
    ASSERT_FALSE(funcBody.empty());

    // All these operations should still be present
    EXPECT_NE(funcBody.find("open_file_dma"), std::string::npos)
        << "open_file_dma should still be called";
    EXPECT_NE(funcBody.find("sortAndMergeEntries"), std::string::npos)
        << "sortAndMergeEntries() should still be called";
    EXPECT_NE(funcBody.find("TOMBSTONE_MAGIC"), std::string::npos)
        << "Header magic number should still be written";
    EXPECT_NE(funcBody.find("calculateFileChecksum"), std::string::npos)
        << "File checksum should still be calculated and written";
    EXPECT_NE(funcBody.find("isDirty = false"), std::string::npos)
        << "isDirty should still be set to false on success";
}
