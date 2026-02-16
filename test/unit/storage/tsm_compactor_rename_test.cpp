#include <gtest/gtest.h>
#include <fstream>
#include <string>

// =============================================================================
// Source code inspection tests for TSMCompactor::compact() rename operation
//
// These tests verify that the compact() method uses the async
// seastar::rename_file() instead of the blocking std::filesystem::rename().
// Blocking calls on the Seastar reactor thread stall all tasks on that shard.
// =============================================================================

class TSMCompactorRenameTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(TSM_COMPACTOR_SOURCE_PATH);
        ASSERT_TRUE(file.is_open())
            << "Could not open tsm_compactor.cpp at: " << TSM_COMPACTOR_SOURCE_PATH;
        sourceCode.assign(
            std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Extract the compact() method body from the source code
    std::string getCompactMethodBody() {
        // Find the compact() method signature
        auto pos = sourceCode.find("TSMCompactor::compact(");
        EXPECT_NE(pos, std::string::npos) << "Could not find TSMCompactor::compact()";
        if (pos == std::string::npos) return "";

        // Find the opening brace of the method
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos) return "";

        // Track brace depth to find the end of the method
        int depth = 1;
        size_t i = bracePos + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }

        return sourceCode.substr(pos, i - pos);
    }
};

// Test 1: Verify there are NO blocking fs::rename calls in compact()
TEST_F(TSMCompactorRenameTest, NoBlockingFsRename) {
    std::string compactBody = getCompactMethodBody();
    ASSERT_FALSE(compactBody.empty()) << "Could not extract compact() method body";

    // Check that fs::rename is NOT used anywhere in compact()
    bool hasBlockingRename = (compactBody.find("fs::rename") != std::string::npos);

    EXPECT_FALSE(hasBlockingRename)
        << "Found blocking 'fs::rename' call in TSMCompactor::compact(). "
        << "This blocks the Seastar reactor thread during the kernel rename syscall. "
        << "Use 'co_await seastar::rename_file()' instead.\n"
        << "compact() body (first 500 chars from rename area):\n"
        << compactBody.substr(
               compactBody.find("rename") != std::string::npos
                   ? compactBody.find("rename") - 50 : 0,
               200);
}

// Test 2: Verify the code uses seastar::rename_file instead
TEST_F(TSMCompactorRenameTest, UsesAsyncRename) {
    std::string compactBody = getCompactMethodBody();
    ASSERT_FALSE(compactBody.empty()) << "Could not extract compact() method body";

    // Check that seastar::rename_file is used
    bool hasAsyncRename = (compactBody.find("seastar::rename_file") != std::string::npos);

    EXPECT_TRUE(hasAsyncRename)
        << "Expected 'seastar::rename_file' in TSMCompactor::compact() for "
        << "non-blocking file rename. The Seastar reactor must not be blocked "
        << "by synchronous filesystem operations.";
}

// Test 3: Verify the async rename is properly co_awaited (not fire-and-forget)
TEST_F(TSMCompactorRenameTest, RenameIsCoAwaited) {
    std::string compactBody = getCompactMethodBody();
    ASSERT_FALSE(compactBody.empty()) << "Could not extract compact() method body";

    // Check that co_await is used with seastar::rename_file
    bool hasCoAwaitRename = (compactBody.find("co_await seastar::rename_file") != std::string::npos);

    EXPECT_TRUE(hasCoAwaitRename)
        << "Expected 'co_await seastar::rename_file' in TSMCompactor::compact(). "
        << "The rename future must be co_awaited to ensure the operation completes "
        << "before the method returns the output path. A fire-and-forget rename "
        << "could cause the caller to use a path that doesn't exist yet.";
}
