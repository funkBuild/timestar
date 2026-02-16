#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <map>
#include <variant>

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

        // If none of the relative paths worked, try absolute
        std::ifstream file("/home/matt/Desktop/source/tsdb/lib/http/http_write_handler.cpp");
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
        }
    }

    // Find the position of a pattern in the source code, starting from a given offset
    size_t findPattern(const std::string& pattern, size_t startPos = 0) const {
        return sourceCode.find(pattern, startPos);
    }

    // Extract the processWritePoint function body from source
    std::string extractProcessWritePoint() const {
        size_t start = sourceCode.find("HttpWriteHandler::processWritePoint(");
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
TEST_F(HttpWriteHandlerAtomicityTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty())
        << "Could not load http_write_handler.cpp source file";
    ASSERT_NE(sourceCode.find("processWritePoint"), std::string::npos)
        << "Source file does not contain processWritePoint";
}

// Verify processWritePoint function exists and has the expected structure
TEST_F(HttpWriteHandlerAtomicityTest, ProcessWritePointExists) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty())
        << "Could not extract processWritePoint function body";

    // Should contain all 4 type branches
    EXPECT_NE(funcBody.find("holds_alternative<double>"), std::string::npos)
        << "Missing double branch";
    EXPECT_NE(funcBody.find("holds_alternative<bool>"), std::string::npos)
        << "Missing bool branch";
    EXPECT_NE(funcBody.find("holds_alternative<std::string>"), std::string::npos)
        << "Missing string branch";
    EXPECT_NE(funcBody.find("holds_alternative<int64_t>"), std::string::npos)
        << "Missing int64_t branch";
}

// Core test: In the double branch, engine.insert() must appear BEFORE engine.indexMetadata()
TEST_F(HttpWriteHandlerAtomicityTest, DoubleBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    // Find the double branch
    size_t doubleBranch = funcBody.find("holds_alternative<double>");
    ASSERT_NE(doubleBranch, std::string::npos);

    // Find the next branch boundary (bool branch) to limit search scope
    size_t nextBranch = funcBody.find("holds_alternative<bool>", doubleBranch);
    std::string doubleSection = funcBody.substr(doubleBranch,
        nextBranch != std::string::npos ? nextBranch - doubleBranch : std::string::npos);

    // In this section, engine.insert should appear BEFORE engine.indexMetadata
    size_t insertPos = doubleSection.find("engine.insert(");
    size_t metadataPos = doubleSection.find("engine.indexMetadata(");

    ASSERT_NE(insertPos, std::string::npos)
        << "Could not find engine.insert() in double branch";
    ASSERT_NE(metadataPos, std::string::npos)
        << "Could not find engine.indexMetadata() in double branch";

    EXPECT_LT(insertPos, metadataPos)
        << "BUG: In the double branch, engine.indexMetadata() appears BEFORE engine.insert(). "
           "Data must be inserted before metadata for crash safety. "
           "If data insert fails, phantom metadata entries will be created.";
}

// Core test: In the bool branch, engine.insert() must appear BEFORE engine.indexMetadata()
TEST_F(HttpWriteHandlerAtomicityTest, BoolBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    size_t boolBranch = funcBody.find("holds_alternative<bool>");
    ASSERT_NE(boolBranch, std::string::npos);

    size_t nextBranch = funcBody.find("holds_alternative<std::string>", boolBranch);
    std::string boolSection = funcBody.substr(boolBranch,
        nextBranch != std::string::npos ? nextBranch - boolBranch : std::string::npos);

    size_t insertPos = boolSection.find("engine.insert(");
    size_t metadataPos = boolSection.find("engine.indexMetadata(");

    ASSERT_NE(insertPos, std::string::npos)
        << "Could not find engine.insert() in bool branch";
    ASSERT_NE(metadataPos, std::string::npos)
        << "Could not find engine.indexMetadata() in bool branch";

    EXPECT_LT(insertPos, metadataPos)
        << "BUG: In the bool branch, engine.indexMetadata() appears BEFORE engine.insert(). "
           "Data must be inserted before metadata for crash safety.";
}

// Core test: In the string branch, engine.insert() must appear BEFORE engine.indexMetadata()
TEST_F(HttpWriteHandlerAtomicityTest, StringBranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    size_t stringBranch = funcBody.find("holds_alternative<std::string>");
    ASSERT_NE(stringBranch, std::string::npos);

    size_t nextBranch = funcBody.find("holds_alternative<int64_t>", stringBranch);
    std::string stringSection = funcBody.substr(stringBranch,
        nextBranch != std::string::npos ? nextBranch - stringBranch : std::string::npos);

    size_t insertPos = stringSection.find("engine.insert(");
    size_t metadataPos = stringSection.find("engine.indexMetadata(");

    ASSERT_NE(insertPos, std::string::npos)
        << "Could not find engine.insert() in string branch";
    ASSERT_NE(metadataPos, std::string::npos)
        << "Could not find engine.indexMetadata() in string branch";

    EXPECT_LT(insertPos, metadataPos)
        << "BUG: In the string branch, engine.indexMetadata() appears BEFORE engine.insert(). "
           "Data must be inserted before metadata for crash safety.";
}

// Core test: In the int64_t branch, engine.insert() must appear BEFORE engine.indexMetadata()
TEST_F(HttpWriteHandlerAtomicityTest, Int64BranchDataBeforeMetadata) {
    std::string funcBody = extractProcessWritePoint();
    ASSERT_FALSE(funcBody.empty());

    size_t intBranch = funcBody.find("holds_alternative<int64_t>");
    ASSERT_NE(intBranch, std::string::npos);

    // int64_t is the last branch, so search to end of function
    std::string intSection = funcBody.substr(intBranch);

    size_t insertPos = intSection.find("engine.insert(");
    size_t metadataPos = intSection.find("engine.indexMetadata(");

    ASSERT_NE(insertPos, std::string::npos)
        << "Could not find engine.insert() in int64_t branch";
    ASSERT_NE(metadataPos, std::string::npos)
        << "Could not find engine.indexMetadata() in int64_t branch";

    EXPECT_LT(insertPos, metadataPos)
        << "BUG: In the int64_t branch, engine.indexMetadata() appears BEFORE engine.insert(). "
           "Data must be inserted before metadata for crash safety.";
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
        if (sourceCode[pos] == '{') braceCount++;
        else if (sourceCode[pos] == '}') braceCount--;
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
        funcBody.find("data first") != std::string::npos ||
        funcBody.find("Insert data first") != std::string::npos ||
        funcBody.find("crash safety") != std::string::npos ||
        funcBody.find("crash-safe") != std::string::npos;

    EXPECT_TRUE(hasDataFirstComment)
        << "processWritePoint should contain a comment explaining the "
           "data-before-metadata ordering rationale for crash safety";
}
