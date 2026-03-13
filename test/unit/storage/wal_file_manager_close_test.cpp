#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Source code inspection tests for WALFileManager::close()
//
// These tests verify that the close() method iterates over ALL memory stores
// in the memoryStores vector, not just memoryStores[0]. The original bug
// caused memory store leaks during shutdown when multiple stores existed
// (e.g., the active store plus older stores still being converted to TSM).
// =============================================================================

class WALFileManagerCloseTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(WAL_FILE_MANAGER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open wal_file_manager.hpp at: " << WAL_FILE_MANAGER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Extract the close() method body from the source code
    std::string getCloseMethodBody() {
        // Find the close() method signature
        auto pos = sourceCode.find("future<> close()");
        EXPECT_NE(pos, std::string::npos) << "Could not find close() method";
        if (pos == std::string::npos)
            return "";

        // Find the opening brace of the method
        auto bracePos = sourceCode.find('{', pos);
        if (bracePos == std::string::npos)
            return "";

        // Track brace depth to find the end of the method
        int depth = 1;
        size_t i = bracePos + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{')
                depth++;
            else if (sourceCode[i] == '}')
                depth--;
            i++;
        }

        return sourceCode.substr(pos, i - pos);
    }
};

// Test 1: Verify the close() method uses a loop over memoryStores
TEST_F(WALFileManagerCloseTest, CloseIteratesAllStores) {
    std::string closeBody = getCloseMethodBody();
    ASSERT_FALSE(closeBody.empty()) << "Could not extract close() method body";

    // The close method should iterate over all memory stores with a for loop
    // e.g., "for (auto& store : memoryStores)" or similar range-based for
    bool hasForLoop = (closeBody.find("for (") != std::string::npos || closeBody.find("for(") != std::string::npos);
    bool iteratesMemoryStores = (closeBody.find("memoryStores") != std::string::npos && hasForLoop);

    EXPECT_TRUE(iteratesMemoryStores) << "The close() method must use a loop (for/while) to iterate over "
                                      << "all entries in memoryStores, not just access a single element.\n"
                                      << "Method contents:\n"
                                      << closeBody;
}

// Test 2: Verify there's error handling (try/catch) inside the loop
TEST_F(WALFileManagerCloseTest, CloseHandlesErrorsPerStore) {
    std::string closeBody = getCloseMethodBody();
    ASSERT_FALSE(closeBody.empty()) << "Could not extract close() method body";

    // Find the for loop
    auto forPos = closeBody.find("for (");
    if (forPos == std::string::npos)
        forPos = closeBody.find("for(");
    ASSERT_NE(forPos, std::string::npos) << "Could not find for loop in close()";

    // Extract the for loop body
    auto bracePos = closeBody.find('{', forPos);
    ASSERT_NE(bracePos, std::string::npos);

    int depth = 1;
    size_t i = bracePos + 1;
    while (i < closeBody.size() && depth > 0) {
        if (closeBody[i] == '{')
            depth++;
        else if (closeBody[i] == '}')
            depth--;
        i++;
    }
    std::string loopBody = closeBody.substr(forPos, i - forPos);

    // The loop body should contain a try/catch block so that one store's
    // failure doesn't prevent closing the remaining stores
    bool hasTry = (loopBody.find("try") != std::string::npos);
    bool hasCatch = (loopBody.find("catch") != std::string::npos);

    EXPECT_TRUE(hasTry && hasCatch) << "The for loop in close() must contain a try/catch block inside "
                                    << "the loop body so that a failure closing one memory store does "
                                    << "not prevent closing the remaining stores.\n"
                                    << "Loop contents:\n"
                                    << loopBody;
}

// Test 3: Verify each store close is logged individually
TEST_F(WALFileManagerCloseTest, CloseLogsEachStore) {
    std::string closeBody = getCloseMethodBody();
    ASSERT_FALSE(closeBody.empty()) << "Could not extract close() method body";

    // Find the for loop
    auto forPos = closeBody.find("for (");
    if (forPos == std::string::npos)
        forPos = closeBody.find("for(");
    ASSERT_NE(forPos, std::string::npos) << "Could not find for loop in close()";

    // Extract the for loop body
    auto bracePos = closeBody.find('{', forPos);
    ASSERT_NE(bracePos, std::string::npos);

    int depth = 1;
    size_t i = bracePos + 1;
    while (i < closeBody.size() && depth > 0) {
        if (closeBody[i] == '{')
            depth++;
        else if (closeBody[i] == '}')
            depth--;
        i++;
    }
    std::string loopBody = closeBody.substr(forPos, i - forPos);

    // The loop should log each store closure for debugging production shutdown
    bool hasInfoLog =
        (loopBody.find("wal_log.info") != std::string::npos || loopBody.find("log.info") != std::string::npos);

    EXPECT_TRUE(hasInfoLog) << "The for loop in close() must log each memory store closure "
                            << "individually (using wal_log.info or similar) for debugging "
                            << "production shutdown issues.\n"
                            << "Loop contents:\n"
                            << loopBody;
}

// Test 4: Verify there's no memoryStores[0] direct access pattern remaining
TEST_F(WALFileManagerCloseTest, CloseNotJustFirstStore) {
    std::string closeBody = getCloseMethodBody();
    ASSERT_FALSE(closeBody.empty()) << "Could not extract close() method body";

    // The buggy pattern accessed only memoryStores[0]. After the fix,
    // there should be no direct index access like memoryStores[0] in close().
    bool hasDirectIndexAccess = (closeBody.find("memoryStores[0]") != std::string::npos);

    EXPECT_FALSE(hasDirectIndexAccess) << "Found 'memoryStores[0]' in close() method. The close() method "
                                       << "should iterate through ALL memory stores using a range-based for "
                                       << "loop, not access only the first element. During shutdown, there "
                                       << "may be multiple stores (the active store plus older stores still "
                                       << "being converted to TSM).\n"
                                       << "Method contents:\n"
                                       << closeBody;
}
