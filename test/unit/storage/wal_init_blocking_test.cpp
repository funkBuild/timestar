#include <gtest/gtest.h>
#include <fstream>
#include <string>

// =============================================================================
// Source code inspection tests for blocking std::filesystem calls
//
// These tests verify that WALFileManager::init(), Engine::createDirectoryStructure(),
// and LevelDBIndex::open() do NOT use bare blocking std::filesystem calls on the
// Seastar reactor thread. All such calls must be wrapped in seastar::async().
// =============================================================================

// ---------------------------------------------------------------------------
// Helper: extract a method body by name from source code
// ---------------------------------------------------------------------------
static std::string extractMethodBody(const std::string& source,
                                     const std::string& methodSignature) {
    auto pos = source.find(methodSignature);
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

// ---------------------------------------------------------------------------
// Check whether a pattern occurs in the method body but ONLY inside a
// seastar::async lambda.  Returns true if the pattern is found OUTSIDE
// seastar::async (i.e. bare / blocking).
// ---------------------------------------------------------------------------
static bool hasBareCall(const std::string& methodBody,
                        const std::string& pattern) {
    size_t searchStart = 0;
    while (true) {
        auto pos = methodBody.find(pattern, searchStart);
        if (pos == std::string::npos) return false;  // pattern not found at all

        // Walk backwards to see if we are inside a seastar::async block.
        // We look for "seastar::async" before this position and check that
        // the matching opening brace is still open (depth > 0).
        bool insideAsync = false;

        // Find the most recent "seastar::async" before pos
        std::string asyncMarker = "seastar::async";
        auto asyncPos = methodBody.rfind(asyncMarker, pos);
        if (asyncPos != std::string::npos) {
            // Find the opening brace of the async lambda after asyncPos
            auto asyncBrace = methodBody.find('{', asyncPos);
            if (asyncBrace != std::string::npos && asyncBrace < pos) {
                // Count brace depth from asyncBrace to pos
                int depth = 0;
                for (size_t j = asyncBrace; j < pos; ++j) {
                    if (methodBody[j] == '{') depth++;
                    else if (methodBody[j] == '}') depth--;
                }
                // If depth > 0, we are still inside the async block
                if (depth > 0) {
                    insideAsync = true;
                }
            }
        }

        if (!insideAsync) {
            return true;  // Found the pattern outside of seastar::async
        }

        searchStart = pos + pattern.size();
    }
}

// ===========================================================================
// WALFileManager tests
// ===========================================================================

class WALInitBlockingTest : public ::testing::Test {
protected:
    std::string walSource;
    std::string engineSource;
    std::string indexSource;

    void SetUp() override {
        {
            std::ifstream file(WAL_FILE_MANAGER_CPP_SOURCE_PATH);
            ASSERT_TRUE(file.is_open())
                << "Could not open wal_file_manager.cpp at: "
                << WAL_FILE_MANAGER_CPP_SOURCE_PATH;
            walSource.assign(std::istreambuf_iterator<char>(file),
                             std::istreambuf_iterator<char>());
            ASSERT_FALSE(walSource.empty());
        }
        {
            std::ifstream file(ENGINE_CPP_SOURCE_PATH);
            ASSERT_TRUE(file.is_open())
                << "Could not open engine.cpp at: " << ENGINE_CPP_SOURCE_PATH;
            engineSource.assign(std::istreambuf_iterator<char>(file),
                                std::istreambuf_iterator<char>());
            ASSERT_FALSE(engineSource.empty());
        }
        {
            std::ifstream file(LEVELDB_INDEX_CPP_SOURCE_PATH);
            ASSERT_TRUE(file.is_open())
                << "Could not open leveldb_index.cpp at: "
                << LEVELDB_INDEX_CPP_SOURCE_PATH;
            indexSource.assign(std::istreambuf_iterator<char>(file),
                               std::istreambuf_iterator<char>());
            ASSERT_FALSE(indexSource.empty());
        }
    }
};

// Test 1: WAL init must not have bare fs::exists outside seastar::async
TEST_F(WALInitBlockingTest, WALInitNoBlockingFsExists) {
    std::string initBody = extractMethodBody(walSource, "WALFileManager::init(");
    ASSERT_FALSE(initBody.empty())
        << "Could not extract WALFileManager::init() method body";

    bool hasBare = hasBareCall(initBody, "fs::exists");

    EXPECT_FALSE(hasBare)
        << "Found bare 'fs::exists' call in WALFileManager::init() outside of "
        << "seastar::async. This is a blocking syscall that can stall the "
        << "Seastar reactor thread. Wrap it in seastar::async().";
}

// Test 2: WAL init must not have bare fs::directory_iterator outside seastar::async
TEST_F(WALInitBlockingTest, WALInitNoBlockingDirectoryIterator) {
    std::string initBody = extractMethodBody(walSource, "WALFileManager::init(");
    ASSERT_FALSE(initBody.empty())
        << "Could not extract WALFileManager::init() method body";

    bool hasBare = hasBareCall(initBody, "fs::directory_iterator");

    EXPECT_FALSE(hasBare)
        << "Found bare 'fs::directory_iterator' call in WALFileManager::init() "
        << "outside of seastar::async. This is a blocking syscall that can stall "
        << "the Seastar reactor thread. Wrap it in seastar::async().";
}

// Test 3: WAL init must use seastar::async for the directory scan
TEST_F(WALInitBlockingTest, WALInitUsesAsyncForDirectoryScan) {
    std::string initBody = extractMethodBody(walSource, "WALFileManager::init(");
    ASSERT_FALSE(initBody.empty())
        << "Could not extract WALFileManager::init() method body";

    bool hasAsync = (initBody.find("seastar::async") != std::string::npos);

    EXPECT_TRUE(hasAsync)
        << "Expected 'seastar::async' in WALFileManager::init() to wrap "
        << "blocking std::filesystem directory scan operations. The Seastar "
        << "reactor must not be blocked by synchronous filesystem operations.";
}

// Test 4: Engine must not have bare fs::create_directories outside seastar::async
TEST_F(WALInitBlockingTest, EngineNoBlockingCreateDirectories) {
    std::string createDirBody =
        extractMethodBody(engineSource, "Engine::createDirectoryStructure()");
    ASSERT_FALSE(createDirBody.empty())
        << "Could not extract Engine::createDirectoryStructure() method body";

    bool hasBare = hasBareCall(createDirBody, "fs::create_directories");

    EXPECT_FALSE(hasBare)
        << "Found bare 'fs::create_directories' call in "
        << "Engine::createDirectoryStructure() outside of seastar::async. "
        << "This is a blocking syscall that can stall the Seastar reactor "
        << "thread. Wrap it in seastar::async().";
}

// Test 5: LevelDBIndex::open() must not have bare std::filesystem::create_directories
//         outside seastar::async
TEST_F(WALInitBlockingTest, LevelDBIndexNoBlockingCreateDirectories) {
    std::string openBody =
        extractMethodBody(indexSource, "LevelDBIndex::open()");
    ASSERT_FALSE(openBody.empty())
        << "Could not extract LevelDBIndex::open() method body";

    bool hasBare =
        hasBareCall(openBody, "std::filesystem::create_directories");

    EXPECT_FALSE(hasBare)
        << "Found bare 'std::filesystem::create_directories' call in "
        << "LevelDBIndex::open() outside of seastar::async. This is a blocking "
        << "syscall that can stall the Seastar reactor thread. Wrap it in "
        << "seastar::async().";
}
