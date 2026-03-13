#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Source code inspection tests for WALFileManager::rolloverMemoryStore()
//
// These tests verify that the rollover sequence is ordered correctly to
// prevent a race condition where concurrent inserts can arrive (via the
// Seastar event loop during co_await yields) and hit a closed memory store.
//
// Correct ordering:
//   1. Create new MemoryStore (make_shared<MemoryStore>)
//   2. Init new store WAL   (store->initWAL)
//   3. Install new store    (memoryStores.insert)
//   4. Close old store      (previousStore->close)
//   5. Convert to TSM       (convertWalToTsm)
//
// The critical invariant is that memoryStores[0] always points to an open
// store, even across co_await yield points.
// =============================================================================

// Helper: extract a method body by name from source code
static std::string extractMethodBody(const std::string& source, const std::string& methodSignature) {
    auto pos = source.find(methodSignature);
    if (pos == std::string::npos)
        return "";

    // Find the opening brace of the method
    auto bracePos = source.find('{', pos);
    if (bracePos == std::string::npos)
        return "";

    // Track brace depth to find the end of the method
    int depth = 1;
    size_t i = bracePos + 1;
    while (i < source.size() && depth > 0) {
        if (source[i] == '{')
            depth++;
        else if (source[i] == '}')
            depth--;
        i++;
    }

    return source.substr(pos, i - pos);
}

// Helper: find the position of a pattern within a string, returning npos if
// not found. Searches from 'startPos'.
static size_t findPattern(const std::string& body, const std::string& pattern, size_t startPos = 0) {
    return body.find(pattern, startPos);
}

class WALRolloverRaceTest : public ::testing::Test {
protected:
    std::string source;
    std::string rolloverBody;

    void SetUp() override {
        std::ifstream file(WAL_FILE_MANAGER_CPP_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open wal_file_manager.cpp at: " << WAL_FILE_MANAGER_CPP_SOURCE_PATH;
        source.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(source.empty());

        rolloverBody = extractMethodBody(source, "WALFileManager::rolloverMemoryStore()");
        ASSERT_FALSE(rolloverBody.empty()) << "Could not extract WALFileManager::rolloverMemoryStore() body";
    }
};

// Test 1: New MemoryStore creation happens BEFORE previousStore->close()
TEST_F(WALRolloverRaceTest, NewStoreCreatedBeforeOldStoreClosed) {
    size_t makeSharedPos = findPattern(rolloverBody, "make_shared<MemoryStore>");
    size_t closePos = findPattern(rolloverBody, "previousStore->close()");

    ASSERT_NE(makeSharedPos, std::string::npos) << "Could not find 'make_shared<MemoryStore>' in rolloverMemoryStore()";
    ASSERT_NE(closePos, std::string::npos) << "Could not find 'previousStore->close()' in rolloverMemoryStore()";

    EXPECT_LT(makeSharedPos, closePos) << "make_shared<MemoryStore> must appear BEFORE previousStore->close() "
                                       << "to prevent a race window where inserts hit a closed store. "
                                       << "The new store must be created before the old one is closed.";
}

// Test 2: initWAL on new store happens BEFORE previousStore->close()
TEST_F(WALRolloverRaceTest, InitWALBeforeOldStoreClosed) {
    // Find initWAL that is on the new store (not previousStore)
    // We look for "store->initWAL" which is the new store's init
    size_t initWalPos = findPattern(rolloverBody, "store->initWAL()");
    size_t closePos = findPattern(rolloverBody, "previousStore->close()");

    ASSERT_NE(initWalPos, std::string::npos) << "Could not find 'store->initWAL()' in rolloverMemoryStore()";
    ASSERT_NE(closePos, std::string::npos) << "Could not find 'previousStore->close()' in rolloverMemoryStore()";

    EXPECT_LT(initWalPos, closePos) << "store->initWAL() must appear BEFORE previousStore->close(). "
                                    << "The new store must be fully initialized before the old one is "
                                    << "closed, so it can accept inserts immediately.";
}

// Test 3: memoryStores.insert (installing new store) happens BEFORE
// previousStore->close()
TEST_F(WALRolloverRaceTest, StoreInstalledBeforeOldStoreClosed) {
    size_t insertPos = findPattern(rolloverBody, "memoryStores.insert(memoryStores.begin()");
    size_t closePos = findPattern(rolloverBody, "previousStore->close()");

    ASSERT_NE(insertPos, std::string::npos) << "Could not find 'memoryStores.insert(memoryStores.begin()' in "
                                            << "rolloverMemoryStore()";
    ASSERT_NE(closePos, std::string::npos) << "Could not find 'previousStore->close()' in rolloverMemoryStore()";

    EXPECT_LT(insertPos, closePos) << "memoryStores.insert() must appear BEFORE previousStore->close(). "
                                   << "The new store must be at memoryStores[0] before the old store is "
                                   << "closed, so any insert coroutine that runs during the co_await in "
                                   << "close() will find an open store at memoryStores[0].";
}

// Test 4: There is no window where close() is called before
// memoryStores.insert -- verify the full ordering:
//   make_shared < initWAL < memoryStores.insert < previousStore->close
TEST_F(WALRolloverRaceTest, FullOrderingIsCorrect) {
    size_t makeSharedPos = findPattern(rolloverBody, "make_shared<MemoryStore>");
    size_t initWalPos = findPattern(rolloverBody, "store->initWAL()");
    size_t insertPos = findPattern(rolloverBody, "memoryStores.insert(memoryStores.begin()");
    size_t closePos = findPattern(rolloverBody, "previousStore->close()");

    ASSERT_NE(makeSharedPos, std::string::npos) << "Could not find 'make_shared<MemoryStore>'";
    ASSERT_NE(initWalPos, std::string::npos) << "Could not find 'store->initWAL()'";
    ASSERT_NE(insertPos, std::string::npos) << "Could not find 'memoryStores.insert(memoryStores.begin()'";
    ASSERT_NE(closePos, std::string::npos) << "Could not find 'previousStore->close()'";

    EXPECT_LT(makeSharedPos, initWalPos) << "make_shared<MemoryStore> must come before store->initWAL()";
    EXPECT_LT(initWalPos, insertPos) << "store->initWAL() must come before memoryStores.insert()";
    EXPECT_LT(insertPos, closePos) << "memoryStores.insert() must come before previousStore->close(). "
                                   << "This ensures there is NO window where memoryStores[0] points to a "
                                   << "closed store. Without this ordering, a concurrent insert coroutine "
                                   << "dispatched by the event loop during a co_await yield could call "
                                   << "insert() on a closed MemoryStore.";
}
