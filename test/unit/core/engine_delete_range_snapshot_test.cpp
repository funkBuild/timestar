// Source-inspection tests for the deleteRangeImpl iterator invalidation fix.
//
// Bug: deleteRangeImpl() iterated over tsmFileManager.getSequencedTsmFiles()
// (a const std::map&) with co_await suspension points inside the loop body.
// Background compaction can call addTSMFile/removeTSMFiles during any
// suspension, which mutates the map and invalidates the live iterator.
// This is undefined behavior.
//
// Fix: Snapshot the TSM file pointers into a local std::vector before the
// loop, then iterate the snapshot.  This matches the established pattern
// used in prefetchSeriesIndices() and sweepTombstoneRewrites().
//
// These are source-inspection tests because deleteRangeImpl is a Seastar
// coroutine requiring the full Seastar runtime.  We verify the fix by
// reading and parsing the source code.

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

class EngineDeleteRangeSnapshotTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef ENGINE_SOURCE_PATH
        std::ifstream file(ENGINE_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/core/engine.cpp",
            "../../lib/core/engine.cpp",
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

    // Extract the deleteRangeImpl function body from source.
    std::string extractDeleteRangeImpl() const {
        size_t start = sourceCode.find("Engine::deleteRangeImpl");
        if (start == std::string::npos)
            return "";

        size_t braceStart = sourceCode.find('{', start);
        if (braceStart == std::string::npos)
            return "";

        int depth = 1;
        size_t pos = braceStart + 1;
        while (pos < sourceCode.size() && depth > 0) {
            if (sourceCode[pos] == '{')
                depth++;
            else if (sourceCode[pos] == '}')
                depth--;
            pos++;
        }

        return sourceCode.substr(start, pos - start);
    }
};

TEST_F(EngineDeleteRangeSnapshotTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load engine.cpp source file";
    ASSERT_NE(sourceCode.find("Engine::deleteRangeImpl"), std::string::npos)
        << "Source file does not contain Engine::deleteRangeImpl()";
}

// The core safety invariant: deleteRangeImpl must snapshot TSM file pointers
// into a local vector before iterating with co_await suspension points.
TEST_F(EngineDeleteRangeSnapshotTest, SnapshotsTsmFilesBeforeCoAwaitLoop) {
    std::string funcBody = extractDeleteRangeImpl();
    ASSERT_FALSE(funcBody.empty()) << "Could not extract deleteRangeImpl() function body";

    // Must contain a local snapshot vector (tsmSnapshot or similar)
    EXPECT_NE(funcBody.find("tsmSnapshot"), std::string::npos)
        << "BUG: deleteRangeImpl() must snapshot TSM file pointers into a "
           "local vector (tsmSnapshot) before iterating. Without this, "
           "co_await suspension points allow background compaction to "
           "invalidate the iterator over getSequencedTsmFiles().";
}

// The co_await loop must iterate the snapshot, not getSequencedTsmFiles() directly.
TEST_F(EngineDeleteRangeSnapshotTest, CoAwaitLoopIteratesSnapshot) {
    std::string funcBody = extractDeleteRangeImpl();
    ASSERT_FALSE(funcBody.empty());

    // Find all loops that contain co_await (the dangerous pattern).
    // There should be a "for (const auto& tsmFile : tsmSnapshot)" loop
    // with "co_await tsmFile->deleteRange" inside.

    // First, verify the snapshot population loop exists (no co_await):
    // "for (const auto& [rank, tsmFile] : tsmFileManager.getSequencedTsmFiles())"
    //   followed by "tsmSnapshot.push_back(tsmFile)"
    size_t populateLoop = funcBody.find("getSequencedTsmFiles()");
    ASSERT_NE(populateLoop, std::string::npos)
        << "deleteRangeImpl() should read from getSequencedTsmFiles() to populate the snapshot";

    size_t pushBack = funcBody.find("tsmSnapshot.push_back", populateLoop);
    ASSERT_NE(pushBack, std::string::npos)
        << "deleteRangeImpl() should push_back into tsmSnapshot from getSequencedTsmFiles()";

    // The co_await deleteRange call must NOT be inside a loop over getSequencedTsmFiles.
    // Verify: between getSequencedTsmFiles() and tsmSnapshot.push_back, no co_await.
    std::string populateSection = funcBody.substr(populateLoop, pushBack - populateLoop);
    EXPECT_EQ(populateSection.find("co_await"), std::string::npos)
        << "BUG: There must be no co_await between getSequencedTsmFiles() "
           "and tsmSnapshot.push_back. The snapshot population loop must "
           "be synchronous to avoid iterator invalidation.";
}

// Verify the co_await deleteRange call iterates tsmSnapshot, not the map.
TEST_F(EngineDeleteRangeSnapshotTest, DeleteRangeUsesSnapshotIterator) {
    std::string funcBody = extractDeleteRangeImpl();
    ASSERT_FALSE(funcBody.empty());

    // Find "co_await" followed by "deleteRange" (the actual tombstone writes)
    size_t deleteRangeCoAwait = funcBody.find("co_await");
    bool foundDeleteRangeInSnapshotLoop = false;

    while (deleteRangeCoAwait != std::string::npos) {
        // Check if this co_await is for a deleteRange call
        size_t lineEnd = funcBody.find('\n', deleteRangeCoAwait);
        std::string line = funcBody.substr(deleteRangeCoAwait,
                                           lineEnd != std::string::npos ? lineEnd - deleteRangeCoAwait : 80);

        if (line.find("deleteRange") != std::string::npos) {
            // This co_await is a deleteRange call.  Find the enclosing for-loop
            // and verify it iterates tsmSnapshot, not getSequencedTsmFiles.
            // Search backwards for the nearest "for" statement.
            size_t searchStart = (deleteRangeCoAwait > 200) ? deleteRangeCoAwait - 200 : 0;
            std::string preceding = funcBody.substr(searchStart, deleteRangeCoAwait - searchStart);

            // The enclosing loop should reference tsmSnapshot
            EXPECT_NE(preceding.rfind("tsmSnapshot"), std::string::npos)
                << "BUG: co_await deleteRange() is not inside a loop over "
                   "tsmSnapshot. It must iterate the snapshot vector, not "
                   "getSequencedTsmFiles() directly.";

            foundDeleteRangeInSnapshotLoop = true;
            break;
        }

        deleteRangeCoAwait = funcBody.find("co_await", deleteRangeCoAwait + 1);
    }

    EXPECT_TRUE(foundDeleteRangeInSnapshotLoop)
        << "deleteRangeImpl() should contain a co_await deleteRange() call "
           "inside a loop over tsmSnapshot.";
}

// Negative test: the direct iteration pattern with co_await must NOT exist.
TEST_F(EngineDeleteRangeSnapshotTest, NoDirectMapIterationWithCoAwait) {
    std::string funcBody = extractDeleteRangeImpl();
    ASSERT_FALSE(funcBody.empty());

    // Scan for the dangerous pattern: a for-loop over getSequencedTsmFiles()
    // that contains co_await in its body.  We check this by finding each
    // getSequencedTsmFiles() call and verifying no co_await appears between
    // its enclosing '{' and matching '}'.
    size_t pos = 0;
    while ((pos = funcBody.find("getSequencedTsmFiles()", pos)) != std::string::npos) {
        // Find the opening brace of this for-loop body
        size_t loopBrace = funcBody.find('{', pos);
        if (loopBrace == std::string::npos) {
            pos++;
            continue;
        }

        // Match braces to find the loop body extent
        int depth = 1;
        size_t end = loopBrace + 1;
        while (end < funcBody.size() && depth > 0) {
            if (funcBody[end] == '{')
                depth++;
            else if (funcBody[end] == '}')
                depth--;
            end++;
        }

        std::string loopBody = funcBody.substr(loopBrace, end - loopBrace);
        EXPECT_EQ(loopBody.find("co_await"), std::string::npos)
            << "BUG: Found co_await inside a for-loop directly iterating "
               "getSequencedTsmFiles(). This is iterator invalidation UB. "
               "Snapshot into a local vector first, then iterate the snapshot.";

        pos++;
    }
}
