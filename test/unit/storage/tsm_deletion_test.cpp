// Source-inspection tests verifying TSM file deletion after compaction.
//
// These tests read the actual source files at test time to confirm that:
//   1. removeTSMFiles() calls scheduleDelete() to physically delete files
//   2. The dead scheduleDeletionFlag / ref-counting code has been removed
//   3. scheduleDelete() properly closes the file and removes it from disk

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

namespace {

// Read an entire source file into a string using the compile-time path macros.
std::string readSourceFile(const char* path) {
    std::ifstream in(path);
    EXPECT_TRUE(in.is_open()) << "Could not open source file: " << path;
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

// ---------------------------------------------------------------------------
// 1. removeTSMFiles must call scheduleDelete (actual file deletion)
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RemoveTSMFilesCallsScheduleDelete) {
    std::string src = readSourceFile(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    // removeTSMFiles must invoke scheduleDelete() to physically remove TSM files
    EXPECT_NE(src.find("scheduleDelete"), std::string::npos)
        << "removeTSMFiles should call scheduleDelete() to delete physical files";
}

// ---------------------------------------------------------------------------
// 2. removeTSMFiles must NOT use markForDeletion (the old dead-code path)
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RemoveTSMFilesDoesNotUseMarkForDeletion) {
    std::string src = readSourceFile(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    EXPECT_EQ(src.find("markForDeletion"), std::string::npos)
        << "removeTSMFiles should not use the old markForDeletion mechanism";
}

// ---------------------------------------------------------------------------
// 3. scheduleDeletionFlag dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, ScheduleDeletionFlagRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("scheduleDeletionFlag"), std::string::npos)
        << "scheduleDeletionFlag should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 4. refCount dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RefCountRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("refCount"), std::string::npos) << "refCount should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 5. addRef / releaseRef / getRefCount dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RefCountMethodsRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("addRef"), std::string::npos) << "addRef() should be removed from tsm.hpp (dead code)";

    EXPECT_EQ(hdr.find("releaseRef"), std::string::npos) << "releaseRef() should be removed from tsm.hpp (dead code)";

    EXPECT_EQ(hdr.find("getRefCount"), std::string::npos) << "getRefCount() should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 6. markForDeletion dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, MarkForDeletionRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("markForDeletion"), std::string::npos)
        << "markForDeletion() should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 7. maybeScheduleDeletion dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, MaybeScheduleDeletionRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("maybeScheduleDeletion"), std::string::npos)
        << "maybeScheduleDeletion() should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 8. scheduleDelete exists in tsm.hpp (the useful method is kept)
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, ScheduleDeleteMethodExists) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_NE(hdr.find("scheduleDelete"), std::string::npos) << "scheduleDelete() should be declared in tsm.hpp";
}

// ---------------------------------------------------------------------------
// 9. scheduleDelete unlinks file (deferred close — fd stays open for readers)
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, ScheduleDeleteUnlinksFile) {
    // scheduleDelete must call remove_file (unlink) but must NOT explicitly
    // close the file handle — concurrent readers may still hold shared_ptr<TSM>
    // references with in-flight DMA reads.  The fd is closed naturally when
    // the last shared_ptr<TSM> drops and ~seastar::file runs.
    std::string hppPath(TSM_HPP_SOURCE_PATH);
    std::string cppPath = hppPath.substr(0, hppPath.size() - 3) + "cpp";  // .hpp -> .cpp

    std::string src = readSourceFile(cppPath.c_str());
    ASSERT_FALSE(src.empty()) << "Could not read tsm.cpp at: " << cppPath;

    EXPECT_NE(src.find("remove_file"), std::string::npos)
        << "scheduleDelete should call seastar::remove_file to unlink the physical file";

    // Extract the scheduleDelete function body and verify it does NOT call close()
    auto funcStart = src.find("TSM::scheduleDelete()");
    ASSERT_NE(funcStart, std::string::npos) << "scheduleDelete function not found in tsm.cpp";

    // Find the end of the function (next function or end of file)
    // Look for the closing brace pattern of the function
    auto bodyStart = src.find('{', funcStart);
    ASSERT_NE(bodyStart, std::string::npos);

    // Simple brace-matching to find function end
    int depth = 0;
    size_t funcEnd = bodyStart;
    for (size_t i = bodyStart; i < src.size(); ++i) {
        if (src[i] == '{')
            ++depth;
        else if (src[i] == '}') {
            --depth;
            if (depth == 0) {
                funcEnd = i;
                break;
            }
        }
    }

    std::string funcBody = src.substr(bodyStart, funcEnd - bodyStart + 1);

    // The function should NOT contain tsmFile.close() — deferred close pattern
    EXPECT_EQ(funcBody.find("tsmFile.close()"), std::string::npos)
        << "scheduleDelete must NOT explicitly close the file handle (deferred close pattern)";
}

// ---------------------------------------------------------------------------
// 10. removeTSMFiles also cleans up tombstone files
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RemoveTSMFilesDeletesTombstones) {
    std::string src = readSourceFile(TSM_FILE_MANAGER_CPP_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    EXPECT_NE(src.find("deleteTombstoneFile"), std::string::npos)
        << "removeTSMFiles should delete associated tombstone files";
}

// ---------------------------------------------------------------------------
// 11. Deferred-close: query paths snapshot the TSM file map
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, QueryPathsSnapshotTsmFileMap) {
    // queryTsm and queryTsmAggregated must copy the file map into a local
    // vector before iterating, so that compaction removing files mid-query
    // does not invalidate iterators.
    std::string hppPath(TSM_HPP_SOURCE_PATH);
    std::string dir = hppPath.substr(0, hppPath.rfind('/'));
    // query_runner.cpp lives under lib/query — go up from lib/storage
    std::string qrDir = dir.substr(0, dir.rfind('/')) + "/query";
    std::string qrPath = qrDir + "/query_runner.cpp";

    std::string src = readSourceFile(qrPath.c_str());
    ASSERT_FALSE(src.empty()) << "Could not read query_runner.cpp at: " << qrPath;

    // The snapshot pattern creates a local vector from the map's begin/end
    // instead of taking a const& to the live map.
    auto countOccurrences = [](const std::string& hay, const std::string& needle) {
        size_t count = 0;
        size_t pos = 0;
        while ((pos = hay.find(needle, pos)) != std::string::npos) {
            ++count;
            pos += needle.size();
        }
        return count;
    };

    // queryTsm used to take a const& — verify it no longer does
    EXPECT_EQ(src.find("const auto& seqFiles = fileManager->getSequencedTsmFiles()"), std::string::npos)
        << "queryTsm should snapshot the TSM file map, not take a const reference";

    // There should be at least two snapshot vectors (queryTsm + queryTsmAggregated)
    EXPECT_GE(countOccurrences(src, "getSequencedTsmFiles().begin()"), 2u)
        << "Both queryTsm and queryTsmAggregated should snapshot the TSM file map";
}

// ---------------------------------------------------------------------------
// 12. Deferred-close: prefetchSeriesIndices snapshots the file map
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, PrefetchSnapshotsTsmFileMap) {
    std::string hppPath(TSM_HPP_SOURCE_PATH);
    std::string dir = hppPath.substr(0, hppPath.rfind('/'));
    // engine.cpp lives under lib/core
    std::string engineDir = dir.substr(0, dir.rfind('/')) + "/core";
    std::string enginePath = engineDir + "/engine.cpp";

    std::string src = readSourceFile(enginePath.c_str());
    ASSERT_FALSE(src.empty()) << "Could not read engine.cpp at: " << enginePath;

    // Find the prefetchSeriesIndices function
    auto funcStart = src.find("Engine::prefetchSeriesIndices");
    ASSERT_NE(funcStart, std::string::npos) << "prefetchSeriesIndices not found in engine.cpp";

    // Extract function body
    auto bodyStart = src.find('{', funcStart);
    ASSERT_NE(bodyStart, std::string::npos);
    int depth = 0;
    size_t funcEnd = bodyStart;
    for (size_t i = bodyStart; i < src.size(); ++i) {
        if (src[i] == '{')
            ++depth;
        else if (src[i] == '}') {
            --depth;
            if (depth == 0) {
                funcEnd = i;
                break;
            }
        }
    }
    std::string funcBody = src.substr(bodyStart, funcEnd - bodyStart + 1);

    // Should create a snapshot vector, not iterate the map directly
    EXPECT_NE(funcBody.find("tsmSnapshot"), std::string::npos)
        << "prefetchSeriesIndices should use a snapshot vector of TSM file pointers";
}
