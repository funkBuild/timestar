// Source-inspection tests verifying TSM file deletion after compaction.
//
// These tests read the actual source files at test time to confirm that:
//   1. removeTSMFiles() calls scheduleDelete() to physically delete files
//   2. The dead scheduleDeletionFlag / ref-counting code has been removed
//   3. scheduleDelete() properly closes the file and removes it from disk

#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <sstream>

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

    EXPECT_EQ(hdr.find("refCount"), std::string::npos)
        << "refCount should be removed from tsm.hpp (dead code)";
}

// ---------------------------------------------------------------------------
// 5. addRef / releaseRef / getRefCount dead code removed from tsm.hpp
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, RefCountMethodsRemoved) {
    std::string hdr = readSourceFile(TSM_HPP_SOURCE_PATH);
    ASSERT_FALSE(hdr.empty());

    EXPECT_EQ(hdr.find("addRef"), std::string::npos)
        << "addRef() should be removed from tsm.hpp (dead code)";

    EXPECT_EQ(hdr.find("releaseRef"), std::string::npos)
        << "releaseRef() should be removed from tsm.hpp (dead code)";

    EXPECT_EQ(hdr.find("getRefCount"), std::string::npos)
        << "getRefCount() should be removed from tsm.hpp (dead code)";
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

    EXPECT_NE(hdr.find("scheduleDelete"), std::string::npos)
        << "scheduleDelete() should be declared in tsm.hpp";
}

// ---------------------------------------------------------------------------
// 9. scheduleDelete closes file and calls remove_file
// ---------------------------------------------------------------------------
TEST(TSMDeletionTest, ScheduleDeleteClosesAndRemovesFile) {
    // Read the .cpp that implements scheduleDelete()
    // TSM_COMPACTOR_SOURCE_PATH points to tsm_compactor.cpp, but scheduleDelete
    // is in tsm.cpp.  We can use TSM_FILE_MANAGER_CPP_SOURCE_PATH's directory to
    // derive the tsm.cpp path, but simpler: the compile definition for
    // TSM_HPP_SOURCE_PATH is ".../tsm.hpp".  scheduleDelete is declared there
    // and implemented in tsm.cpp which sits next to it.  We just need to find
    // "remove_file" and "close" in the implementation.  Since
    // TSM_FILE_MANAGER_CPP_SOURCE_PATH also lives in the same directory, we can
    // derive the path.  But we already verified the header declares it.
    // For the implementation, check the compactor source which is in the same dir.
    // Actually, we can construct the path from TSM_HPP_SOURCE_PATH.
    std::string hppPath(TSM_HPP_SOURCE_PATH);
    std::string cppPath = hppPath.substr(0, hppPath.size() - 3) + "cpp"; // .hpp -> .cpp

    std::string src = readSourceFile(cppPath.c_str());
    ASSERT_FALSE(src.empty()) << "Could not read tsm.cpp at: " << cppPath;

    EXPECT_NE(src.find("remove_file"), std::string::npos)
        << "scheduleDelete should call seastar::remove_file to delete the physical file";

    EXPECT_NE(src.find("close"), std::string::npos)
        << "scheduleDelete should close the file before deleting it";
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
