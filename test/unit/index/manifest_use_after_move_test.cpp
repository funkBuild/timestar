#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Bug #15: Manifest use-after-move
//
// In manifest.cpp recovery, after `files_.push_back(std::move(f))`, the code
// read `f.fileNumber` which is undefined behavior (use-after-move).
// The fix saves `f.fileNumber` into a local variable before the move.
// =============================================================================

#ifndef MANIFEST_SOURCE_PATH
TEST(ManifestUseAfterMove, SourceInspection_NoUseAfterMove) {
    GTEST_SKIP() << "MANIFEST_SOURCE_PATH not defined";
}
TEST(ManifestUseAfterMove, SourceInspection_SavesFileNumberBeforeMove) {
    GTEST_SKIP() << "MANIFEST_SOURCE_PATH not defined";
}
#else
class ManifestUseAfterMoveTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(MANIFEST_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open manifest.cpp at: " << MANIFEST_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }
};

TEST_F(ManifestUseAfterMoveTest, SourceInspection_NoUseAfterMove) {
    // For every push_back(std::move(<ident>)), the moved identifier must not
    // be dereferenced again before the end of its scope window. Identifiers
    // are extracted rather than hardcoded so a rename cannot silently turn
    // this test into a no-op (it did once: `f` became `file`).
    const std::string pattern = "push_back(std::move(";
    size_t pos = 0;
    int occurrences = 0;

    while ((pos = sourceCode.find(pattern, pos)) != std::string::npos) {
        const size_t identBegin = pos + pattern.size();
        const size_t identEnd = sourceCode.find(')', identBegin);
        ASSERT_NE(identEnd, std::string::npos);
        const std::string ident = sourceCode.substr(identBegin, identEnd - identBegin);
        ++occurrences;

        // Find the next closing brace (end of scope) or next 80 chars, whichever is first
        const size_t afterPos = identEnd;
        const size_t nextBrace = sourceCode.find('}', afterPos);
        size_t checkEnd = std::min(afterPos + 80, sourceCode.size());
        if (nextBrace != std::string::npos && nextBrace < checkEnd) {
            checkEnd = nextBrace;
        }

        const auto afterMove = sourceCode.substr(afterPos, checkEnd - afterPos);
        EXPECT_EQ(afterMove.find(ident + "."), std::string::npos)
            << "Found access to " << ident << " after std::move(" << ident << ") (occurrence #" << occurrences
            << "). This is use-after-move undefined behavior.\n"
            << "Code after move:\n"
            << afterMove;
        pos = identEnd;
    }

    EXPECT_GE(occurrences, 1) << "Expected at least one push_back(std::move(...)) in manifest.cpp";
}

TEST_F(ManifestUseAfterMoveTest, SourceInspection_CompactionPathReadsFileNumberFromCopyOrSavedLocal) {
    // The historical bug moved the new file into files_ and then read its
    // fileNumber. The fixed compaction path pushes a copy and pins the
    // fileNumber into a local before the removal lambda uses it; the new
    // file must never be moved into files_.
    EXPECT_NE(sourceCode.find("files_.push_back(newFile)"), std::string::npos)
        << "Compaction path should push a copy of newFile";
    EXPECT_NE(sourceCode.find("newFn = newFile.fileNumber"), std::string::npos)
        << "Compaction path should pin newFile.fileNumber into a local for the removal lambda";
    EXPECT_EQ(sourceCode.find("push_back(std::move(newFile))"), std::string::npos)
        << "newFile is read after insertion and must not be moved into files_";
}
#endif
