#include <gtest/gtest.h>

#include <fstream>
#include <string>

// =============================================================================
// Bug #15: Manifest use-after-move
//
// In manifest.cpp recovery, after moving a decoded file record into a vector,
// the code read the moved-from record's fileNumber.  Recovery now stages and
// validates records before publishing them, but the AddFile path must still
// save the file number before moving the decoded record.
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
    // Find every move of a decoded `file` record and verify the moved-from
    // record is not subsequently dereferenced in the same statement block.
    std::string pattern = "push_back(std::move(file))";
    size_t pos = 0;
    int occurrences = 0;

    while ((pos = sourceCode.find(pattern, pos)) != std::string::npos) {
        ++occurrences;
        size_t afterPos = pos + pattern.size();

        // Find the next closing brace (end of scope) or next 80 chars, whichever is first
        size_t nextBrace = sourceCode.find('}', afterPos);
        size_t checkEnd = std::min(afterPos + 80, sourceCode.size());
        if (nextBrace != std::string::npos && nextBrace < checkEnd) {
            checkEnd = nextBrace;
        }

        auto afterMove = sourceCode.substr(afterPos, checkEnd - afterPos);

        // There should NOT be file.fileNumber after the move in this window.
        EXPECT_EQ(afterMove.find("file.fileNumber"), std::string::npos)
            << "Found access to file.fileNumber after std::move(file) "
            << "(occurrence #" << occurrences << "). "
            << "This is use-after-move undefined behavior.\n"
            << "Code after move:\n"
            << afterMove;
        pos += pattern.size();
    }

    EXPECT_GE(occurrences, 1) << "Expected at least one push_back(std::move(file)) in manifest.cpp";
}

TEST_F(ManifestUseAfterMoveTest, SourceInspection_SavesFileNumberBeforeMove) {
    const auto savePos = sourceCode.find("const auto fileNumber = file.fileNumber;");
    ASSERT_NE(savePos, std::string::npos) << "Recovery must save file.fileNumber before moving the record";

    const auto movePos = sourceCode.find("files_.push_back(std::move(file));", savePos);
    ASSERT_NE(movePos, std::string::npos) << "Expected the decoded AddFile record to be published";
    EXPECT_LT(savePos, movePos);

    const auto advancePos = sourceCode.find("nextFileNumber_ = fileNumber + 1;", movePos);
    ASSERT_NE(advancePos, std::string::npos) << "Recovery must advance from the saved value";
    EXPECT_LT(movePos, advancePos);
}
#endif
