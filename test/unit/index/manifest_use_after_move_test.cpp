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
    // Find all occurrences of push_back(std::move(f)) and verify none are
    // followed by f.fileNumber before the next closing brace '}' (staying in scope).
    std::string pattern = "push_back(std::move(f))";
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

        // There should NOT be f.fileNumber after the move in this window
        EXPECT_EQ(afterMove.find("f.fileNumber"), std::string::npos)
            << "Found access to f.fileNumber after std::move(f) "
            << "(occurrence #" << occurrences << "). "
            << "This is use-after-move undefined behavior.\n"
            << "Code after move:\n" << afterMove;
        pos += pattern.size();
    }

    EXPECT_GE(occurrences, 1) << "Expected at least one push_back(std::move(f)) in manifest.cpp";
}

TEST_F(ManifestUseAfterMoveTest, SourceInspection_SavesFileNumberBeforeMove) {
    // The fix pattern: save fileNumber to a local before the move.
    // Look for "fn = f.fileNumber" appearing before the second push_back(std::move(f)).
    // The second occurrence is in the recovery function where the bug was.

    std::string pattern = "push_back(std::move(f))";
    size_t firstPos = sourceCode.find(pattern);
    ASSERT_NE(firstPos, std::string::npos);
    size_t secondPos = sourceCode.find(pattern, firstPos + pattern.size());
    ASSERT_NE(secondPos, std::string::npos) << "Expected at least two push_back(std::move(f))";

    // Check the 200 chars before the second move for the saved variable
    size_t start = (secondPos > 200) ? secondPos - 200 : 0;
    auto beforeMove = sourceCode.substr(start, secondPos - start);

    // Should find something like "fn = f.fileNumber"
    bool savesFileNumber = (beforeMove.find("= f.fileNumber") != std::string::npos);
    EXPECT_TRUE(savesFileNumber)
        << "Should save f.fileNumber to a local variable before std::move(f) "
        << "in the recovery function.\n"
        << "Code before move:\n" << beforeMove;
}
#endif
