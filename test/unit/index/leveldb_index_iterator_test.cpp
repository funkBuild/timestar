#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Source-inspection tests: verify all LevelDB iterators use RAII (unique_ptr)
// =============================================================================

class LevelDBIndexIteratorTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
        std::ifstream file(LEVELDB_INDEX_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open leveldb_index.cpp at: " << LEVELDB_INDEX_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());
    }

    // Helper: extract the body of a named method from the source code.
    // Looks for the method signature, then collects everything from the
    // opening '{' to the matching closing '}'.
    std::string extractMethodBody(const std::string& methodName) const {
        size_t pos = sourceCode.find(methodName);
        if (pos == std::string::npos)
            return "";

        // Find the opening brace of the method body
        size_t braceStart = sourceCode.find('{', pos);
        if (braceStart == std::string::npos)
            return "";

        // Match braces to find the end of the method
        int depth = 0;
        size_t i = braceStart;
        for (; i < sourceCode.size(); ++i) {
            if (sourceCode[i] == '{')
                depth++;
            else if (sourceCode[i] == '}') {
                depth--;
                if (depth == 0)
                    break;
            }
        }

        return sourceCode.substr(braceStart, i - braceStart + 1);
    }

    // Helper: check if a method body contains a raw "leveldb::Iterator*" that
    // is NOT part of a unique_ptr<leveldb::Iterator> declaration.
    // We do this by checking that the body has no "leveldb::Iterator*" pattern
    // that appears on a line without "unique_ptr".
    bool hasRawIteratorPointer(const std::string& body) const {
        std::istringstream stream(body);
        std::string line;
        while (std::getline(stream, line)) {
            // Check if this line declares a leveldb::Iterator pointer
            if (line.find("leveldb::Iterator*") != std::string::npos ||
                line.find("leveldb::Iterator *") != std::string::npos) {
                // It's OK if it's inside a unique_ptr declaration
                if (line.find("unique_ptr") == std::string::npos) {
                    return true;  // Raw pointer found!
                }
            }
        }
        return false;
    }

    // Helper: check if a string contains "delete it;"
    bool hasDeleteIt(const std::string& text) const {
        // Search for "delete it;" with flexible whitespace
        size_t pos = 0;
        while ((pos = text.find("delete", pos)) != std::string::npos) {
            size_t afterDelete = pos + 6;  // length of "delete"
            // Skip whitespace after "delete"
            while (afterDelete < text.size() && (text[afterDelete] == ' ' || text[afterDelete] == '\t')) {
                afterDelete++;
            }
            // Check if the next token is "it" followed by ";"
            if (afterDelete + 2 <= text.size() && text[afterDelete] == 'i' && text[afterDelete + 1] == 't') {
                size_t afterIt = afterDelete + 2;
                // Skip whitespace after "it"
                while (afterIt < text.size() && (text[afterIt] == ' ' || text[afterIt] == '\t')) {
                    afterIt++;
                }
                if (afterIt < text.size() && text[afterIt] == ';') {
                    return true;
                }
            }
            pos = afterDelete;
        }
        return false;
    }
};

// 1. findSeriesByTag uses unique_ptr, not raw pointer
TEST_F(LevelDBIndexIteratorTest, FindSeriesByTagUsesUniquePtr) {
    std::string body = extractMethodBody("LevelDBIndex::findSeriesByTag");
    ASSERT_FALSE(body.empty()) << "Could not find findSeriesByTag method body";

    EXPECT_NE(body.find("std::unique_ptr<leveldb::Iterator>"), std::string::npos)
        << "findSeriesByTag should use std::unique_ptr<leveldb::Iterator> for RAII.";

    EXPECT_FALSE(hasRawIteratorPointer(body)) << "findSeriesByTag should NOT use raw leveldb::Iterator* pointer.";
}

// 2. getSeriesGroupedByTag uses unique_ptr, not raw pointer
TEST_F(LevelDBIndexIteratorTest, GetSeriesGroupedByTagUsesUniquePtr) {
    std::string body = extractMethodBody("LevelDBIndex::getSeriesGroupedByTag");
    ASSERT_FALSE(body.empty()) << "Could not find getSeriesGroupedByTag method body";

    EXPECT_NE(body.find("std::unique_ptr<leveldb::Iterator>"), std::string::npos)
        << "getSeriesGroupedByTag should use std::unique_ptr<leveldb::Iterator> for RAII.";

    EXPECT_FALSE(hasRawIteratorPointer(body)) << "getSeriesGroupedByTag should NOT use raw leveldb::Iterator* pointer.";
}

// 3. getAllSeriesForMeasurement uses unique_ptr, not raw pointer
TEST_F(LevelDBIndexIteratorTest, GetAllSeriesForMeasurementUsesUniquePtr) {
    std::string body = extractMethodBody("LevelDBIndex::getAllSeriesForMeasurement");
    ASSERT_FALSE(body.empty()) << "Could not find getAllSeriesForMeasurement method body";

    EXPECT_NE(body.find("std::unique_ptr<leveldb::Iterator>"), std::string::npos)
        << "getAllSeriesForMeasurement should use std::unique_ptr<leveldb::Iterator> for RAII.";

    EXPECT_FALSE(hasRawIteratorPointer(body))
        << "getAllSeriesForMeasurement should NOT use raw leveldb::Iterator* pointer.";
}

// 4. No raw "delete it;" calls remain for iterator cleanup
TEST_F(LevelDBIndexIteratorTest, NoRawDeleteIterator) {
    // "delete it;" should not appear anywhere in the file -- the unique_ptr
    // handles cleanup automatically, including on exception paths.
    EXPECT_FALSE(hasDeleteIt(sourceCode)) << "Found 'delete it;' in leveldb_index.cpp. All LevelDB iterators "
                                             "should be managed by std::unique_ptr -- manual delete is not needed.";
}

// 5. Every NewIterator() call in the file is wrapped in unique_ptr
TEST_F(LevelDBIndexIteratorTest, ConsistentIteratorPattern) {
    // Count all occurrences of NewIterator
    size_t totalNewIterator = 0;
    size_t uniquePtrNewIterator = 0;

    std::string searchStr = "NewIterator";
    size_t pos = 0;
    while ((pos = sourceCode.find(searchStr, pos)) != std::string::npos) {
        totalNewIterator++;

        // Check if this NewIterator is preceded by "unique_ptr<leveldb::Iterator>"
        // within the same line (look backwards for the pattern)
        size_t lineStart = sourceCode.rfind('\n', pos);
        if (lineStart == std::string::npos)
            lineStart = 0;
        std::string linePrefix = sourceCode.substr(lineStart, pos - lineStart);

        if (linePrefix.find("unique_ptr<leveldb::Iterator>") != std::string::npos) {
            uniquePtrNewIterator++;
        }

        pos += searchStr.length();
    }

    ASSERT_GT(totalNewIterator, 0u) << "Expected at least one NewIterator() call in leveldb_index.cpp";
    EXPECT_EQ(totalNewIterator, uniquePtrNewIterator)
        << "All " << totalNewIterator << " NewIterator() calls should be wrapped in "
        << "std::unique_ptr<leveldb::Iterator>, but only " << uniquePtrNewIterator
        << " are. Every iterator must use RAII for exception safety.";
}
