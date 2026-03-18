#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #8: Stale thread-local tlStringDict after co_await
//
// In readSingleBlock, getFullIndexEntry sets tlStringDict, then a co_await
// (dma_read_exactly) suspends the coroutine. Another coroutine on the same
// thread could overwrite tlStringDict during suspension. The fix captures
// the dictionary pointer into a local variable BEFORE the co_await, then
// uses the local variable for the dictionary decode check.
// =============================================================================

class TlStringDictStaleTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef TSM_CPP_SOURCE_PATH
        std::ifstream file(TSM_CPP_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/storage/tsm.cpp",
            "../../lib/storage/tsm.cpp",
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

    // Extract the readSingleBlock function body
    std::string extractReadSingleBlock() const {
        auto pos = sourceCode.find("TSM::readSingleBlock");
        if (pos == std::string::npos) return "";
        auto braceStart = sourceCode.find('{', pos);
        if (braceStart == std::string::npos) return "";
        int depth = 1;
        size_t i = braceStart + 1;
        while (i < sourceCode.size() && depth > 0) {
            if (sourceCode[i] == '{') depth++;
            else if (sourceCode[i] == '}') depth--;
            i++;
        }
        return sourceCode.substr(pos, i - pos);
    }
};

TEST_F(TlStringDictStaleTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load tsm.cpp source file";
}

TEST_F(TlStringDictStaleTest, LocalDictCapturedBeforeCoAwait) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // Verify localDict is declared before the dma_read co_await
    auto localDictPos = body.find("localDict = tlStringDict");
    auto coAwaitPos = body.find("co_await tsmFile.dma_read_exactly");
    ASSERT_NE(localDictPos, std::string::npos)
        << "readSingleBlock must capture tlStringDict into localDict before co_await";
    ASSERT_NE(coAwaitPos, std::string::npos)
        << "readSingleBlock must contain a co_await dma_read_exactly call";
    EXPECT_LT(localDictPos, coAwaitPos)
        << "localDict must be captured BEFORE the co_await suspension point";
}

TEST_F(TlStringDictStaleTest, TlStringDictNotReadAfterCoAwait) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // Find the co_await position
    auto coAwaitPos = body.find("co_await tsmFile.dma_read_exactly");
    ASSERT_NE(coAwaitPos, std::string::npos);

    // Everything after the co_await should NOT reference tlStringDict in code
    // (comments mentioning tlStringDict are fine — we strip them)
    std::string afterCoAwait = body.substr(coAwaitPos);

    // Strip single-line comments (// ...) before checking for tlStringDict usage
    std::string codeOnly;
    std::istringstream stream(afterCoAwait);
    std::string line;
    while (std::getline(stream, line)) {
        auto commentPos = line.find("//");
        if (commentPos != std::string::npos) {
            line = line.substr(0, commentPos);
        }
        codeOnly += line + "\n";
    }

    auto tlUsage = codeOnly.find("tlStringDict");
    EXPECT_EQ(tlUsage, std::string::npos)
        << "tlStringDict must not be read in code after co_await — use localDict instead";
}

TEST_F(TlStringDictStaleTest, LocalDictUsedForDictionaryCheck) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // The dictionary decode check should use localDict, not tlStringDict
    EXPECT_NE(body.find("localDict && !localDict->empty()"), std::string::npos)
        << "Dictionary decode check must use localDict, not tlStringDict";
}
