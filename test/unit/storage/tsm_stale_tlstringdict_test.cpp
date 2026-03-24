#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Verify that the thread-local tlStringDict has been fully removed and replaced
// with explicit dictionary parameter passing in readSingleBlock.
//
// The old code used a thread_local global `tlStringDict` which was fragile:
// any coroutine calling decodeBlock without the right dictionary being set
// could silently corrupt data. The fix passes the dictionary pointer
// explicitly through the call chain.
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

TEST_F(TlStringDictStaleTest, TlStringDictRemovedFromSource) {
    // The thread-local global tlStringDict should no longer exist in the source
    // (comments mentioning its removal are fine, but there should be no definition)
    auto pos = sourceCode.find("static thread_local const std::vector<std::string>* tlStringDict");
    EXPECT_EQ(pos, std::string::npos)
        << "tlStringDict thread-local global should be removed — dictionary is now passed explicitly";
}

TEST_F(TlStringDictStaleTest, LocalDictCapturedBeforeCoAwait) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // Verify localDict is set from the explicit stringDict parameter before co_await.
    auto localDictPos = body.find("localDict = stringDict");
    auto coAwaitPos = body.find("co_await tsmFile.dma_read_exactly");
    ASSERT_NE(localDictPos, std::string::npos)
        << "readSingleBlock must set localDict from stringDict param before co_await";
    ASSERT_NE(coAwaitPos, std::string::npos)
        << "readSingleBlock must contain a co_await dma_read_exactly call";
    EXPECT_LT(localDictPos, coAwaitPos)
        << "localDict must be set BEFORE the co_await suspension point";
}

TEST_F(TlStringDictStaleTest, TlStringDictNotUsedInReadSingleBlock) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // Strip single-line comments (// ...) before checking for tlStringDict usage
    std::string codeOnly;
    std::istringstream stream(body);
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
        << "tlStringDict must not appear in readSingleBlock — use stringDict/localDict instead";
}

TEST_F(TlStringDictStaleTest, LocalDictUsedForDictionaryCheck) {
    std::string body = extractReadSingleBlock();
    ASSERT_FALSE(body.empty()) << "Could not find readSingleBlock function";

    // The dictionary decode check should use localDict, not tlStringDict
    EXPECT_NE(body.find("localDict && !localDict->empty()"), std::string::npos)
        << "Dictionary decode check must use localDict, not tlStringDict";
}
