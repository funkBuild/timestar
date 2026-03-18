#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #20: Tombstone file descriptor leak on I/O errors in load()
//
// The old catch block used catch(const std::exception&) and re-threw without
// closing the file descriptor first. Since co_await cannot be used inside
// a catch block (GCC 14 limitation), the fix uses the exception_ptr pattern:
// catch(...) captures the exception, then the fd is closed outside the catch
// block before rethrowing.
// =============================================================================

class TombstoneFdLeakTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef TSM_TOMBSTONE_SOURCE_PATH
        std::ifstream file(TSM_TOMBSTONE_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/storage/tsm_tombstone.cpp",
            "../../lib/storage/tsm_tombstone.cpp",
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

    std::string extractLoadFunction() const {
        auto pos = sourceCode.find("TSMTombstone::load()");
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

TEST_F(TombstoneFdLeakTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load tsm_tombstone.cpp source file";
}

TEST_F(TombstoneFdLeakTest, UsesCatchAllNotCatchException) {
    std::string body = extractLoadFunction();
    ASSERT_FALSE(body.empty()) << "Could not find TSMTombstone::load() function";

    // The catch block must use catch(...) to capture all exceptions,
    // not catch(const std::exception&) which misses non-std exceptions
    // and previously re-threw without closing the fd.
    EXPECT_NE(body.find("catch (...)"), std::string::npos)
        << "TSMTombstone::load() must use catch(...) for exception-safe fd cleanup";
}

TEST_F(TombstoneFdLeakTest, UsesExceptionPtrPattern) {
    std::string body = extractLoadFunction();
    ASSERT_FALSE(body.empty()) << "Could not find TSMTombstone::load() function";

    // Must capture exception with current_exception and rethrow later
    EXPECT_NE(body.find("current_exception"), std::string::npos)
        << "TSMTombstone::load() must capture exception with std::current_exception()";
    EXPECT_NE(body.find("rethrow_exception"), std::string::npos)
        << "TSMTombstone::load() must rethrow with std::rethrow_exception()";
}

TEST_F(TombstoneFdLeakTest, ClosesFileOutsideCatch) {
    std::string body = extractLoadFunction();
    ASSERT_FALSE(body.empty()) << "Could not find TSMTombstone::load() function";

    // The close() call must appear AFTER the catch block, not inside it
    auto catchPos = body.find("catch (...)");
    ASSERT_NE(catchPos, std::string::npos);

    // Find the close() that is outside the catch block
    // It should use co_await close() after the catch block ends
    auto afterCatch = body.substr(catchPos);
    // Find closing brace of catch block
    auto catchBrace = afterCatch.find('{');
    ASSERT_NE(catchBrace, std::string::npos);
    int depth = 1;
    size_t pos = catchBrace + 1;
    while (pos < afterCatch.size() && depth > 0) {
        if (afterCatch[pos] == '{') depth++;
        else if (afterCatch[pos] == '}') depth--;
        pos++;
    }
    auto afterCatchBlock = afterCatch.substr(pos);

    EXPECT_NE(afterCatchBlock.find("co_await close()"), std::string::npos)
        << "co_await close() must be called AFTER the catch block to ensure fd cleanup";
}

TEST_F(TombstoneFdLeakTest, DeclaresExceptionPtrBeforeTry) {
    std::string body = extractLoadFunction();
    ASSERT_FALSE(body.empty()) << "Could not find TSMTombstone::load() function";

    // exception_ptr must be declared before the try block
    auto exPtrPos = body.find("exception_ptr");
    auto tryPos = body.find("try {");
    if (tryPos == std::string::npos) tryPos = body.find("try\n");
    ASSERT_NE(exPtrPos, std::string::npos);
    ASSERT_NE(tryPos, std::string::npos);
    EXPECT_LT(exPtrPos, tryPos)
        << "std::exception_ptr must be declared before the try block";
}
