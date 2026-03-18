#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #11: Out-of-bounds access on empty timestamps vector
//
// In coalesceWrites, the "timestamps" JSON key may exist as an array where
// all elements are non-numeric, leaving the timestamps vector empty after
// parsing. Later code accesses timestamps[0] without checking, causing UB.
// The fix adds a guard: if timestamps is empty after parsing, push the
// default timestamp.
// =============================================================================

class HttpWriteEmptyTimestampsTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef HTTP_WRITE_HANDLER_SOURCE_PATH
        std::ifstream file(HTTP_WRITE_HANDLER_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/http/http_write_handler.cpp",
            "../../lib/http/http_write_handler.cpp",
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

    // Extract the coalesceWrites function body
    std::string extractCoalesceWrites() const {
        auto pos = sourceCode.find("coalesceWrites");
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

TEST_F(HttpWriteEmptyTimestampsTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load http_write_handler.cpp source file";
}

TEST_F(HttpWriteEmptyTimestampsTest, EmptyTimestampsGuardExists) {
    std::string body = extractCoalesceWrites();
    ASSERT_FALSE(body.empty()) << "Could not find coalesceWrites function";

    // Find the timestamp parsing section (marked by "Extract timestamps")
    auto parseSection = body.find("Extract timestamps");
    ASSERT_NE(parseSection, std::string::npos)
        << "Could not find timestamp parsing section in coalesceWrites";

    // After the parsing section, before "Extract tags", there should be the guard
    auto tagsSection = body.find("Extract tags", parseSection);
    ASSERT_NE(tagsSection, std::string::npos)
        << "Could not find tags section after timestamp parsing";

    // The guard must exist between timestamp parsing and tag extraction
    auto region = body.substr(parseSection, tagsSection - parseSection);
    EXPECT_NE(region.find("timestamps.empty()"), std::string::npos)
        << "coalesceWrites must check for empty timestamps after parsing, before tag extraction";
}

TEST_F(HttpWriteEmptyTimestampsTest, GuardPushesDefaultTimestamp) {
    std::string body = extractCoalesceWrites();
    ASSERT_FALSE(body.empty()) << "Could not find coalesceWrites function";

    // Find the guard block and verify it pushes the default
    auto guardPos = body.find("timestamps.empty()");
    ASSERT_NE(guardPos, std::string::npos);

    // Within 200 chars of the guard, there should be a push_back of defaultTimestampNs
    auto region = body.substr(guardPos, 200);
    EXPECT_NE(region.find("defaultTimestampNs"), std::string::npos)
        << "The empty-timestamps guard must push defaultTimestampNs";
}
