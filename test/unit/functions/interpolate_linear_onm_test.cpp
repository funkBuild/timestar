#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #23: FunctionUtils::interpolateLinear O(n*m) regression
//
// The inner search index `size_t i = 0` was declared inside the outer loop,
// resetting to 0 on every target timestamp. For sorted target timestamps,
// the search should maintain its position across iterations for O(n+m).
// The fix hoists the index declaration before the outer loop.
// =============================================================================

class InterpolateLinearOnmTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef FUNCTION_TYPES_SOURCE_PATH
        std::ifstream file(FUNCTION_TYPES_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/functions/function_types.hpp",
            "../../lib/functions/function_types.hpp",
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

    // Extract the interpolateLinear function body
    std::string extractInterpolateLinear() const {
        auto pos = sourceCode.find("interpolateLinear");
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

TEST_F(InterpolateLinearOnmTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load function_types.hpp";
}

TEST_F(InterpolateLinearOnmTest, SearchIndexDeclaredBeforeOuterLoop) {
    std::string body = extractInterpolateLinear();
    ASSERT_FALSE(body.empty()) << "Could not find interpolateLinear function";

    // The search index must be declared BEFORE the for loop over targetTimestamps.
    // Find "size_t i = 0" or equivalent search position initialization
    auto indexDeclPos = body.find("size_t i = 0");
    auto forLoopPos = body.find("for (uint64_t targetTime");
    if (forLoopPos == std::string::npos) {
        forLoopPos = body.find("for (auto targetTime");
    }

    ASSERT_NE(forLoopPos, std::string::npos)
        << "Could not find the outer loop over targetTimestamps";
    ASSERT_NE(indexDeclPos, std::string::npos)
        << "Could not find search index declaration (size_t i = 0)";

    EXPECT_LT(indexDeclPos, forLoopPos)
        << "Search index 'size_t i = 0' must be declared BEFORE the outer loop "
        << "to maintain position across iterations (O(n+m) instead of O(n*m))";
}

TEST_F(InterpolateLinearOnmTest, NoResetInsideLoop) {
    std::string body = extractInterpolateLinear();
    ASSERT_FALSE(body.empty()) << "Could not find interpolateLinear function";

    // Find the for loop body and verify there's no "size_t i = 0" inside it
    auto forLoopPos = body.find("for (uint64_t targetTime");
    if (forLoopPos == std::string::npos) {
        forLoopPos = body.find("for (auto targetTime");
    }
    ASSERT_NE(forLoopPos, std::string::npos);

    // Everything after the for loop start
    auto loopBody = body.substr(forLoopPos);

    // Find the opening brace of the for loop
    auto bracePos = loopBody.find('{');
    ASSERT_NE(bracePos, std::string::npos);
    auto insideLoop = loopBody.substr(bracePos + 1);

    // There should be no "size_t i = 0" inside the loop body
    // (the while loop uses i but shouldn't redeclare it)
    auto resetPos = insideLoop.find("size_t i = 0");
    EXPECT_EQ(resetPos, std::string::npos)
        << "Found 'size_t i = 0' inside the loop body — this resets the search index "
        << "on every iteration, causing O(n*m) complexity";
}
