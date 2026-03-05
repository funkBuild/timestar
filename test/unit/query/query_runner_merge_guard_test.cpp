#include <gtest/gtest.h>
#include <fstream>
#include <string>

// =============================================================================
// Source-code inspection tests: verify the bestIdx == SIZE_MAX guard is present
// in QueryRunner's mergeSmallNSpans function.
//
// The bug: mergeSmallNSpans initialises bestIdx to SIZE_MAX as a sentinel for
// "no valid span found".  The while loop condition is `activeCount > 0`, but
// activeCount might theoretically be out of sync with the actual exhaustion
// state of all spans.  If every span is exhausted but the loop body is still
// entered, the inner min-finding loop will not update bestIdx (all spans are
// skipped via `continue`), leaving bestIdx == SIZE_MAX.  The code then
// evaluates `spans[SIZE_MAX].val()` — an out-of-bounds access / UB.
//
// The fix: add `if (bestIdx == SIZE_MAX) break;` immediately after the
// min-finding loop and before the first use of bestIdx.
// =============================================================================

class QueryRunnerMergeGuardTest : public ::testing::Test {
protected:
    std::string sourceCode;
    // The extracted mergeSmallNSpans function body
    std::string funcBody;

    void SetUp() override {
        std::ifstream file(QUERY_RUNNER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open())
            << "Could not open query_runner.cpp at: " << QUERY_RUNNER_SOURCE_PATH;
        sourceCode.assign(
            std::istreambuf_iterator<char>(file),
            std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());

        // Extract the mergeSmallNSpans function body.
        auto funcPos = sourceCode.find("mergeSmallNSpans(");
        ASSERT_NE(funcPos, std::string::npos)
            << "Expected mergeSmallNSpans in query_runner.cpp";

        // Find the opening brace of the function body.
        auto openBrace = sourceCode.find('{', funcPos);
        ASSERT_NE(openBrace, std::string::npos);

        // Brace-match to find the closing brace.
        int depth = 1;
        size_t pos = openBrace + 1;
        while (pos < sourceCode.size() && depth > 0) {
            if (sourceCode[pos] == '{') depth++;
            else if (sourceCode[pos] == '}') depth--;
            pos++;
        }
        ASSERT_EQ(depth, 0) << "Unmatched braces in mergeSmallNSpans";

        funcBody = sourceCode.substr(openBrace, pos - openBrace);
    }
};

// Verify that the bestIdx == SIZE_MAX guard is present in mergeSmallNSpans.
// Without this guard, if all spans are exhausted but activeCount is stale,
// the code accesses spans[SIZE_MAX] — an out-of-bounds access.
TEST_F(QueryRunnerMergeGuardTest, BestIdxSentinelGuardPresent) {
    // The guard must check for the SIZE_MAX sentinel before using bestIdx.
    // Acceptable forms:
    //   if (bestIdx == SIZE_MAX) break;
    bool hasGuard =
        funcBody.find("bestIdx == SIZE_MAX") != std::string::npos ||
        funcBody.find("SIZE_MAX == bestIdx") != std::string::npos;

    EXPECT_TRUE(hasGuard)
        << "mergeSmallNSpans is missing the 'if (bestIdx == SIZE_MAX) break;' "
        << "guard before 'spans[bestIdx].val()'. "
        << "If activeCount is stale and all spans are actually exhausted, "
        << "the inner min-finding loop will leave bestIdx == SIZE_MAX, "
        << "causing an out-of-bounds access on 'spans[SIZE_MAX]'.";
}

// Verify the guard appears BEFORE the first use of bestIdx as a vector index.
// The guard is only meaningful if it breaks out before the dangerous access.
TEST_F(QueryRunnerMergeGuardTest, GuardAppearsBeforeSpansBestIdxAccess) {
    auto guardPos = funcBody.find("bestIdx == SIZE_MAX");
    if (guardPos == std::string::npos) {
        guardPos = funcBody.find("SIZE_MAX == bestIdx");
    }
    ASSERT_NE(guardPos, std::string::npos)
        << "Guard not found; cannot verify placement (see BestIdxSentinelGuardPresent).";

    // Find the first use of spans[bestIdx] after the guard.
    auto accessPos = funcBody.find("spans[bestIdx]", guardPos);
    ASSERT_NE(accessPos, std::string::npos)
        << "Expected 'spans[bestIdx]' to appear after the SIZE_MAX guard.";

    EXPECT_LT(guardPos, accessPos)
        << "The 'bestIdx == SIZE_MAX' guard must appear BEFORE 'spans[bestIdx]' "
        << "to be effective. The guard protects the vector access from "
        << "an out-of-bounds index when no active span was found.";
}

// Verify the guard uses a 'break' statement so the while loop terminates
// when no active span is found (as opposed to just skipping the emit step).
TEST_F(QueryRunnerMergeGuardTest, GuardUsesBreakToTerminateLoop) {
    auto guardPos = funcBody.find("bestIdx == SIZE_MAX");
    if (guardPos == std::string::npos) {
        guardPos = funcBody.find("SIZE_MAX == bestIdx");
    }
    ASSERT_NE(guardPos, std::string::npos)
        << "Guard not found; cannot verify break statement.";

    // After the guard check, within the next ~50 characters, expect 'break'
    auto nearGuard = funcBody.substr(guardPos, 100);
    EXPECT_NE(nearGuard.find("break"), std::string::npos)
        << "The SIZE_MAX guard should use 'break' to exit the while loop. "
        << "If no active span is found (bestIdx == SIZE_MAX), the merge is "
        << "complete and the loop should terminate immediately.";
}
