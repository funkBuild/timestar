#include <gtest/gtest.h>

#include <fstream>
#include <regex>
#include <string>

// =============================================================================
// Source code inspection tests: verify the parallel_for_each race condition fix
// in QueryRunner::queryTsm is in place.
//
// The bug: parallel_for_each launches coroutines that all push_back to a shared
// mutable vector. Even in Seastar's cooperative scheduling model, this is unsafe
// because reallocation during one coroutine's push_back can invalidate references
// held by other suspended coroutines.
//
// The fix: pre-allocate a vector of std::optional<TSMResult<T>> with one slot
// per file, where each coroutine writes to its own indexed slot.
// =============================================================================

class QueryRunnerRaceTest : public ::testing::Test {
protected:
    std::string sourceCode;
    // The extracted parallel_for_each lambda body (between the lambda's { and
    // the closing of the parallel_for_each call)
    std::string pfeBody;

    void SetUp() override {
        std::ifstream file(QUERY_RUNNER_SOURCE_PATH);
        ASSERT_TRUE(file.is_open()) << "Could not open query_runner.cpp at: " << QUERY_RUNNER_SOURCE_PATH;
        sourceCode.assign(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>());
        ASSERT_FALSE(sourceCode.empty());

        // Extract the parallel_for_each lambda body by brace-matching.
        // We find "parallel_for_each" then the lambda's opening '{' and
        // match braces to locate its closing '}'.
        auto pfePos = sourceCode.find("parallel_for_each");
        ASSERT_NE(pfePos, std::string::npos) << "Expected parallel_for_each in query_runner.cpp";

        // Find the lambda opening brace: look for "-> seastar::future<> {" pattern
        // which is the opening of the lambda body
        auto lambdaSig = sourceCode.find("-> seastar::future<>", pfePos);
        ASSERT_NE(lambdaSig, std::string::npos) << "Expected lambda signature in parallel_for_each";

        auto lambdaOpen = sourceCode.find('{', lambdaSig);
        ASSERT_NE(lambdaOpen, std::string::npos);

        // Brace-match to find the closing brace
        int depth = 1;
        size_t pos = lambdaOpen + 1;
        while (pos < sourceCode.size() && depth > 0) {
            if (sourceCode[pos] == '{')
                depth++;
            else if (sourceCode[pos] == '}')
                depth--;
            pos++;
        }
        ASSERT_EQ(depth, 0) << "Unmatched braces in parallel_for_each lambda";

        pfeBody = sourceCode.substr(lambdaOpen, pos - lambdaOpen);
    }
};

// Verify that tsmResults.push_back / tsmResults.emplace_back does NOT appear
// inside the parallel_for_each lambda. The safe pattern uses indexed slot assignment.
TEST_F(QueryRunnerRaceTest, NoSharedMutableVectorInParallelForEach) {
    // The unsafe pattern: tsmResults.push_back or tsmResults.emplace_back
    EXPECT_EQ(pfeBody.find("tsmResults.push_back"), std::string::npos)
        << "Found 'tsmResults.push_back' inside parallel_for_each lambda. "
        << "This is a race condition - concurrent coroutines can push_back "
        << "to the shared vector after co_await suspension points. "
        << "Use pre-allocated indexed slots (std::optional) instead.";

    EXPECT_EQ(pfeBody.find("tsmResults.emplace_back"), std::string::npos)
        << "Found 'tsmResults.emplace_back' inside parallel_for_each lambda. "
        << "This is a race condition - use pre-allocated indexed slots instead.";
}

// Verify the code uses the pre-allocated slots pattern (std::optional)
TEST_F(QueryRunnerRaceTest, UsesPreAllocatedSlots) {
    // The fix should use std::optional for pre-allocated slots
    EXPECT_NE(sourceCode.find("optional"), std::string::npos)
        << "Expected std::optional usage in query_runner.cpp for "
        << "pre-allocated slot pattern to avoid concurrent push_back.";

    // Should include the <optional> header
    EXPECT_NE(sourceCode.find("#include <optional>"), std::string::npos)
        << "Expected '#include <optional>' in query_runner.cpp.";

    // The slot vector should be sized to match the number of TSM files
    EXPECT_NE(sourceCode.find("tsmSlots"), std::string::npos)
        << "Expected 'tsmSlots' vector for pre-allocated indexed slots.";
}

// Verify that index assignment happens BEFORE any co_await in the lambda.
// In Seastar's cooperative scheduling, parallel_for_each invokes the lambda
// sequentially before any suspension, so the index assignment is safe as long
// as it occurs before the first co_await.
TEST_F(QueryRunnerRaceTest, IndexAssignmentBeforeCoAwait) {
    // Find the index assignment pattern (myIdx = slotIdx++ or similar)
    auto idxAssignPos = pfeBody.find("myIdx");
    ASSERT_NE(idxAssignPos, std::string::npos)
        << "Expected 'myIdx' index variable assignment in parallel_for_each lambda.";

    // Find the first co_await in the lambda body
    auto firstCoAwait = pfeBody.find("co_await");
    ASSERT_NE(firstCoAwait, std::string::npos) << "Expected co_await in the parallel_for_each lambda.";

    // The index assignment must come BEFORE the first co_await
    EXPECT_LT(idxAssignPos, firstCoAwait) << "Index assignment (myIdx) must occur BEFORE the first co_await "
                                          << "to be safe in Seastar's cooperative scheduling. "
                                          << "parallel_for_each invokes the lambda sequentially before suspension, "
                                          << "so the index increment is safe only if it precedes co_await.";
}
