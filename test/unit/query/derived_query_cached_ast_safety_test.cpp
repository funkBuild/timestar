// Tests for the cachedAst_ dangling-pointer bug fix in DerivedQueryExecutor.
//
// Bug: In executeWithAnomaly(), cachedAst_ was set to ast.get() before calling
// execute(request). If execute() threw, cachedAst_ was not reset to nullptr in
// the catch block. When the local unique_ptr<ExpressionNode> (ast) was destroyed
// on stack unwind, cachedAst_ became a dangling pointer. Subsequent calls to
// execute() would dereference freed memory (UB).
//
// Fix: A scope guard (RAII struct) now ensures cachedAst_ is always reset to
// nullptr when the scope exits, whether normally or via exception.
//
// Tests:
//  1. Source-inspection: executeWithAnomaly uses a scope guard for cachedAst_.
//  2. Integration: cachedAst_ is nullptr after executeWithAnomaly throws.
//  3. Integration: executor is safely reusable after executeWithAnomaly throws.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/query/derived_query.hpp"
#include "../../../lib/query/derived_query_executor.hpp"
#include "../../../lib/query/expression_parser.hpp"
#include "../../seastar_gtest.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <fstream>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <sstream>
#include <string>

using namespace timestar;

// ---------------------------------------------------------------------------
// Source-inspection helper
// ---------------------------------------------------------------------------

static std::string readSource(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) return "";
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// ---------------------------------------------------------------------------
// Fixture with friend access to DerivedQueryExecutor::cachedAst_
// The class must live in namespace timestar to match the friend declaration.
// ---------------------------------------------------------------------------

namespace timestar {

class DerivedQueryCachedAstSafetyTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }

    // Access the private cachedAst_ member via friend privilege.
    static const ExpressionNode* getCachedAst(const DerivedQueryExecutor& executor) { return executor.cachedAst_; }
};

}  // namespace timestar

using timestar::DerivedQueryCachedAstSafetyTest;

// ---------------------------------------------------------------------------
// Test 1: Source-inspection — scope guard protects cachedAst_
// ---------------------------------------------------------------------------

TEST_F(DerivedQueryCachedAstSafetyTest, SourceInspection_ScopeGuardProtectsCachedAst) {
#ifndef DERIVED_QUERY_EXECUTOR_SOURCE_PATH
    GTEST_SKIP() << "DERIVED_QUERY_EXECUTOR_SOURCE_PATH not defined";
#else
    std::string src = readSource(DERIVED_QUERY_EXECUTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read source file";

    // The old bug pattern was: set cachedAst_, co_await execute(), then manually
    // reset to nullptr — with no cleanup on exception. The fix must use an RAII
    // guard so that cachedAst_ is reset on both normal and exceptional paths.

    // Verify the scope guard pattern exists near the cachedAst_ assignment
    EXPECT_TRUE(src.find("~CachedAstGuard()") != std::string::npos ||
                src.find("scope_guard") != std::string::npos ||
                src.find("ScopeGuard") != std::string::npos)
        << "executeWithAnomaly must use an RAII scope guard to reset cachedAst_";

    // Verify the guard resets cachedAst_ to nullptr
    EXPECT_TRUE(src.find("ref = nullptr") != std::string::npos || src.find("cachedAst_ = nullptr") != std::string::npos)
        << "The scope guard destructor must reset cachedAst_ to nullptr";
#endif
}

// ---------------------------------------------------------------------------
// Test 2: cachedAst_ is nullptr after executeWithAnomaly throws
// ---------------------------------------------------------------------------

TEST_F(DerivedQueryCachedAstSafetyTest, CachedAstIsNullptrAfterException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // Verify cachedAst_ starts as nullptr
        ASSERT_EQ(getCachedAst(executor), nullptr);

        // Construct a request with a valid (non-anomaly, non-forecast) formula
        // that will cause execute() to fail. An incomplete formula like "a +"
        // will trigger ExpressionParseException inside execute(), which
        // executeWithAnomaly() catches and rethrows as DerivedQueryException.
        DerivedQueryRequest request;
        request.formula = "a +";  // incomplete expression — parse will fail
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;

        // The call must throw DerivedQueryException
        EXPECT_THROW(executor.executeWithAnomaly(request).get(), DerivedQueryException);

        // After the exception, cachedAst_ MUST be nullptr (the bug left it dangling)
        EXPECT_EQ(getCachedAst(executor), nullptr)
            << "cachedAst_ must be nullptr after executeWithAnomaly throws — "
               "dangling pointer bug is present if this fails";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// Test 3: Executor is safely reusable after executeWithAnomaly throws
// ---------------------------------------------------------------------------

TEST_F(DerivedQueryCachedAstSafetyTest, ExecutorReusableAfterException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // First call: will throw due to bad formula
        {
            DerivedQueryRequest request;
            request.formula = "a +";  // bad formula
            request.startTime = 1000;
            request.endTime = 5000;

            QueryRequest qr;
            qr.measurement = "metric";
            qr.fields = {"val"};
            qr.startTime = 1000;
            qr.endTime = 5000;
            request.queries["a"] = qr;

            EXPECT_THROW(executor.executeWithAnomaly(request).get(), DerivedQueryException);
        }

        // Second call: should work without UB. Use a valid formula with no data
        // — execute() will return an empty result (no crash, no ASAN violation).
        {
            DerivedQueryRequest request;
            request.formula = "a";  // valid formula
            request.startTime = 1000;
            request.endTime = 5000;

            QueryRequest qr;
            qr.measurement = "nonexistent_metric";
            qr.fields = {"val"};
            qr.startTime = 1000;
            qr.endTime = 5000;
            request.queries["a"] = qr;

            // This must not crash or trigger ASAN. With the bug, cachedAst_
            // would be a dangling pointer and execute() at line 151 would
            // dereference freed memory.
            DerivedQueryResultVariant result;
            EXPECT_NO_THROW(result = executor.executeWithAnomaly(request).get());

            // Verify we got a valid (empty) result
            ASSERT_TRUE(std::holds_alternative<DerivedQueryResult>(result));
            auto& derivedResult = std::get<DerivedQueryResult>(result);
            EXPECT_TRUE(derivedResult.timestamps.empty());
        }

        // cachedAst_ must still be nullptr after successful call
        EXPECT_EQ(getCachedAst(executor), nullptr);
    })
        .join()
        .get();
}
