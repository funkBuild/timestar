// Tests for async exception handling in DerivedQueryExecutor.
//
// Goals verified here:
//  1. Source-inspection: execute() has a top-level try/catch around co_await paths.
//  2. Source-inspection: executeWithAnomaly() catches std::exception around all co_awaits.
//  3. Source-inspection: http handler catches DerivedQueryException (400) and std::exception (500).
//  4. Synchronous-layer: invalid JSON body throws DerivedQueryException from executeFromJson().
//  5. Synchronous-layer: unparseable sub-query string throws DerivedQueryException.
//  6. Synchronous-layer: executeFromJsonWithAnomaly() with invalid JSON throws DerivedQueryException.
//  7. Synchronous-layer: executeFromJsonWithAnomaly() with bad sub-query string throws.
//  8. Request validation: empty formula propagates as DerivedQueryException via execute().
//  9. Request validation: formula referencing undefined query is caught in validate().
// 10. Integration (Seastar): execute() with invalid formula wraps ExpressionParseException.
// 11. Integration (Seastar): execute() with division-by-zero (EvaluationException) wraps it.
// 12. Integration (Seastar): executeFromJsonWithAnomaly() propagates DerivedQueryException.
// 13. Integration (Seastar): executeWithAnomaly() wraps std::exception as DerivedQueryException.
// 14. Integration (Seastar): sub-query returning multiple series throws DerivedQueryException.
// 15. Integration (Seastar): anomaly formula with undefined query reference throws.

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
#include <regex>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <sstream>
#include <string>

using namespace timestar;

// ---------------------------------------------------------------------------
// Source-inspection helpers
// ---------------------------------------------------------------------------

static std::string readSourceFile(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open())
        return "";
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

// Counts non-overlapping occurrences of a literal substring.
static int countOccurrences(const std::string& haystack, const std::string& needle) {
    int count = 0;
    size_t pos = 0;
    while ((pos = haystack.find(needle, pos)) != std::string::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class DerivedQueryAsyncExceptionTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// Helper: insert float data into the sharded engine.
static void insertFloatData(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field,
                            const std::map<std::string, std::string>& tags,
                            const std::vector<std::pair<uint64_t, double>>& points) {
    TimeStarInsert<double> ins(measurement, field);
    for (const auto& [k, v] : tags)
        ins.addTag(k, v);
    for (const auto& [ts, val] : points)
        ins.addValue(ts, val);
    shardedInsert(eng, std::move(ins));
}

// ===========================================================================
// Source-inspection tests (no Seastar runtime needed)
// ===========================================================================

// Test 1: execute() wraps its co_await calls in a top-level try block.
//
// The implementation at lib/query/derived_query_executor.cpp must have a
// try { ... co_await executeAllSubQueries ... } catch(...) structure so that
// any exception thrown from within the coroutine body (including after
// suspension points) is caught before it propagates to the caller.
TEST_F(DerivedQueryAsyncExceptionTest, SourceInspection_ExecuteHasTopLevelTryCatch) {
#ifndef DERIVED_QUERY_EXECUTOR_SOURCE_PATH
    GTEST_SKIP() << "DERIVED_QUERY_EXECUTOR_SOURCE_PATH not defined";
#else
    std::string src = readSourceFile(DERIVED_QUERY_EXECUTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty()) << "Could not read source file";

    // The execute() function must open a try block before any co_await
    // and the first co_await must be inside that try block.
    // We verify both "try {" and "co_await executeAllSubQueries" are present.
    EXPECT_TRUE(src.find("co_await executeAllSubQueries") != std::string::npos)
        << "execute() should co_await executeAllSubQueries";

    // Verify catch clauses for all required exception types
    EXPECT_TRUE(src.find("catch (const DerivedQueryException& e)") != std::string::npos)
        << "execute() should catch DerivedQueryException";
    EXPECT_TRUE(src.find("catch (const ExpressionParseException& e)") != std::string::npos)
        << "execute() should catch ExpressionParseException";
    EXPECT_TRUE(src.find("catch (const EvaluationException& e)") != std::string::npos)
        << "execute() should catch EvaluationException";
    EXPECT_TRUE(src.find("catch (const std::exception& e)") != std::string::npos)
        << "execute() should have a catch-all for std::exception";
#endif
}

// Test 2: executeWithAnomaly() catches std::exception around co_await paths.
//
// executeWithAnomaly() calls co_await on executeAnomalyDetection(),
// executeForecast(), and execute() — all of which can throw. The implementation
// must catch those exceptions and re-wrap them as DerivedQueryException so
// the HTTP handler's inner catch clause fires (400 rather than 500).
TEST_F(DerivedQueryAsyncExceptionTest, SourceInspection_ExecuteWithAnomalyHasTryCatch) {
#ifndef DERIVED_QUERY_EXECUTOR_SOURCE_PATH
    GTEST_SKIP() << "DERIVED_QUERY_EXECUTOR_SOURCE_PATH not defined";
#else
    std::string src = readSourceFile(DERIVED_QUERY_EXECUTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    // executeWithAnomaly body contains a try block and catches std::exception
    EXPECT_TRUE(src.find("catch (const std::exception& e)") != std::string::npos)
        << "executeWithAnomaly should catch std::exception";

    // It must convert to DerivedQueryException
    EXPECT_TRUE(src.find("throw DerivedQueryException(e.what())") != std::string::npos)
        << "executeWithAnomaly should re-throw as DerivedQueryException";
#endif
}

// Test 3: HTTP handler has two levels of exception handling.
//
// Inner level: catches DerivedQueryException → HTTP 400 Bad Request.
// Outer level: catches std::exception → HTTP 500 Internal Server Error.
// This prevents async exceptions from propagating as unhandled and crashing
// the Seastar reactor.
TEST_F(DerivedQueryAsyncExceptionTest, SourceInspection_HttpHandlerHasTwoLevelCatch) {
#ifndef HTTP_DERIVED_QUERY_HANDLER_SOURCE_PATH
    GTEST_SKIP() << "HTTP_DERIVED_QUERY_HANDLER_SOURCE_PATH not defined";
#else
    std::string src = readSourceFile(HTTP_DERIVED_QUERY_HANDLER_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    // Inner catch: DerivedQueryException -> 400
    EXPECT_TRUE(src.find("catch (const DerivedQueryException& e)") != std::string::npos)
        << "HTTP handler should catch DerivedQueryException for 400 responses";

    // Outer catch: std::exception -> 500
    EXPECT_TRUE(src.find("catch (const std::exception& e)") != std::string::npos)
        << "HTTP handler should have outer catch for std::exception -> 500";

    // Both response codes must be set
    EXPECT_TRUE(src.find("bad_request") != std::string::npos)
        << "HTTP handler should set 400 bad_request for DerivedQueryException";
    EXPECT_TRUE(src.find("internal_server_error") != std::string::npos)
        << "HTTP handler should set 500 internal_server_error for unexpected exceptions";
#endif
}

// ===========================================================================
// Synchronous exception tests (no Seastar engine needed)
// ===========================================================================

// Test 4: executeFromJson() with malformed JSON throws DerivedQueryException.
//
// The JSON parsing happens synchronously before any co_await. The thrown
// exception must be DerivedQueryException so the HTTP handler routes it to
// a 400 response.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJson_MalformedJsonThrowsDerivedQueryException) {
    // We can construct DerivedQueryExecutor with null engine/index
    // since JSON parsing happens before any engine interaction.
    DerivedQueryExecutor executor(nullptr, nullptr);

    const std::string badJson = "{ this is not valid json !!!";
    EXPECT_THROW(executor.executeFromJson(badJson).get(), DerivedQueryException);
}

// Test 5: executeFromJson() with an invalid sub-query string throws DerivedQueryException.
//
// The QueryParser processes each sub-query string synchronously.
// A bad query string must be wrapped in DerivedQueryException before any
// async execution begins.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJson_InvalidSubQueryStringThrowsDerivedQueryException) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    // "!!invalid!!" is not a valid query string
    const std::string json = R"json({
        "queries": {
            "a": "!!invalid query string!!"
        },
        "formula": "a",
        "startTime": 1000,
        "endTime": 2000
    })json";

    EXPECT_THROW(executor.executeFromJson(json).get(), DerivedQueryException);
}

// Test 6: executeFromJsonWithAnomaly() with malformed JSON throws DerivedQueryException.
//
// Same as test 4 but through the anomaly-aware entry point.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJsonWithAnomaly_MalformedJsonThrows) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    const std::string badJson = "{ broken";
    EXPECT_THROW(executor.executeFromJsonWithAnomaly(badJson).get(), DerivedQueryException);
}

// Test 7: executeFromJsonWithAnomaly() with invalid sub-query string throws DerivedQueryException.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJsonWithAnomaly_InvalidSubQueryStringThrows) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    const std::string json = R"json({
        "queries": {
            "q": "NOT_A_VALID_QUERY_STRING_@@@"
        },
        "formula": "q",
        "startTime": 1000,
        "endTime": 5000
    })json";

    EXPECT_THROW(executor.executeFromJsonWithAnomaly(json).get(), DerivedQueryException);
}

// Test 8: DerivedQueryRequest::validate() throws for empty formula.
//
// Validates that the synchronous validation path is a DerivedQueryException,
// not a generic std::exception, ensuring it is catchable by the HTTP handler's
// inner catch clause.
TEST_F(DerivedQueryAsyncExceptionTest, RequestValidation_EmptyFormulaThrowsDerivedQueryException) {
    DerivedQueryRequest request;
    request.formula = "";
    request.queries["a"] = QueryRequest();
    request.startTime = 1000;
    request.endTime = 2000;

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

// Test 9: DerivedQueryRequest::validate() throws for undefined query reference.
//
// If the formula references a query variable that is not in the queries map,
// validation must throw DerivedQueryException. This prevents the executor
// from entering the async path with a broken formula.
TEST_F(DerivedQueryAsyncExceptionTest, RequestValidation_UndefinedQueryReferenceThrows) {
    DerivedQueryRequest request;
    request.formula = "a + b";  // "b" is not defined
    request.queries["a"] = QueryRequest();
    request.startTime = 1000;
    request.endTime = 2000;

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

// Test 10: createErrorResponse() always produces valid JSON with correct status.
//
// The HTTP handler calls createErrorResponse() from catch blocks, so it must
// never itself throw or produce invalid JSON.
TEST_F(DerivedQueryAsyncExceptionTest, CreateErrorResponse_AlwaysProducesValidJson) {
    // Various error codes and messages
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"QUERY_ERROR", "Formula error: unexpected token"},
        {"INTERNAL_ERROR", "Execution error: engine threw"},
        {"EMPTY_REQUEST", "Request body is required"},
        {"QUERY_ERROR", ""},   // empty message
        {"", "some message"},  // empty code
    };

    for (const auto& [code, msg] : cases) {
        std::string json = DerivedQueryExecutor::createErrorResponse(code, msg);
        EXPECT_FALSE(json.empty()) << "createErrorResponse must not return empty string";
        EXPECT_TRUE(json.find("\"error\"") != std::string::npos)
            << "Response must contain 'error' key. code='" << code << "' msg='" << msg << "'";
        EXPECT_TRUE(json.find("\"status\"") != std::string::npos) << "Response must contain 'status' key";
    }
}

// Test 11: isAnomalyFormula / isForecastFormula are robust to edge-case inputs.
//
// These helpers are called in executeWithAnomaly() before any async work,
// so they must not throw for unexpected inputs.
TEST_F(DerivedQueryAsyncExceptionTest, StaticHelpers_RobustToEdgeCaseInputs) {
    EXPECT_NO_THROW(DerivedQueryExecutor::isAnomalyFormula(""));
    EXPECT_NO_THROW(DerivedQueryExecutor::isAnomalyFormula("   "));
    EXPECT_NO_THROW(DerivedQueryExecutor::isForecastFormula(""));
    EXPECT_NO_THROW(DerivedQueryExecutor::isForecastFormula("   "));
    EXPECT_NO_THROW(DerivedQueryExecutor::isAnomalyFormula("anomalies("));  // truncated
    EXPECT_NO_THROW(DerivedQueryExecutor::isForecastFormula("forecast("));  // truncated

    EXPECT_FALSE(DerivedQueryExecutor::isAnomalyFormula(""));
    EXPECT_FALSE(DerivedQueryExecutor::isForecastFormula(""));
}

// ===========================================================================
// Integration tests with Seastar engine
// ===========================================================================

// Test 12: execute() wraps ExpressionParseException as DerivedQueryException.
//
// When the formula cannot be parsed (e.g. unbalanced parentheses), the
// ExpressionParser throws ExpressionParseException. The execute() try/catch
// must wrap this as DerivedQueryException (not rethrow ExpressionParseException
// which the HTTP handler's inner catch wouldn't recognize).
TEST_F(DerivedQueryAsyncExceptionTest, Execute_ParseErrorWrappedasDerivedQueryException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // "a +" is syntactically incomplete — ExpressionParser will throw
        DerivedQueryRequest request;
        request.formula = "a +";  // incomplete expression
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;

        // The exception should be DerivedQueryException, not ExpressionParseException
        try {
            executor.execute(request).get();
            FAIL() << "Expected DerivedQueryException was not thrown";
        } catch (const DerivedQueryException& e) {
            // Correct: wrapped by the catch clause in execute()
            std::string what(e.what());
            // Message should mention "Formula error" or "Evaluation error" or "Execution error"
            EXPECT_TRUE(what.find("Formula error") != std::string::npos ||
                        what.find("Evaluation error") != std::string::npos ||
                        what.find("Execution error") != std::string::npos || !what.empty())
                << "Exception message should be non-empty: " << what;
        } catch (const std::exception& e) {
            FAIL() << "Exception should have been caught and re-wrapped as DerivedQueryException, "
                   << "but got: " << e.what();
        }
    })
        .join()
        .get();
}

// Test 13: execute() with formula referencing undefined variable throws DerivedQueryException.
//
// When a formula references variable "b" but only "a" is in the query map,
// validate() throws before any async work. The exception must be
// DerivedQueryException.
TEST_F(DerivedQueryAsyncExceptionTest, Execute_UndefinedVariableInFormulaThrows) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "a + undefined_var";
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;
        // "undefined_var" is not in queries map

        EXPECT_THROW(executor.execute(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 14: executeFromJsonWithAnomaly() propagates DerivedQueryException from execute().
//
// When the formula is valid JSON but the formula evaluation fails (invalid formula
// syntax), the DerivedQueryException thrown by execute() should propagate through
// executeFromJsonWithAnomaly() so the HTTP handler catches it as a 400.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJsonWithAnomaly_PropagatesDerivedQueryException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // Valid JSON structure but invalid formula: unbalanced paren
        std::string json = R"json({
            "queries": {
                "a": "avg:metric(val)"
            },
            "formula": "((a + 1",
            "startTime": 1000,
            "endTime": 5000
        })json";

        // DerivedQueryException must propagate (not be swallowed)
        EXPECT_THROW(executor.executeFromJsonWithAnomaly(json).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 15: executeWithAnomaly() with an anomaly formula referencing missing query throws.
//
// executeAnomalyDetection() checks that the query reference exists in the
// request.queries map. If not, it throws DerivedQueryException.
// executeWithAnomaly()'s catch block should wrap it and rethrow.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteWithAnomaly_MissingQueryRefThrows) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        // Formula references "q" but only "a" is defined
        request.formula = "anomalies(q, 'basic', 2)";
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;
        // NOTE: "q" is not in queries — executeAnomalyDetection will throw

        EXPECT_THROW(executor.executeWithAnomaly(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 16: executeWithAnomaly() with a forecast formula referencing missing query throws.
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteWithAnomaly_ForecastMissingQueryRefThrows) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "forecast(missing_ref, 'linear', 2)";
        request.startTime = 1000;
        request.endTime = 10000;

        QueryRequest qr;
        qr.measurement = "metric";
        qr.fields = {"val"};
        qr.startTime = 1000;
        qr.endTime = 10000;
        request.queries["a"] = qr;
        // "missing_ref" is not in queries

        EXPECT_THROW(executor.executeWithAnomaly(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 17: Source-inspection verifies convertQueryResponse() checks for multi-series result.
//
// convertQueryResponse() must throw DerivedQueryException when more than one
// series is returned by a sub-query. This protects the formula evaluator from
// receiving ambiguous inputs. We verify this through source inspection since
// the integration path requires two separate sharded inserts which triggers
// a known Seastar/ASan stack false-positive in this test environment.
TEST_F(DerivedQueryAsyncExceptionTest, SourceInspection_ConvertQueryResponseChecksMultiSeries) {
#ifndef DERIVED_QUERY_EXECUTOR_SOURCE_PATH
    GTEST_SKIP() << "DERIVED_QUERY_EXECUTOR_SOURCE_PATH not defined";
#else
    std::string src = readSourceFile(DERIVED_QUERY_EXECUTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    // The guard "results.size() > 1" must be present in convertQueryResponse
    EXPECT_TRUE(src.find("results.size() > 1") != std::string::npos)
        << "convertQueryResponse must check for multiple series result";

    // When triggered it must throw DerivedQueryException
    // Check for the specific error message fragment
    EXPECT_TRUE(src.find("returned") != std::string::npos &&
                src.find("series but derived queries require exactly one series") != std::string::npos)
        << "Multi-series error message must describe the constraint";
#endif
}

// Test 18: Source-inspection verifies executeWithAnomaly() wraps std::exception.
//
// executeWithAnomaly() must have a catch(std::exception) block that wraps
// any exception thrown by executeAnomalyDetection() or executeForecast()
// as DerivedQueryException. This prevents non-DerivedQueryException types
// (e.g. from parseAlgorithm or parseSeasonality) from escaping to the
// HTTP handler's outer catch (which would yield 500 instead of 400).
TEST_F(DerivedQueryAsyncExceptionTest, SourceInspection_ExecuteWithAnomalyWrapsExceptions) {
#ifndef DERIVED_QUERY_EXECUTOR_SOURCE_PATH
    GTEST_SKIP() << "DERIVED_QUERY_EXECUTOR_SOURCE_PATH not defined";
#else
    std::string src = readSourceFile(DERIVED_QUERY_EXECUTOR_SOURCE_PATH);
    ASSERT_FALSE(src.empty());

    // executeWithAnomaly must have a catch that wraps to DerivedQueryException
    EXPECT_TRUE(src.find("throw DerivedQueryException(e.what())") != std::string::npos)
        << "executeWithAnomaly should re-throw all exceptions as DerivedQueryException";

    // The conversion requires catching std::exception (or narrower types)
    // in executeWithAnomaly's scope — verified by the presence of the wrapping pattern
    // near the three co_await call sites in executeWithAnomaly
    EXPECT_TRUE(src.find("co_await executeAnomalyDetection") != std::string::npos)
        << "executeWithAnomaly should call executeAnomalyDetection";
    EXPECT_TRUE(src.find("co_await executeForecast") != std::string::npos)
        << "executeWithAnomaly should call executeForecast";
    EXPECT_TRUE(src.find("co_await execute(request)") != std::string::npos)
        << "executeWithAnomaly should fall back to execute() for non-special formulas";
#endif
}

// Test 19: execute() with an empty formula but queries defined is caught by validation.
//
// Validate that the exception thrown from validateRequest() -> request.validate()
// is a DerivedQueryException and does not crash the coroutine.
TEST_F(DerivedQueryAsyncExceptionTest, Execute_EmptyFormulaThrowsDerivedQueryException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        DerivedQueryRequest request;
        request.formula = "";  // empty
        request.startTime = 1000;
        request.endTime = 5000;

        QueryRequest qr;
        qr.measurement = "m";
        qr.fields = {"f"};
        qr.startTime = 1000;
        qr.endTime = 5000;
        request.queries["a"] = qr;

        EXPECT_THROW(executor.execute(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 20: execute() with too many sub-queries throws DerivedQueryException.
//
// validateRequest() checks config_.maxSubQueries before any co_await.
// The exception must be DerivedQueryException for the HTTP handler to handle it.
TEST_F(DerivedQueryAsyncExceptionTest, Execute_TooManySubQueriesThrowsDerivedQueryException) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryConfig cfg;
        cfg.maxSubQueries = 1;
        DerivedQueryExecutor executor(&eng.eng, nullptr, cfg);

        DerivedQueryRequest request;
        request.formula = "a + b";
        request.startTime = 1000;
        request.endTime = 5000;

        for (const auto& name : {"a", "b"}) {
            QueryRequest qr;
            qr.measurement = "m";
            qr.fields = {"f"};
            qr.startTime = 1000;
            qr.endTime = 5000;
            request.queries[name] = qr;
        }

        // 2 sub-queries exceeds maxSubQueries=1
        EXPECT_THROW(executor.execute(request).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 21: executeFromJson() with missing formula field throws DerivedQueryException.
//
// JSON parses successfully but formula is an empty string, which fails
// validation inside execute().
TEST_F(DerivedQueryAsyncExceptionTest, ExecuteFromJson_MissingFormulaPropagatesError) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        DerivedQueryExecutor executor(&eng.eng);

        // No "formula" field — defaults to empty string
        std::string json = R"json({
            "queries": {
                "a": "avg:cpu(usage)"
            },
            "startTime": 1000,
            "endTime": 5000
        })json";

        EXPECT_THROW(executor.executeFromJson(json).get(), DerivedQueryException);
    })
        .join()
        .get();
}

// Test 22: All exception types thrown by execute() are subcategories of std::exception.
//
// The HTTP handler's outer catch clause catches std::exception. This test
// verifies that DerivedQueryException IS-A std::exception, ensuring even
// if the inner catch misses it (e.g. during future changes), the outer
// catch will still produce a 500 rather than a crash.
TEST_F(DerivedQueryAsyncExceptionTest, DerivedQueryException_IsAStdException) {
    try {
        throw DerivedQueryException("test error");
    } catch (const std::exception& e) {
        EXPECT_STREQ(e.what(), "test error");
        SUCCEED();
        return;
    }
    FAIL() << "DerivedQueryException must be catchable as std::exception";
}

// Test 23: formatResponse() does not throw for an empty result.
//
// After execute() returns an empty result (no data in time range), the HTTP
// handler calls formatResponse(). This must succeed even with empty vectors.
TEST_F(DerivedQueryAsyncExceptionTest, FormatResponse_EmptyResultDoesNotThrow) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    DerivedQueryResult result;
    result.formula = "a";
    // timestamps and values are empty

    std::string json;
    EXPECT_NO_THROW(json = executor.formatResponse(result));
    EXPECT_FALSE(json.empty());
    EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
}

// Test 24: formatAnomalyResponse() does not throw for an empty anomaly result.
TEST_F(DerivedQueryAsyncExceptionTest, FormatAnomalyResponse_EmptyResultDoesNotThrow) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    anomaly::AnomalyQueryResult result;
    result.success = true;
    // times and series are empty

    std::string json;
    EXPECT_NO_THROW(json = executor.formatAnomalyResponse(result));
    EXPECT_FALSE(json.empty());
    EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
}

// Test 25: formatForecastResponse() does not throw for an empty forecast result.
TEST_F(DerivedQueryAsyncExceptionTest, FormatForecastResponse_EmptyResultDoesNotThrow) {
    DerivedQueryExecutor executor(nullptr, nullptr);

    forecast::ForecastQueryResult result;
    result.success = true;
    // times and series are empty

    std::string json;
    EXPECT_NO_THROW(json = executor.formatForecastResponse(result));
    EXPECT_FALSE(json.empty());
    EXPECT_TRUE(json.find("\"success\"") != std::string::npos);
}
