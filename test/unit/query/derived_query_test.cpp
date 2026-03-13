#include "derived_query.hpp"

#include "query_parser.hpp"

#include <gtest/gtest.h>

using namespace timestar;

class DerivedQueryTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}

    QueryRequest makeSimpleQuery(const std::string& measurement, const std::string& field) {
        QueryRequest req;
        req.measurement = measurement;
        req.fields.push_back(field);
        req.aggregation = AggregationMethod::AVG;
        req.startTime = 1000000000;
        req.endTime = 2000000000;
        return req;
    }
};

// ==================== DerivedQueryRequest Tests ====================

TEST_F(DerivedQueryTest, BasicValidation) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.queries["b"] = makeSimpleQuery("cpu", "idle");
    request.formula = "a + b";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, ValidateEmptyQueries) {
    DerivedQueryRequest request;
    request.formula = "a + b";

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryTest, ValidateEmptyFormula) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.formula = "";

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryTest, ValidateInvalidFormula) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.formula = "a + + b";  // Invalid syntax

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryTest, ValidateUndefinedQueryReference) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.formula = "a + b";  // 'b' is not defined

    EXPECT_THROW(request.validate(), DerivedQueryException);
}

TEST_F(DerivedQueryTest, ValidateComplexFormula) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.queries["b"] = makeSimpleQuery("cpu", "idle");
    request.queries["c"] = makeSimpleQuery("cpu", "system");
    request.formula = "(a / (a + b + c)) * 100";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, ValidateFormulaWithFunctions) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.queries["b"] = makeSimpleQuery("cpu", "idle");
    request.formula = "max(a, b)";

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, GetReferencedQueries) {
    DerivedQueryRequest request;
    request.queries["a"] = makeSimpleQuery("cpu", "usage");
    request.queries["b"] = makeSimpleQuery("cpu", "idle");
    request.queries["c"] = makeSimpleQuery("cpu", "system");
    request.formula = "a + b";

    auto refs = request.getReferencedQueries();
    EXPECT_EQ(refs.size(), 2);
    EXPECT_TRUE(refs.count("a"));
    EXPECT_TRUE(refs.count("b"));
    EXPECT_FALSE(refs.count("c"));  // Not referenced in formula
}

TEST_F(DerivedQueryTest, ApplyGlobalTimeRange) {
    DerivedQueryRequest request;
    request.startTime = 5000;
    request.endTime = 10000;

    QueryRequest queryA;
    queryA.measurement = "cpu";
    queryA.startTime = 0;  // Will be overwritten
    queryA.endTime = 0;    // Will be overwritten
    request.queries["a"] = queryA;

    QueryRequest queryB;
    queryB.measurement = "memory";
    queryB.startTime = 1000;  // Already set, should not change
    queryB.endTime = 2000;    // Already set, should not change
    request.queries["b"] = queryB;

    request.formula = "a + b";
    request.applyGlobalTimeRange();

    EXPECT_EQ(request.queries["a"].startTime, 5000);
    EXPECT_EQ(request.queries["a"].endTime, 10000);
    EXPECT_EQ(request.queries["b"].startTime, 1000);  // Unchanged
    EXPECT_EQ(request.queries["b"].endTime, 2000);    // Unchanged
}

// ==================== DerivedQueryBuilder Tests ====================

TEST_F(DerivedQueryTest, BuilderBasicUsage) {
    auto request = DerivedQueryBuilder()
                       .addQuery("a", makeSimpleQuery("cpu", "usage"))
                       .addQuery("b", makeSimpleQuery("cpu", "idle"))
                       .setFormula("a + b")
                       .setTimeRange(1000, 2000)
                       .build();

    EXPECT_EQ(request.queries.size(), 2);
    EXPECT_EQ(request.formula, "a + b");
    EXPECT_EQ(request.startTime, 1000);
    EXPECT_EQ(request.endTime, 2000);
}

TEST_F(DerivedQueryTest, BuilderWithQueryString) {
    auto request = DerivedQueryBuilder()
                       .addQuery("a", "avg:cpu(usage){host:server1}")
                       .addQuery("b", "avg:cpu(idle){host:server1}")
                       .setFormula("a / (a + b) * 100")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_EQ(request.queries["a"].measurement, "cpu");
    EXPECT_EQ(request.queries["a"].fields[0], "usage");
    EXPECT_EQ(request.queries["b"].fields[0], "idle");
}

TEST_F(DerivedQueryTest, BuilderWithAggregationInterval) {
    auto request = DerivedQueryBuilder()
                       .addQuery("a", makeSimpleQuery("cpu", "usage"))
                       .setFormula("a")
                       .setTimeRange(1000, 2000)
                       .setAggregationInterval(60000000000)  // 1 minute in ns
                       .build();

    EXPECT_EQ(request.aggregationInterval, 60000000000);
}

TEST_F(DerivedQueryTest, BuilderValidationFailsOnBuild) {
    EXPECT_THROW(
        {
            DerivedQueryBuilder()
                .addQuery("a", makeSimpleQuery("cpu", "usage"))
                .setFormula("a + undefined")  // References undefined query
                .build();
        },
        DerivedQueryException);
}

// ==================== SubQueryResult Tests ====================

TEST_F(DerivedQueryTest, SubQueryResultEmpty) {
    SubQueryResult result;
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(result.size(), 0);
}

TEST_F(DerivedQueryTest, SubQueryResultWithData) {
    SubQueryResult result;
    result.queryName = "a";
    result.timestamps = {1000, 2000, 3000};
    result.values = {1.0, 2.0, 3.0};
    result.measurement = "cpu";
    result.field = "usage";

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3);
}

// ==================== DerivedQueryResult Tests ====================

TEST_F(DerivedQueryTest, DerivedQueryResultEmpty) {
    DerivedQueryResult result;
    EXPECT_TRUE(result.empty());
    EXPECT_EQ(result.size(), 0);
}

TEST_F(DerivedQueryTest, DerivedQueryResultWithData) {
    DerivedQueryResult result;
    result.timestamps = {1000, 2000, 3000};
    result.values = {10.0, 20.0, 30.0};
    result.formula = "a * 10";
    result.stats.pointCount = 3;

    EXPECT_FALSE(result.empty());
    EXPECT_EQ(result.size(), 3);
    EXPECT_EQ(result.stats.pointCount, 3);
}

// ==================== Real-World Formula Tests ====================

TEST_F(DerivedQueryTest, ErrorRateFormula) {
    // errors / total * 100
    auto request = DerivedQueryBuilder()
                       .addQuery("errors", makeSimpleQuery("http", "errors"))
                       .addQuery("total", makeSimpleQuery("http", "requests"))
                       .setFormula("errors / total * 100")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_EQ(request.queries.size(), 2);
    auto refs = request.getReferencedQueries();
    EXPECT_TRUE(refs.count("errors"));
    EXPECT_TRUE(refs.count("total"));
}

TEST_F(DerivedQueryTest, CPUUtilizationFormula) {
    // (a / (a + b)) * 100 where a=usage, b=idle
    auto request = DerivedQueryBuilder()
                       .addQuery("a", makeSimpleQuery("cpu", "usage"))
                       .addQuery("b", makeSimpleQuery("cpu", "idle"))
                       .setFormula("(a / (a + b)) * 100")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, MemoryUsageFormula) {
    // (used / total) * 100
    auto request = DerivedQueryBuilder()
                       .addQuery("used", makeSimpleQuery("memory", "used"))
                       .addQuery("total", makeSimpleQuery("memory", "total"))
                       .setFormula("(used / total) * 100")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, MinMaxNormalizationFormula) {
    // (value - min_val) / (max_val - min_val)
    auto request = DerivedQueryBuilder()
                       .addQuery("value", makeSimpleQuery("sensor", "temperature"))
                       .addQuery("min_val", makeSimpleQuery("sensor", "temp_min"))
                       .addQuery("max_val", makeSimpleQuery("sensor", "temp_max"))
                       .setFormula("(value - min_val) / (max_val - min_val)")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_NO_THROW(request.validate());
}

TEST_F(DerivedQueryTest, ComplexFormulaWithFunctions) {
    // abs(a - b) / max(a, b)
    auto request = DerivedQueryBuilder()
                       .addQuery("a", makeSimpleQuery("sensor", "value1"))
                       .addQuery("b", makeSimpleQuery("sensor", "value2"))
                       .setFormula("abs(a - b) / max(a, b)")
                       .setTimeRange(1000000000, 2000000000)
                       .build();

    EXPECT_NO_THROW(request.validate());
}
