#include <gtest/gtest.h>
#include "derived_query_executor.hpp"
#include "derived_query.hpp"
#include "http_query_handler.hpp"

using namespace timestar;

class DerivedQueryMultiSeriesTest : public ::testing::Test {
protected:
    // Helper: create a DerivedQueryExecutor with null engine/index
    // (convertQueryResponse doesn't use them)
    DerivedQueryExecutor makeExecutor() {
        return DerivedQueryExecutor(nullptr, nullptr);
    }

    // Helper: create a simple QueryRequest
    QueryRequest makeQuery(const std::string& measurement, const std::string& field) {
        QueryRequest req;
        req.measurement = measurement;
        req.fields.push_back(field);
        req.aggregation = AggregationMethod::AVG;
        req.startTime = 1000000000;
        req.endTime = 2000000000;
        return req;
    }

    // Helper: create a SeriesResult with double values
    SeriesResult makeSeries(
        const std::string& measurement,
        const std::map<std::string, std::string>& tags,
        const std::string& fieldName,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values) {

        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        FieldValues fv = values;
        sr.fields[fieldName] = std::make_pair(timestamps, fv);
        return sr;
    }

    // Expose the private convertQueryResponse for testing via friend access
    SubQueryResult callConvert(
        DerivedQueryExecutor& executor,
        const std::string& name,
        const QueryRequest& query,
        const std::vector<SeriesResult>& results) {
        return executor.convertQueryResponse(name, query, results);
    }
};

// ==================== Empty Results ====================

TEST_F(DerivedQueryMultiSeriesTest, EmptyResultsReturnEmptySubQueryResult) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");
    std::vector<SeriesResult> emptyResults;

    auto result = callConvert(executor, "a", query, emptyResults);

    EXPECT_EQ(result.queryName, "a");
    EXPECT_EQ(result.measurement, "cpu");
    EXPECT_TRUE(result.timestamps.empty());
    EXPECT_TRUE(result.values.empty());
    EXPECT_TRUE(result.tags.empty());
    EXPECT_TRUE(result.field.empty());
}

// ==================== Single Series (Happy Path) ====================

TEST_F(DerivedQueryMultiSeriesTest, SingleSeriesResultWorkCorrectly) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "cpu",
        {{"host", "server1"}},
        "usage",
        {1000, 2000, 3000},
        {10.0, 20.0, 30.0}
    ));

    auto result = callConvert(executor, "a", query, results);

    EXPECT_EQ(result.queryName, "a");
    EXPECT_EQ(result.measurement, "cpu");
    EXPECT_EQ(result.field, "usage");
    EXPECT_EQ(result.tags.at("host"), "server1");
    ASSERT_EQ(result.timestamps.size(), 3);
    EXPECT_EQ(result.timestamps[0], 1000);
    EXPECT_EQ(result.timestamps[2], 3000);
    ASSERT_EQ(result.values.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

// ==================== Multiple Series (Bug) ====================

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesThrowsDerivedQueryException) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "cpu",
        {{"host", "server1"}},
        "usage",
        {1000, 2000},
        {10.0, 20.0}
    ));
    results.push_back(makeSeries(
        "cpu",
        {{"host", "server2"}},
        "usage",
        {1000, 2000},
        {30.0, 40.0}
    ));

    EXPECT_THROW(
        callConvert(executor, "my_query", query, results),
        DerivedQueryException
    );
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageContainsQueryName) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "cpu", {{"host", "server1"}}, "usage", {1000}, {10.0}
    ));
    results.push_back(makeSeries(
        "cpu", {{"host", "server2"}}, "usage", {1000}, {20.0}
    ));

    try {
        callConvert(executor, "error_rate", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("error_rate"), std::string::npos)
            << "Error message should contain the query name 'error_rate', got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageContainsSeriesCount) {
    auto executor = makeExecutor();
    auto query = makeQuery("temperature", "value");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "temperature", {{"location", "us-west"}}, "value", {1000}, {72.0}
    ));
    results.push_back(makeSeries(
        "temperature", {{"location", "us-east"}}, "value", {1000}, {68.0}
    ));
    results.push_back(makeSeries(
        "temperature", {{"location", "eu-west"}}, "value", {1000}, {65.0}
    ));

    try {
        callConvert(executor, "temp_avg", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        // Should mention 3 series were returned
        EXPECT_NE(msg.find("3"), std::string::npos)
            << "Error message should contain the count '3', got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageSuggestsScopeFilters) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "cpu", {{"host", "a"}}, "usage", {1000}, {1.0}
    ));
    results.push_back(makeSeries(
        "cpu", {{"host", "b"}}, "usage", {1000}, {2.0}
    ));

    try {
        callConvert(executor, "load", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        // Should suggest adding scope filters to narrow the result
        EXPECT_NE(msg.find("scope"), std::string::npos)
            << "Error message should mention scope filters, got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, ExactlyTwoSeriesAlsoThrows) {
    auto executor = makeExecutor();
    auto query = makeQuery("disk", "free");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries(
        "disk", {{"device", "sda"}}, "free", {1000, 2000}, {100.0, 200.0}
    ));
    results.push_back(makeSeries(
        "disk", {{"device", "sdb"}}, "free", {1000, 2000}, {300.0, 400.0}
    ));

    EXPECT_THROW(
        callConvert(executor, "disk_check", query, results),
        DerivedQueryException
    );
}
