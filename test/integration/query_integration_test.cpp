#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../../lib/query/series_matcher.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <thread>

using namespace timestar;

class QueryIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Initialize time range for tests
        startTime = 1704067200000000000ULL;  // 2024-01-01 00:00:00 in nanoseconds
        endTime = 1704070800000000000ULL;    // 2024-01-01 01:00:00 in nanoseconds
    }

    uint64_t startTime;
    uint64_t endTime;

    // Helper to create mock time series data
    SeriesResult createMockSeries(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                  const std::string& field, int numPoints = 60, double baseValue = 20.0,
                                  double increment = 0.5) {
        SeriesResult result;
        result.measurement = measurement;
        result.tags = tags;

        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        uint64_t timeStep = (endTime - startTime) / numPoints;

        for (int i = 0; i < numPoints; ++i) {
            timestamps.push_back(startTime + i * timeStep);
            // Add some variation to make it interesting
            double value = baseValue + i * increment;
            if (i % 5 == 0)
                value += 2.0;  // Spike every 5 points
            if (i % 7 == 0)
                value -= 1.0;  // Dip every 7 points
            values.push_back(value);
        }

        result.fields[field] = std::make_pair(timestamps, FieldValues(values));
        return result;
    }
};

// Test the complete query pipeline with realistic data
TEST_F(QueryIntegrationTest, MetricsQueryWithAggregation) {
    // Create mock metrics data
    std::vector<SeriesResult> mockData;

    // CPU metrics from different hosts
    mockData.push_back(
        createMockSeries("cpu", {{"host", "server01"}, {"datacenter", "us-west"}}, "usage_percent", 60, 45.0, 0.3));

    mockData.push_back(
        createMockSeries("cpu", {{"host", "server02"}, {"datacenter", "us-west"}}, "usage_percent", 60, 50.0, 0.4));

    mockData.push_back(
        createMockSeries("cpu", {{"host", "server03"}, {"datacenter", "us-east"}}, "usage_percent", 60, 40.0, 0.25));

    // Apply 5-minute average aggregation
    uint64_t interval = 5 * 60 * 1000000000ULL;  // 5 minutes

    for (auto& series : mockData) {
        for (auto& [field, data] : series.fields) {
            auto& timestamps = data.first;
            auto& values = data.second;

            // Only aggregate numeric (double) values
            if (std::holds_alternative<std::vector<double>>(values)) {
                auto& doubleValues = std::get<std::vector<double>>(values);

                auto aggregated = Aggregator::aggregate(timestamps, doubleValues, AggregationMethod::AVG, interval);

                timestamps.clear();
                doubleValues.clear();

                for (const auto& point : aggregated) {
                    timestamps.push_back(point.timestamp);
                    doubleValues.push_back(point.value);
                }
            }
        }
    }

    // Verify aggregation reduced data points
    for (const auto& series : mockData) {
        auto& fieldData = series.fields.at("usage_percent");
        auto& timestamps = fieldData.first;
        auto& valuesVariant = fieldData.second;

        // 1 hour of data in 5-minute buckets = 12 buckets
        EXPECT_LE(timestamps.size(), 12);
        EXPECT_GE(timestamps.size(), 10);  // At least 10 buckets with data

        // Verify values are reasonable
        if (std::holds_alternative<std::vector<double>>(valuesVariant)) {
            auto& values = std::get<std::vector<double>>(valuesVariant);
            for (double value : values) {
                EXPECT_GE(value, 30.0);  // CPU usage between 30-80%
                EXPECT_LE(value, 80.0);
            }
        }
    }
}

TEST_F(QueryIntegrationTest, GroupByDatacenter) {
    // Create mock data
    std::vector<SeriesResult> mockData;

    mockData.push_back(createMockSeries("network",
                                        {{"host", "server01"}, {"datacenter", "us-west"}, {"interface", "eth0"}},
                                        "bytes_sent", 30, 1000000.0, 50000.0));

    mockData.push_back(createMockSeries("network",
                                        {{"host", "server02"}, {"datacenter", "us-west"}, {"interface", "eth0"}},
                                        "bytes_sent", 30, 1200000.0, 60000.0));

    mockData.push_back(createMockSeries("network",
                                        {{"host", "server03"}, {"datacenter", "us-east"}, {"interface", "eth0"}},
                                        "bytes_sent", 30, 900000.0, 40000.0));

    // Group by datacenter
    std::map<std::string, std::vector<const SeriesResult*>> groups;

    for (const auto& series : mockData) {
        auto it = series.tags.find("datacenter");
        if (it != series.tags.end()) {
            groups[it->second].push_back(&series);
        }
    }

    ASSERT_EQ(groups.size(), 2);             // us-west and us-east
    EXPECT_EQ(groups["us-west"].size(), 2);  // 2 servers in us-west
    EXPECT_EQ(groups["us-east"].size(), 1);  // 1 server in us-east

    // Aggregate within each group (sum bytes_sent)
    std::map<std::string, double> datacenterTotals;

    for (const auto& [dc, seriesList] : groups) {
        std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> groupData;

        for (const auto* series : seriesList) {
            auto& fieldData = series->fields.at("bytes_sent");
            if (std::holds_alternative<std::vector<double>>(fieldData.second)) {
                groupData.push_back(std::make_pair(fieldData.first, std::get<std::vector<double>>(fieldData.second)));
            }
        }

        auto aggregated = Aggregator::aggregateMultiple(groupData, AggregationMethod::SUM, 0);

        // Sum up all the per-timestamp results to get the overall total
        double total = 0.0;
        for (const auto& point : aggregated) {
            total += point.value;
        }
        datacenterTotals[dc] = total;
    }

    // Verify us-west has more total traffic (2 servers vs 1)
    EXPECT_GT(datacenterTotals["us-west"], datacenterTotals["us-east"]);
}

TEST_F(QueryIntegrationTest, MaxTemperatureQuery) {
    // Create temperature sensor data
    std::vector<SeriesResult> sensorData;

    // Sensors in different rooms
    sensorData.push_back(createMockSeries(
        "temperature", {{"room", "server_room"}, {"building", "A"}}, "celsius", 120, 18.0, 0.1  // Server room: 18-30°C
        ));

    sensorData.push_back(createMockSeries(
        "temperature", {{"room", "office"}, {"building", "A"}}, "celsius", 120, 20.0, 0.05  // Office: 20-26°C
        ));

    sensorData.push_back(createMockSeries(
        "temperature", {{"room", "storage"}, {"building", "B"}}, "celsius", 120, 15.0, 0.08  // Storage: 15-25°C
        ));

    // Find maximum temperature across all sensors
    double globalMax = 0.0;
    std::string maxLocation;
    uint64_t maxTimestamp = 0;

    for (const auto& series : sensorData) {
        auto& fieldData = series.fields.at("celsius");
        auto& timestamps = fieldData.first;
        auto& valuesVariant = fieldData.second;

        if (std::holds_alternative<std::vector<double>>(valuesVariant)) {
            auto& values = std::get<std::vector<double>>(valuesVariant);
            auto maxIt = std::max_element(values.begin(), values.end());
            if (maxIt != values.end() && *maxIt > globalMax) {
                globalMax = *maxIt;
                size_t idx = std::distance(values.begin(), maxIt);
                maxTimestamp = timestamps[idx];
                maxLocation = series.tags.at("room");
            }
        }
    }

    // Server room should have the highest temperature
    EXPECT_GT(globalMax, 25.0);
    EXPECT_LT(globalMax, 35.0);
    EXPECT_EQ(maxLocation, "server_room");
}

TEST_F(QueryIntegrationTest, TimeRangeFiltering) {
    // Create data spanning multiple hours
    SeriesResult longSeries;
    longSeries.measurement = "events";
    longSeries.tags = {{"source", "app1"}};

    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Create data from -1 hour to +2 hours relative to our time range
    uint64_t extendedStart = startTime - 3600000000000ULL;  // 1 hour before
    uint64_t extendedEnd = endTime + 7200000000000ULL;      // 2 hours after

    for (uint64_t t = extendedStart; t <= extendedEnd; t += 60000000000ULL) {
        timestamps.push_back(t);
        values.push_back(100.0);
    }

    longSeries.fields["count"] = std::make_pair(timestamps, values);

    // Filter to our time range
    std::vector<uint64_t> filteredTimestamps;
    std::vector<double> filteredValues;

    for (size_t i = 0; i < timestamps.size(); ++i) {
        if (timestamps[i] >= startTime && timestamps[i] <= endTime) {
            filteredTimestamps.push_back(timestamps[i]);
            filteredValues.push_back(values[i]);
        }
    }

    // Should have exactly 60 points (1 per minute for 1 hour)
    EXPECT_EQ(filteredTimestamps.size(), 61);  // Inclusive of both endpoints
    EXPECT_GE(filteredTimestamps.front(), startTime);
    EXPECT_LE(filteredTimestamps.back(), endTime);
}

TEST_F(QueryIntegrationTest, ComplexAggregationPipeline) {
    // Simulate a complex monitoring query:
    // "Show me the 95th percentile of response times per service,
    //  grouped by region, in 10-minute windows"

    std::vector<SeriesResult> responseTimeData;

    // Services in different regions
    std::vector<std::string> services = {"api", "web", "db"};
    std::vector<std::string> regions = {"us-west", "us-east", "eu-central"};

    for (const auto& service : services) {
        for (const auto& region : regions) {
            responseTimeData.push_back(createMockSeries("response_time", {{"service", service}, {"region", region}},
                                                        "milliseconds",
                                                        180,                                     // 3 points per minute
                                                        50.0 + (service == "db" ? 100.0 : 0.0),  // DB is slower
                                                        0.5));
        }
    }

    // Step 1: Calculate percentiles per series (we'll use MAX as approximation)
    uint64_t interval = 10 * 60 * 1000000000ULL;  // 10 minutes

    for (auto& series : responseTimeData) {
        auto& fieldData = series.fields.at("milliseconds");
        auto& timestamps = fieldData.first;
        auto& values = fieldData.second;

        if (std::holds_alternative<std::vector<double>>(values)) {
            auto& doubleValues = std::get<std::vector<double>>(values);

            auto aggregated = Aggregator::aggregate(timestamps, doubleValues, AggregationMethod::MAX, interval);

            timestamps.clear();
            doubleValues.clear();

            for (const auto& point : aggregated) {
                timestamps.push_back(point.timestamp);
                doubleValues.push_back(point.value);
            }
        }
    }

    // Step 2: Group by region
    std::map<std::string, std::vector<SeriesResult*>> regionGroups;
    for (auto& series : responseTimeData) {
        regionGroups[series.tags.at("region")].push_back(&series);
    }

    ASSERT_EQ(regionGroups.size(), 3);  // 3 regions

    // Step 3: Calculate regional averages
    std::map<std::string, double> regionalAverages;

    for (const auto& [region, seriesList] : regionGroups) {
        double sum = 0.0;
        int count = 0;

        for (const auto* series : seriesList) {
            auto& fieldData = series->fields.at("milliseconds");
            if (std::holds_alternative<std::vector<double>>(fieldData.second)) {
                auto& doubleVals = std::get<std::vector<double>>(fieldData.second);
                for (double val : doubleVals) {
                    sum += val;
                    count++;
                }
            }
        }

        regionalAverages[region] = count > 0 ? sum / count : 0.0;
    }

    // Verify all regions have data
    for (const auto& [region, avg] : regionalAverages) {
        EXPECT_GT(avg, 0.0);
        EXPECT_LT(avg, 500.0);  // Response times should be reasonable
    }
}

TEST_F(QueryIntegrationTest, HandleEmptyAndSparseData) {
    // Test with sparse data (missing timestamps)
    SeriesResult sparseSeries;
    sparseSeries.measurement = "sparse_metric";
    sparseSeries.tags = {{"type", "sparse"}};

    std::vector<uint64_t> timestamps;
    std::vector<double> values;

    // Only add data at specific times (every 10 minutes)
    for (int i = 0; i < 6; ++i) {
        timestamps.push_back(startTime + i * 600000000000ULL);  // 10 min intervals
        values.push_back(100.0 + i * 10.0);
    }

    sparseSeries.fields["value"] = std::make_pair(timestamps, FieldValues(values));

    // Aggregate with 5-minute intervals
    uint64_t interval = 5 * 60 * 1000000000ULL;
    auto aggregated = Aggregator::aggregate(timestamps, values, AggregationMethod::AVG, interval);

    // Should have at most 12 buckets for 1 hour with 5-min intervals
    // But with sparse data, many buckets will be empty
    EXPECT_LE(aggregated.size(), 12);
    EXPECT_GE(aggregated.size(), 6);  // At least 6 points of data

    // Test with completely empty data
    std::vector<uint64_t> emptyTimestamps;
    std::vector<double> emptyValues;

    auto emptyResult = Aggregator::aggregate(emptyTimestamps, emptyValues, AggregationMethod::AVG, interval);

    EXPECT_EQ(emptyResult.size(), 0);
}

TEST_F(QueryIntegrationTest, QueryParsingAndValidation) {
    // Test various query formats
    std::vector<std::string> validQueries = {"avg:cpu(usage)", "max:memory(used,free){host:server*}",
                                             "sum:network(bytes_in,bytes_out){} by {datacenter}",
                                             "latest:temperature(celsius){room:server_room}",
                                             "min:disk(used_percent){datacenter:us-*} by {host,datacenter}"};

    for (const auto& query : validQueries) {
        try {
            auto request = QueryParser::parseQueryString(query);
            EXPECT_FALSE(request.measurement.empty());

            // Verify aggregation method is set
            EXPECT_GE(static_cast<int>(request.aggregation), 0);
            EXPECT_LE(static_cast<int>(request.aggregation), 4);
        } catch (const std::exception& e) {
            FAIL() << "Failed to parse valid query: " << query << " Error: " << e.what();
        }
    }

    // Test invalid queries
    std::vector<std::string> invalidQueries = {
        "invalid:measurement(",       // Missing closing parenthesis
        ":measurement(field)",        // Missing aggregation
        "avg:",                       // Missing measurement
        "avg:measurement(field){",    // Unclosed brace
        "avg:measurement(field) by",  // Missing group-by clause
        // Note: "avg:measurement" is valid (fields are optional)
        // Note: "avg:measurement()" is valid (empty = all fields)
    };

    for (const auto& query : invalidQueries) {
        EXPECT_THROW(QueryParser::parseQueryString(query), QueryParseException) << "Query should be invalid: " << query;
    }
}

TEST_F(QueryIntegrationTest, SeriesMatchingWithPatterns) {
    // Test wildcard and regex matching
    std::map<std::string, std::string> seriesTags = {
        {"host", "web-server-01"}, {"datacenter", "us-west-2a"}, {"environment", "production"}};

    // Test wildcard patterns
    struct TestCase {
        std::map<std::string, std::string> pattern;
        bool shouldMatch;
        std::string description;
    };

    std::vector<TestCase> testCases = {
        {{{"host", "web-server-01"}}, true, "Exact match"},
        {{{"host", "web-server-*"}}, true, "Wildcard suffix"},
        {{{"host", "*-01"}}, true, "Wildcard prefix"},
        {{{"host", "*server*"}}, true, "Wildcard middle"},
        {{{"host", "db-*"}}, false, "No match with wildcard"},
        {{{"datacenter", "us-*"}}, true, "Datacenter wildcard"},
        {{{"datacenter", "*-2a"}}, true, "Datacenter suffix wildcard"},
        {{{"environment", "prod*"}}, true, "Environment prefix"},
        {{{"environment", "staging"}}, false, "Wrong environment"},
        {{{"host", "web-*"}, {"datacenter", "us-*"}}, true, "Multiple wildcards match"},
        {{{"host", "web-*"}, {"datacenter", "eu-*"}}, false, "One wildcard doesn't match"}};

    for (const auto& tc : testCases) {
        bool result = SeriesMatcher::matches(seriesTags, tc.pattern);
        EXPECT_EQ(result, tc.shouldMatch) << "Test case: " << tc.description;
    }
}

TEST_F(QueryIntegrationTest, PerformanceWithLargeDataset) {
    // Create a large dataset to test performance
    std::vector<SeriesResult> largeDataset;

    // Create 100 series with 1000 points each
    for (int i = 0; i < 100; ++i) {
        std::string host = "host" + std::to_string(i);
        std::string dc = (i < 50) ? "us-west" : "us-east";

        largeDataset.push_back(createMockSeries("metric", {{"host", host}, {"datacenter", dc}}, "value",
                                                1000,  // 1000 data points
                                                100.0 + i, 0.01));
    }

    auto start = std::chrono::high_resolution_clock::now();

    // Aggregate all series with 1-minute intervals
    uint64_t interval = 60000000000ULL;  // 1 minute

    for (auto& series : largeDataset) {
        auto& fieldData = series.fields.at("value");
        auto& timestamps = fieldData.first;
        auto& values = fieldData.second;

        if (std::holds_alternative<std::vector<double>>(values)) {
            auto& doubleValues = std::get<std::vector<double>>(values);

            auto aggregated = Aggregator::aggregate(timestamps, doubleValues, AggregationMethod::AVG, interval);

            // Just verify it completes
            EXPECT_GT(aggregated.size(), 0);
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Should complete in reasonable time (< 1 second for 100k points)
    EXPECT_LT(duration.count(), 1000) << "Aggregation took too long: " << duration.count() << "ms";
}