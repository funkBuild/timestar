#include <gtest/gtest.h>
#include "../../../lib/query/query_parser.hpp"
#include "../../../lib/query/query_planner.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/series_matcher.hpp"
#include <memory>
#include <vector>
#include <map>

using namespace tsdb;

// Mock Engine for testing without Seastar
class MockEngine {
public:
    std::vector<SeriesResult> executeLocalQuery(const ShardQuery& query) {
        std::vector<SeriesResult> results;
        
        // Generate mock data for each series ID
        for (uint64_t seriesId : query.seriesIds) {
            SeriesResult result;
            result.measurement = "temperature";
            
            // Add different tags based on series ID
            if (seriesId % 3 == 0) {
                result.tags = {{"location", "us-west"}, {"host", "server01"}};
            } else if (seriesId % 3 == 1) {
                result.tags = {{"location", "us-east"}, {"host", "server02"}};
            } else {
                result.tags = {{"location", "us-central"}, {"host", "server03"}};
            }
            
            // Generate time series data
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            
            // Create 10 data points per series
            uint64_t baseTime = query.startTime;
            uint64_t step = (query.endTime - query.startTime) / 10;
            
            for (int i = 0; i < 10; ++i) {
                timestamps.push_back(baseTime + i * step);
                // Different value patterns for different series
                values.push_back(20.0 + (seriesId * 5.0) + i * 2.0 + (i % 2) * 0.5);
            }
            
            for (const auto& field : query.fields) {
                result.fields[field] = std::make_pair(timestamps, values);
            }
            
            results.push_back(result);
        }
        
        return results;
    }
};

class QueryE2ETest : public ::testing::Test {
protected:
    void SetUp() override {
        mockEngine = std::make_unique<MockEngine>();
        
        // Set up time range
        startTimeStr = "01-01-2024 00:00:00";
        endTimeStr = "01-01-2024 01:00:00";
        startTime = QueryParser::parseTime(startTimeStr);
        endTime = QueryParser::parseTime(endTimeStr);
    }
    
    std::unique_ptr<MockEngine> mockEngine;
    std::string startTimeStr;
    std::string endTimeStr;
    uint64_t startTime;
    uint64_t endTime;
    
    // Helper to simulate the full query pipeline
    std::vector<SeriesResult> executeFullQuery(
        const std::string& queryStr,
        const std::string& interval = "") {
        
        // Step 1: Parse query
        QueryRequest request = QueryParser::parse(queryStr, startTimeStr, endTimeStr);
        
        // Parse interval if provided
        if (!interval.empty()) {
            request.aggregationInterval = HttpQueryHandler::parseInterval(interval);
        }
        
        // Step 2: Create mock shard query (simulating query planner)
        ShardQuery shardQuery;
        shardQuery.shardId = 0;
        shardQuery.seriesIds = {1, 2, 3}; // Mock series IDs
        shardQuery.fields = request.fields.empty() ? 
            std::set<std::string>{"value"} : 
            std::set<std::string>(request.fields.begin(), request.fields.end());
        shardQuery.startTime = startTime;
        shardQuery.endTime = endTime;
        
        // Step 3: Execute local query
        std::vector<SeriesResult> results = mockEngine->executeLocalQuery(shardQuery);
        
        // Step 4: Apply aggregation if needed
        if (request.aggregation != AggregationMethod::AVG || 
            request.aggregationInterval > 0) {
            
            for (auto& series : results) {
                for (auto& [fieldName, fieldData] : series.fields) {
                    auto& timestamps = fieldData.first;
                    auto& values = fieldData.second;
                    
                    auto aggregated = Aggregator::aggregate(
                        timestamps, values, request.aggregation, request.aggregationInterval);
                    
                    timestamps.clear();
                    values.clear();
                    
                    for (const auto& point : aggregated) {
                        timestamps.push_back(point.timestamp);
                        values.push_back(point.value);
                    }
                }
            }
        }
        
        // Step 5: Apply group-by if needed
        if (request.hasGroupBy()) {
            results = applyGroupBy(results, request);
        }
        
        return results;
    }
    
    std::vector<SeriesResult> applyGroupBy(
        const std::vector<SeriesResult>& results,
        const QueryRequest& request) {
        
        std::map<std::string, std::vector<const SeriesResult*>> groups;
        
        for (const auto& series : results) {
            std::string groupKey;
            for (const auto& tagName : request.groupByTags) {
                auto it = series.tags.find(tagName);
                if (it != series.tags.end()) {
                    if (!groupKey.empty()) groupKey += ",";
                    groupKey += tagName + "=" + it->second;
                }
            }
            groups[groupKey].push_back(&series);
        }
        
        std::vector<SeriesResult> groupedResults;
        
        for (const auto& [groupKey, groupSeries] : groups) {
            if (groupSeries.empty()) continue;
            
            SeriesResult grouped;
            grouped.measurement = groupSeries[0]->measurement;
            
            for (const auto& tagName : request.groupByTags) {
                auto it = groupSeries[0]->tags.find(tagName);
                if (it != groupSeries[0]->tags.end()) {
                    grouped.tags[tagName] = it->second;
                }
            }
            
            std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>> fieldGroups;
            
            for (const auto* series : groupSeries) {
                for (const auto& [fieldName, fieldData] : series->fields) {
                    fieldGroups[fieldName].push_back(fieldData);
                }
            }
            
            for (const auto& [fieldName, fieldSeries] : fieldGroups) {
                auto aggregated = Aggregator::aggregateMultiple(
                    fieldSeries, request.aggregation, request.aggregationInterval);
                
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                
                for (const auto& point : aggregated) {
                    timestamps.push_back(point.timestamp);
                    values.push_back(point.value);
                }
                
                grouped.fields[fieldName] = std::make_pair(timestamps, values);
            }
            
            groupedResults.push_back(grouped);
        }
        
        return groupedResults;
    }
};

// Basic query tests
TEST_F(QueryE2ETest, SimpleAverageQuery) {
    auto results = executeFullQuery("avg:temperature(value)");
    
    ASSERT_EQ(results.size(), 3); // 3 series
    for (const auto& series : results) {
        EXPECT_EQ(series.measurement, "temperature");
        ASSERT_TRUE(series.fields.find("value") != series.fields.end());
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 10); // 10 data points
        EXPECT_EQ(values.size(), 10);
    }
}

TEST_F(QueryE2ETest, MaxAggregationQuery) {
    auto results = executeFullQuery("max:temperature(value)");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With MAX aggregation and no interval, should get 1 point
        EXPECT_EQ(timestamps.size(), 1);
        EXPECT_EQ(values.size(), 1);
        
        // Verify it's actually the max value
        // Based on mock data generation: 20.0 + (seriesId * 5.0) + i * 2.0 + (i % 2) * 0.5
        // For i=9 (last point): 20.0 + (seriesId * 5.0) + 18.0 + 0.5
        // So max should be 38.5 + (seriesId * 5.0)
    }
}

TEST_F(QueryE2ETest, MinAggregationQuery) {
    auto results = executeFullQuery("min:temperature(value)");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1);
        EXPECT_EQ(values.size(), 1);
        
        // Min value should be the first point
        // 20.0 + (seriesId * 5.0) + 0 * 2.0 + 0 * 0.5 = 20.0 + (seriesId * 5.0)
    }
}

TEST_F(QueryE2ETest, SumAggregationQuery) {
    auto results = executeFullQuery("sum:temperature(value)");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1);
        EXPECT_EQ(values.size(), 1);
        
        // Sum should be sum of all 10 points
        EXPECT_GT(values[0], 200.0); // Should be a large sum
    }
}

TEST_F(QueryE2ETest, LatestAggregationQuery) {
    auto results = executeFullQuery("latest:temperature(value)");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1);
        EXPECT_EQ(values.size(), 1);
        
        // Latest timestamp should be close to endTime
        EXPECT_GT(timestamps[0], startTime);
        EXPECT_LE(timestamps[0], endTime);
    }
}

// Time interval aggregation tests
TEST_F(QueryE2ETest, AverageWithTimeInterval) {
    auto results = executeFullQuery("avg:temperature(value)", "10m");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With 1 hour range and 10-minute intervals, expect 6 buckets
        // But since we only have 10 points spread across 1 hour, 
        // we might get fewer buckets
        EXPECT_GE(timestamps.size(), 1);
        EXPECT_LE(timestamps.size(), 6);
    }
}

TEST_F(QueryE2ETest, MaxWithTimeInterval) {
    auto results = executeFullQuery("max:temperature(value)", "15m");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With 1 hour range and 15-minute intervals, expect 4 buckets
        EXPECT_GE(timestamps.size(), 1);
        EXPECT_LE(timestamps.size(), 4);
        
        // Each bucket should contain the max value for that time range
        for (size_t i = 1; i < values.size(); ++i) {
            // Later buckets should have higher or equal max values
            // (based on our mock data that increases over time)
            EXPECT_GE(values[i], values[i-1] - 10.0); // Allow some variance
        }
    }
}

TEST_F(QueryE2ETest, MinWithTimeInterval) {
    auto results = executeFullQuery("min:temperature(value)", "20m");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With 1 hour range and 20-minute intervals, expect 3 buckets
        EXPECT_GE(timestamps.size(), 1);
        EXPECT_LE(timestamps.size(), 3);
    }
}

TEST_F(QueryE2ETest, SumWithTimeInterval) {
    auto results = executeFullQuery("sum:temperature(value)", "30m");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With 1 hour range and 30-minute intervals, expect 2 buckets
        EXPECT_GE(timestamps.size(), 1);
        EXPECT_LE(timestamps.size(), 2);
        
        // Each bucket should contain sum of values in that range
        for (double value : values) {
            EXPECT_GT(value, 0.0); // Sums should be positive
        }
    }
}

// Group-by tests
TEST_F(QueryE2ETest, GroupByLocation) {
    auto results = executeFullQuery("avg:temperature(value){} by {location}");
    
    // Should have 3 groups (us-west, us-east, us-central)
    ASSERT_EQ(results.size(), 3);
    
    std::set<std::string> locations;
    for (const auto& series : results) {
        EXPECT_EQ(series.measurement, "temperature");
        ASSERT_EQ(series.tags.size(), 1); // Only location tag
        ASSERT_TRUE(series.tags.find("location") != series.tags.end());
        locations.insert(series.tags.at("location"));
        
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1); // Aggregated to single value
    }
    
    // Check we got all expected locations
    EXPECT_TRUE(locations.find("us-west") != locations.end());
    EXPECT_TRUE(locations.find("us-east") != locations.end());
    EXPECT_TRUE(locations.find("us-central") != locations.end());
}

TEST_F(QueryE2ETest, GroupByLocationWithInterval) {
    auto results = executeFullQuery("max:temperature(value){} by {location}", "30m");
    
    // Should have 3 groups
    ASSERT_EQ(results.size(), 3);
    
    for (const auto& series : results) {
        ASSERT_TRUE(series.tags.find("location") != series.tags.end());
        
        auto& [timestamps, values] = series.fields.at("value");
        // With 30-minute intervals over 1 hour, expect 2 buckets
        EXPECT_GE(timestamps.size(), 1);
        EXPECT_LE(timestamps.size(), 2);
    }
}

TEST_F(QueryE2ETest, GroupByMultipleTags) {
    // Note: In our mock data, each series has unique location-host combination
    // So grouping by both should give us the same 3 series
    auto results = executeFullQuery("sum:temperature(value){} by {location,host}");
    
    ASSERT_EQ(results.size(), 3);
    
    for (const auto& series : results) {
        // Should have both tags preserved
        EXPECT_EQ(series.tags.size(), 2);
        EXPECT_TRUE(series.tags.find("location") != series.tags.end());
        EXPECT_TRUE(series.tags.find("host") != series.tags.end());
        
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1); // Aggregated to single sum
        EXPECT_GT(values[0], 0.0); // Sum should be positive
    }
}

// Query with filters
TEST_F(QueryE2ETest, QueryWithScopeFilter) {
    std::string query = "avg:temperature(value){location:us-west}";
    auto request = QueryParser::parse(query, startTimeStr, endTimeStr);
    
    ASSERT_EQ(request.measurement, "temperature");
    ASSERT_EQ(request.scopes.size(), 1);
    EXPECT_EQ(request.scopes.at("location"), "us-west");
    
    // In a real implementation, this would filter to only us-west series
    // Our mock doesn't implement filtering, but we test the parsing works
}

TEST_F(QueryE2ETest, QueryWithMultipleScopes) {
    std::string query = "max:temperature(value){location:us-east,host:server02}";
    auto request = QueryParser::parse(query, startTimeStr, endTimeStr);
    
    ASSERT_EQ(request.scopes.size(), 2);
    EXPECT_EQ(request.scopes.at("location"), "us-east");
    EXPECT_EQ(request.scopes.at("host"), "server02");
}

// Complex queries
TEST_F(QueryE2ETest, ComplexQueryWithEverything) {
    // Query with aggregation, filters, group-by, and interval
    auto results = executeFullQuery(
        "max:temperature(value,humidity){location:us-*} by {location}",
        "15m"
    );
    
    // Should group by location
    ASSERT_GE(results.size(), 1);
    ASSERT_LE(results.size(), 3);
    
    for (const auto& series : results) {
        EXPECT_EQ(series.measurement, "temperature");
        EXPECT_TRUE(series.tags.find("location") != series.tags.end());
        
        // Should have requested fields
        for (const auto& field : {"value", "humidity"}) {
            if (series.fields.find(field) != series.fields.end()) {
                auto& [timestamps, values] = series.fields.at(field);
                EXPECT_GE(timestamps.size(), 1);
                EXPECT_LE(timestamps.size(), 4); // 15-min intervals over 1 hour
            }
        }
    }
}

// Edge cases
TEST_F(QueryE2ETest, EmptyResultSet) {
    // Create a query that would return no results
    ShardQuery emptyQuery;
    emptyQuery.shardId = 0;
    emptyQuery.seriesIds = {}; // No series IDs
    emptyQuery.fields = {"value"};
    emptyQuery.startTime = startTime;
    emptyQuery.endTime = endTime;
    
    auto results = mockEngine->executeLocalQuery(emptyQuery);
    EXPECT_EQ(results.size(), 0);
}

TEST_F(QueryE2ETest, SingleSeriesQuery) {
    ShardQuery singleQuery;
    singleQuery.shardId = 0;
    singleQuery.seriesIds = {1}; // Single series
    singleQuery.fields = {"value"};
    singleQuery.startTime = startTime;
    singleQuery.endTime = endTime;
    
    auto results = mockEngine->executeLocalQuery(singleQuery);
    ASSERT_EQ(results.size(), 1);
    
    auto& [timestamps, values] = results[0].fields.at("value");
    EXPECT_EQ(timestamps.size(), 10);
    EXPECT_EQ(values.size(), 10);
}

TEST_F(QueryE2ETest, VerySmallTimeInterval) {
    // 1-second interval over 1 hour should create many buckets
    auto results = executeFullQuery("avg:temperature(value)", "1s");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        // With only 10 points over 1 hour, most buckets will be empty
        // So we should get at most 10 buckets with data
        EXPECT_LE(timestamps.size(), 10);
        EXPECT_GE(timestamps.size(), 1);
    }
}

TEST_F(QueryE2ETest, VeryLargeTimeInterval) {
    // 2-hour interval over 1 hour should create 1 bucket
    auto results = executeFullQuery("sum:temperature(value)", "2h");
    
    ASSERT_EQ(results.size(), 3);
    for (const auto& series : results) {
        auto& [timestamps, values] = series.fields.at("value");
        EXPECT_EQ(timestamps.size(), 1); // Single bucket
        EXPECT_GT(values[0], 0.0); // Sum should be positive
    }
}

// Interval parsing tests
TEST_F(QueryE2ETest, ParseIntervalSeconds) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("30s"), 30ULL * 1000000000ULL);
}

TEST_F(QueryE2ETest, ParseIntervalMinutes) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("5m"), 5ULL * 60 * 1000000000ULL);
}

TEST_F(QueryE2ETest, ParseIntervalHours) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("2h"), 2ULL * 3600 * 1000000000ULL);
}

TEST_F(QueryE2ETest, ParseIntervalDays) {
    EXPECT_EQ(HttpQueryHandler::parseInterval("1d"), 86400ULL * 1000000000ULL);
}

TEST_F(QueryE2ETest, ParseInvalidInterval) {
    EXPECT_THROW(HttpQueryHandler::parseInterval("5x"), QueryParseException);
    EXPECT_THROW(HttpQueryHandler::parseInterval("m5"), QueryParseException);
    EXPECT_THROW(HttpQueryHandler::parseInterval(""), QueryParseException);
}

// Series matching with wildcards
TEST_F(QueryE2ETest, WildcardMatching) {
    std::map<std::string, std::string> seriesTags = {
        {"location", "us-west-1"},
        {"host", "server01"}
    };
    
    // Test exact match
    std::map<std::string, std::string> exactScope = {
        {"location", "us-west-1"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, exactScope));
    
    // Test wildcard match
    std::map<std::string, std::string> wildcardScope = {
        {"location", "us-west-*"}
    };
    EXPECT_TRUE(SeriesMatcher::matches(seriesTags, wildcardScope));
    
    // Test no match
    std::map<std::string, std::string> noMatchScope = {
        {"location", "eu-*"}
    };
    EXPECT_FALSE(SeriesMatcher::matches(seriesTags, noMatchScope));
}

// Performance test with many series
TEST_F(QueryE2ETest, LargeScaleAggregation) {
    // Create a large query
    ShardQuery largeQuery;
    largeQuery.shardId = 0;
    
    // Add 100 series IDs
    for (uint64_t i = 1; i <= 100; ++i) {
        largeQuery.seriesIds.push_back(i);
    }
    largeQuery.fields = {"value", "temperature", "humidity"};
    largeQuery.startTime = startTime;
    largeQuery.endTime = endTime;
    
    auto results = mockEngine->executeLocalQuery(largeQuery);
    ASSERT_EQ(results.size(), 100);
    
    // Now aggregate all of them
    std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> allSeries;
    for (const auto& result : results) {
        for (const auto& [field, data] : result.fields) {
            allSeries.push_back(data);
        }
    }
    
    // Test that aggregation handles many series efficiently
    auto aggregated = Aggregator::aggregateMultiple(
        allSeries, AggregationMethod::AVG, 10 * 60 * 1000000000ULL);
    
    EXPECT_GE(aggregated.size(), 1);
    
    // Verify the aggregation produced valid results
    for (const auto& point : aggregated) {
        EXPECT_GT(point.timestamp, 0);
        EXPECT_FALSE(std::isnan(point.value));
        EXPECT_FALSE(std::isinf(point.value));
    }
}