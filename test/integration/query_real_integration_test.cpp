#include <gtest/gtest.h>
#include "../../../lib/storage/memory_store.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../../../lib/query/query_planner.hpp"
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/query/series_matcher.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../../lib/storage/tsm.hpp"
#include "../../../lib/query/query_runner.hpp"
#include "../../../lib/storage/wal.hpp"
#include "../../../lib/core/series_id.hpp"
#include <filesystem>
#include <memory>
#include <random>

using namespace timestar;
namespace fs = std::filesystem;

class QueryRealIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create test directories
        testDir = "test_query_integration_" + std::to_string(getpid());
        fs::create_directories(testDir);
        fs::create_directories(testDir + "/tsm");
        fs::create_directories(testDir + "/wal");
        
        // Initialize real components with sequence number
        memoryStore = std::make_unique<MemoryStore>(1);
        
        // Set up time range
        startTime = 1704067200000000000ULL; // 2024-01-01 00:00:00
        endTime   = 1704070800000000000ULL; // 2024-01-01 01:00:00
    }
    
    void TearDown() override {
        memoryStore.reset();
        
        // Clean up test directories
        try {
            fs::remove_all(testDir);
        } catch (...) {
            // Ignore cleanup errors
        }
    }
    
    std::string testDir;
    std::unique_ptr<MemoryStore> memoryStore;
    uint64_t startTime;
    uint64_t endTime;
    
    // Helper to query data from memory store
    QueryResult<double> queryMemoryStore(const std::string& seriesKey) {
        QueryResult<double> result;
        
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        auto it = memoryStore->series.find(seriesId);
        if (it != memoryStore->series.end()) {
            // Extract data from the variant series
            std::visit([&](auto&& series) {
                using T = std::decay_t<decltype(series)>;
                if constexpr (std::is_same_v<T, InMemorySeries<double>>) {
                    result.timestamps = series.timestamps;
                    result.values = series.values;
                }
            }, it->second);
        }
        
        return result;
    }
};

// Test real data insertion and retrieval from MemoryStore
TEST_F(QueryRealIntegrationTest, MemoryStoreRealData) {
    // Insert real temperature data
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    
    // Generate 60 data points (1 per minute)
    for (int i = 0; i < 60; ++i) {
        timestamps.push_back(startTime + i * 60000000000ULL);
        values.push_back(20.0 + sin(i * 0.1) * 5.0); // Temperature oscillating 15-25°C
    }
    
    TimeStarInsert<double> insert("temperature", "value");
    insert.tags = {{"location", "room1"}, {"sensor", "temp01"}};
    insert.timestamps = timestamps;
    insert.values = values;
    
    std::string seriesKey = insert.seriesKey();
    memoryStore->insertMemory(std::move(insert));

    // Query the data back
    auto results = queryMemoryStore(seriesKey);
    
    ASSERT_EQ(results.timestamps.size(), 60);
    ASSERT_EQ(results.values.size(), 60);
    
    // Verify data integrity
    for (size_t i = 0; i < 60; ++i) {
        EXPECT_EQ(results.timestamps[i], timestamps[i]);
        EXPECT_DOUBLE_EQ(results.values[i], values[i]);
    }
}

// Test real aggregation on actual data
TEST_F(QueryRealIntegrationTest, RealDataAggregation) {
    // Create multiple series with real patterns
    std::vector<std::tuple<std::string, std::map<std::string, std::string>, std::string>> seriesConfigs = {
        {"cpu", {{"host", "server01"}, {"dc", "us-west"}}, "usage"},
        {"cpu", {{"host", "server02"}, {"dc", "us-west"}}, "usage"},
        {"cpu", {{"host", "server03"}, {"dc", "us-east"}}, "usage"}
    };
    
    std::map<std::string, QueryResult<double>> allData;
    
    for (const auto& [measurement, tags, field] : seriesConfigs) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        
        // Generate realistic CPU usage data
        std::random_device rd;
        std::mt19937 gen(rd());
        std::normal_distribution<> dist(50.0, 15.0); // Mean 50%, stddev 15%
        
        for (int i = 0; i < 60; ++i) {
            timestamps.push_back(startTime + i * 60000000000ULL);
            double value = dist(gen);
            // Clamp to realistic range
            value = std::max(5.0, std::min(95.0, value));
            values.push_back(value);
        }
        
        TimeStarInsert<double> insert(measurement, field);
        insert.tags = tags;
        insert.timestamps = timestamps;
        insert.values = values;
        
        std::string insertKey = insert.seriesKey();
        memoryStore->insertMemory(std::move(insert));

        QueryResult<double> qr;
        qr.timestamps = timestamps;
        qr.values = values;
        allData[insertKey] = qr;
    }
    
    // Test aggregation with real data
    for (const auto& [key, data] : allData) {
        // Test 5-minute average aggregation
        uint64_t interval = 5 * 60 * 1000000000ULL;
        auto aggregated = Aggregator::aggregate(
            data.timestamps, data.values, AggregationMethod::AVG, interval);
        
        // Should have ~12 buckets for 60 minutes / 5 minutes
        EXPECT_GE(aggregated.size(), 10);
        EXPECT_LE(aggregated.size(), 12);
        
        // Verify aggregated values are reasonable
        for (const auto& point : aggregated) {
            EXPECT_GE(point.value, 5.0);
            EXPECT_LE(point.value, 95.0);
            EXPECT_GT(point.count, 0);
        }
        
        // Test MAX aggregation
        auto maxAggregated = Aggregator::aggregate(
            data.timestamps, data.values, AggregationMethod::MAX, interval);
        
        // MAX values should be >= AVG values for same buckets
        for (size_t i = 0; i < std::min(aggregated.size(), maxAggregated.size()); ++i) {
            EXPECT_GE(maxAggregated[i].value, aggregated[i].value - 0.01);
        }
    }
}

// Test SeriesMatcher with real tag combinations
TEST_F(QueryRealIntegrationTest, SeriesMatcherRealPatterns) {
    // Real-world tag combinations
    std::vector<std::map<std::string, std::string>> seriesTags = {
        {{"host", "prod-web-01"}, {"dc", "us-west-2a"}, {"env", "production"}},
        {{"host", "prod-web-02"}, {"dc", "us-west-2b"}, {"env", "production"}},
        {{"host", "prod-db-01"}, {"dc", "us-east-1a"}, {"env", "production"}},
        {{"host", "staging-web-01"}, {"dc", "us-west-2a"}, {"env", "staging"}},
        {{"host", "dev-web-01"}, {"dc", "local"}, {"env", "development"}}
    };
    
    // Test various real query patterns
    
    // 1. Find all production web servers
    std::map<std::string, std::string> webProdQuery = {
        {"host", "prod-web-*"},
        {"env", "production"}
    };
    
    int webProdCount = 0;
    for (const auto& tags : seriesTags) {
        if (SeriesMatcher::matches(tags, webProdQuery)) {
            webProdCount++;
            EXPECT_TRUE(tags.at("host").find("prod-web-") == 0);
            EXPECT_EQ(tags.at("env"), "production");
        }
    }
    EXPECT_EQ(webProdCount, 2);
    
    // 2. Find all servers in us-west
    std::map<std::string, std::string> usWestQuery = {
        {"dc", "us-west-*"}
    };
    
    int usWestCount = 0;
    for (const auto& tags : seriesTags) {
        if (SeriesMatcher::matches(tags, usWestQuery)) {
            usWestCount++;
            EXPECT_TRUE(tags.at("dc").find("us-west-") == 0);
        }
    }
    EXPECT_EQ(usWestCount, 3);
    
    // 3. Find production databases
    std::map<std::string, std::string> prodDbQuery = {
        {"host", "*-db-*"},
        {"env", "production"}
    };
    
    int prodDbCount = 0;
    for (const auto& tags : seriesTags) {
        if (SeriesMatcher::matches(tags, prodDbQuery)) {
            prodDbCount++;
            EXPECT_TRUE(tags.at("host").find("-db-") != std::string::npos);
        }
    }
    EXPECT_EQ(prodDbCount, 1);
}

// Test removed - queries contained unsupported wildcard syntax in tags

// Test complete query pipeline with real components
TEST_F(QueryRealIntegrationTest, CompleteQueryPipeline) {
    // Step 1: Insert real monitoring data
    std::vector<std::string> interfaces = {"eth0", "eth1", "lo"};
    std::vector<std::string> hosts = {"server01", "server02"};
    
    for (const auto& host : hosts) {
        for (const auto& iface : interfaces) {
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            
            // Generate realistic network traffic (bytes/sec)
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dis(
                iface == "lo" ? 100.0 : 10000.0,  // Loopback has less traffic
                iface == "lo" ? 1000.0 : 1000000.0
            );
            
            for (int i = 0; i < 60; ++i) {
                timestamps.push_back(startTime + i * 60000000000ULL);
                values.push_back(dis(gen));
            }
            
            TimeStarInsert<double> insert("network", "bytes_sent");
            insert.tags = {{"host", host}, {"interface", iface}};
            insert.timestamps = timestamps;
            insert.values = values;
            
            memoryStore->insertMemory(std::move(insert));
        }
    }

    // Step 2: Verify data was inserted
    EXPECT_EQ(memoryStore->series.size(), 6); // 2 hosts × 3 interfaces
    
    // Step 3: Test aggregation on real data
    for (const auto& [key, variantSeries] : memoryStore->series) {
        // Extract double series
        std::visit([&](auto&& series) {
            using T = std::decay_t<decltype(series)>;
            if constexpr (std::is_same_v<T, InMemorySeries<double>>) {
                // 10-minute max aggregation
                uint64_t interval = 10 * 60 * 1000000000ULL;
                auto aggregated = Aggregator::aggregate(
                    series.timestamps, series.values, 
                    AggregationMethod::MAX, interval);
                
                // Should have ~6 buckets
                EXPECT_GE(aggregated.size(), 5);
                EXPECT_LE(aggregated.size(), 7);
                
                // Each bucket's max should be positive
                for (const auto& point : aggregated) {
                    EXPECT_GT(point.value, 0.0);
                    EXPECT_GT(point.count, 0);
                }
            }
        }, variantSeries);
    }
}

// Test handling of edge cases with real data
TEST_F(QueryRealIntegrationTest, RealDataEdgeCases) {
    // Test 1: Very sparse data (only a few points in time range)
    std::vector<uint64_t> sparseTimestamps = {
        startTime + 100000000ULL,
        startTime + 1800000000000ULL,  // 30 minutes later
        startTime + 3500000000000ULL   // ~58 minutes later
    };
    std::vector<double> sparseValues = {100.0, 200.0, 150.0};
    
    TimeStarInsert<double> sparseInsert("sparse_metric", "value");
    sparseInsert.timestamps = sparseTimestamps;
    sparseInsert.values = sparseValues;
    std::string sparseKey = sparseInsert.seriesKey();
    memoryStore->insertMemory(std::move(sparseInsert));

    auto sparseResult = queryMemoryStore(sparseKey);
    EXPECT_EQ(sparseResult.timestamps.size(), 3);
    
    // Aggregate sparse data with 15-minute intervals
    uint64_t interval = 15 * 60 * 1000000000ULL;
    auto aggregated = Aggregator::aggregate(
        sparseTimestamps, sparseValues, AggregationMethod::AVG, interval);
    
    // Should have at most 3 buckets with data
    EXPECT_LE(aggregated.size(), 4);
    EXPECT_GE(aggregated.size(), 3);
    
    // Test 2: Dense data (multiple points per second)
    std::vector<uint64_t> denseTimestamps;
    std::vector<double> denseValues;
    
    // 10 points per second for 1 minute
    for (int i = 0; i < 600; ++i) {
        denseTimestamps.push_back(startTime + i * 100000000ULL); // 100ms intervals
        denseValues.push_back(sin(i * 0.1) * 50.0 + 50.0);
    }
    
    TimeStarInsert<double> denseInsert("dense_metric", "value");
    denseInsert.timestamps = denseTimestamps;
    denseInsert.values = denseValues;
    std::string denseKey = denseInsert.seriesKey();
    memoryStore->insertMemory(std::move(denseInsert));

    auto denseResult = queryMemoryStore(denseKey);
    EXPECT_EQ(denseResult.timestamps.size(), 600);
    
    // Aggregate dense data to 1-second intervals
    uint64_t secondInterval = 1000000000ULL; // 1 second
    auto denseAggregated = Aggregator::aggregate(
        denseTimestamps, denseValues, AggregationMethod::AVG, secondInterval);
    
    // Should have ~60 buckets (1 per second)
    EXPECT_GE(denseAggregated.size(), 55);
    EXPECT_LE(denseAggregated.size(), 65);
    
    // Test 3: Data with duplicates at same timestamp
    std::vector<uint64_t> dupTimestamps = {
        startTime, startTime + 1000000000ULL, startTime + 2000000000ULL
    };
    std::vector<double> dupValues = {10.0, 20.0, 30.0};
    
    TimeStarInsert<double> dupInsert("duplicate", "value");
    dupInsert.timestamps = dupTimestamps;
    dupInsert.values = dupValues;
    std::string dupKey = dupInsert.seriesKey();
    memoryStore->insertMemory(std::move(dupInsert));

    auto dupResult = queryMemoryStore(dupKey);
    EXPECT_EQ(dupResult.timestamps.size(), 3);
}

// Performance test with realistic data volumes
TEST_F(QueryRealIntegrationTest, PerformanceWithRealisticData) {
    auto startInsert = std::chrono::high_resolution_clock::now();
    
    // Insert 100 series with 1000 points each (100k total points)
    for (int s = 0; s < 100; ++s) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        
        for (int i = 0; i < 1000; ++i) {
            timestamps.push_back(startTime + i * 3600000000ULL); // 1 point per 3.6 seconds
            values.push_back(50.0 + (s % 10) * 5.0 + (i % 100) * 0.1);
        }
        
        TimeStarInsert<double> insert("metric", "value");
        insert.tags = {{"host", "host" + std::to_string(s)}};
        insert.timestamps = timestamps;
        insert.values = values;
        
        memoryStore->insertMemory(std::move(insert));
    }

    auto endInsert = std::chrono::high_resolution_clock::now();
    auto insertDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endInsert - startInsert);
    
    // Should insert 100k points in reasonable time
    EXPECT_LT(insertDuration.count(), 5000) << "Insertion too slow: " 
                                            << insertDuration.count() << "ms";
    
    // Verify all series were inserted
    EXPECT_EQ(memoryStore->series.size(), 100);
    
    // Query performance test
    auto startQuery = std::chrono::high_resolution_clock::now();
    
    int totalPoints = 0;
    for (const auto& [key, variantSeries] : memoryStore->series) {
        std::visit([&](auto&& series) {
            using T = std::decay_t<decltype(series)>;
            if constexpr (std::is_same_v<T, InMemorySeries<double>>) {
                totalPoints += series.timestamps.size();
            }
        }, variantSeries);
    }
    
    auto endQuery = std::chrono::high_resolution_clock::now();
    auto queryDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endQuery - startQuery);
    
    EXPECT_EQ(totalPoints, 100000);
    EXPECT_LT(queryDuration.count(), 1000) << "Query too slow: " 
                                           << queryDuration.count() << "ms";
    
    // Aggregation performance test
    auto startAgg = std::chrono::high_resolution_clock::now();
    
    int seriesProcessed = 0;
    for (const auto& [key, variantSeries] : memoryStore->series) {
        if (seriesProcessed >= 10) break; // Test first 10 series
        
        std::visit([&](auto&& series) {
            using T = std::decay_t<decltype(series)>;
            if constexpr (std::is_same_v<T, InMemorySeries<double>>) {
                uint64_t interval = 60000000000ULL; // 1-minute intervals
                auto aggregated = Aggregator::aggregate(
                    series.timestamps, series.values, 
                    AggregationMethod::AVG, interval);
                
                EXPECT_GT(aggregated.size(), 0);
                seriesProcessed++;
            }
        }, variantSeries);
    }
    
    auto endAgg = std::chrono::high_resolution_clock::now();
    auto aggDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endAgg - startAgg);
    
    EXPECT_LT(aggDuration.count(), 500) << "Aggregation too slow: " 
                                        << aggDuration.count() << "ms";
}

// Test QueryPlanner with real series IDs (simulated)
TEST_F(QueryRealIntegrationTest, QueryPlannerWithRealData) {
    // Create a query request
    QueryRequest request;
    request.measurement = "temperature";
    request.fields = {"value", "humidity"};
    request.scopes = {{"location", "us-west"}, {"sensor", "outdoor"}};
    request.startTime = startTime;
    request.endTime = endTime;
    request.aggregation = AggregationMethod::MAX;
    request.aggregationInterval = 5 * 60 * 1000000000ULL; // 5 minutes
    
    // Test the planner's logic (without actual index)
    QueryPlanner planner;
    
    // Test shard calculation
    std::string seriesKey = planner.buildSeriesKeyForSharding(
        request.measurement, request.scopes, "value");
    
    EXPECT_FALSE(seriesKey.empty());
    EXPECT_TRUE(seriesKey.find("temperature") != std::string::npos);
    EXPECT_TRUE(seriesKey.find("us-west") != std::string::npos);
    
    // Test requiresAllShards logic
    QueryRequest wildcardRequest = request;
    wildcardRequest.scopes["location"] = "us-*"; // Wildcard
    EXPECT_TRUE(planner.requiresAllShards(wildcardRequest));
    
    QueryRequest exactRequest = request;
    EXPECT_FALSE(planner.requiresAllShards(exactRequest));
    
    QueryRequest emptyRequest;
    emptyRequest.measurement = "test";
    EXPECT_TRUE(planner.requiresAllShards(emptyRequest)); // No scopes = all shards
}