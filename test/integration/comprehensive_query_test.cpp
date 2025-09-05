#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include "../../../lib/storage/engine.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include <random>
#include <chrono>
#include <cstdlib>

using namespace seastar;
using namespace tsdb;

class ComprehensiveQueryTest : public ::testing::Test {
protected:
    static std::unique_ptr<Engine> engine;
    static uint64_t baseTime;
    static std::string testMeasurement;
    
    static void SetUpTestSuite() {
        baseTime = std::chrono::system_clock::now().time_since_epoch().count();
        
        // Generate random measurement name
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(1000, 9999);
        testMeasurement = "test_metrics_" + std::to_string(dis(gen));
    }
    
    static void TearDownTestSuite() {
        // Clean up test data directories
        if (engine) {
            engine.reset();
        }
        std::system("rm -rf shard_* 2>/dev/null");
    }
    
    // Helper to create test timestamps (100 points, 1 second apart)
    static std::vector<uint64_t> createTestTimestamps(int count = 100) {
        std::vector<uint64_t> timestamps;
        for (int i = 0; i < count; i++) {
            timestamps.push_back(baseTime - (count - i - 1) * 1000000000ULL);
        }
        return timestamps;
    }
    
    // Helper to create test field data with pattern
    static std::vector<double> createTestFieldData(int count, double multiplier) {
        std::vector<double> values;
        for (int i = 0; i < count; i++) {
            values.push_back(0.1 * multiplier * i);
        }
        return values;
    }
    
    // Helper to calculate average of multiple vectors
    static std::vector<double> calculateAverage(const std::vector<std::vector<double>>& arrays) {
        if (arrays.empty() || arrays[0].empty()) return {};
        
        size_t len = arrays[0].size();
        std::vector<double> result(len, 0.0);
        
        for (const auto& arr : arrays) {
            for (size_t i = 0; i < arr.size() && i < len; i++) {
                result[i] += arr[i];
            }
        }
        
        for (auto& val : result) {
            val /= arrays.size();
        }
        
        return result;
    }
    
    // Check if values are close (within 5% tolerance)
    static bool isCloseTo(double a, double b, double tolerance = 0.05) {
        if (b == 0) return std::abs(a) < tolerance;
        return std::abs(a - b) / std::abs(b) < tolerance;
    }
    
    static bool vectorsClose(const std::vector<double>& a, const std::vector<double>& b, double tolerance = 0.05) {
        if (a.size() != b.size()) return false;
        for (size_t i = 0; i < a.size(); i++) {
            if (!isCloseTo(a[i], b[i], tolerance)) return false;
        }
        return true;
    }
};

std::unique_ptr<Engine> ComprehensiveQueryTest::engine;
uint64_t ComprehensiveQueryTest::baseTime;
std::string ComprehensiveQueryTest::testMeasurement;

// Test fixture for async tests
class AsyncQueryTest : public ComprehensiveQueryTest {
protected:
    static future<> insertTestData() {
        auto timestamps = createTestTimestamps(100);
        
        // Insert data for device aaaaa
        co_await engine->insert(testMeasurement + ".moisture", 
            "deviceId=aaaaa,paddock=back-paddock", "value1", 
            timestamps, createTestFieldData(100, 1.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=aaaaa,paddock=back-paddock", "value2",
            timestamps, createTestFieldData(100, 2.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=aaaaa,paddock=back-paddock", "value3",
            timestamps, createTestFieldData(100, 3.0));
        
        // Insert data for device bbbbb
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=bbbbb,paddock=back-paddock", "value1",
            timestamps, createTestFieldData(100, 4.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=bbbbb,paddock=back-paddock", "value2",
            timestamps, createTestFieldData(100, 5.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=bbbbb,paddock=back-paddock", "value3",
            timestamps, createTestFieldData(100, 6.0));
        
        // Insert data for device ccccc (different paddock)
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=ccccc,paddock=front-paddock", "value1",
            timestamps, createTestFieldData(100, 7.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=ccccc,paddock=front-paddock", "value2",
            timestamps, createTestFieldData(100, 8.0));
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=ccccc,paddock=front-paddock", "value3",
            timestamps, createTestFieldData(100, 9.0));
    }
    
    static future<> insertImageData() {
        std::vector<uint64_t> timestamps = {
            baseTime - 2000000000ULL,  // 2 seconds ago
            baseTime - 1000000000ULL   // 1 second ago
        };
        
        // Simulate binary/image data as strings (base64 encoded references)
        std::vector<std::string> imageRefs = {
            "ref::image1::s3://bucket/image1.jpeg",
            "ref::image2::s3://bucket/image2.jpeg"
        };
        
        co_await engine->insert(testMeasurement + ".images",
            "deviceId=camera", "image",
            timestamps, imageRefs);
    }
    
    static future<> insertBooleanData() {
        std::vector<uint64_t> timestamps = {
            baseTime - 2000000000ULL,  // 2 seconds ago
            baseTime - 1000000000ULL   // 1 second ago
        };
        
        std::vector<bool> values = {true, false};
        
        co_await engine->insert(testMeasurement + ".boolean",
            "deviceId=sensor", "value",
            timestamps, values);
    }
};

// Test MIN aggregation function
TEST_F(AsyncQueryTest, MinAggregationQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query with MIN aggregation
        std::string query = "min:" + testMeasurement + ".moisture(){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // Should have all three fields
        EXPECT_EQ(series.fields.size(), 3);
        ASSERT_TRUE(series.fields.find("value1") != series.fields.end());
        ASSERT_TRUE(series.fields.find("value2") != series.fields.end());
        ASSERT_TRUE(series.fields.find("value3") != series.fields.end());
        
        // MIN should return device aaaaa's values (lowest multiplier)
        auto expectedValue1 = createTestFieldData(100, 1.0);
        auto expectedValue2 = createTestFieldData(100, 2.0);
        auto expectedValue3 = createTestFieldData(100, 3.0);
        
        EXPECT_TRUE(vectorsClose(series.fields["value1"].second, expectedValue1));
        EXPECT_TRUE(vectorsClose(series.fields["value2"].second, expectedValue2));
        EXPECT_TRUE(vectorsClose(series.fields["value3"].second, expectedValue3));
        
        engine->stop().get();
    });
}

// Test MAX aggregation function
TEST_F(AsyncQueryTest, MaxAggregationQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query with MAX aggregation
        std::string query = "max:" + testMeasurement + ".moisture(){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // MAX should return device ccccc's values (highest multiplier)
        auto expectedValue1 = createTestFieldData(100, 7.0);
        auto expectedValue2 = createTestFieldData(100, 8.0);
        auto expectedValue3 = createTestFieldData(100, 9.0);
        
        EXPECT_TRUE(vectorsClose(series.fields["value1"].second, expectedValue1));
        EXPECT_TRUE(vectorsClose(series.fields["value2"].second, expectedValue2));
        EXPECT_TRUE(vectorsClose(series.fields["value3"].second, expectedValue3));
        
        engine->stop().get();
    });
}

// Test AVG aggregation function (default)
TEST_F(AsyncQueryTest, AvgAggregationQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query with AVG aggregation (no explicit aggregation method defaults to avg)
        std::string query = "avg:" + testMeasurement + ".moisture(){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // AVG should return average of all three devices
        auto device1Value1 = createTestFieldData(100, 1.0);
        auto device2Value1 = createTestFieldData(100, 4.0);
        auto device3Value1 = createTestFieldData(100, 7.0);
        auto expectedValue1 = calculateAverage({device1Value1, device2Value1, device3Value1});
        
        EXPECT_TRUE(vectorsClose(series.fields["value1"].second, expectedValue1));
        
        engine->stop().get();
    });
}

// Test query with specific fields
TEST_F(AsyncQueryTest, QueryWithSpecificFields) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query only value1 and value2 fields
        std::string query = "avg:" + testMeasurement + ".moisture(value1,value2){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // Should only have value1 and value2
        EXPECT_EQ(series.fields.size(), 2);
        EXPECT_TRUE(series.fields.find("value1") != series.fields.end());
        EXPECT_TRUE(series.fields.find("value2") != series.fields.end());
        EXPECT_FALSE(series.fields.find("value3") != series.fields.end());
        
        engine->stop().get();
    });
}

// Test query with scope filtering
TEST_F(AsyncQueryTest, QueryWithScopeFilter) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query only back-paddock data
        std::string query = "avg:" + testMeasurement + ".moisture(value1,value2,value3){paddock:back-paddock}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // Should average only aaaaa and bbbbb (both in back-paddock)
        auto device1Value1 = createTestFieldData(100, 1.0);
        auto device2Value1 = createTestFieldData(100, 4.0);
        auto expectedValue1 = calculateAverage({device1Value1, device2Value1});
        
        EXPECT_TRUE(vectorsClose(series.fields["value1"].second, expectedValue1));
        
        engine->stop().get();
    });
}

// Test query with group by
TEST_F(AsyncQueryTest, QueryWithGroupBy) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Group by deviceId
        std::string query = "avg:" + testMeasurement + ".moisture(value1){paddock:back-paddock} by {deviceId}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        // Should have 2 series (aaaaa and bbbbb, both in back-paddock)
        ASSERT_EQ(result.series.size(), 2);
        
        // Find the series for each device
        SeriesResult* deviceA = nullptr;
        SeriesResult* deviceB = nullptr;
        
        for (auto& series : result.series) {
            if (series.tags["deviceId"] == "aaaaa") deviceA = &series;
            if (series.tags["deviceId"] == "bbbbb") deviceB = &series;
        }
        
        ASSERT_NE(deviceA, nullptr);
        ASSERT_NE(deviceB, nullptr);
        
        // Check values
        auto expectedA = createTestFieldData(100, 1.0);
        auto expectedB = createTestFieldData(100, 4.0);
        
        EXPECT_TRUE(vectorsClose(deviceA->fields["value1"].second, expectedA));
        EXPECT_TRUE(vectorsClose(deviceB->fields["value1"].second, expectedB));
        
        engine->stop().get();
    });
}

// Test boolean data type
TEST_F(AsyncQueryTest, BooleanDataQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertBooleanData().get();
        
        std::string query = "latest:" + testMeasurement + ".boolean(){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 10000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        ASSERT_TRUE(series.fields.find("value") != series.fields.end());
        auto& values = series.fields["value"].second;
        
        // Latest aggregation with boolean should preserve the values
        ASSERT_EQ(values.size(), 2);
        // Note: booleans are stored as 1.0 and 0.0 in double format
        EXPECT_EQ(values[0], 1.0);  // true
        EXPECT_EQ(values[1], 0.0);  // false
        
        engine->stop().get();
    });
}

// Test string/binary data (images)
TEST_F(AsyncQueryTest, ImageDataQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertImageData().get();
        
        std::string query = "latest:" + testMeasurement + ".images(){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 10000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        ASSERT_TRUE(series.fields.find("image") != series.fields.end());
        // String fields stored separately in string_fields
        ASSERT_TRUE(series.string_fields.find("image") != series.string_fields.end());
        
        auto& imageRefs = series.string_fields["image"].second;
        ASSERT_EQ(imageRefs.size(), 2);
        
        // Check the references match pattern
        EXPECT_TRUE(imageRefs[0].find("ref::") == 0);
        EXPECT_TRUE(imageRefs[1].find("ref::") == 0);
        EXPECT_TRUE(imageRefs[0].find("s3://") != std::string::npos);
        EXPECT_TRUE(imageRefs[1].find("s3://") != std::string::npos);
        
        engine->stop().get();
    });
}

// Test with time range filtering
TEST_F(AsyncQueryTest, TimeRangeFiltering) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query only last 50 seconds of data
        uint64_t midPoint = baseTime - 50000000000ULL;
        std::string query = "avg:" + testMeasurement + ".moisture(value1){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, midPoint, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        auto& timestamps = series.fields["value1"].first;
        // Should only have approximately 50 points
        EXPECT_LE(timestamps.size(), 51);
        EXPECT_GE(timestamps.size(), 49);
        
        // All timestamps should be within range
        for (auto ts : timestamps) {
            EXPECT_GE(ts, midPoint);
            EXPECT_LE(ts, baseTime);
        }
        
        engine->stop().get();
    });
}

// Test SUM aggregation
TEST_F(AsyncQueryTest, SumAggregationQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        std::string query = "sum:" + testMeasurement + ".moisture(value1){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // SUM should add all three devices' values
        auto device1 = createTestFieldData(100, 1.0);
        auto device2 = createTestFieldData(100, 4.0);
        auto device3 = createTestFieldData(100, 7.0);
        
        std::vector<double> expectedSum(100);
        for (size_t i = 0; i < 100; i++) {
            expectedSum[i] = device1[i] + device2[i] + device3[i];
        }
        
        EXPECT_TRUE(vectorsClose(series.fields["value1"].second, expectedSum));
        
        engine->stop().get();
    });
}

// Test LATEST aggregation
TEST_F(AsyncQueryTest, LatestAggregationQuery) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        std::string query = "latest:" + testMeasurement + ".moisture(value1){}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        auto& timestamps = series.fields["value1"].first;
        auto& values = series.fields["value1"].second;
        
        // Latest should return the most recent value from each series
        // In our case, it should aggregate the last points from all series
        EXPECT_GE(timestamps.size(), 1);
        
        // The last timestamp should be close to baseTime
        if (!timestamps.empty()) {
            EXPECT_LE(std::abs((long long)(timestamps.back() - baseTime)), 1000000000LL);
        }
        
        engine->stop().get();
    });
}

// Test query error cases
TEST_F(AsyncQueryTest, InvalidQueryErrors) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        // Missing aggregation method
        EXPECT_THROW(
            QueryParser::parseQueryString(testMeasurement + ".moisture(){}"),
            QueryParseException
        );
        
        // Invalid aggregation method
        EXPECT_THROW(
            QueryParser::parseQueryString("invalid:" + testMeasurement + ".moisture(){}"),
            QueryParseException
        );
        
        // Missing measurement
        EXPECT_THROW(
            QueryParser::parseQueryString("avg:(value1){}"),
            QueryParseException
        );
        
        // Missing fields parentheses
        EXPECT_THROW(
            QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture{}"),
            QueryParseException
        );
        
        // Unclosed scope brace
        EXPECT_THROW(
            QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture(){location:us-west"),
            QueryParseException
        );
        
        // Malformed group by
        EXPECT_THROW(
            QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture(){} by deviceId}"),
            QueryParseException
        );
        
        engine->stop().get();
    });
}

// Test inserting new data and cache invalidation
TEST_F(AsyncQueryTest, CacheInvalidationAfterInsert) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // First query
        std::string query = "avg:" + testMeasurement + ".moisture(){} by {deviceId}";
        auto result1 = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result1.series.size(), 3); // aaaaa, bbbbb, ccccc
        
        // Insert new device data
        auto timestamps = createTestTimestamps(100);
        co_await engine->insert(testMeasurement + ".moisture",
            "deviceId=zzzzzz,paddock=back-paddock", "value1",
            timestamps, createTestFieldData(100, 1.0));
        
        // Query again
        auto result2 = HttpQueryHandler::executeQuery(
            *engine, query, baseTime - 100000000000ULL, baseTime).get();
        
        ASSERT_EQ(result2.series.size(), 4); // aaaaa, bbbbb, ccccc, zzzzzz
        
        // Verify new device is in results
        bool foundNewDevice = false;
        for (const auto& series : result2.series) {
            if (series.tags.at("deviceId") == "zzzzzz") {
                foundNewDevice = true;
                break;
            }
        }
        EXPECT_TRUE(foundNewDevice);
        
        engine->stop().get();
    });
}

// Test fields with same prefix
TEST_F(AsyncQueryTest, FieldsWithSamePrefix) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        // Insert data with field names that share prefixes
        std::vector<uint64_t> timestamps = {1, 2, 3, 4, 5, 6};
        std::vector<double> pnfValues = {0, 0, 0, 0, 0, 0};
        std::vector<double> pnfStatusValues = {1, 1, 1, 1, 1, 1};
        
        co_await engine->insert("lid_data",
            "meter_id=33616", "pnf",
            timestamps, pnfValues);
        co_await engine->insert("lid_data",
            "meter_id=33616", "pnf_status",
            timestamps, pnfStatusValues);
        
        // Query only the pnf field
        std::string query = "avg:lid_data(pnf){meter_id:33616}";
        auto result = HttpQueryHandler::executeQuery(
            *engine, query, 0, 1000).get();
        
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];
        
        // Should only have pnf field, not pnf_status
        EXPECT_EQ(series.fields.size(), 1);
        EXPECT_TRUE(series.fields.find("pnf") != series.fields.end());
        EXPECT_FALSE(series.fields.find("pnf_status") != series.fields.end());
        
        auto& values = series.fields["pnf"].second;
        for (auto val : values) {
            EXPECT_EQ(val, 0.0);
        }
        
        engine->stop().get();
    });
}

// Test with aggregation intervals
TEST_F(AsyncQueryTest, AggregationWithTimeIntervals) {
    seastar::thread([this] {
        engine = std::make_unique<Engine>(".", 1);
        engine->start().get();
        
        insertTestData().get();
        
        // Query with 10-second intervals
        rapidjson::Document request;
        request.SetObject();
        auto& allocator = request.GetAllocator();
        
        std::string queryStr = "avg:" + testMeasurement + ".moisture(value1){}";
        request.AddMember("query", rapidjson::Value(queryStr.c_str(), allocator), allocator);
        request.AddMember("startTime", baseTime - 100000000000ULL, allocator);
        request.AddMember("endTime", baseTime, allocator);
        request.AddMember("aggregationInterval", "10s", allocator);
        
        auto result = HttpQueryHandler::handleQueryRequest(*engine, request).get();
        
        // Parse response
        rapidjson::Document response;
        response.Parse(result.c_str());
        
        ASSERT_TRUE(response.HasMember("series"));
        const auto& series = response["series"].GetArray();
        ASSERT_EQ(series.Size(), 1);
        
        const auto& fields = series[0]["fields"].GetObject();
        ASSERT_TRUE(fields.HasMember("value1"));
        
        const auto& timestamps = fields["value1"]["timestamps"].GetArray();
        // With 100 seconds of data and 10-second intervals, expect ~10 buckets
        EXPECT_GE(timestamps.Size(), 9);
        EXPECT_LE(timestamps.Size(), 11);
        
        engine->stop().get();
    });
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    
    seastar::app_template app;
    
    return app.run(argc, argv, [] {
        return seastar::async([] {
            auto result = RUN_ALL_TESTS();
            seastar::engine().exit(result);
        });
    });
}