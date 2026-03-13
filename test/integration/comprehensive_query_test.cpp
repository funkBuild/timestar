#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/query/query_parser.hpp"
#include "../test_helpers.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <random>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>

using namespace seastar;
using namespace timestar;

class ComprehensiveQueryTest : public ::testing::Test {
protected:
    static seastar::sharded<Engine>* engineSharded;
    static std::unique_ptr<timestar::HttpQueryHandler> queryHandler;
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
        if (queryHandler) {
            queryHandler.reset();
        }
        if (engineSharded) {
            engineSharded = nullptr;
        }
        cleanTestShardDirectories();
    }

    void SetUp() override {
        // Clean shard directories before each test to prevent TSM file leakage
        // between tests that each create their own ScopedShardedEngine
        cleanTestShardDirectories();
    }

    void TearDown() override { cleanTestShardDirectories(); }

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
        if (arrays.empty() || arrays[0].empty())
            return {};

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
        if (b == 0)
            return std::abs(a) < tolerance;
        return std::abs(a - b) / std::abs(b) < tolerance;
    }

    static bool vectorsClose(const std::vector<double>& a, const std::vector<double>& b, double tolerance = 0.05) {
        if (a.size() != b.size())
            return false;
        for (size_t i = 0; i < a.size(); i++) {
            if (!isCloseTo(a[i], b[i], tolerance))
                return false;
        }
        return true;
    }

    // Helper to extract double values from FieldValues variant
    static std::vector<double> getDoubleValues(const FieldValues& fv) {
        if (std::holds_alternative<std::vector<double>>(fv)) {
            return std::get<std::vector<double>>(fv);
        }
        return {};
    }

    // Helper to extract string values from FieldValues variant
    static std::vector<std::string> getStringValues(const FieldValues& fv) {
        if (std::holds_alternative<std::vector<std::string>>(fv)) {
            return std::get<std::vector<std::string>>(fv);
        }
        return {};
    }

    // Helper to execute query via HttpQueryHandler
    static QueryResponse executeQuery(const std::string& queryStr, uint64_t startTime, uint64_t endTime) {
        auto request = QueryParser::parseQueryString(queryStr);
        request.startTime = startTime;
        request.endTime = endTime;
        return queryHandler->executeQuery(request).get();
    }

    // Helper to insert test data using shardedInsert for correct shard routing
    static void insertTestDataSync(seastar::sharded<Engine>& eng) {
        auto timestamps = createTestTimestamps(100);
        auto insertSeries = [&](const std::string& deviceId, const std::string& paddock, const std::string& fieldName,
                                double multiplier) {
            TimeStarInsert<double> insert(testMeasurement + ".moisture", fieldName);
            insert.addTag("deviceId", deviceId);
            insert.addTag("paddock", paddock);
            auto fieldData = createTestFieldData(100, multiplier);
            for (int i = 0; i < 100; i++) {
                insert.addValue(timestamps[i], fieldData[i]);
            }
            shardedInsert(eng, std::move(insert));
        };

        // Insert data for device aaaaa
        insertSeries("aaaaa", "back-paddock", "value1", 1.0);
        insertSeries("aaaaa", "back-paddock", "value2", 2.0);
        insertSeries("aaaaa", "back-paddock", "value3", 3.0);

        // Insert data for device bbbbb
        insertSeries("bbbbb", "back-paddock", "value1", 4.0);
        insertSeries("bbbbb", "back-paddock", "value2", 5.0);
        insertSeries("bbbbb", "back-paddock", "value3", 6.0);

        // Insert data for device ccccc (different paddock)
        insertSeries("ccccc", "front-paddock", "value1", 7.0);
        insertSeries("ccccc", "front-paddock", "value2", 8.0);
        insertSeries("ccccc", "front-paddock", "value3", 9.0);
    }

    static void insertImageDataSync(seastar::sharded<Engine>& eng) {
        TimeStarInsert<std::string> insert(testMeasurement + ".images", "image");
        insert.addTag("deviceId", "camera");
        insert.addValue(baseTime - 2000000000ULL, "ref::image1::s3://bucket/image1.jpeg");
        insert.addValue(baseTime - 1000000000ULL, "ref::image2::s3://bucket/image2.jpeg");
        shardedInsert(eng, std::move(insert));
    }

    static void insertBooleanDataSync(seastar::sharded<Engine>& eng) {
        TimeStarInsert<bool> insert(testMeasurement + ".boolean", "value");
        insert.addTag("deviceId", "sensor");
        insert.addValue(baseTime - 2000000000ULL, true);
        insert.addValue(baseTime - 1000000000ULL, false);
        shardedInsert(eng, std::move(insert));
    }
};

seastar::sharded<Engine>* ComprehensiveQueryTest::engineSharded = nullptr;
std::unique_ptr<timestar::HttpQueryHandler> ComprehensiveQueryTest::queryHandler;
uint64_t ComprehensiveQueryTest::baseTime;
std::string ComprehensiveQueryTest::testMeasurement;

// Test fixture for async tests
class AsyncQueryTest : public ComprehensiveQueryTest {};

// Test MIN aggregation function
TEST_F(AsyncQueryTest, MinAggregationQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query with MIN aggregation
        std::string query = "min:" + testMeasurement + ".moisture(){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

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

        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value1"].second), expectedValue1));
        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value2"].second), expectedValue2));
        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value3"].second), expectedValue3));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test MAX aggregation function
TEST_F(AsyncQueryTest, MaxAggregationQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query with MAX aggregation
        std::string query = "max:" + testMeasurement + ".moisture(){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        // MAX should return device ccccc's values (highest multiplier)
        auto expectedValue1 = createTestFieldData(100, 7.0);
        auto expectedValue2 = createTestFieldData(100, 8.0);
        auto expectedValue3 = createTestFieldData(100, 9.0);

        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value1"].second), expectedValue1));
        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value2"].second), expectedValue2));
        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value3"].second), expectedValue3));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test AVG aggregation function (default)
TEST_F(AsyncQueryTest, AvgAggregationQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query with AVG aggregation (no explicit aggregation method defaults to avg)
        std::string query = "avg:" + testMeasurement + ".moisture(){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        // AVG should return average of all three devices
        auto device1Value1 = createTestFieldData(100, 1.0);
        auto device2Value1 = createTestFieldData(100, 4.0);
        auto device3Value1 = createTestFieldData(100, 7.0);
        auto expectedValue1 = calculateAverage({device1Value1, device2Value1, device3Value1});

        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value1"].second), expectedValue1));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test query with specific fields
TEST_F(AsyncQueryTest, QueryWithSpecificFields) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query only value1 and value2 fields
        std::string query = "avg:" + testMeasurement + ".moisture(value1,value2){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        // Should only have value1 and value2
        EXPECT_EQ(series.fields.size(), 2);
        EXPECT_TRUE(series.fields.find("value1") != series.fields.end());
        EXPECT_TRUE(series.fields.find("value2") != series.fields.end());
        EXPECT_FALSE(series.fields.find("value3") != series.fields.end());

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test query with scope filtering
TEST_F(AsyncQueryTest, QueryWithScopeFilter) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query only back-paddock data
        std::string query = "avg:" + testMeasurement + ".moisture(value1,value2,value3){paddock:back-paddock}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        // Should average only aaaaa and bbbbb (both in back-paddock)
        auto device1Value1 = createTestFieldData(100, 1.0);
        auto device2Value1 = createTestFieldData(100, 4.0);
        auto expectedValue1 = calculateAverage({device1Value1, device2Value1});

        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value1"].second), expectedValue1));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test query with group by
TEST_F(AsyncQueryTest, QueryWithGroupBy) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Group by deviceId
        std::string query = "avg:" + testMeasurement + ".moisture(value1){paddock:back-paddock} by {deviceId}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        // Should have 2 series (aaaaa and bbbbb, both in back-paddock)
        ASSERT_EQ(result.series.size(), 2);

        // Find the series for each device
        SeriesResult* deviceA = nullptr;
        SeriesResult* deviceB = nullptr;

        for (auto& series : result.series) {
            if (series.tags["deviceId"] == "aaaaa")
                deviceA = &series;
            if (series.tags["deviceId"] == "bbbbb")
                deviceB = &series;
        }

        ASSERT_NE(deviceA, nullptr);
        ASSERT_NE(deviceB, nullptr);

        // Check values
        auto expectedA = createTestFieldData(100, 1.0);
        auto expectedB = createTestFieldData(100, 4.0);

        EXPECT_TRUE(vectorsClose(getDoubleValues(deviceA->fields["value1"].second), expectedA));
        EXPECT_TRUE(vectorsClose(getDoubleValues(deviceB->fields["value1"].second), expectedB));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test boolean data type
TEST_F(AsyncQueryTest, BooleanDataQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertBooleanDataSync(eng.eng);

        std::string query = "latest:" + testMeasurement + ".boolean(){}";
        auto result = executeQuery(query, baseTime - 10000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        ASSERT_TRUE(series.fields.find("value") != series.fields.end());
        auto values = getDoubleValues(series.fields["value"].second);

        // Latest aggregation with boolean should preserve the values
        ASSERT_EQ(values.size(), 2);
        // Note: booleans are stored as 1.0 and 0.0 in double format
        EXPECT_EQ(values[0], 1.0);  // true
        EXPECT_EQ(values[1], 0.0);  // false

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test string/binary data (images)
TEST_F(AsyncQueryTest, ImageDataQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertImageDataSync(eng.eng);

        std::string query = "latest:" + testMeasurement + ".images(){}";
        auto result = executeQuery(query, baseTime - 10000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        ASSERT_TRUE(series.fields.find("image") != series.fields.end());

        // String fields are stored in the variant as std::vector<std::string>
        auto imageRefs = getStringValues(series.fields["image"].second);
        ASSERT_EQ(imageRefs.size(), 2);

        // Check the references match pattern
        EXPECT_TRUE(imageRefs[0].find("ref::") == 0);
        EXPECT_TRUE(imageRefs[1].find("ref::") == 0);
        EXPECT_TRUE(imageRefs[0].find("s3://") != std::string::npos);
        EXPECT_TRUE(imageRefs[1].find("s3://") != std::string::npos);

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test with time range filtering
TEST_F(AsyncQueryTest, TimeRangeFiltering) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query only last 50 seconds of data
        uint64_t midPoint = baseTime - 50000000000ULL;
        std::string query = "avg:" + testMeasurement + ".moisture(value1){}";
        auto result = executeQuery(query, midPoint, baseTime);

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

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test SUM aggregation
TEST_F(AsyncQueryTest, SumAggregationQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        std::string query = "sum:" + testMeasurement + ".moisture(value1){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

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

        EXPECT_TRUE(vectorsClose(getDoubleValues(series.fields["value1"].second), expectedSum));

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test LATEST aggregation
TEST_F(AsyncQueryTest, LatestAggregationQuery) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        std::string query = "latest:" + testMeasurement + ".moisture(value1){}";
        auto result = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        auto& timestamps = series.fields["value1"].first;
        auto values = getDoubleValues(series.fields["value1"].second);

        // Latest should return the most recent value from each series
        // In our case, it should aggregate the last points from all series
        EXPECT_GE(timestamps.size(), 1);

        // The last timestamp should be close to baseTime
        if (!timestamps.empty()) {
            EXPECT_LE(std::abs((long long)(timestamps.back() - baseTime)), 1000000000LL);
        }

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test query error cases
TEST_F(AsyncQueryTest, InvalidQueryErrors) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        // Missing aggregation method
        EXPECT_THROW(QueryParser::parseQueryString(testMeasurement + ".moisture(){}"), QueryParseException);

        // Invalid aggregation method
        EXPECT_THROW(QueryParser::parseQueryString("invalid:" + testMeasurement + ".moisture(){}"),
                     QueryParseException);

        // Missing measurement
        EXPECT_THROW(QueryParser::parseQueryString("avg:(value1){}"), QueryParseException);

        // Missing fields parentheses
        EXPECT_THROW(QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture{}"), QueryParseException);

        // Unclosed scope brace
        EXPECT_THROW(QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture(){location:us-west"),
                     QueryParseException);

        // Malformed group by
        EXPECT_THROW(QueryParser::parseQueryString("avg:" + testMeasurement + ".moisture(){} by deviceId}"),
                     QueryParseException);

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test inserting new data and cache invalidation
TEST_F(AsyncQueryTest, CacheInvalidationAfterInsert) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // First query
        std::string query = "avg:" + testMeasurement + ".moisture(){} by {deviceId}";
        auto result1 = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result1.series.size(), 3);  // aaaaa, bbbbb, ccccc

        // Insert new device data using TimeStarInsert
        auto timestamps = createTestTimestamps(100);
        auto fieldData = createTestFieldData(100, 1.0);
        TimeStarInsert<double> newDeviceInsert(testMeasurement + ".moisture", "value1");
        newDeviceInsert.addTag("deviceId", "zzzzzz");
        newDeviceInsert.addTag("paddock", "back-paddock");
        for (int i = 0; i < 100; i++) {
            newDeviceInsert.addValue(timestamps[i], fieldData[i]);
        }
        shardedInsert(eng.eng, std::move(newDeviceInsert));

        // Query again
        auto result2 = executeQuery(query, baseTime - 100000000000ULL, baseTime);

        ASSERT_EQ(result2.series.size(), 4);  // aaaaa, bbbbb, ccccc, zzzzzz

        // Verify new device is in results
        bool foundNewDevice = false;
        for (const auto& series : result2.series) {
            if (series.tags.at("deviceId") == "zzzzzz") {
                foundNewDevice = true;
                break;
            }
        }
        EXPECT_TRUE(foundNewDevice);

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test fields with same prefix
TEST_F(AsyncQueryTest, FieldsWithSamePrefix) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        // Insert data with field names that share prefixes
        {
            TimeStarInsert<double> insert("lid_data", "pnf");
            insert.addTag("meter_id", "33616");
            for (uint64_t i = 1; i <= 6; i++) {
                insert.addValue(i, 0.0);
            }
            shardedInsert(eng.eng, std::move(insert));
        }
        {
            TimeStarInsert<double> insert("lid_data", "pnf_status");
            insert.addTag("meter_id", "33616");
            for (uint64_t i = 1; i <= 6; i++) {
                insert.addValue(i, 1.0);
            }
            shardedInsert(eng.eng, std::move(insert));
        }

        // Query only the pnf field
        std::string query = "avg:lid_data(pnf){meter_id:33616}";
        auto result = executeQuery(query, 0, 1000);

        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        // Should only have pnf field, not pnf_status
        EXPECT_EQ(series.fields.size(), 1);
        EXPECT_TRUE(series.fields.find("pnf") != series.fields.end());
        EXPECT_FALSE(series.fields.find("pnf_status") != series.fields.end());

        auto values = getDoubleValues(series.fields["pnf"].second);
        for (auto val : values) {
            EXPECT_EQ(val, 0.0);
        }

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}

// Test with aggregation intervals
TEST_F(AsyncQueryTest, AggregationWithTimeIntervals) {
    seastar::thread([this] {
        ScopedShardedEngine eng;
        eng.startWithBackground();
        queryHandler = std::make_unique<timestar::HttpQueryHandler>(&eng.eng, nullptr);
        engineSharded = &eng.eng;

        insertTestDataSync(eng.eng);

        // Query with 10-second interval
        std::string queryStr = "avg:" + testMeasurement + ".moisture(value1){}";
        auto request = QueryParser::parseQueryString(queryStr);
        request.startTime = baseTime - 100000000000ULL;
        request.endTime = baseTime;
        request.aggregationInterval = timestar::HttpQueryHandler::parseInterval("10s");

        auto result = queryHandler->executeQuery(request).get();

        ASSERT_TRUE(result.success);
        ASSERT_EQ(result.series.size(), 1);
        auto& series = result.series[0];

        ASSERT_TRUE(series.fields.find("value1") != series.fields.end());
        auto& timestamps = series.fields["value1"].first;

        // With 100 seconds of data and 10-second intervals, expect ~10 buckets
        EXPECT_GE(timestamps.size(), 9);
        EXPECT_LE(timestamps.size(), 11);

        queryHandler.reset();
        engineSharded = nullptr;
    })
        .join()
        .get();
}
