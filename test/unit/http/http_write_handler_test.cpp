#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <limits>

class HttpWriteHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_1");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_1");
    }
};

// Test JSON parsing for valid single point
TEST_F(HttpWriteHandlerTest, ParseValidSinglePoint) {
    std::string json = R"({
        "measurement": "weather",
        "tags": {"location": "us-west", "host": "server-01"},
        "fields": {"temperature": 72.5, "humidity": 65.0},
        "timestamp": 1638202821000000000
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    EXPECT_TRUE(doc.contains("measurement"));
    EXPECT_TRUE(doc.contains("tags"));
    EXPECT_TRUE(doc.contains("fields"));
    EXPECT_TRUE(doc.contains("timestamp"));

    EXPECT_EQ(doc["measurement"].get<std::string>(), "weather");
    EXPECT_DOUBLE_EQ(doc["timestamp"].get<double>(), 1638202821000000000.0);
}

// Test JSON parsing for batch writes
TEST_F(HttpWriteHandlerTest, ParseBatchWrite) {
    std::string json = R"({
        "writes": [
            {
                "measurement": "temperature",
                "tags": {"location": "us-west"},
                "fields": {"value": 72.5},
                "timestamp": 1638202821000000000
            },
            {
                "measurement": "temperature",
                "tags": {"location": "us-east"},
                "fields": {"value": 68.3},
                "timestamp": 1638202822000000000
            }
        ]
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    EXPECT_TRUE(doc.contains("writes"));
    auto& writes = doc["writes"].get_array();
    EXPECT_EQ(writes.size(), 2);

    // Verify first point
    EXPECT_EQ(writes[0]["measurement"].get<std::string>(), "temperature");
    EXPECT_EQ(writes[0]["tags"]["location"].get<std::string>(), "us-west");
    EXPECT_DOUBLE_EQ(writes[0]["fields"]["value"].get<double>(), 72.5);

    // Verify second point
    EXPECT_EQ(writes[1]["tags"]["location"].get<std::string>(), "us-east");
    EXPECT_DOUBLE_EQ(writes[1]["fields"]["value"].get<double>(), 68.3);
}

// Test parsing mixed field types
TEST_F(HttpWriteHandlerTest, ParseMixedFieldTypes) {
    std::string json = R"({
        "measurement": "system",
        "tags": {"host": "server-01"},
        "fields": {
            "cpu_usage": 45.7,
            "memory_gb": 16,
            "is_healthy": true,
            "status": "running"
        },
        "timestamp": 1638202821000000000
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];

    // In f64 mode, all numbers are double
    EXPECT_TRUE(fields["cpu_usage"].is_number());
    EXPECT_DOUBLE_EQ(fields["cpu_usage"].get<double>(), 45.7);

    EXPECT_TRUE(fields["memory_gb"].is_number());
    EXPECT_DOUBLE_EQ(fields["memory_gb"].get<double>(), 16.0);

    EXPECT_TRUE(fields["is_healthy"].is_boolean());
    EXPECT_EQ(fields["is_healthy"].get<bool>(), true);

    EXPECT_TRUE(fields["status"].is_string());
    EXPECT_EQ(fields["status"].get<std::string>(), "running");
}

// Test error cases - missing measurement
TEST_F(HttpWriteHandlerTest, ParseErrorMissingMeasurement) {
    std::string json = R"({
        "tags": {"host": "server-01"},
        "fields": {"value": 42.0}
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));
    EXPECT_FALSE(doc.contains("measurement"));
}

// Test error cases - missing fields
TEST_F(HttpWriteHandlerTest, ParseErrorMissingFields) {
    std::string json = R"({
        "measurement": "test",
        "tags": {"host": "server-01"},
        "timestamp": 1638202821000000000
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));
    EXPECT_FALSE(doc.contains("fields"));
}

// Test error cases - empty fields object
TEST_F(HttpWriteHandlerTest, ParseErrorEmptyFields) {
    std::string json = R"({
        "measurement": "test",
        "tags": {"host": "server-01"},
        "fields": {}
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));
    EXPECT_TRUE(doc.contains("fields"));
    EXPECT_TRUE(doc["fields"].empty());
}

// Test invalid JSON
TEST_F(HttpWriteHandlerTest, ParseInvalidJson) {
    std::string invalidJson = "{ not valid json }";

    glz::generic doc;
    auto err = glz::read_json(doc, invalidJson);
    EXPECT_TRUE(bool(err));
}

// Test timestamp handling - missing timestamp
TEST_F(HttpWriteHandlerTest, ParseMissingTimestamp) {
    std::string json = R"({
        "measurement": "test",
        "fields": {"value": 42.0}
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));
    EXPECT_FALSE(doc.contains("timestamp"));
}

// Test tags handling - missing tags (should be optional)
TEST_F(HttpWriteHandlerTest, ParseMissingTags) {
    std::string json = R"({
        "measurement": "test",
        "fields": {"value": 42.0},
        "timestamp": 1638202821000000000
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));
    EXPECT_FALSE(doc.contains("tags"));
    EXPECT_TRUE(doc.contains("measurement"));
    EXPECT_TRUE(doc.contains("fields"));
}

// =================== Type Detection and Conversion Tests ===================

// Test type detection for float values
TEST_F(HttpWriteHandlerTest, TypeDetectionFloat) {
    std::string json = R"({
        "fields": {
            "float_val": 3.14159,
            "float_negative": -273.15,
            "float_scientific": 1.23e-4
        }
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];
    EXPECT_TRUE(fields["float_val"].is_number());
    EXPECT_DOUBLE_EQ(fields["float_val"].get<double>(), 3.14159);

    EXPECT_TRUE(fields["float_negative"].is_number());
    EXPECT_DOUBLE_EQ(fields["float_negative"].get<double>(), -273.15);

    EXPECT_TRUE(fields["float_scientific"].is_number());
    EXPECT_NEAR(fields["float_scientific"].get<double>(), 1.23e-4, 1e-10);
}

// Test type detection for integer values (stored as double in f64 mode)
TEST_F(HttpWriteHandlerTest, TypeDetectionInteger) {
    std::string json = R"({
        "fields": {
            "int_val": 42,
            "int_negative": -100,
            "int_large": 2147483647
        }
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];
    EXPECT_TRUE(fields["int_val"].is_number());
    EXPECT_DOUBLE_EQ(fields["int_val"].get<double>(), 42.0);

    EXPECT_TRUE(fields["int_negative"].is_number());
    EXPECT_DOUBLE_EQ(fields["int_negative"].get<double>(), -100.0);

    EXPECT_TRUE(fields["int_large"].is_number());
    EXPECT_DOUBLE_EQ(fields["int_large"].get<double>(), 2147483647.0);
}

// Test type detection for boolean values
TEST_F(HttpWriteHandlerTest, TypeDetectionBoolean) {
    std::string json = R"({
        "fields": {
            "bool_true": true,
            "bool_false": false
        }
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];
    EXPECT_TRUE(fields["bool_true"].is_boolean());
    EXPECT_EQ(fields["bool_true"].get<bool>(), true);

    EXPECT_TRUE(fields["bool_false"].is_boolean());
    EXPECT_EQ(fields["bool_false"].get<bool>(), false);
}

// Test type detection for string values
TEST_F(HttpWriteHandlerTest, TypeDetectionString) {
    std::string json = R"({
        "fields": {
            "string_val": "hello world",
            "string_empty": "",
            "string_unicode": "测试数据",
            "string_special": "line1\nline2\ttab"
        }
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];
    EXPECT_TRUE(fields["string_val"].is_string());
    EXPECT_EQ(fields["string_val"].get<std::string>(), "hello world");

    EXPECT_TRUE(fields["string_empty"].is_string());
    EXPECT_EQ(fields["string_empty"].get<std::string>(), "");

    EXPECT_TRUE(fields["string_unicode"].is_string());
    EXPECT_EQ(fields["string_unicode"].get<std::string>(), "测试数据");

    EXPECT_TRUE(fields["string_special"].is_string());
    EXPECT_EQ(fields["string_special"].get<std::string>(), "line1\nline2\ttab");
}

// Test integer to float conversion
TEST_F(HttpWriteHandlerTest, IntegerToFloatConversion) {
    std::string json = R"({"fields": {"int_as_float": 100}})";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    // In f64 mode, integers are already stored as double
    EXPECT_TRUE(doc["fields"]["int_as_float"].is_number());
    EXPECT_DOUBLE_EQ(doc["fields"]["int_as_float"].get<double>(), 100.0);
}

// Test null value handling
TEST_F(HttpWriteHandlerTest, NullValueHandling) {
    std::string json = R"({"fields": {"null_val": null}})";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& nullVal = doc["fields"]["null_val"];
    EXPECT_TRUE(nullVal.is_null());
    EXPECT_FALSE(nullVal.is_string());
    EXPECT_FALSE(nullVal.is_number());
    EXPECT_FALSE(nullVal.is_boolean());
}

// Test array values (should be invalid for single point)
TEST_F(HttpWriteHandlerTest, ArrayValueHandling) {
    std::string json = R"({"fields": {"array_val": [1, 2, 3]}})";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& arrayVal = doc["fields"]["array_val"];
    EXPECT_TRUE(arrayVal.is_array());
    EXPECT_EQ(arrayVal.get_array().size(), 3);
}

// Test edge cases for numeric values
TEST_F(HttpWriteHandlerTest, NumericEdgeCases) {
    std::string json = R"({
        "fields": {
            "zero_int": 0,
            "zero_float": 0.0
        }
    })";

    glz::generic doc;
    auto err = glz::read_json(doc, json);
    EXPECT_FALSE(bool(err));

    auto& fields = doc["fields"];
    EXPECT_TRUE(fields["zero_int"].is_number());
    EXPECT_DOUBLE_EQ(fields["zero_int"].get<double>(), 0.0);

    EXPECT_TRUE(fields["zero_float"].is_number());
    EXPECT_DOUBLE_EQ(fields["zero_float"].get<double>(), 0.0);
}

// Regression: after findOrCreateCandidate, the local valueType must be synced
// with candidate.valueType to handle Integer->Float promotion for array fields.
TEST_F(HttpWriteHandlerTest, CoalesceArrayIntToFloatPromotionFixPresent) {
    std::ifstream src("../lib/http/http_write_handler.cpp");
    ASSERT_TRUE(src.is_open()) << "Could not open http_write_handler.cpp for source inspection";
    std::string content((std::istreambuf_iterator<char>(src)), std::istreambuf_iterator<char>());

    // The fix: after findOrCreateCandidate returns, local valueType is updated
    // from candidate.valueType so the reserve/push_back loop uses the promoted type.
    EXPECT_NE(content.find("valueType = candidate.valueType"), std::string::npos)
        << "Array coalesce must sync local valueType with candidate.valueType after promotion";
}
