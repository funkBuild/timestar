#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <filesystem>
#include <limits>
#include <cmath>

class HttpWriteHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test data
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_1");
    }
    
    void TearDown() override {
        // Clean up test data
        std::filesystem::remove_all("shard_0");
        std::filesystem::remove_all("shard_1");
    }
    
    // Helper to create JSON string
    std::string createJsonPoint(const std::string& measurement,
                                const std::map<std::string, std::string>& tags,
                                const std::map<std::string, rapidjson::Value>& fields,
                                uint64_t timestamp = 0) {
        rapidjson::Document doc;
        doc.SetObject();
        auto& allocator = doc.GetAllocator();
        
        doc.AddMember("measurement", rapidjson::Value(measurement.c_str(), allocator), allocator);
        
        // Add tags
        rapidjson::Value tagsObj(rapidjson::kObjectType);
        for (const auto& [key, value] : tags) {
            tagsObj.AddMember(
                rapidjson::Value(key.c_str(), allocator),
                rapidjson::Value(value.c_str(), allocator),
                allocator
            );
        }
        doc.AddMember("tags", tagsObj, allocator);
        
        // Add fields
        rapidjson::Value fieldsObj(rapidjson::kObjectType);
        for (const auto& [key, value] : fields) {
            fieldsObj.AddMember(
                rapidjson::Value(key.c_str(), allocator),
                rapidjson::Value(value, allocator),
                allocator
            );
        }
        doc.AddMember("fields", fieldsObj, allocator);
        
        // Add timestamp if provided
        if (timestamp > 0) {
            doc.AddMember("timestamp", timestamp, allocator);
        }
        
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);
        
        return buffer.GetString();
    }
};

// Test JSON parsing for valid single point
TEST_F(HttpWriteHandlerTest, ParseValidSinglePoint) {
    rapidjson::Document doc;
    rapidjson::Value::AllocatorType allocator;
    
    std::map<std::string, rapidjson::Value> fields;
    fields["temperature"] = rapidjson::Value(72.5);
    fields["humidity"] = rapidjson::Value(65.0);
    
    std::string json = createJsonPoint(
        "weather",
        {{"location", "us-west"}, {"host", "server-01"}},
        fields,
        1638202821000000000
    );
    
    doc.Parse(json.c_str());
    EXPECT_FALSE(doc.HasParseError());
    EXPECT_TRUE(doc.HasMember("measurement"));
    EXPECT_TRUE(doc.HasMember("tags"));
    EXPECT_TRUE(doc.HasMember("fields"));
    EXPECT_TRUE(doc.HasMember("timestamp"));
    
    EXPECT_STREQ(doc["measurement"].GetString(), "weather");
    EXPECT_EQ(doc["timestamp"].GetUint64(), 1638202821000000000);
}

// Test JSON parsing for batch writes
TEST_F(HttpWriteHandlerTest, ParseBatchWrite) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value writes(rapidjson::kArrayType);
    
    // Create first point
    rapidjson::Document point1;
    point1.SetObject();
    point1.AddMember("measurement", "temperature", allocator);
    
    rapidjson::Value tags1(rapidjson::kObjectType);
    tags1.AddMember("location", "us-west", allocator);
    point1.AddMember("tags", tags1, allocator);
    
    rapidjson::Value fields1(rapidjson::kObjectType);
    fields1.AddMember("value", 72.5, allocator);
    point1.AddMember("fields", fields1, allocator);
    point1.AddMember("timestamp", 1638202821000000000, allocator);
    
    writes.PushBack(point1, allocator);
    
    // Create second point
    rapidjson::Document point2;
    point2.SetObject();
    point2.AddMember("measurement", "temperature", allocator);
    
    rapidjson::Value tags2(rapidjson::kObjectType);
    tags2.AddMember("location", "us-east", allocator);
    point2.AddMember("tags", tags2, allocator);
    
    rapidjson::Value fields2(rapidjson::kObjectType);
    fields2.AddMember("value", 68.3, allocator);
    point2.AddMember("fields", fields2, allocator);
    point2.AddMember("timestamp", 1638202822000000000, allocator);
    
    writes.PushBack(point2, allocator);
    
    doc.AddMember("writes", writes, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    // Parse and verify
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_TRUE(parsedDoc.HasMember("writes"));
    EXPECT_TRUE(parsedDoc["writes"].IsArray());
    EXPECT_EQ(parsedDoc["writes"].Size(), 2);
    
    // Verify first point
    const auto& p1 = parsedDoc["writes"][0];
    EXPECT_STREQ(p1["measurement"].GetString(), "temperature");
    EXPECT_STREQ(p1["tags"]["location"].GetString(), "us-west");
    EXPECT_DOUBLE_EQ(p1["fields"]["value"].GetDouble(), 72.5);
    
    // Verify second point
    const auto& p2 = parsedDoc["writes"][1];
    EXPECT_STREQ(p2["measurement"].GetString(), "temperature");
    EXPECT_STREQ(p2["tags"]["location"].GetString(), "us-east");
    EXPECT_DOUBLE_EQ(p2["fields"]["value"].GetDouble(), 68.3);
}

// Test parsing mixed field types
TEST_F(HttpWriteHandlerTest, ParseMixedFieldTypes) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("measurement", "system", allocator);
    
    rapidjson::Value tags(rapidjson::kObjectType);
    tags.AddMember("host", "server-01", allocator);
    doc.AddMember("tags", tags, allocator);
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("cpu_usage", 45.7, allocator);        // float
    fields.AddMember("memory_gb", 16, allocator);          // integer
    fields.AddMember("is_healthy", true, allocator);       // boolean
    fields.AddMember("status", "running", allocator);      // string
    doc.AddMember("fields", fields, allocator);
    
    doc.AddMember("timestamp", 1638202821000000000, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    // Parse and verify types
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    
    const auto& fieldsObj = parsedDoc["fields"];
    EXPECT_TRUE(fieldsObj["cpu_usage"].IsDouble());
    EXPECT_TRUE(fieldsObj["memory_gb"].IsInt());
    EXPECT_TRUE(fieldsObj["is_healthy"].IsBool());
    EXPECT_TRUE(fieldsObj["status"].IsString());
    
    EXPECT_DOUBLE_EQ(fieldsObj["cpu_usage"].GetDouble(), 45.7);
    EXPECT_EQ(fieldsObj["memory_gb"].GetInt(), 16);
    EXPECT_EQ(fieldsObj["is_healthy"].GetBool(), true);
    EXPECT_STREQ(fieldsObj["status"].GetString(), "running");
}

// Test error cases - missing measurement
TEST_F(HttpWriteHandlerTest, ParseErrorMissingMeasurement) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    // No measurement field
    rapidjson::Value tags(rapidjson::kObjectType);
    tags.AddMember("host", "server-01", allocator);
    doc.AddMember("tags", tags, allocator);
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("value", 42.0, allocator);
    doc.AddMember("fields", fields, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_FALSE(parsedDoc.HasMember("measurement"));
}

// Test error cases - missing fields
TEST_F(HttpWriteHandlerTest, ParseErrorMissingFields) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("measurement", "test", allocator);
    
    rapidjson::Value tags(rapidjson::kObjectType);
    tags.AddMember("host", "server-01", allocator);
    doc.AddMember("tags", tags, allocator);
    
    // No fields
    doc.AddMember("timestamp", 1638202821000000000, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_FALSE(parsedDoc.HasMember("fields"));
}

// Test error cases - empty fields object
TEST_F(HttpWriteHandlerTest, ParseErrorEmptyFields) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("measurement", "test", allocator);
    
    rapidjson::Value tags(rapidjson::kObjectType);
    tags.AddMember("host", "server-01", allocator);
    doc.AddMember("tags", tags, allocator);
    
    // Empty fields object
    rapidjson::Value fields(rapidjson::kObjectType);
    doc.AddMember("fields", fields, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_TRUE(parsedDoc.HasMember("fields"));
    EXPECT_EQ(parsedDoc["fields"].MemberCount(), 0);
}

// Test invalid JSON
TEST_F(HttpWriteHandlerTest, ParseInvalidJson) {
    std::string invalidJson = "{ not valid json }";
    
    rapidjson::Document doc;
    doc.Parse(invalidJson.c_str());
    
    EXPECT_TRUE(doc.HasParseError());
}

// Test timestamp handling - missing timestamp
TEST_F(HttpWriteHandlerTest, ParseMissingTimestamp) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("measurement", "test", allocator);
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("value", 42.0, allocator);
    doc.AddMember("fields", fields, allocator);
    
    // No timestamp field
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_FALSE(parsedDoc.HasMember("timestamp"));
}

// Test tags handling - missing tags (should be optional)
TEST_F(HttpWriteHandlerTest, ParseMissingTags) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("measurement", "test", allocator);
    
    // No tags field
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("value", 42.0, allocator);
    doc.AddMember("fields", fields, allocator);
    
    doc.AddMember("timestamp", 1638202821000000000, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    std::string json = buffer.GetString();
    
    rapidjson::Document parsedDoc;
    parsedDoc.Parse(json.c_str());
    
    EXPECT_FALSE(parsedDoc.HasParseError());
    EXPECT_FALSE(parsedDoc.HasMember("tags"));
    EXPECT_TRUE(parsedDoc.HasMember("measurement"));
    EXPECT_TRUE(parsedDoc.HasMember("fields"));
}

// =================== Type Detection and Conversion Tests ===================

// Test type detection for float values
TEST_F(HttpWriteHandlerTest, TypeDetectionFloat) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("float_val", 3.14159, allocator);
    fields.AddMember("float_negative", -273.15, allocator);
    fields.AddMember("float_scientific", 1.23e-4, allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["float_val"].IsDouble() || fieldsObj["float_val"].IsFloat());
    EXPECT_TRUE(fieldsObj["float_negative"].IsDouble() || fieldsObj["float_negative"].IsFloat());
    EXPECT_TRUE(fieldsObj["float_scientific"].IsDouble() || fieldsObj["float_scientific"].IsFloat());
    
    EXPECT_DOUBLE_EQ(fieldsObj["float_val"].GetDouble(), 3.14159);
    EXPECT_DOUBLE_EQ(fieldsObj["float_negative"].GetDouble(), -273.15);
    EXPECT_NEAR(fieldsObj["float_scientific"].GetDouble(), 1.23e-4, 1e-10);
}

// Test type detection for integer values
TEST_F(HttpWriteHandlerTest, TypeDetectionInteger) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("int_val", 42, allocator);
    fields.AddMember("int_negative", -100, allocator);
    fields.AddMember("int_large", 2147483647, allocator);  // MAX_INT
    fields.AddMember("int64_val", static_cast<int64_t>(9223372036854775807LL), allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["int_val"].IsInt());
    EXPECT_TRUE(fieldsObj["int_negative"].IsInt());
    EXPECT_TRUE(fieldsObj["int_large"].IsInt());
    EXPECT_TRUE(fieldsObj["int64_val"].IsInt64());
    
    EXPECT_EQ(fieldsObj["int_val"].GetInt(), 42);
    EXPECT_EQ(fieldsObj["int_negative"].GetInt(), -100);
    EXPECT_EQ(fieldsObj["int_large"].GetInt(), 2147483647);
    EXPECT_EQ(fieldsObj["int64_val"].GetInt64(), 9223372036854775807LL);
}

// Test type detection for boolean values
TEST_F(HttpWriteHandlerTest, TypeDetectionBoolean) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("bool_true", true, allocator);
    fields.AddMember("bool_false", false, allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["bool_true"].IsBool());
    EXPECT_TRUE(fieldsObj["bool_false"].IsBool());
    
    EXPECT_EQ(fieldsObj["bool_true"].GetBool(), true);
    EXPECT_EQ(fieldsObj["bool_false"].GetBool(), false);
}

// Test type detection for string values
TEST_F(HttpWriteHandlerTest, TypeDetectionString) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("string_val", "hello world", allocator);
    fields.AddMember("string_empty", "", allocator);
    fields.AddMember("string_unicode", "测试数据", allocator);
    fields.AddMember("string_special", "line1\nline2\ttab", allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["string_val"].IsString());
    EXPECT_TRUE(fieldsObj["string_empty"].IsString());
    EXPECT_TRUE(fieldsObj["string_unicode"].IsString());
    EXPECT_TRUE(fieldsObj["string_special"].IsString());
    
    EXPECT_STREQ(fieldsObj["string_val"].GetString(), "hello world");
    EXPECT_STREQ(fieldsObj["string_empty"].GetString(), "");
    EXPECT_STREQ(fieldsObj["string_unicode"].GetString(), "测试数据");
    EXPECT_STREQ(fieldsObj["string_special"].GetString(), "line1\nline2\ttab");
}

// Test integer to float conversion
TEST_F(HttpWriteHandlerTest, IntegerToFloatConversion) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("int_as_float", 100, allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    // Integer can be read as double
    EXPECT_TRUE(fieldsObj["int_as_float"].IsInt());
    double floatVal = fieldsObj["int_as_float"].GetDouble();
    EXPECT_DOUBLE_EQ(floatVal, 100.0);
}

// Test null value handling
TEST_F(HttpWriteHandlerTest, NullValueHandling) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("null_val", rapidjson::Value(rapidjson::kNullType), allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["null_val"].IsNull());
    EXPECT_FALSE(fieldsObj["null_val"].IsString());
    EXPECT_FALSE(fieldsObj["null_val"].IsNumber());
    EXPECT_FALSE(fieldsObj["null_val"].IsBool());
}

// Test array values (should be invalid for single point)
TEST_F(HttpWriteHandlerTest, ArrayValueHandling) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    rapidjson::Value array(rapidjson::kArrayType);
    array.PushBack(1, allocator);
    array.PushBack(2, allocator);
    array.PushBack(3, allocator);
    fields.AddMember("array_val", array, allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["array_val"].IsArray());
    EXPECT_EQ(fieldsObj["array_val"].Size(), 3);
}

// Test edge cases for numeric values
TEST_F(HttpWriteHandlerTest, NumericEdgeCases) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    rapidjson::Value fields(rapidjson::kObjectType);
    fields.AddMember("zero_int", 0, allocator);
    fields.AddMember("zero_float", 0.0, allocator);
    fields.AddMember("inf_positive", std::numeric_limits<double>::infinity(), allocator);
    fields.AddMember("inf_negative", -std::numeric_limits<double>::infinity(), allocator);
    fields.AddMember("nan_val", std::numeric_limits<double>::quiet_NaN(), allocator);
    doc.AddMember("fields", fields, allocator);
    
    const auto& fieldsObj = doc["fields"];
    
    EXPECT_TRUE(fieldsObj["zero_int"].IsInt());
    EXPECT_EQ(fieldsObj["zero_int"].GetInt(), 0);
    
    EXPECT_TRUE(fieldsObj["zero_float"].IsDouble() || fieldsObj["zero_float"].IsFloat());
    EXPECT_DOUBLE_EQ(fieldsObj["zero_float"].GetDouble(), 0.0);
    
    EXPECT_TRUE(fieldsObj["inf_positive"].IsDouble());
    EXPECT_TRUE(std::isinf(fieldsObj["inf_positive"].GetDouble()));
    EXPECT_GT(fieldsObj["inf_positive"].GetDouble(), 0);
    
    EXPECT_TRUE(fieldsObj["inf_negative"].IsDouble());
    EXPECT_TRUE(std::isinf(fieldsObj["inf_negative"].GetDouble()));
    EXPECT_LT(fieldsObj["inf_negative"].GetDouble(), 0);
    
    EXPECT_TRUE(fieldsObj["nan_val"].IsDouble());
    EXPECT_TRUE(std::isnan(fieldsObj["nan_val"].GetDouble()));
}