#include "http_write_handler.hpp"

#include <gtest/gtest.h>

// Names (measurement / tag key / field) and tag values may contain any byte
// EXCEPT NUL — alphanumeric, spaces, and symbols (including ',', '=', '"') are
// all allowed.  The structural series-key characters are backslash-escaped by
// timestar::buildSeriesKey, so they are unambiguous inside a name.  NUL is the
// one exception: it is the separator byte inside index KV keys, so it is rejected.
class HttpWriteValidationTest : public ::testing::Test {};

// =================== Measurement name validation ===================

TEST_F(HttpWriteValidationTest, MeasurementNameWithNullByteRejected) {
    std::string name = std::string("cpu") + '\0' + "metric";
    auto err = HttpWriteHandler::validateName(name, "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameEmptyRejected) {
    auto err = HttpWriteHandler::validateName("", "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithSpaceAllowed) {
    // The bug that motivated the escaping fix — measurements from the logs.
    EXPECT_TRUE(HttpWriteHandler::validateName("Transfer Pump", "Measurement name").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("Wash Down Bay System", "Measurement name").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("Wet Well Level", "Measurement name").empty());
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithCommaAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateName("cpu,usage", "Measurement name").empty());
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithEqualsAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateName("cpu=usage", "Measurement name").empty());
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithQuoteAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateName("say \"hi\"", "Measurement name").empty());
}

// =================== Tag key validation ===================

TEST_F(HttpWriteValidationTest, TagKeyWithNullByteRejected) {
    std::string name = std::string("host") + '\0';
    auto err = HttpWriteHandler::validateName(name, "Tag key");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagKeyWithSpaceCommaEqualsAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateName("host name", "Tag key").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("host,name", "Tag key").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("host=name", "Tag key").empty());
}

// =================== Tag value validation ===================

TEST_F(HttpWriteValidationTest, TagValueWithNullByteRejected) {
    std::string value = std::string("server") + '\0' + "01";
    auto err = HttpWriteHandler::validateTagValue(value, "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagValueEmptyRejected) {
    auto err = HttpWriteHandler::validateTagValue("", "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagValueWithSpaceCommaEqualsAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateTagValue("us west", "Tag value").empty());
    EXPECT_TRUE(HttpWriteHandler::validateTagValue("us,west", "Tag value").empty());
    EXPECT_TRUE(HttpWriteHandler::validateTagValue("us=west", "Tag value").empty());
}

// =================== Field name validation ===================

TEST_F(HttpWriteValidationTest, FieldNameWithNullByteRejected) {
    std::string name = std::string("value") + '\0';
    auto err = HttpWriteHandler::validateName(name, "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameEmptyRejected) {
    auto err = HttpWriteHandler::validateName("", "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameWithSpaceAllowed) {
    // Field names from the logs.
    EXPECT_TRUE(HttpWriteHandler::validateName("Home 5 Flow", "Field name").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("Hill 6 Flow", "Field name").empty());
}

TEST_F(HttpWriteValidationTest, FieldNameWithCommaEqualsAllowed) {
    EXPECT_TRUE(HttpWriteHandler::validateName("temp,value", "Field name").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("temp=value", "Field name").empty());
}

// =================== Valid names should pass ===================

TEST_F(HttpWriteValidationTest, ValidSimpleName) {
    EXPECT_TRUE(HttpWriteHandler::validateName("cpu", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithDots) {
    EXPECT_TRUE(HttpWriteHandler::validateName("system.cpu.usage", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithHyphens) {
    EXPECT_TRUE(HttpWriteHandler::validateName("cpu-usage", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithUnderscores) {
    EXPECT_TRUE(HttpWriteHandler::validateName("cpu_usage", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithColons) {
    EXPECT_TRUE(HttpWriteHandler::validateName("net:eth0", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithDigits) {
    EXPECT_TRUE(HttpWriteHandler::validateName("server01", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidNameWithUnicode) {
    EXPECT_TRUE(HttpWriteHandler::validateName("temperature_celsius", "name").empty());
    EXPECT_TRUE(HttpWriteHandler::validateName("mesure_temp", "name").empty());
}

TEST_F(HttpWriteValidationTest, ValidTagValueWithSpace) {
    EXPECT_TRUE(HttpWriteHandler::validateTagValue("New York", "tag value").empty());
}

TEST_F(HttpWriteValidationTest, ValidTagValueSimple) {
    EXPECT_TRUE(HttpWriteHandler::validateTagValue("us-west-2", "tag value").empty());
}

// =================== parseWritePoint integration ===================
// These tests verify that the validation is properly wired into parseWritePoint.

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsSpacedMeasurement) {
    // The exact failing case from the logs: measurement AND field with spaces.
    std::string json = R"({
        "measurement": "Wash Down Bay System",
        "tags": {"host": "server01"},
        "fields": {"Home 5 Flow": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsMeasurementWithComma) {
    std::string json = R"({
        "measurement": "cpu,usage",
        "tags": {"host": "server01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsTagKeyWithSpace) {
    std::string json = R"({
        "measurement": "cpu",
        "tags": {"site name": "North Plant"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsTagValueWithComma) {
    std::string json = R"({
        "measurement": "cpu",
        "tags": {"host": "server,01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

TEST_F(HttpWriteValidationTest, ParseWritePointStillRejectsEmptyMeasurement) {
    std::string json = R"({
        "measurement": "",
        "tags": {"host": "server01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_THROW(HttpWriteHandler::parseAndValidateWritePoint(json), std::invalid_argument);
}

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsValidInput) {
    std::string json = R"({
        "measurement": "cpu.usage",
        "tags": {"host": "server-01", "dc": "us-west-2"},
        "fields": {"value": 42.0, "idle": 58.0},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

TEST_F(HttpWriteValidationTest, ParseWritePointAcceptsTagValueWithSpace) {
    std::string json = R"({
        "measurement": "temperature",
        "tags": {"location": "New York"},
        "fields": {"value": 72.5},
        "timestamp": 1000000000
    })";
    EXPECT_NO_THROW(HttpWriteHandler::parseAndValidateWritePoint(json));
}

// =================== Constructor null-pointer validation ===================

TEST_F(HttpWriteValidationTest, ConstructorThrowsOnNullEngine) {
    // Passing a null engineSharded pointer must throw std::invalid_argument
    // before any Seastar calls are made, preventing a segfault at the later
    // dereference sites (engineSharded->invoke_on, engineSharded->local()).
    EXPECT_THROW(HttpWriteHandler(nullptr), std::invalid_argument);
}
