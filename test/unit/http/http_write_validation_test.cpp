#include "http_write_handler.hpp"

#include <gtest/gtest.h>

class HttpWriteValidationTest : public ::testing::Test {};

// =================== Measurement name validation ===================

TEST_F(HttpWriteValidationTest, MeasurementNameWithNullByte) {
    std::string name = std::string("cpu") + '\0' + "metric";
    auto err = HttpWriteHandler::validateName(name, "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithComma) {
    auto err = HttpWriteHandler::validateName("cpu,usage", "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("comma"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithEquals) {
    auto err = HttpWriteHandler::validateName("cpu=usage", "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("equals"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameWithSpace) {
    auto err = HttpWriteHandler::validateName("cpu usage", "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("space"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, MeasurementNameEmpty) {
    auto err = HttpWriteHandler::validateName("", "Measurement name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
}

// =================== Tag key validation ===================

TEST_F(HttpWriteValidationTest, TagKeyWithNullByte) {
    std::string name = std::string("host") + '\0';
    auto err = HttpWriteHandler::validateName(name, "Tag key");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagKeyWithComma) {
    auto err = HttpWriteHandler::validateName("host,name", "Tag key");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("comma"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagKeyWithEquals) {
    auto err = HttpWriteHandler::validateName("host=name", "Tag key");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("equals"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagKeyWithSpace) {
    auto err = HttpWriteHandler::validateName("host name", "Tag key");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("space"), std::string::npos);
}

// =================== Tag value validation ===================

TEST_F(HttpWriteValidationTest, TagValueWithNullByte) {
    std::string value = std::string("server") + '\0' + "01";
    auto err = HttpWriteHandler::validateTagValue(value, "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagValueWithComma) {
    auto err = HttpWriteHandler::validateTagValue("us,west", "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("comma"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagValueWithEquals) {
    auto err = HttpWriteHandler::validateTagValue("us=west", "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("equals"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, TagValueWithSpaceAllowed) {
    auto err = HttpWriteHandler::validateTagValue("us west", "Tag value");
    EXPECT_TRUE(err.empty()) << "Spaces should be allowed in tag values, got: " << err;
}

TEST_F(HttpWriteValidationTest, TagValueEmpty) {
    auto err = HttpWriteHandler::validateTagValue("", "Tag value");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
}

// =================== Field name validation ===================

TEST_F(HttpWriteValidationTest, FieldNameWithNullByte) {
    std::string name = std::string("value") + '\0';
    auto err = HttpWriteHandler::validateName(name, "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("null"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameWithComma) {
    auto err = HttpWriteHandler::validateName("temp,value", "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("comma"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameWithEquals) {
    auto err = HttpWriteHandler::validateName("temp=value", "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("equals"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameWithSpace) {
    auto err = HttpWriteHandler::validateName("temp value", "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("space"), std::string::npos);
}

TEST_F(HttpWriteValidationTest, FieldNameEmpty) {
    auto err = HttpWriteHandler::validateName("", "Field name");
    EXPECT_FALSE(err.empty());
    EXPECT_NE(err.find("empty"), std::string::npos);
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
// These tests verify that the validation is properly wired into parseWritePoint

TEST_F(HttpWriteValidationTest, ParseWritePointRejectsMeasurementWithComma) {
    std::string json = R"({
        "measurement": "cpu,usage",
        "tags": {"host": "server01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_THROW(HttpWriteHandler::parseAndValidateWritePoint(json), std::invalid_argument);
}

TEST_F(HttpWriteValidationTest, ParseWritePointRejectsTagKeyWithEquals) {
    std::string json = R"({
        "measurement": "cpu",
        "tags": {"host=name": "server01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_THROW(HttpWriteHandler::parseAndValidateWritePoint(json), std::invalid_argument);
}

TEST_F(HttpWriteValidationTest, ParseWritePointRejectsTagValueWithComma) {
    std::string json = R"({
        "measurement": "cpu",
        "tags": {"host": "server,01"},
        "fields": {"value": 42.0},
        "timestamp": 1000000000
    })";
    EXPECT_THROW(HttpWriteHandler::parseAndValidateWritePoint(json), std::invalid_argument);
}

TEST_F(HttpWriteValidationTest, ParseWritePointRejectsFieldNameWithSpace) {
    std::string json = R"({
        "measurement": "cpu",
        "tags": {"host": "server01"},
        "fields": {"bad field": 42.0},
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
