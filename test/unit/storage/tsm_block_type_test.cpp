#include "../../../lib/storage/tsm.hpp"

#include <gtest/gtest.h>

#include <string>

// Test suite for TSM block type validation logic
class TSMBlockTypeTest : public ::testing::Test {};

// Verify getValueType returns correct enum values for each supported type
TEST_F(TSMBlockTypeTest, GetValueTypeDouble) {
    EXPECT_EQ(TSM::getValueType<double>(), TSMValueType::Float);
}

TEST_F(TSMBlockTypeTest, GetValueTypeBool) {
    EXPECT_EQ(TSM::getValueType<bool>(), TSMValueType::Boolean);
}

TEST_F(TSMBlockTypeTest, GetValueTypeString) {
    EXPECT_EQ(TSM::getValueType<std::string>(), TSMValueType::String);
}

// Verify the underlying enum values match what is stored in TSM block headers
TEST_F(TSMBlockTypeTest, EnumValueFloat) {
    EXPECT_EQ(static_cast<uint8_t>(TSMValueType::Float), 0);
}

TEST_F(TSMBlockTypeTest, EnumValueBoolean) {
    EXPECT_EQ(static_cast<uint8_t>(TSMValueType::Boolean), 1);
}

TEST_F(TSMBlockTypeTest, EnumValueString) {
    EXPECT_EQ(static_cast<uint8_t>(TSMValueType::String), 2);
}

// Verify round-trip: uint8_t -> TSMValueType -> uint8_t
TEST_F(TSMBlockTypeTest, RoundTripCastFloat) {
    uint8_t stored = 0;
    TSMValueType reconstructed = static_cast<TSMValueType>(stored);
    EXPECT_EQ(reconstructed, TSMValueType::Float);
    EXPECT_EQ(static_cast<uint8_t>(reconstructed), stored);
}

TEST_F(TSMBlockTypeTest, RoundTripCastBoolean) {
    uint8_t stored = 1;
    TSMValueType reconstructed = static_cast<TSMValueType>(stored);
    EXPECT_EQ(reconstructed, TSMValueType::Boolean);
    EXPECT_EQ(static_cast<uint8_t>(reconstructed), stored);
}

TEST_F(TSMBlockTypeTest, RoundTripCastString) {
    uint8_t stored = 2;
    TSMValueType reconstructed = static_cast<TSMValueType>(stored);
    EXPECT_EQ(reconstructed, TSMValueType::String);
    EXPECT_EQ(static_cast<uint8_t>(reconstructed), stored);
}

// Verify that each type is distinct from the others via getValueType
TEST_F(TSMBlockTypeTest, TypesAreDistinct) {
    EXPECT_NE(TSM::getValueType<double>(), TSM::getValueType<bool>());
    EXPECT_NE(TSM::getValueType<double>(), TSM::getValueType<std::string>());
    EXPECT_NE(TSM::getValueType<bool>(), TSM::getValueType<std::string>());
}

// Verify mismatch detection logic (same logic used in readBlock/readSingleBlock/decodeBlock)
TEST_F(TSMBlockTypeTest, MismatchDetectionFloatVsBool) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::Float);
    TSMValueType expectedType = TSM::getValueType<bool>();
    EXPECT_NE(static_cast<TSMValueType>(blockType), expectedType);
}

TEST_F(TSMBlockTypeTest, MismatchDetectionFloatVsString) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::Float);
    TSMValueType expectedType = TSM::getValueType<std::string>();
    EXPECT_NE(static_cast<TSMValueType>(blockType), expectedType);
}

TEST_F(TSMBlockTypeTest, MismatchDetectionBoolVsFloat) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::Boolean);
    TSMValueType expectedType = TSM::getValueType<double>();
    EXPECT_NE(static_cast<TSMValueType>(blockType), expectedType);
}

TEST_F(TSMBlockTypeTest, MatchDetectionFloat) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::Float);
    TSMValueType expectedType = TSM::getValueType<double>();
    EXPECT_EQ(static_cast<TSMValueType>(blockType), expectedType);
}

TEST_F(TSMBlockTypeTest, MatchDetectionBool) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::Boolean);
    TSMValueType expectedType = TSM::getValueType<bool>();
    EXPECT_EQ(static_cast<TSMValueType>(blockType), expectedType);
}

TEST_F(TSMBlockTypeTest, MatchDetectionString) {
    uint8_t blockType = static_cast<uint8_t>(TSMValueType::String);
    TSMValueType expectedType = TSM::getValueType<std::string>();
    EXPECT_EQ(static_cast<TSMValueType>(blockType), expectedType);
}
