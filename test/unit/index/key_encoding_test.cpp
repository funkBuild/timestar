#include "../../../lib/index/key_encoding.hpp"

#include <gtest/gtest.h>

#include <string>

using namespace timestar::index::keys;

TEST(KeyEncodingTest, EncodeMeasurementFieldsKey) {
    auto key = encodeMeasurementFieldsKey("cpu");
    EXPECT_EQ(key[0], static_cast<char>(MEASUREMENT_FIELDS));
    EXPECT_EQ(key.substr(1), "cpu");
}

TEST(KeyEncodingTest, EncodeSeriesMetadataKey) {
    auto id = SeriesId128::fromHex("0102030405060708090a0b0c0d0e0f10");
    auto key = encodeSeriesMetadataKey(id);
    EXPECT_EQ(key[0], static_cast<char>(SERIES_METADATA));
    EXPECT_EQ(key.size(), 17u);  // 1 prefix + 16 bytes
}

TEST(KeyEncodingTest, EncodeFieldTypeKey) {
    auto key = encodeFieldTypeKey("weather", "temperature");
    EXPECT_EQ(key[0], static_cast<char>(FIELD_TYPE));
    // Should contain measurement + \0 + field
    EXPECT_NE(key.find('\0'), std::string::npos);
}

TEST(KeyEncodingTest, SeriesMetadataRoundtrip) {
    SeriesMetadata meta;
    meta.measurement = "weather";
    meta.field = "temperature";
    meta.tags = {{"location", "us-west"}, {"host", "server01"}};

    auto encoded = encodeSeriesMetadata(meta);
    auto decoded = decodeSeriesMetadata(encoded);

    EXPECT_EQ(decoded.measurement, "weather");
    EXPECT_EQ(decoded.field, "temperature");
    EXPECT_EQ(decoded.tags.size(), 2u);
    EXPECT_EQ(decoded.tags["location"], "us-west");
    EXPECT_EQ(decoded.tags["host"], "server01");
}

TEST(KeyEncodingTest, StringSetRoundtrip) {
    std::set<std::string> original = {"temperature", "humidity", "pressure"};
    auto encoded = encodeStringSet(original);
    auto decoded = decodeStringSet(encoded);
    EXPECT_EQ(decoded, original);
}

TEST(KeyEncodingTest, EmptyStringSet) {
    std::set<std::string> original;
    auto encoded = encodeStringSet(original);
    auto decoded = decodeStringSet(encoded);
    EXPECT_TRUE(decoded.empty());
}

TEST(KeyEncodingTest, SeriesIdRoundtrip) {
    auto id = SeriesId128::fromHex("aabbccdd11223344aabbccdd11223344");
    auto encoded = id.toBytes();
    auto decoded = SeriesId128::fromBytes(encoded);
    EXPECT_EQ(decoded, id);
}

TEST(KeyEncodingTest, EncodeRetentionPolicyKey) {
    auto key = encodeRetentionPolicyKey("metrics");
    EXPECT_EQ(key[0], static_cast<char>(RETENTION_POLICY));
    EXPECT_EQ(key.substr(1), "metrics");
}

TEST(KeyEncodingTest, EncodeMeasurementSeriesKey) {
    auto id = SeriesId128::fromHex("0102030405060708090a0b0c0d0e0f10");
    auto key = encodeMeasurementSeriesKey("weather", id);
    EXPECT_EQ(key[0], static_cast<char>(MEASUREMENT_SERIES));
    EXPECT_EQ(key.size(), 1u + 7u + 1u + 16u);  // prefix + "weather" + \0 + 16 bytes

    auto prefix = encodeMeasurementSeriesPrefix("weather");
    EXPECT_TRUE(key.starts_with(prefix));
}
