#include "line_parser.hpp"

#include <gtest/gtest.h>

#include <map>
#include <string>

TEST(SeriesKeyParserTest, SimpleMeasurementAndField) {
    std::string key = "cpu value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    EXPECT_TRUE(parser.tags.empty());
}

TEST(SeriesKeyParserTest, MeasurementWithSingleTag) {
    std::string key = "cpu,host=server01 value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 1u);
    EXPECT_EQ(parser.tags.at("host"), "server01");
}

TEST(SeriesKeyParserTest, MeasurementWithMultipleTags) {
    std::string key = "cpu,host=server01,region=us-west value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 2u);
    EXPECT_EQ(parser.tags.at("host"), "server01");
    EXPECT_EQ(parser.tags.at("region"), "us-west");
}

TEST(SeriesKeyParserTest, MeasurementNoTags) {
    std::string key = "temperature value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "temperature");
    EXPECT_EQ(parser.field, "value");
    EXPECT_TRUE(parser.tags.empty());
}

TEST(SeriesKeyParserTest, QuotedTagValue) {
    // Quoted value containing a comma - the quote prevents the comma from being
    // treated as a tag delimiter.
    std::string key = "cpu,host=\"server,01\" value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 1u);
    // The parser keeps the quotes in the value as part of the string_view.
    auto it = parser.tags.find("host");
    ASSERT_NE(it, parser.tags.end());
    // The value includes the quotes since the parser only uses them for delimiter control.
    EXPECT_NE(it->second.find("server,01"), std::string_view::npos);
}

TEST(SeriesKeyParserTest, QuotedTagValueWithSpace) {
    // Quoted value containing a space - the quote prevents the space from being
    // treated as the field separator.
    std::string key = "cpu,host=\"server 01\" value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 1u);
    auto it = parser.tags.find("host");
    ASSERT_NE(it, parser.tags.end());
    EXPECT_NE(it->second.find("server 01"), std::string_view::npos);
}

TEST(SeriesKeyParserTest, MultipleTagsSorted) {
    // std::map sorts keys alphabetically, verify ordering.
    std::string key = "cpu,zebra=z,alpha=a,middle=m value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 3u);

    // Verify map iteration order is alphabetical by key.
    auto it = parser.tags.begin();
    EXPECT_EQ(it->first, "alpha");
    ++it;
    EXPECT_EQ(it->first, "middle");
    ++it;
    EXPECT_EQ(it->first, "zebra");
}

TEST(SeriesKeyParserTest, LongMeasurementName) {
    std::string key = "system.cpu.usage,host=server01 idle";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "system.cpu.usage");
    EXPECT_EQ(parser.field, "idle");
    ASSERT_EQ(parser.tags.size(), 1u);
    EXPECT_EQ(parser.tags.at("host"), "server01");
}

TEST(SeriesKeyParserTest, SingleCharField) {
    std::string key = "m,t=v f";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "m");
    EXPECT_EQ(parser.field, "f");
    ASSERT_EQ(parser.tags.size(), 1u);
    EXPECT_EQ(parser.tags.at("t"), "v");
}

TEST(SeriesKeyParserTest, FieldWithDot) {
    std::string key = "measurement,tag=value field.name";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "measurement");
    EXPECT_EQ(parser.field, "field.name");
    ASSERT_EQ(parser.tags.size(), 1u);
    EXPECT_EQ(parser.tags.at("tag"), "value");
}

TEST(SeriesKeyParserTest, TagValueWithEquals) {
    // parseKeypair uses find('=') which finds the first '=', so only the first
    // '=' is treated as the key-value delimiter.
    std::string key = "cpu,path=/usr/bin=test value";
    SeriesKeyParser parser(key);

    EXPECT_EQ(parser.measurement, "cpu");
    EXPECT_EQ(parser.field, "value");
    ASSERT_EQ(parser.tags.size(), 1u);
    EXPECT_EQ(parser.tags.at("path"), "/usr/bin=test");
}
