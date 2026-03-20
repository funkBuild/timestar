#include "../../lib/utils/line_parser.hpp"
#include <gtest/gtest.h>
#include <stdexcept>

TEST(LineParserValidationTest, EmptyInputThrows) {
    EXPECT_THROW(SeriesKeyParser(""), std::invalid_argument);
}

TEST(LineParserValidationTest, UnclosedQuoteThrows) {
    EXPECT_THROW(SeriesKeyParser("cpu,host=\"server01 value"), std::invalid_argument);
}

TEST(LineParserValidationTest, ValidInputNoTags) {
    SeriesKeyParser p("cpu value");
    EXPECT_EQ(p.measurement, "cpu");
    EXPECT_EQ(p.field, "value");
    EXPECT_TRUE(p.tags.empty());
}

TEST(LineParserValidationTest, ValidInputWithTags) {
    SeriesKeyParser p("cpu,host=server01,region=us-west value");
    EXPECT_EQ(p.measurement, "cpu");
    EXPECT_EQ(p.field, "value");
    EXPECT_EQ(p.tags.size(), 2u);
    EXPECT_EQ(p.tags["host"], "server01");
    EXPECT_EQ(p.tags["region"], "us-west");
}

TEST(LineParserValidationTest, QuotedValueWithComma) {
    SeriesKeyParser p("cpu,host=\"server,01\" value");
    EXPECT_EQ(p.measurement, "cpu");
    EXPECT_EQ(p.tags["host"], "\"server,01\"");
    EXPECT_EQ(p.field, "value");
}

TEST(LineParserValidationTest, MatchedQuotesAreAccepted) {
    // Quotes that open and close correctly should not throw
    EXPECT_NO_THROW(SeriesKeyParser("cpu,host=\"server01\" value"));
}
