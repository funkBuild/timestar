#include "../../../lib/http/floating_json_array.hpp"

#include <gtest/gtest.h>

using timestar::http::FloatingJsonArray;

TEST(FloatingJsonArrayTest, ExplicitFloatingTokensKeepTheFastPath) {
    for (const std::string input :
         {"[0.0,1.5]", "[100.0,1.5]", "[-0.0,1.5]", "[1e2,1.5]", "[1E+2,1.5]", " \n[ \t0.0,1.5]"}) {
        FloatingJsonArray parsed;
        auto error = glz::read_json(parsed, input);
        ASSERT_FALSE(error) << input;
        ASSERT_EQ(parsed.values.size(), 2u);
        EXPECT_DOUBLE_EQ(parsed.values.back(), 1.5);
    }
}

TEST(FloatingJsonArrayTest, IntegersAndMixedTypesDeclineWithoutRounding) {
    for (const std::string input : {"[0,1.5]", "[-10,1.5]", "[9007199254740993,1.5]", "[18446744073709551615,1.5]",
                                    "[true,1.5]", "[null,1.5]", "[]"}) {
        FloatingJsonArray parsed;
        EXPECT_TRUE(glz::read_json(parsed, input)) << input;
        EXPECT_TRUE(parsed.values.empty()) << input;
    }
}

TEST(FloatingJsonArrayTest, LookaheadDoesNotAcceptMalformedArrays) {
    for (const std::string input : {"[", "[ ", "[1e,2.0]", "[1.0,", "[0.0,true]", "[0.0,\"text\"]"}) {
        FloatingJsonArray parsed;
        EXPECT_TRUE(glz::read_json(parsed, input)) << input;
    }
}
