// Unit tests for the lossless-coercion rules used when a write's type differs
// from the type its series is bound to (lib/core/value_coercion.hpp).
//
// The governing rule: convert only when the conversion cannot lose information
// and has exactly one sensible reading. std::nullopt means "reject the write",
// and is never itself a stored value -- "abcd" must not become NaN or 0.

#include "../../../lib/core/value_coercion.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>

using timestar::coerceValue;

// ---------------------------------------------------------------------------
// Into a float-bound series
// ---------------------------------------------------------------------------
TEST(ValueCoercionTest, IntoFloat) {
    EXPECT_DOUBLE_EQ(coerceValue<double>(true).value(), 1.0);
    EXPECT_DOUBLE_EQ(coerceValue<double>(false).value(), 0.0);
    EXPECT_DOUBLE_EQ(coerceValue<double>(int64_t{42}).value(), 42.0);
    EXPECT_DOUBLE_EQ(coerceValue<double>(std::string("12.5")).value(), 12.5);
    EXPECT_DOUBLE_EQ(coerceValue<double>(std::string("-3")).value(), -3.0);

    // Not numbers.
    EXPECT_FALSE(coerceValue<double>(std::string("abcd")).has_value());
    EXPECT_FALSE(coerceValue<double>(std::string("")).has_value());
    // Partial parses must fail, not silently truncate to 12.
    EXPECT_FALSE(coerceValue<double>(std::string("12abc")).has_value());
    EXPECT_FALSE(coerceValue<double>(std::string(" 12")).has_value());

    // Beyond 2^53 an int64 no longer round-trips through a double.
    EXPECT_FALSE(coerceValue<double>(int64_t{9007199254740993LL}).has_value());
}

// ---------------------------------------------------------------------------
// Into an integer-bound series
// ---------------------------------------------------------------------------
TEST(ValueCoercionTest, IntoInteger) {
    // The JSON-serialiser case: 10.0 is exactly 10.
    EXPECT_EQ(coerceValue<int64_t>(10.0).value(), 10);
    EXPECT_EQ(coerceValue<int64_t>(-7.0).value(), -7);
    EXPECT_EQ(coerceValue<int64_t>(true).value(), 1);
    EXPECT_EQ(coerceValue<int64_t>(false).value(), 0);
    EXPECT_EQ(coerceValue<int64_t>(std::string("123")).value(), 123);

    // Fractional values would be truncated -- reject instead.
    EXPECT_FALSE(coerceValue<int64_t>(10.5).has_value());
    EXPECT_FALSE(coerceValue<int64_t>(std::string("10.5")).has_value());
    EXPECT_FALSE(coerceValue<int64_t>(std::string("abcd")).has_value());

    // Non-finite has no integer reading.
    EXPECT_FALSE(coerceValue<int64_t>(std::numeric_limits<double>::quiet_NaN()).has_value());
    EXPECT_FALSE(coerceValue<int64_t>(std::numeric_limits<double>::infinity()).has_value());
    // Out of range.
    EXPECT_FALSE(coerceValue<int64_t>(1e300).has_value());
}

// ---------------------------------------------------------------------------
// Into a boolean-bound series: only exact 0/1 round-trip
// ---------------------------------------------------------------------------
TEST(ValueCoercionTest, IntoBoolean) {
    EXPECT_TRUE(coerceValue<bool>(1.0).value());
    EXPECT_FALSE(coerceValue<bool>(0.0).value());
    EXPECT_TRUE(coerceValue<bool>(int64_t{1}).value());
    EXPECT_FALSE(coerceValue<bool>(int64_t{0}).value());
    EXPECT_TRUE(coerceValue<bool>(std::string("true")).value());
    EXPECT_FALSE(coerceValue<bool>(std::string("false")).value());

    // 5 -> true would discard the 5.
    EXPECT_FALSE(coerceValue<bool>(5.0).has_value());
    EXPECT_FALSE(coerceValue<bool>(int64_t{5}).has_value());
    EXPECT_FALSE(coerceValue<bool>(-1.0).has_value());
    // Only the exact literals; "yes"/"1"/"TRUE" are guesses.
    EXPECT_FALSE(coerceValue<bool>(std::string("yes")).has_value());
    EXPECT_FALSE(coerceValue<bool>(std::string("TRUE")).has_value());
}

// ---------------------------------------------------------------------------
// Into a string-bound series: everything has a lossless rendering
// ---------------------------------------------------------------------------
TEST(ValueCoercionTest, IntoString) {
    EXPECT_EQ(coerceValue<std::string>(true).value(), "true");
    EXPECT_EQ(coerceValue<std::string>(false).value(), "false");
    EXPECT_EQ(coerceValue<std::string>(int64_t{-42}).value(), "-42");

    // Doubles use the shortest form that reads back identically.
    auto s = coerceValue<std::string>(12.5).value();
    EXPECT_EQ(s, "12.5");
    EXPECT_DOUBLE_EQ(coerceValue<double>(s).value(), 12.5);
}

// A double -> string -> double round trip must be exact for awkward values,
// otherwise coercing into a string series would quietly lose precision.
TEST(ValueCoercionTest, DoubleToStringRoundTripsExactly) {
    const double values[] = {0.1, 1.0 / 3.0, 1e-300, 1e300, -0.0, 123456789.123456789};
    for (double v : values) {
        auto s = coerceValue<std::string>(v).value();
        auto back = coerceValue<double>(s);
        ASSERT_TRUE(back.has_value()) << "failed to parse back: " << s;
        EXPECT_EQ(std::signbit(*back), std::signbit(v)) << "sign lost for " << s;
        EXPECT_DOUBLE_EQ(*back, v) << "round trip changed the value via " << s;
    }
}
