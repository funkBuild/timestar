#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>

// Tests for integer-to-double precision loss detection.
// IEEE 754 double-precision can represent integers exactly up to 2^53.
// Beyond that, some integer values cannot be represented exactly,
// causing silent precision loss when cast to double.

class HttpWriteIntegerPrecisionTest : public ::testing::Test {
protected:
    // IEEE 754 double can represent integers exactly up to 2^53
    static constexpr int64_t MAX_EXACT_DOUBLE_INT = 1LL << 53;  // 9007199254740992

    static bool wouldLosePrecision(int64_t value) {
        return value > MAX_EXACT_DOUBLE_INT || value < -MAX_EXACT_DOUBLE_INT;
    }
};

// =================== Values within safe range convert exactly ===================

TEST_F(HttpWriteIntegerPrecisionTest, ZeroConvertsExactly) {
    int64_t value = 0;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, OneConvertsExactly) {
    int64_t value = 1;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, NegativeOneConvertsExactly) {
    int64_t value = -1;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, MaxExactDoubleIntConvertsExactly) {
    // 2^53 = 9007199254740992 is the largest integer that double represents exactly
    int64_t value = MAX_EXACT_DOUBLE_INT;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, NegativeMaxExactDoubleIntConvertsExactly) {
    int64_t value = -MAX_EXACT_DOUBLE_INT;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, CommonTimestampSecondsConvertsExactly) {
    // Unix timestamp in seconds (well within safe range)
    int64_t value = 1707998400;  // Feb 15, 2024
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

TEST_F(HttpWriteIntegerPrecisionTest, CommonTimestampMillisConvertsExactly) {
    // Unix timestamp in milliseconds
    int64_t value = 1707998400000LL;
    EXPECT_FALSE(wouldLosePrecision(value));
    EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value);
}

// =================== Values exceeding 2^53 lose precision ===================

TEST_F(HttpWriteIntegerPrecisionTest, TwoPow53PlusOneLosesPrecision) {
    // 2^53 + 1 = 9007199254740993 cannot be represented exactly as double
    int64_t value = (1LL << 53) + 1;
    EXPECT_TRUE(wouldLosePrecision(value));
    // Prove the bug: cast to double and back gives a different value
    double d = static_cast<double>(value);
    int64_t roundtrip = static_cast<int64_t>(d);
    EXPECT_NE(roundtrip, value) << "Expected precision loss for " << value
                                << " but round-trip gave back the same value";
}

TEST_F(HttpWriteIntegerPrecisionTest, NegativeTwoPow53MinusOneLosesPrecision) {
    int64_t value = -(1LL << 53) - 1;
    EXPECT_TRUE(wouldLosePrecision(value));
    double d = static_cast<double>(value);
    int64_t roundtrip = static_cast<int64_t>(d);
    EXPECT_NE(roundtrip, value) << "Expected precision loss for " << value
                                << " but round-trip gave back the same value";
}

TEST_F(HttpWriteIntegerPrecisionTest, TwoPow53PlusThreeLosesPrecision) {
    // 9007199254740995 -> becomes 9007199254740996.0 when cast to double
    int64_t value = (1LL << 53) + 3;
    EXPECT_TRUE(wouldLosePrecision(value));
    double d = static_cast<double>(value);
    int64_t roundtrip = static_cast<int64_t>(d);
    EXPECT_NE(roundtrip, value);
}

TEST_F(HttpWriteIntegerPrecisionTest, LargeNanosecondTimestampFlagged) {
    // Nanosecond timestamps are typically ~19 digits, well beyond 2^53.
    // wouldLosePrecision is conservative: it flags all values > 2^53,
    // even though some specific values (like round numbers) may happen
    // to be exactly representable as double.
    int64_t value = 1707998400000000000LL;
    EXPECT_TRUE(wouldLosePrecision(value)) << "Nanosecond timestamp should be flagged (exceeds 2^53)";
}

TEST_F(HttpWriteIntegerPrecisionTest, OddNanosecondTimestampActuallyLosesPrecision) {
    // A nanosecond timestamp with an odd last digit will actually lose precision
    int64_t value = 1707998400000000001LL;
    EXPECT_TRUE(wouldLosePrecision(value));
    double d = static_cast<double>(value);
    int64_t roundtrip = static_cast<int64_t>(d);
    EXPECT_NE(roundtrip, value) << "Odd nanosecond timestamp " << value
                                << " should actually lose precision when stored as double";
}

TEST_F(HttpWriteIntegerPrecisionTest, Int64MaxLosesPrecision) {
    int64_t value = INT64_MAX;
    EXPECT_TRUE(wouldLosePrecision(value));
}

TEST_F(HttpWriteIntegerPrecisionTest, Int64MinLosesPrecision) {
    int64_t value = INT64_MIN;
    EXPECT_TRUE(wouldLosePrecision(value));
}

// =================== Boundary behavior ===================

TEST_F(HttpWriteIntegerPrecisionTest, BoundaryExactlyAtThreshold) {
    // 2^53 exactly should NOT be flagged (it converts exactly)
    EXPECT_FALSE(wouldLosePrecision(MAX_EXACT_DOUBLE_INT));
    EXPECT_FALSE(wouldLosePrecision(-MAX_EXACT_DOUBLE_INT));

    // 2^53 + 1 SHOULD be flagged
    EXPECT_TRUE(wouldLosePrecision(MAX_EXACT_DOUBLE_INT + 1));
    EXPECT_TRUE(wouldLosePrecision(-MAX_EXACT_DOUBLE_INT - 1));
}

TEST_F(HttpWriteIntegerPrecisionTest, SafeValuesRoundTripExactly) {
    // Various values within the safe range all round-trip perfectly
    int64_t safe_values[] = {
        0,
        1,
        -1,
        42,
        -42,
        1000000,
        -1000000,
        1LL << 32,  // 4294967296
        -(1LL << 32),
        (1LL << 52),  // 4503599627370496
        -(1LL << 52),
        MAX_EXACT_DOUBLE_INT,  // 9007199254740992
        -MAX_EXACT_DOUBLE_INT,
    };

    for (int64_t value : safe_values) {
        EXPECT_FALSE(wouldLosePrecision(value)) << "Value " << value << " should be safe";
        EXPECT_EQ(static_cast<int64_t>(static_cast<double>(value)), value)
            << "Value " << value << " should round-trip exactly";
    }
}

TEST_F(HttpWriteIntegerPrecisionTest, UnsafeValuesDetected) {
    // Various values beyond the safe range
    int64_t unsafe_values[] = {
        MAX_EXACT_DOUBLE_INT + 1,
        MAX_EXACT_DOUBLE_INT + 3,
        -MAX_EXACT_DOUBLE_INT - 1,
        -MAX_EXACT_DOUBLE_INT - 3,
        1LL << 54,
        1LL << 60,
        1707998400000000000LL,  // Nanosecond timestamp
        INT64_MAX,
        INT64_MIN,
    };

    for (int64_t value : unsafe_values) {
        EXPECT_TRUE(wouldLosePrecision(value)) << "Value " << value << " should be flagged as unsafe";
    }
}

// =================== Specific precision loss examples ===================

TEST_F(HttpWriteIntegerPrecisionTest, DemonstrateActualPrecisionLossValues) {
    // 2^53 + 1 -> rounds to 2^53
    {
        int64_t value = 9007199254740993LL;
        double d = static_cast<double>(value);
        EXPECT_EQ(d, 9007199254740992.0) << "2^53+1 should round to 2^53 when cast to double";
    }

    // 2^53 + 2 -> actually representable (even number)
    {
        int64_t value = 9007199254740994LL;
        double d = static_cast<double>(value);
        EXPECT_EQ(d, 9007199254740994.0) << "2^53+2 happens to be representable as double (even)";
        // Note: wouldLosePrecision still returns true because
        // the value exceeds the threshold, even though this particular
        // value happens to survive. The check is conservative.
        EXPECT_TRUE(wouldLosePrecision(value));
    }

    // 2^53 + 3 -> rounds to 2^53 + 4
    {
        int64_t value = 9007199254740995LL;
        double d = static_cast<double>(value);
        EXPECT_EQ(d, 9007199254740996.0) << "2^53+3 should round to 2^53+4 when cast to double";
    }
}
