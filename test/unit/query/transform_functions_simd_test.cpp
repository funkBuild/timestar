#include "../../../lib/query/transform/transform_functions_simd.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <vector>

namespace simd = timestar::transform::simd;

// ============================================================================
// Correctness tests for the Highway-dispatched transform kernels.
// Each kernel is validated against a small inline scalar reference
// implementation, on arrays large enough to exercise the SIMD body
// (size > SIMD_MIN_SIZE) and including NaN inputs.
// ============================================================================

namespace ref {

// Reference: diff that treats a negative delta as a counter reset (returns
// the current value); NaN operands propagate NaN; result[0] is NaN.
std::vector<double> monotonic_diff(const std::vector<double>& values) {
    if (values.size() < 2) {
        return std::vector<double>(values.size(), std::nan(""));
    }
    std::vector<double> result(values.size());
    result[0] = std::nan("");
    for (size_t i = 1; i < values.size(); ++i) {
        if (std::isnan(values[i]) || std::isnan(values[i - 1])) {
            result[i] = std::nan("");
        } else {
            double dv = values[i] - values[i - 1];
            result[i] = (dv >= 0) ? dv : values[i];
        }
    }
    return result;
}

// Reference: values below the threshold become NaN; NaN inputs pass through.
std::vector<double> cutoff_min(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] < threshold) ? std::nan("") : values[i];
    }
    return result;
}

// Reference: values above the threshold become NaN; NaN inputs pass through.
std::vector<double> cutoff_max(const std::vector<double>& values, double threshold) {
    std::vector<double> result(values.size());
    for (size_t i = 0; i < values.size(); ++i) {
        result[i] = (!std::isnan(values[i]) && values[i] > threshold) ? std::nan("") : values[i];
    }
    return result;
}

}  // namespace ref

// NaN-aware equality: NaN == NaN counts as equal.
static bool nearEqual(double a, double b) {
    if (std::isnan(a) && std::isnan(b)) {
        return true;
    }
    return a == b;
}

class TransformFunctionsSimdTest : public ::testing::Test {};

TEST_F(TransformFunctionsSimdTest, MonotonicDiffMatchesScalarRef) {
    // Array size > SIMD_MIN_SIZE to trigger the SIMD code path, with counter
    // resets and NaN values at various positions (including adjacent to resets).
    std::vector<double> values(32);
    for (size_t i = 0; i < 32; ++i) {
        values[i] = static_cast<double>(i * 10);
    }
    values[5] = 10.0;           // Reset: 10 - 40 = -30, should return 10.0
    values[15] = 20.0;          // Reset: 20 - 130 = -110, should return 20.0
    values[25] = 5.0;           // Reset: 5 - 230 = -225, should return 5.0
    values[9] = std::nan("");   // NaN input: result[9] and result[10] must be NaN
    values[20] = std::nan("");  // NaN in SIMD body

    auto result = simd::monotonic_diff(values);
    auto expected = ref::monotonic_diff(values);

    ASSERT_EQ(result.size(), expected.size());
    EXPECT_TRUE(std::isnan(result[0]));

    // Spot-check the documented semantics directly
    EXPECT_DOUBLE_EQ(result[5], 10.0) << "Counter reset at index 5 should return current value (10.0)";
    EXPECT_DOUBLE_EQ(result[15], 20.0) << "Counter reset at index 15 should return current value (20.0)";
    EXPECT_DOUBLE_EQ(result[25], 5.0) << "Counter reset at index 25 should return current value (5.0)";
    EXPECT_TRUE(std::isnan(result[9])) << "NaN input should produce NaN";
    EXPECT_TRUE(std::isnan(result[10])) << "Previous-value NaN should produce NaN";

    for (size_t i = 0; i < result.size(); ++i) {
        EXPECT_TRUE(nearEqual(result[i], expected[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << expected[i];
    }
}

TEST_F(TransformFunctionsSimdTest, CutoffMinMatchesScalarRef) {
    // Large array to trigger SIMD path, with NaN values mixed in
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 7 == 0) {
            values[i] = std::nan("");  // NaN at positions 0, 7, 14, 21, ...
        } else {
            values[i] = static_cast<double>(i) - 50.0;  // Range: -49 to 49
        }
    }

    auto result = simd::cutoff_min(values, 0.0);
    auto expected = ref::cutoff_min(values, 0.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i % 7 == 0) {
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved at index " << i;
        } else if (values[i] < 0.0) {
            EXPECT_TRUE(std::isnan(result[i]))
                << "Value " << values[i] << " below threshold should be NaN at index " << i;
        } else {
            EXPECT_DOUBLE_EQ(result[i], values[i])
                << "Value " << values[i] << " at/above threshold should be preserved at index " << i;
        }
        EXPECT_TRUE(nearEqual(result[i], expected[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << expected[i];
    }
}

TEST_F(TransformFunctionsSimdTest, CutoffMaxMatchesScalarRef) {
    // Large array to trigger SIMD path, with NaN values mixed in
    std::vector<double> values(100);
    for (size_t i = 0; i < values.size(); ++i) {
        if (i % 7 == 0) {
            values[i] = std::nan("");  // NaN at positions 0, 7, 14, 21, ...
        } else {
            values[i] = static_cast<double>(i);  // Range: 0 to 99
        }
    }

    auto result = simd::cutoff_max(values, 50.0);
    auto expected = ref::cutoff_max(values, 50.0);

    ASSERT_EQ(result.size(), values.size());
    for (size_t i = 0; i < result.size(); ++i) {
        if (i % 7 == 0) {
            EXPECT_TRUE(std::isnan(result[i])) << "NaN should be preserved at index " << i;
        } else if (values[i] > 50.0) {
            EXPECT_TRUE(std::isnan(result[i]))
                << "Value " << values[i] << " above threshold should be NaN at index " << i;
        } else {
            EXPECT_DOUBLE_EQ(result[i], values[i])
                << "Value " << values[i] << " at/below threshold should be preserved at index " << i;
        }
        EXPECT_TRUE(nearEqual(result[i], expected[i]))
            << "SIMD and scalar disagree at index " << i << ": simd=" << result[i] << ", scalar=" << expected[i];
    }
}
