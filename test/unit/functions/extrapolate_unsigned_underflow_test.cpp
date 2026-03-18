#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>

// =============================================================================
// Bug #22: Unsigned underflow in extrapolate boundary mode
//
// In interpolation_functions.cpp, the extrapolate branch computes:
//   static_cast<double>(targetTime - input.timestampAt(0))
// When targetTime < timestampAt(0), the unsigned subtraction wraps to a
// huge positive number, producing a wildly incorrect extrapolation.
// The fix casts both operands to double BEFORE subtraction:
//   static_cast<double>(targetTime) - static_cast<double>(input.timestampAt(0))
// =============================================================================

class ExtrapolateUnderflowTest : public ::testing::Test {
protected:
    std::string sourceCode;

    void SetUp() override {
#ifdef INTERPOLATION_FUNCTIONS_SOURCE_PATH
        std::ifstream file(INTERPOLATION_FUNCTIONS_SOURCE_PATH);
        if (file.is_open()) {
            std::stringstream ss;
            ss << file.rdbuf();
            sourceCode = ss.str();
            return;
        }
#endif
        std::vector<std::string> paths = {
            "../lib/functions/interpolation_functions.cpp",
            "../../lib/functions/interpolation_functions.cpp",
        };
        for (const auto& path : paths) {
            std::ifstream f(path);
            if (f.is_open()) {
                std::stringstream ss;
                ss << f.rdbuf();
                sourceCode = ss.str();
                return;
            }
        }
    }
};

TEST_F(ExtrapolateUnderflowTest, SourceFileLoaded) {
    ASSERT_FALSE(sourceCode.empty()) << "Could not load interpolation_functions.cpp";
}

TEST_F(ExtrapolateUnderflowTest, ExtrapolateCastsBothOperandsToDouble) {
    // Find the extrapolation computation near "Before first point"
    auto pos = sourceCode.find("Before first point");
    ASSERT_NE(pos, std::string::npos) << "Could not find 'Before first point' section";

    // Look at the next ~1000 chars for the extrapolation formula
    auto region = sourceCode.substr(pos, 1000);

    // The fix pattern: both operands cast to double before subtraction
    // static_cast<double>(targetTime) - static_cast<double>(input.timestampAt(0))
    EXPECT_NE(region.find("static_cast<double>(targetTime)"), std::string::npos)
        << "targetTime must be cast to double before subtraction to avoid unsigned underflow";

    EXPECT_NE(region.find("static_cast<double>(input.timestampAt(0))"), std::string::npos)
        << "timestampAt(0) must be cast to double before subtraction to avoid unsigned underflow";

    // The bug pattern should NOT be present: subtraction inside the cast
    // static_cast<double>(targetTime - input.timestampAt(0))
    EXPECT_EQ(region.find("static_cast<double>(targetTime - input.timestampAt(0))"), std::string::npos)
        << "Must NOT subtract uint64_t values before casting — this causes unsigned underflow";
}
