#include <gtest/gtest.h>
#include "../../../lib/encoding/integer_encoder.hpp"
#include <vector>
#include <cstdint>
#include <limits>
#include <fstream>
#include <string>
#include <sstream>

// Helper to do encode/decode round-trip and verify
static void verifyRoundTrip(const std::vector<uint64_t>& input) {
    AlignedBuffer encoded = IntegerEncoder::encode(input);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] = IntegerEncoder::decode(slice, input.size(), decoded, 0, UINT64_MAX);

    ASSERT_EQ(added, input.size()) << "Number of decoded values should match input size";
    ASSERT_EQ(decoded.size(), input.size()) << "Decoded vector size should match input size";
    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i
            << ": expected " << input[i] << " got " << decoded[i];
    }
}

// Test 1: Non-monotonic timestamps (timestamps that go backwards)
TEST(IntegerEncoderOverflow, NonMonotonicTimestamps) {
    // Values go 100 -> 50 (backwards) -> 200 (forward again)
    std::vector<uint64_t> values = {100, 50, 200};
    verifyRoundTrip(values);
}

// Test 2: Large deltas spanning significant portion of uint64_t range
TEST(IntegerEncoderOverflow, LargeDeltas) {
    std::vector<uint64_t> values = {0, UINT64_MAX / 2, UINT64_MAX};
    verifyRoundTrip(values);
}

// Test 3: Equal timestamps (zero deltas)
TEST(IntegerEncoderOverflow, EqualTimestamps) {
    std::vector<uint64_t> values = {1000, 1000, 1000, 2000};
    verifyRoundTrip(values);
}

// Test 4: Alternating pattern to stress delta-of-delta
TEST(IntegerEncoderOverflow, AlternatingPattern) {
    std::vector<uint64_t> values = {100, 200, 100, 200, 100};
    verifyRoundTrip(values);
}

// Test 5: Source-inspection test verifying the fix uses static_cast<int64_t>
TEST(IntegerEncoderOverflow, SourceUsesExplicitSignedCast) {
#ifdef INTEGER_ENCODER_BASIC_SOURCE_PATH
    std::string sourcePath = INTEGER_ENCODER_BASIC_SOURCE_PATH;
#else
    GTEST_SKIP() << "INTEGER_ENCODER_BASIC_SOURCE_PATH not defined";
#endif

    std::ifstream file(sourcePath);
    ASSERT_TRUE(file.is_open()) << "Could not open source file: " << sourcePath;

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string source = buffer.str();

    // The encode function should use static_cast<int64_t> for delta calculations
    // Check that the first delta line uses explicit casting
    EXPECT_NE(source.find("static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0])"),
              std::string::npos)
        << "First delta calculation should use static_cast<int64_t> for explicit signed arithmetic";

    // Check that the delta-of-delta calculations in the unrolled loop use explicit casting
    EXPECT_NE(source.find("static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i-1])"),
              std::string::npos)
        << "Delta-of-delta calculations should use static_cast<int64_t> for explicit signed arithmetic";

    // Check that the decode path uses explicit casting for reconstruction
    EXPECT_NE(source.find("static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta)"),
              std::string::npos)
        << "Decode reconstruction should use explicit cast: static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta)";

    // Verify the old implicit conversion pattern is NOT present
    // The pattern "int64_t delta = values[1] - values[0]" without casts should be gone
    // We look for the specific problematic pattern: assignment to int64_t from uint64_t subtraction
    // without static_cast
    size_t pos = 0;
    bool found_uncasted_delta = false;
    while ((pos = source.find("values[1] - values[0]", pos)) != std::string::npos) {
        // Check if there's a static_cast nearby (within 50 chars before)
        size_t start = (pos > 50) ? pos - 50 : 0;
        std::string context = source.substr(start, pos - start);
        if (context.find("static_cast<int64_t>") == std::string::npos) {
            found_uncasted_delta = true;
            break;
        }
        pos++;
    }
    EXPECT_FALSE(found_uncasted_delta)
        << "Found uncasted 'values[1] - values[0]' pattern - should use static_cast<int64_t>";
}
