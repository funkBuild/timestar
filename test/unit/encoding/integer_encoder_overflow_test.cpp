#include "../../../lib/encoding/integer_encoder.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <vector>

// Helper to do encode/decode round-trip and verify
static void verifyRoundTrip(const std::vector<uint64_t>& input) {
    AlignedBuffer encoded = IntegerEncoder::encode(input);

    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> decoded;
    auto [skipped, added] = IntegerEncoder::decode(slice, input.size(), decoded, 0, UINT64_MAX);

    ASSERT_EQ(added, input.size()) << "Number of decoded values should match input size";
    ASSERT_EQ(decoded.size(), input.size()) << "Decoded vector size should match input size";
    for (size_t i = 0; i < input.size(); i++) {
        EXPECT_EQ(decoded[i], input[i]) << "Mismatch at index " << i << ": expected " << input[i] << " got "
                                        << decoded[i];
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

