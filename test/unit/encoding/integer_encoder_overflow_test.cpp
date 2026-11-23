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


TEST(IntegerEncoderOverflow, FullUnsignedDomainAcrossBlocksAndScalarTails) {
    constexpr uint64_t high = uint64_t{1} << 63;
    for (size_t n : {2, 3, 7, 17, 1023, 1024, 1025, 2051}) {
        std::vector<uint64_t> values;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t extremes[] = {UINT64_MAX, 0, high - 1, high, 1, UINT64_MAX - 1, 7};
            values.push_back(extremes[i % 7]);
        }
        verifyRoundTrip(values);
    }
}

TEST(IntegerEncoderOverflow, FilteredDecodeCrossesSignedBoundary) {
    constexpr uint64_t high = uint64_t{1} << 63;
    std::vector<uint64_t> input{0, 1, high - 2, high - 1, high, high + 1, UINT64_MAX - 1, UINT64_MAX};
    auto encoded = IntegerEncoder::encode(input);
    Slice slice(encoded.data.data(), encoded.size());
    std::vector<uint64_t> output;
    const auto [skipped, added] = IntegerEncoder::decode(slice, input.size(), output, high - 1, UINT64_MAX - 1);
    EXPECT_EQ(skipped, 3);
    EXPECT_EQ(added, 4);
    EXPECT_EQ(output, (std::vector<uint64_t>{high - 1, high, high + 1, UINT64_MAX - 1}));
}
