#include <gtest/gtest.h>
#include "tsxor_encoder.hpp"
#include <vector>
#include <cstdint>
#include <cmath>
#include <limits>

// --- Window tests ---

TEST(WindowTest, InitiallyZeros) {
    Window w;
    EXPECT_TRUE(w.contains(0));
    EXPECT_EQ(w.getLast(), 0u);
}

TEST(WindowTest, InsertAndContains) {
    Window w;
    w.insert(1);
    w.insert(2);
    w.insert(3);
    EXPECT_TRUE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));
    EXPECT_FALSE(w.contains(99));
}

TEST(WindowTest, InsertPushesOutOld) {
    // Window of size 3, initially filled with zeros
    Window w(3);
    EXPECT_TRUE(w.contains(0));

    w.insert(1);
    w.insert(2);
    w.insert(3);
    // At this point the 3 initial zeros have been pushed out
    EXPECT_FALSE(w.contains(0));
    EXPECT_TRUE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));

    w.insert(4);
    // Now 1 should be pushed out
    EXPECT_FALSE(w.contains(1));
    EXPECT_TRUE(w.contains(2));
    EXPECT_TRUE(w.contains(3));
    EXPECT_TRUE(w.contains(4));
}

TEST(WindowTest, GetIndexOf) {
    Window w(5);
    w.insert(10);
    w.insert(20);
    w.insert(30);
    // Deque front is 30, then 20, then 10, then two zeros
    EXPECT_EQ(w.getIndexOf(30), 0);
    EXPECT_EQ(w.getIndexOf(20), 1);
    EXPECT_EQ(w.getIndexOf(10), 2);
}

TEST(WindowTest, GetLast) {
    Window w;
    w.insert(42);
    EXPECT_EQ(w.getLast(), 42u);

    w.insert(99);
    EXPECT_EQ(w.getLast(), 99u);
}

TEST(WindowTest, GetByOffset) {
    Window w(5);
    w.insert(10);
    w.insert(20);
    w.insert(30);
    // get(0) should be the front (last inserted)
    EXPECT_EQ(w.get(0), w.getLast());
    EXPECT_EQ(w.get(0), 30u);
    EXPECT_EQ(w.get(1), 20u);
    EXPECT_EQ(w.get(2), 10u);
}

TEST(WindowTest, GetCandidateExact) {
    Window w;
    uint64_t val = 0xDEADBEEF;
    w.insert(val);
    // getCandidate with the same value should return it (XOR=0, best match)
    uint64_t candidate = w.getCandidate(val);
    EXPECT_EQ(candidate, val);
}

TEST(WindowTest, GetCandidateBestMatch) {
    Window w;
    // Insert values with different bit patterns
    w.insert(0xFF00FF00FF00FF00ULL);  // alternating bytes
    w.insert(0x0000000000000001ULL);  // very different from target
    w.insert(0xFFFFFFFFFFFFFFFEULL);  // close to all-ones

    // Target is all-ones: 0xFFFFFFFFFFFFFFFF
    // Best match should be 0xFFFFFFFFFFFFFFFE (XOR = 1, most leading+trailing zeros)
    uint64_t target = 0xFFFFFFFFFFFFFFFFULL;
    uint64_t candidate = w.getCandidate(target);
    EXPECT_EQ(candidate, 0xFFFFFFFFFFFFFFFEULL);
}

// --- TsxorEncoder tests ---

TEST(TsxorEncoderTest, EncodeEmptyVector) {
    std::vector<double> values;
    CompressedBuffer result = TsxorEncoder::encode(values);
    // Empty input should produce a buffer with no data written
    EXPECT_EQ(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeConstantValues) {
    std::vector<double> values(100, 42.0);
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeIncreasingValues) {
    std::vector<double> values(100);
    for (int i = 0; i < 100; i++) {
        values[i] = static_cast<double>(i) * 1.5;
    }
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}

TEST(TsxorEncoderTest, EncodeProducesCompressedOutput) {
    // 1000 constant doubles should compress very well via window matching
    std::vector<double> values(1000, 42.0);

    CompressedBuffer result = TsxorEncoder::encode(values);
    size_t rawSize = 1000 * sizeof(double);
    EXPECT_LT(result.size(), rawSize)
        << "Compressed size (" << result.size()
        << ") should be less than raw size (" << rawSize << ")";
}

TEST(TsxorEncoderTest, EncodeHandlesSpecialValues) {
    std::vector<double> values = {
        0.0,
        -0.0,
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::min(),
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::denorm_min()
    };

    // Should not crash
    CompressedBuffer result = TsxorEncoder::encode(values);
    EXPECT_GT(result.size(), 0u);
}
