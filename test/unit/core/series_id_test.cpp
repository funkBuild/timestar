#include <gtest/gtest.h>
#include <string>
#include <unordered_map>

#include "series_id.hpp"

TEST(SeriesId128Test, DefaultConstructorIsZero) {
    SeriesId128 id;
    EXPECT_TRUE(id.isZero());
}

TEST(SeriesId128Test, FromSeriesKeyNotZero) {
    SeriesId128 id = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    EXPECT_FALSE(id.isZero());
}

TEST(SeriesId128Test, Determinism) {
    SeriesId128 a = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128 b = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    EXPECT_EQ(a, b);
}

TEST(SeriesId128Test, DifferentKeysAreDifferent) {
    SeriesId128 a = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128 b = SeriesId128::fromSeriesKey("memory,host=server02 usage");
    EXPECT_NE(a, b);
}

TEST(SeriesId128Test, ToBytesFromBytesRoundtrip) {
    SeriesId128 original = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    std::string bytes = original.toBytes();
    SeriesId128 restored = SeriesId128::fromBytes(bytes);
    EXPECT_EQ(original, restored);
}

TEST(SeriesId128Test, ToHexFromHexRoundtrip) {
    SeriesId128 original = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    std::string hex = original.toHex();
    SeriesId128 restored = SeriesId128::fromHex(hex);
    EXPECT_EQ(original, restored);
}

TEST(SeriesId128Test, HexLength) {
    SeriesId128 id = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    std::string hex = id.toHex();
    EXPECT_EQ(hex.length(), 32u);  // 16 bytes * 2 hex chars each
}

TEST(SeriesId128Test, BytesLength) {
    SeriesId128 id = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    std::string bytes = id.toBytes();
    EXPECT_EQ(bytes.size(), 16u);
}

TEST(SeriesId128Test, FromBytesInvalidLength) {
    EXPECT_THROW(SeriesId128::fromBytes("tooshort"), std::runtime_error);
    EXPECT_THROW(SeriesId128::fromBytes("this string is way too long for 16 bytes"), std::runtime_error);
    EXPECT_THROW(SeriesId128::fromBytes(""), std::runtime_error);
}

TEST(SeriesId128Test, FromHexInvalidLength) {
    EXPECT_THROW(SeriesId128::fromHex("abcdef"), std::runtime_error);
    EXPECT_THROW(SeriesId128::fromHex(""), std::runtime_error);
    EXPECT_THROW(SeriesId128::fromHex("00112233445566778899aabbccddeeff00"), std::runtime_error);
}

TEST(SeriesId128Test, ComparisonOperators) {
    SeriesId128 a = SeriesId128::fromSeriesKey("aaa");
    SeriesId128 b = SeriesId128::fromSeriesKey("bbb");

    // a and b are different, so one must be less than the other.
    if (a < b) {
        EXPECT_TRUE(a < b);
        EXPECT_TRUE(b > a);
        EXPECT_TRUE(a <= b);
        EXPECT_TRUE(b >= a);
        EXPECT_FALSE(a > b);
        EXPECT_FALSE(b < a);
    } else {
        EXPECT_TRUE(b < a);
        EXPECT_TRUE(a > b);
        EXPECT_TRUE(b <= a);
        EXPECT_TRUE(a >= b);
        EXPECT_FALSE(b > a);
        EXPECT_FALSE(a < b);
    }

    // Self-comparison.
    EXPECT_TRUE(a <= a);
    EXPECT_TRUE(a >= a);
    EXPECT_FALSE(a < a);
    EXPECT_FALSE(a > a);
}

TEST(SeriesId128Test, EqualityOperators) {
    SeriesId128 a = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128 b = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128 c = SeriesId128::fromSeriesKey("memory,host=server02 usage");

    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(SeriesId128Test, HashConsistency) {
    SeriesId128 id = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128::Hash hasher;

    size_t hash1 = hasher(id);
    size_t hash2 = hasher(id);
    EXPECT_EQ(hash1, hash2);
}

TEST(SeriesId128Test, HashDifferentIds) {
    SeriesId128 a = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    SeriesId128 b = SeriesId128::fromSeriesKey("memory,host=server02 usage");
    SeriesId128::Hash hasher;

    // SHA1-based hashes of different keys should almost certainly differ.
    EXPECT_NE(hasher(a), hasher(b));
}

TEST(SeriesId128Test, StdHashSpecialization) {
    SeriesId128 id = SeriesId128::fromSeriesKey("cpu,host=server01 value");
    std::hash<SeriesId128> stdHasher;

    // Should compile and produce a consistent value.
    size_t h1 = stdHasher(id);
    size_t h2 = stdHasher(id);
    EXPECT_EQ(h1, h2);

    // Can be used as key in unordered_map.
    std::unordered_map<SeriesId128, int> map;
    map[id] = 42;
    EXPECT_EQ(map[id], 42);
}

TEST(SeriesId128Test, ExplicitStringConstructor) {
    SeriesId128 fromConstructor("some_key");
    SeriesId128 fromFactory = SeriesId128::fromSeriesKey("some_key");
    EXPECT_EQ(fromConstructor, fromFactory);
}
