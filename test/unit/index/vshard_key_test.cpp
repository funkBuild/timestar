#include "../../../lib/index/native/vshard_key.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using timestar::VShardId;
using timestar::index::parseVShardIndexKey;
using timestar::index::vshardIndexKey;
using timestar::index::vshardKeyPrefix;
using timestar::index::vshardKeyRange;

TEST(VShardKeyTest, PrefixIsBigEndianTwoBytes) {
    const auto key = vshardIndexKey(VShardId{0x0123}, "payload");
    ASSERT_EQ(key.size(), 2u + 7u);
    EXPECT_EQ(static_cast<uint8_t>(key[0]), 0x01);  // big-endian high byte
    EXPECT_EQ(static_cast<uint8_t>(key[1]), 0x23);
    EXPECT_EQ(key.substr(2), "payload");
}

TEST(VShardKeyTest, ParseRoundTrips) {
    const std::string inner("the-inner-key\x00\xff", 15);  // embedded NUL preserved by length
    for (uint16_t v : {uint16_t{0}, uint16_t{1}, uint16_t{255}, uint16_t{256}, uint16_t{4095}}) {
        const auto key = vshardIndexKey(VShardId{v}, inner);
        const auto parsed = parseVShardIndexKey(key);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(parsed->first.value(), v);
        EXPECT_EQ(parsed->second, inner);
    }
}

TEST(VShardKeyTest, ParseRejectsShortAndReservedKeys) {
    EXPECT_FALSE(parseVShardIndexKey("").has_value());
    EXPECT_FALSE(parseVShardIndexKey("x").has_value());  // 1 byte < prefix
    // A prefix of 0x1000 (4096) is reserved/out of range.
    std::string reserved;
    reserved.push_back(static_cast<char>(0x10));
    reserved.push_back(static_cast<char>(0x00));
    reserved.append("rest");
    EXPECT_FALSE(parseVShardIndexKey(reserved).has_value());
}

TEST(VShardKeyTest, KeyRangeIsHalfOpenAndOrdersByVShard) {
    // A VShard's keys sort within [begin, end) and before the next VShard.
    const auto [begin5, end5] = vshardKeyRange(VShardId{5});
    EXPECT_EQ(begin5, vshardKeyPrefix(VShardId{5}));
    EXPECT_EQ(end5, vshardKeyPrefix(VShardId{6}));

    // Any key of VShard 5 is in [begin5, end5); a key of VShard 6 is not.
    const auto k5 = vshardIndexKey(VShardId{5}, "z");
    const auto k6 = vshardIndexKey(VShardId{6}, "");
    EXPECT_GE(k5, begin5);
    EXPECT_LT(k5, end5);
    EXPECT_GE(k6, end5);  // start of the next range

    // The maximum VShard's end bound is beyond any valid prefix (0x1000).
    const auto [begin4095, end4095] = vshardKeyRange(VShardId{4095});
    EXPECT_EQ(static_cast<uint8_t>(end4095[0]), 0x10);
    EXPECT_EQ(static_cast<uint8_t>(end4095[1]), 0x00);
    EXPECT_LT(vshardIndexKey(VShardId{4095}, "\xff\xff"), end4095);
}

TEST(VShardKeyTest, LexicographicOrderGroupsByVShard) {
    // A high-value inner key of a lower VShard still sorts before any key of a
    // higher VShard -- the whole point of the big-endian prefix.
    const auto lowVShardHighKey = vshardIndexKey(VShardId{1}, "\xff\xff\xff");
    const auto highVShardLowKey = vshardIndexKey(VShardId{2}, "");
    EXPECT_LT(lowVShardHighKey, highVShardLowKey);
}

}  // namespace
