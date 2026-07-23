#include "../../../lib/storage/vshard_verification_hash.hpp"

#include "../../../lib/core/series_id.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <stdexcept>
#include <string>

namespace {

using timestar::VShardVerificationHash;

SeriesId128 sid(uint8_t fill) {
    return SeriesId128::fromBytes(std::string(16, static_cast<char>(fill)));
}

TEST(VShardVerificationHashTest, EmptyIsWellDefinedAndStable) {
    VShardVerificationHash a;
    VShardVerificationHash b;
    EXPECT_EQ(a.digestHex().size(), 32u);
    EXPECT_EQ(a.digestHex(), b.digestHex());
}

TEST(VShardVerificationHashTest, IdenticalStreamsMatch) {
    VShardVerificationHash a;
    VShardVerificationHash b;
    for (auto* h : {&a, &b}) {
        h->addFloat(sid(1), 100, 1.5);
        h->addFloat(sid(1), 200, 2.5);
        h->addInteger(sid(2), 100, 42);
        h->addString(sid(3), 50, "hello");
    }
    EXPECT_EQ(a.digestHex(), b.digestHex());
}

TEST(VShardVerificationHashTest, AnyValueDifferenceChangesTheHash) {
    VShardVerificationHash base;
    base.addFloat(sid(1), 100, 1.5);
    const std::string baseHex = base.digestHex();

    VShardVerificationHash diffVal;
    diffVal.addFloat(sid(1), 100, 1.6);  // different value
    EXPECT_NE(diffVal.digestHex(), baseHex);

    VShardVerificationHash diffTs;
    diffTs.addFloat(sid(1), 101, 1.5);  // different timestamp
    EXPECT_NE(diffTs.digestHex(), baseHex);

    VShardVerificationHash diffSeries;
    diffSeries.addFloat(sid(2), 100, 1.5);  // different series
    EXPECT_NE(diffSeries.digestHex(), baseHex);
}

TEST(VShardVerificationHashTest, SignedZeroAndNaNBitsAreDistinguished) {
    VShardVerificationHash pos;
    pos.addFloat(sid(1), 1, 0.0);
    VShardVerificationHash neg;
    neg.addFloat(sid(1), 1, -0.0);  // same numeric value, different bits
    EXPECT_NE(pos.digestHex(), neg.digestHex());

    VShardVerificationHash nan;
    nan.addFloat(sid(1), 1, std::nan(""));
    EXPECT_NE(nan.digestHex(), pos.digestHex());
}

TEST(VShardVerificationHashTest, TypeTagPreventsCrossTypeCollision) {
    // Float 0.0 and integer 0 share the same value bits; the type tag must still
    // separate them.
    VShardVerificationHash asFloat;
    asFloat.addFloat(sid(1), 1, 0.0);
    VShardVerificationHash asInt;
    asInt.addInteger(sid(1), 1, 0);
    EXPECT_NE(asFloat.digestHex(), asInt.digestHex());

    // Boolean false (byte 0) must not collide with either.
    VShardVerificationHash asBool;
    asBool.addBoolean(sid(1), 1, false);
    EXPECT_NE(asBool.digestHex(), asFloat.digestHex());
    EXPECT_NE(asBool.digestHex(), asInt.digestHex());
}

TEST(VShardVerificationHashTest, StringLengthPrefixAvoidsBoundaryCollision) {
    // Without length prefixing, ("a" then "b") and ("ab" then "") could collide.
    VShardVerificationHash split;
    split.addString(sid(1), 1, "a");
    split.addString(sid(1), 2, "b");

    VShardVerificationHash joined;
    joined.addString(sid(1), 1, "ab");
    joined.addString(sid(1), 2, "");
    EXPECT_NE(split.digestHex(), joined.digestHex());
}

TEST(VShardVerificationHashTest, OutOfOrderOrDuplicateFailsClosed) {
    {
        VShardVerificationHash h;
        h.addFloat(sid(1), 200, 1.0);
        EXPECT_THROW(h.addFloat(sid(1), 100, 1.0), std::logic_error);  // timestamp regression
    }
    {
        VShardVerificationHash h;
        h.addFloat(sid(1), 100, 1.0);
        EXPECT_THROW(h.addFloat(sid(1), 100, 9.0), std::logic_error);  // duplicate (series, ts)
    }
    {
        VShardVerificationHash h;
        h.addFloat(sid(5), 100, 1.0);
        EXPECT_THROW(h.addFloat(sid(1), 100, 1.0), std::logic_error);  // series regression
    }
    {
        // Series increasing but timestamp resets is fine: ts is scoped per series.
        VShardVerificationHash h;
        h.addFloat(sid(1), 900, 1.0);
        EXPECT_NO_THROW(h.addFloat(sid(2), 1, 1.0));
    }
}

TEST(VShardVerificationHashTest, DigestIsNonDestructive) {
    VShardVerificationHash h;
    h.addFloat(sid(1), 1, 1.0);
    const std::string first = h.digestHex();
    EXPECT_EQ(h.digestHex(), first);  // idempotent

    h.addFloat(sid(1), 2, 2.0);  // more points after digesting
    EXPECT_NE(h.digestHex(), first);
}

}  // namespace
