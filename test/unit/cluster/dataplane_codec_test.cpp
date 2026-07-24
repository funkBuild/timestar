// Data-plane wire codec: round-trip + bounds/truncation robustness. Pure.
#include "../../../lib/cluster/data/dataplane_codec.hpp"

#include <gtest/gtest.h>

using namespace timestar::data;

namespace {
SeriesId128 sid(const std::string& k) { return SeriesId128::fromSeriesKey(k); }
}  // namespace

TEST(DataPlaneCodecTest, PointsRoundTrip) {
    std::vector<DataPoint> pts = {{sid("m,h=1 v"), 100, 1.5}, {sid("m,h=2 v"), 200, -3.25}};
    auto back = decodePoints(encodePoints(pts));
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->size(), 2u);
    EXPECT_TRUE((*back)[0].series == pts[0].series);
    EXPECT_EQ((*back)[0].timestamp, 100u);
    EXPECT_EQ((*back)[1].value, -3.25);
    EXPECT_TRUE(decodePoints(encodePoints({})).value().empty());  // empty batch
}

TEST(DataPlaneCodecTest, QuerySpecRoundTrip) {
    QuerySpec s{10, 20, AggMethod::Avg, {sid("a"), sid("b")}};
    auto back = decodeQuerySpec(encodeQuerySpec(s));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->startTime, 10u);
    EXPECT_EQ(back->endTime, 20u);
    EXPECT_EQ(back->method, AggMethod::Avg);
    EXPECT_EQ(back->series.size(), 2u);
}

TEST(DataPlaneCodecTest, QuerySpecBogusMethodRejected) {
    // A frame whose method byte is past AggMethod::Max(5) must decode to nullopt,
    // never an out-of-range enumerator reaching queryLocal.
    std::string wire = encodeQuerySpec(QuerySpec{10, 20, AggMethod::Max, {}});
    wire[16] = static_cast<char>(200);  // method byte = after startTime(8)+endTime(8)
    EXPECT_FALSE(decodeQuerySpec(wire).has_value());
    wire[16] = static_cast<char>(6);  // one past Max
    EXPECT_FALSE(decodeQuerySpec(wire).has_value());
    wire[16] = static_cast<char>(5);  // Max is still valid
    ASSERT_TRUE(decodeQuerySpec(wire).has_value());
    EXPECT_EQ(decodeQuerySpec(wire)->method, AggMethod::Max);
}

TEST(DataPlaneCodecTest, QueryPartialRoundTrip) {
    QueryPartial part;
    part.raw = {{sid("x"), 1, 2.0}};
    AggState st;
    st.add(3.0);
    st.add(7.0);  // count 2, sum 10, min 3, max 7
    part.perSeries[sid("y")] = st;
    auto back = decodeQueryPartial(encodeQueryPartial(part));
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->raw.size(), 1u);
    EXPECT_EQ(back->raw[0].value, 2.0);
    ASSERT_EQ(back->perSeries.size(), 1u);
    EXPECT_EQ(back->perSeries.at(sid("y")), st);
}

TEST(DataPlaneCodecTest, TruncatedAndBogusCountRejected) {
    std::string full = encodePoints({{sid("m,h=1 v"), 100, 1.5}});
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodePoints(full.substr(0, n)).has_value()) << "prefix " << n;
    EXPECT_TRUE(decodePoints(full).has_value());

    // Bogus point count with no bytes behind it.
    std::string bogus(4, static_cast<char>(0xff));  // count = 0xffffffff
    EXPECT_FALSE(decodePoints(bogus).has_value());
    // Partial (huge nagg after a valid raw section) rejected.
    std::string p = encodeQueryPartial({});  // raw=0, agg=0
    p[p.size() - 1] = static_cast<char>(0xff);  // corrupt agg count high byte
    EXPECT_FALSE(decodeQueryPartial(p).has_value());
}
