// Integration F.5b (stage 1): the unfinalized cross-core query partial wire codec.
// Round-trips PartialAggregationResult across all three internal representations
// (bucketStates, sortedStates, sortedValues + collapsedState), and -- the property
// that actually matters -- proves the DECODED partials merge to the SAME result as
// the originals through the real Aggregator::mergePartialAggregationsGrouped, so the
// cross-node merge equals the single-node answer.
#include "../../../lib/cluster/data/agg_partial_codec.hpp"
#include "../../../lib/query/aggregator.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>

using namespace timestar::data;
using timestar::AggregationMethod;
using timestar::AggregationState;
using timestar::GroupedAggregationResult;
using timestar::PartialAggregationResult;

namespace {

AggregationState stateOf(std::vector<std::pair<double, uint64_t>> vals, bool collectRaw = false) {
    AggregationState s;
    s.collectRaw = collectRaw;
    for (auto [v, t] : vals)
        s.addValue(v, t);
    return s;
}

bool stateEq(const AggregationState& a, const AggregationState& b) {
    auto de = [](double x, double y) { return (std::isnan(x) && std::isnan(y)) || x == y; };
    return de(a.sum, b.sum) && de(a.sumCompensation, b.sumCompensation) && de(a.min, b.min) && de(a.max, b.max) &&
           de(a.latest, b.latest) && a.latestTimestamp == b.latestTimestamp && de(a.first, b.first) &&
           a.firstTimestamp == b.firstTimestamp && a.count == b.count && de(a.m2, b.m2) && de(a.mean, b.mean) &&
           a.rawValues == b.rawValues && a.rawValuesSaturated == b.rawValuesSaturated && a.collectRaw == b.collectRaw;
}

// A partial using the bucketStates representation (interval > 0).
PartialAggregationResult bucketed(const std::string& gk, const std::map<std::string, std::string>& tags) {
    PartialAggregationResult p;
    p.measurement = "m";
    p.fieldName = "v";
    p.groupKey = gk;
    p.groupKeyHash = std::hash<std::string>{}(gk);
    p.cachedTags = tags;
    p.bucketStates[1000] = stateOf({{10, 1000}, {20, 1001}});
    p.bucketStates[2000] = stateOf({{30, 2000}});
    p.totalPoints = 3;
    return p;
}

// A partial using the sortedStates representation (interval == 0, fallback path).
PartialAggregationResult sortedStates(const std::string& gk) {
    PartialAggregationResult p;
    p.measurement = "m";
    p.fieldName = "v";
    p.groupKey = gk;
    p.groupKeyHash = std::hash<std::string>{}(gk);
    p.sortedTimestamps = {5, 5};
    p.sortedStates = {stateOf({{10, 5}}), stateOf({{30, 5}})};  // two series at ts=5
    p.totalPoints = 2;
    return p;
}
}  // namespace

TEST(AggPartialCodec, RoundTripsAllRepresentations) {
    std::vector<PartialAggregationResult> in;
    in.push_back(bucketed("m\0region=west\0v", {{"region", "west"}}));
    in.push_back(sortedStates("m\0region=east\0v"));
    // A sortedValues (pushdown compact) partial + a collapsedState + rawValues.
    {
        PartialAggregationResult p;
        p.measurement = "m";
        p.fieldName = "v";
        p.groupKey = std::string("m\0\0v", 4);
        p.groupKeyHash = std::hash<std::string>{}(p.groupKey);
        p.sortedTimestamps = {1, 2, 3};
        p.sortedValues = {1.5, 2.5, 3.5};
        p.collapsedState = stateOf({{1.5, 1}, {2.5, 2}, {3.5, 3}}, /*collectRaw=*/true);
        p.totalPoints = 3;
        in.push_back(std::move(p));
    }
    // An empty partial (all-default) must survive too.
    in.push_back(PartialAggregationResult{});

    auto back = decodePartials(encodePartials(in));
    ASSERT_TRUE(back.has_value());
    ASSERT_EQ(back->size(), in.size());
    for (size_t i = 0; i < in.size(); ++i) {
        const auto& a = in[i];
        const auto& b = (*back)[i];
        EXPECT_EQ(a.measurement, b.measurement) << i;
        EXPECT_EQ(a.fieldName, b.fieldName) << i;
        EXPECT_EQ(a.groupKey, b.groupKey) << i;
        EXPECT_EQ(a.groupKeyHash, b.groupKeyHash) << i;
        EXPECT_EQ(a.cachedTags, b.cachedTags) << i;
        EXPECT_EQ(a.totalPoints, b.totalPoints) << i;
        EXPECT_EQ(a.sortedTimestamps, b.sortedTimestamps) << i;
        EXPECT_EQ(a.sortedValues, b.sortedValues) << i;
        ASSERT_EQ(a.bucketStates.size(), b.bucketStates.size()) << i;
        for (const auto& [bucket, st] : a.bucketStates) {
            ASSERT_TRUE(b.bucketStates.count(bucket)) << i;
            EXPECT_TRUE(stateEq(st, b.bucketStates.at(bucket))) << i << " bucket " << bucket;
        }
        ASSERT_EQ(a.sortedStates.size(), b.sortedStates.size()) << i;
        for (size_t k = 0; k < a.sortedStates.size(); ++k)
            EXPECT_TRUE(stateEq(a.sortedStates[k], b.sortedStates[k])) << i << " state " << k;
        EXPECT_EQ(a.collapsedState.has_value(), b.collapsedState.has_value()) << i;
        if (a.collapsedState && b.collapsedState)
            EXPECT_TRUE(stateEq(*a.collapsedState, *b.collapsedState)) << i;
    }
}

TEST(AggPartialCodec, TruncationAndCorruptionRejected) {
    std::vector<PartialAggregationResult> in{bucketed("m\0region=west\0v", {{"region", "west"}})};
    std::string full = encodePartials(in);
    ASSERT_TRUE(decodePartials(full).has_value());
    for (size_t n = 0; n < full.size(); ++n)
        EXPECT_FALSE(decodePartials(full.substr(0, n)).has_value()) << "prefix " << n;
    std::string bad = full;
    bad[bad.size() - 1] ^= 0xff;  // flip FNV trailer
    EXPECT_FALSE(decodePartials(bad).has_value());
}

// The property that matters: two partials of the same group, encoded then decoded,
// merge to the SAME grouped result as the originals -- spread (a fold-of-one-non-
// identity method) across two series at equal timestamp = 20, unchanged by the wire.
TEST(AggPartialCodec, DecodedPartialsMergeIdentically) {
    seastar::async([] {
        auto makePair = [] {
            std::vector<PartialAggregationResult> v;
            v.push_back(sortedStates("m\0region=west\0v"));  // series A: 10 at ts5
            // series B in the SAME group, value 30 at ts5.
            PartialAggregationResult b;
            b.measurement = "m";
            b.fieldName = "v";
            b.groupKey = std::string("m\0region=west\0v", 15);
            b.groupKeyHash = std::hash<std::string>{}(b.groupKey);
            b.cachedTags = {{"region", "west"}};
            b.sortedTimestamps = {5};
            b.sortedStates = {stateOf({{30, 5}})};
            b.totalPoints = 1;
            v[0].groupKey = b.groupKey;  // put A in the same group as B
            v[0].groupKeyHash = b.groupKeyHash;
            v[0].cachedTags = b.cachedTags;
            v.push_back(std::move(b));
            return v;
        };

        auto orig = makePair();
        auto decoded = decodePartials(encodePartials(makePair()));
        ASSERT_TRUE(decoded.has_value());

        auto mo = timestar::Aggregator::mergePartialAggregationsGrouped(orig, AggregationMethod::SPREAD).get();
        auto md = timestar::Aggregator::mergePartialAggregationsGrouped(*decoded, AggregationMethod::SPREAD).get();

        ASSERT_EQ(mo.size(), 1u);
        ASSERT_EQ(md.size(), mo.size());
        // Same group, same field, same spread value across the wire.
        EXPECT_EQ(md[0].measurement, mo[0].measurement);
        EXPECT_EQ(md[0].tags, mo[0].tags);
        auto pointValues = [](const GroupedAggregationResult& g) {
            std::vector<double> out;
            for (const auto& p : g.points)
                out.push_back(p.value);
            for (double d : g.rawValues)
                out.push_back(d);
            return out;
        };
        EXPECT_EQ(pointValues(md[0]), pointValues(mo[0]));
        ASSERT_FALSE(pointValues(mo[0]).empty());
        EXPECT_DOUBLE_EQ(pointValues(mo[0])[0], 20.0);  // spread = 30 - 10
    }).get();
}
