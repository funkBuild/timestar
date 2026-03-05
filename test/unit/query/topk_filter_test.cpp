#include <gtest/gtest.h>
#include "topk_filter.hpp"
#include "expression_evaluator.hpp"
#include "expression_parser.hpp"

#include <cmath>
#include <limits>
#include <vector>
#include <map>
#include <string>

using namespace tsdb;

// ==================== Helper Utilities ====================

static AlignedSeries makeSeries(
    const std::vector<uint64_t>& ts,
    const std::vector<double>& vals) {
    return AlignedSeries(ts, vals);
}

// Build a GroupedSeries with the given tag key/value pair and series data.
static GroupedSeries makeGroup(
    const std::string& tagKey, const std::string& tagValue,
    const std::vector<uint64_t>& ts,
    const std::vector<double>& vals) {
    GroupedSeries g;
    g.group_tags[tagKey] = tagValue;
    g.series = makeSeries(ts, vals);
    return g;
}

// Build a GroupedSeries with arbitrary tag map.
static GroupedSeries makeGroupTags(
    const std::map<std::string, std::string>& tags,
    const std::vector<uint64_t>& ts,
    const std::vector<double>& vals) {
    GroupedSeries g;
    g.group_tags = tags;
    g.series = makeSeries(ts, vals);
    return g;
}

// Shared timestamps used in most tests (4 points, 1s apart in ns)
static const std::vector<uint64_t> kTs4 = {1000000000ULL, 2000000000ULL,
                                             3000000000ULL, 4000000000ULL};

// ==================== seriesMeanIgnoreNaN Tests ====================

TEST(SeriesMeanIgnoreNaN, AllFinite) {
    auto s = makeSeries({1000, 2000, 3000, 4000}, {1.0, 2.0, 3.0, 4.0});
    double m = seriesMeanIgnoreNaN(s);
    EXPECT_DOUBLE_EQ(m, 2.5);
}

TEST(SeriesMeanIgnoreNaN, SomeNaN) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto s = makeSeries({1000, 2000, 3000, 4000},
                        {nan, 2.0, nan, 4.0});
    double m = seriesMeanIgnoreNaN(s);
    // mean of {2.0, 4.0} = 3.0
    EXPECT_DOUBLE_EQ(m, 3.0);
}

TEST(SeriesMeanIgnoreNaN, AllNaN) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    auto s = makeSeries({1000, 2000}, {nan, nan});
    double m = seriesMeanIgnoreNaN(s);
    // All-NaN: returns -infinity
    EXPECT_TRUE(std::isinf(m));
    EXPECT_LT(m, 0.0);
}

TEST(SeriesMeanIgnoreNaN, Empty) {
    AlignedSeries s;
    double m = seriesMeanIgnoreNaN(s);
    // Empty: returns -infinity
    EXPECT_TRUE(std::isinf(m));
    EXPECT_LT(m, 0.0);
}

TEST(SeriesMeanIgnoreNaN, SinglePoint) {
    auto s = makeSeries({1000}, {42.0});
    double m = seriesMeanIgnoreNaN(s);
    EXPECT_DOUBLE_EQ(m, 42.0);
}

// ==================== topk() Tests ====================

TEST(TopkFilter, TopkFourGroupsKeepTwo) {
    // 4 groups with means: host1=10, host2=40, host3=20, host4=30
    // topk(2, ...) should keep host2 (mean=40) and host4 (mean=30)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "host1", kTs4, {8.0,  10.0, 10.0, 12.0})); // mean=10
    groups.push_back(makeGroup("host", "host2", kTs4, {38.0, 40.0, 40.0, 42.0})); // mean=40
    groups.push_back(makeGroup("host", "host3", kTs4, {18.0, 20.0, 20.0, 22.0})); // mean=20
    groups.push_back(makeGroup("host", "host4", kTs4, {28.0, 30.0, 30.0, 32.0})); // mean=30

    auto result = topk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    // Original order preserved among selected (host2 was index 1, host4 was index 3)
    EXPECT_EQ(result[0].group_tags.at("host"), "host2");
    EXPECT_EQ(result[1].group_tags.at("host"), "host4");
}

TEST(TopkFilter, BottomkFourGroupsKeepTwo) {
    // 4 groups with means: host1=10, host2=40, host3=20, host4=30
    // bottomk(2, ...) should keep host1 (mean=10) and host3 (mean=20)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "host1", kTs4, {8.0,  10.0, 10.0, 12.0})); // mean=10
    groups.push_back(makeGroup("host", "host2", kTs4, {38.0, 40.0, 40.0, 42.0})); // mean=40
    groups.push_back(makeGroup("host", "host3", kTs4, {18.0, 20.0, 20.0, 22.0})); // mean=20
    groups.push_back(makeGroup("host", "host4", kTs4, {28.0, 30.0, 30.0, 32.0})); // mean=30

    auto result = bottomk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    // Original order preserved among selected (host1 was index 0, host3 was index 2)
    EXPECT_EQ(result[0].group_tags.at("host"), "host1");
    EXPECT_EQ(result[1].group_tags.at("host"), "host3");
}

TEST(TopkFilter, TopkNEqualsGroupCount) {
    // N == number of groups: all groups returned, in original order
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kTs4, {1.0, 1.0, 1.0, 1.0}));
    groups.push_back(makeGroup("host", "b", kTs4, {5.0, 5.0, 5.0, 5.0}));
    groups.push_back(makeGroup("host", "c", kTs4, {3.0, 3.0, 3.0, 3.0}));

    auto result = topk(3, std::move(groups));

    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0].group_tags.at("host"), "a");
    EXPECT_EQ(result[1].group_tags.at("host"), "b");
    EXPECT_EQ(result[2].group_tags.at("host"), "c");
}

TEST(TopkFilter, BottomkNEqualsGroupCount) {
    // N == number of groups: all groups returned, in original order
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "x", kTs4, {100.0, 100.0, 100.0, 100.0}));
    groups.push_back(makeGroup("host", "y", kTs4, {200.0, 200.0, 200.0, 200.0}));

    auto result = bottomk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].group_tags.at("host"), "x");
    EXPECT_EQ(result[1].group_tags.at("host"), "y");
}

TEST(TopkFilter, TopkNGreaterThanGroupCount) {
    // N > number of groups: all groups returned unchanged (original order)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("dc", "dc1", kTs4, {50.0, 50.0, 50.0, 50.0}));
    groups.push_back(makeGroup("dc", "dc2", kTs4, {20.0, 20.0, 20.0, 20.0}));

    auto result = topk(100, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].group_tags.at("dc"), "dc1");
    EXPECT_EQ(result[1].group_tags.at("dc"), "dc2");
}

TEST(TopkFilter, BottomkNGreaterThanGroupCount) {
    // N > number of groups: all groups returned unchanged (original order)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("dc", "us-west", kTs4, {7.0, 7.0, 7.0, 7.0}));

    auto result = bottomk(999, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("dc"), "us-west");
}

TEST(TopkFilter, TopkN1KeepsHighestGroup) {
    // topk(1, ...) with 4 groups: only the group with the highest mean is kept
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kTs4, {1.0, 1.0, 1.0, 1.0}));   // mean=1
    groups.push_back(makeGroup("host", "b", kTs4, {99.0, 99.0, 99.0, 99.0})); // mean=99
    groups.push_back(makeGroup("host", "c", kTs4, {50.0, 50.0, 50.0, 50.0})); // mean=50
    groups.push_back(makeGroup("host", "d", kTs4, {10.0, 10.0, 10.0, 10.0})); // mean=10

    auto result = topk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("host"), "b");
    // Verify the series data is preserved correctly
    for (double v : result[0].series.values) {
        EXPECT_DOUBLE_EQ(v, 99.0);
    }
}

TEST(TopkFilter, BottomkN1KeepsLowestGroup) {
    // bottomk(1, ...) with 4 groups: only the group with the lowest mean is kept
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kTs4, {1.0, 1.0, 1.0, 1.0}));   // mean=1
    groups.push_back(makeGroup("host", "b", kTs4, {99.0, 99.0, 99.0, 99.0})); // mean=99
    groups.push_back(makeGroup("host", "c", kTs4, {50.0, 50.0, 50.0, 50.0})); // mean=50
    groups.push_back(makeGroup("host", "d", kTs4, {10.0, 10.0, 10.0, 10.0})); // mean=10

    auto result = bottomk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("host"), "a");
    for (double v : result[0].series.values) {
        EXPECT_DOUBLE_EQ(v, 1.0);
    }
}

TEST(TopkFilter, SingleGroupTopkKeepsIt) {
    // Single group input: topk(1, ...) keeps it
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "only", kTs4, {5.0, 10.0, 15.0, 20.0}));

    auto result = topk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("host"), "only");
}

TEST(TopkFilter, SingleGroupBottomkKeepsIt) {
    // Single group input: bottomk(1, ...) keeps it
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "solo", kTs4, {100.0, 200.0, 300.0, 400.0}));

    auto result = bottomk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("host"), "solo");
}

// ==================== NaN-handling Tests ====================

TEST(TopkFilter, NanOnlyGroupRanksLowestForTopk) {
    // A NaN-only group has mean=-inf, so it ranks last in topk and is excluded first.
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "nan_host",
        kTs4, {nan, nan, nan, nan}));   // mean=-inf
    groups.push_back(makeGroup("host", "low_host",
        kTs4, {2.0, 2.0, 2.0, 2.0}));  // mean=2
    groups.push_back(makeGroup("host", "high_host",
        kTs4, {8.0, 8.0, 8.0, 8.0}));  // mean=8

    // topk(2, ...) keeps the 2 highest: high_host (8) and low_host (2)
    auto result = topk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    // Original indices: nan_host=0, low_host=1, high_host=2
    // Selected: low_host (idx 1) and high_host (idx 2), sorted by original index
    EXPECT_EQ(result[0].group_tags.at("host"), "low_host");
    EXPECT_EQ(result[1].group_tags.at("host"), "high_host");
}

TEST(TopkFilter, NanOnlyGroupRanksLowestForBottomk) {
    // A NaN-only group has mean=-inf, so it ranks first in bottomk (lowest mean).
    // bottomk(1, ...) keeps only the NaN group.
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "normal1",
        kTs4, {5.0, 5.0, 5.0, 5.0}));  // mean=5
    groups.push_back(makeGroup("host", "nan_host",
        kTs4, {nan, nan, nan, nan}));   // mean=-inf -> lowest
    groups.push_back(makeGroup("host", "normal2",
        kTs4, {3.0, 3.0, 3.0, 3.0}));  // mean=3

    // bottomk(1, ...) keeps the 1 lowest: nan_host (mean=-inf)
    auto result = bottomk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("host"), "nan_host");
}

TEST(TopkFilter, MixedNaNAndFiniteTopk) {
    // Some NaN values in otherwise-valid groups are ignored in mean computation
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    // Group A: values {nan, 10, nan, 10} -> mean=10 (ignoring NaN)
    groups.push_back(makeGroup("zone", "A",
        kTs4, {nan, 10.0, nan, 10.0}));
    // Group B: values {5, 5, 5, 5} -> mean=5
    groups.push_back(makeGroup("zone", "B",
        kTs4, {5.0, 5.0, 5.0, 5.0}));
    // Group C: values {20, 20, 20, 20} -> mean=20
    groups.push_back(makeGroup("zone", "C",
        kTs4, {20.0, 20.0, 20.0, 20.0}));

    // topk(2, ...) keeps C (mean=20) and A (mean=10)
    auto result = topk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    // Original indices: A=0, B=1, C=2 -> selected A(0) and C(2), sorted by original index
    EXPECT_EQ(result[0].group_tags.at("zone"), "A");
    EXPECT_EQ(result[1].group_tags.at("zone"), "C");
}

TEST(TopkFilter, MixedNaNAndFiniteBottomk) {
    // bottomk with some NaN values in groups
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    // Group A: values {nan, 10, nan, 10} -> mean=10
    groups.push_back(makeGroup("zone", "A",
        kTs4, {nan, 10.0, nan, 10.0}));
    // Group B: values {5, 5, 5, 5} -> mean=5
    groups.push_back(makeGroup("zone", "B",
        kTs4, {5.0, 5.0, 5.0, 5.0}));
    // Group C: values {20, 20, 20, 20} -> mean=20
    groups.push_back(makeGroup("zone", "C",
        kTs4, {20.0, 20.0, 20.0, 20.0}));

    // bottomk(2, ...) keeps B (mean=5) and A (mean=10)
    auto result = bottomk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    // Original indices: A=0, B=1, C=2 -> selected A(0) and B(1)
    EXPECT_EQ(result[0].group_tags.at("zone"), "A");
    EXPECT_EQ(result[1].group_tags.at("zone"), "B");
}

// ==================== Edge Cases ====================

TEST(TopkFilter, TopkN0ReturnsEmpty) {
    // topk(0, ...) -> no groups kept
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "x", kTs4, {1.0, 2.0, 3.0, 4.0}));
    groups.push_back(makeGroup("host", "y", kTs4, {5.0, 6.0, 7.0, 8.0}));

    auto result = topk(0, std::move(groups));
    EXPECT_EQ(result.size(), 0u);
}

TEST(TopkFilter, BottomkN0ReturnsEmpty) {
    // bottomk(0, ...) -> no groups kept
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "x", kTs4, {1.0, 2.0, 3.0, 4.0}));

    auto result = bottomk(0, std::move(groups));
    EXPECT_EQ(result.size(), 0u);
}

TEST(TopkFilter, TopkEmptyGroupList) {
    // Empty input -> empty output (N >= 0 = 0 groups)
    std::vector<GroupedSeries> groups;
    auto result = topk(5, std::move(groups));
    EXPECT_EQ(result.size(), 0u);
}

TEST(TopkFilter, BottomkEmptyGroupList) {
    std::vector<GroupedSeries> groups;
    auto result = bottomk(5, std::move(groups));
    EXPECT_EQ(result.size(), 0u);
}

TEST(TopkFilter, TopkNegativeNThrows) {
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kTs4, {1.0, 2.0, 3.0, 4.0}));
    EXPECT_THROW(topk(-1, std::move(groups)), EvaluationException);
}

TEST(TopkFilter, BottomkNegativeNThrows) {
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kTs4, {1.0, 2.0, 3.0, 4.0}));
    EXPECT_THROW(bottomk(-1, std::move(groups)), EvaluationException);
}

TEST(TopkFilter, OriginalOrderPreservedTopk) {
    // Confirm that the original insertion order is preserved among selected groups.
    // Groups in order: A(mean=3), B(mean=1), C(mean=5), D(mean=2)
    // topk(3, ...) keeps A, C, D (drop B which has mean=1)
    // Selected in original order: A(idx 0), C(idx 2), D(idx 3)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("g", "A", kTs4, {3.0, 3.0, 3.0, 3.0})); // mean=3
    groups.push_back(makeGroup("g", "B", kTs4, {1.0, 1.0, 1.0, 1.0})); // mean=1 <- dropped
    groups.push_back(makeGroup("g", "C", kTs4, {5.0, 5.0, 5.0, 5.0})); // mean=5
    groups.push_back(makeGroup("g", "D", kTs4, {2.0, 2.0, 2.0, 2.0})); // mean=2

    auto result = topk(3, std::move(groups));

    ASSERT_EQ(result.size(), 3u);
    EXPECT_EQ(result[0].group_tags.at("g"), "A");
    EXPECT_EQ(result[1].group_tags.at("g"), "C");
    EXPECT_EQ(result[2].group_tags.at("g"), "D");
}

TEST(TopkFilter, OriginalOrderPreservedBottomk) {
    // Groups in order: A(mean=3), B(mean=1), C(mean=5), D(mean=2)
    // bottomk(2, ...) keeps B (mean=1) and D (mean=2)
    // Selected in original order: B(idx 1), D(idx 3)
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("g", "A", kTs4, {3.0, 3.0, 3.0, 3.0})); // mean=3
    groups.push_back(makeGroup("g", "B", kTs4, {1.0, 1.0, 1.0, 1.0})); // mean=1 <- kept
    groups.push_back(makeGroup("g", "C", kTs4, {5.0, 5.0, 5.0, 5.0})); // mean=5
    groups.push_back(makeGroup("g", "D", kTs4, {2.0, 2.0, 2.0, 2.0})); // mean=2 <- kept

    auto result = bottomk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].group_tags.at("g"), "B");
    EXPECT_EQ(result[1].group_tags.at("g"), "D");
}

TEST(TopkFilter, MultiTagGroups) {
    // Groups identified by multiple tags
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroupTags({{"host", "a"}, {"dc", "east"}},
        kTs4, {10.0, 10.0, 10.0, 10.0})); // mean=10
    groups.push_back(makeGroupTags({{"host", "b"}, {"dc", "west"}},
        kTs4, {30.0, 30.0, 30.0, 30.0})); // mean=30
    groups.push_back(makeGroupTags({{"host", "c"}, {"dc", "east"}},
        kTs4, {20.0, 20.0, 20.0, 20.0})); // mean=20

    // topk(2, ...) should keep host=b (mean=30) and host=c (mean=20)
    auto result = topk(2, std::move(groups));

    ASSERT_EQ(result.size(), 2u);
    EXPECT_EQ(result[0].group_tags.at("host"), "b");
    EXPECT_EQ(result[1].group_tags.at("host"), "c");
}

TEST(TopkFilter, SeriesDataIntactAfterFilter) {
    // Verify that the actual AlignedSeries timestamps and values survive the filter
    std::vector<GroupedSeries> groups;
    std::vector<uint64_t> ts1 = {100ULL, 200ULL, 300ULL};
    std::vector<double>   v1  = {1.0, 2.0, 3.0};
    std::vector<uint64_t> ts2 = {100ULL, 200ULL, 300ULL};
    std::vector<double>   v2  = {10.0, 20.0, 30.0};

    groups.push_back(makeGroup("role", "low",  ts1, v1));  // mean=2
    groups.push_back(makeGroup("role", "high", ts2, v2));  // mean=20

    auto result = topk(1, std::move(groups));

    ASSERT_EQ(result.size(), 1u);
    EXPECT_EQ(result[0].group_tags.at("role"), "high");
    ASSERT_EQ(result[0].series.timestamps->size(), 3u);
    EXPECT_EQ((*result[0].series.timestamps)[0], 100ULL);
    EXPECT_EQ((*result[0].series.timestamps)[2], 300ULL);
    EXPECT_DOUBLE_EQ(result[0].series.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result[0].series.values[2], 30.0);
}

// ==================== Parser Round-Trip Tests ====================

TEST(TopkFilterParser, TopkParsesSyntaxCorrectly) {
    // topk(2, a) should parse without error
    ExpressionParser parser("topk(2, a)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::TOPK);
    EXPECT_EQ(ast->asFunctionCall().args.size(), 2u);
}

TEST(TopkFilterParser, BottomkParsesSyntaxCorrectly) {
    // bottomk(3, b) should parse without error
    ExpressionParser parser("bottomk(3, b)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::BOTTOMK);
    EXPECT_EQ(ast->asFunctionCall().args.size(), 2u);
}

TEST(TopkFilterParser, TopkToStringRoundTrip) {
    ExpressionParser parser("topk(2, a)");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "topk(2, a)");
}

TEST(TopkFilterParser, BottomkToStringRoundTrip) {
    ExpressionParser parser("bottomk(3, b)");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "bottomk(3, b)");
}

TEST(TopkFilterParser, TopkGetQueryReferences) {
    // The parser should record the query reference inside topk()
    ExpressionParser parser("topk(5, myquery)");
    auto ast = parser.parse();
    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.count("myquery"), 1u);
}

TEST(TopkFilterParser, BottomkGetQueryReferences) {
    ExpressionParser parser("bottomk(1, series_a)");
    auto ast = parser.parse();
    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.count("series_a"), 1u);
}

TEST(TopkFilterParser, TopkWrongArgCountThrows) {
    // topk with 1 arg should throw
    EXPECT_THROW(
        { ExpressionParser p("topk(2)"); p.parse(); },
        ExpressionParseException);
}

TEST(TopkFilterParser, BottomkWrongArgCountThrows) {
    // bottomk with 3 args should throw
    EXPECT_THROW(
        { ExpressionParser p("bottomk(1, a, b)"); p.parse(); },
        ExpressionParseException);
}

// ==================== GroupedSeries Cross-Series Aggregation Tests ====================

// Shared helper: common 4-point timestamps (same as kTs4 but explicit here for clarity)
static const std::vector<uint64_t> kCsTs = {10ULL, 20ULL, 30ULL, 40ULL};

// ------ avg_of_series ------

TEST(CrossSeries, AvgOfSeriesThreeGroups) {
    // Three groups with values {1,2,3,4}, {3,4,5,6}, {5,6,7,8}
    // Means per timestamp: T=10 → (1+3+5)/3=3, T=20 → (2+4+6)/3=4
    //                      T=30 → (3+5+7)/3=5, T=40 → (4+6+8)/3=6
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kCsTs, {1.0, 2.0, 3.0, 4.0}));
    groups.push_back(makeGroup("host", "b", kCsTs, {3.0, 4.0, 5.0, 6.0}));
    groups.push_back(makeGroup("host", "c", kCsTs, {5.0, 6.0, 7.0, 8.0}));

    auto result = avg_of_series(std::move(groups));

    ASSERT_EQ(result.group_tags.size(), 0u);  // No group tags on result
    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 3.0);
    EXPECT_DOUBLE_EQ(result.series.values[1], 4.0);
    EXPECT_DOUBLE_EQ(result.series.values[2], 5.0);
    EXPECT_DOUBLE_EQ(result.series.values[3], 6.0);
}

TEST(CrossSeries, AvgOfSeriesWithNaN) {
    // Group A: {NaN, 2, NaN, 4}, Group B: {6, NaN, 8, NaN}
    // At T=10: only B contributes → avg = 6
    // At T=20: only A contributes → avg = 2
    // At T=30: only B contributes → avg = 8
    // At T=40: only A contributes → avg = 4
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("x", "a", kCsTs, {nan, 2.0, nan, 4.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {6.0, nan, 8.0, nan}));

    auto result = avg_of_series(std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 6.0);
    EXPECT_DOUBLE_EQ(result.series.values[1], 2.0);
    EXPECT_DOUBLE_EQ(result.series.values[2], 8.0);
    EXPECT_DOUBLE_EQ(result.series.values[3], 4.0);
}

TEST(CrossSeries, AvgOfSeriesAllNaNAtTimestamp) {
    // If all series are NaN at a timestamp the result is NaN
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("x", "a", kCsTs, {nan, 2.0, nan, 4.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {nan, 3.0, nan, 5.0}));

    auto result = avg_of_series(std::move(groups));

    EXPECT_TRUE(std::isnan(result.series.values[0]));
    EXPECT_DOUBLE_EQ(result.series.values[1], 2.5);
    EXPECT_TRUE(std::isnan(result.series.values[2]));
    EXPECT_DOUBLE_EQ(result.series.values[3], 4.5);
}

TEST(CrossSeries, AvgOfSeriesSingleGroup) {
    // Single group → passthrough of its values
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "only", kCsTs, {7.0, 8.0, 9.0, 10.0}));

    auto result = avg_of_series(std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 7.0);
    EXPECT_DOUBLE_EQ(result.series.values[3], 10.0);
}

TEST(CrossSeries, AvgOfSeriesEmptyInput) {
    std::vector<GroupedSeries> groups;
    auto result = avg_of_series(std::move(groups));
    EXPECT_TRUE(result.series.empty());
    EXPECT_EQ(result.group_tags.size(), 0u);
}

// ------ sum_of_series ------

TEST(CrossSeries, SumOfSeriesThreeGroups) {
    // Values {1,2,3,4}, {10,20,30,40}, {100,200,300,400}
    // Sums: 111, 222, 333, 444
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kCsTs, {1.0,   2.0,   3.0,   4.0}));
    groups.push_back(makeGroup("host", "b", kCsTs, {10.0,  20.0,  30.0,  40.0}));
    groups.push_back(makeGroup("host", "c", kCsTs, {100.0, 200.0, 300.0, 400.0}));

    auto result = sum_of_series(std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 111.0);
    EXPECT_DOUBLE_EQ(result.series.values[1], 222.0);
    EXPECT_DOUBLE_EQ(result.series.values[2], 333.0);
    EXPECT_DOUBLE_EQ(result.series.values[3], 444.0);
}

TEST(CrossSeries, SumOfSeriesNaNIgnored) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    // At T=10: 5 + nan = 5 (only non-NaN contributes)
    // At T=20: 3 + 7    = 10
    groups.push_back(makeGroup("x", "a", kCsTs, {5.0, 3.0, nan, 9.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {nan, 7.0, nan, 1.0}));

    auto result = sum_of_series(std::move(groups));

    EXPECT_DOUBLE_EQ(result.series.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.series.values[1], 10.0);
    EXPECT_TRUE(std::isnan(result.series.values[2]));  // Both NaN → NaN
    EXPECT_DOUBLE_EQ(result.series.values[3], 10.0);
}

// ------ min_of_series ------

TEST(CrossSeries, MinOfSeriesThreeGroups) {
    // At each timestamp the minimum across 3 groups
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kCsTs, {10.0, 5.0,  2.0, 8.0}));
    groups.push_back(makeGroup("host", "b", kCsTs, {3.0,  9.0,  7.0, 1.0}));
    groups.push_back(makeGroup("host", "c", kCsTs, {6.0,  4.0, 11.0, 3.0}));

    auto result = min_of_series(std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 3.0);   // min(10,3,6)
    EXPECT_DOUBLE_EQ(result.series.values[1], 4.0);   // min(5,9,4)
    EXPECT_DOUBLE_EQ(result.series.values[2], 2.0);   // min(2,7,11)
    EXPECT_DOUBLE_EQ(result.series.values[3], 1.0);   // min(8,1,3)
}

TEST(CrossSeries, MinOfSeriesNaNIgnored) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("x", "a", kCsTs, {nan, 3.0, nan, 9.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {5.0, nan, nan, 2.0}));

    auto result = min_of_series(std::move(groups));

    EXPECT_DOUBLE_EQ(result.series.values[0], 5.0);   // only b
    EXPECT_DOUBLE_EQ(result.series.values[1], 3.0);   // only a
    EXPECT_TRUE(std::isnan(result.series.values[2])); // both NaN
    EXPECT_DOUBLE_EQ(result.series.values[3], 2.0);   // min(9,2)
}

// ------ max_of_series ------

TEST(CrossSeries, MaxOfSeriesThreeGroups) {
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kCsTs, {10.0, 5.0,  2.0, 8.0}));
    groups.push_back(makeGroup("host", "b", kCsTs, {3.0,  9.0,  7.0, 1.0}));
    groups.push_back(makeGroup("host", "c", kCsTs, {6.0,  4.0, 11.0, 3.0}));

    auto result = max_of_series(std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 10.0);  // max(10,3,6)
    EXPECT_DOUBLE_EQ(result.series.values[1], 9.0);   // max(5,9,4)
    EXPECT_DOUBLE_EQ(result.series.values[2], 11.0);  // max(2,7,11)
    EXPECT_DOUBLE_EQ(result.series.values[3], 8.0);   // max(8,1,3)
}

TEST(CrossSeries, MaxOfSeriesNaNIgnored) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("x", "a", kCsTs, {nan, 3.0, nan, 9.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {5.0, nan, nan, 2.0}));

    auto result = max_of_series(std::move(groups));

    EXPECT_DOUBLE_EQ(result.series.values[0], 5.0);   // only b
    EXPECT_DOUBLE_EQ(result.series.values[1], 3.0);   // only a
    EXPECT_TRUE(std::isnan(result.series.values[2])); // both NaN
    EXPECT_DOUBLE_EQ(result.series.values[3], 9.0);   // max(9,2)
}

// ------ percentile_of_series ------

TEST(CrossSeries, PercentileP50IsMedian) {
    // Three groups: {1,2,3,4}, {3,4,5,6}, {5,6,7,8}
    // Sorted at T=10: [1,3,5] → median=3
    // Sorted at T=20: [2,4,6] → median=4
    // Sorted at T=30: [3,5,7] → median=5
    // Sorted at T=40: [4,6,8] → median=6
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("host", "a", kCsTs, {1.0, 2.0, 3.0, 4.0}));
    groups.push_back(makeGroup("host", "b", kCsTs, {3.0, 4.0, 5.0, 6.0}));
    groups.push_back(makeGroup("host", "c", kCsTs, {5.0, 6.0, 7.0, 8.0}));

    auto result = percentile_of_series(50.0, std::move(groups));

    ASSERT_EQ(result.series.size(), 4u);
    EXPECT_DOUBLE_EQ(result.series.values[0], 3.0);
    EXPECT_DOUBLE_EQ(result.series.values[1], 4.0);
    EXPECT_DOUBLE_EQ(result.series.values[2], 5.0);
    EXPECT_DOUBLE_EQ(result.series.values[3], 6.0);
}

TEST(CrossSeries, PercentileP0SameAsMin) {
    // p=0 must equal min_of_series
    std::vector<GroupedSeries> gMin, gP0;
    for (auto* gp : {&gMin, &gP0}) {
        gp->push_back(makeGroup("h", "a", kCsTs, {1.0, 4.0, 9.0, 16.0}));
        gp->push_back(makeGroup("h", "b", kCsTs, {3.0, 2.0, 5.0,  8.0}));
        gp->push_back(makeGroup("h", "c", kCsTs, {2.0, 7.0, 3.0, 12.0}));
    }
    auto minResult = min_of_series(std::move(gMin));
    auto p0Result  = percentile_of_series(0.0, std::move(gP0));

    ASSERT_EQ(minResult.series.size(), p0Result.series.size());
    for (size_t i = 0; i < minResult.series.size(); ++i) {
        EXPECT_DOUBLE_EQ(p0Result.series.values[i], minResult.series.values[i]);
    }
}

TEST(CrossSeries, PercentileP100SameAsMax) {
    // p=100 must equal max_of_series
    std::vector<GroupedSeries> gMax, gP100;
    for (auto* gp : {&gMax, &gP100}) {
        gp->push_back(makeGroup("h", "a", kCsTs, {1.0, 4.0, 9.0, 16.0}));
        gp->push_back(makeGroup("h", "b", kCsTs, {3.0, 2.0, 5.0,  8.0}));
        gp->push_back(makeGroup("h", "c", kCsTs, {2.0, 7.0, 3.0, 12.0}));
    }
    auto maxResult  = max_of_series(std::move(gMax));
    auto p100Result = percentile_of_series(100.0, std::move(gP100));

    ASSERT_EQ(maxResult.series.size(), p100Result.series.size());
    for (size_t i = 0; i < maxResult.series.size(); ++i) {
        EXPECT_DOUBLE_EQ(p100Result.series.values[i], maxResult.series.values[i]);
    }
}

TEST(CrossSeries, PercentileInterpolation) {
    // Two groups: [1.0, 5.0] at T=10. p=25 → 0.25*(5-1)+1 = 2.0
    std::vector<uint64_t> ts = {10ULL};
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("h", "a", ts, {1.0}));
    groups.push_back(makeGroup("h", "b", ts, {5.0}));

    auto result = percentile_of_series(25.0, std::move(groups));

    ASSERT_EQ(result.series.size(), 1u);
    // idx = 0.25 * 1 = 0.25; lo=0, hi=1; frac=0.25; 1*(1-0.25)+5*0.25 = 2.0
    EXPECT_DOUBLE_EQ(result.series.values[0], 2.0);
}

TEST(CrossSeries, PercentileWithNaN) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    std::vector<GroupedSeries> groups;
    // At T=10: only {5,7} → median=6
    // At T=20: only {3,nan} → 3 (single value)
    groups.push_back(makeGroup("x", "a", kCsTs, {5.0, 3.0, nan, 4.0}));
    groups.push_back(makeGroup("x", "b", kCsTs, {7.0, nan, nan, 8.0}));

    auto result = percentile_of_series(50.0, std::move(groups));

    EXPECT_DOUBLE_EQ(result.series.values[0], 6.0);   // median of [5,7]
    EXPECT_DOUBLE_EQ(result.series.values[1], 3.0);   // single value
    EXPECT_TRUE(std::isnan(result.series.values[2])); // all NaN
    EXPECT_DOUBLE_EQ(result.series.values[3], 6.0);   // median of [4,8]
}

TEST(CrossSeries, PercentileInvalidPThrows) {
    std::vector<GroupedSeries> groups;
    groups.push_back(makeGroup("h", "a", kCsTs, {1.0, 2.0, 3.0, 4.0}));
    EXPECT_THROW(percentile_of_series(-1.0, groups), EvaluationException);
    EXPECT_THROW(percentile_of_series(101.0, groups), EvaluationException);
}

// ------ Different timestamps across groups ------

TEST(CrossSeries, MismatchedTimestamps) {
    // Group A has T={10,20,30}, Group B has T={20,30,40}
    // Union timestamps = {10,20,30,40}
    // T=10: only A=1 → avg=1, sum=1, min=1, max=1
    // T=20: A=2, B=10 → avg=6, sum=12, min=2, max=10
    // T=30: A=3, B=20 → avg=11.5, sum=23, min=3, max=20
    // T=40: only B=30 → avg=30, sum=30, min=30, max=30
    std::vector<GroupedSeries> gAvg, gSum, gMin, gMax;
    for (auto* gp : {&gAvg, &gSum, &gMin, &gMax}) {
        GroupedSeries ga, gb;
        ga.series = makeSeries({10ULL, 20ULL, 30ULL}, {1.0, 2.0, 3.0});
        ga.group_tags["h"] = "a";
        gb.series = makeSeries({20ULL, 30ULL, 40ULL}, {10.0, 20.0, 30.0});
        gb.group_tags["h"] = "b";
        gp->push_back(std::move(ga));
        gp->push_back(std::move(gb));
    }

    auto avgResult = avg_of_series(std::move(gAvg));
    auto sumResult = sum_of_series(std::move(gSum));
    auto minResult = min_of_series(std::move(gMin));
    auto maxResult = max_of_series(std::move(gMax));

    ASSERT_EQ(avgResult.series.size(), 4u);
    ASSERT_EQ((*avgResult.series.timestamps)[0], 10ULL);
    ASSERT_EQ((*avgResult.series.timestamps)[3], 40ULL);

    // avg
    EXPECT_DOUBLE_EQ(avgResult.series.values[0], 1.0);
    EXPECT_DOUBLE_EQ(avgResult.series.values[1], 6.0);
    EXPECT_DOUBLE_EQ(avgResult.series.values[2], 11.5);
    EXPECT_DOUBLE_EQ(avgResult.series.values[3], 30.0);

    // sum
    EXPECT_DOUBLE_EQ(sumResult.series.values[0], 1.0);
    EXPECT_DOUBLE_EQ(sumResult.series.values[1], 12.0);
    EXPECT_DOUBLE_EQ(sumResult.series.values[2], 23.0);
    EXPECT_DOUBLE_EQ(sumResult.series.values[3], 30.0);

    // min
    EXPECT_DOUBLE_EQ(minResult.series.values[0], 1.0);
    EXPECT_DOUBLE_EQ(minResult.series.values[1], 2.0);
    EXPECT_DOUBLE_EQ(minResult.series.values[2], 3.0);
    EXPECT_DOUBLE_EQ(minResult.series.values[3], 30.0);

    // max
    EXPECT_DOUBLE_EQ(maxResult.series.values[0], 1.0);
    EXPECT_DOUBLE_EQ(maxResult.series.values[1], 10.0);
    EXPECT_DOUBLE_EQ(maxResult.series.values[2], 20.0);
    EXPECT_DOUBLE_EQ(maxResult.series.values[3], 30.0);
}

// ==================== Expression Evaluator Cross-Series Tests ====================

// Helper: create a 4-point aligned series with kCsTs timestamps.
static AlignedSeries makeCsSeries(const std::vector<double>& vals) {
    return AlignedSeries(kCsTs, vals);
}

TEST(CrossSeriesEval, AvgOfSeriesTwoSeries) {
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({1.0, 3.0, 5.0, 7.0});
    qr["b"] = makeCsSeries({3.0, 5.0, 7.0, 9.0});

    ExpressionParser parser("avg_of_series(a, b)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_DOUBLE_EQ(result.values[0], 2.0);   // (1+3)/2
    EXPECT_DOUBLE_EQ(result.values[1], 4.0);   // (3+5)/2
    EXPECT_DOUBLE_EQ(result.values[2], 6.0);   // (5+7)/2
    EXPECT_DOUBLE_EQ(result.values[3], 8.0);   // (7+9)/2
}

TEST(CrossSeriesEval, SumOfSeriesTwoSeries) {
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({1.0, 2.0, 3.0, 4.0});
    qr["b"] = makeCsSeries({10.0, 20.0, 30.0, 40.0});

    ExpressionParser parser("sum_of_series(a, b)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_DOUBLE_EQ(result.values[0], 11.0);
    EXPECT_DOUBLE_EQ(result.values[1], 22.0);
    EXPECT_DOUBLE_EQ(result.values[2], 33.0);
    EXPECT_DOUBLE_EQ(result.values[3], 44.0);
}

TEST(CrossSeriesEval, MinOfSeriesTwoSeries) {
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({5.0, 1.0, 9.0, 3.0});
    qr["b"] = makeCsSeries({2.0, 8.0, 4.0, 7.0});

    ExpressionParser parser("min_of_series(a, b)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_DOUBLE_EQ(result.values[0], 2.0);
    EXPECT_DOUBLE_EQ(result.values[1], 1.0);
    EXPECT_DOUBLE_EQ(result.values[2], 4.0);
    EXPECT_DOUBLE_EQ(result.values[3], 3.0);
}

TEST(CrossSeriesEval, MaxOfSeriesTwoSeries) {
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({5.0, 1.0, 9.0, 3.0});
    qr["b"] = makeCsSeries({2.0, 8.0, 4.0, 7.0});

    ExpressionParser parser("max_of_series(a, b)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    EXPECT_DOUBLE_EQ(result.values[1], 8.0);
    EXPECT_DOUBLE_EQ(result.values[2], 9.0);
    EXPECT_DOUBLE_EQ(result.values[3], 7.0);
}

TEST(CrossSeriesEval, PercentileOfSeriesP50ThreeSeries) {
    // percentile_of_series(50, a, b, c) → median at each point
    // a={1,2,3,4}, b={3,4,5,6}, c={5,6,7,8}
    // Sorted at each point: [1,3,5],[2,4,6],[3,5,7],[4,6,8] → medians: 3,4,5,6
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({1.0, 2.0, 3.0, 4.0});
    qr["b"] = makeCsSeries({3.0, 4.0, 5.0, 6.0});
    qr["c"] = makeCsSeries({5.0, 6.0, 7.0, 8.0});

    ExpressionParser parser("percentile_of_series(50, a, b, c)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    EXPECT_DOUBLE_EQ(result.values[0], 3.0);
    EXPECT_DOUBLE_EQ(result.values[1], 4.0);
    EXPECT_DOUBLE_EQ(result.values[2], 5.0);
    EXPECT_DOUBLE_EQ(result.values[3], 6.0);
}

TEST(CrossSeriesEval, PercentileOfSeriesNaNIgnored) {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    ExpressionEvaluator evaluator;
    ExpressionEvaluator::QueryResultMap qr;
    qr["a"] = makeCsSeries({nan, 3.0, nan, 9.0});
    qr["b"] = makeCsSeries({5.0, nan, nan, 1.0});

    ExpressionParser parser("percentile_of_series(50, a, b)");
    auto ast = parser.parse();
    auto result = evaluator.evaluate(*ast, qr);

    ASSERT_EQ(result.size(), 4u);
    // T=10: only b=5 → 5
    EXPECT_DOUBLE_EQ(result.values[0], 5.0);
    // T=20: only a=3 → 3
    EXPECT_DOUBLE_EQ(result.values[1], 3.0);
    // T=30: both NaN → NaN
    EXPECT_TRUE(std::isnan(result.values[2]));
    // T=40: [1,9] sorted, p=50 → (1+9)/2=5
    EXPECT_DOUBLE_EQ(result.values[3], 5.0);
}

// ------ Parser round-trip tests ------

TEST(CrossSeriesParser, AvgOfSeriesParsesSyntax) {
    ExpressionParser parser("avg_of_series(a, b, c)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->type, ExprNodeType::FUNCTION_CALL);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::AVG_OF_SERIES);
    EXPECT_EQ(ast->asFunctionCall().args.size(), 3u);
}

TEST(CrossSeriesParser, SumOfSeriesParsesSyntax) {
    ExpressionParser parser("sum_of_series(x, y)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::SUM_OF_SERIES);
    EXPECT_EQ(ast->asFunctionCall().args.size(), 2u);
}

TEST(CrossSeriesParser, MinOfSeriesParsesSyntax) {
    ExpressionParser parser("min_of_series(x, y)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::MIN_OF_SERIES);
}

TEST(CrossSeriesParser, MaxOfSeriesParsesSyntax) {
    ExpressionParser parser("max_of_series(x, y)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::MAX_OF_SERIES);
}

TEST(CrossSeriesParser, PercentileOfSeriesParsesSyntax) {
    ExpressionParser parser("percentile_of_series(95, a, b)");
    auto ast = parser.parse();
    ASSERT_NE(ast, nullptr);
    EXPECT_EQ(ast->asFunctionCall().func, FunctionType::PERCENTILE_OF_SERIES);
    EXPECT_EQ(ast->asFunctionCall().args.size(), 3u);
}

TEST(CrossSeriesParser, AvgOfSeriesToStringRoundTrip) {
    ExpressionParser parser("avg_of_series(a, b)");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "avg_of_series(a, b)");
}

TEST(CrossSeriesParser, PercentileOfSeriesToStringRoundTrip) {
    ExpressionParser parser("percentile_of_series(50, a, b)");
    auto ast = parser.parse();
    EXPECT_EQ(ast->toString(), "percentile_of_series(50, a, b)");
}

TEST(CrossSeriesParser, PercentileOfSeriesOneArgThrows) {
    // Only the p argument, no series → should throw (needs >= 2 args)
    EXPECT_THROW(
        { ExpressionParser p("percentile_of_series(50)"); p.parse(); },
        ExpressionParseException);
}

TEST(CrossSeriesParser, AvgOfSeriesQueryRefs) {
    ExpressionParser parser("avg_of_series(cpu, mem, disk)");
    auto ast = parser.parse();
    auto refs = parser.getQueryReferences();
    EXPECT_EQ(refs.count("cpu"), 1u);
    EXPECT_EQ(refs.count("mem"), 1u);
    EXPECT_EQ(refs.count("disk"), 1u);
}
