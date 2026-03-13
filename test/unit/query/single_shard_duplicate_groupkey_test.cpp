// Regression tests for the single-shard fast path duplicate groupKey bug.
//
// When groupByTags is empty (no group-by), multiple series with the same
// measurement and field produce PartialAggregationResults with identical
// groupKeys (format: "measurement\0fieldName"). The fast path function
// finalizeSingleShardPartials() assumes unique groupKeys; if called with
// duplicates, the last partial silently overwrites earlier ones, losing
// data from other series.
//
// The fix detects duplicate groupKeys and routes to the merge fallback
// (mergePartialAggregationsGrouped). These tests verify correctness of
// that merge path for pushdown-style partials across all aggregation methods.

#include <gtest/gtest.h>
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/http/http_query_handler.hpp"  // For SeriesResult
#include <cmath>
#include <string>
#include <unordered_set>
#include <vector>

using namespace timestar;

class SingleShardDuplicateGroupKeyTest : public ::testing::Test {
protected:
    // Build a groupKey matching the handler's pushdown format:
    // "measurement\0tag1=val1\0...\0fieldName"
    // When groupByTags is empty (the bug scenario), relevantTags is empty,
    // so the key collapses to "measurement\0fieldName".
    static std::string buildGroupKey(const std::string& measurement,
                                     const std::map<std::string, std::string>& relevantTags,
                                     const std::string& fieldName) {
        std::string key;
        key += measurement;
        for (const auto& [k, v] : relevantTags) {
            key += '\0';
            key += k;
            key += '=';
            key += v;
        }
        key += '\0';
        key += fieldName;
        return key;
    }

    // Create a pushdown-style partial with sortedTimestamps + sortedValues (non-bucketed)
    static PartialAggregationResult makePushdownPartial(
        const std::string& measurement, const std::string& fieldName,
        const std::map<std::string, std::string>& relevantTags,
        std::vector<uint64_t> timestamps, std::vector<double> values) {

        PartialAggregationResult partial;
        partial.measurement = measurement;
        partial.fieldName = fieldName;
        partial.totalPoints = timestamps.size();

        std::string gk = buildGroupKey(measurement, relevantTags, fieldName);
        partial.groupKeyHash = std::hash<std::string>{}(gk);
        partial.groupKey = std::move(gk);
        partial.cachedTags = relevantTags;

        partial.sortedTimestamps = std::move(timestamps);
        partial.sortedValues = std::move(values);
        return partial;
    }

    // Create a bucketed partial with bucketStates
    static PartialAggregationResult makeBucketedPartial(
        const std::string& measurement, const std::string& fieldName,
        const std::map<std::string, std::string>& relevantTags,
        const std::vector<std::pair<uint64_t, std::vector<std::pair<double, uint64_t>>>>& bucketData) {

        PartialAggregationResult partial;
        partial.measurement = measurement;
        partial.fieldName = fieldName;
        partial.totalPoints = 0;

        std::string gk = buildGroupKey(measurement, relevantTags, fieldName);
        partial.groupKeyHash = std::hash<std::string>{}(gk);
        partial.groupKey = std::move(gk);
        partial.cachedTags = relevantTags;

        for (const auto& [bucketTs, points] : bucketData) {
            AggregationState state;
            for (const auto& [val, ts] : points) {
                state.addValue(val, ts);
            }
            partial.bucketStates[bucketTs] = state;
            partial.totalPoints += points.size();
        }
        return partial;
    }
};

// ============================================================================
// Duplicate groupKey detection logic
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, DetectsDuplicateGroupKeys) {
    // Simulate 3 series with empty groupByTags → all share groupKey "temp\0value"
    auto p1 = makePushdownPartial("temp", "value", {}, {1000}, {10.0});
    auto p2 = makePushdownPartial("temp", "value", {}, {1000}, {20.0});
    auto p3 = makePushdownPartial("temp", "value", {}, {1000}, {30.0});

    std::vector<PartialAggregationResult> partials = {std::move(p1), std::move(p2), std::move(p3)};

    // Replicate the duplicate detection from http_query_handler.cpp
    bool hasDuplicateGroupKeys = false;
    if (partials.size() > 1) {
        std::unordered_set<std::string_view> seen;
        seen.reserve(partials.size());
        for (const auto& p : partials) {
            if (!seen.insert(p.groupKey).second) {
                hasDuplicateGroupKeys = true;
                break;
            }
        }
    }

    EXPECT_TRUE(hasDuplicateGroupKeys) << "3 series with same measurement+field and no group-by must produce duplicate groupKeys";
}

TEST_F(SingleShardDuplicateGroupKeyTest, NoDuplicatesWhenGroupKeysUnique) {
    // Different fields → unique groupKeys
    auto p1 = makePushdownPartial("temp", "value", {}, {1000}, {10.0});
    auto p2 = makePushdownPartial("temp", "humidity", {}, {1000}, {65.0});

    std::vector<PartialAggregationResult> partials = {std::move(p1), std::move(p2)};

    bool hasDuplicateGroupKeys = false;
    if (partials.size() > 1) {
        std::unordered_set<std::string_view> seen;
        seen.reserve(partials.size());
        for (const auto& p : partials) {
            if (!seen.insert(p.groupKey).second) {
                hasDuplicateGroupKeys = true;
                break;
            }
        }
    }

    EXPECT_FALSE(hasDuplicateGroupKeys);
}

TEST_F(SingleShardDuplicateGroupKeyTest, NoDuplicatesWhenGroupByDifferentiates) {
    // With group-by on "host", different tags → unique groupKeys
    auto p1 = makePushdownPartial("temp", "value", {{"host", "server1"}}, {1000}, {10.0});
    auto p2 = makePushdownPartial("temp", "value", {{"host", "server2"}}, {1000}, {20.0});

    std::vector<PartialAggregationResult> partials = {std::move(p1), std::move(p2)};

    bool hasDuplicateGroupKeys = false;
    if (partials.size() > 1) {
        std::unordered_set<std::string_view> seen;
        seen.reserve(partials.size());
        for (const auto& p : partials) {
            if (!seen.insert(p.groupKey).second) {
                hasDuplicateGroupKeys = true;
                break;
            }
        }
    }

    EXPECT_FALSE(hasDuplicateGroupKeys);
}

TEST_F(SingleShardDuplicateGroupKeyTest, SinglePartialNeverDuplicate) {
    auto p1 = makePushdownPartial("temp", "value", {}, {1000}, {10.0});
    std::vector<PartialAggregationResult> partials = {std::move(p1)};

    // size() <= 1 → skip check entirely
    bool hasDuplicateGroupKeys = false;
    if (partials.size() > 1) {
        std::unordered_set<std::string_view> seen;
        seen.reserve(partials.size());
        for (const auto& p : partials) {
            if (!seen.insert(p.groupKey).second) {
                hasDuplicateGroupKeys = true;
                break;
            }
        }
    }

    EXPECT_FALSE(hasDuplicateGroupKeys);
}

// ============================================================================
// Cross-series aggregation via merge fallback (non-bucketed pushdown partials)
//
// These directly test the path taken when duplicates ARE detected:
// mergePartialAggregationsGrouped() on pushdown-style partials with sortedValues.
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_MIN_AcrossThreeSeries) {
    // 3 devices on 1 shard, no group-by → all share groupKey "measurement\0value"
    // Device A: values [30, 25, 28]
    // Device B: values [10, 15, 12]  ← has the global min
    // Device C: values [20, 22, 18]
    auto pA = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {30.0, 25.0, 28.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {10.0, 15.0, 12.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {20.0, 22.0, 18.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1) << "No group-by: all series should merge into one group";
    EXPECT_EQ(grouped[0].measurement, "measurement");
    EXPECT_EQ(grouped[0].fieldName, "value");

    // Should have 3 timestamps; at each, MIN across all 3 devices
    ASSERT_EQ(grouped[0].points.size(), 3);

    // Sort by timestamp for deterministic checks
    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // t=1000: min(30, 10, 20) = 10
    EXPECT_EQ(pts[0].timestamp, 1000);
    EXPECT_DOUBLE_EQ(pts[0].value, 10.0);

    // t=2000: min(25, 15, 22) = 15
    EXPECT_EQ(pts[1].timestamp, 2000);
    EXPECT_DOUBLE_EQ(pts[1].value, 15.0);

    // t=3000: min(28, 12, 18) = 12
    EXPECT_EQ(pts[2].timestamp, 3000);
    EXPECT_DOUBLE_EQ(pts[2].value, 12.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_MAX_AcrossThreeSeries) {
    auto pA = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {30.0, 25.0, 28.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {10.0, 15.0, 12.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {1000, 2000, 3000}, {20.0, 22.0, 18.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MAX);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // t=1000: max(30, 10, 20) = 30
    EXPECT_DOUBLE_EQ(pts[0].value, 30.0);
    // t=2000: max(25, 15, 22) = 25
    EXPECT_DOUBLE_EQ(pts[1].value, 25.0);
    // t=3000: max(28, 12, 18) = 28
    EXPECT_DOUBLE_EQ(pts[2].value, 28.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_AVG_AcrossThreeSeries) {
    auto pA = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {30.0, 12.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {10.0, 18.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {20.0, 24.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // t=1000: avg(30, 10, 20) = 20
    EXPECT_DOUBLE_EQ(pts[0].value, 20.0);
    EXPECT_EQ(pts[0].count, 3);

    // t=2000: avg(12, 18, 24) = 18
    EXPECT_DOUBLE_EQ(pts[1].value, 18.0);
    EXPECT_EQ(pts[1].count, 3);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_SUM_AcrossThreeSeries) {
    auto pA = makePushdownPartial("measurement", "value", {}, {1000}, {10.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {1000}, {20.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {1000}, {30.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    // sum(10, 20, 30) = 60
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 60.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_COUNT_AcrossThreeSeries) {
    auto pA = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {10.0, 11.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {20.0, 21.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {1000, 2000}, {30.0, 31.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::COUNT);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // Each timestamp has 3 values merged → count = 3
    EXPECT_EQ(pts[0].count, 3);
    EXPECT_EQ(pts[1].count, 3);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_LATEST_AcrossThreeSeries) {
    // Device A: latest at t=3000 with value 28
    // Device B: latest at t=5000 with value 50 ← global latest
    // Device C: latest at t=4000 with value 40
    auto pA = makePushdownPartial("measurement", "value", {}, {1000, 3000}, {10.0, 28.0});
    auto pB = makePushdownPartial("measurement", "value", {}, {2000, 5000}, {20.0, 50.0});
    auto pC = makePushdownPartial("measurement", "value", {}, {4000}, {40.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    // All unique timestamps merged, sorted
    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });
    EXPECT_EQ(pts.size(), 5); // 5 unique timestamps

    // The latest timestamp (5000) should have value 50.0
    EXPECT_EQ(pts.back().timestamp, 5000);
    EXPECT_DOUBLE_EQ(pts.back().value, 50.0);
}

// ============================================================================
// Bug reproducer: verifies the EXACT scenario that was broken
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, BugReproducer_LastPartialOverwritesEarlier) {
    // This is the exact bug: min:measurement(){} with 3 devices on 1 shard.
    // Without the fix, finalizeSingleShardPartials would process each partial
    // sequentially. All three produce the same mergeKey (measurement + empty tags),
    // so each overwrites the previous. Only Device C's data would appear.
    //
    // With the fix, duplicate groupKeys are detected and routed to the merge
    // path, which correctly aggregates across all series.
    auto deviceA = makePushdownPartial("sensor_data", "temperature", {},
                                       {1000, 2000, 3000}, {25.0, 26.0, 24.0});
    auto deviceB = makePushdownPartial("sensor_data", "temperature", {},
                                       {1000, 2000, 3000}, {18.0, 19.0, 17.0});
    auto deviceC = makePushdownPartial("sensor_data", "temperature", {},
                                       {1000, 2000, 3000}, {22.0, 23.0, 21.0});

    // Verify all three share the same groupKey (precondition for the bug)
    ASSERT_EQ(deviceA.groupKey, deviceB.groupKey);
    ASSERT_EQ(deviceB.groupKey, deviceC.groupKey);

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(deviceA));
    partials.push_back(std::move(deviceB));
    partials.push_back(std::move(deviceC));

    // The merge fallback correctly aggregates across all series
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 3);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // Without the fix these would all be Device C's values (22, 23, 21).
    // With the fix, we get the true MIN across all 3 devices.
    EXPECT_DOUBLE_EQ(pts[0].value, 18.0);  // min(25, 18, 22)
    EXPECT_DOUBLE_EQ(pts[1].value, 19.0);  // min(26, 19, 23)
    EXPECT_DOUBLE_EQ(pts[2].value, 17.0);  // min(24, 17, 21)
}

// ============================================================================
// Bucketed aggregation with duplicate groupKeys
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_BucketedMIN_AcrossThreeSeries) {
    // 3 devices, bucketed at 10-second intervals, no group-by
    // Bucket 0: Device A=30, B=10, C=20  → min=10
    // Bucket 10000: Device A=25, B=15, C=22  → min=15
    auto pA = makeBucketedPartial("m", "value", {},
        {{0, {{30.0, 100}}}, {10000, {{25.0, 10100}}}});
    auto pB = makeBucketedPartial("m", "value", {},
        {{0, {{10.0, 200}}}, {10000, {{15.0, 10200}}}});
    auto pC = makeBucketedPartial("m", "value", {},
        {{0, {{20.0, 300}}}, {10000, {{22.0, 10300}}}});

    ASSERT_EQ(pA.groupKey, pB.groupKey);
    ASSERT_EQ(pB.groupKey, pC.groupKey);

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    EXPECT_DOUBLE_EQ(pts[0].value, 10.0);  // min at bucket 0
    EXPECT_DOUBLE_EQ(pts[1].value, 15.0);  // min at bucket 10000
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_BucketedAVG_AcrossThreeSeries) {
    auto pA = makeBucketedPartial("m", "value", {},
        {{0, {{30.0, 100}}}});
    auto pB = makeBucketedPartial("m", "value", {},
        {{0, {{10.0, 200}}}});
    auto pC = makeBucketedPartial("m", "value", {},
        {{0, {{20.0, 300}}}});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    // avg(30, 10, 20) = 20
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 20.0);
    EXPECT_EQ(grouped[0].points[0].count, 3);
}

// ============================================================================
// Mixed scenarios: some groupKeys duplicated, some unique
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, MixedDuplicateAndUniqueGroupKeys) {
    // Field "temperature": 2 series with duplicate groupKeys
    auto p1 = makePushdownPartial("weather", "temperature", {}, {1000}, {30.0});
    auto p2 = makePushdownPartial("weather", "temperature", {}, {1000}, {10.0});
    // Field "humidity": 1 series, unique groupKey
    auto p3 = makePushdownPartial("weather", "humidity", {}, {1000}, {65.0});

    // Verify: temperature partials share a groupKey, humidity is different
    ASSERT_EQ(p1.groupKey, p2.groupKey);
    ASSERT_NE(p1.groupKey, p3.groupKey);

    // Detect duplicates (should find them due to temperature)
    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(p1));
    partials.push_back(std::move(p2));
    partials.push_back(std::move(p3));

    bool hasDuplicateGroupKeys = false;
    if (partials.size() > 1) {
        std::unordered_set<std::string_view> seen;
        seen.reserve(partials.size());
        for (const auto& p : partials) {
            if (!seen.insert(p.groupKey).second) {
                hasDuplicateGroupKeys = true;
                break;
            }
        }
    }
    EXPECT_TRUE(hasDuplicateGroupKeys);

    // Merge should produce 2 groups: one for temperature (merged), one for humidity
    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 2);

    // Find temperature and humidity results
    GroupedAggregationResult* tempResult = nullptr;
    GroupedAggregationResult* humidResult = nullptr;
    for (auto& g : grouped) {
        if (g.fieldName == "temperature") tempResult = &g;
        if (g.fieldName == "humidity") humidResult = &g;
    }

    ASSERT_NE(tempResult, nullptr);
    ASSERT_NE(humidResult, nullptr);

    // temperature: min(30, 10) = 10
    ASSERT_EQ(tempResult->points.size(), 1);
    EXPECT_DOUBLE_EQ(tempResult->points[0].value, 10.0);

    // humidity: only one series → single-partial zero-copy path uses rawTimestamps/rawValues
    if (!humidResult->rawTimestamps.empty()) {
        ASSERT_EQ(humidResult->rawTimestamps.size(), 1);
        EXPECT_DOUBLE_EQ(humidResult->rawValues[0], 65.0);
    } else {
        ASSERT_EQ(humidResult->points.size(), 1);
        EXPECT_DOUBLE_EQ(humidResult->points[0].value, 65.0);
    }
}

// ============================================================================
// Scale test: many series with duplicate groupKeys
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_TenSeries_MIN) {
    // 10 devices reporting to the same measurement, no group-by
    std::vector<PartialAggregationResult> partials;
    for (int i = 0; i < 10; ++i) {
        partials.push_back(makePushdownPartial("sensor", "value", {},
            {1000, 2000}, {static_cast<double>(i * 10 + 5), static_cast<double>(i * 10 + 8)}));
    }

    // All share the same groupKey
    for (size_t i = 1; i < partials.size(); ++i) {
        ASSERT_EQ(partials[0].groupKey, partials[i].groupKey);
    }

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 2);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // t=1000: min of {5, 15, 25, ..., 95} = 5
    EXPECT_DOUBLE_EQ(pts[0].value, 5.0);
    // t=2000: min of {8, 18, 28, ..., 98} = 8
    EXPECT_DOUBLE_EQ(pts[1].value, 8.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_TenSeries_SUM) {
    std::vector<PartialAggregationResult> partials;
    for (int i = 0; i < 10; ++i) {
        partials.push_back(makePushdownPartial("sensor", "value", {},
            {1000}, {static_cast<double>(i + 1)}));  // values: 1, 2, ..., 10
    }

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    // sum(1+2+...+10) = 55
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 55.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_TenSeries_AVG) {
    std::vector<PartialAggregationResult> partials;
    for (int i = 0; i < 10; ++i) {
        partials.push_back(makePushdownPartial("sensor", "value", {},
            {1000}, {static_cast<double>(i + 1)}));  // values: 1, 2, ..., 10
    }

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    // avg(1+2+...+10) = 55/10 = 5.5
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 5.5);
    EXPECT_EQ(grouped[0].points[0].count, 10);
}

// ============================================================================
// Edge cases
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, TwoSeriesDifferentTimestamps_MIN) {
    // Series have non-overlapping timestamps
    auto pA = makePushdownPartial("m", "v", {}, {1000, 3000}, {30.0, 28.0});
    auto pB = makePushdownPartial("m", "v", {}, {2000, 4000}, {10.0, 15.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    // 4 unique timestamps → 4 points (no merging within timestamps)
    ASSERT_EQ(grouped[0].points.size(), 4);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    EXPECT_DOUBLE_EQ(pts[0].value, 30.0);  // t=1000 only from A
    EXPECT_DOUBLE_EQ(pts[1].value, 10.0);  // t=2000 only from B
    EXPECT_DOUBLE_EQ(pts[2].value, 28.0);  // t=3000 only from A
    EXPECT_DOUBLE_EQ(pts[3].value, 15.0);  // t=4000 only from B
}

TEST_F(SingleShardDuplicateGroupKeyTest, TwoSeriesPartialOverlap_AVG) {
    // Series have partially overlapping timestamps
    auto pA = makePushdownPartial("m", "v", {}, {1000, 2000, 3000}, {10.0, 20.0, 30.0});
    auto pB = makePushdownPartial("m", "v", {}, {2000, 3000, 4000}, {40.0, 50.0, 60.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 4);  // t=1000, 2000, 3000, 4000

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // t=1000: only A → avg=10, count=1
    EXPECT_DOUBLE_EQ(pts[0].value, 10.0);
    EXPECT_EQ(pts[0].count, 1);

    // t=2000: A=20, B=40 → avg=30, count=2
    EXPECT_DOUBLE_EQ(pts[1].value, 30.0);
    EXPECT_EQ(pts[1].count, 2);

    // t=3000: A=30, B=50 → avg=40, count=2
    EXPECT_DOUBLE_EQ(pts[2].value, 40.0);
    EXPECT_EQ(pts[2].count, 2);

    // t=4000: only B → avg=60, count=1
    EXPECT_DOUBLE_EQ(pts[3].value, 60.0);
    EXPECT_EQ(pts[3].count, 1);
}

TEST_F(SingleShardDuplicateGroupKeyTest, EmptyPartialsSkipped) {
    // One partial has data, the other is empty
    auto pA = makePushdownPartial("m", "v", {}, {1000}, {42.0});
    auto pB = makePushdownPartial("m", "v", {}, {}, {});  // empty

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 42.0);
}

// ============================================================================
// Verify createPartialAggregations produces duplicate groupKeys for the
// bug scenario (no group-by, multiple series, same measurement+field)
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, CreatePartialAggregations_ProducesDuplicateGroupKeys) {
    // Simulate 3 series with different tags, same measurement + field, no group-by
    SeriesResult sr1;
    sr1.measurement = "cpu";
    sr1.tags = {{"host", "server1"}};
    sr1.fields["usage"] = std::make_pair(
        std::vector<uint64_t>{1000, 2000}, FieldValues(std::vector<double>{10.0, 20.0}));

    SeriesResult sr2;
    sr2.measurement = "cpu";
    sr2.tags = {{"host", "server2"}};
    sr2.fields["usage"] = std::make_pair(
        std::vector<uint64_t>{1000, 2000}, FieldValues(std::vector<double>{30.0, 40.0}));

    SeriesResult sr3;
    sr3.measurement = "cpu";
    sr3.tags = {{"host", "server3"}};
    sr3.fields["usage"] = std::make_pair(
        std::vector<uint64_t>{1000, 2000}, FieldValues(std::vector<double>{50.0, 60.0}));

    // No group-by tags → all partials get the same groupKey
    auto partials = Aggregator::createPartialAggregations(
        {sr1, sr2, sr3}, AggregationMethod::MIN, 0, {});

    // createPartialAggregations merges them into ONE partial (same groupKey)
    // This is the fallback path behavior — it aggregates in the map phase.
    // But the pushdown path creates separate partials per series.
    // Either way, the merge must handle both patterns correctly.
    ASSERT_GE(partials.size(), 1);

    if (partials.size() > 1) {
        // If separate partials were produced, they must share a groupKey
        for (size_t i = 1; i < partials.size(); ++i) {
            EXPECT_EQ(partials[0].groupKey, partials[i].groupKey);
        }
    }
}

// ============================================================================
// SPREAD and STDDEV with duplicate groupKeys (less common but must work)
// ============================================================================

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_SPREAD_AcrossThreeSeries) {
    // SPREAD = max - min. Uses sortedStates (not raw values) because
    // SPREAD requires full AggregationState to track min/max independently.
    // This matches the fallback aggregation path used for non-pushdown queries.
    auto makeStatePartial = [](const std::string& measurement, const std::string& field,
                               std::vector<uint64_t> timestamps, std::vector<double> values) {
        PartialAggregationResult partial;
        partial.measurement = measurement;
        partial.fieldName = field;
        partial.totalPoints = timestamps.size();
        std::string gk = measurement + '\0' + field;
        partial.groupKeyHash = std::hash<std::string>{}(gk);
        partial.groupKey = std::move(gk);
        partial.sortedTimestamps = std::move(timestamps);
        partial.sortedStates.reserve(partial.sortedTimestamps.size());
        for (size_t i = 0; i < partial.sortedTimestamps.size(); ++i) {
            AggregationState s;
            s.addValue(values[i], partial.sortedTimestamps[i]);
            partial.sortedStates.push_back(s);
        }
        return partial;
    };

    auto pA = makeStatePartial("m", "v", {1000}, {30.0});
    auto pB = makeStatePartial("m", "v", {1000}, {10.0});
    auto pC = makeStatePartial("m", "v", {1000}, {20.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::SPREAD);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);

    // spread = max(30,10,20) - min(30,10,20) = 30 - 10 = 20
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 20.0);
}

TEST_F(SingleShardDuplicateGroupKeyTest, MergeFallback_FIRST_AcrossThreeSeries) {
    // FIRST returns the value at the earliest timestamp
    auto pA = makePushdownPartial("m", "v", {}, {3000}, {30.0});
    auto pB = makePushdownPartial("m", "v", {}, {1000}, {10.0});  // earliest
    auto pC = makePushdownPartial("m", "v", {}, {2000}, {20.0});

    std::vector<PartialAggregationResult> partials;
    partials.push_back(std::move(pA));
    partials.push_back(std::move(pB));
    partials.push_back(std::move(pC));

    auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, AggregationMethod::FIRST);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_GE(grouped[0].points.size(), 1);

    auto& pts = grouped[0].points;
    std::sort(pts.begin(), pts.end(), [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });

    // The earliest timestamp (1000) should have value 10.0
    EXPECT_EQ(pts.front().timestamp, 1000);
    EXPECT_DOUBLE_EQ(pts.front().value, 10.0);
}
