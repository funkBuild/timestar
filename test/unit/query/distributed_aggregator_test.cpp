#include <gtest/gtest.h>
#include "../../../lib/query/aggregator.hpp"
#include "../../../lib/http/http_query_handler.hpp"  // For SeriesResult
#include <vector>
#include <cmath>

using namespace timestar;

// Test fixture for distributed aggregation tests
class DistributedAggregatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create sample SeriesResult data for testing
    }

    // Helper to create a SeriesResult with double values
    SeriesResult createSeriesResult(
        const std::string& measurement,
        const std::map<std::string, std::string>& tags,
        const std::string& fieldName,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values) {

        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        sr.fields[fieldName] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }
};

// ============================================================================
// AggregationState Tests
// ============================================================================

TEST_F(DistributedAggregatorTest, AggregationStateAddValue) {
    AggregationState state;

    state.addValue(10.0, 1000);
    EXPECT_DOUBLE_EQ(state.sum, 10.0);
    EXPECT_DOUBLE_EQ(state.min, 10.0);
    EXPECT_DOUBLE_EQ(state.max, 10.0);
    EXPECT_DOUBLE_EQ(state.latest, 10.0);
    EXPECT_EQ(state.latestTimestamp, 1000);
    EXPECT_EQ(state.count, 1);

    state.addValue(20.0, 2000);
    EXPECT_DOUBLE_EQ(state.sum, 30.0);
    EXPECT_DOUBLE_EQ(state.min, 10.0);
    EXPECT_DOUBLE_EQ(state.max, 20.0);
    EXPECT_DOUBLE_EQ(state.latest, 20.0);
    EXPECT_EQ(state.latestTimestamp, 2000);
    EXPECT_EQ(state.count, 2);

    state.addValue(5.0, 1500); // Earlier timestamp but lower value
    EXPECT_DOUBLE_EQ(state.sum, 35.0);
    EXPECT_DOUBLE_EQ(state.min, 5.0);
    EXPECT_DOUBLE_EQ(state.max, 20.0);
    EXPECT_DOUBLE_EQ(state.latest, 20.0); // Latest should remain 20.0 (timestamp 2000)
    EXPECT_EQ(state.latestTimestamp, 2000);
    EXPECT_EQ(state.count, 3);
}

TEST_F(DistributedAggregatorTest, AggregationStateMerge) {
    AggregationState state1;
    state1.addValue(10.0, 1000);
    state1.addValue(20.0, 2000);

    AggregationState state2;
    state2.addValue(15.0, 1500);
    state2.addValue(25.0, 2500);

    state1.merge(state2);

    EXPECT_DOUBLE_EQ(state1.sum, 70.0); // 10 + 20 + 15 + 25
    EXPECT_DOUBLE_EQ(state1.min, 10.0);
    EXPECT_DOUBLE_EQ(state1.max, 25.0);
    EXPECT_DOUBLE_EQ(state1.latest, 25.0); // Latest from state2
    EXPECT_EQ(state1.latestTimestamp, 2500);
    EXPECT_EQ(state1.count, 4);
}

TEST_F(DistributedAggregatorTest, AggregationStateGetValueAVG) {
    AggregationState state;
    state.addValue(10.0, 1000);
    state.addValue(20.0, 2000);
    state.addValue(30.0, 3000);

    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::AVG), 20.0); // (10+20+30)/3
}

TEST_F(DistributedAggregatorTest, AggregationStateGetValueMIN) {
    AggregationState state;
    state.addValue(30.0, 1000);
    state.addValue(10.0, 2000);
    state.addValue(20.0, 3000);

    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MIN), 10.0);
}

TEST_F(DistributedAggregatorTest, AggregationStateGetValueMAX) {
    AggregationState state;
    state.addValue(10.0, 1000);
    state.addValue(30.0, 2000);
    state.addValue(20.0, 3000);

    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::MAX), 30.0);
}

TEST_F(DistributedAggregatorTest, AggregationStateGetValueSUM) {
    AggregationState state;
    state.addValue(10.0, 1000);
    state.addValue(20.0, 2000);
    state.addValue(30.0, 3000);

    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::SUM), 60.0);
}

TEST_F(DistributedAggregatorTest, AggregationStateGetValueLATEST) {
    AggregationState state;
    state.addValue(10.0, 3000);
    state.addValue(20.0, 1000);
    state.addValue(30.0, 2000);

    EXPECT_DOUBLE_EQ(state.getValue(AggregationMethod::LATEST), 10.0); // Latest by timestamp (3000)
}

// ============================================================================
// createPartialAggregations Tests
// ============================================================================

TEST_F(DistributedAggregatorTest, CreatePartialAggregationsBasic) {
    std::vector<uint64_t> timestamps = {1000, 2000, 3000};
    std::vector<double> values = {10.0, 20.0, 30.0};

    SeriesResult sr = createSeriesResult("cpu", {{"host", "server1"}}, "usage", timestamps, values);
    std::vector<SeriesResult> seriesResults = {sr};

    auto partials = Aggregator::createPartialAggregations(
        seriesResults, AggregationMethod::AVG, 0, {});

    ASSERT_EQ(partials.size(), 1);
    EXPECT_EQ(partials[0].measurement, "cpu");
    EXPECT_EQ(partials[0].fieldName, "usage");
    EXPECT_EQ(partials[0].totalPoints, 3);

    // Should have sorted timestamp/state vectors (interval == 0)
    EXPECT_EQ(partials[0].sortedTimestamps.size(), 3);
    EXPECT_TRUE(partials[0].bucketStates.empty());
}

TEST_F(DistributedAggregatorTest, CreatePartialAggregationsWithInterval) {
    std::vector<uint64_t> timestamps = {
        1000000000,           // Bucket 0
        2000000000,           // Bucket 2000000000
        3000000000,           // Bucket 3000000000
        4000000000,           // Bucket 4000000000
        5000000000            // Bucket 5000000000
    };
    std::vector<double> values = {10.0, 20.0, 30.0, 40.0, 50.0};

    SeriesResult sr = createSeriesResult("cpu", {{"host", "server1"}}, "usage", timestamps, values);
    std::vector<SeriesResult> seriesResults = {sr};

    uint64_t interval = 2000000000; // 2 second buckets
    auto partials = Aggregator::createPartialAggregations(
        seriesResults, AggregationMethod::AVG, interval, {});

    ASSERT_EQ(partials.size(), 1);

    // Should have bucket states (interval > 0)
    EXPECT_EQ(partials[0].bucketStates.size(), 3); // Buckets at 0, 2000000000, 4000000000
    EXPECT_TRUE(partials[0].sortedTimestamps.empty());
}

TEST_F(DistributedAggregatorTest, CreatePartialAggregationsWithGroupBy) {
    // Create two series with different tags
    std::vector<uint64_t> timestamps = {1000, 2000, 3000};
    std::vector<double> values1 = {10.0, 20.0, 30.0};
    std::vector<double> values2 = {15.0, 25.0, 35.0};

    SeriesResult sr1 = createSeriesResult("cpu", {{"host", "server1"}, {"region", "us-west"}},
                                          "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {{"host", "server2"}, {"region", "us-west"}},
                                          "usage", timestamps, values2);

    std::vector<SeriesResult> seriesResults = {sr1, sr2};

    // Group by region (should merge server1 and server2 into one group)
    auto partials = Aggregator::createPartialAggregations(
        seriesResults, AggregationMethod::AVG, 0, {"region"});

    ASSERT_EQ(partials.size(), 1); // Only one group (us-west)
    // Tags are encoded in groupKey, not stored separately
    auto parsedTags = PartialAggregationResult::parseTagsFromGroupKey(partials[0].groupKey);
    EXPECT_EQ(parsedTags.size(), 1);
    EXPECT_EQ(parsedTags["region"], "us-west");
    EXPECT_EQ(partials[0].totalPoints, 6); // 3 points from each series
}

TEST_F(DistributedAggregatorTest, CreatePartialAggregationsMultipleFields) {
    std::vector<uint64_t> timestamps = {1000, 2000, 3000};
    std::vector<double> cpuValues = {10.0, 20.0, 30.0};
    std::vector<double> memValues = {100.0, 200.0, 300.0};

    SeriesResult sr;
    sr.measurement = "system";
    sr.tags = {{"host", "server1"}};
    sr.fields["cpu"] = std::make_pair(timestamps, FieldValues(cpuValues));
    sr.fields["memory"] = std::make_pair(timestamps, FieldValues(memValues));

    std::vector<SeriesResult> seriesResults = {sr};

    auto partials = Aggregator::createPartialAggregations(
        seriesResults, AggregationMethod::AVG, 0, {});

    // Should create separate partials for each field
    ASSERT_EQ(partials.size(), 2);

    // Find cpu and memory partials
    PartialAggregationResult* cpuPartial = nullptr;
    PartialAggregationResult* memPartial = nullptr;

    for (auto& p : partials) {
        if (p.fieldName == "cpu") cpuPartial = &p;
        if (p.fieldName == "memory") memPartial = &p;
    }

    ASSERT_NE(cpuPartial, nullptr);
    ASSERT_NE(memPartial, nullptr);

    EXPECT_EQ(cpuPartial->totalPoints, 3);
    EXPECT_EQ(memPartial->totalPoints, 3);
}

// ============================================================================
// mergePartialAggregationsGrouped Tests
// ============================================================================

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedBasic) {
    // Create two partial results from different "shards"
    std::vector<uint64_t> timestamps1 = {1000, 2000};
    std::vector<double> values1 = {10.0, 20.0};
    SeriesResult sr1 = createSeriesResult("cpu", {{"host", "server1"}}, "usage", timestamps1, values1);

    std::vector<uint64_t> timestamps2 = {3000, 4000};
    std::vector<double> values2 = {30.0, 40.0};
    SeriesResult sr2 = createSeriesResult("cpu", {{"host", "server1"}}, "usage", timestamps2, values2);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::AVG, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::AVG, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1); // One group (same measurement/tags/field)
    EXPECT_EQ(grouped[0].measurement, "cpu");
    EXPECT_EQ(grouped[0].fieldName, "usage");
    EXPECT_EQ(grouped[0].points.size(), 4); // 4 unique timestamps

    // Verify values
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 10.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[1].value, 20.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[2].value, 30.0);
    EXPECT_DOUBLE_EQ(grouped[0].points[3].value, 40.0);
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedWithBuckets) {
    std::vector<uint64_t> timestamps = {
        1000000000,  // Bucket 0
        2500000000,  // Bucket 2000000000
        4500000000   // Bucket 4000000000
    };
    std::vector<double> values1 = {10.0, 20.0, 30.0};
    std::vector<double> values2 = {15.0, 25.0, 35.0};

    SeriesResult sr1 = createSeriesResult("cpu", {{"host", "server1"}}, "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {{"host", "server2"}}, "usage", timestamps, values2);

    uint64_t interval = 2000000000; // 2 second buckets

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::AVG, interval, {"host"});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::AVG, interval, {"host"});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG);

    // Should have 2 groups (server1 and server2)
    ASSERT_EQ(grouped.size(), 2);

    // Each group should have 3 buckets
    for (const auto& g : grouped) {
        EXPECT_EQ(g.points.size(), 3);
    }
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedAVG) {
    // Test that AVG is correctly computed when merging states
    std::vector<uint64_t> timestamps = {1000};
    std::vector<double> values1 = {10.0};
    std::vector<double> values2 = {20.0};
    std::vector<double> values3 = {30.0};

    SeriesResult sr1 = createSeriesResult("cpu", {}, "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {}, "usage", timestamps, values2);
    SeriesResult sr3 = createSeriesResult("cpu", {}, "usage", timestamps, values3);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::AVG, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::AVG, 0, {});
    auto partials3 = Aggregator::createPartialAggregations({sr3}, AggregationMethod::AVG, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());
    allPartials.insert(allPartials.end(), partials3.begin(), partials3.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 20.0); // (10+20+30)/3 = 20
    EXPECT_EQ(grouped[0].points[0].count, 3);
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedMIN) {
    std::vector<uint64_t> timestamps = {1000};
    std::vector<double> values1 = {30.0};
    std::vector<double> values2 = {10.0};
    std::vector<double> values3 = {20.0};

    SeriesResult sr1 = createSeriesResult("cpu", {}, "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {}, "usage", timestamps, values2);
    SeriesResult sr3 = createSeriesResult("cpu", {}, "usage", timestamps, values3);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::MIN, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::MIN, 0, {});
    auto partials3 = Aggregator::createPartialAggregations({sr3}, AggregationMethod::MIN, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());
    allPartials.insert(allPartials.end(), partials3.begin(), partials3.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MIN);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 10.0); // MIN of 30, 10, 20
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedMAX) {
    std::vector<uint64_t> timestamps = {1000};
    std::vector<double> values1 = {30.0};
    std::vector<double> values2 = {10.0};
    std::vector<double> values3 = {20.0};

    SeriesResult sr1 = createSeriesResult("cpu", {}, "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {}, "usage", timestamps, values2);
    SeriesResult sr3 = createSeriesResult("cpu", {}, "usage", timestamps, values3);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::MAX, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::MAX, 0, {});
    auto partials3 = Aggregator::createPartialAggregations({sr3}, AggregationMethod::MAX, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());
    allPartials.insert(allPartials.end(), partials3.begin(), partials3.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::MAX);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 30.0); // MAX of 30, 10, 20
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedSUM) {
    std::vector<uint64_t> timestamps = {1000};
    std::vector<double> values1 = {10.0};
    std::vector<double> values2 = {20.0};
    std::vector<double> values3 = {30.0};

    SeriesResult sr1 = createSeriesResult("cpu", {}, "usage", timestamps, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {}, "usage", timestamps, values2);
    SeriesResult sr3 = createSeriesResult("cpu", {}, "usage", timestamps, values3);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::SUM, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::SUM, 0, {});
    auto partials3 = Aggregator::createPartialAggregations({sr3}, AggregationMethod::SUM, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());
    allPartials.insert(allPartials.end(), partials3.begin(), partials3.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::SUM);

    ASSERT_EQ(grouped.size(), 1);
    ASSERT_EQ(grouped[0].points.size(), 1);
    EXPECT_DOUBLE_EQ(grouped[0].points[0].value, 60.0); // SUM of 10+20+30
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedLATEST) {
    std::vector<uint64_t> timestamps1 = {1000};
    std::vector<uint64_t> timestamps2 = {3000};
    std::vector<uint64_t> timestamps3 = {2000};
    std::vector<double> values1 = {10.0};
    std::vector<double> values2 = {30.0};
    std::vector<double> values3 = {20.0};

    SeriesResult sr1 = createSeriesResult("cpu", {}, "usage", timestamps1, values1);
    SeriesResult sr2 = createSeriesResult("cpu", {}, "usage", timestamps2, values2);
    SeriesResult sr3 = createSeriesResult("cpu", {}, "usage", timestamps3, values3);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::LATEST, 0, {});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::LATEST, 0, {});
    auto partials3 = Aggregator::createPartialAggregations({sr3}, AggregationMethod::LATEST, 0, {});

    std::vector<PartialAggregationResult> allPartials;
    allPartials.insert(allPartials.end(), partials1.begin(), partials1.end());
    allPartials.insert(allPartials.end(), partials2.begin(), partials2.end());
    allPartials.insert(allPartials.end(), partials3.begin(), partials3.end());

    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::LATEST);

    ASSERT_EQ(grouped.size(), 1);
    // Should have 3 points (one per unique timestamp)
    EXPECT_EQ(grouped[0].points.size(), 3);

    // Find the point with value 30.0 (latest by timestamp)
    bool foundLatest = false;
    for (const auto& point : grouped[0].points) {
        if (point.timestamp == 3000) {
            EXPECT_DOUBLE_EQ(point.value, 30.0);
            foundLatest = true;
        }
    }
    EXPECT_TRUE(foundLatest);
}

TEST_F(DistributedAggregatorTest, MergePartialAggregationsGroupedEmptyInput) {
    std::vector<PartialAggregationResult> emptyPartials;

    auto grouped = Aggregator::mergePartialAggregationsGrouped(emptyPartials, AggregationMethod::AVG);

    EXPECT_EQ(grouped.size(), 0);
}

TEST_F(DistributedAggregatorTest, HashBasedGroupingConsistency) {
    // Verify that hash-based grouping produces consistent results
    std::vector<uint64_t> timestamps = {1000};
    std::vector<double> values = {10.0};

    // Create series with same metadata but in different order
    SeriesResult sr1 = createSeriesResult("cpu", {{"host", "server1"}, {"region", "us-west"}}, "usage", timestamps, values);
    SeriesResult sr2 = createSeriesResult("cpu", {{"host", "server1"}, {"region", "us-west"}}, "usage", timestamps, values);

    auto partials1 = Aggregator::createPartialAggregations({sr1}, AggregationMethod::AVG, 0, {"host", "region"});
    auto partials2 = Aggregator::createPartialAggregations({sr2}, AggregationMethod::AVG, 0, {"host", "region"});

    // Hash should be identical
    EXPECT_EQ(partials1[0].groupKeyHash, partials2[0].groupKeyHash);

    // Should merge into single group
    std::vector<PartialAggregationResult> allPartials = {partials1[0], partials2[0]};
    auto grouped = Aggregator::mergePartialAggregationsGrouped(allPartials, AggregationMethod::AVG);

    ASSERT_EQ(grouped.size(), 1);
}
