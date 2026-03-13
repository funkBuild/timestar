#include "../../../lib/query/query_planner.hpp"

#include "../../../lib/query/query_parser.hpp"

#include <gtest/gtest.h>

using namespace timestar;

class QueryPlannerTest : public ::testing::Test {
protected:
    std::unique_ptr<QueryPlanner> planner;

    void SetUp() override { planner = std::make_unique<QueryPlanner>(); }

    void TearDown() override {}
};

// Test basic plan creation
TEST_F(QueryPlannerTest, CreateBasicPlan) {
    QueryRequest request;
    request.measurement = "temperature";
    request.fields = {"value"};
    request.scopes = {{"location", "us-west"}};
    request.startTime = 1000000000;
    request.endTime = 2000000000;
    request.aggregation = AggregationMethod::AVG;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    // Should have found series for us-west temperature sensors
    EXPECT_GT(plan.estimatedSeriesCount, 0);
    EXPECT_EQ(plan.aggregation, AggregationMethod::AVG);
    EXPECT_TRUE(plan.groupByTags.empty());
}

// Test plan with all fields
TEST_F(QueryPlannerTest, CreatePlanAllFields) {
    QueryRequest request;
    request.measurement = "temperature";
    // Empty fields means all fields
    request.scopes = {{"location", "us-east"}, {"sensor", "temp-01"}};
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    // Mock implementation returns 3 default fields when all fields requested
    EXPECT_EQ(plan.estimatedSeriesCount, 3);  // Mock returns 3 fields
}

// Test plan with no matching series
TEST_F(QueryPlannerTest, CreatePlanNoMatches) {
    QueryRequest request;
    // Empty measurement means no series
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    EXPECT_EQ(plan.estimatedSeriesCount, 0);
    EXPECT_TRUE(plan.shardQueries.empty());
}

// Test plan with group by
TEST_F(QueryPlannerTest, CreatePlanWithGroupBy) {
    QueryRequest request;
    request.measurement = "temperature";
    request.fields = {"value"};
    request.scopes = {{"location", "us-west"}};
    request.groupByTags = {"sensor"};
    request.startTime = 1000000000;
    request.endTime = 2000000000;
    request.aggregation = AggregationMethod::MAX;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    EXPECT_EQ(plan.aggregation, AggregationMethod::MAX);
    EXPECT_EQ(plan.groupByTags.size(), 1);
    EXPECT_EQ(plan.groupByTags[0], "sensor");
}

// Test shard calculation
TEST_F(QueryPlannerTest, ShardCalculation) {
    // Test that series are distributed to shards correctly
    QueryRequest request;
    request.measurement = "cpu";
    request.fields = {"usage_percent"};
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    // Series should be distributed across shards
    // With smp::count == 0 (test environment), should use shard 0
    if (!plan.shardQueries.empty()) {
        EXPECT_GE(plan.shardQueries[0].shardId, 0);
    }
}

// Test plan with multiple fields
TEST_F(QueryPlannerTest, CreatePlanMultipleFields) {
    QueryRequest request;
    request.measurement = "temperature";
    request.fields = {"value", "humidity"};
    request.scopes = {{"location", "us-east"}, {"sensor", "temp-01"}};
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    // Mock returns one series ID per requested field
    EXPECT_EQ(plan.estimatedSeriesCount, 2);

    // Check that fields are set in shard queries
    if (!plan.shardQueries.empty()) {
        auto& sq = plan.shardQueries[0];
        EXPECT_TRUE(sq.fields.count("value") > 0 || sq.fields.count("humidity") > 0);
    }
}

// Test requiresAllShards function
TEST_F(QueryPlannerTest, RequiresAllShards) {
    QueryPlanner planner;

    // Empty scopes requires all shards
    QueryRequest request1;
    request1.measurement = "test";
    EXPECT_TRUE(planner.requiresAllShards(request1));

    // Exact match doesn't require all shards
    QueryRequest request2;
    request2.measurement = "test";
    request2.scopes = {{"host", "server-01"}};
    EXPECT_FALSE(planner.requiresAllShards(request2));

    // Wildcard requires all shards
    QueryRequest request3;
    request3.measurement = "test";
    request3.scopes = {{"host", "server-*"}};
    EXPECT_TRUE(planner.requiresAllShards(request3));

    // Regex requires all shards
    QueryRequest request4;
    request4.measurement = "test";
    request4.scopes = {{"host", "/server-[0-9]+/"}};
    EXPECT_TRUE(planner.requiresAllShards(request4));
}

// Test series key building for sharding
TEST_F(QueryPlannerTest, SeriesKeyBuilding) {
    std::string key =
        planner->buildSeriesKeyForSharding("temperature", {{"location", "us-west"}, {"sensor", "temp-01"}}, "value");

    // Should build consistent key with sorted tags and space separator for field
    EXPECT_EQ(key, "temperature,location=us-west,sensor=temp-01 value");

    // Test with different tag order (should produce same key due to map sorting)
    std::map<std::string, std::string> tags;
    tags["sensor"] = "temp-01";
    tags["location"] = "us-west";

    std::string key2 = planner->buildSeriesKeyForSharding("temperature", tags, "value");

    EXPECT_EQ(key, key2);
}

// Test plan merging requirement
TEST_F(QueryPlannerTest, RequiresMerging) {
    // Single shard query doesn't require merging
    QueryRequest request1;
    request1.measurement = "temperature";
    request1.fields = {"value"};
    request1.scopes = {{"location", "us-west"}, {"sensor", "temp-01"}};
    request1.startTime = 1000000000;
    request1.endTime = 2000000000;

    QueryPlan plan1 = planner->createPlanSync(request1, nullptr);

    // With single shard (test environment), no merging needed
    if (plan1.shardQueries.size() == 1) {
        EXPECT_FALSE(plan1.requiresMerging);
    }

    // Query that might span multiple shards would require merging
    QueryRequest request2;
    request2.measurement = "cpu";
    request2.fields = {"usage_percent"};
    request2.startTime = 1000000000;
    request2.endTime = 2000000000;

    QueryPlan plan2 = planner->createPlanSync(request2, nullptr);

    // If multiple shards involved, merging is required
    if (plan2.shardQueries.size() > 1) {
        EXPECT_TRUE(plan2.requiresMerging);
    }
}

// Test empty query plan
TEST_F(QueryPlannerTest, EmptyPlan) {
    QueryRequest request;
    // No measurement specified
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    EXPECT_EQ(plan.estimatedSeriesCount, 0);
    EXPECT_TRUE(plan.shardQueries.empty());
    EXPECT_FALSE(plan.requiresMerging);
}

// Test plan with time range
TEST_F(QueryPlannerTest, PlanWithTimeRange) {
    QueryRequest request;
    request.measurement = "temperature";
    request.fields = {"value"};
    request.startTime = 1000000000;
    request.endTime = 2000000000;

    QueryPlan plan = planner->createPlanSync(request, nullptr);

    // Verify time range is propagated to shard queries
    for (const auto& sq : plan.shardQueries) {
        EXPECT_EQ(sq.startTime, 1000000000);
        EXPECT_EQ(sq.endTime, 2000000000);
    }
}

// Test aggregation method propagation
TEST_F(QueryPlannerTest, AggregationPropagation) {
    std::vector<AggregationMethod> methods = {AggregationMethod::AVG, AggregationMethod::MIN, AggregationMethod::MAX,
                                              AggregationMethod::SUM, AggregationMethod::LATEST};

    for (auto method : methods) {
        QueryRequest request;
        request.measurement = "temperature";
        request.fields = {"value"};
        request.aggregation = method;
        request.startTime = 1000000000;
        request.endTime = 2000000000;

        QueryPlan plan = planner->createPlanSync(request, nullptr);

        EXPECT_EQ(plan.aggregation, method);
    }
}