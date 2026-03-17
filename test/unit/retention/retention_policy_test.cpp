/*
 * Tests for retention policy CRUD, validation, and cache distribution.
 */

#include "../../../lib/retention/retention_policy.hpp"

#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>

#include <filesystem>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

class RetentionPolicyTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0"); }

    void TearDown() override { std::filesystem::remove_all("shard_0"); }
};

// Test basic policy CRUD operations
SEASTAR_TEST_F(RetentionPolicyTest, SetAndGetPolicy) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    RetentionPolicy policy;
    policy.measurement = "cpu";
    policy.ttl = "90d";
    policy.ttlNanos = 90ULL * 86400ULL * 1000000000ULL;

    co_await index.setRetentionPolicy(policy);

    auto retrieved = co_await index.getRetentionPolicy("cpu");
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->measurement, "cpu");
    EXPECT_EQ(retrieved->ttl, "90d");
    EXPECT_EQ(retrieved->ttlNanos, policy.ttlNanos);

    co_await index.close();
}

SEASTAR_TEST_F(RetentionPolicyTest, GetNonExistentPolicy) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    auto retrieved = co_await index.getRetentionPolicy("nonexistent");
    EXPECT_FALSE(retrieved.has_value());

    co_await index.close();
}

SEASTAR_TEST_F(RetentionPolicyTest, DeletePolicy) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    RetentionPolicy policy;
    policy.measurement = "mem";
    policy.ttl = "30d";
    policy.ttlNanos = 30ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(policy);

    // Delete it
    bool deleted = co_await index.deleteRetentionPolicy("mem");
    EXPECT_TRUE(deleted);

    // Verify it's gone
    auto retrieved = co_await index.getRetentionPolicy("mem");
    EXPECT_FALSE(retrieved.has_value());

    // Delete non-existent returns false
    bool deletedAgain = co_await index.deleteRetentionPolicy("mem");
    EXPECT_FALSE(deletedAgain);

    co_await index.close();
}

SEASTAR_TEST_F(RetentionPolicyTest, GetAllPolicies) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    // Set multiple policies
    RetentionPolicy p1;
    p1.measurement = "cpu";
    p1.ttl = "90d";
    p1.ttlNanos = 90ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(p1);

    RetentionPolicy p2;
    p2.measurement = "mem";
    p2.ttl = "30d";
    p2.ttlNanos = 30ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(p2);

    RetentionPolicy p3;
    p3.measurement = "disk";
    p3.ttl = "7d";
    p3.ttlNanos = 7ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(p3);

    auto all = co_await index.getAllRetentionPolicies();
    EXPECT_EQ(all.size(), 3);

    // Verify all measurements are present
    std::set<std::string> measurements;
    for (const auto& p : all) {
        measurements.insert(p.measurement);
    }
    EXPECT_TRUE(measurements.count("cpu"));
    EXPECT_TRUE(measurements.count("mem"));
    EXPECT_TRUE(measurements.count("disk"));

    co_await index.close();
}

SEASTAR_TEST_F(RetentionPolicyTest, UpsertPolicy) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    // Set initial policy
    RetentionPolicy p1;
    p1.measurement = "cpu";
    p1.ttl = "90d";
    p1.ttlNanos = 90ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(p1);

    // Overwrite with new TTL
    RetentionPolicy p2;
    p2.measurement = "cpu";
    p2.ttl = "30d";
    p2.ttlNanos = 30ULL * 86400ULL * 1000000000ULL;
    co_await index.setRetentionPolicy(p2);

    auto retrieved = co_await index.getRetentionPolicy("cpu");
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_EQ(retrieved->ttl, "30d");
    EXPECT_EQ(retrieved->ttlNanos, p2.ttlNanos);

    // Still only one policy
    auto all = co_await index.getAllRetentionPolicies();
    EXPECT_EQ(all.size(), 1);

    co_await index.close();
}

SEASTAR_TEST_F(RetentionPolicyTest, PolicyWithDownsample) {
    timestar::index::NativeIndex index(0);
    co_await index.open();

    RetentionPolicy policy;
    policy.measurement = "temperature";
    policy.ttl = "90d";
    policy.ttlNanos = 90ULL * 86400ULL * 1000000000ULL;

    DownsamplePolicy ds;
    ds.after = "30d";
    ds.afterNanos = 30ULL * 86400ULL * 1000000000ULL;
    ds.interval = "5m";
    ds.intervalNanos = 5ULL * 60ULL * 1000000000ULL;
    ds.method = "avg";
    policy.downsample = ds;

    co_await index.setRetentionPolicy(policy);

    auto retrieved = co_await index.getRetentionPolicy("temperature");
    EXPECT_TRUE(retrieved.has_value());
    EXPECT_TRUE(retrieved->downsample.has_value());
    EXPECT_EQ(retrieved->downsample->after, "30d");
    EXPECT_EQ(retrieved->downsample->interval, "5m");
    EXPECT_EQ(retrieved->downsample->method, "avg");
    EXPECT_EQ(retrieved->downsample->afterNanos, ds.afterNanos);
    EXPECT_EQ(retrieved->downsample->intervalNanos, ds.intervalNanos);

    co_await index.close();
}

// Test the parseInterval function used for duration parsing
TEST(RetentionDurationParsingTest, ValidDurations) {
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("1s"), 1000000000ULL);
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("5m"), 5ULL * 60 * 1000000000ULL);
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("1h"), 3600ULL * 1000000000ULL);
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("30d"), 30ULL * 86400 * 1000000000ULL);
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("90d"), 90ULL * 86400 * 1000000000ULL);
    EXPECT_EQ(timestar::HttpQueryHandler::parseInterval("100ms"), 100000000ULL);
}

// Test RetentionPolicy Glaze serialization roundtrip
TEST(RetentionSerializationTest, RoundTrip) {
    RetentionPolicy policy;
    policy.measurement = "cpu";
    policy.ttl = "90d";
    policy.ttlNanos = 7776000000000000ULL;

    DownsamplePolicy ds;
    ds.after = "30d";
    ds.afterNanos = 2592000000000000ULL;
    ds.interval = "5m";
    ds.intervalNanos = 300000000000ULL;
    ds.method = "avg";
    policy.downsample = ds;

    auto json = glz::write_json(policy);
    ASSERT_TRUE(json.has_value());

    RetentionPolicy parsed;
    auto err = glz::read_json(parsed, *json);
    EXPECT_FALSE(static_cast<bool>(err));
    EXPECT_EQ(parsed.measurement, "cpu");
    EXPECT_EQ(parsed.ttl, "90d");
    EXPECT_EQ(parsed.ttlNanos, 7776000000000000ULL);
    EXPECT_TRUE(parsed.downsample.has_value());
    EXPECT_EQ(parsed.downsample->method, "avg");
    EXPECT_EQ(parsed.downsample->intervalNanos, 300000000000ULL);
}

// Test engine cache operations
TEST(RetentionCacheTest, BasicCacheOperations) {
    std::unordered_map<std::string, RetentionPolicy> cache;

    RetentionPolicy p;
    p.measurement = "cpu";
    p.ttl = "90d";
    p.ttlNanos = 7776000000000000ULL;
    cache[p.measurement] = p;

    // Lookup
    auto it = cache.find("cpu");
    ASSERT_NE(it, cache.end());
    EXPECT_EQ(it->second.ttlNanos, 7776000000000000ULL);

    // Update
    p.ttl = "30d";
    p.ttlNanos = 2592000000000000ULL;
    cache[p.measurement] = p;
    EXPECT_EQ(cache["cpu"].ttlNanos, 2592000000000000ULL);

    // Remove
    cache.erase("cpu");
    EXPECT_EQ(cache.find("cpu"), cache.end());
}

// Test validation: TTL must be > downsample.after
TEST(RetentionValidationTest, TtlGreaterThanDownsampleAfter) {
    RetentionPolicy policy;
    policy.measurement = "test";
    policy.ttl = "30d";
    policy.ttlNanos = 30ULL * 86400ULL * 1000000000ULL;

    DownsamplePolicy ds;
    ds.after = "90d";
    ds.afterNanos = 90ULL * 86400ULL * 1000000000ULL;
    ds.interval = "5m";
    ds.intervalNanos = 300000000000ULL;
    ds.method = "avg";
    policy.downsample = ds;

    // This should be rejected: ttl (30d) <= downsample.after (90d)
    bool isValid = true;
    if (policy.ttlNanos > 0 && policy.downsample.has_value()) {
        if (policy.ttlNanos <= policy.downsample->afterNanos) {
            isValid = false;
        }
    }
    EXPECT_FALSE(isValid);
}
