#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>

using namespace timestar::index;

// =============================================================================
// Regression tests for tombstone persistence through SSTable flush.
//
// BUG: doFlushImmutableMemTable() skipped tombstone entries when writing to
// SSTable. After flush, deleted keys would resurrect from older SSTables.
//
// Tests use the public retention policy API (which internally calls kvPut,
// kvGet, kvDelete) and close/reopen to trigger memtable flush to SSTable.
// =============================================================================

class NativeIndexTombstoneFlushTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::remove_all("shard_0/native_index"); }
    void TearDown() override { std::filesystem::remove_all("shard_0/native_index"); }
};

// ---------------------------------------------------------------------------
// Core bug: set policy -> flush -> delete policy -> flush -> get must be nullopt
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(NativeIndexTombstoneFlushTest, DeletedRetentionPolicyDoesNotResurrectAfterFlush) {
    RetentionPolicy policy;
    policy.measurement = "test_meas";
    policy.ttl = "1h";
    policy.ttlNanos = 3600'000'000'000ULL;

    // Phase 1: Write policy and flush to SSTable via close/reopen
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.setRetentionPolicy(policy);

        // Verify it's readable
        auto val = co_await index.getRetentionPolicy("test_meas");
        EXPECT_TRUE(val.has_value());
        if (!val.has_value()) { co_await index.close(); co_return; }
        EXPECT_EQ(val->ttlNanos, 3600'000'000'000ULL);

        co_await index.close();  // triggers flushMemTable -> SSTable S1
    }

    // Phase 2: Reopen, delete the policy, close (flush tombstone to SSTable S2)
    {
        NativeIndex index(0);
        co_await index.open();

        // Verify policy survived reopen (read from SSTable)
        auto val = co_await index.getRetentionPolicy("test_meas");
        EXPECT_TRUE(val.has_value()) << "Policy should survive reopen";
        if (!val.has_value()) { co_await index.close(); co_return; }

        // Delete the policy (creates tombstone in memtable)
        auto deleted = co_await index.deleteRetentionPolicy("test_meas");
        EXPECT_TRUE(deleted);

        // Verify deletion visible while tombstone is in memtable
        auto val2 = co_await index.getRetentionPolicy("test_meas");
        EXPECT_FALSE(val2.has_value()) << "Policy should be deleted (tombstone in memtable)";

        co_await index.close();  // flush tombstone to SSTable S2
    }

    // Phase 3: Reopen — tombstone in S2 must suppress value in S1
    {
        NativeIndex index(0);
        co_await index.open();

        auto val = co_await index.getRetentionPolicy("test_meas");
        EXPECT_FALSE(val.has_value())
            << "Deleted retention policy resurrected after tombstone flush!";

        // getAllRetentionPolicies (uses kvPrefixScan) must also not find it
        auto all = co_await index.getAllRetentionPolicies();
        for (const auto& p : all) {
            EXPECT_NE(p.measurement, "test_meas")
                << "Prefix scan found tombstoned retention policy";
        }

        co_await index.close();
    }
}

// ---------------------------------------------------------------------------
// Verify that a tombstone followed by re-insert works correctly
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(NativeIndexTombstoneFlushTest, ReinsertAfterDeleteAndFlushWorks) {
    RetentionPolicy policy;
    policy.measurement = "reinsert_meas";
    policy.ttl = "100s";
    policy.ttlNanos = 100'000'000'000ULL;

    // Write, close (flush to S1)
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.setRetentionPolicy(policy);
        co_await index.close();
    }

    // Delete, close (flush tombstone to S2)
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.deleteRetentionPolicy("reinsert_meas");
        co_await index.close();
    }

    // Re-insert with a new value, close (flush to S3)
    {
        NativeIndex index(0);
        co_await index.open();
        policy.ttl = "999s";
        policy.ttlNanos = 999'000'000'000ULL;
        co_await index.setRetentionPolicy(policy);
        co_await index.close();
    }

    // Reopen — should see the re-inserted value, not the tombstone
    {
        NativeIndex index(0);
        co_await index.open();

        auto val = co_await index.getRetentionPolicy("reinsert_meas");
        EXPECT_TRUE(val.has_value()) << "Re-inserted policy should be visible";
        if (!val.has_value()) { co_await index.close(); co_return; }
        EXPECT_EQ(val->ttlNanos, 999'000'000'000ULL);

        co_await index.close();
    }
}

// ---------------------------------------------------------------------------
// Tombstone-only flush: memtable has only deletes, no live puts.
// The SSTable must still be written so the tombstone suppresses older values.
// ---------------------------------------------------------------------------
SEASTAR_TEST_F(NativeIndexTombstoneFlushTest, TombstoneOnlyFlushSuppressesOlderValues) {
    RetentionPolicy policyA;
    policyA.measurement = "sole_meas";
    policyA.ttl = "42s";
    policyA.ttlNanos = 42'000'000'000ULL;

    // Write policy, close (flush to S1)
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.setRetentionPolicy(policyA);
        co_await index.close();
    }

    // Delete with nothing else in memtable, close (tombstone-only flush to S2)
    {
        NativeIndex index(0);
        co_await index.open();
        co_await index.deleteRetentionPolicy("sole_meas");
        co_await index.close();
    }

    // Reopen — must not find the policy
    {
        NativeIndex index(0);
        co_await index.open();

        auto val = co_await index.getRetentionPolicy("sole_meas");
        EXPECT_FALSE(val.has_value())
            << "Tombstone-only SSTable failed to suppress old value";

        co_await index.close();
    }
}
