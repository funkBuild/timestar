#include "../../../lib/config/timestar_config.hpp"
#include "../../../lib/index/native/manifest.hpp"
#include "../../../lib/index/native/native_index.hpp"
#include "../../../lib/retention/retention_policy.hpp"
#include "../../seastar_gtest.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <filesystem>
#include <format>

using namespace timestar::index;

// =============================================================================
// Micro gate for the tiered compaction policy (L0→L1→L2 cascades).
//
// Before the tiered policy, the index NEVER merged L1: every memtable flush
// eventually produced an L1 file that lived forever, so the SSTable count grew
// without bound and every kvGet/kvPrefixScan walked all files.
//
// This test shrinks write_buffer_size so ordinary public-API writes force
// dozens of memtable flushes through the real flush path (maybeFlushMemTable →
// CompactionEngine::maybeCompact cascade), then asserts:
//   1. filesAtLevel(1) stays below the L1 threshold (L1→L2 compaction ran)
//   2. the total file count stays bounded
//   3. every key is still readable (correctness across multi-level merges)
// =============================================================================

class NativeIndexCompactionGateTest : public ::testing::Test {
public:
    void SetUp() override {
        std::filesystem::remove_all("shard_0/native_index");
        savedConfig_ = timestar::config();
        auto cfg = savedConfig_;
        cfg.index.write_buffer_size = 4096;  // tiny buffer → one flush per ~4KB written
        timestar::setGlobalConfig(cfg);
    }
    void TearDown() override {
        timestar::setGlobalConfig(savedConfig_);
        std::filesystem::remove_all("shard_0/native_index");
    }

    timestar::TimestarConfig savedConfig_;
};

SEASTAR_TEST_F(NativeIndexCompactionGateTest, ManyFlushesKeepSSTableCountBounded) {
    constexpr int kPolicies = 2000;  // ~300KB of KV data → ~50+ flushes at a 4KB buffer

    {
        NativeIndex index(0);
        co_await index.open();

        for (int i = 0; i < kPolicies; ++i) {
            RetentionPolicy policy;
            policy.measurement = std::format("gate_meas_{:05d}", i);
            policy.ttl = std::format("{}h", (i % 100) + 1);
            policy.ttlNanos = static_cast<uint64_t>((i % 100) + 1) * 3600ULL * 1'000'000'000ULL;
            co_await index.setRetentionPolicy(policy);
        }

        // Correctness: every policy readable through memtable + all SSTables
        // (exercises kvGet with key-range pruning and multi-level merges).
        for (int i = 0; i < kPolicies; i += 97) {
            auto val = co_await index.getRetentionPolicy(std::format("gate_meas_{:05d}", i));
            EXPECT_TRUE(val.has_value()) << "policy " << i << " lost after flush/compaction";
            if (val.has_value()) {
                EXPECT_EQ(val->ttlNanos, static_cast<uint64_t>((i % 100) + 1) * 3600ULL * 1'000'000'000ULL);
            }
        }

        // getAllRetentionPolicies walks a full prefix scan across all sources.
        auto all = co_await index.getAllRetentionPolicies();
        EXPECT_EQ(all.size(), static_cast<size_t>(kPolicies));

        co_await index.close();
    }

    // Inspect the manifest directly: the tiered policy must have kept every
    // level bounded. Without L1→L2 compaction, ~50 flushes leave ~12+ L1
    // files; with it, L1 stays below the threshold of 8.
    {
        auto manifestDir = std::filesystem::absolute("shard_0/native_index").string();
        auto manifest = co_await Manifest::open(manifestDir);
        EXPECT_LT(manifest.filesAtLevel(0).size(), 4u);
        EXPECT_LT(manifest.filesAtLevel(1).size(), 8u) << "L1 -> L2 compaction never ran";
        EXPECT_LT(manifest.filesAtLevel(2).size(), 8u);
        EXPECT_LE(manifest.files().size(), 12u) << "SSTable count must stay bounded by tiered compaction";
        co_await manifest.close();
    }

    // Reopen: recovery must see the compacted state (manifest v2 + maxKey
    // pruning) and still return correct data.
    {
        NativeIndex index(0);
        co_await index.open();
        auto val = co_await index.getRetentionPolicy("gate_meas_00042");
        EXPECT_TRUE(val.has_value());
        if (val.has_value()) {
            EXPECT_EQ(val->ttlNanos, 43ULL * 3600ULL * 1'000'000'000ULL);
        }
        auto missing = co_await index.getRetentionPolicy("gate_meas_99999");
        EXPECT_FALSE(missing.has_value());
        co_await index.close();
    }
}
