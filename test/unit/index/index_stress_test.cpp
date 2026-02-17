/*
 * Stress and concurrency tests for LevelDB and Metadata index components.
 *
 * Tests cover:
 *   - Concurrent fiber access to LevelDBIndex
 *   - Large-scale indexing performance
 *   - Index recovery / persistence under load
 *   - Memory usage patterns under sustained writes
 *   - MetadataIndex async operations and consistency
 *   - Cross-component metadata coordination
 */

#include <gtest/gtest.h>
#include "../../seastar_gtest.hpp"
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/timer.hh>
#include <filesystem>
#include <chrono>
#include <random>
#include <numeric>

#include "../../../lib/index/leveldb_index.hpp"
#include "../../../lib/index/metadata_index.hpp"
#include "../../../lib/core/tsdb_value.hpp"
#include "../../../lib/core/series_id.hpp"

// ---------------------------------------------------------------------------
//  Test fixtures
// ---------------------------------------------------------------------------

static const std::string STRESS_METADATA_PATH = "/tmp/test_metadata_index_stress";

class LevelDBIndexStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all("shard_0");
    }

    void TearDown() override {
        std::filesystem::remove_all("shard_0");
    }
};

class MetadataIndexStressTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::remove_all(STRESS_METADATA_PATH);
    }

    void TearDown() override {
        std::filesystem::remove_all(STRESS_METADATA_PATH);
    }
};

// ---------------------------------------------------------------------------
//  LevelDBIndex stress tests
// ---------------------------------------------------------------------------

// Test concurrent fiber access: many fibers creating series simultaneously
SEASTAR_TEST_F(LevelDBIndexStressTest, ConcurrentFiberSeriesCreation) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_CONCURRENT = 200;
    std::vector<seastar::future<SeriesId128>> futures;
    futures.reserve(NUM_CONCURRENT);

    // Launch many concurrent getOrCreateSeriesId calls
    for (int i = 0; i < NUM_CONCURRENT; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "server-" + std::to_string(i % 20)},
            {"region", "region-" + std::to_string(i % 5)},
            {"cpu", std::to_string(i % 4)}
        };
        futures.push_back(
            index.getOrCreateSeriesId("stress_metric",
                                      std::move(tags),
                                      "field_" + std::to_string(i % 3)));
    }

    auto ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());

    // Verify we got valid IDs for all requests
    for (const auto& id : ids) {
        EXPECT_FALSE(id.isZero());
    }

    // Verify uniqueness: the number of unique combinations is
    // 20 hosts * 5 regions * 4 cpus * 3 fields = up to 1200 unique series,
    // but we only create 200 with overlapping combos.  Unique count
    // depends on the actual combinations generated.
    std::set<SeriesId128> uniqueIds(ids.begin(), ids.end());

    // With 200 entries and possible collisions from modular arithmetic,
    // we should have some duplicates
    EXPECT_GT(uniqueIds.size(), 0u);
    EXPECT_LE(uniqueIds.size(), static_cast<size_t>(NUM_CONCURRENT));

    // Verify series count in the index
    size_t count = co_await index.getSeriesCount();
    EXPECT_EQ(count, uniqueIds.size());

    co_await index.close();
    co_return;
}

// Test concurrent reads and writes interleaved
SEASTAR_TEST_F(LevelDBIndexStressTest, ConcurrentReadWriteMix) {
    LevelDBIndex index(0);
    co_await index.open();

    // Phase 1: Seed some initial data
    const int SEED_COUNT = 50;
    std::vector<SeriesId128> seededIds;
    for (int i = 0; i < SEED_COUNT; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)},
            {"dc", "dc-" + std::to_string(i % 3)}
        };
        auto id = co_await index.getOrCreateSeriesId("rw_metric", tags, "value");
        seededIds.push_back(id);
    }

    // Phase 2: Mix of reads and writes concurrently
    const int MIX_OPS = 300;
    std::vector<seastar::future<>> mixFutures;
    mixFutures.reserve(MIX_OPS);

    for (int i = 0; i < MIX_OPS; i++) {
        if (i % 3 == 0) {
            // Write: create new series
            std::map<std::string, std::string> tags = {
                {"host", "new-host-" + std::to_string(i)},
                {"dc", "dc-" + std::to_string(i % 5)}
            };
            mixFutures.push_back(
                index.getOrCreateSeriesId("rw_metric", std::move(tags),
                                          "field_" + std::to_string(i % 2))
                    .discard_result());
        } else if (i % 3 == 1) {
            // Read: query metadata for an existing series
            auto sid = seededIds[i % SEED_COUNT];
            mixFutures.push_back(
                index.getSeriesMetadata(sid).discard_result());
        } else {
            // Read: get fields for measurement
            mixFutures.push_back(
                index.getFields("rw_metric").discard_result());
        }
    }

    co_await seastar::when_all_succeed(mixFutures.begin(), mixFutures.end());

    // Verify the index is still consistent
    auto fields = co_await index.getFields("rw_metric");
    EXPECT_GE(fields.size(), 1u);

    auto tags = co_await index.getTags("rw_metric");
    EXPECT_GE(tags.size(), 1u);

    co_await index.close();
    co_return;
}

// Test large-scale indexing: create many series and verify all are retrievable
SEASTAR_TEST_F(LevelDBIndexStressTest, LargeScaleIndexing) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_MEASUREMENTS = 5;
    const int NUM_HOSTS = 50;
    const int NUM_FIELDS = 3;
    const int TOTAL_SERIES = NUM_MEASUREMENTS * NUM_HOSTS * NUM_FIELDS;

    std::vector<SeriesId128> allIds;
    allIds.reserve(TOTAL_SERIES);

    auto startTime = std::chrono::steady_clock::now();

    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "large_metric_" + std::to_string(m);
        for (int h = 0; h < NUM_HOSTS; h++) {
            for (int f = 0; f < NUM_FIELDS; f++) {
                std::map<std::string, std::string> tags = {
                    {"host", "host-" + std::to_string(h)},
                    {"datacenter", "dc-" + std::to_string(h % 5)},
                    {"rack", "rack-" + std::to_string(h % 10)}
                };
                auto id = co_await index.getOrCreateSeriesId(
                    measurement, tags, "field_" + std::to_string(f));
                allIds.push_back(id);
            }
        }
    }

    auto endTime = std::chrono::steady_clock::now();
    auto durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime).count();

    // Verify total series count
    size_t totalCount = co_await index.getSeriesCount();
    EXPECT_EQ(totalCount, static_cast<size_t>(TOTAL_SERIES));

    // Verify all IDs are unique
    std::set<SeriesId128> uniqueIds(allIds.begin(), allIds.end());
    EXPECT_EQ(uniqueIds.size(), static_cast<size_t>(TOTAL_SERIES));

    // Verify each measurement has the correct number of fields
    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "large_metric_" + std::to_string(m);
        auto fields = co_await index.getFields(measurement);
        EXPECT_EQ(fields.size(), static_cast<size_t>(NUM_FIELDS));
    }

    // Verify tag value counts
    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "large_metric_" + std::to_string(m);
        auto dcValues = co_await index.getTagValues(measurement, "datacenter");
        EXPECT_EQ(dcValues.size(), 5u);  // 5 datacenters (0-4)

        auto rackValues = co_await index.getTagValues(measurement, "rack");
        EXPECT_EQ(rackValues.size(), 10u);  // 10 racks (0-9)
    }

    // Performance sanity check: should complete in reasonable time
    // (generous threshold to avoid flaky tests)
    EXPECT_LT(durationMs, 30000) << "Indexing " << TOTAL_SERIES
        << " series took " << durationMs << "ms (expected < 30s)";

    co_await index.close();
    co_return;
}

// Test idempotency: repeated getOrCreateSeriesId returns consistent results
SEASTAR_TEST_F(LevelDBIndexStressTest, IdempotencyUnderLoad) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_SERIES = 50;
    const int REPETITIONS = 10;

    // First pass: create all series and record IDs
    std::vector<SeriesId128> expectedIds;
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)},
            {"env", i % 2 == 0 ? "prod" : "staging"}
        };
        auto id = co_await index.getOrCreateSeriesId("idempotent_metric", tags, "value");
        expectedIds.push_back(id);
    }

    // Repeated passes: verify same IDs returned each time
    for (int r = 0; r < REPETITIONS; r++) {
        std::vector<seastar::future<SeriesId128>> futures;
        futures.reserve(NUM_SERIES);

        for (int i = 0; i < NUM_SERIES; i++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(i)},
                {"env", i % 2 == 0 ? "prod" : "staging"}
            };
            futures.push_back(
                index.getOrCreateSeriesId("idempotent_metric",
                                          std::move(tags), "value"));
        }

        auto ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        for (int i = 0; i < NUM_SERIES; i++) {
            EXPECT_EQ(ids[i], expectedIds[i])
                << "Mismatch at series " << i << " on repetition " << r;
        }
    }

    // Series count should remain the same
    size_t count = co_await index.getSeriesCount();
    EXPECT_EQ(count, static_cast<size_t>(NUM_SERIES));

    co_await index.close();
    co_return;
}

// Test persistence under load: create series, close, reopen, verify all data
SEASTAR_TEST_F(LevelDBIndexStressTest, PersistenceUnderLoad) {
    const int NUM_SERIES = 200;
    std::vector<SeriesId128> originalIds;
    originalIds.reserve(NUM_SERIES);

    // Phase 1: Create many series
    {
        auto index = std::make_unique<LevelDBIndex>(0);
        co_await index->open();

        for (int i = 0; i < NUM_SERIES; i++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(i)},
                {"region", "region-" + std::to_string(i % 10)},
                {"service", "svc-" + std::to_string(i % 7)}
            };
            auto id = co_await index->getOrCreateSeriesId(
                "persist_metric", tags, "field_" + std::to_string(i % 4));
            originalIds.push_back(id);
        }

        co_await index->close();
    }

    // Phase 2: Reopen and verify all data survived
    {
        auto index = std::make_unique<LevelDBIndex>(0);
        co_await index->open();

        // Verify series count
        size_t count = co_await index->getSeriesCount();
        EXPECT_EQ(count, static_cast<size_t>(NUM_SERIES));

        // Verify each series returns the same ID
        for (int i = 0; i < NUM_SERIES; i++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(i)},
                {"region", "region-" + std::to_string(i % 10)},
                {"service", "svc-" + std::to_string(i % 7)}
            };
            auto id = co_await index->getOrCreateSeriesId(
                "persist_metric", tags, "field_" + std::to_string(i % 4));
            EXPECT_EQ(id, originalIds[i])
                << "ID mismatch for series " << i << " after reopen";
        }

        // Verify metadata is intact
        auto fields = co_await index->getFields("persist_metric");
        EXPECT_EQ(fields.size(), 4u);  // field_0 through field_3

        auto tags = co_await index->getTags("persist_metric");
        EXPECT_EQ(tags.size(), 3u);  // host, region, service

        auto regions = co_await index->getTagValues("persist_metric", "region");
        EXPECT_EQ(regions.size(), 10u);  // region-0 through region-9

        co_await index->close();
    }

    co_return;
}

// Test concurrent tag queries and series discovery
SEASTAR_TEST_F(LevelDBIndexStressTest, ConcurrentTagQueries) {
    LevelDBIndex index(0);
    co_await index.open();

    // Set up a moderate number of series
    const int NUM_HOSTS = 20;
    const int NUM_DCS = 4;
    for (int h = 0; h < NUM_HOSTS; h++) {
        for (int dc = 0; dc < NUM_DCS; dc++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(h)},
                {"datacenter", "dc-" + std::to_string(dc)}
            };
            co_await index.getOrCreateSeriesId("query_metric", tags, "value");
        }
    }

    // Fire off many concurrent tag-based queries
    const int NUM_QUERIES = 100;
    std::vector<seastar::future<std::vector<SeriesId128>>> queryFutures;
    queryFutures.reserve(NUM_QUERIES);

    for (int q = 0; q < NUM_QUERIES; q++) {
        if (q % 2 == 0) {
            queryFutures.push_back(
                index.findSeriesByTag("query_metric", "datacenter",
                                      "dc-" + std::to_string(q % NUM_DCS)));
        } else {
            queryFutures.push_back(
                index.findSeriesByTag("query_metric", "host",
                                      "host-" + std::to_string(q % NUM_HOSTS)));
        }
    }

    auto results = co_await seastar::when_all_succeed(queryFutures.begin(), queryFutures.end());

    // Verify query results
    for (int q = 0; q < NUM_QUERIES; q++) {
        if (q % 2 == 0) {
            // Datacenter query: each DC has NUM_HOSTS series
            EXPECT_EQ(results[q].size(), static_cast<size_t>(NUM_HOSTS))
                << "DC query " << q << " returned wrong count";
        } else {
            // Host query: each host has NUM_DCS series
            EXPECT_EQ(results[q].size(), static_cast<size_t>(NUM_DCS))
                << "Host query " << q << " returned wrong count";
        }
    }

    co_await index.close();
    co_return;
}

// Test group-by operations at scale
SEASTAR_TEST_F(LevelDBIndexStressTest, GroupByAtScale) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_REGIONS = 8;
    const int HOSTS_PER_REGION = 15;

    // Create series across many regions and hosts
    for (int r = 0; r < NUM_REGIONS; r++) {
        for (int h = 0; h < HOSTS_PER_REGION; h++) {
            std::map<std::string, std::string> tags = {
                {"region", "region-" + std::to_string(r)},
                {"host", "host-" + std::to_string(r * 100 + h)},
                {"tier", h < 5 ? "critical" : "standard"}
            };
            co_await index.getOrCreateSeriesId("groupby_metric", tags, "value");
        }
    }

    // Group by region
    auto byRegion = co_await index.getSeriesGroupedByTag("groupby_metric", "region");
    EXPECT_EQ(byRegion.size(), static_cast<size_t>(NUM_REGIONS));
    for (const auto& [region, ids] : byRegion) {
        EXPECT_EQ(ids.size(), static_cast<size_t>(HOSTS_PER_REGION))
            << "Region " << region << " has wrong series count";
    }

    // Group by tier
    auto byTier = co_await index.getSeriesGroupedByTag("groupby_metric", "tier");
    EXPECT_EQ(byTier.size(), 2u);  // critical and standard
    EXPECT_EQ(byTier["critical"].size(),
              static_cast<size_t>(NUM_REGIONS * 5));  // 5 per region
    EXPECT_EQ(byTier["standard"].size(),
              static_cast<size_t>(NUM_REGIONS * (HOSTS_PER_REGION - 5)));

    co_await index.close();
    co_return;
}

// Test field stats updates under concurrent load
SEASTAR_TEST_F(LevelDBIndexStressTest, ConcurrentFieldStatsUpdates) {
    LevelDBIndex index(0);
    co_await index.open();

    // Create some series
    const int NUM_SERIES = 30;
    std::vector<SeriesId128> seriesIds;
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)}
        };
        auto id = co_await index.getOrCreateSeriesId("stats_metric", tags, "value");
        seriesIds.push_back(id);
    }

    // Concurrently update field stats for all series
    std::vector<seastar::future<>> updateFutures;
    updateFutures.reserve(NUM_SERIES);

    for (int i = 0; i < NUM_SERIES; i++) {
        LevelDBIndex::FieldStats stats;
        stats.dataType = "float";
        stats.minTime = static_cast<int64_t>(i) * 1000000;
        stats.maxTime = static_cast<int64_t>(i + 1) * 1000000;
        stats.pointCount = static_cast<uint64_t>((i + 1) * 100);

        updateFutures.push_back(
            index.updateFieldStats(seriesIds[i], "value", stats));
    }

    co_await seastar::when_all_succeed(updateFutures.begin(), updateFutures.end());

    // Verify all stats were written correctly
    for (int i = 0; i < NUM_SERIES; i++) {
        auto stats = co_await index.getFieldStats(seriesIds[i], "value");
        EXPECT_TRUE(stats.has_value()) << "Missing stats for series " << i;
        if (stats.has_value()) {
            EXPECT_EQ(stats->dataType, "float");
            EXPECT_EQ(stats->minTime, static_cast<int64_t>(i) * 1000000);
            EXPECT_EQ(stats->maxTime, static_cast<int64_t>(i + 1) * 1000000);
            EXPECT_EQ(stats->pointCount, static_cast<uint64_t>((i + 1) * 100));
        }
    }

    co_await index.close();
    co_return;
}

// Test compaction after heavy writes
SEASTAR_TEST_F(LevelDBIndexStressTest, CompactionAfterHeavyWrites) {
    LevelDBIndex index(0);
    co_await index.open();

    // Write a substantial number of series to generate LevelDB sstable files
    const int NUM_SERIES = 500;
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)},
            {"env", "env-" + std::to_string(i % 3)},
            {"cluster", "cluster-" + std::to_string(i % 7)}
        };
        co_await index.getOrCreateSeriesId("compact_metric", tags, "value");
    }

    // Trigger compaction
    co_await index.compact();

    // Verify all data is still accessible after compaction
    size_t count = co_await index.getSeriesCount();
    EXPECT_EQ(count, static_cast<size_t>(NUM_SERIES));

    auto fields = co_await index.getFields("compact_metric");
    EXPECT_EQ(fields.size(), 1u);

    auto clusters = co_await index.getTagValues("compact_metric", "cluster");
    EXPECT_EQ(clusters.size(), 7u);

    // Verify findSeriesByTag still works post-compaction
    auto env0Series = co_await index.findSeriesByTag("compact_metric", "env", "env-0");
    // env-0 gets hosts 0,3,6,...,498 => ceil(500/3) hosts
    size_t expectedEnv0 = (NUM_SERIES + 2) / 3;
    EXPECT_EQ(env0Series.size(), expectedEnv0);

    co_await index.close();
    co_return;
}

// Test indexInsert with TSDBInsert under load (template path)
SEASTAR_TEST_F(LevelDBIndexStressTest, IndexInsertTemplateStress) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_INSERTS = 100;
    std::vector<seastar::future<SeriesId128>> futures;
    futures.reserve(NUM_INSERTS);

    for (int i = 0; i < NUM_INSERTS; i++) {
        TSDBInsert<double> insert("tmpl_metric", "field_" + std::to_string(i % 5));
        insert.addTag("host", "host-" + std::to_string(i % 10));
        insert.addTag("region", "region-" + std::to_string(i % 3));
        futures.push_back(index.indexInsert(insert));
    }

    auto ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());

    // Verify all returned valid IDs
    for (const auto& id : ids) {
        EXPECT_FALSE(id.isZero());
    }

    // Verify metadata was populated correctly
    auto fields = co_await index.getFields("tmpl_metric");
    EXPECT_EQ(fields.size(), 5u);  // field_0 through field_4

    auto tags = co_await index.getTags("tmpl_metric");
    EXPECT_EQ(tags.size(), 2u);  // host, region

    auto hosts = co_await index.getTagValues("tmpl_metric", "host");
    EXPECT_EQ(hosts.size(), 10u);

    auto regions = co_await index.getTagValues("tmpl_metric", "region");
    EXPECT_EQ(regions.size(), 3u);

    co_await index.close();
    co_return;
}

// Test multiple measurements with overlapping tag keys
SEASTAR_TEST_F(LevelDBIndexStressTest, MultipleMeasurementsIsolation) {
    LevelDBIndex index(0);
    co_await index.open();

    const int NUM_MEASUREMENTS = 10;
    const int SERIES_PER_MEASUREMENT = 20;

    // Create series across many measurements, all using "host" tag
    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "isolation_metric_" + std::to_string(m);
        for (int s = 0; s < SERIES_PER_MEASUREMENT; s++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(s)},
                {"type", "type-" + std::to_string(s % 3)}
            };
            co_await index.getOrCreateSeriesId(measurement, tags, "value");
        }
    }

    // Verify measurements are isolated
    auto allMeasurements = co_await index.getAllMeasurements();
    EXPECT_EQ(allMeasurements.size(), static_cast<size_t>(NUM_MEASUREMENTS));

    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "isolation_metric_" + std::to_string(m);

        auto allSeries = (co_await index.getAllSeriesForMeasurement(measurement)).value();
        EXPECT_EQ(allSeries.size(), static_cast<size_t>(SERIES_PER_MEASUREMENT))
            << "Measurement " << measurement << " has wrong series count";

        auto hosts = co_await index.getTagValues(measurement, "host");
        EXPECT_EQ(hosts.size(), static_cast<size_t>(SERIES_PER_MEASUREMENT));

        auto types = co_await index.getTagValues(measurement, "type");
        EXPECT_EQ(types.size(), 3u);
    }

    // Total series count should be NUM_MEASUREMENTS * SERIES_PER_MEASUREMENT
    size_t totalCount = co_await index.getSeriesCount();
    EXPECT_EQ(totalCount,
              static_cast<size_t>(NUM_MEASUREMENTS * SERIES_PER_MEASUREMENT));

    co_await index.close();
    co_return;
}

// ---------------------------------------------------------------------------
//  MetadataIndex stress tests
// ---------------------------------------------------------------------------

// Test concurrent series creation in MetadataIndex
SEASTAR_TEST_F(MetadataIndexStressTest, ConcurrentSeriesCreation) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_CONCURRENT = 500;
    std::vector<seastar::future<uint64_t>> futures;
    futures.reserve(NUM_CONCURRENT);

    for (int i = 0; i < NUM_CONCURRENT; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)},
            {"dc", "dc-" + std::to_string(i % 5)}
        };
        futures.push_back(
            index->getOrCreateSeriesId("stress_measurement", tags, "value"));
    }

    auto ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());

    // All IDs should be valid and unique (each combo is unique)
    std::set<uint64_t> uniqueIds(ids.begin(), ids.end());
    EXPECT_EQ(uniqueIds.size(), static_cast<size_t>(NUM_CONCURRENT));

    for (auto id : ids) {
        EXPECT_GT(id, 0u);
    }

    // All series should be discoverable by measurement
    auto foundIds = co_await index->findSeriesByMeasurement("stress_measurement");
    EXPECT_EQ(foundIds.size(), static_cast<size_t>(NUM_CONCURRENT));

    co_await index->close();
    co_return;
}

// Test MetadataIndex idempotency under concurrent repeated access
SEASTAR_TEST_F(MetadataIndexStressTest, IdempotentConcurrentAccess) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_SERIES = 30;
    const int WAVES = 5;

    // Create initial series
    std::vector<uint64_t> expectedIds;
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"sensor", "sensor-" + std::to_string(i)}
        };
        auto id = co_await index->getOrCreateSeriesId("idempotent_test", tags, "value");
        expectedIds.push_back(id);
    }

    // Re-create the same series in concurrent waves
    for (int w = 0; w < WAVES; w++) {
        std::vector<seastar::future<uint64_t>> futures;
        futures.reserve(NUM_SERIES);
        for (int i = 0; i < NUM_SERIES; i++) {
            std::map<std::string, std::string> tags = {
                {"sensor", "sensor-" + std::to_string(i)}
            };
            futures.push_back(
                index->getOrCreateSeriesId("idempotent_test", tags, "value"));
        }
        auto ids = co_await seastar::when_all_succeed(futures.begin(), futures.end());
        for (int i = 0; i < NUM_SERIES; i++) {
            EXPECT_EQ(ids[i], expectedIds[i])
                << "ID mismatch at series " << i << " wave " << w;
        }
    }

    // Series count should not have increased
    auto found = co_await index->findSeriesByMeasurement("idempotent_test");
    EXPECT_EQ(found.size(), static_cast<size_t>(NUM_SERIES));

    co_await index->close();
    co_return;
}

// Test MetadataIndex persistence after bulk writes
SEASTAR_TEST_F(MetadataIndexStressTest, PersistenceAfterBulkWrites) {
    const int NUM_SERIES = 300;
    std::vector<uint64_t> originalIds;
    originalIds.reserve(NUM_SERIES);

    // Phase 1: Bulk create
    {
        auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
        co_await index->init();

        for (int i = 0; i < NUM_SERIES; i++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(i)},
                {"app", "app-" + std::to_string(i % 8)}
            };
            auto id = co_await index->getOrCreateSeriesId(
                "persist_test", tags, "metric_" + std::to_string(i % 4));
            originalIds.push_back(id);
        }

        co_await index->close();
    }

    // Phase 2: Reopen and verify
    {
        auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
        co_await index->init();

        // Verify all series can be found
        auto found = co_await index->findSeriesByMeasurement("persist_test");
        EXPECT_EQ(found.size(), static_cast<size_t>(NUM_SERIES));

        // Verify IDs match (spot check every 10th series)
        for (int i = 0; i < NUM_SERIES; i += 10) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(i)},
                {"app", "app-" + std::to_string(i % 8)}
            };
            auto id = co_await index->getOrCreateSeriesId(
                "persist_test", tags, "metric_" + std::to_string(i % 4));
            EXPECT_EQ(id, originalIds[i])
                << "ID mismatch for series " << i << " after reopen";
        }

        // Verify tag values are persisted
        auto appValues = co_await index->getTagValues("persist_test", "app");
        EXPECT_EQ(appValues.size(), 8u);

        auto hostValues = co_await index->getTagValues("persist_test", "host");
        EXPECT_EQ(hostValues.size(), static_cast<size_t>(NUM_SERIES));

        co_await index->close();
    }

    co_return;
}

// Test MetadataIndex tag-based queries at scale
SEASTAR_TEST_F(MetadataIndexStressTest, TagQueryAtScale) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_ENVS = 3;        // prod, staging, dev
    const int NUM_SERVICES = 10;
    const int NUM_INSTANCES = 5;
    const int TOTAL = NUM_ENVS * NUM_SERVICES * NUM_INSTANCES;

    std::string envNames[] = {"prod", "staging", "dev"};

    // Create a matrix of series
    for (int e = 0; e < NUM_ENVS; e++) {
        for (int s = 0; s < NUM_SERVICES; s++) {
            for (int inst = 0; inst < NUM_INSTANCES; inst++) {
                std::map<std::string, std::string> tags = {
                    {"env", envNames[e]},
                    {"service", "svc-" + std::to_string(s)},
                    {"instance", "inst-" + std::to_string(inst)}
                };
                co_await index->getOrCreateSeriesId("scale_query", tags, "latency");
            }
        }
    }

    // Query by single tag
    auto prodSeries = co_await index->findSeriesByTag("scale_query", "env", "prod");
    EXPECT_EQ(prodSeries.size(),
              static_cast<size_t>(NUM_SERVICES * NUM_INSTANCES));

    auto svc0Series = co_await index->findSeriesByTag("scale_query", "service", "svc-0");
    EXPECT_EQ(svc0Series.size(),
              static_cast<size_t>(NUM_ENVS * NUM_INSTANCES));

    // Query by composite tags
    std::map<std::string, std::string> filter = {
        {"env", "prod"},
        {"service", "svc-0"}
    };
    auto filtered = co_await index->findSeriesByTags("scale_query", filter);
    EXPECT_EQ(filtered.size(), static_cast<size_t>(NUM_INSTANCES));

    // Group by env
    auto groupedByEnv = co_await index->getSeriesGroupedByTag("scale_query", "env");
    EXPECT_EQ(groupedByEnv.size(), static_cast<size_t>(NUM_ENVS));
    for (const auto& [env, ids] : groupedByEnv) {
        EXPECT_EQ(ids.size(), static_cast<size_t>(NUM_SERVICES * NUM_INSTANCES))
            << "Wrong count for env: " << env;
    }

    // Verify total
    auto all = co_await index->findSeriesByMeasurement("scale_query");
    EXPECT_EQ(all.size(), static_cast<size_t>(TOTAL));

    co_await index->close();
    co_return;
}

// Test MetadataIndex series update and delete under load
SEASTAR_TEST_F(MetadataIndexStressTest, UpdateAndDeleteUnderLoad) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_SERIES = 100;
    std::vector<uint64_t> ids;
    ids.reserve(NUM_SERIES);

    // Create series
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)}
        };
        auto id = co_await index->getOrCreateSeriesId("delete_test", tags, "value");
        ids.push_back(id);
    }

    // Update metadata for all series concurrently
    std::vector<seastar::future<>> updateFutures;
    updateFutures.reserve(NUM_SERIES);
    for (int i = 0; i < NUM_SERIES; i++) {
        MetadataSeriesInfo info;
        info.seriesId = ids[i];
        info.measurement = "delete_test";
        info.tags = {{"host", "host-" + std::to_string(i)}};
        info.fields = {"value"};
        info.minTime = static_cast<int64_t>(i) * 1000000;
        info.maxTime = static_cast<int64_t>(i + 1) * 1000000;
        info.shardId = 0;

        updateFutures.push_back(index->updateSeriesMetadata(info));
    }
    co_await seastar::when_all_succeed(updateFutures.begin(), updateFutures.end());

    // Verify updates
    for (int i = 0; i < NUM_SERIES; i += 10) {
        auto meta = co_await index->getSeriesMetadata(ids[i]);
        EXPECT_TRUE(meta.has_value());
        if (meta.has_value()) {
            EXPECT_EQ(meta->minTime, static_cast<int64_t>(i) * 1000000);
            EXPECT_EQ(meta->maxTime, static_cast<int64_t>(i + 1) * 1000000);
        }
    }

    // Delete every other series
    for (int i = 0; i < NUM_SERIES; i += 2) {
        co_await index->deleteSeries(ids[i]);
    }

    // Verify deleted series are gone
    for (int i = 0; i < NUM_SERIES; i += 2) {
        auto meta = co_await index->getSeriesMetadata(ids[i]);
        EXPECT_FALSE(meta.has_value())
            << "Series " << ids[i] << " should have been deleted";
    }

    // Verify remaining series are intact
    for (int i = 1; i < NUM_SERIES; i += 2) {
        auto meta = co_await index->getSeriesMetadata(ids[i]);
        EXPECT_TRUE(meta.has_value())
            << "Series " << ids[i] << " should still exist";
    }

    co_await index->close();
    co_return;
}

// Test MetadataIndex field stats updates at scale
SEASTAR_TEST_F(MetadataIndexStressTest, FieldStatsAtScale) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_SERIES = 50;
    const int NUM_UPDATES = 5;
    std::vector<uint64_t> seriesIds;

    // Create series
    for (int i = 0; i < NUM_SERIES; i++) {
        std::map<std::string, std::string> tags = {
            {"sensor", "sensor-" + std::to_string(i)}
        };
        auto id = co_await index->getOrCreateSeriesId("fstats_test", tags, "temp");
        seriesIds.push_back(id);
    }

    // Simulate multiple rounds of stats updates (like compaction rounds)
    for (int round = 0; round < NUM_UPDATES; round++) {
        std::vector<seastar::future<>> futures;
        futures.reserve(NUM_SERIES);

        for (int i = 0; i < NUM_SERIES; i++) {
            FieldStats stats;
            stats.dataType = "float";
            stats.minValue = static_cast<double>(i);
            stats.maxValue = static_cast<double>(i + round * 10);
            stats.pointCount = static_cast<uint64_t>((round + 1) * 100);

            futures.push_back(
                index->updateFieldStats("fstats_test", "temp",
                                        seriesIds[i], stats));
        }

        co_await seastar::when_all_succeed(futures.begin(), futures.end());
    }

    // Verify the index stats are accessible
    std::string stats = index->getStats();
    EXPECT_FALSE(stats.empty());

    co_await index->close();
    co_return;
}

// Test MetadataIndex cross-measurement isolation
SEASTAR_TEST_F(MetadataIndexStressTest, CrossMeasurementIsolation) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    const int NUM_MEASUREMENTS = 8;
    const int SERIES_PER = 25;

    // Create series across many measurements, all using "host" tag key
    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "isolated_" + std::to_string(m);
        for (int s = 0; s < SERIES_PER; s++) {
            std::map<std::string, std::string> tags = {
                {"host", "host-" + std::to_string(s)},
                {"level", s < 10 ? "critical" : "normal"}
            };
            co_await index->getOrCreateSeriesId(measurement, tags, "value");
        }
    }

    // Verify each measurement is isolated
    for (int m = 0; m < NUM_MEASUREMENTS; m++) {
        std::string measurement = "isolated_" + std::to_string(m);

        auto seriesIds = co_await index->findSeriesByMeasurement(measurement);
        EXPECT_EQ(seriesIds.size(), static_cast<size_t>(SERIES_PER))
            << "Wrong count for measurement " << measurement;

        // Verify tag values are measurement-scoped
        auto hostValues = co_await index->getTagValues(measurement, "host");
        EXPECT_EQ(hostValues.size(), static_cast<size_t>(SERIES_PER));

        auto levelValues = co_await index->getTagValues(measurement, "level");
        EXPECT_EQ(levelValues.size(), 2u);  // "critical" and "normal"
    }

    co_await index->close();
    co_return;
}

// Test concurrent mixed operations: creates, queries, metadata reads in parallel
SEASTAR_TEST_F(MetadataIndexStressTest, MixedConcurrentOperations) {
    auto index = std::make_unique<MetadataIndex>(STRESS_METADATA_PATH);
    co_await index->init();

    // Seed initial data
    const int SEED = 50;
    std::vector<uint64_t> seededIds;
    for (int i = 0; i < SEED; i++) {
        std::map<std::string, std::string> tags = {
            {"host", "host-" + std::to_string(i)},
            {"env", i % 2 == 0 ? "prod" : "staging"}
        };
        auto id = co_await index->getOrCreateSeriesId("mixed_ops", tags, "value");
        seededIds.push_back(id);
    }

    // Fire a mixed batch of concurrent operations
    const int BATCH_SIZE = 200;
    std::vector<seastar::future<>> futures;
    futures.reserve(BATCH_SIZE);

    for (int i = 0; i < BATCH_SIZE; i++) {
        int op = i % 5;
        switch (op) {
            case 0: {
                // Create new series
                std::map<std::string, std::string> tags = {
                    {"host", "new-host-" + std::to_string(i)},
                    {"env", "dev"}
                };
                futures.push_back(
                    index->getOrCreateSeriesId("mixed_ops", std::move(tags), "value")
                        .discard_result());
                break;
            }
            case 1: {
                // Get metadata for an existing series
                futures.push_back(
                    index->getSeriesMetadata(seededIds[i % SEED])
                        .discard_result());
                break;
            }
            case 2: {
                // Find series by measurement
                futures.push_back(
                    index->findSeriesByMeasurement("mixed_ops")
                        .discard_result());
                break;
            }
            case 3: {
                // Find series by tag
                futures.push_back(
                    index->findSeriesByTag("mixed_ops", "env",
                                           i % 2 == 0 ? "prod" : "staging")
                        .discard_result());
                break;
            }
            case 4: {
                // Get tag values
                futures.push_back(
                    index->getTagValues("mixed_ops", "env")
                        .discard_result());
                break;
            }
        }
    }

    co_await seastar::when_all_succeed(futures.begin(), futures.end());

    // Verify index is still consistent after the storm
    auto allSeries = co_await index->findSeriesByMeasurement("mixed_ops");
    EXPECT_GE(allSeries.size(), static_cast<size_t>(SEED));

    // Verify tag queries still work
    auto envValues = co_await index->getTagValues("mixed_ops", "env");
    EXPECT_GE(envValues.size(), 2u);  // at least "prod" and "staging" from seed

    co_await index->close();
    co_return;
}
