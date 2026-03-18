// Tests for Engine::query(series, seriesId, startTime, endTime) — the 4-arg overload
// that accepts a pre-computed SeriesId128 to avoid redundant SHA1 hashing.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>
#include <string>

class EngineQuerySeriesIdTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// Verifies that the 4-arg query overload returns data from the memory store
// when given a pre-computed SeriesId128.
TEST_F(EngineQuerySeriesIdTest, QueryWithPrecomputedSeriesIdFromMemoryStore) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("pressure", "value");
        insert.addTag("site", "alpha");
        insert.addValue(1000, 101.3);
        insert.addValue(2000, 101.5);
        insert.addValue(3000, 101.7);

        std::string seriesKey = insert.seriesKey();
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        eng->insert(std::move(insert)).get();

        // Use the 4-arg overload with pre-computed SeriesId128
        auto resultOpt = eng->query(seriesKey, seriesId, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 3u);
        EXPECT_DOUBLE_EQ(result.values[0], 101.3);
        EXPECT_DOUBLE_EQ(result.values[1], 101.5);
        EXPECT_DOUBLE_EQ(result.values[2], 101.7);
    })
        .join()
        .get();
}

// Verifies that the 4-arg query overload returns data after a rollover (TSM path).
TEST_F(EngineQuerySeriesIdTest, QueryWithPrecomputedSeriesIdFromTSM) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("pressure", "value");
        insert.addTag("site", "beta");
        insert.addValue(1000, 99.0);
        insert.addValue(2000, 99.5);
        insert.addValue(3000, 100.0);
        insert.addValue(4000, 100.5);

        std::string seriesKey = insert.seriesKey();
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        eng->insert(std::move(insert)).get();

        // Flush to TSM so the query reads from disk
        eng->rolloverMemoryStore().get();

        auto resultOpt = eng->query(seriesKey, seriesId, 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());

        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        ASSERT_EQ(result.timestamps.size(), 4u);
        EXPECT_DOUBLE_EQ(result.values[0], 99.0);
        EXPECT_DOUBLE_EQ(result.values[3], 100.5);
    })
        .join()
        .get();
}

// Verifies that the 3-arg and 4-arg overloads produce identical results.
TEST_F(EngineQuerySeriesIdTest, ThreeArgAndFourArgOverloadsReturnSameResults) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("voltage", "reading");
        insert.addTag("panel", "south");
        insert.addValue(1000, 12.1);
        insert.addValue(2000, 12.3);
        insert.addValue(3000, 12.5);

        std::string seriesKey = insert.seriesKey();
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        eng->insert(std::move(insert)).get();
        eng->rolloverMemoryStore().get();

        // Insert more data into memory store so query merges TSM + memory
        {
            TimeStarInsert<double> insert2("voltage", "reading");
            insert2.addTag("panel", "south");
            insert2.addValue(4000, 12.7);
            insert2.addValue(5000, 12.9);
            eng->insert(std::move(insert2)).get();
        }

        // Query with 3-arg (recomputes SHA1 internally)
        auto result3 = eng->query(seriesKey, 0, UINT64_MAX).get();
        // Query with 4-arg (pre-computed SeriesId128)
        auto result4 = eng->query(seriesKey, seriesId, 0, UINT64_MAX).get();

        ASSERT_TRUE(result3.has_value());
        ASSERT_TRUE(result4.has_value());

        auto& r3 = std::get<QueryResult<double>>(result3.value());
        auto& r4 = std::get<QueryResult<double>>(result4.value());

        ASSERT_EQ(r3.timestamps.size(), r4.timestamps.size());
        EXPECT_EQ(r3.timestamps.size(), 5u);

        for (size_t i = 0; i < r3.timestamps.size(); ++i) {
            EXPECT_EQ(r3.timestamps[i], r4.timestamps[i]);
            EXPECT_DOUBLE_EQ(r3.values[i], r4.values[i]);
        }
    })
        .join()
        .get();
}

// Verifies that the 4-arg overload returns nullopt for a non-existent series.
TEST_F(EngineQuerySeriesIdTest, QueryWithPrecomputedSeriesIdNonExistent) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::string seriesKey = "nonexistent_measurement value";
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

        auto resultOpt = eng->query(seriesKey, seriesId, 0, UINT64_MAX).get();
        EXPECT_FALSE(resultOpt.has_value());
    })
        .join()
        .get();
}
