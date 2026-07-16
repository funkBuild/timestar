// Behavioral regression tests for the deleteRangeBySeries() phantom metadata bug.
//
// Bug history: deleteRangeBySeries() called getOrCreateSeriesId(), which CREATES
// index metadata for a series that does not exist. Deleting a non-existent
// series therefore left behind phantom metadata entries (series mapping,
// series metadata, measurement-series index).
//
// Fix: use getSeriesId() (lookup-only) on the local index and co_return false
// early when the series does not exist.
//
// These tests drive a real Engine (real data dir, real NativeIndex) and observe
// the externally visible contract:
//   - deleting a non-existent series returns false
//   - it leaves NO trace in the index (getSeriesId stays nullopt)
//   - deleting an existing series still works and removes the data
//
// If getOrCreateSeriesId() were ever reintroduced in the delete path, the
// phantom-metadata assertions below fail.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

class EngineDeletePhantomTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// ---------------------------------------------------------------------------
// 1. Deleting a series that was never written returns false (no-op)
// ---------------------------------------------------------------------------
TEST_F(EngineDeletePhantomTest, DeleteNonExistentSeriesReturnsFalse) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"location", "nowhere"}};
        bool deleted = eng->deleteRangeBySeries("phantom_m", tags, "value", 0, UINT64_MAX).get();

        EXPECT_FALSE(deleted) << "Deleting a series that was never written must be a no-op returning false";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 2. Core regression: deleting a non-existent series must NOT create phantom
//    metadata in the index
// ---------------------------------------------------------------------------
TEST_F(EngineDeletePhantomTest, DeleteNonExistentSeriesCreatesNoPhantomMetadata) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        std::map<std::string, std::string> tags{{"location", "nowhere"}};
        eng->deleteRangeBySeries("phantom_m", tags, "value", 0, UINT64_MAX).get();

        // The lookup-only probe: if the delete path used getOrCreateSeriesId,
        // this now returns a value (the phantom).
        auto seriesIdOpt = eng->getIndex().getSeriesId("phantom_m", tags, "value").get();
        EXPECT_FALSE(seriesIdOpt.has_value())
            << "BUG: deleteRangeBySeries() created phantom index metadata for a "
               "series that never existed (getOrCreateSeriesId regression)";

        // The measurement must not have appeared either.
        auto measurements = eng->getAllMeasurements().get();
        EXPECT_EQ(std::count(measurements.begin(), measurements.end(), "phantom_m"), 0)
            << "BUG: deleting a non-existent series registered its measurement";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 3. Deleting with tags that do not match any existing series is a no-op and
//    must not create a phantom series next to the real one
// ---------------------------------------------------------------------------
TEST_F(EngineDeletePhantomTest, DeleteWithWrongTagsIsNoOpAndCreatesNoPhantom) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        // Real series: temp,location=us-west value
        TimeStarInsert<double> insert("temp", "value");
        insert.addTag("location", "us-west");
        insert.addValue(1000, 20.0);
        insert.addValue(2000, 21.0);
        insert.addValue(3000, 22.0);
        eng->insert(std::move(insert)).get();

        // Delete a series with the same measurement/field but different tags.
        std::map<std::string, std::string> wrongTags{{"location", "us-east"}};
        bool deleted = eng->deleteRangeBySeries("temp", wrongTags, "value", 0, UINT64_MAX).get();
        EXPECT_FALSE(deleted) << "Delete with non-matching tags must return false";

        // No phantom series for the wrong tag combination.
        auto phantomId = eng->getIndex().getSeriesId("temp", wrongTags, "value").get();
        EXPECT_FALSE(phantomId.has_value()) << "BUG: delete with non-matching tags created a phantom series entry";

        // The real series and its data are untouched.
        std::map<std::string, std::string> realTags{{"location", "us-west"}};
        auto realId = eng->getIndex().getSeriesId("temp", realTags, "value").get();
        EXPECT_TRUE(realId.has_value());

        auto resultOpt = eng->query("temp,location=us-west value", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        EXPECT_EQ(result.timestamps.size(), 3u) << "Delete of a non-existent series must not touch real data";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 4. Deleting an existing series still works: returns true and removes data
// ---------------------------------------------------------------------------
TEST_F(EngineDeletePhantomTest, DeleteExistingSeriesRemovesData) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("disk", "usage");
        insert.addTag("host", "h1");
        insert.addValue(1000, 50.0);
        insert.addValue(2000, 55.0);
        insert.addValue(3000, 60.0);
        eng->insert(std::move(insert)).get();

        std::map<std::string, std::string> tags{{"host", "h1"}};
        bool deleted = eng->deleteRangeBySeries("disk", tags, "usage", 0, UINT64_MAX).get();
        EXPECT_TRUE(deleted) << "Deleting an existing series must return true";

        // Data is gone.
        auto resultOpt = eng->query("disk,host=h1 usage", 0, UINT64_MAX).get();
        if (resultOpt.has_value()) {
            auto& result = std::get<QueryResult<double>>(resultOpt.value());
            EXPECT_EQ(result.timestamps.size(), 0u) << "All points must be deleted";
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 5. Sub-range delete removes only the requested time window
// ---------------------------------------------------------------------------
TEST_F(EngineDeletePhantomTest, DeleteSubRangeKeepsPointsOutsideRange) {
    seastar::thread([] {
        ScopedEngine eng;
        eng.init();

        TimeStarInsert<double> insert("net", "bytes");
        insert.addTag("iface", "eth0");
        for (uint64_t ts = 1000; ts <= 5000; ts += 1000) {
            insert.addValue(ts, static_cast<double>(ts));
        }
        eng->insert(std::move(insert)).get();

        std::map<std::string, std::string> tags{{"iface", "eth0"}};
        bool deleted = eng->deleteRangeBySeries("net", tags, "bytes", 2000, 3000).get();
        EXPECT_TRUE(deleted);

        auto resultOpt = eng->query("net,iface=eth0 bytes", 0, UINT64_MAX).get();
        ASSERT_TRUE(resultOpt.has_value());
        auto& result = std::get<QueryResult<double>>(resultOpt.value());
        std::vector<uint64_t> expected{1000, 4000, 5000};
        EXPECT_EQ(result.timestamps, expected) << "Only points in [2000,3000] should be deleted";
    })
        .join()
        .get();
}
