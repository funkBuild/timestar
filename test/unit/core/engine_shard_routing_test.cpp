// Behavioral tests for series -> shard routing invariants and the
// distributed-index model.
//
// Invariants guarded:
//
// 1. Canonical routing: the owning shard of a series is
//        routeToCore(SeriesId128::fromSeriesKey(buildSeriesKey(m, tags, f)))
//    Every producer (test helpers, HTTP write handler, TimeStarInsert) must
//    derive the shard from the canonical series-key string. A historical bug
//    routed via a different key construction ("fromComponents"), landing data
//    on a different shard than queries looked at.
//
// 2. Distributed index: each shard's NativeIndex holds metadata ONLY for the
//    series whose data lives on that shard. executeLocalQuery() and
//    deleteByPattern() must consult the LOCAL index (no shard-0 routing, no
//    global view). We verify this behaviorally: the owning shard sees the
//    series, every other shard does not.
//
// These replace the previous source-inspection tests (which grepped
// engine.cpp for "index.getSeriesId" etc.) with tests that drive a real
// sharded Engine. If executeLocalQuery/deleteByPattern were changed to route
// metadata lookups to shard 0 (or a global index), the owning-shard
// assertions below fail; if data routing diverged from the canonical series
// key, the placement assertions fail.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/query/shard_query.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <string>
#include <vector>

// Multi-shard tests skip on developer boxes with a forced single shard, but
// must never skip silently in CI (see engine_metadata_test.cpp).
#define REQUIRE_MULTI_SHARD()                                                               \
    if (seastar::smp::count < 2) {                                                          \
        if (std::getenv("CI")) {                                                            \
            FAIL() << "Multi-shard test would be skipped in CI — run the suite with -c 2+"; \
        }                                                                                   \
        GTEST_SKIP() << "Need at least 2 shards for this test";                             \
    }

class EngineShardRoutingTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }

    // Canonical owning shard for a series, per the routing contract.
    static unsigned owningShard(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                const std::string& field) {
        std::string key = timestar::buildSeriesKey(measurement, tags, field);
        return timestar::routeToCore(SeriesId128::fromSeriesKey(key));
    }

    // Insert one double series (3 points at ts 1000/2000/3000) on its
    // canonically routed shard, mimicking the HTTP write path.
    static void insertOnOwningShard(seastar::sharded<Engine>& eng, const std::string& measurement,
                                    const std::map<std::string, std::string>& tags, const std::string& field) {
        TimeStarInsert<double> insert(measurement, field);
        for (const auto& [k, v] : tags) {
            insert.addTag(k, v);
        }
        insert.addValue(1000, 1.0);
        insert.addValue(2000, 2.0);
        insert.addValue(3000, 3.0);

        unsigned shard = owningShard(measurement, tags, field);
        eng.invoke_on(shard, [insert](Engine& engine) mutable { return engine.insert(std::move(insert)); }).get();
    }

    // Count of data points visible for the series via a local data query on
    // the given shard (0 if the series is unknown there).
    static size_t pointsOnShard(seastar::sharded<Engine>& eng, unsigned shard, const std::string& seriesKey) {
        auto resultOpt =
            eng.invoke_on(shard, [seriesKey](Engine& engine) { return engine.query(seriesKey, 0, UINT64_MAX); }).get();
        if (!resultOpt.has_value()) {
            return 0;
        }
        return std::get<QueryResult<double>>(resultOpt.value()).timestamps.size();
    }
};

// ---------------------------------------------------------------------------
// 1. TimeStarInsert's series key equals the canonical buildSeriesKey() string.
//    All routing flows through SeriesId128::fromSeriesKey(<this string>), so
//    any divergence here silently splits data placement from query lookup.
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, InsertSeriesKeyMatchesCanonicalBuildSeriesKey) {
    std::map<std::string, std::string> tags{{"host", "h1"}, {"rack", "r2"}};

    TimeStarInsert<double> insert("route_canon", "value");
    for (const auto& [k, v] : tags) {
        insert.addTag(k, v);
    }

    std::string canonical = timestar::buildSeriesKey("route_canon", tags, "value");
    EXPECT_EQ(insert.seriesKey(), canonical)
        << "TimeStarInsert::seriesKey() diverged from timestar::buildSeriesKey(); "
           "shard routing (fromSeriesKey) would place data and queries on different shards";

    EXPECT_EQ(timestar::routeToCore(SeriesId128::fromSeriesKey(insert.seriesKey())),
              timestar::routeToCore(SeriesId128::fromSeriesKey(canonical)));
}

// ---------------------------------------------------------------------------
// 2. Data inserted on the canonically routed shard is visible there and ONLY
//    there (per-shard data + per-shard index, no global state).
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, CanonicalRoutingPlacesDataAndMetadataOnOwningShardOnly) {
    REQUIRE_MULTI_SHARD();

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        const std::string measurement = "route_place";
        const std::map<std::string, std::string> tags{{"host", "h1"}};
        const std::string field = "value";
        const std::string seriesKey = timestar::buildSeriesKey(measurement, tags, field);
        const unsigned shard = owningShard(measurement, tags, field);

        insertOnOwningShard(eng.eng, measurement, tags, field);

        // Data on the owning shard, nowhere else.
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            size_t points = pointsOnShard(eng.eng, s, seriesKey);
            if (s == shard) {
                EXPECT_EQ(points, 3u) << "Data must live on the canonically routed shard " << shard;
            } else {
                EXPECT_EQ(points, 0u) << "Data leaked to non-owning shard " << s;
            }
        }

        // Index metadata co-located with the data: only the owning shard's
        // local index resolves the series.
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            auto idOpt = eng.eng
                             .invoke_on(s,
                                        [measurement, tags, field](Engine& engine) {
                                            return engine.getIndex().getSeriesId(measurement, tags, field);
                                        })
                             .get();
            EXPECT_EQ(idOpt.has_value(), s == shard)
                << "Series metadata must exist on the owning shard's local index only (shard " << s << ")";
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 3. executeLocalQuery consults the LOCAL index: the owning shard returns the
//    series data; any other shard returns nothing for the same series ID.
//    (If metadata lookups were routed to shard 0 or a global index, both
//    assertions would invert.)
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, ExecuteLocalQueryUsesLocalIndexOnly) {
    REQUIRE_MULTI_SHARD();

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        const std::string measurement = "route_elq";
        const std::map<std::string, std::string> tags{{"host", "h1"}};
        const std::string field = "value";
        const unsigned shard = owningShard(measurement, tags, field);
        const SeriesId128 seriesId = SeriesId128::fromSeriesKey(timestar::buildSeriesKey(measurement, tags, field));

        insertOnOwningShard(eng.eng, measurement, tags, field);

        auto runLocalQuery = [&eng, &seriesId](unsigned s) {
            timestar::ShardQuery sq;
            sq.shardId = s;
            sq.seriesIds = {seriesId};
            sq.startTime = 0;
            sq.endTime = UINT64_MAX;
            return eng.eng.invoke_on(s, [sq](Engine& engine) { return engine.executeLocalQuery(sq); }).get();
        };

        // Owning shard: local index resolves the series ID and returns data.
        auto owningResults = runLocalQuery(shard);
        ASSERT_EQ(owningResults.size(), 1u)
            << "executeLocalQuery on the owning shard must resolve the series via its LOCAL index";
        EXPECT_EQ(owningResults[0].measurement, measurement);
        ASSERT_TRUE(owningResults[0].fields.count(field) > 0);
        EXPECT_EQ(owningResults[0].fields.at(field).first.size(), 3u);

        // Every other shard: local index has no such series -> empty result.
        const unsigned other = (shard + 1) % seastar::smp::count;
        auto otherResults = runLocalQuery(other);
        EXPECT_TRUE(otherResults.empty())
            << "executeLocalQuery on a non-owning shard must not see the series (its local index is authoritative)";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 4. deleteByPattern consults the LOCAL index: a non-owning shard deletes
//    nothing; the owning shard deletes the series.
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, DeleteByPatternUsesLocalIndexOnly) {
    REQUIRE_MULTI_SHARD();

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        const std::string measurement = "route_del";
        const std::map<std::string, std::string> tags{{"host", "h1"}};
        const std::string field = "value";
        const std::string seriesKey = timestar::buildSeriesKey(measurement, tags, field);
        const unsigned shard = owningShard(measurement, tags, field);
        const unsigned other = (shard + 1) % seastar::smp::count;

        insertOnOwningShard(eng.eng, measurement, tags, field);

        Engine::DeleteRequest request;
        request.measurement = measurement;
        request.startTime = 0;
        request.endTime = UINT64_MAX;

        // Non-owning shard: its local index has no series for this
        // measurement, so nothing is deleted and the data survives.
        auto otherResult =
            eng.eng.invoke_on(other, [request](Engine& engine) { return engine.deleteByPattern(request); }).get();
        EXPECT_EQ(otherResult.seriesDeleted, 0u)
            << "deleteByPattern on a non-owning shard must be a local no-op (local index model)";
        EXPECT_EQ(pointsOnShard(eng.eng, shard, seriesKey), 3u)
            << "Data must survive a deleteByPattern issued on a non-owning shard";

        // Owning shard: local index finds the series, delete succeeds.
        auto owningResult =
            eng.eng.invoke_on(shard, [request](Engine& engine) { return engine.deleteByPattern(request); }).get();
        EXPECT_EQ(owningResult.seriesDeleted, 1u)
            << "deleteByPattern on the owning shard must find the series in its LOCAL index";
        EXPECT_EQ(pointsOnShard(eng.eng, shard, seriesKey), 0u) << "Data must be gone after owning-shard delete";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// 5. End-to-end: the HTTP write handler routes by the canonical series key.
//    Data written through the public handler must land on
//    routeToCore(fromSeriesKey(buildSeriesKey(...))) — the shard every reader
//    (scatter-gather query, delete, subscription) computes independently.
// ---------------------------------------------------------------------------
TEST_F(EngineShardRoutingTest, HttpWriteHandlerRoutesByCanonicalSeriesKey) {
    REQUIRE_MULTI_SHARD();

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        auto req = std::make_unique<seastar::http::request>();
        req->_headers["Content-Type"] = "application/json";
        req->content = R"({
            "measurement": "route_http",
            "tags": {"host": "h9", "dc": "west"},
            "fields": {"value": 42.5},
            "timestamp": 1000
        })";

        auto rep = handler.handleWrite(std::move(req)).get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << "Write failed: " << rep->_content;

        const std::map<std::string, std::string> tags{{"dc", "west"}, {"host", "h9"}};
        const std::string seriesKey = timestar::buildSeriesKey("route_http", tags, "value");
        const unsigned expectedShard = timestar::routeToCore(SeriesId128::fromSeriesKey(seriesKey));

        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            size_t points = pointsOnShard(eng.eng, s, seriesKey);
            if (s == expectedShard) {
                EXPECT_EQ(points, 1u) << "HTTP write handler must route data to the canonical shard " << expectedShard
                                      << " (fromSeriesKey(buildSeriesKey(...)))";
            } else {
                EXPECT_EQ(points, 0u) << "HTTP write handler placed data on non-canonical shard " << s;
            }
        }
    })
        .join()
        .get();
}
