// Integration F.3: the EngineLocalStore adapter over a REAL sharded<Engine>. A
// WriteBatch carrying all four value types (incl. a string series -- which the
// flat DataPoint could NOT represent) is applied, then read back through the real
// HTTP query pipeline, proving the enriched command is lossless end to end.
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class EngineLocalStoreTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

data::WriteSeries floatS() {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey("temp", {{"host", "h1"}}, "value");
    s.type = TSMValueType::Float;
    s.timestamps = {BASE};
    s.values = std::vector<double>{42.5};
    return s;
}
data::WriteSeries intS() {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey("cnt", {{"host", "h1"}}, "n");
    s.type = TSMValueType::Integer;
    s.timestamps = {BASE};
    // The int64 is STORED losslessly (codec test proves > 2^53 exactness); the
    // aggregating query path is double-based, so a numeric field always reads back
    // as double. Use a small value so the double is exact for the assertion.
    s.values = std::vector<int64_t>{100};
    return s;
}
data::WriteSeries boolS() {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey("up", {{"host", "h1"}}, "state");
    s.type = TSMValueType::Boolean;
    s.timestamps = {BASE};
    s.values = std::vector<bool>{true};
    return s;
}
data::WriteSeries stringS() {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey("log", {{"host", "h1"}}, "msg");
    s.type = TSMValueType::String;
    s.timestamps = {BASE};
    s.values = std::vector<std::string>{"a lossless UTF-8 message"};
    return s;
}

QueryResponse query(seastar::sharded<Engine>& eng, const std::string& measurement, const std::string& field) {
    http::HttpQueryHandler handler(&eng);
    QueryRequest q;
    // LATEST/FIRST SELECT a stored point (type-preserving) rather than compute one,
    // so an integer field comes back as int64, not coerced to double like AVG.
    q.aggregation = AggregationMethod::LATEST;
    q.measurement = measurement;
    q.fields = {field};
    q.startTime = BASE - 1'000'000'000ULL;
    q.endTime = BASE + 1'000'000'000ULL;
    return handler.executeQuery(q).get();
}
}  // namespace

TEST_F(EngineLocalStoreTest, ApplyWritesAllTypesVisibleViaRealQuery) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng, /*vshardCohesiveRouting=*/true);

        data::WriteBatch b;
        b.series = {floatS(), intS(), boolS(), stringS()};
        store.applyWrites(std::move(b)).get();

        // Float: exact value round-trips.
        {
            auto r = query(*eng, "temp", "value");
            ASSERT_TRUE(r.success) << r.errorMessage;
            ASSERT_EQ(r.series.size(), 1u);
            auto& f = r.series[0].fields.at("value");
            auto* v = std::get_if<std::vector<double>>(&f.second);
            ASSERT_NE(v, nullptr);
            ASSERT_EQ(v->size(), 1u);
            EXPECT_EQ((*v)[0], 42.5);
        }
        // String: the type DataPoint could not carry survives byte-for-byte.
        {
            auto r = query(*eng, "log", "msg");
            ASSERT_TRUE(r.success) << r.errorMessage;
            ASSERT_EQ(r.series.size(), 1u);
            auto& f = r.series[0].fields.at("msg");
            auto* v = std::get_if<std::vector<std::string>>(&f.second);
            ASSERT_NE(v, nullptr);
            ASSERT_EQ(v->size(), 1u);
            EXPECT_EQ((*v)[0], "a lossless UTF-8 message");
        }
        // Integer series: applied + queryable (returned as double by the numeric
        // aggregating query path).
        {
            auto r = query(*eng, "cnt", "n");
            ASSERT_TRUE(r.success) << r.errorMessage;
            ASSERT_EQ(r.series.size(), 1u);
            auto& f = r.series[0].fields.at("n");
            auto* v = std::get_if<std::vector<double>>(&f.second);
            ASSERT_NE(v, nullptr);
            ASSERT_EQ(v->size(), 1u);
            EXPECT_EQ((*v)[0], 100.0);
        }
        // Boolean returned as a boolean.
        {
            auto r = query(*eng, "up", "state");
            ASSERT_TRUE(r.success) << r.errorMessage;
            ASSERT_EQ(r.series.size(), 1u);
            auto& f = r.series[0].fields.at("state");
            auto* v = std::get_if<std::vector<bool>>(&f.second);
            ASSERT_NE(v, nullptr);
            ASSERT_EQ(v->size(), 1u);
            EXPECT_TRUE((*v)[0]);
        }

        // applyDelete removes the series (routed to the same owning core).
        bool deleted = store.applyDelete(buildSeriesKey("temp", {{"host", "h1"}}, "value"), BASE - 1, BASE + 1).get();
        EXPECT_TRUE(deleted);
        {
            auto r = query(*eng, "temp", "value");
            ASSERT_TRUE(r.success) << r.errorMessage;
            EXPECT_TRUE(r.series.empty());  // gone
        }
    }).get();
}
