// Integration F.3: the EngineLocalStore adapter over a REAL sharded<Engine>. A
// WriteBatch carrying all four value types (incl. a string series -- which the
// flat DataPoint could NOT represent) is applied, then read back through the real
// HTTP query pipeline, proving the enriched command is lossless end to end.
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/core/placement_table.hpp"  // virtualShard
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
        cluster::EngineLocalStore store(*eng);

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

        // queryLocal runs the real query pipeline and returns the node's UNFINALIZED
        // partials (the coordinator merges these across nodes). A string field is
        // non-numeric, so it arrives in nonNumeric (passthrough), not partials.
        {
            data::NodeQueryRequest nq;
            nq.request.aggregation = AggregationMethod::LATEST;
            nq.request.measurement = "log";  // the string series
            nq.request.fields = {"msg"};
            nq.request.startTime = BASE - 1'000'000'000ULL;
            nq.request.endTime = BASE + 1'000'000'000ULL;
            data::NodeQueryPartial partial = store.queryLocal(std::move(nq)).get();
            EXPECT_TRUE(partial.incompleteReasons.empty());
            ASSERT_EQ(partial.nonNumeric.size(), 1u);
            auto& f = partial.nonNumeric[0].fields.at("msg");
            auto* v = std::get_if<std::vector<std::string>>(&f.second);
            ASSERT_NE(v, nullptr);
            EXPECT_EQ((*v)[0], "a lossless UTF-8 message");
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

// M3 RF=3 read: queryLocal restricted to req.vshards returns ONLY series in those
// VShards, so a series replicated on every node is counted once cluster-wide (the
// coordinator asks each leader for only the VShards it leads). The correctness-
// critical anti-double-count piece.
TEST_F(EngineLocalStoreTest, QueryLocalHonorsVShardFilter) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        // Two measurements whose series land in (distinct) VShards.
        const std::string keyA = buildSeriesKey("mA", {{"h", "1"}}, "value");
        const std::string keyB = buildSeriesKey("mB", {{"h", "1"}}, "value");
        const uint16_t vsA = timestar::virtualShard(SeriesId128::fromSeriesKey(keyA));
        const uint16_t vsB = timestar::virtualShard(SeriesId128::fromSeriesKey(keyB));
        ASSERT_NE(vsA, vsB);

        data::WriteBatch b;
        b.series.push_back(floatS());  // temp/value (some vshard)
        {
            data::WriteSeries a;
            a.seriesKey = keyA;
            a.type = TSMValueType::Float;
            a.timestamps = {BASE};
            a.values = std::vector<double>{7.0};
            b.series.push_back(std::move(a));
        }
        store.applyWrites(std::move(b)).get();

        auto q = [&](const std::string& m, std::vector<uint16_t> vshards) {
            data::NodeQueryRequest nq;
            nq.request.aggregation = AggregationMethod::LATEST;
            nq.request.measurement = m;
            nq.request.fields = {"value"};
            nq.request.startTime = BASE - 1'000'000'000ULL;
            nq.request.endTime = BASE + 1'000'000'000ULL;
            nq.vshards = std::move(vshards);
            return store.queryLocal(std::move(nq)).get();
        };

        // Filter = {vsA} -> mA's series is included.
        auto inA = q("mA", {vsA});
        EXPECT_EQ(inA.partials.size() + inA.nonNumeric.size(), 1u) << "mA in-filter must be present";
        // Filter = {vsB} (NOT mA's vshard) -> mA is dropped (empty).
        auto outA = q("mA", {vsB});
        EXPECT_TRUE(outA.partials.empty() && outA.nonNumeric.empty()) << "mA out-of-filter must be excluded";
    }).get();
}

TEST_F(EngineLocalStoreTest, ReplicatedQueryRequiresExactVShardLeaderFence) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        std::vector<uint16_t> fenced;
        store.setLeaderReadFence([&fenced](const std::vector<uint16_t>& vshards) {
            fenced = vshards;
            return seastar::make_ready_future<bool>(true);
        });

        data::NodeQueryRequest req;
        req.request.aggregation = AggregationMethod::LATEST;
        req.request.measurement = "missing";
        req.request.fields = {"value"};
        req.request.startTime = BASE - 1;
        req.request.endTime = BASE + 1;
        req.vshards = {11, 22};
        auto ok = store.queryLocal(req).get();
        EXPECT_TRUE(ok.incompleteReasons.empty());
        EXPECT_EQ(fenced, (std::vector<uint16_t>{11, 22}));

        data::NodeQueryRequest unrestricted = req;
        unrestricted.vshards.clear();
        auto refused = store.queryLocal(std::move(unrestricted)).get();
        ASSERT_EQ(refused.incompleteReasons.size(), 1u);
        EXPECT_NE(refused.incompleteReasons.front().find("no VShard filter"), std::string::npos);

        store.setLeaderReadFence([](const std::vector<uint16_t>&) {
            return seastar::make_ready_future<bool>(false);
        });
        auto noQuorum = store.queryLocal(std::move(req)).get();
        ASSERT_EQ(noQuorum.incompleteReasons.size(), 1u);
        EXPECT_NE(noQuorum.incompleteReasons.front().find("quorum"), std::string::npos);
    }).get();
}

TEST_F(EngineLocalStoreTest, QueryMetadataReturnsOwnedSchema) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        data::WriteBatch b;
        b.series = {floatS(), stringS()};  // measurements "temp"/value and "log"/msg
        store.applyWrites(std::move(b)).get();

        auto meas = store.queryMetadata({data::MetadataKind::Measurements, "", "", ""}).get();
        std::sort(meas.items.begin(), meas.items.end());
        EXPECT_EQ(meas.items, (std::vector<std::string>{"log", "temp"}));

        // Fields carries "name\x1ftype" so the coordinator union preserves the type.
        auto fields = store.queryMetadata({data::MetadataKind::Fields, "temp", "", ""}).get();
        ASSERT_EQ(fields.items.size(), 1u);
        EXPECT_EQ(fields.items[0].substr(0, fields.items[0].find('\x1f')), "value");

        auto card = store.queryMetadata({data::MetadataKind::MeasurementCardinality, "temp", "", ""}).get();
        EXPECT_GE(card.cardinality, 1.0);  // at least the one series
    }).get();
}
