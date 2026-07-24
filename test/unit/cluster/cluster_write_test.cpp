// Integration M2 (write path): TimeStarInsert -> WriteSeries conversion is lossless
// (seriesKey identity, type tag, timestamps/values/revisions), and end to end an
// inserts->WriteBatch->NodeWriteRouter->EngineLocalStore write is queryable exactly
// like a direct insert.
#include "../../../lib/cluster/data/node_write_router.hpp"
#include "../../../lib/cluster/integration/cluster_write.hpp"
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;
using timestar::control::ControlMap;
using timestar::raft::NodeId;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class ClusterWriteTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// An in-memory NodeTransport that never gets a remote group in the single-node map.
class NoTransport : public data::NodeTransport {
public:
    seastar::future<> forwardWriteBatch(NodeId, data::WriteBatch) override {
        return seastar::make_exception_future<>(std::runtime_error("no remote in single-node map"));
    }
    seastar::future<data::NodeQueryPartial> queryNode(NodeId, data::NodeQueryRequest) override {
        return seastar::make_exception_future<data::NodeQueryPartial>(std::runtime_error("unused"));
    }
};

ControlMap singleNodeMap() {
    ControlMap m;
    m.epoch = 1;
    for (uint16_t v = 0; v < 4096; ++v)
        m.placement[v] = {1};  // node 1 owns everything
    return m;
}
}  // namespace

TEST_F(ClusterWriteTest, InsertToWriteSeriesIsLossless) {
    TimeStarInsert<double> d("temp", "value");
    d.addTag("host", "h1");
    d.addValue(BASE, 42.5);
    d.addValue(BASE + 1, 43.0);
    d.revisions = {7, 8};

    data::WriteSeries s = cluster::writeSeriesFromInsert<double>(d);
    EXPECT_EQ(s.seriesKey, buildSeriesKey("temp", {{"host", "h1"}}, "value"));
    EXPECT_EQ(s.type, TSMValueType::Float);
    EXPECT_EQ(s.timestamps, (std::vector<uint64_t>{BASE, BASE + 1}));
    ASSERT_TRUE(std::holds_alternative<std::vector<double>>(s.values));
    EXPECT_EQ(std::get<std::vector<double>>(s.values), (std::vector<double>{42.5, 43.0}));
    EXPECT_EQ(s.revisions, (std::vector<uint64_t>{7, 8}));
    EXPECT_TRUE(s.consistent());

    // Type tags for the other three value types map correctly.
    TimeStarInsert<int64_t> i("cnt", "n");
    i.addValue(BASE, 100);
    EXPECT_EQ(cluster::writeSeriesFromInsert<int64_t>(i).type, TSMValueType::Integer);
    TimeStarInsert<bool> b("up", "state");
    b.addValue(BASE, true);
    EXPECT_EQ(cluster::writeSeriesFromInsert<bool>(b).type, TSMValueType::Boolean);
    TimeStarInsert<std::string> st("log", "msg");
    st.addValue(BASE, std::string("hi"));
    EXPECT_EQ(cluster::writeSeriesFromInsert<std::string>(st).type, TSMValueType::String);
}

TEST_F(ClusterWriteTest, InsertsThroughRouterAreQueryable) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);
        NoTransport transport;
        data::VShardDirectory dir(1, singleNodeMap());
        data::NodeWriteRouter router(dir, store, transport);

        // Build per-field inserts (as the write handler does), all value types.
        std::vector<TimeStarInsert<double>> doubles;
        {
            TimeStarInsert<double> d("temp", "value");
            d.addTag("host", "h1");
            d.addValue(BASE, 42.5);
            doubles.push_back(std::move(d));
        }
        std::vector<TimeStarInsert<std::string>> strings;
        {
            TimeStarInsert<std::string> s("log", "msg");
            s.addTag("host", "h1");
            s.addValue(BASE, std::string("a lossless message"));
            strings.push_back(std::move(s));
        }

        data::WriteBatch batch;
        cluster::appendInsertsToBatch(batch, doubles, {}, strings, {});
        ASSERT_EQ(batch.series.size(), 2u);
        router.write(std::move(batch)).get();

        http::HttpQueryHandler handler(&*eng);
        auto query = [&](const std::string& m, const std::string& f) {
            QueryRequest q;
            q.aggregation = AggregationMethod::LATEST;
            q.measurement = m;
            q.fields = {f};
            q.startTime = BASE - 1'000'000'000ULL;
            q.endTime = BASE + 1'000'000'000ULL;
            return handler.executeQuery(q).get();
        };

        auto rd = query("temp", "value");
        ASSERT_TRUE(rd.success) << rd.errorMessage;
        ASSERT_EQ(rd.series.size(), 1u);
        EXPECT_EQ(std::get<std::vector<double>>(rd.series[0].fields.at("value").second)[0], 42.5);

        auto rs = query("log", "msg");
        ASSERT_TRUE(rs.success) << rs.errorMessage;
        ASSERT_EQ(rs.series.size(), 1u);
        EXPECT_EQ(std::get<std::vector<std::string>>(rs.series[0].fields.at("msg").second)[0], "a lossless message");
    }).get();
}
