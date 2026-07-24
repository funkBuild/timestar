// Integration F.5b (stage 2): the produce/finalize split of the query engine.
// For a SINGLE node, executeQuery(req) must equal finalizeClusterPartials(
// queryLocalPartials(req)) for every query shape -- because the coordinator will
// build the cluster answer out of exactly those two halves. This is the oracle
// that the split preserves canonical semantics (raw passthrough, epoch bucketing,
// cross-series spread group-by, non-numeric LATEST) before any cluster wiring.
#include "../../../lib/cluster/integration/engine_local_store.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <gtest/gtest.h>

#include <seastar/core/thread.hh>

using namespace timestar;

namespace {
constexpr uint64_t BASE = 1'700'000'000'000'000'000ULL;

class QueryPartialSplitTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

data::WriteSeries fS(const std::string& m, std::map<std::string, std::string> tags, const std::string& f,
                     TSMValueType type, std::vector<uint64_t> ts,
                     std::variant<std::vector<double>, std::vector<int64_t>, std::vector<bool>, std::vector<std::string>>
                         vals) {
    data::WriteSeries s;
    s.seriesKey = buildSeriesKey(m, tags, f);
    s.type = type;
    s.timestamps = std::move(ts);
    s.values = std::move(vals);
    return s;
}

// Canonicalize a response to an order-independent, type-aware string list so two
// responses can be compared for exact equality.
std::vector<std::string> canon(const QueryResponse& r) {
    std::vector<std::string> out;
    for (const auto& s : r.series) {
        std::string tagStr;
        for (const auto& [k, v] : s.tags)
            tagStr += k + "=" + v + ",";
        for (const auto& [field, data] : s.fields) {
            std::string line = s.measurement + "|" + tagStr + "|" + field + "|";
            for (uint64_t t : data.first)
                line += std::to_string(t) + ",";
            line += "|";
            const auto& fv = data.second;
            if (auto* d = std::get_if<std::vector<double>>(&fv))
                for (double x : *d)
                    line += std::to_string(x) + ",";
            else if (auto* b = std::get_if<std::vector<bool>>(&fv))
                for (bool x : *b)
                    line += (x ? "T" : "F");
            else if (auto* st = std::get_if<std::vector<std::string>>(&fv))
                for (const auto& x : *st)
                    line += x + ",";
            else if (auto* i = std::get_if<std::vector<int64_t>>(&fv))
                for (int64_t x : *i)
                    line += std::to_string(x) + ",";
            out.push_back(std::move(line));
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

QueryRequest baseReq() {
    QueryRequest q;
    q.measurement = "m";
    q.startTime = BASE - 1'000'000'000ULL;
    q.endTime = BASE + 1'000'000'000ULL;
    return q;
}
}  // namespace

TEST_F(QueryPartialSplitTest, SplitEqualsExecuteQueryForEveryShape) {
    seastar::async([] {
        ScopedShardedEngine eng;
        eng.start();
        cluster::EngineLocalStore store(*eng);

        data::WriteBatch b;
        // Cross-series numeric data at shared + distinct timestamps for spread/avg.
        b.series.push_back(fS("m", {{"region", "west"}, {"host", "h1"}}, "v", TSMValueType::Float, {BASE, BASE + 60},
                              std::vector<double>{10.0, 12.0}));
        b.series.push_back(fS("m", {{"region", "west"}, {"host", "h2"}}, "v", TSMValueType::Float, {BASE, BASE + 60},
                              std::vector<double>{30.0, 8.0}));
        b.series.push_back(fS("m", {{"region", "east"}, {"host", "h3"}}, "v", TSMValueType::Float, {BASE},
                              std::vector<double>{5.0}));
        // A string and a bool series (non-numeric passthrough / LATEST).
        b.series.push_back(fS("m", {{"region", "west"}, {"host", "h1"}}, "msg", TSMValueType::String, {BASE},
                              std::vector<std::string>{"hello"}));
        b.series.push_back(fS("m", {{"region", "east"}, {"host", "h3"}}, "up", TSMValueType::Boolean, {BASE},
                              std::vector<bool>{true}));
        store.applyWrites(std::move(b)).get();

        http::HttpQueryHandler handler(&*eng);

        auto check = [&](QueryRequest q, const char* label) {
            QueryResponse direct = handler.executeQuery(q).get();
            auto np = handler.queryLocalPartials(q).get();
            ASSERT_TRUE(np.ok) << label << ": " << np.errorResponse.errorMessage;
            QueryResponse split =
                handler.finalizeClusterPartials(q, std::move(np.partials), std::move(np.nonNumeric)).get();
            ASSERT_TRUE(direct.success) << label << " direct: " << direct.errorMessage;
            ASSERT_TRUE(split.success) << label << " split: " << split.errorMessage;
            EXPECT_EQ(canon(split), canon(direct)) << "MISMATCH for " << label;
        };

        // Raw per-timestamp avg across series (interval 0).
        { QueryRequest q = baseReq(); q.aggregation = AggregationMethod::AVG; q.fields = {"v"}; check(q, "avg-raw"); }
        // spread group-by region (the fold-of-one-non-identity trap).
        {
            QueryRequest q = baseReq();
            q.aggregation = AggregationMethod::SPREAD;
            q.fields = {"v"};
            q.groupByTags = {"region"};
            check(q, "spread-by-region");
        }
        // Bucketed avg (epoch-aligned).
        {
            QueryRequest q = baseReq();
            q.aggregation = AggregationMethod::AVG;
            q.fields = {"v"};
            q.aggregationInterval = 100;
            check(q, "avg-bucketed");
        }
        // sum group-by region.
        {
            QueryRequest q = baseReq();
            q.aggregation = AggregationMethod::SUM;
            q.fields = {"v"};
            q.groupByTags = {"region"};
            check(q, "sum-by-region");
        }
        // Non-numeric string (LATEST) + bool, all fields.
        { QueryRequest q = baseReq(); q.aggregation = AggregationMethod::LATEST; check(q, "latest-allfields"); }
        // count.
        { QueryRequest q = baseReq(); q.aggregation = AggregationMethod::COUNT; q.fields = {"v"}; check(q, "count"); }
    }).get();
}
