// Query-semantics equivalence tests (TimeStar vs the DynamoDB timeseries engine
// it replaces).  Each test pins one rule that iot-core's read paths depend on;
// the scenarios mirror the equivalence report case-for-case.
//
// The governing rule these all follow from:
//
//     Aggregate ACROSS SERIES at equal timestamps.  Never aggregate over time.
//     Every distinct timestamp in the range survives into the result.
//
// (LATEST/FIRST are the defined exceptions — they name a single point by
// construction — and an explicit aggregationInterval is the caller opting in to
// temporal bucketing.)
//
// Covered:
//   T1  a `by` clause must not change the temporal aggregation default
//   T2  boolean fields come back as true/false, keeping their series tags
//   T3  booleans are non-numeric: aggregation methods do not coerce them
//   T4  an unknown `by` tag returns [], as an unknown scope tag already does

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr uint64_t kMs = 1000000ULL;  // nanoseconds per millisecond

// Bucket-aligned base instant (the report's 1700000000000 ms), in nanoseconds.
constexpr uint64_t kBase = 1700000000000ULL * kMs;

// One series entry of a decoded query response.  `values` stays generic so a
// test can assert the JSON type that was emitted (number vs bool vs string),
// which is the whole point of the boolean cases.
struct DecodedSeries {
    std::map<std::string, std::string> tags;
    std::string field;
    std::vector<uint64_t> timestamps;
    glz::generic values;
};

}  // namespace

class DynamoEquivalenceTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }

    static std::unique_ptr<seastar::http::request> makeJsonRequest(const std::string& body) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = body;
        req->_headers["Content-Type"] = "application/json";
        return req;
    }

    static std::unique_ptr<seastar::http::request> makeQueryRequest(const std::string& query, uint64_t startTime,
                                                                    uint64_t endTime,
                                                                    const std::string& aggregationInterval = "") {
        std::string body = R"({"query":")" + query + R"(","startTime":)" + std::to_string(startTime) +
                           R"(,"endTime":)" + std::to_string(endTime);
        if (!aggregationInterval.empty()) {
            body += R"(,"aggregationInterval":")" + aggregationInterval + R"(")";
        }
        body += "}";
        return makeJsonRequest(body);
    }

    // Run a query and decode every (series, field) pair, sorted by tags then
    // field so assertions do not depend on shard or hash ordering.
    static std::vector<DecodedSeries> runQuery(HttpQueryHandler& handler, const std::string& query, uint64_t startTime,
                                               uint64_t endTime, const std::string& interval = "") {
        auto rep = handler.handleQuery(makeQueryRequest(query, startTime, endTime, interval)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        EXPECT_FALSE(ec) << rep->_content.substr(0, 500);

        std::vector<DecodedSeries> out;
        auto& obj = parsed.get<glz::generic::object_t>();
        if (!obj.contains("series")) {
            return out;
        }
        for (auto& seriesEntry : obj["series"].get<glz::generic::array_t>()) {
            auto& se = seriesEntry.get<glz::generic::object_t>();
            // Tags travel the wire as groupTags: ["key=value", ...]; the Node
            // client re-inflates them into the tags object the report shows.
            std::map<std::string, std::string> tags;
            if (se.contains("groupTags")) {
                for (auto& t : se["groupTags"].get<glz::generic::array_t>()) {
                    const std::string kv = t.get<std::string>();
                    const auto eq = kv.find('=');
                    if (eq != std::string::npos) {
                        tags[kv.substr(0, eq)] = kv.substr(eq + 1);
                    }
                }
            }
            if (!se.contains("fields")) {
                continue;
            }
            for (auto& [fieldName, fieldData] : se["fields"].get<glz::generic::object_t>()) {
                auto& fd = fieldData.get<glz::generic::object_t>();
                DecodedSeries ds;
                ds.tags = tags;
                ds.field = fieldName;
                for (auto& t : fd["timestamps"].get<glz::generic::array_t>()) {
                    ds.timestamps.push_back(static_cast<uint64_t>(t.get<double>()));
                }
                ds.values = fd["values"];
                out.push_back(std::move(ds));
            }
        }
        std::sort(out.begin(), out.end(), [](const DecodedSeries& a, const DecodedSeries& b) {
            return std::tie(a.tags, a.field) < std::tie(b.tags, b.field);
        });
        return out;
    }

    static std::vector<double> numbers(const DecodedSeries& ds) {
        std::vector<double> out;
        for (auto& v : ds.values.get<glz::generic::array_t>()) {
            out.push_back(v.get<double>());
        }
        return out;
    }

    static std::vector<bool> bools(const DecodedSeries& ds) {
        std::vector<bool> out;
        for (auto& v : ds.values.get<glz::generic::array_t>()) {
            out.push_back(v.get<bool>());
        }
        return out;
    }

    static std::vector<std::string> strings(const DecodedSeries& ds) {
        std::vector<std::string> out;
        for (auto& v : ds.values.get<glz::generic::array_t>()) {
            out.push_back(v.get<std::string>());
        }
        return out;
    }

    static void writeOk(HttpWriteHandler& handler, const std::string& body) {
        auto rep = handler.handleWrite(makeJsonRequest(body)).get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
    }

    // Roll all memory stores over and wait for the async background conversion
    // to land at least one TSM file.
    static void flushToTsm(seastar::sharded<Engine>& eng) {
        eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
        for (int attempt = 0; attempt < 100; ++attempt) {
            size_t files =
                eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
                    .get();
            if (files >= 1) {
                return;
            }
            seastar::sleep(std::chrono::milliseconds(100)).get();
        }
        FAIL() << "TSM conversion did not produce a file within 10s";
    }
};

// ---------------------------------------------------------------------------
// T1: a `by` clause must not change the temporal aggregation default.
//
// Two series x three timestamps.  The query with `by {p}` and the query without
// it differ only in grouping, so they must agree on the time axis: three
// timestamps either way.  Adding an aggregationInterval is the only way to ask
// for temporal collapsing.
// ---------------------------------------------------------------------------
TEST_F(DynamoEquivalenceTest, GroupByDoesNotCollapseTheTimeAxis) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        const std::string ts = R"([)" + std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "," +
                               std::to_string(kBase + 2000 * kMs) + "]";
        writeOk(writeHandler, R"({"measurement":"dyn_t1","tags":{"p":"front"},)"
                              R"("fields":{"v":[10.0,20.0,30.0]},"timestamps":)" +
                                  ts + "}");
        writeOk(writeHandler, R"({"measurement":"dyn_t1","tags":{"p":"back"},)"
                              R"("fields":{"v":[100.0,200.0,300.0]},"timestamps":)" +
                                  ts + "}");

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;
        const std::vector<uint64_t> expectedTs{kBase, kBase + 1000 * kMs, kBase + 2000 * kMs};

        // A — no `by`, no interval: cross-series average at each timestamp.
        {
            auto series = runQuery(queryHandler, "avg:dyn_t1(v){}", start, end);
            ASSERT_EQ(series.size(), 1u);
            EXPECT_EQ(series[0].timestamps, expectedTs);
            EXPECT_EQ(numbers(series[0]), (std::vector<double>{55.0, 110.0, 165.0}));
        }

        // B — `by {p}`, still no interval.  Grouping is orthogonal to the time
        // axis, so every timestamp must still survive; each group holds one
        // series here, so its values pass through unaggregated.
        auto grouped = runQuery(queryHandler, "avg:dyn_t1(v){} by {p}", start, end);
        ASSERT_EQ(grouped.size(), 2u) << "expected one group per distinct value of p";
        EXPECT_EQ(grouped[0].tags["p"], "back");
        EXPECT_EQ(grouped[0].timestamps, expectedTs) << "REGRESSION: `by` collapsed the time axis";
        EXPECT_EQ(numbers(grouped[0]), (std::vector<double>{100.0, 200.0, 300.0}));
        EXPECT_EQ(grouped[1].tags["p"], "front");
        EXPECT_EQ(grouped[1].timestamps, expectedTs) << "REGRESSION: `by` collapsed the time axis";
        EXPECT_EQ(numbers(grouped[1]), (std::vector<double>{10.0, 20.0, 30.0}));

        // C — `by {p}` with a 1ms interval.  Each point already sits in its own
        // millisecond bucket, so this must agree with B exactly.  iot-core used
        // to send this interval on every query purely to force B to behave.
        auto bucketed = runQuery(queryHandler, "avg:dyn_t1(v){} by {p}", start, end, "1ms");
        ASSERT_EQ(bucketed.size(), grouped.size());
        for (size_t i = 0; i < bucketed.size(); ++i) {
            EXPECT_EQ(bucketed[i].tags, grouped[i].tags);
            EXPECT_EQ(bucketed[i].timestamps, grouped[i].timestamps)
                << "an interval that cannot merge any points must not change the result";
            EXPECT_EQ(numbers(bucketed[i]), numbers(grouped[i]));
        }
    })
        .join()
        .get();
}

// A real interval must still collapse: same data, 10s bucket, one point per
// group.  Guards against "fix T1 by ignoring aggregationInterval under by {}".
TEST_F(DynamoEquivalenceTest, GroupByStillBucketsWhenAnIntervalIsGiven) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        const std::string ts = R"([)" + std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "," +
                               std::to_string(kBase + 2000 * kMs) + "]";
        writeOk(writeHandler, R"({"measurement":"dyn_t1b","tags":{"p":"front"},)"
                              R"("fields":{"v":[10.0,20.0,30.0]},"timestamps":)" +
                                  ts + "}");
        writeOk(writeHandler, R"({"measurement":"dyn_t1b","tags":{"p":"back"},)"
                              R"("fields":{"v":[100.0,200.0,300.0]},"timestamps":)" +
                                  ts + "}");

        // All three points fall in one epoch-aligned 10s bucket.
        auto series =
            runQuery(queryHandler, "avg:dyn_t1b(v){} by {p}", kBase - 10000 * kMs, kBase + 60000 * kMs, "10s");
        ASSERT_EQ(series.size(), 2u);
        EXPECT_EQ(series[0].tags["p"], "back");
        ASSERT_EQ(series[0].timestamps.size(), 1u) << "an explicit interval must still bucket";
        EXPECT_EQ(numbers(series[0]), (std::vector<double>{200.0}));  // avg(100,200,300)
        EXPECT_EQ(series[1].tags["p"], "front");
        ASSERT_EQ(series[1].timestamps.size(), 1u);
        EXPECT_EQ(numbers(series[1]), (std::vector<double>{20.0}));  // avg(10,20,30)
    })
        .join()
        .get();
}

// Cross-series aggregation within a group: two series share a group and a
// timestamp, so the group's value at that timestamp is their average — the
// time axis still survives.
TEST_F(DynamoEquivalenceTest, GroupByAggregatesAcrossSeriesAtEqualTimestamps) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        const std::string ts = R"([)" + std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "]";
        // Two hosts in the same rack: they must fold together per timestamp.
        writeOk(writeHandler, R"({"measurement":"dyn_t1c","tags":{"rack":"r1","host":"h1"},)"
                              R"("fields":{"v":[10.0,20.0]},"timestamps":)" +
                                  ts + "}");
        writeOk(writeHandler, R"({"measurement":"dyn_t1c","tags":{"rack":"r1","host":"h2"},)"
                              R"("fields":{"v":[30.0,40.0]},"timestamps":)" +
                                  ts + "}");

        auto series = runQuery(queryHandler, "avg:dyn_t1c(v){} by {rack}", kBase - 10000 * kMs, kBase + 60000 * kMs);
        ASSERT_EQ(series.size(), 1u);
        EXPECT_EQ(series[0].tags["rack"], "r1");
        EXPECT_EQ(series[0].tags.count("host"), 0u) << "only group-by tags belong on a grouped series";
        EXPECT_EQ(series[0].timestamps, (std::vector<uint64_t>{kBase, kBase + 1000 * kMs}));
        // avg(10,30)=20 at t0; avg(20,40)=30 at t1.
        EXPECT_EQ(numbers(series[0]), (std::vector<double>{20.0, 30.0}));
    })
        .join()
        .get();
}

// Every method must agree between `by {tag}` and no-`by`, not just the ones
// where folding a single value happens to be the identity.  SPREAD/STDDEV/STDVAR
// over one value are 0 — the ungrouped fast path used to pass the raw value
// through instead (it special-cased only COUNT), so `by {p}` changed the answer
// for exactly those three while avg/min/max/sum/median were unaffected.  An
// avg-only test passes vacuously here, which is why this sweeps every method.
TEST_F(DynamoEquivalenceTest, GroupingNeverChangesTheAnswerForAnyMethod) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr uint64_t kSecond = 1000ULL * kMs;
        const std::string ts = R"([)" + std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "," +
                               std::to_string(kBase + 2 * kSecond) + "]";
        writeOk(writeHandler, R"({"measurement":"dyn_all","tags":{"p":"front"},)"
                              R"("fields":{"v":[10.0,20.0,30.0]},"timestamps":)" +
                                  ts + "}");

        const uint64_t start = kBase;
        const uint64_t end = kBase + 3 * kSecond;

        for (const std::string& method :
             {"avg", "min", "max", "sum", "count", "median", "spread", "stddev", "stdvar"}) {
            auto ungrouped = runQuery(queryHandler, method + ":dyn_all(v){}", start, end);
            auto grouped = runQuery(queryHandler, method + ":dyn_all(v){} by {p}", start, end);
            ASSERT_EQ(ungrouped.size(), 1u) << method;
            ASSERT_EQ(grouped.size(), 1u) << method;
            EXPECT_EQ(grouped[0].timestamps, ungrouped[0].timestamps) << method << ": grouping changed the time axis";
            EXPECT_EQ(numbers(grouped[0]), numbers(ungrouped[0]))
                << method << ": grouping changed the VALUES — only which series fold together may differ";
        }

        // A single series per group means each timestamp folds one value, so the
        // spread/variance of that set is 0 — not the value itself.
        for (const std::string& method : {"spread", "stddev", "stdvar"}) {
            auto series = runQuery(queryHandler, method + ":dyn_all(v){}", start, end);
            ASSERT_EQ(series.size(), 1u) << method;
            EXPECT_EQ(numbers(series[0]), (std::vector<double>{0.0, 0.0, 0.0}))
                << method << " of a single value per timestamp must be 0, not the raw value";
        }

        // With two series folding at equal timestamps the same methods produce
        // real spreads — proving the 0s above are a correct fold, not a stub.
        writeOk(writeHandler, R"({"measurement":"dyn_two","tags":{"r":"r1","h":"h1"},)"
                              R"("fields":{"v":[10.0,20.0]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "]}");
        writeOk(writeHandler, R"({"measurement":"dyn_two","tags":{"r":"r1","h":"h2"},)"
                              R"("fields":{"v":[30.0,40.0]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "]}");
        {
            auto spread = runQuery(queryHandler, "spread:dyn_two(v){} by {r}", kBase, kBase + 2 * kSecond);
            ASSERT_EQ(spread.size(), 1u);
            EXPECT_EQ(numbers(spread[0]), (std::vector<double>{20.0, 20.0}));  // |30-10|, |40-20|
            auto stddev = runQuery(queryHandler, "stddev:dyn_two(v){} by {r}", kBase, kBase + 2 * kSecond);
            ASSERT_EQ(stddev.size(), 1u);
            EXPECT_EQ(numbers(stddev[0]), (std::vector<double>{10.0, 10.0}));
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// T2/T3: booleans are non-numeric.  They come back as true/false in the type
// they were written in, they keep their series tags, and the named aggregation
// method does not coerce them into arithmetic — exactly as strings already
// behave.
// ---------------------------------------------------------------------------
TEST_F(DynamoEquivalenceTest, BooleansAreReturnedAsBooleansNotNumbers) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        writeOk(writeHandler, R"({"measurement":"dyn_t2","tags":{"d":"d1"},)"
                              R"("fields":{"active":[true,false],"label":["alpha","beta"]},)"
                              R"("timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "]}");

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;
        const std::vector<uint64_t> expectedTs{kBase, kBase + 1000 * kMs};

        // Every aggregation method must be ignored for a boolean field, on both
        // the interval and no-interval paths.
        for (const std::string& method : {"latest", "avg", "sum", "min", "max", "count", "first"}) {
            for (const std::string& interval : {std::string(""), std::string("1ms")}) {
                auto series = runQuery(queryHandler, method + ":dyn_t2(active){}", start, end, interval);
                ASSERT_EQ(series.size(), 1u) << "method=" << method << " interval=" << interval;
                EXPECT_EQ(series[0].tags["d"], "d1")
                    << "REGRESSION: boolean query dropped its series tags (method=" << method << ")";
                EXPECT_EQ(series[0].timestamps, expectedTs) << "method=" << method << " interval=" << interval;
                EXPECT_EQ(bools(series[0]), (std::vector<bool>{true, false}))
                    << "REGRESSION: boolean coerced to numeric (method=" << method << " interval=" << interval << ")";
            }
        }

        // The string field on the same measurement behaves identically — this is
        // the precedent booleans are being brought in line with.
        {
            auto series = runQuery(queryHandler, "avg:dyn_t2(label){}", start, end, "1ms");
            ASSERT_EQ(series.size(), 1u);
            EXPECT_EQ(series[0].tags["d"], "d1");
            EXPECT_EQ(strings(series[0]), (std::vector<std::string>{"alpha", "beta"}));
        }
    })
        .join()
        .get();
}

// The raw JSON must carry `true`/`false` literals, not 1/0.  Decoding via
// glz::generic would happily read a number as a bool-ish value, so assert on
// the serialized bytes.
TEST_F(DynamoEquivalenceTest, BooleanJsonEmitsTrueFalseLiterals) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        writeOk(writeHandler, R"({"measurement":"dyn_t2json","tags":{"d":"d1"},)"
                              R"("fields":{"active":[true,false]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "]}");

        auto rep = queryHandler
                       .handleQuery(makeQueryRequest("latest:dyn_t2json(active){}", kBase - 10000 * kMs,
                                                     kBase + 60000 * kMs, "1ms"))
                       .get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
        const std::string body(rep->_content);
        EXPECT_NE(body.find("[true,false]"), std::string::npos) << "expected boolean literals in JSON, got: " << body;
        EXPECT_EQ(body.find("[1,0]"), std::string::npos) << "booleans must not serialize as 1/0: " << body;
    })
        .join()
        .get();
}

// Booleans must survive a flush to TSM identically — the result may not depend
// on whether the data sits in the memory store or on disk.
TEST_F(DynamoEquivalenceTest, BooleanSemanticsArePlacementIndependent) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.startWithBackground();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        writeOk(writeHandler, R"({"measurement":"dyn_t2tsm","tags":{"d":"d1"},)"
                              R"("fields":{"active":[true,false]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + 1000 * kMs) + "]}");

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;

        auto beforeFlush = runQuery(queryHandler, "avg:dyn_t2tsm(active){}", start, end, "1ms");
        ASSERT_EQ(beforeFlush.size(), 1u);
        EXPECT_EQ(bools(beforeFlush[0]), (std::vector<bool>{true, false}));

        // Roll the memory store over into a TSM file on every shard.
        flushToTsm(eng.eng);

        auto afterFlush = runQuery(queryHandler, "avg:dyn_t2tsm(active){}", start, end, "1ms");
        ASSERT_EQ(afterFlush.size(), 1u) << "boolean series lost after flush to TSM";
        EXPECT_EQ(afterFlush[0].tags, beforeFlush[0].tags);
        EXPECT_EQ(afterFlush[0].timestamps, beforeFlush[0].timestamps);
        EXPECT_EQ(bools(afterFlush[0]), (std::vector<bool>{true, false}))
            << "boolean result changed once the data moved to TSM";
    })
        .join()
        .get();
}

// With an interval that spans several points, booleans reduce to
// LATEST-per-bucket (the string rule), not to a numeric mean.
TEST_F(DynamoEquivalenceTest, BooleansReduceToLatestPerBucket) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr uint64_t kSecond = 1000ULL * kMs;
        // 4 points, 1s apart: [true, false, false, true].
        writeOk(writeHandler, R"({"measurement":"dyn_t3bucket","tags":{"d":"d1"},)"
                              R"("fields":{"active":[true,false,false,true]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "," +
                                  std::to_string(kBase + 2 * kSecond) + "," + std::to_string(kBase + 3 * kSecond) +
                                  "]}");

        // 2s buckets: LATEST per bucket => [false, true] at bucket starts.
        auto series = runQuery(queryHandler, "avg:dyn_t3bucket(active){}", kBase, kBase + 4 * kSecond, "2s");
        ASSERT_EQ(series.size(), 1u);
        ASSERT_EQ(series[0].timestamps.size(), 2u) << "expected one boolean per 2s bucket";
        EXPECT_EQ(bools(series[0]), (std::vector<bool>{false, true})) << "LATEST value per bucket expected";
        EXPECT_EQ(series[0].timestamps[0], (kBase / (2 * kSecond)) * (2 * kSecond))
            << "bucket-start timestamp expected";
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// An aggregationInterval that covers the whole range must still be honoured.
//
// Not one of the reported cases — found while fixing T2/T3.  LATEST/FIRST whose
// interval covered the range had that interval dropped to reach the sparse
// fast path, which silently changed the answer: non-numeric fields reverted
// from LATEST-per-bucket (one value) to raw passthrough (every value), and
// numeric points came back stamped with their raw timestamp instead of the
// bucket start.  The fast path is still taken (the range is one bucket here);
// it just no longer leaks into the result.
// ---------------------------------------------------------------------------
TEST_F(DynamoEquivalenceTest, WholeRangeIntervalIsHonouredForEveryMethodAndType) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr uint64_t kSecond = 1000ULL * kMs;
        constexpr uint64_t kHour = 3600ULL * kSecond;
        writeOk(writeHandler, R"({"measurement":"dyn_wr","tags":{"t":"a"},)"
                              R"("fields":{"b":[true,false,true,true],"s":["a","b","c","d"],)"
                              R"("f":[1.0,2.0,3.0,4.0]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "," +
                                  std::to_string(kBase + 2 * kSecond) + "," + std::to_string(kBase + 3 * kSecond) +
                                  "]}");

        // A 1h bucket swallows the whole 4s range. kBase is not 1h-aligned, so
        // the bucket start is well before it — that is the timestamp every
        // field must carry.
        const uint64_t bucketStart = (kBase / kHour) * kHour;
        ASSERT_LT(bucketStart, kBase) << "test needs a range that is NOT bucket-aligned";

        for (const std::string& method : {"latest", "first", "avg", "min", "max"}) {
            auto series = runQuery(queryHandler, method + ":dyn_wr()", kBase, kBase + 4 * kSecond, "1h");
            std::string got;
            for (const auto& d : series)
                got += d.field + " ";
            ASSERT_EQ(series.size(), 3u) << "method=" << method
                                         << ": expected one entry per field (b, s, f), got: " << got;

            for (const auto& ds : series) {
                ASSERT_EQ(ds.timestamps.size(), 1u) << "method=" << method << " field=" << ds.field
                                                    << ": a whole-range interval must collapse to exactly one bucket";
                EXPECT_EQ(ds.timestamps[0], bucketStart)
                    << "method=" << method << " field=" << ds.field << ": expected the bucket start, not a raw ts";
            }

            // Non-numeric: method ignored, LATEST-per-bucket, written type kept.
            const auto& boolSeries =
                *std::find_if(series.begin(), series.end(), [](const DecodedSeries& d) { return d.field == "b"; });
            EXPECT_EQ(bools(boolSeries), (std::vector<bool>{true})) << "method=" << method;
            const auto& strSeries =
                *std::find_if(series.begin(), series.end(), [](const DecodedSeries& d) { return d.field == "s"; });
            EXPECT_EQ(strings(strSeries), (std::vector<std::string>{"d"})) << "method=" << method;
        }

        // Numeric still aggregates per the named method within that bucket.
        auto fieldOf = [&](const std::string& method) {
            auto series = runQuery(queryHandler, method + ":dyn_wr(f)", kBase, kBase + 4 * kSecond, "1h");
            EXPECT_EQ(series.size(), 1u);
            return numbers(series[0]);
        };
        EXPECT_EQ(fieldOf("latest"), (std::vector<double>{4.0}));
        EXPECT_EQ(fieldOf("first"), (std::vector<double>{1.0}));
        EXPECT_EQ(fieldOf("avg"), (std::vector<double>{2.5}));
    })
        .join()
        .get();
}

// A numeric field must survive alongside a non-numeric one on the batch
// LATEST/FIRST path.  Not a reported case: the batch path resolves the numeric
// series and hands the leftover non-numeric ones to the streaming path, whose
// result used to be ASSIGNED over the batch partials rather than appended —
// silently dropping every numeric field from the response.
//
// The bug only bites when a numeric and a non-numeric series land on the SAME
// shard (a shard whose contexts are fully resolved by the batch path never
// reaches the streaming call).  Series are routed by a hash of
// measurement+tags+field, so field names are chosen here to force the
// collision rather than left to luck — otherwise this test passes or fails
// depending on the shard count.
TEST_F(DynamoEquivalenceTest, NumericFieldsSurviveAlongsideNonNumericOnBatchLatest) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        const std::string measurement = "dyn_mix";
        const std::map<std::string, std::string> tags{{"p", "front"}};
        auto shardOf = [&](const std::string& field) {
            return timestar::routeToCore(SeriesId128::fromSeriesKey(buildSeriesKey(measurement, tags, field)));
        };

        // Pin the string field, then find a numeric field name on its shard.
        const std::string strField = "s";
        const unsigned targetShard = shardOf(strField);
        std::string numField;
        for (int i = 0; i < 4096 && numField.empty(); ++i) {
            const std::string candidate = "f" + std::to_string(i);
            if (shardOf(candidate) == targetShard) {
                numField = candidate;
            }
        }
        ASSERT_FALSE(numField.empty()) << "no numeric field name collides with '" << strField << "' on shard "
                                       << targetShard;

        constexpr uint64_t kSecond = 1000ULL * kMs;
        writeOk(writeHandler, R"({"measurement":")" + measurement + R"(","tags":{"p":"front"},"fields":{")" + numField +
                                  R"(":[1.0,2.0],")" + strField + R"(":["a","b"]},"timestamps":[)" +
                                  std::to_string(kBase) + "," + std::to_string(kBase + kSecond) + "]}");

        // No interval + LATEST => batch fast path resolves the numeric series
        // and leaves the string one for the streaming path, which the `by`
        // clause engages.  That is exactly where the overwrite happened.
        for (const std::string& query : {"latest:" + measurement + "() by {p}", "latest:" + measurement + "()"}) {
            auto series = runQuery(queryHandler, query, kBase - kSecond, kBase + 10 * kSecond);
            std::string got;
            for (const auto& d : series)
                got += d.field + " ";
            EXPECT_EQ(series.size(), 2u) << query << ": expected both fields — got: " << got;

            auto numeric =
                std::find_if(series.begin(), series.end(), [&](const DecodedSeries& d) { return d.field == numField; });
            ASSERT_NE(numeric, series.end())
                << query << ": numeric field '" << numField << "' (shard " << targetShard
                << ") was dropped by the non-numeric series sharing its shard — got: " << got;
            EXPECT_EQ(numbers(*numeric), (std::vector<double>{2.0})) << "latest of " << numField;
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// T4: an unknown `by` tag returns [], matching the scope path.  Silently
// ignoring it answered `by {devicId}` with a fleet-wide aggregate.
// ---------------------------------------------------------------------------
TEST_F(DynamoEquivalenceTest, UnknownGroupByTagReturnsNoSeries) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        writeOk(writeHandler, R"({"measurement":"dyn_t4","tags":{"p":"front"},)"
                              R"("fields":{"v":10.0},"timestamp":)" +
                                  std::to_string(kBase) + "}");
        writeOk(writeHandler, R"({"measurement":"dyn_t4","tags":{"p":"back"},)"
                              R"("fields":{"v":100.0},"timestamp":)" +
                                  std::to_string(kBase) + "}");

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;

        // A known tag groups as expected.
        {
            auto series = runQuery(queryHandler, "avg:dyn_t4(v){} by {p}", start, end, "1ms");
            ASSERT_EQ(series.size(), 2u);
            EXPECT_EQ(series[0].tags["p"], "back");
            EXPECT_EQ(numbers(series[0]), (std::vector<double>{100.0}));
            EXPECT_EQ(series[1].tags["p"], "front");
            EXPECT_EQ(numbers(series[1]), (std::vector<double>{10.0}));
        }

        // An unknown grouping key resolves to no tag values, hence no series —
        // NOT a silent fleet-wide avg(10,100)=55.
        for (const std::string& interval : {std::string(""), std::string("1ms")}) {
            auto series = runQuery(queryHandler, "avg:dyn_t4(v){} by {nosuchtag}", start, end, interval);
            EXPECT_TRUE(series.empty()) << "REGRESSION: unknown `by` tag was ignored and answered as ungrouped "
                                        << "(interval='" << interval << "')";
        }

        // A partially-unknown grouping key set is still unknown.
        {
            auto series = runQuery(queryHandler, "avg:dyn_t4(v){} by {p,nosuchtag}", start, end, "1ms");
            EXPECT_TRUE(series.empty()) << "REGRESSION: `by {p,nosuchtag}` silently degraded to `by {p}`";
        }

        // The scope path already behaves this way — pin it so the two stay
        // consistent.
        {
            auto series = runQuery(queryHandler, "avg:dyn_t4(v){nosuchtag:x}", start, end, "1ms");
            EXPECT_TRUE(series.empty()) << "unknown scope tag must match no series";
        }
    })
        .join()
        .get();
}
