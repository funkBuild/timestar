// Regression tests for: a query spanning duplicate-timestamp data returned
// {"status":"success","series":[],"point_count":0} — an empty result reported as
// a success, indistinguishable from "this range genuinely holds no data".
//
// HOW IT HAPPENED
//
// Rewriting an already-flushed point puts the same timestamp in two TSM files.
// That defeats the aggregation pushdown, which reads block statistics and cannot
// dedup: TSM files whose series time ranges overlap force queryTsmAggregated()
// to decline (return nullopt). The fallback then calls Engine::query(), which
// MATERIALISES every raw point of the series so the merge can dedup with
// newest-file-wins. For a large series that allocation is hundreds of MB, and on
// a memory-constrained shard it throws std::bad_alloc.
//
// The fallback caught that exception and dropped the series. Both call sites
// were effectively silent — one logged at debug level, the other through
// LOG_QUERY_PATH, a macro that compiles to nothing unless TIMESTAR_LOG_INSERT/
// QUERY_PATH is defined. So the shard did ~20 s of work, produced zero points,
// and the handler reported success.
//
// Observed on a 1 GB/shard server with 20M points of which the last 10% were
// rewritten:
//   query 0..18,000,000 -> count=18000001 in     0 ms   (pushdown, no overlap)
//   query 0..18,500,000 -> EMPTY          in 19788 ms   (fallback, bad_alloc)
// Querying only the overlapped window worked and was correct, which is what made
// the failure look like a query-range quirk rather than dropped data.
//
// WHAT IS PINNED HERE
//   1. The data shape itself: duplicate timestamps across flushed files still
//      dedup correctly and last-write-wins, so a regression in the merge shows up
//      as wrong numbers rather than as a silent empty result.
//   2. The invariant that matters: a series the query could not read must FAIL
//      the query, never be omitted from a "success" response.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <fstream>
#include <seastar/core/thread.hh>
#include <sstream>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr uint64_t kMs = 1000000ULL;  // ns per ms
constexpr uint64_t kBase = 1700000000000ULL * kMs;

std::unique_ptr<seastar::http::request> jsonRequest(const std::string& body) {
    auto req = std::make_unique<seastar::http::request>();
    req->content = body;
    req->_headers["Content-Type"] = "application/json";
    return req;
}

void writeOk(HttpWriteHandler& handler, const std::string& body) {
    auto rep = handler.handleWrite(jsonRequest(body)).get();
    ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
}

std::string queryBody(const std::string& query, uint64_t startTime, uint64_t endTime, const std::string& interval) {
    std::string body = R"({"query":")" + query + R"(","startTime":)" + std::to_string(startTime) + R"(,"endTime":)" +
                       std::to_string(endTime);
    if (!interval.empty()) {
        body += R"(,"aggregationInterval":")" + interval + R"(")";
    }
    return body + "}";
}

// Returns the single aggregated value, or nullopt when the response carried no
// series at all (which is the failure this file exists to catch).
std::optional<double> singleValue(HttpQueryHandler& handler, const std::string& query, uint64_t startTime,
                                  uint64_t endTime, const std::string& interval) {
    auto rep = handler.handleQuery(jsonRequest(queryBody(query, startTime, endTime, interval))).get();
    EXPECT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

    glz::generic parsed;
    auto ec = glz::read_json(parsed, rep->_content);
    EXPECT_FALSE(ec) << rep->_content.substr(0, 400);

    auto& obj = parsed.get<glz::generic::object_t>();
    if (!obj.contains("series")) {
        return std::nullopt;
    }
    auto& arr = obj["series"].get<glz::generic::array_t>();
    if (arr.empty()) {
        return std::nullopt;
    }
    auto& se = arr[0].get<glz::generic::object_t>();
    auto& fields = se["fields"].get<glz::generic::object_t>();
    for (auto& [name, fieldObj] : fields) {
        auto& fo = fieldObj.get<glz::generic::object_t>();
        auto& vals = fo["values"].get<glz::generic::array_t>();
        if (!vals.empty()) {
            return vals[0].get<double>();
        }
    }
    return std::nullopt;
}

std::string readSourceFile(const std::string& relativePath) {
    // Tests may run from the build root or from a per-test subdirectory.
    for (const std::string& prefix : {"", "../", "../../", "../../../"}) {
        std::ifstream in(prefix + relativePath);
        if (in.good()) {
            std::ostringstream ss;
            ss << in.rdbuf();
            return ss.str();
        }
    }
    return {};
}

}  // namespace

class QueryOverlapCompletenessTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// The data shape from the incident: a point is written, then REWRITTEN at the
// same timestamp later, so both copies exist in different storage generations.
// A query spanning both the untouched and the rewritten region must return the
// deduped, newest values -- not an empty result, and not both copies.
TEST_F(QueryOverlapCompletenessTest, QuerySpanningRewrittenPointsReturnsDedupedNewestValues) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // 20 points at 1s spacing, all 1.0.
        {
            std::string ts = "[";
            std::string vs = "[";
            for (int i = 0; i < 20; ++i) {
                if (i) {
                    ts += ",";
                    vs += ",";
                }
                ts += std::to_string(kBase + static_cast<uint64_t>(i) * 1000 * kMs);
                vs += "1.0";
            }
            ts += "]";
            vs += "]";
            writeOk(writeHandler,
                    R"({"measurement":"ovl","tags":{"d":"a"},"fields":{"v":)" + vs + R"(},"timestamps":)" + ts + "}");
        }

        // Rewrite the LAST 5 of those exact timestamps with 2.0. Written after
        // the first batch, so these are a later write generation for the same
        // timestamps -- the duplicate-timestamp condition that makes the
        // aggregation pushdown decline and forces the dedup merge.
        {
            std::string ts = "[";
            std::string vs = "[";
            for (int i = 15; i < 20; ++i) {
                if (i > 15) {
                    ts += ",";
                    vs += ",";
                }
                ts += std::to_string(kBase + static_cast<uint64_t>(i) * 1000 * kMs);
                vs += "2.0";
            }
            ts += "]";
            vs += "]";
            writeOk(writeHandler,
                    R"({"measurement":"ovl","tags":{"d":"a"},"fields":{"v":)" + vs + R"(},"timestamps":)" + ts + "}");
        }

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;

        // The whole range, spanning untouched AND rewritten points. This is the
        // query that used to come back empty.
        auto count = singleValue(queryHandler, "count:ovl(v){}", start, end, "365d");
        ASSERT_TRUE(count.has_value()) << "query returned NO series for a range that demonstrably holds data -- "
                                          "the incomplete-result bug has regressed";
        EXPECT_DOUBLE_EQ(*count, 20.0) << "expected 20 distinct timestamps; got " << *count
                                       << " (25 would mean duplicates were not deduped)";

        // 15 points at 1.0 + 5 rewritten to 2.0 = 25. Proves last-write-wins
        // survived the merge rather than the stale copies winning (which would
        // give 20) or both copies counting (which would give 30).
        auto sum = singleValue(queryHandler, "sum:ovl(v){}", start, end, "365d");
        ASSERT_TRUE(sum.has_value()) << "sum query returned no series";
        EXPECT_DOUBLE_EQ(*sum, 25.0) << "last-write-wins broken across the rewritten region";

        // Same assertions restricted to the rewritten window alone: this path
        // kept working throughout the incident, so a difference between the two
        // localises a regression to the spanning case.
        auto windowSum = singleValue(queryHandler, "sum:ovl(v){}", kBase + 15000 * kMs, kBase + 19000 * kMs, "365d");
        ASSERT_TRUE(windowSum.has_value());
        EXPECT_DOUBLE_EQ(*windowSum, 10.0) << "rewritten window should be 5 points x 2.0";
    })
        .join()
        .get();
}

// The invariant that actually failed. Forcing a real std::bad_alloc needs a
// memory-constrained shard, which a unit test cannot arrange, so this pins the
// two structural properties that made the failure silent instead.
TEST_F(QueryOverlapCompletenessTest, DroppedSeriesFailsTheQueryAndIsLoggedUnconditionally) {
    const std::string src = readSourceFile("lib/http/http_query_handler.cpp");
    ASSERT_FALSE(src.empty()) << "could not read http_query_handler.cpp";

    // 1. A series that could not be read must be COUNTED, so the handler can
    //    tell an empty-because-no-data result from an empty-because-it-failed
    //    one. Previously the catch blocks just returned.
    EXPECT_NE(src.find("++droppedSeries"), std::string::npos)
        << "the fallback error path must record that a series was dropped";
    EXPECT_NE(src.find("QUERY_INCOMPLETE"), std::string::npos)
        << "a query that dropped series must fail, not report success with a partial/empty result";

    // 2. It must be logged in a NORMAL build. LOG_QUERY_PATH compiles to a no-op
    //    unless TIMESTAR_LOG_QUERY_PATH is set, so routing the drop through it
    //    meant production had no trace of it at all.
    const auto dropLog = src.find("Dropping series");
    EXPECT_NE(dropLog, std::string::npos) << "the drop must be logged";
    if (dropLog != std::string::npos) {
        // Find the start of the logging statement and confirm it is a direct
        // logger call rather than the conditionally-compiled macro.
        const auto lineStart = src.rfind('\n', dropLog);
        const auto stmtStart = src.rfind("timestar::http_log", dropLog);
        const auto macroStart = src.rfind("LOG_QUERY_PATH", dropLog);
        EXPECT_NE(stmtStart, std::string::npos);
        // The nearest preceding logging construct must be the unconditional one.
        EXPECT_TRUE(macroStart == std::string::npos || stmtStart > macroStart)
            << "drop is logged via LOG_QUERY_PATH, which is compiled out by default";
        (void)lineStart;
    }
}

// Efficiency, not just correctness: a rewritten point must not disqualify the
// WHOLE series from statistics pushdown.
//
// Both overlap gates used to be all-or-nothing — one overlapping block pair, or
// any TSM data inside the memory range, and queryTsmAggregated() declined for the
// entire series. The fallback then materialised every point to dedup, which is
// what threw std::bad_alloc on a large series. Overlap is normally localised (a
// backfill of one window), so the gates now isolate the affected sub-ranges and
// keep the zero-decode stats path for the rest.
//
// The observable consequence, and what this asserts: a series that is mostly
// untouched with a small rewritten tail answers with exactly the same numbers as
// one with no rewrites at all. On the real 20M-point repro this was the
// difference between a 19.8 s failure and a 13 ms answer.
TEST_F(QueryOverlapCompletenessTest, RewrittenTailDoesNotDisqualifyTheWholeSeries) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // Two series over identical timestamps. "clean" is never rewritten;
        // "dirty" has its last point rewritten, which is the only difference.
        auto writeSeries = [&](const char* measurement, int count, double value, int from) {
            std::string ts = "[";
            std::string vs = "[";
            for (int i = from; i < from + count; ++i) {
                if (i > from) {
                    ts += ",";
                    vs += ",";
                }
                ts += std::to_string(kBase + static_cast<uint64_t>(i) * 1000 * kMs);
                vs += std::to_string(value);
            }
            ts += "]";
            vs += "]";
            writeOk(writeHandler, std::string(R"({"measurement":")") + measurement +
                                      R"(","tags":{"d":"a"},"fields":{"v":)" + vs + R"(},"timestamps":)" + ts + "}");
        };

        writeSeries("ovl_clean", 12, 1.0, 0);
        writeSeries("ovl_dirty", 12, 1.0, 0);
        // Rewrite only the final point of the dirty series, same timestamp.
        writeSeries("ovl_dirty", 1, 5.0, 11);

        const uint64_t start = kBase - 10000 * kMs;
        const uint64_t end = kBase + 60000 * kMs;

        auto cleanCount = singleValue(queryHandler, "count:ovl_clean(v){}", start, end, "365d");
        auto dirtyCount = singleValue(queryHandler, "count:ovl_dirty(v){}", start, end, "365d");
        ASSERT_TRUE(cleanCount.has_value());
        ASSERT_TRUE(dirtyCount.has_value()) << "a rewritten tail must not make the series unqueryable";

        // Identical point counts: the rewrite replaced a point, it did not add one.
        EXPECT_DOUBLE_EQ(*cleanCount, 12.0);
        EXPECT_DOUBLE_EQ(*dirtyCount, 12.0) << "rewritten point was double-counted or the series was dropped";

        // The rewritten value wins: 11 x 1.0 + 1 x 5.0 = 16.
        auto dirtySum = singleValue(queryHandler, "sum:ovl_dirty(v){}", start, end, "365d");
        ASSERT_TRUE(dirtySum.has_value());
        EXPECT_DOUBLE_EQ(*dirtySum, 16.0) << "last-write-wins broken for the rewritten tail";

        // And the untouched region of the dirty series still agrees with clean,
        // which is the part that must keep using the pushdown.
        const uint64_t headEnd = kBase + 5000 * kMs;
        auto cleanHead = singleValue(queryHandler, "sum:ovl_clean(v){}", start, headEnd, "365d");
        auto dirtyHead = singleValue(queryHandler, "sum:ovl_dirty(v){}", start, headEnd, "365d");
        ASSERT_TRUE(cleanHead.has_value());
        ASSERT_TRUE(dirtyHead.has_value());
        EXPECT_DOUBLE_EQ(*dirtyHead, *cleanHead)
            << "the untouched part of a partially-rewritten series must aggregate identically";
    })
        .join()
        .get();
}
