// Behavioral regression tests for HTTP API edge cases:
//
// (a) A measurement name above the index's hard limit (10000 bytes) must be
//     rejected with 400 on the FIRST attempt AND every retry, and must never
//     appear in /measurements.  Historically the first attempt wrote the data,
//     poisoned the handler's known-series cache, and failed metadata indexing
//     with a 500 ("SeriesMetadata has suspiciously long strings"); the RETRY
//     then skipped metadata indexing entirely and reported success, while the
//     measurement stayed permanently missing from /measurements.
//
// (b) A batch whose single series exceeds the WAL segment threshold must be
//     rejected with 413 Payload Too Large (flat error body), not a 500.
//
// (c) String fields must be included in interval queries as LATEST-per-bucket
//     (canonical rule, see CLAUDE.md "String Fields in Queries").  They used
//     to be silently dropped from any query with an aggregationInterval.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_metadata_handler.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <seastar/util/defer.hh>
#include <string>
#include <vector>

using namespace timestar;

class ApiEdgeRegressionsTest : public ::testing::Test {
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
};

// ---------------------------------------------------------------------------
// (a) Oversized measurement name: 400 on first AND second attempt, absent
//     from /measurements, and no partial state that turns the retry into a
//     fake success.
// ---------------------------------------------------------------------------
TEST_F(ApiEdgeRegressionsTest, OversizedMeasurementNameRejectedAtomically) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpMetadataHandler metadataHandler(&eng.eng);

        const std::string longName(10001, 'm');
        const std::string body = R"({"measurement": ")" + longName +
                                 R"(", "tags": {"host": "h1"}, "fields": {"v": 1.5}, "timestamp": 1000000000})";

        // First attempt: flat 400, not a 500.
        auto rep1 = writeHandler.handleWrite(makeJsonRequest(body)).get();
        EXPECT_EQ(rep1->_status, seastar::http::reply::status_type::bad_request)
            << "first write of an oversized measurement must be a 400, got body: " << rep1->_content;
        EXPECT_NE(rep1->_content.find("error"), std::string::npos);

        // Retry of the SAME body: must fail identically — a failed validation
        // must leave no trace that makes the retry "succeed".
        auto rep2 = writeHandler.handleWrite(makeJsonRequest(body)).get();
        EXPECT_EQ(rep2->_status, seastar::http::reply::status_type::bad_request)
            << "RETRY of an oversized measurement must also be a 400 (known-series cache poisoning), got body: "
            << rep2->_content;

        // The measurement must not appear in /measurements.
        auto measReq = std::make_unique<seastar::http::request>();
        auto measRep = metadataHandler.handleMeasurements(std::move(measReq)).get();
        const std::string measBody(measRep->_content);
        EXPECT_EQ(measBody.find(longName), std::string::npos) << "oversized measurement leaked into /measurements";

        // Batch form: same 400 both times.
        const std::string batchBody = R"({"writes": [{"measurement": ")" + longName +
                                      R"(", "tags": {"host": "h1"}, "fields": {"v": 2.5}, "timestamp": 2000000000}]})";
        auto rep3 = writeHandler.handleWrite(makeJsonRequest(batchBody)).get();
        EXPECT_EQ(rep3->_status, seastar::http::reply::status_type::bad_request) << rep3->_content;
        auto rep4 = writeHandler.handleWrite(makeJsonRequest(batchBody)).get();
        EXPECT_EQ(rep4->_status, seastar::http::reply::status_type::bad_request) << rep4->_content;
    })
        .join()
        .get();
}

// Companion limits: oversized field name and oversized tag value are 400 too.
TEST_F(ApiEdgeRegressionsTest, OversizedFieldAndTagIdentifiersRejected) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);

        const std::string longField(10001, 'f');
        auto repField = writeHandler
                            .handleWrite(makeJsonRequest(R"({"measurement": "edge_m", "fields": {")" + longField +
                                                         R"(": 1.0}, "timestamp": 1000000000})"))
                            .get();
        EXPECT_EQ(repField->_status, seastar::http::reply::status_type::bad_request) << repField->_content;

        const std::string longTagValue(1001, 't');
        auto repTag =
            writeHandler
                .handleWrite(makeJsonRequest(R"({"measurement": "edge_m", "tags": {"host": ")" + longTagValue +
                                             R"("}, "fields": {"v": 1.0}, "timestamp": 1000000000})"))
                .get();
        EXPECT_EQ(repTag->_status, seastar::http::reply::status_type::bad_request) << repTag->_content;
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// (b) A batch that exceeds the WAL segment threshold (and writes nothing)
//     must return 413 with a flat error body, not a 500 and not "partial".
// ---------------------------------------------------------------------------
TEST_F(ApiEdgeRegressionsTest, OversizedBatchReturns413) {
    seastar::thread([] {
        // Pin the WAL segment threshold to 16MB for this test. The 413
        // injection needs a request whose WAL estimate EXCEEDS the segment
        // limit while its body stays under the 64MB HTTP body cap -- with the
        // default threshold now also 64MB, no such request exists, so the
        // test provides its own threshold instead of assuming the default.
        auto savedCfg = timestar::config();
        auto testCfg = savedCfg;
        testCfg.storage.wal_size_threshold = 16 * 1024 * 1024;
        timestar::setGlobalConfig(testCfg);
        auto restoreCfg = seastar::defer([&] { timestar::setGlobalConfig(savedCfg); });

        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);

        // 42 x 1MB string values in ONE series: the WAL estimator projects
        // ~16.8MB (0.40 compression ratio) — above the 16MB segment limit,
        // so the owning shard rejects the whole batch before persisting.
        constexpr int kBigValues = 42;
        const std::string bigValue(1024 * 1024, 'x');

        std::string body;
        body.reserve(static_cast<size_t>(kBigValues + 1) * (1024 * 1024 + 16));
        body += R"({"writes": [{"measurement": "edge_too_big", "tags": {"host": "h1"}, "fields": {"payload": [)";
        for (int i = 0; i < kBigValues; ++i) {
            if (i > 0)
                body += ',';
            body += '"';
            body += bigValue;
            body += '"';
        }
        body += R"(]}, "timestamps": [)";
        for (int i = 0; i < kBigValues; ++i) {
            if (i > 0)
                body += ',';
            body += std::to_string((i + 1) * 1000);
        }
        body += "]}]}";

        auto rep = writeHandler.handleWrite(makeJsonRequest(body)).get();
        EXPECT_EQ(rep->_status, seastar::http::reply::status_type::payload_too_large)
            << "an oversized batch must be a 413 client error, got status " << static_cast<int>(rep->_status)
            << " body: " << rep->_content.substr(0, 500);

        // Flat error body: {"status":"error", ..., "error":"..."}
        glz::generic parsed;
        auto ec = glz::read_json(parsed, rep->_content);
        ASSERT_FALSE(ec) << "413 body must be valid JSON: " << rep->_content.substr(0, 500);
        auto& obj = parsed.get<glz::generic::object_t>();
        ASSERT_TRUE(obj.contains("status"));
        EXPECT_EQ(obj["status"].get<std::string>(), "error");
        ASSERT_TRUE(obj.contains("error")) << "flat error body must carry an \"error\" field";
        EXPECT_NE(obj["error"].get<std::string>().find("too large"), std::string::npos);
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// (c) String fields in interval queries: included as LATEST-per-bucket with
//     bucket-start timestamps; numeric fields from the same measurement keep
//     working.  Pins the canonical rule from CLAUDE.md.
// ---------------------------------------------------------------------------
TEST_F(ApiEdgeRegressionsTest, StringFieldsIncludedInIntervalQueriesAsLatestPerBucket) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr uint64_t kSecond = 1000000000ULL;
        const uint64_t base = 1704067200ULL * kSecond;  // bucket-aligned (whole seconds)

        // 4 points, 1s apart: string field s + float field f.
        std::string body = R"({"measurement": "edge_strint", "tags": {"t": "a"}, "fields": {)";
        body += R"("s": ["a", "b", "c", "d"], "f": [1.0, 2.0, 3.0, 4.0]},)";
        body += R"("timestamps": [)" + std::to_string(base) + "," + std::to_string(base + kSecond) + "," +
                std::to_string(base + 2 * kSecond) + "," + std::to_string(base + 3 * kSecond) + "]}";
        auto wrep = writeHandler.handleWrite(makeJsonRequest(body)).get();
        ASSERT_EQ(wrep->_status, seastar::http::reply::status_type::ok) << wrep->_content;

        auto runQuery = [&](const std::string& interval) {
            auto rep =
                queryHandler.handleQuery(makeQueryRequest("latest:edge_strint()", base, base + 4 * kSecond, interval))
                    .get();
            EXPECT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
            glz::generic parsed;
            auto ec = glz::read_json(parsed, rep->_content);
            EXPECT_FALSE(ec) << rep->_content.substr(0, 500);
            return parsed;
        };

        // Helper: find a field across all series entries; returns (timestamps, values-as-generic).
        auto findField = [](glz::generic& parsed,
                            const std::string& field) -> std::optional<std::pair<std::vector<uint64_t>, glz::generic>> {
            auto& obj = parsed.get<glz::generic::object_t>();
            if (!obj.contains("series")) {
                return std::nullopt;
            }
            for (auto& seriesEntry : obj["series"].get<glz::generic::array_t>()) {
                auto& se = seriesEntry.get<glz::generic::object_t>();
                if (!se.contains("fields")) {
                    continue;
                }
                auto& fields = se["fields"].get<glz::generic::object_t>();
                if (!fields.contains(field)) {
                    continue;
                }
                auto& fd = fields[field].get<glz::generic::object_t>();
                std::vector<uint64_t> ts;
                for (auto& t : fd["timestamps"].get<glz::generic::array_t>()) {
                    ts.push_back(static_cast<uint64_t>(t.get<double>()));
                }
                return std::make_pair(std::move(ts), fd["values"]);
            }
            return std::nullopt;
        };

        // 1s buckets: one string value per bucket (identical to raw), bucket
        // starts as timestamps.
        {
            auto parsed = runQuery("1s");
            auto f = findField(parsed, "f");
            ASSERT_TRUE(f.has_value()) << "numeric field missing from interval query";
            auto s = findField(parsed, "s");
            ASSERT_TRUE(s.has_value()) << "REGRESSION: string field dropped from interval query";
            ASSERT_EQ(s->first.size(), 4u);
            std::vector<std::string> got;
            for (auto& v : s->second.get<glz::generic::array_t>()) {
                got.push_back(v.get<std::string>());
            }
            EXPECT_EQ(got, (std::vector<std::string>{"a", "b", "c", "d"}));
            for (size_t i = 0; i < 4; ++i) {
                EXPECT_EQ(s->first[i], base + i * kSecond) << "bucket-start timestamp expected";
            }
        }

        // 2s buckets: LATEST-per-bucket — {b, d} at bucket starts.
        {
            auto parsed = runQuery("2s");
            auto s = findField(parsed, "s");
            ASSERT_TRUE(s.has_value()) << "REGRESSION: string field dropped from interval query";
            ASSERT_EQ(s->first.size(), 2u) << "expected one string value per 2s bucket";
            std::vector<std::string> got;
            for (auto& v : s->second.get<glz::generic::array_t>()) {
                got.push_back(v.get<std::string>());
            }
            EXPECT_EQ(got, (std::vector<std::string>{"b", "d"})) << "LATEST value per bucket expected";
            EXPECT_EQ(s->first[0], (base / (2 * kSecond)) * (2 * kSecond));
        }

        // No interval: raw passthrough is unchanged (all 4 values).
        {
            auto rep =
                queryHandler.handleQuery(makeQueryRequest("latest:edge_strint(s)", base, base + 4 * kSecond)).get();
            ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
            glz::generic parsed;
            ASSERT_FALSE(glz::read_json(parsed, rep->_content));
            auto s = findField(parsed, "s");
            ASSERT_TRUE(s.has_value());
            EXPECT_EQ(s->first.size(), 4u) << "no-interval string passthrough must stay raw";
        }
    })
        .join()
        .get();
}
