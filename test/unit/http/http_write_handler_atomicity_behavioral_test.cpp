// Behavioral regression tests for HTTP write handler batch atomicity and
// failure accounting, driven through the public handler API
// (HttpWriteHandler::handleWrite) against a real sharded Engine.
//
// Bugs guarded (formerly asserted via source inspection in
// http_write_handler_atomicity_test.cpp):
//
// Bug 1: Batch write failures must decrement pointsWritten. Historically,
//   per-point insert failures were caught and logged but pointsWritten was
//   never decremented, so the response reported more points written than were
//   actually persisted (silent data loss). Failure attribution is per shard
//   today, so the invariant is: points_written + failed_writes == total
//   points in the request, with status "partial" whenever failed_writes > 0.
//
// Bug 3: The single-write fast path must not swallow batch writes whose
//   "writes" key appears after byte 64 of the body. The observable contract:
//   a batch body is processed as a batch — every point in the array is
//   written and queryable — regardless of where the "writes" key sits.
//
// Structural properties that cannot be observed behaviorally (int64_t
// pointsWritten declarations, data-before-metadata ordering, the npos scan
// condition itself) remain in the source-inspection file.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"
#include "../../../lib/core/series_id.hpp"
#include "../../../lib/core/timestar_value.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/utils/series_key.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>
#include <string>

class HttpWriteHandlerAtomicityBehavioralTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }

    static std::unique_ptr<seastar::http::request> makeWriteRequest(const std::string& body) {
        auto req = std::make_unique<seastar::http::request>();
        req->content = body;
        req->_headers["Content-Type"] = "application/json";
        return req;
    }

    // Sum of points visible for a series across all shards (routing-agnostic).
    static size_t totalPoints(seastar::sharded<Engine>& eng, const std::string& measurement,
                              const std::map<std::string, std::string>& tags, const std::string& field) {
        std::string seriesKey = timestar::buildSeriesKey(measurement, tags, field);
        size_t total = 0;
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            auto resultOpt =
                eng.invoke_on(s, [seriesKey](Engine& engine) { return engine.query(seriesKey, 0, UINT64_MAX); }).get();
            if (resultOpt.has_value()) {
                total += std::get<QueryResult<double>>(resultOpt.value()).timestamps.size();
            }
        }
        return total;
    }

    struct ParsedWriteResponse {
        std::string status;
        double pointsWritten = -1;
        double failedWrites = 0;
    };

    static ParsedWriteResponse parseResponse(const std::string& content) {
        ParsedWriteResponse out;
        glz::generic parsed;
        auto ec = glz::read_json(parsed, content);
        EXPECT_FALSE(ec) << "Failed to parse write response: " << content;
        if (ec) {
            return out;
        }
        auto& obj = parsed.get<glz::generic::object_t>();
        if (obj.contains("status")) {
            out.status = obj["status"].get<std::string>();
        }
        if (obj.contains("points_written")) {
            out.pointsWritten = obj["points_written"].get<double>();
        }
        if (obj.contains("failed_writes")) {
            out.failedWrites = obj["failed_writes"].get<double>();
        }
        return out;
    }
};

// ---------------------------------------------------------------------------
// Bug 1: A batch in which one shard's insert fails must report a "partial"
// response whose accounting is consistent:
//     points_written + failed_writes == total points in the request
// If the failure path stopped decrementing pointsWritten, points_written
// stays at the full count while failed_writes > 0, breaking the invariant
// (and over-reporting persisted data to the client).
//
// Failure injection: a single series whose WAL-estimated batch size exceeds
// the 16MB WAL threshold. Engine::insertBatch on the owning shard throws
// "Insert batch too large" before persisting anything, which is exactly the
// per-shard failure the accounting must attribute.
// ---------------------------------------------------------------------------
TEST_F(HttpWriteHandlerAtomicityBehavioralTest, ShardFailureAccountingIsConsistent) {
    // The test needs at least one shard that is NOT the failing one, so the
    // small series can succeed and the response is "partial" (with a single
    // shard, every point rides the failing batch and the server correctly
    // answers 413 instead).
    if (seastar::smp::count < 2) {
        GTEST_SKIP() << "requires >= 2 shards to place healthy series off the failing shard";
    }

    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // 42 string values of 1MB each => 42MB raw. The WAL batch estimator
        // applies its initial string compression ratio of 0.40, giving a
        // ~16.8MB estimate for one series — above the 16MB WAL threshold, so
        // the owning shard's Engine::insertBatch throws before persisting.
        constexpr int kBigValues = 42;
        const std::string bigValue(1024 * 1024, 'x');

        std::map<std::string, std::string> smallTags{{"host", "h1"}};

        // Pick small-series measurements that provably route to a DIFFERENT
        // shard than atomic_big. Series placement is hash-based, so with a
        // small shard count (CI runners) fixed names can all collide onto the
        // failing shard — turning the intended "partial" into an all-points
        // 413 and making the test layout-dependent.
        const unsigned bigShard = timestar::routeToCore(
            SeriesId128::fromSeriesKey(timestar::buildSeriesKey("atomic_big", smallTags, "payload")));
        std::vector<std::string> smallMeasurements;
        for (int candidate = 0; smallMeasurements.size() < 3; ++candidate) {
            ASSERT_LT(candidate, 4096) << "could not find off-shard small series names";
            std::string name = "atomic_small" + std::to_string(candidate);
            const unsigned shard =
                timestar::routeToCore(SeriesId128::fromSeriesKey(timestar::buildSeriesKey(name, smallTags, "v")));
            if (shard != bigShard) {
                smallMeasurements.push_back(std::move(name));
            }
        }

        std::string body;
        body.reserve((kBigValues + 1) * (1024 * 1024 + 16));
        body += R"({"writes": [{"measurement": "atomic_big", "tags": {"host": "h1"}, "fields": {"payload": [)";
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
        body += "]}";
        // Three small, valid double points in separate series on healthy shards.
        for (const auto& name : smallMeasurements) {
            body += R"(, {"measurement": ")" + name +
                    R"(", "tags": {"host": "h1"}, "fields": {"v": 1.5}, "timestamp": 1000})";
        }
        body += "]}";

        constexpr double kTotalPoints = kBigValues + 3;  // 42 big + 3 small

        auto rep = handler.handleWrite(makeWriteRequest(body)).get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

        auto resp = HttpWriteHandlerAtomicityBehavioralTest::parseResponse(rep->_content);

        // The oversized shard batch must have failed.
        EXPECT_EQ(resp.status, "partial") << "A batch with a failed shard insert must return status \"partial\", "
                                          << "not \"" << resp.status << "\"";
        EXPECT_GE(resp.failedWrites, static_cast<double>(kBigValues))
            << "All points routed to the failing shard must be counted as failed";

        // THE accounting invariant (Bug 1): failed points must be subtracted
        // from points_written.
        EXPECT_EQ(resp.pointsWritten + resp.failedWrites, kTotalPoints)
            << "BUG: batch failure accounting is inconsistent — failed shard points "
               "were not decremented from points_written (silent data loss reporting)";
        EXPECT_LE(resp.pointsWritten, 3.0) << "points_written must not include the failed oversized series";

        // The oversized series must not have been persisted anywhere.
        std::map<std::string, std::string> tags{{"host", "h1"}};
        std::string bigKey = timestar::buildSeriesKey("atomic_big", tags, "payload");
        for (unsigned s = 0; s < seastar::smp::count; ++s) {
            auto resultOpt =
                eng.eng.invoke_on(s, [bigKey](Engine& engine) { return engine.query(bigKey, 0, UINT64_MAX); }).get();
            if (resultOpt.has_value()) {
                EXPECT_EQ(std::get<QueryResult<std::string>>(resultOpt.value()).timestamps.size(), 0u)
                    << "Failed batch data must not be persisted (shard " << s << ")";
            }
        }

        // The healthy-shard series must have been persisted despite the
        // failing shard ("partial" means the rest of the batch succeeded).
        for (const auto& name : smallMeasurements) {
            EXPECT_EQ(HttpWriteHandlerAtomicityBehavioralTest::totalPoints(eng.eng, name, smallTags, "v"), 1u)
                << "Small series '" << name << "' on a healthy shard must be written";
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// Bug 1 (companion): an unparseable batch entry yields a partial response
// with the valid points still written and queryable — never a blanket
// "success" that hides the dropped entry.
// ---------------------------------------------------------------------------
TEST_F(HttpWriteHandlerAtomicityBehavioralTest, InvalidBatchEntryYieldsPartialAndKeepsValidPoints) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // Middle entry has an invalid measurement name (commas are reserved
        // series-key separators), so it is rejected at parse/validation time.
        std::string body = R"({"writes": [
            {"measurement": "atomic_ok_a", "tags": {"host": "h1"}, "fields": {"v": 1.0}, "timestamp": 1000},
            {"measurement": "bad,name", "tags": {"host": "h1"}, "fields": {"v": 2.0}, "timestamp": 2000},
            {"measurement": "atomic_ok_b", "tags": {"host": "h1"}, "fields": {"v": 3.0}, "timestamp": 3000}
        ]})";

        auto rep = handler.handleWrite(makeWriteRequest(body)).get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

        auto resp = HttpWriteHandlerAtomicityBehavioralTest::parseResponse(rep->_content);
        EXPECT_EQ(resp.status, "partial") << "A batch with a rejected entry must not report blanket success";
        EXPECT_EQ(resp.failedWrites, 1.0);
        EXPECT_EQ(resp.pointsWritten, 2.0);

        // Both valid points must be persisted and queryable.
        std::map<std::string, std::string> tags{{"host", "h1"}};
        EXPECT_EQ(HttpWriteHandlerAtomicityBehavioralTest::totalPoints(eng.eng, "atomic_ok_a", tags, "v"), 1u);
        EXPECT_EQ(HttpWriteHandlerAtomicityBehavioralTest::totalPoints(eng.eng, "atomic_ok_b", tags, "v"), 1u);
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// Bug 3: a batch body whose "writes" key appears after byte 64 must still be
// processed as a batch — every point written and queryable. The historical
// bug limited the fast-path's batch detection scan to the first 64 bytes and
// silently dropped the entire "writes" array when the key appeared later.
// ---------------------------------------------------------------------------
TEST_F(HttpWriteHandlerAtomicityBehavioralTest, LateWritesKeyBatchIsFullyProcessed) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler handler(&eng.eng);

        // 100 bytes of leading whitespace push the "writes" key past byte 64
        // (valid JSON). The body is also >256 bytes so the fast-path
        // pre-screening in handleWrite is actually exercised.
        std::string body = "{" + std::string(100, ' ') + R"("writes": [
            {"measurement": "late_writes_m", "tags": {"host": "h1"}, "fields": {"v": 1.0}, "timestamp": 1000},
            {"measurement": "late_writes_m", "tags": {"host": "h2"}, "fields": {"v": 2.0}, "timestamp": 2000},
            {"measurement": "late_writes_m", "tags": {"host": "h3"}, "fields": {"v": 3.0}, "timestamp": 3000}
        ]})";
        ASSERT_GT(body.size(), 256u);
        ASSERT_GT(body.find("\"writes\""), 64u);

        auto rep = handler.handleWrite(makeWriteRequest(body)).get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

        auto resp = HttpWriteHandlerAtomicityBehavioralTest::parseResponse(rep->_content);
        EXPECT_EQ(resp.status, "success") << rep->_content;
        EXPECT_EQ(resp.pointsWritten, 3.0)
            << "BUG: batch with late \"writes\" key was not processed as a batch — points were dropped";

        // Every point of the array must actually be queryable.
        for (int i = 1; i <= 3; ++i) {
            std::map<std::string, std::string> tags{{"host", "h" + std::to_string(i)}};
            EXPECT_EQ(HttpWriteHandlerAtomicityBehavioralTest::totalPoints(eng.eng, "late_writes_m", tags, "v"), 1u)
                << "Point " << i << " of the late-\"writes\" batch was dropped";
        }
    })
        .join()
        .get();
}
