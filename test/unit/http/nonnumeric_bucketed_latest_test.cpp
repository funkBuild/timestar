// Regression tests for two failures on the bucketed non-numeric read path,
// both found while reproducing a reported query-side std::bad_alloc.
//
// Observed on a 2-shard, 1 GB server holding one string series of 8M points
// (measurement BaseStats, field fill_status) that had been flushed to TSM:
//
//   1. SILENT WRONG VALUES.  A bucketed `latest:` over a multi-bucket range
//      returned HTTP 200 with the correct bucket TIMESTAMPS and empty string
//      VALUES:
//         {"timestamps":[1699999200e9,1700002800e9,1700006400e9],
//          "values":["","",""]}
//      The same series queried over a narrow range returned the real values, so
//      decoding was fine -- the answer depended on the query range, i.e. on
//      which plan was chosen.  This is worse than data that goes missing: the
//      caller cannot tell a wrong answer from a right one.
//
//   2. SEGFAULT.  The same query shape over a range that falls inside a SINGLE
//      epoch bucket killed the shard outright
//      ("Segmentation fault on shard 0, in scheduling group main").
//
// Both reproduced on unpatched code, so neither is a side effect of the
// bad_alloc work they were found alongside.
//
// WHAT IS PINNED HERE
//   A non-numeric (string/boolean) series must come back in its written type
//   with its written values on EVERY plan -- single-bucket and multi-bucket,
//   memory-resident and TSM-resident.  Per CLAUDE.md the numeric fast paths
//   (sparse-index LATEST/FIRST resolution, aggregation pushdown) must refuse
//   these types; a gate that fails open hands back a default-constructed value
//   for a type it cannot represent, which is exactly symptom 1.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr uint64_t kSec = 1000000000ULL;  // ns per second
constexpr uint64_t kHour = 3600ULL * kSec;
// Epoch-ALIGNED base, so bucket boundaries are predictable in the assertions.
constexpr uint64_t kBase = 1700000000ULL * kSec - (1700000000ULL * kSec) % kHour;

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

struct StringSeries {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;
};

// Extracts the first series' first field as (timestamps, string values).
// Fails the test if any value is not a JSON string -- a numeric value here
// means a non-numeric series was folded through a numeric path.
StringSeries queryStrings(HttpQueryHandler& handler, const std::string& query, uint64_t startTime, uint64_t endTime,
                          const std::string& interval) {
    auto rep = handler.handleQuery(jsonRequest(queryBody(query, startTime, endTime, interval))).get();
    EXPECT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

    StringSeries out;
    glz::generic parsed;
    auto ec = glz::read_json(parsed, rep->_content);
    EXPECT_FALSE(ec) << rep->_content.substr(0, 400);
    if (ec) {
        return out;
    }

    auto& obj = parsed.get<glz::generic::object_t>();
    if (!obj.contains("series")) {
        return out;
    }
    auto& arr = obj["series"].get<glz::generic::array_t>();
    if (arr.empty()) {
        return out;
    }
    auto& se = arr[0].get<glz::generic::object_t>();
    auto& fields = se["fields"].get<glz::generic::object_t>();
    for (auto& [name, fieldObj] : fields) {
        auto& fo = fieldObj.get<glz::generic::object_t>();
        auto& ts = fo["timestamps"].get<glz::generic::array_t>();
        auto& vals = fo["values"].get<glz::generic::array_t>();
        for (auto& t : ts) {
            out.timestamps.push_back(static_cast<uint64_t>(t.get<double>()));
        }
        for (auto& v : vals) {
            EXPECT_TRUE(v.holds<std::string>())
                << "non-numeric series came back as a number: " << rep->_content.substr(0, 300);
            if (v.holds<std::string>()) {
                out.values.push_back(v.get<std::string>());
            }
        }
        break;
    }
    return out;
}

void flushToTsm(seastar::sharded<Engine>& eng, size_t minFiles) {
    eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int attempt = 0; attempt < 100; ++attempt) {
        size_t files =
            eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
                .get();
        if (files >= minFiles) {
            return;
        }
        seastar::sleep(std::chrono::milliseconds(100)).get();
    }
    FAIL() << "TSM conversion did not produce " << minFiles << " file(s) within 10s";
}

// Writes `count` string points at `spacing` starting at kBase, in batches.
// Value at index i is "filling_state_<i>", so the expected LATEST of any
// bucket is derivable from the bucket's last index.
void writeStringSeries(HttpWriteHandler& handler, const std::string& measurement, size_t count, uint64_t spacing) {
    constexpr size_t kBatch = 500;
    for (size_t start = 0; start < count; start += kBatch) {
        const size_t end = std::min(count, start + kBatch);
        std::string ts = "[";
        std::string vs = "[";
        for (size_t i = start; i < end; ++i) {
            if (i > start) {
                ts += ",";
                vs += ",";
            }
            ts += std::to_string(kBase + static_cast<uint64_t>(i) * spacing);
            vs += "\"filling_state_" + std::to_string(i) + "\"";
        }
        ts += "]";
        vs += "]";
        writeOk(handler, R"({"measurement":")" + measurement + R"(","tags":{"project":"p1"},"fields":{"fill_status":)" +
                             vs + R"(},"field_types":{"fill_status":"string"},"timestamps":)" + ts + "}");
    }
}

}  // namespace

class NonNumericBucketedLatestTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// Symptom 1: multi-bucket range over a TSM-resident string series returned
// correct timestamps with empty values.
TEST_F(NonNumericBucketedLatestTest, MultiBucketLatestOnTsmStringSeriesReturnsWrittenValues) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // ~3 hours at 3s spacing => 3,600 points spanning 3 epoch buckets.
        constexpr size_t kCount = 3600;
        constexpr uint64_t kSpacing = 3 * kSec;
        writeStringSeries(writeHandler, "BaseStatsMulti", kCount, kSpacing);
        flushToTsm(eng.eng, 1);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount - 1) * kSpacing;

        auto got = queryStrings(queryHandler, "latest:BaseStatsMulti(fill_status){}", start, end, "1h");

        ASSERT_FALSE(got.values.empty()) << "bucketed latest returned no values for a series that has data";
        ASSERT_EQ(got.values.size(), got.timestamps.size());

        for (size_t i = 0; i < got.values.size(); ++i) {
            EXPECT_FALSE(got.values[i].empty())
                << "bucket " << i << " (ts=" << got.timestamps[i]
                << ") came back as an EMPTY string -- a numeric fast path produced a default-constructed "
                   "value for a string series";
            EXPECT_EQ(got.values[i].rfind("filling_state_", 0), 0u)
                << "bucket " << i << " value '" << got.values[i] << "' was never written";
        }

        // The last bucket's LATEST must be the final written point.
        EXPECT_EQ(got.values.back(), "filling_state_" + std::to_string(kCount - 1));
    })
        .join()
        .get();
}

// Symptom 2: a range lying inside ONE epoch bucket segfaulted the shard.
// Reaching the assertions at all is most of the value here.
TEST_F(NonNumericBucketedLatestTest, SingleBucketLatestOnTsmStringSeriesDoesNotCrashAndReturnsWrittenValue) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        // 30 minutes at 3s spacing => 600 points, all inside one 1h bucket.
        constexpr size_t kCount = 600;
        constexpr uint64_t kSpacing = 3 * kSec;
        writeStringSeries(writeHandler, "BaseStatsSingle", kCount, kSpacing);
        flushToTsm(eng.eng, 1);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount - 1) * kSpacing;
        ASSERT_EQ(start / kHour, end / kHour) << "test setup: range must fall inside one bucket";

        auto got = queryStrings(queryHandler, "latest:BaseStatsSingle(fill_status){}", start, end, "1h");

        ASSERT_EQ(got.values.size(), 1u) << "single-bucket latest must return exactly one point";
        EXPECT_FALSE(got.values[0].empty()) << "single-bucket latest returned an EMPTY string";
        EXPECT_EQ(got.values[0], "filling_state_" + std::to_string(kCount - 1));
    })
        .join()
        .get();
}

// The memory-resident path is the control: it was always correct, and it defines
// the answer the TSM path must agree with.  Placement must not change the value.
TEST_F(NonNumericBucketedLatestTest, MemoryAndTsmResidentStringSeriesAgree) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();

        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr size_t kCount = 3600;
        constexpr uint64_t kSpacing = 3 * kSec;
        writeStringSeries(writeHandler, "BaseStatsPlacement", kCount, kSpacing);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount - 1) * kSpacing;

        auto beforeFlush = queryStrings(queryHandler, "latest:BaseStatsPlacement(fill_status){}", start, end, "1h");
        flushToTsm(eng.eng, 1);
        auto afterFlush = queryStrings(queryHandler, "latest:BaseStatsPlacement(fill_status){}", start, end, "1h");

        EXPECT_EQ(beforeFlush.timestamps, afterFlush.timestamps)
            << "bucket timestamps changed when the data moved from memory to TSM";
        EXPECT_EQ(beforeFlush.values, afterFlush.values)
            << "bucketed latest VALUES changed when the data moved from memory to TSM -- the answer "
               "depends on data placement";
    })
        .join()
        .get();
}
