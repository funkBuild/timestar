// Tests for the bounded re-read that recovers a non-numeric bucketed query whose
// single-shot read exhausted memory (detail::queryNonNumericBucketedChunked).
//
// BACKGROUND
//
// A non-numeric field queried WITH an aggregationInterval reduces to
// LATEST-per-bucket, so its RESULT is O(buckets) -- but assembly asked
// Engine::query() for the whole range first, making peak memory O(points in
// range).  On a 2-shard 1GB server a `latest:` over a multi-million-point string
// series therefore threw std::bad_alloc while the answer it was building was
// three values.  The recovery re-reads in chunks, reducing each chunk before the
// next is read, so peak memory is O(points in one chunk).
//
// WHAT IS RISKY HERE
//
// Chunk boundaries do NOT align to bucket boundaries -- they cannot, because a
// single bucket may itself be too large to materialise (a 1h bucket over 1ms
// data holds 3.6M points).  So a bucket is routinely SPLIT across chunks, and
// the per-chunk reductions must merge by bucket with later-wins rather than
// concatenate.  Get that wrong and a split bucket either duplicates (two entries
// for one bucket) or reports the wrong value (an earlier chunk's point winning
// over a later one).
//
// Production reaches this function only after an allocation failure, which a
// unit test cannot arrange.  These tests therefore call it directly and compare
// against the ordinary bucketed query over the same range -- the answer must not
// depend on which path produced it.  `initialChunkWidth` is forced small so that
// buckets are split many times over, which is exactly the case that would
// otherwise go untested.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/core/placement_table.hpp"
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

constexpr uint64_t kSec = 1000000000ULL;
constexpr uint64_t kHour = 3600ULL * kSec;
constexpr uint64_t kBase = 1700000000ULL * kSec - (1700000000ULL * kSec) % kHour;  // epoch-aligned

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

struct Reduced {
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;
};

// Pull (timestamps, string values) out of a SeriesResult.
Reduced toReduced(const timestar::SeriesResult& sr) {
    Reduced out;
    for (const auto& [name, data] : sr.fields) {
        out.timestamps = data.first;
        if (const auto* strs = std::get_if<std::vector<std::string>>(&data.second)) {
            out.values = *strs;
        }
        break;
    }
    return out;
}

// The ordinary bucketed query, used as the reference answer.
Reduced queryNormally(HttpQueryHandler& handler, const std::string& measurement, uint64_t start, uint64_t end,
                      const std::string& interval) {
    std::string body = R"({"query":"latest:)" + measurement + R"((fill_status){}","startTime":)" +
                       std::to_string(start) + R"(,"endTime":)" + std::to_string(end) + R"(,"aggregationInterval":")" +
                       interval + R"("})";
    auto rep = handler.handleQuery(jsonRequest(body)).get();
    EXPECT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;

    Reduced out;
    glz::generic parsed;
    auto ec = glz::read_json(parsed, rep->_content);
    EXPECT_FALSE(ec) << rep->_content.substr(0, 300);
    if (ec) {
        return out;
    }
    auto& obj = parsed.get<glz::generic::object_t>();
    auto& arr = obj["series"].get<glz::generic::array_t>();
    if (arr.empty()) {
        return out;
    }
    auto& fields = arr[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
    for (auto& [name, fieldObj] : fields) {
        auto& fo = fieldObj.get<glz::generic::object_t>();
        for (auto& t : fo["timestamps"].get<glz::generic::array_t>()) {
            out.timestamps.push_back(static_cast<uint64_t>(t.get<double>()));
        }
        for (auto& v : fo["values"].get<glz::generic::array_t>()) {
            out.values.push_back(v.get<std::string>());
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

// Value at index i is "filling_state_<i>", so the expected LATEST of a bucket is
// derivable from the index of its last point.
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

// Resolve the series key/id the way the query path does, so the helper can be
// called directly for one series.
struct SeriesHandle {
    std::string key;
    SeriesId128 id;
    unsigned shard = 0;
};

SeriesHandle handleFor(const std::string& measurement) {
    SeriesHandle h;
    h.key = timestar::buildSeriesKey(measurement, {{"project", "p1"}}, "fill_status");
    // fromSeriesKey, NOT fromComponents: routing must hash the same key the write
    // path hashed, or the lookup lands on a shard that holds nothing.
    h.id = SeriesId128::fromSeriesKey(h.key);
    h.shard = timestar::routeToCore(h.id);
    return h;
}

}  // namespace

class NonNumericChunkedRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// The core property: chunked and single-shot assembly must agree exactly, even
// when chunks are far narrower than a bucket so every bucket is split.
TEST_F(NonNumericChunkedRecoveryTest, ChunkedRecoveryMatchesTheSingleShotAnswer) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr size_t kCount = 3600;          // 3 hours at 3s spacing
        constexpr uint64_t kSpacing = 3 * kSec;  // => 3 one-hour buckets
        writeStringSeries(writeHandler, "ChunkRec", kCount, kSpacing);
        flushToTsm(eng.eng, 1);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount - 1) * kSpacing;

        auto expected = queryNormally(queryHandler, "ChunkRec", start, end, "1h");
        ASSERT_FALSE(expected.values.empty()) << "reference query returned nothing";

        auto h = handleFor("ChunkRec");

        // Chunk widths far below one bucket (1h), so buckets split repeatedly.
        for (uint64_t width : {kHour / 2, kHour / 7, 60 * kSec, 7 * kSec}) {
            auto recovered = eng.eng
                                 .invoke_on(h.shard,
                                            [key = h.key, id = h.id, start, end, width](Engine& engine) {
                                                return timestar::http::detail::queryNonNumericBucketedChunked(
                                                    engine, key, id, "fill_status", {{"project", "p1"}}, "ChunkRec",
                                                    start, end, kHour, width);
                                            })
                                 .get();

            ASSERT_TRUE(recovered.has_value()) << "recovery declined a string series (width=" << width << ")";
            auto got = toReduced(*recovered);

            EXPECT_EQ(got.timestamps, expected.timestamps) << "bucket timestamps differ at chunk width " << width;
            EXPECT_EQ(got.values, expected.values) << "bucket VALUES differ at chunk width " << width
                                                   << " -- a bucket split across chunks did not merge with later-wins";
        }
    })
        .join()
        .get();
}

// A bucket split across chunks must produce ONE entry, not one per chunk.
TEST_F(NonNumericChunkedRecoveryTest, SplitBucketYieldsOneEntryPerBucket) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        constexpr size_t kCount = 3600;
        constexpr uint64_t kSpacing = 3 * kSec;
        writeStringSeries(writeHandler, "ChunkSplit", kCount, kSpacing);
        flushToTsm(eng.eng, 1);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount - 1) * kSpacing;
        auto h = handleFor("ChunkSplit");

        auto recovered = eng.eng
                             .invoke_on(h.shard,
                                        [key = h.key, id = h.id, start, end](Engine& engine) {
                                            return timestar::http::detail::queryNonNumericBucketedChunked(
                                                engine, key, id, "fill_status", {{"project", "p1"}}, "ChunkSplit",
                                                start, end, kHour, 5 * kSec);  // ~720 chunks per bucket
                                        })
                             .get();

        ASSERT_TRUE(recovered.has_value());
        auto got = toReduced(*recovered);

        // Three one-hour buckets, strictly ascending, no duplicates.
        ASSERT_EQ(got.timestamps.size(), 3u) << "a split bucket emitted more than one entry";
        for (size_t i = 1; i < got.timestamps.size(); ++i) {
            EXPECT_LT(got.timestamps[i - 1], got.timestamps[i]) << "bucket timestamps not strictly ascending";
        }
        for (size_t i = 0; i < got.timestamps.size(); ++i) {
            EXPECT_EQ(got.timestamps[i] % kHour, 0u) << "bucket start is not epoch-aligned";
        }

        // LATEST of the final bucket is the last written point.
        ASSERT_EQ(got.values.size(), 3u);
        EXPECT_EQ(got.values.back(), "filling_state_" + std::to_string(kCount - 1))
            << "an earlier chunk's value won over a later one -- merge is not later-wins";
    })
        .join()
        .get();
}

// A numeric series must be declined (nullopt): those fold through
// AggregationState, and the caller reports the drop rather than using this path.
TEST_F(NonNumericChunkedRecoveryTest, NumericSeriesIsDeclined) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);

        writeOk(writeHandler,
                R"({"measurement":"ChunkNum","tags":{"project":"p1"},"fields":{"fill_status":1.5},"timestamp":)" +
                    std::to_string(kBase + 5 * kSec) + "}");
        flushToTsm(eng.eng, 1);

        auto h = handleFor("ChunkNum");
        auto recovered = eng.eng
                             .invoke_on(h.shard,
                                        [key = h.key, id = h.id](Engine& engine) {
                                            return timestar::http::detail::queryNonNumericBucketedChunked(
                                                engine, key, id, "fill_status", {{"project", "p1"}}, "ChunkNum", kBase,
                                                kBase + kHour, kHour, 60 * kSec);
                                        })
                             .get();

        EXPECT_FALSE(recovered.has_value()) << "a numeric series must be declined, not reduced as non-numeric";
    })
        .join()
        .get();
}

// An empty range is a successful recovery with nothing in it -- NOT a failure,
// which the caller would otherwise report as a dropped series.
TEST_F(NonNumericChunkedRecoveryTest, EmptyRangeRecoversWithNoFields) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);

        writeStringSeries(writeHandler, "ChunkEmpty", 100, 3 * kSec);
        flushToTsm(eng.eng, 1);

        auto h = handleFor("ChunkEmpty");
        // A window far past the data.
        const uint64_t start = kBase + 48 * kHour;
        auto recovered = eng.eng
                             .invoke_on(h.shard,
                                        [key = h.key, id = h.id, start](Engine& engine) {
                                            return timestar::http::detail::queryNonNumericBucketedChunked(
                                                engine, key, id, "fill_status", {{"project", "p1"}}, "ChunkEmpty",
                                                start, start + kHour, kHour, 60 * kSec);
                                        })
                             .get();

        ASSERT_TRUE(recovered.has_value()) << "an empty range must recover, not fail";
        EXPECT_TRUE(recovered->fields.empty()) << "an empty range must not fabricate a field";
    })
        .join()
        .get();
}
