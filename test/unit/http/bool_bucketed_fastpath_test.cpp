// Bucketed queries over BOOLEAN series must give the same LATEST-per-bucket
// answer on EVERY plan and EVERY data placement.
//
// WHY THIS TEST EXISTS
//
// The Jul 22 2026 incident fix routes bool series with an aggregationInterval
// through the bucketed-LATEST pushdown (true/false folded as 1.0/0.0, reverse
// block scan, early termination) instead of materialising the full range and
// reducing afterwards.  That is a NEW plan for an existing canonical answer,
// so this test pins plan-equivalence:
//
//   fast path (epoch grid, TSM data)
//     == chunked reader (start-anchored grid chosen to coincide with epoch)
//     == the reduction computed independently in this test
//
// across placements: memory-only, flushed to TSM, and split across two
// overlapping TSM files (which trips the LWW gate and forces the fallback).
// The canonical rule (CLAUDE.md "Non-Numeric Fields in Queries"): value with
// the greatest timestamp inside each epoch-aligned bucket, returned at the
// bucket start, in the WRITTEN type (JSON true/false, never 1/0).

#include "../../../lib/core/engine.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <map>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr uint64_t kSec = 1000000000ULL;
// kBase is a multiple of 250s, so a start-anchored grid at interval 250s is
// IDENTICAL to the epoch grid — same buckets, different plan.
constexpr uint64_t kBase = 1700000000ULL * kSec;
constexpr uint64_t kIntervalSec = 250;
constexpr size_t kCount = 6000;  // several TSM blocks

bool valueAt(size_t i) {
    return (i % 3) != 0;
}

std::unique_ptr<seastar::http::request> jsonRequest(const std::string& body) {
    auto req = std::make_unique<seastar::http::request>();
    req->content = body;
    req->_headers["Content-Type"] = "application/json";
    return req;
}

// Write points [from, to) of the bool series in batched array form.
void writeBoolRange(HttpWriteHandler& handler, size_t from, size_t to) {
    constexpr size_t kBatch = 1000;
    for (size_t start = from; start < to; start += kBatch) {
        const size_t end = std::min(to, start + kBatch);
        std::string ts = "[";
        std::string vs = "[";
        for (size_t i = start; i < end; ++i) {
            if (i > start) {
                ts += ",";
                vs += ",";
            }
            ts += std::to_string(kBase + static_cast<uint64_t>(i) * kSec);
            vs += valueAt(i) ? "true" : "false";
        }
        ts += "]";
        vs += "]";
        auto rep = handler
                       .handleWrite(jsonRequest(R"({"measurement":"boolfast","tags":{"host":"h0"},"fields":{"b":)" +
                                                vs + R"(},"field_types":{"b":"bool"},"timestamps":)" + ts + "}"))
                       .get();
        ASSERT_EQ(rep->_status, seastar::http::reply::status_type::ok) << rep->_content;
    }
}

void flushToTsm(seastar::sharded<Engine>& eng, size_t minFiles) {
    eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int attempt = 0; attempt < 200; ++attempt) {
        const size_t files =
            eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
                .get();
        if (files >= minFiles) {
            return;
        }
        seastar::sleep(std::chrono::milliseconds(50)).get();
    }
    FAIL() << "TSM files did not appear after rollover";
}

struct BucketedResult {
    std::vector<uint64_t> timestamps;
    std::vector<bool> values;
    bool operator==(const BucketedResult&) const = default;
};

// Run the bucketed query through the full handler and extract field "b".
// anchored=false: epoch grid (fast-path eligible).  anchored=true:
// bucketAlignment=start (bypasses every epoch-only fast path -> chunked
// reader), which with kBase % interval == 0 produces the SAME buckets.
BucketedResult runBucketed(HttpQueryHandler& handler, uint64_t start, uint64_t end, bool anchored) {
    std::string body = R"({"query":"avg:boolfast(b){}","startTime":)" + std::to_string(start) + R"(,"endTime":)" +
                       std::to_string(end) + R"(,"aggregationInterval":")" + std::to_string(kIntervalSec) + R"(s")";
    if (anchored) {
        body += R"(,"bucketAlignment":"start")";
    }
    body += "}";
    auto rep = handler.handleQuery(jsonRequest(body)).get();
    EXPECT_EQ(static_cast<int>(rep->_status), 200) << rep->_content.substr(0, 300);

    BucketedResult out;
    glz::generic parsed;
    if (glz::read_json(parsed, rep->_content)) {
        ADD_FAILURE() << "unparseable response: " << rep->_content.substr(0, 300);
        return out;
    }
    auto& obj = parsed.get<glz::generic::object_t>();
    auto& arr = obj["series"].get<glz::generic::array_t>();
    if (arr.empty()) {
        return out;
    }
    EXPECT_EQ(arr.size(), 1u);
    auto& fields = arr[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
    auto& fieldObj = fields["b"].get<glz::generic::object_t>();
    for (auto& t : fieldObj["timestamps"].get<glz::generic::array_t>()) {
        out.timestamps.push_back(static_cast<uint64_t>(t.get<double>()));
    }
    for (auto& v : fieldObj["values"].get<glz::generic::array_t>()) {
        // The written type must survive: JSON true/false, never 1/0.
        if (!v.holds<bool>()) {
            ADD_FAILURE() << "bool bucket value serialized as non-bool";
            return out;
        }
        out.values.push_back(v.get<bool>());
    }
    return out;
}

// The canonical answer, computed independently: LATEST-per-epoch-bucket over
// points [0, upTo) intersected with [start, end] (endTime inclusive).
BucketedResult expectedReduction(uint64_t start, uint64_t end) {
    std::map<uint64_t, std::pair<uint64_t, bool>> buckets;  // bucket -> (ts, value)
    const uint64_t interval = kIntervalSec * kSec;
    for (size_t i = 0; i < kCount; ++i) {
        const uint64_t ts = kBase + static_cast<uint64_t>(i) * kSec;
        if (ts < start || ts > end) {
            continue;
        }
        const uint64_t bucket = ts / interval * interval;
        auto& slot = buckets[bucket];
        if (slot.first <= ts) {
            slot = {ts, valueAt(i)};
        }
    }
    BucketedResult out;
    for (auto& [bucket, tsVal] : buckets) {
        out.timestamps.push_back(bucket);
        out.values.push_back(tsVal.second);
    }
    return out;
}

}  // namespace

class BoolBucketedFastpathTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

TEST_F(BoolBucketedFastpathTest, EveryPlanAndPlacementAgrees) {
    seastar::thread([] {
        ScopedShardedEngine eng;
        eng.start();
        HttpWriteHandler writeHandler(&eng.eng);
        HttpQueryHandler queryHandler(&eng.eng);

        const uint64_t start = kBase;
        const uint64_t end = kBase + static_cast<uint64_t>(kCount) * kSec;
        const auto expected = expectedReduction(start, end);
        ASSERT_FALSE(expected.timestamps.empty());

        // ---- Placement 1: memory only ----
        writeBoolRange(writeHandler, 0, kCount);
        auto memEpoch = runBucketed(queryHandler, start, end, /*anchored=*/false);
        auto memAnchored = runBucketed(queryHandler, start, end, /*anchored=*/true);
        EXPECT_EQ(memEpoch, expected) << "memory placement, epoch grid";
        EXPECT_EQ(memAnchored, expected) << "memory placement, anchored plan";

        // ---- Placement 2: flushed to TSM (fast-path territory) ----
        flushToTsm(eng.eng, 1);
        auto tsmEpoch = runBucketed(queryHandler, start, end, /*anchored=*/false);
        auto tsmAnchored = runBucketed(queryHandler, start, end, /*anchored=*/true);
        EXPECT_EQ(tsmEpoch, expected) << "TSM placement, epoch grid (bucketed-LATEST pushdown)";
        EXPECT_EQ(tsmAnchored, expected) << "TSM placement, anchored plan (chunked reader)";

        // A sub-range whose grid still matches both plans: exercises partial
        // bucket coverage and the single-bucket sparse-stat resolution.
        const uint64_t subStart = kBase + 1000 * kSec;
        const uint64_t subEnd = kBase + 1249 * kSec;  // exactly one 250s bucket
        const auto subExpected = expectedReduction(subStart, subEnd);
        EXPECT_EQ(runBucketed(queryHandler, subStart, subEnd, false), subExpected) << "single-bucket, epoch";
        EXPECT_EQ(runBucketed(queryHandler, subStart, subEnd, true), subExpected) << "single-bucket, anchored";

        // ---- Placement 3: REWRITE half the points -> two TSM files whose
        // series time ranges overlap.  The LWW gate must refuse the
        // early-terminating fast path and the fallback must give the same
        // canonical answer (rewrites are overwrites, so values are unchanged
        // -- what is being tested is that the PLAN switch is invisible). ----
        writeBoolRange(writeHandler, kCount / 2, kCount);
        flushToTsm(eng.eng, 2);
        auto overlapEpoch = runBucketed(queryHandler, start, end, /*anchored=*/false);
        auto overlapAnchored = runBucketed(queryHandler, start, end, /*anchored=*/true);
        EXPECT_EQ(overlapEpoch, expected) << "overlapping-files placement, epoch grid (LWW fallback)";
        EXPECT_EQ(overlapAnchored, expected) << "overlapping-files placement, anchored plan";
    })
        .join()
        .get();
}
