// Stack-level property fuzzing: well-formed data through write -> storage ->
// query, asserting the answer does not depend on anything the caller did not
// ask about.  Design and rationale: docs/fuzzer_design.md.
//
// WHY THIS EXISTS (and why decoder_mutation_fuzz_test.cpp does not cover it)
//
// That file fuzzes MALFORMED BYTES into a decoder -- the "unvalidated count
// sizes an allocation" class.  It never runs a query.  Every bug found on
// 2026-07-20 was the opposite: perfectly well-formed data returning a WRONG
// ANSWER, where the answer varied with something invisible to the caller:
//
//   * StringEncoder::decode cleared a vector TSM shares across all blocks of a
//     series, so a multi-BLOCK string series kept every block's timestamps but
//     only the last block's values (3600 vs 600).  Consumers index values[i] by
//     a timestamp index => out-of-bounds read => empty strings on an HTTP 200,
//     and a segfaulted shard.  Varied with: BLOCK COUNT.
//   * The filtered FFOR decode did not clamp to the requested count, so the
//     tail of the final 1024-value group was reconstructed and emitted as real
//     points, and the inflated count desynced the value decoder.  Varied with:
//     REQUESTED COUNT vs FFOR GROUP SIZE.
//
// A hand-written test fixes one layout and passes forever.  These properties
// vary the layout on purpose.
//
// PROPERTIES ASSERTED HERE (phase 1)
//
//   P1 Placement invariance -- same query before and after a TSM flush returns
//      the same answer.  (Catches the string-decoder bug: multi-block layout
//      only exists after a flush.)
//   P2 Length coherence -- |timestamps| == |values| for every field.  (The
//      desync itself, observed at the API boundary.)
//   P3 Range decomposition -- raw [a,c] == raw [a,b] ++ raw (b,c].  (Catches the
//      FFOR over-read: a sub-range read takes the FILTERED decode path while the
//      full-range read does not, so only a split disagrees.)
//   P8 No phantom timestamps -- every returned timestamp was actually written.
//      (Catches the FFOR over-read directly: extrapolated points continue the
//      arithmetic progression and look plausible, so only membership catches
//      them.)
//
// Phase 1 deliberately compares only EXACT-comparable results: raw reads and
// bucketed LATEST both return stored values verbatim, so no floating-point
// tolerance is needed.  sum/avg placement invariance needs a ULP tolerance
// (different placements fold in different orders and IEEE addition is not
// associative) and is left to phase 3 rather than bolted on wrongly here.
//
// DETERMINISM: seeded from TIMESTAR_FUZZ_SEED (default fixed), and every failure
// message prints the seed plus the generated workload so it can be replayed.
// TIMESTAR_FUZZ_ITERATIONS scales the run for nightly use.

#include "../../../lib/core/engine.hpp"
#include "../../../lib/encoding/integer_encoder.hpp"
#include "../../../lib/http/http_query_handler.hpp"
#include "../../../lib/http/http_write_handler.hpp"
#include "../../../lib/storage/tsm_writer.hpp"
#include "../../test_helpers.hpp"

#include <glaze/json.hpp>

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <random>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <set>
#include <string>
#include <vector>

using namespace timestar;

namespace {

constexpr uint64_t kSec = 1000000000ULL;
constexpr uint64_t kHour = 3600ULL * kSec;
// Epoch-aligned so bucket starts are predictable.
constexpr uint64_t kBase = 1700000000ULL * kSec - (1700000000ULL * kSec) % kHour;

size_t envSize(const char* name, size_t fallback) {
    if (const char* v = std::getenv(name)) {
        char* end = nullptr;
        const unsigned long long parsed = std::strtoull(v, &end, 10);
        if (end != v && parsed > 0) {
            return static_cast<size_t>(parsed);
        }
    }
    return fallback;
}

// ---------------------------------------------------------------------------
// Generated workload
// ---------------------------------------------------------------------------

enum class FieldType { Float, Integer, Bool, String };

const char* typeName(FieldType t) {
    switch (t) {
        case FieldType::Float:
            return "float";
        case FieldType::Integer:
            return "int";
        case FieldType::Bool:
            return "bool";
        case FieldType::String:
            return "string";
    }
    return "float";
}

struct GeneratedSeries {
    std::string measurement;
    std::string field = "v";
    std::string tagValue;
    FieldType type = FieldType::Float;
    std::vector<uint64_t> timestamps;
    std::vector<std::string> jsonValues;  // literal as written
    std::vector<std::string> canonical;   // canonical form for comparison

    std::string describe() const {
        return measurement + " type=" + typeName(type) + " points=" + std::to_string(timestamps.size());
    }
};

// Point counts must straddle the structural boundaries the bugs lived on, and
// those boundaries are DERIVED from the code rather than guessed:
//
//   * MaxPointsPerBlock() (config, default 3000) -- the multi-BLOCK threshold.
//     The string-decoder bug needs >= 2 blocks in one file; a generator whose
//     largest series is exactly one block cannot produce it. An earlier revision
//     of this file hard-coded 1024 as the block size and therefore silently
//     could not rediscover that bug at all.
//   * IntegerEncoderFFOR::kBlockSize (1024) -- the FFOR group size, where the
//     filtered-decode over-read lived.
//
// If either constant changes, these counts follow it.
std::vector<size_t> interestingCounts() {
    const size_t block = MaxPointsPerBlock();
    const size_t ffor = IntegerEncoderFFOR::kBlockSize;
    return {
        1,
        2,
        17,
        ffor - 1,
        ffor,
        ffor + 1,  // FFOR group edges
        block - 1,
        block,
        block + 1,  // first multi-block edge
        2 * block,
        2 * block + 1,  // three blocks
        3 * block + 7,  // several blocks, unaligned tail
    };
}

// ONE canonical rendering for every numeric value, used on both sides of every
// comparison. Generated values and response values must go through this same
// function or the harness reports formatting differences as product bugs
// (std::to_string(double) renders -267765 as "-267765.000000").
//
// %.17g round-trips a double exactly and prints integral values without a
// trailing ".000000". Generated magnitudes stay well inside 2^53, so integers
// survive the JSON double round-trip without precision loss.
//
// Special values are covered by the dedicated NaN/Inf policy tests rather than
// smuggled in here: a JSON null would defeat the exactness this phase relies on.
std::string canonicalNumber(double d) {
    char buf[40];
    std::snprintf(buf, sizeof(buf), "%.17g", d);
    return std::string(buf);
}

GeneratedSeries generateSeries(std::mt19937_64& rng, size_t index) {
    GeneratedSeries g;
    g.measurement = "fuzz_m" + std::to_string(index);
    g.tagValue = "h" + std::to_string(rng() % 4);

    const FieldType types[] = {FieldType::Float, FieldType::Integer, FieldType::Bool, FieldType::String};

    // COVERAGE BY CONSTRUCTION for the known-risky axes. Pure random sampling
    // left the (string x multi-block) cell -- the one that actually held a bug --
    // unvisited in a default-length run. The first passes enumerate every
    // (type x count) pair deterministically; later iterations sample randomly.
    const auto counts = interestingCounts();
    const size_t combos = 4 * counts.size();
    size_t count;
    if (index < combos) {
        g.type = types[index % 4];
        count = counts[(index / 4) % counts.size()];
    } else {
        g.type = types[rng() % 4];
        count = (rng() % 2 == 0) ? counts[rng() % counts.size()] : (1 + (rng() % (3 * MaxPointsPerBlock())));
    }

    // Spacing: sometimes regular, sometimes irregular, so blocks and buckets do
    // not always align the same way.
    const bool irregular = (rng() % 3) == 0;
    const uint64_t baseStep = 1 + (rng() % 5);  // seconds

    // Low cardinality (<= 50 uniques) takes the STR2 dictionary path; high
    // cardinality takes the raw STRG path.  Both are real branches in
    // decodeBlockFlat and the bug lived in code shared by them.
    const size_t stringCardinality = (rng() % 2 == 0) ? (1 + rng() % 40) : 5000;

    uint64_t ts = kBase;
    g.timestamps.reserve(count);
    g.jsonValues.reserve(count);
    g.canonical.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        g.timestamps.push_back(ts);
        const uint64_t step = irregular ? (1 + (rng() % (baseStep * 3))) : baseStep;
        ts += step * kSec;

        switch (g.type) {
            case FieldType::Float: {
                const double d = static_cast<double>(static_cast<int64_t>(rng() % 20000) - 10000) / 2.0;
                g.jsonValues.push_back(canonicalNumber(d));
                g.canonical.push_back(canonicalNumber(d));
                break;
            }
            case FieldType::Integer: {
                const int64_t v = static_cast<int64_t>(rng() % 1000000) - 500000;
                g.jsonValues.push_back(std::to_string(v));
                g.canonical.push_back(canonicalNumber(static_cast<double>(v)));
                break;
            }
            case FieldType::Bool: {
                const bool b = (rng() % 2) == 0;
                g.jsonValues.push_back(b ? "true" : "false");
                g.canonical.push_back(b ? "true" : "false");
                break;
            }
            case FieldType::String: {
                const std::string s = "s_" + std::to_string(rng() % stringCardinality);
                g.jsonValues.push_back("\"" + s + "\"");
                g.canonical.push_back(s);
                break;
            }
        }
    }
    return g;
}

// ---------------------------------------------------------------------------
// Apply / query
// ---------------------------------------------------------------------------

std::unique_ptr<seastar::http::request> jsonRequest(const std::string& body) {
    auto req = std::make_unique<seastar::http::request>();
    req->content = body;
    req->_headers["Content-Type"] = "application/json";
    return req;
}

// Writes the series in batches. Returns false (with reason) rather than
// asserting, so the caller can attach the seed to the failure.
bool writeSeries(HttpWriteHandler& handler, const GeneratedSeries& g, std::string& failure) {
    constexpr size_t kBatch = 400;
    for (size_t start = 0; start < g.timestamps.size(); start += kBatch) {
        const size_t end = std::min(g.timestamps.size(), start + kBatch);
        std::string ts = "[";
        std::string vs = "[";
        for (size_t i = start; i < end; ++i) {
            if (i > start) {
                ts += ",";
                vs += ",";
            }
            ts += std::to_string(g.timestamps[i]);
            vs += g.jsonValues[i];
        }
        ts += "]";
        vs += "]";

        // field_types is explicit: JSON type detection would read 10.0 as an
        // integer and silently bind the series to the wrong type.
        const std::string body = R"({"measurement":")" + g.measurement + R"(","tags":{"host":")" + g.tagValue +
                                 R"("},"fields":{")" + g.field + R"(":)" + vs + R"(},"field_types":{")" + g.field +
                                 R"(":")" + typeName(g.type) + R"("},"timestamps":)" + ts + "}";
        auto rep = handler.handleWrite(jsonRequest(body)).get();
        if (rep->_status != seastar::http::reply::status_type::ok) {
            failure = "write failed: " + std::string(rep->_content.substr(0, 200));
            return false;
        }
    }
    return true;
}

struct QueryResult {
    bool ok = false;
    std::string error;
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;  // canonical strings
    size_t rawTimestampCount = 0;     // as reported by the response, before pairing
    size_t rawValueCount = 0;
};

// Canonicalise a JSON value so float/int/bool/string all compare as strings.
std::string canonicalOf(glz::generic& v) {
    if (v.holds<std::string>()) {
        return v.get<std::string>();
    }
    if (v.holds<bool>()) {
        return v.get<bool>() ? "true" : "false";
    }
    if (v.holds<double>()) {
        return canonicalNumber(v.get<double>());
    }
    return "null";
}

QueryResult runQuery(HttpQueryHandler& handler, const std::string& measurement, const std::string& field,
                     uint64_t start, uint64_t end, const std::string& interval) {
    QueryResult out;
    std::string body = R"({"query":"latest:)" + measurement + "(" + field + R"(){}","startTime":)" +
                       std::to_string(start) + R"(,"endTime":)" + std::to_string(end);
    if (!interval.empty()) {
        body += R"(,"aggregationInterval":")" + interval + R"(")";
    }
    body += "}";

    auto rep = handler.handleQuery(jsonRequest(body)).get();
    if (rep->_status != seastar::http::reply::status_type::ok) {
        out.error =
            "HTTP " + std::to_string(static_cast<int>(rep->_status)) + ": " + std::string(rep->_content.substr(0, 200));
        return out;
    }

    glz::generic parsed;
    if (glz::read_json(parsed, rep->_content)) {
        out.error = "unparseable response: " + std::string(rep->_content.substr(0, 200));
        return out;
    }
    auto& obj = parsed.get<glz::generic::object_t>();
    if (!obj.contains("series")) {
        out.error = "response has no series field";
        return out;
    }
    auto& arr = obj["series"].get<glz::generic::array_t>();
    out.ok = true;
    if (arr.empty()) {
        return out;  // legitimately empty
    }
    auto& fields = arr[0].get<glz::generic::object_t>()["fields"].get<glz::generic::object_t>();
    for (auto& [name, fieldObj] : fields) {
        auto& fo = fieldObj.get<glz::generic::object_t>();
        auto& tsArr = fo["timestamps"].get<glz::generic::array_t>();
        auto& valArr = fo["values"].get<glz::generic::array_t>();
        out.rawTimestampCount = tsArr.size();
        out.rawValueCount = valArr.size();
        for (auto& t : tsArr) {
            out.timestamps.push_back(static_cast<uint64_t>(t.get<double>()));
        }
        for (auto& v : valArr) {
            out.values.push_back(canonicalOf(v));
        }
        break;
    }
    return out;
}

size_t tsmFileCount(seastar::sharded<Engine>& eng) {
    return eng.map_reduce0([](Engine& engine) { return engine.getTSMFileCount(); }, size_t{0}, std::plus<size_t>())
        .get();
}

// Waits for the rollover to actually LAND a new TSM file, not merely for one to
// exist. Iterations share the shard directories, so "files >= 1" was satisfied
// instantly by a previous iteration's file and the data never moved -- which made
// the placement-invariance property vacuous: it compared memstore against
// memstore and passed no matter what the TSM path did.
//
// Returns false if the data never reached TSM, so the caller can fail loudly
// rather than silently assert nothing.
//
// Note it waits for a file to EXIST rather than for the count to increase: a
// large series auto-rolls while it is still being written, so by flush time
// there may be nothing left to convert. Iterations are isolated (shard dirs are
// cleaned per iteration), so "a file exists" means this iteration's data.
bool flushToTsm(seastar::sharded<Engine>& eng) {
    eng.invoke_on_all([](Engine& engine) { return engine.rolloverMemoryStore(); }).get();
    for (int attempt = 0; attempt < 200; ++attempt) {
        if (tsmFileCount(eng) >= 1) {
            return true;
        }
        seastar::sleep(std::chrono::milliseconds(50)).get();
    }
    return false;
}

// ---------------------------------------------------------------------------
// Properties
// ---------------------------------------------------------------------------

// P2: |timestamps| == |values|. A mismatch means a desynced pair reached the
// API -- downstream code indexes values by a timestamp index, so this is an
// out-of-bounds read, not merely a wrong number.
void checkLengthCoherence(const QueryResult& r, const std::string& ctx) {
    ASSERT_EQ(r.rawTimestampCount, r.rawValueCount)
        << ctx << ": response carried " << r.rawTimestampCount << " timestamps but " << r.rawValueCount
        << " values -- a desynced timestamp/value pair";
}

// P8: every returned timestamp was actually written.
void checkNoPhantomTimestamps(const QueryResult& r, const std::set<uint64_t>& written, const std::string& ctx) {
    for (uint64_t ts : r.timestamps) {
        ASSERT_TRUE(written.count(ts) == 1)
            << ctx << ": returned timestamp " << ts
            << " was never written -- a phantom point (decoded past the end of the encoded run)";
    }
}

}  // namespace

class StackPropertyFuzzTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// One iteration: generate a series, write it, and assert P1/P2/P3/P8 across a
// TSM flush.
TEST_F(StackPropertyFuzzTest, WellFormedDataSurvivesPlacementAndRangeSplitting) {
    const size_t seed = envSize("TIMESTAR_FUZZ_SEED", 20260720);
    // Default covers the full (type x count) cross-product exactly once.
    const size_t iterations = envSize("TIMESTAR_FUZZ_ITERATIONS", 4 * interestingCounts().size());

    seastar::thread([seed, iterations] {
        for (size_t iter = 0; iter < iterations; ++iter) {
            const size_t iterSeed = seed + iter;
            std::mt19937_64 rng(iterSeed);

            // Isolate iterations: they otherwise share shard directories, so a
            // previous iteration's TSM files make this one's flush check
            // meaningless (and its data visible to this iteration's queries).
            cleanTestShardDirectories();

            ScopedShardedEngine eng;
            eng.start();
            HttpWriteHandler writeHandler(&eng.eng);
            HttpQueryHandler queryHandler(&eng.eng);

            auto g = generateSeries(rng, iter);
            const std::string repro = "SEED=" + std::to_string(iterSeed) + " (" + g.describe() + ")";

            std::string failure;
            ASSERT_TRUE(writeSeries(writeHandler, g, failure)) << repro << " " << failure;

            const uint64_t first = g.timestamps.front();
            // +1ns: the API requires startTime < endTime, and a single-point
            // series would otherwise produce an empty range. endTime is
            // inclusive, so this cannot pull in points that were not written.
            const uint64_t last = g.timestamps.back() + 1;
            const std::set<uint64_t> written(g.timestamps.begin(), g.timestamps.end());

            // ---- before flush (memory-resident) ----
            auto rawBefore = runQuery(queryHandler, g.measurement, g.field, first, last, "");
            ASSERT_TRUE(rawBefore.ok) << repro << " raw/before: " << rawBefore.error;
            checkLengthCoherence(rawBefore, repro + " raw/before");
            checkNoPhantomTimestamps(rawBefore, written, repro + " raw/before");

            auto bucketedBefore = runQuery(queryHandler, g.measurement, g.field, first, last, "1h");
            ASSERT_TRUE(bucketedBefore.ok) << repro << " bucketed/before: " << bucketedBefore.error;
            checkLengthCoherence(bucketedBefore, repro + " bucketed/before");

            // A raw read must return every written point in range.
            EXPECT_EQ(rawBefore.timestamps.size(), g.timestamps.size())
                << repro << ": raw read returned " << rawBefore.timestamps.size() << " of " << g.timestamps.size()
                << " written points";
            EXPECT_EQ(rawBefore.values, g.canonical) << repro << ": raw read values differ from what was written";

            // ---- P3: range decomposition (before flush) ----
            // Splitting forces the FILTERED decode path for the sub-ranges while
            // the whole-range read may not take it; only a split disagrees.
            if (g.timestamps.size() >= 4) {
                const size_t midIdx = g.timestamps.size() / 2;
                const uint64_t mid = g.timestamps[midIdx];
                auto lower = runQuery(queryHandler, g.measurement, g.field, first, mid - 1, "");
                auto upper = runQuery(queryHandler, g.measurement, g.field, mid, last, "");
                ASSERT_TRUE(lower.ok) << repro << " split/lower: " << lower.error;
                ASSERT_TRUE(upper.ok) << repro << " split/upper: " << upper.error;
                checkLengthCoherence(lower, repro + " split/lower");
                checkLengthCoherence(upper, repro + " split/upper");
                checkNoPhantomTimestamps(lower, written, repro + " split/lower");
                checkNoPhantomTimestamps(upper, written, repro + " split/upper");

                std::vector<uint64_t> joinedTs = lower.timestamps;
                joinedTs.insert(joinedTs.end(), upper.timestamps.begin(), upper.timestamps.end());
                std::vector<std::string> joinedVals = lower.values;
                joinedVals.insert(joinedVals.end(), upper.values.begin(), upper.values.end());

                EXPECT_EQ(joinedTs, rawBefore.timestamps)
                    << repro << ": [a,b] ++ (b,c] timestamps differ from [a,c] (split at " << mid << ")";
                EXPECT_EQ(joinedVals, rawBefore.values)
                    << repro << ": [a,b] ++ (b,c] values differ from [a,c] (split at " << mid << ")";
            }

            // ---- flush: same data, different physical layout ----
            // The placement property is only meaningful if the data actually
            // moved; assert that rather than assume it.
            ASSERT_TRUE(flushToTsm(eng.eng))
                << repro
                << ": data never reached TSM after rollover -- the placement-invariance "
                   "check below would have compared memstore against memstore and passed vacuously";

            // ---- P1: placement invariance ----
            auto rawAfter = runQuery(queryHandler, g.measurement, g.field, first, last, "");
            ASSERT_TRUE(rawAfter.ok) << repro << " raw/after: " << rawAfter.error;
            checkLengthCoherence(rawAfter, repro + " raw/after");
            checkNoPhantomTimestamps(rawAfter, written, repro + " raw/after");

            EXPECT_EQ(rawAfter.timestamps, rawBefore.timestamps)
                << repro << ": raw TIMESTAMPS changed across a TSM flush -- the answer depends on placement";
            EXPECT_EQ(rawAfter.values, rawBefore.values)
                << repro << ": raw VALUES changed across a TSM flush -- the answer depends on placement";

            auto bucketedAfter = runQuery(queryHandler, g.measurement, g.field, first, last, "1h");
            ASSERT_TRUE(bucketedAfter.ok) << repro << " bucketed/after: " << bucketedAfter.error;
            checkLengthCoherence(bucketedAfter, repro + " bucketed/after");
            EXPECT_EQ(bucketedAfter.timestamps, bucketedBefore.timestamps)
                << repro << ": bucket timestamps changed across a TSM flush";
            EXPECT_EQ(bucketedAfter.values, bucketedBefore.values)
                << repro << ": bucketed VALUES changed across a TSM flush";

            // ---- P3 again, now multi-block on disk ----
            if (g.timestamps.size() >= 4) {
                const size_t midIdx = g.timestamps.size() / 2;
                const uint64_t mid = g.timestamps[midIdx];
                auto lower = runQuery(queryHandler, g.measurement, g.field, first, mid - 1, "");
                auto upper = runQuery(queryHandler, g.measurement, g.field, mid, last, "");
                ASSERT_TRUE(lower.ok) << repro << " split/lower after flush: " << lower.error;
                ASSERT_TRUE(upper.ok) << repro << " split/upper after flush: " << upper.error;
                checkLengthCoherence(lower, repro + " split/lower after flush");
                checkLengthCoherence(upper, repro + " split/upper after flush");
                checkNoPhantomTimestamps(lower, written, repro + " split/lower after flush");
                checkNoPhantomTimestamps(upper, written, repro + " split/upper after flush");

                std::vector<uint64_t> joinedTs = lower.timestamps;
                joinedTs.insert(joinedTs.end(), upper.timestamps.begin(), upper.timestamps.end());
                std::vector<std::string> joinedVals = lower.values;
                joinedVals.insert(joinedVals.end(), upper.values.begin(), upper.values.end());

                EXPECT_EQ(joinedTs, rawAfter.timestamps)
                    << repro << ": after flush, [a,b] ++ (b,c] timestamps differ from [a,c]";
                EXPECT_EQ(joinedVals, rawAfter.values)
                    << repro << ": after flush, [a,b] ++ (b,c] values differ from [a,c]";
            }

            if (::testing::Test::HasFailure()) {
                // Stop at the first failing iteration: the messages already name
                // the seed, and continuing only buries it.
                return;
            }
        }
    })
        .join()
        .get();
}

// ---------------------------------------------------------------------------
// Decoder-level property: a decoder must never emit more values than requested.
//
// WHY THIS IS SEPARATE FROM THE STACK FUZZER ABOVE
//
// The filtered FFOR decode over-read (fixed in "clamp the filtered FFOR decode
// to the requested count") is NOT reachable through the query stack: in the TSM
// read path `timestampSize` comes from the block header and therefore always
// equals the encoded count, so a caller never requests fewer.  The stack fuzzer
// above was run with that fix reverted and correctly still passed -- the
// triggering condition simply does not occur there.
//
// It is reachable by any caller that supplies its own count: the WAL path, the
// protobuf ingest path (which derives a BOUND from payload length, not a count),
// and anything written later.  So the invariant belongs at the decoder API,
// where it can be checked directly.
//
// This fuzzes the (encodedCount, requestedCount, skip, limit, time-filter) space
// that ffor_filtered_decode_bounds_test.cpp pins at fixed points.
// ---------------------------------------------------------------------------

TEST_F(StackPropertyFuzzTest, DecoderNeverEmitsMoreThanRequested) {
    const size_t seed = envSize("TIMESTAR_FUZZ_SEED", 20260720);
    const size_t iterations = envSize("TIMESTAR_FUZZ_ITERATIONS", 300);
    std::mt19937_64 rng(seed);

    const size_t ffor = IntegerEncoderFFOR::kBlockSize;

    for (size_t iter = 0; iter < iterations; ++iter) {
        // Encoded counts clustered around the FFOR group size, where a partial
        // final group means "decoded in whole groups" can overshoot.
        const size_t encodedCount = 1 + (rng() % (3 * ffor));

        std::vector<uint64_t> src;
        src.reserve(encodedCount);
        uint64_t ts = kBase;
        const uint64_t step = 1 + (rng() % 1000);
        for (size_t i = 0; i < encodedCount; ++i) {
            src.push_back(ts);
            ts += step;
        }
        auto encoded = IntegerEncoder::encode(src);

        // Requested count: usually smaller than encoded (the interesting case),
        // sometimes equal or larger.
        size_t requested;
        switch (rng() % 4) {
            case 0:
                requested = 1 + (rng() % encodedCount);
                break;
            case 1:
                requested = encodedCount;
                break;
            case 2:
                requested = encodedCount + 1 + (rng() % 100);
                break;
            default:
                requested = (encodedCount > 1) ? (encodedCount / 2 + (rng() % encodedCount) / 2) : 1;
                break;
        }
        if (requested == 0) {
            requested = 1;
        }

        // Force the FILTERED path half the time. Sentinel bounds (0, UINT64_MAX)
        // select the unfiltered fast path, so a window just inside them is what
        // exercises the other one.
        const bool filtered = (rng() % 2) == 0;
        const uint64_t minTime = filtered ? 1ULL : 0ULL;
        const uint64_t maxTime = filtered ? (UINT64_MAX - 1) : UINT64_MAX;

        Slice slice(encoded.data.data(), encoded.size());
        std::vector<uint64_t> out;
        auto [skipped, added] = IntegerEncoder::decode(slice, static_cast<unsigned>(requested), out, minTime, maxTime);

        const std::string repro = "SEED=" + std::to_string(seed) + " iter=" + std::to_string(iter) +
                                  " encoded=" + std::to_string(encodedCount) +
                                  " requested=" + std::to_string(requested) + " filtered=" + (filtered ? "yes" : "no");

        ASSERT_EQ(out.size(), added) << repro << ": returned count disagrees with what was appended";
        ASSERT_LE(out.size(), requested)
            << repro << ": decoder emitted " << out.size() << " values for a requested count of " << requested
            << " -- it ran to the end of the encoding group instead of stopping at the request";
        ASSERT_LE(skipped + added, requested) << repro << ": decoder consumed more logical positions than requested";

        // Every emitted value must be a real prefix of what was encoded: no
        // phantom points reconstructed from the group's padding.
        const size_t expected = std::min(requested, encodedCount);
        ASSERT_EQ(out.size(), expected) << repro << ": expected the first " << expected << " encoded values";
        for (size_t i = 0; i < out.size(); ++i) {
            ASSERT_EQ(out[i], src[i]) << repro << ": value " << i << " is not what was encoded";
        }
    }
}
