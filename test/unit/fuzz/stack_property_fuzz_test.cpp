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

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <optional>
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

// Query methods the fuzzer drives. Placement invariance holds for all of them;
// only the comparison tolerance differs.
enum class AggMethod { Latest, First, Min, Max, Count, Sum, Avg };

const char* methodName(AggMethod m) {
    switch (m) {
        case AggMethod::Latest:
            return "latest";
        case AggMethod::First:
            return "first";
        case AggMethod::Min:
            return "min";
        case AggMethod::Max:
            return "max";
        case AggMethod::Count:
            return "count";
        case AggMethod::Sum:
            return "sum";
        case AggMethod::Avg:
            return "avg";
    }
    return "latest";
}

// SUM and AVG accumulate, so different placements legitimately fold in a
// different ORDER, and IEEE addition is not associative -- a last-bit difference
// is correct behaviour, not a bug. Everything else either selects a stored value
// (latest/first/min/max) or counts, and must be bit-identical.
bool methodRequiresExactMatch(AggMethod m) {
    return m != AggMethod::Sum && m != AggMethod::Avg;
}

// Non-numeric fields ignore the method entirely (raw passthrough, or
// LATEST-per-bucket with an interval), so only latest/first are meaningful for
// them; the others would be comparing against a different documented rule.
bool methodAppliesTo(AggMethod m, FieldType t) {
    if (t == FieldType::Float || t == FieldType::Integer) {
        return true;
    }
    return m == AggMethod::Latest || m == AggMethod::First;
}

struct GeneratedSeries {
    std::string measurement;
    std::string field = "v";
    std::string tagValue;
    FieldType type = FieldType::Float;
    std::vector<uint64_t> timestamps;
    std::vector<std::string> jsonValues;  // literal as written
    std::vector<std::string> canonical;   // canonical form for comparison

    // Second write that REWRITES a subset of the same timestamps. Duplicate
    // points must overwrite (last-write-wins), so this exercises the dedup/merge
    // path -- which has its own history of returning both copies, or the older
    // one, depending on where the two copies physically landed.
    std::vector<uint64_t> rewriteTs;
    std::vector<std::string> rewriteJson;

    // The state a query must see once everything is written: one value per
    // distinct timestamp, newest wins.
    std::vector<uint64_t> expectedTs;
    std::vector<std::string> expectedCanonical;

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

// ---------------------------------------------------------------------------
// Workload: the complete, SHRINKABLE description of one generated case.
//
// Everything the generator decides lives here as a named field, and
// materialize() is a pure function of it. That is what makes minimisation
// possible: the shrinker simplifies one field at a time and re-runs, rather than
// trying to reverse-engineer a random stream.
// ---------------------------------------------------------------------------
struct Workload {
    FieldType type = FieldType::Float;
    size_t count = 1;
    uint64_t baseStepSec = 1;
    bool irregular = false;
    size_t stringCardinality = 8;
    uint64_t valueSeed = 0;
    size_t index = 0;  // names the measurement, keeps iterations distinct

    // How many of the written timestamps get rewritten with a new value (LWW).
    size_t rewriteCount = 0;
    // Float series only: mix in NaN / +Inf / -Inf / -0.0. NaN is "missing" and
    // +/-Inf is valid data (docs/nan_policy.md), and both have had bugs.
    bool includeSpecials = false;
    // The query shape to check. Placement invariance needs no oracle, so the
    // method can vary freely -- that is what widens the query space cheaply.
    AggMethod method = AggMethod::Latest;
    uint64_t intervalSec = 0;  // 0 = raw read

    std::string describe() const {
        return std::string("type=") + typeName(type) + " count=" + std::to_string(count) +
               " stepSec=" + std::to_string(baseStepSec) + " irregular=" + (irregular ? "yes" : "no") +
               " strCard=" + std::to_string(stringCardinality) + " rewrites=" + std::to_string(rewriteCount) +
               " specials=" + (includeSpecials ? "yes" : "no") + " method=" + methodName(method) +
               " intervalSec=" + std::to_string(intervalSec) + " valueSeed=" + std::to_string(valueSeed);
    }

    // A failure report someone can act on without re-deriving anything.
    std::string reproducer() const {
        std::string s = "\n--- MINIMAL REPRODUCER ---\n";
        s += "  Workload w;\n";
        s += std::string("  w.type = FieldType::") +
             (type == FieldType::Float
                  ? "Float"
                  : (type == FieldType::Integer ? "Integer" : (type == FieldType::Bool ? "Bool" : "String"))) +
             ";\n";
        s += "  w.count = " + std::to_string(count) + ";\n";
        s += "  w.baseStepSec = " + std::to_string(baseStepSec) + ";\n";
        s += "  w.irregular = " + std::string(irregular ? "true" : "false") + ";\n";
        s += "  w.stringCardinality = " + std::to_string(stringCardinality) + ";\n";
        s += "  w.rewriteCount = " + std::to_string(rewriteCount) + ";\n";
        s += "  w.includeSpecials = " + std::string(includeSpecials ? "true" : "false") + ";\n";
        s += std::string("  w.method = AggMethod::") +
             (method == AggMethod::Latest  ? "Latest"
              : method == AggMethod::First ? "First"
              : method == AggMethod::Min   ? "Min"
              : method == AggMethod::Max   ? "Max"
              : method == AggMethod::Count ? "Count"
              : method == AggMethod::Sum   ? "Sum"
                                           : "Avg") +
             ";\n";
        s += "  w.intervalSec = " + std::to_string(intervalSec) + ";\n";
        s += "  w.valueSeed = " + std::to_string(valueSeed) + ";\n";
        s += "  w.index = " + std::to_string(index) + ";\n";
        s += "  EXPECT_FALSE(runWorkload(w).has_value());\n";
        s += "--------------------------\n";
        return s;
    }
};

// One generated value as (json literal, canonical form).
//
// NaN serialises as JSON null and reads back as null, so the canonical form for
// NaN is "null" -- that is the documented wire representation, not a harness
// fudge. +/-Inf are valid data and round-trip as numbers.
std::pair<std::string, std::string> makeValue(const Workload& w, std::mt19937_64& rng) {
    switch (w.type) {
        case FieldType::Float: {
            if (w.includeSpecials && (rng() % 8) == 0) {
                // -0.0 is the only special value the JSON write path accepts.
                //
                // NaN and +/-Infinity are NOT writable over JSON, and this was
                // verified against a live server rather than assumed: `null` in
                // a value array is rejected both ways -- "Mixed types in field
                // array" without field_types, "declared float but the value is
                // not a number" with it. JSON has no Infinity literal either.
                //
                // That is by design, not a gap: the nodejs client always uses
                // protobuf, which carries NaN natively (CLAUDE.md, "Special
                // Float Values"). So NaN/Inf coverage belongs to the protobuf
                // variant (phase 4) -- faking it here with e.g. 1e999 only
                // produces a parse error and tests nothing.
                return {"-0.0", canonicalNumber(-0.0)};
            }
            const double d = static_cast<double>(static_cast<int64_t>(rng() % 20000) - 10000) / 2.0;
            return {canonicalNumber(d), canonicalNumber(d)};
        }
        case FieldType::Integer: {
            const int64_t v = static_cast<int64_t>(rng() % 1000000) - 500000;
            return {std::to_string(v), canonicalNumber(static_cast<double>(v))};
        }
        case FieldType::Bool: {
            const bool b = (rng() % 2) == 0;
            return {b ? "true" : "false", b ? "true" : "false"};
        }
        case FieldType::String: {
            const std::string str = "s_" + std::to_string(rng() % w.stringCardinality);
            return {"\"" + str + "\"", str};
        }
    }
    return {"0", "0"};
}

// Pure: the same Workload always produces the same points.
GeneratedSeries materialize(const Workload& w) {
    std::mt19937_64 rng(w.valueSeed);
    GeneratedSeries g;
    g.measurement = "fuzz_m" + std::to_string(w.index);
    g.tagValue = "h0";
    g.type = w.type;

    uint64_t ts = kBase;
    g.timestamps.reserve(w.count);
    g.jsonValues.reserve(w.count);
    g.canonical.reserve(w.count);

    for (size_t i = 0; i < w.count; ++i) {
        g.timestamps.push_back(ts);
        const uint64_t step = w.irregular ? (1 + (rng() % (w.baseStepSec * 3))) : w.baseStepSec;
        ts += step * kSec;

        auto [json, canon] = makeValue(w, rng);
        g.jsonValues.push_back(std::move(json));
        g.canonical.push_back(std::move(canon));
    }

    // ---- rewrites: the same timestamps written again, newest must win ----
    std::vector<std::string> finalCanonical = g.canonical;
    const size_t rewrites = std::min(w.rewriteCount, w.count);
    for (size_t i = 0; i < rewrites; ++i) {
        // Spread the rewrites across the series rather than clustering at the
        // front: a rewrite in the middle of a block exercises intra-block dedup,
        // one at a block edge exercises the cross-block merge.
        const size_t idx = (w.count == 0) ? 0 : ((i * 7 + 3) % w.count);
        auto [json, canon] = makeValue(w, rng);
        g.rewriteTs.push_back(g.timestamps[idx]);
        g.rewriteJson.push_back(std::move(json));
        finalCanonical[idx] = std::move(canon);
    }

    // ---- expected visible state: one value per distinct timestamp ----
    g.expectedTs = g.timestamps;
    g.expectedCanonical = std::move(finalCanonical);
    return g;
}

// COVERAGE BY CONSTRUCTION for the known-risky axes. Pure random sampling left
// the (string x multi-block) cell -- the one that actually held a bug --
// unvisited in a default-length run. The first pass enumerates every
// (type x count) pair deterministically; later iterations sample randomly.
Workload generateWorkload(size_t index, uint64_t seed) {
    std::mt19937_64 rng(seed + index);
    const FieldType types[] = {FieldType::Float, FieldType::Integer, FieldType::Bool, FieldType::String};
    const auto counts = interestingCounts();
    const size_t combos = 4 * counts.size();

    Workload w;
    w.index = index;
    w.valueSeed = seed + index;
    if (index < combos) {
        w.type = types[index % 4];
        w.count = counts[(index / 4) % counts.size()];
    } else {
        w.type = types[rng() % 4];
        w.count = (rng() % 2 == 0) ? counts[rng() % counts.size()] : (1 + (rng() % (3 * MaxPointsPerBlock())));
    }
    w.irregular = (rng() % 3) == 0;
    w.baseStepSec = 1 + (rng() % 5);
    // Low cardinality (<= 50 uniques) takes the STR2 dictionary path; high
    // cardinality takes the raw STRG path. Both are real branches in
    // decodeBlockFlat.
    w.stringCardinality = (rng() % 2 == 0) ? (1 + rng() % 40) : 5000;

    // Rewrites (last-write-wins) on a third of workloads.
    w.rewriteCount = (rng() % 3 == 0) ? (1 + rng() % std::max<size_t>(1, w.count / 4)) : 0;
    // Specials on half the float workloads.
    w.includeSpecials = (w.type == FieldType::Float) && (rng() % 2 == 0);

    // Query shape. Placement invariance holds for every method, so this widens
    // the checked surface without needing an oracle for each one.
    const AggMethod methods[] = {AggMethod::Latest, AggMethod::First, AggMethod::Min, AggMethod::Max,
                                 AggMethod::Count,  AggMethod::Sum,   AggMethod::Avg};
    do {
        w.method = methods[rng() % 7];
    } while (!methodAppliesTo(w.method, w.type));
    // 0 = raw, otherwise an interval that is sometimes far smaller and sometimes
    // far larger than the data's own span.
    const uint64_t intervals[] = {0, 1, 60, 3600, 86400};
    w.intervalSec = intervals[rng() % 5];
    return w;
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

// Returns a failure description, or nullopt on success. NOTHING in the property
// path uses ASSERT_*/EXPECT_*: a failing candidate must be an ordinary value so
// the shrinker can re-run it, and only the final minimised case is reported to
// gtest.
std::optional<std::string> writeSeries(HttpWriteHandler& handler, const GeneratedSeries& g) {
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
            return "write failed: " + std::string(rep->_content.substr(0, 200));
        }
    }

    // Second pass: rewrite a subset of the same timestamps. These must OVERWRITE
    // (last-write-wins), not accumulate as duplicates.
    for (size_t start = 0; start < g.rewriteTs.size(); start += kBatch) {
        const size_t end = std::min(g.rewriteTs.size(), start + kBatch);
        std::string ts = "[";
        std::string vs = "[";
        for (size_t i = start; i < end; ++i) {
            if (i > start) {
                ts += ",";
                vs += ",";
            }
            ts += std::to_string(g.rewriteTs[i]);
            vs += g.rewriteJson[i];
        }
        ts += "]";
        vs += "]";
        const std::string body = R"({"measurement":")" + g.measurement + R"(","tags":{"host":")" + g.tagValue +
                                 R"("},"fields":{")" + g.field + R"(":)" + vs + R"(},"field_types":{")" + g.field +
                                 R"(":")" + typeName(g.type) + R"("},"timestamps":)" + ts + "}";
        auto rep = handler.handleWrite(jsonRequest(body)).get();
        if (rep->_status != seastar::http::reply::status_type::ok) {
            return "rewrite failed: " + std::string(rep->_content.substr(0, 200));
        }
    }
    return std::nullopt;
}

struct QueryResult {
    bool ok = false;
    std::string error;
    std::vector<uint64_t> timestamps;
    std::vector<std::string> values;  // canonical strings
    size_t rawTimestampCount = 0;     // as reported, before pairing
    size_t rawValueCount = 0;
};

// Canonicalise so float/int/bool/string all compare as strings.
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
                     uint64_t start, uint64_t end, const std::string& interval, AggMethod method = AggMethod::Latest) {
    QueryResult out;
    std::string body = R"({"query":")" + std::string(methodName(method)) + ":" + measurement + "(" + field +
                       R"(){}","startTime":)" + std::to_string(start) + R"(,"endTime":)" + std::to_string(end);
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

// Waits for the rollover to actually LAND data in TSM, not merely for a file to
// have existed at some point. Iterations are isolated (shard dirs cleaned per
// run), so "a file exists" means this run's data.
//
// It waits for existence rather than for the count to INCREASE because a large
// series auto-rolls while it is still being written, so by flush time there may
// be nothing left to convert.
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
// Properties -- each returns a failure description or nullopt
// ---------------------------------------------------------------------------

// P2: |timestamps| == |values|. A mismatch means a desynced pair reached the
// API -- downstream code indexes values by a timestamp index, so this is an
// out-of-bounds read, not merely a wrong number.
std::optional<std::string> checkLengthCoherence(const QueryResult& r, const std::string& ctx) {
    if (r.rawTimestampCount != r.rawValueCount) {
        return ctx + ": response carried " + std::to_string(r.rawTimestampCount) + " timestamps but " +
               std::to_string(r.rawValueCount) + " values -- a desynced timestamp/value pair";
    }
    return std::nullopt;
}

// P8: every returned timestamp was actually written.
std::optional<std::string> checkNoPhantomTimestamps(const QueryResult& r, const std::set<uint64_t>& written,
                                                    const std::string& ctx) {
    for (uint64_t ts : r.timestamps) {
        if (written.count(ts) == 0) {
            return ctx + ": returned timestamp " + std::to_string(ts) +
                   " was never written -- a phantom point (decoded past the end of the encoded run)";
        }
    }
    return std::nullopt;
}

std::optional<std::string> expectEqual(const std::vector<uint64_t>& got, const std::vector<uint64_t>& want,
                                       const std::string& what) {
    if (got == want) {
        return std::nullopt;
    }
    return what + " (got " + std::to_string(got.size()) + " values, expected " + std::to_string(want.size()) + ")";
}

std::optional<std::string> expectEqual(const std::vector<std::string>& got, const std::vector<std::string>& want,
                                       const std::string& what) {
    if (got == want) {
        return std::nullopt;
    }
    std::string detail =
        what + " (got " + std::to_string(got.size()) + " values, expected " + std::to_string(want.size()) + ")";
    for (size_t i = 0; i < std::min(got.size(), want.size()); ++i) {
        if (got[i] != want[i]) {
            detail += "; first difference at index " + std::to_string(i) + ": got '" + got[i] + "' expected '" +
                      want[i] + "'";
            break;
        }
    }
    return detail;
}

// Runs every property against one workload. Returns the FIRST failure, or
// nullopt. Self-contained: builds and tears down its own engine, so the shrinker
// can call it freely.
// Compare two value lists under the tolerance the METHOD deserves. SUM/AVG may
// differ in the last bits across placements (different fold order, and IEEE
// addition is not associative); everything else selects or counts and must match
// exactly. Getting this backwards yields either false alarms or a blind spot.
std::optional<std::string> compareValues(const std::vector<std::string>& got, const std::vector<std::string>& want,
                                         AggMethod method, const std::string& what) {
    if (got.size() != want.size()) {
        return what + ": " + std::to_string(got.size()) + " values vs " + std::to_string(want.size());
    }
    const bool exact = methodRequiresExactMatch(method);
    for (size_t i = 0; i < got.size(); ++i) {
        if (got[i] == want[i]) {
            continue;
        }
        if (exact) {
            return what + ": index " + std::to_string(i) + " got '" + got[i] + "' expected '" + want[i] + "'";
        }
        // Accumulating method: allow a relative epsilon, but only between two
        // finite numbers. null/inf mismatches are still real differences.
        try {
            const double a = std::stod(got[i]);
            const double b = std::stod(want[i]);
            if (std::isfinite(a) && std::isfinite(b)) {
                const double scale = std::max({1.0, std::fabs(a), std::fabs(b)});
                if (std::fabs(a - b) <= 1e-9 * scale) {
                    continue;
                }
            }
        } catch (const std::exception&) {
            // not numeric -- fall through to report
        }
        return what + ": index " + std::to_string(i) + " got '" + got[i] + "' expected '" + want[i] +
               "' (beyond accumulation tolerance)";
    }
    return std::nullopt;
}

// P4: bucket starts must be epoch-aligned, strictly ascending, unique, and
// inside the queried range.
std::optional<std::string> checkBucketWellFormed(const QueryResult& r, uint64_t intervalNs, uint64_t start,
                                                 uint64_t end, const std::string& ctx) {
    if (intervalNs == 0) {
        return std::nullopt;
    }
    for (size_t i = 0; i < r.timestamps.size(); ++i) {
        const uint64_t ts = r.timestamps[i];
        if (ts % intervalNs != 0) {
            return ctx + ": bucket start " + std::to_string(ts) + " is not aligned to the interval";
        }
        if (i > 0 && ts <= r.timestamps[i - 1]) {
            return ctx + ": bucket starts are not strictly ascending at index " + std::to_string(i);
        }
        // The bucket CONTAINING start may begin before it; anything beyond end
        // was never asked for.
        if (ts > end) {
            return ctx + ": bucket start " + std::to_string(ts) + " lies past endTime " + std::to_string(end);
        }
        (void)start;
    }
    return std::nullopt;
}

// Every run needs a measurement name no earlier run in this PROCESS has used.
//
// Cleaning the shard directories is not sufficient isolation: recreating an
// engine over wiped directories leaves process-level state that makes a
// previously-seen measurement resolve to nothing, so the second and later runs
// of the same name return an empty result no matter what was written. Measured
// directly: reusing one name across 12 engine instances failed 11 times; making
// the name unique failed 0 times.
//
// This bit the shrinker before it was fixed: shrink probes re-ran the SAME
// workload index, every probe after the first came back empty, and the shrinker
// read that as "still fails" and happily minimised into a phantom bug.
size_t nextRunId() {
    static size_t counter = 0;
    return counter++;
}

std::optional<std::string> runWorkload(const Workload& w) {
    // Isolate runs: they otherwise share shard directories, so a previous run's
    // TSM files make this one's flush check meaningless.
    cleanTestShardDirectories();

    ScopedShardedEngine eng;
    eng.start();
    HttpWriteHandler writeHandler(&eng.eng);
    HttpQueryHandler queryHandler(&eng.eng);

    GeneratedSeries g = materialize(w);
    g.measurement += "_r" + std::to_string(nextRunId());
    if (auto err = writeSeries(writeHandler, g)) {
        return err;
    }

    const uint64_t first = g.timestamps.front();
    // +1ns: the API requires startTime < endTime, and a single-point series
    // would otherwise be an empty range. endTime is inclusive, so this cannot
    // pull in points that were not written.
    const uint64_t last = g.timestamps.back() + 1;
    const std::set<uint64_t> written(g.timestamps.begin(), g.timestamps.end());

    const std::string intervalStr = w.intervalSec == 0 ? "" : (std::to_string(w.intervalSec) + "s");
    const uint64_t intervalNs = w.intervalSec * kSec;

    // ---- before flush (memory-resident) ----
    // The raw LATEST read is the one case with a known answer (stored values,
    // verbatim), so it doubles as the LWW oracle below.
    auto rawBefore = runQuery(queryHandler, g.measurement, g.field, first, last, "");
    if (!rawBefore.ok) {
        return "raw/before: " + rawBefore.error;
    }
    if (auto e = checkLengthCoherence(rawBefore, "raw/before")) {
        return e;
    }
    if (auto e = checkNoPhantomTimestamps(rawBefore, written, "raw/before")) {
        return e;
    }
    // P9 (LWW): one value per distinct timestamp, newest wins. Duplicates must
    // overwrite -- never appear twice, never resurrect the older value.
    if (auto e = expectEqual(rawBefore.timestamps, g.expectedTs,
                             "raw/before: timestamps differ from what was written (duplicates not deduped?)")) {
        return e;
    }
    if (auto e = expectEqual(rawBefore.values, g.expectedCanonical,
                             "raw/before: values differ from what was written (last-write-wins broken?)")) {
        return e;
    }

    // P6 (idempotence): the same query twice must give the same answer.
    auto rawRepeat = runQuery(queryHandler, g.measurement, g.field, first, last, "");
    if (!rawRepeat.ok) {
        return "raw/repeat: " + rawRepeat.error;
    }
    if (auto e = expectEqual(rawRepeat.timestamps, rawBefore.timestamps,
                             "the same query returned different timestamps on a second run")) {
        return e;
    }
    if (auto e = expectEqual(rawRepeat.values, rawBefore.values,
                             "the same query returned different values on a second run")) {
        return e;
    }

    // P7 (range monotonicity): a narrower range must not surface anything the
    // wider range omitted.
    if (g.timestamps.size() >= 4) {
        const uint64_t mid = g.timestamps[g.timestamps.size() / 2];
        auto narrow = runQuery(queryHandler, g.measurement, g.field, first, mid, "");
        if (!narrow.ok) {
            return "raw/narrow: " + narrow.error;
        }
        const std::set<uint64_t> wide(rawBefore.timestamps.begin(), rawBefore.timestamps.end());
        for (uint64_t ts : narrow.timestamps) {
            if (wide.count(ts) == 0) {
                return "narrowing the range surfaced timestamp " + std::to_string(ts) +
                       " that the wider range did not return";
            }
        }
    }

    // The workload's own query shape (method x interval). Placement invariance
    // needs no oracle, which is what lets the method vary freely.
    auto shapedBefore = runQuery(queryHandler, g.measurement, g.field, first, last, intervalStr, w.method);
    if (!shapedBefore.ok) {
        return "shaped/before: " + shapedBefore.error;
    }
    if (auto e = checkLengthCoherence(shapedBefore, "shaped/before")) {
        return e;
    }
    if (auto e = checkBucketWellFormed(shapedBefore, intervalNs, first, last, "shaped/before")) {
        return e;
    }

    // ---- P3: range decomposition ----
    // Splitting forces the FILTERED decode path for the sub-ranges while the
    // whole-range read may not take it; only a split disagrees.
    auto checkSplit = [&](const QueryResult& whole, const std::string& phase) -> std::optional<std::string> {
        if (g.timestamps.size() < 4) {
            return std::nullopt;
        }
        const uint64_t mid = g.timestamps[g.timestamps.size() / 2];
        auto lower = runQuery(queryHandler, g.measurement, g.field, first, mid - 1, "");
        auto upper = runQuery(queryHandler, g.measurement, g.field, mid, last, "");
        if (!lower.ok) {
            return phase + " split/lower: " + lower.error;
        }
        if (!upper.ok) {
            return phase + " split/upper: " + upper.error;
        }
        if (auto e = checkLengthCoherence(lower, phase + " split/lower")) {
            return e;
        }
        if (auto e = checkLengthCoherence(upper, phase + " split/upper")) {
            return e;
        }
        if (auto e = checkNoPhantomTimestamps(lower, written, phase + " split/lower")) {
            return e;
        }
        if (auto e = checkNoPhantomTimestamps(upper, written, phase + " split/upper")) {
            return e;
        }

        std::vector<uint64_t> joinedTs = lower.timestamps;
        joinedTs.insert(joinedTs.end(), upper.timestamps.begin(), upper.timestamps.end());
        std::vector<std::string> joinedVals = lower.values;
        joinedVals.insert(joinedVals.end(), upper.values.begin(), upper.values.end());

        if (auto e = expectEqual(joinedTs, whole.timestamps, phase + ": [a,b] ++ (b,c] TIMESTAMPS differ from [a,c]")) {
            return e;
        }
        if (auto e = expectEqual(joinedVals, whole.values, phase + ": [a,b] ++ (b,c] VALUES differ from [a,c]")) {
            return e;
        }
        return std::nullopt;
    };

    if (auto e = checkSplit(rawBefore, "before flush")) {
        return e;
    }

    // ---- flush: same data, different physical layout ----
    // The placement property is only meaningful if the data actually moved.
    if (!flushToTsm(eng.eng)) {
        return "data never reached TSM after rollover -- the placement-invariance check would have "
               "compared memstore against memstore and passed vacuously";
    }

    // ---- P1: placement invariance ----
    auto rawAfter = runQuery(queryHandler, g.measurement, g.field, first, last, "");
    if (!rawAfter.ok) {
        return "raw/after: " + rawAfter.error;
    }
    if (auto e = checkLengthCoherence(rawAfter, "raw/after")) {
        return e;
    }
    if (auto e = checkNoPhantomTimestamps(rawAfter, written, "raw/after")) {
        return e;
    }
    if (auto e = expectEqual(rawAfter.timestamps, rawBefore.timestamps,
                             "raw TIMESTAMPS changed across a TSM flush -- the answer depends on placement")) {
        return e;
    }
    if (auto e = expectEqual(rawAfter.values, rawBefore.values,
                             "raw VALUES changed across a TSM flush -- the answer depends on placement")) {
        return e;
    }

    auto shapedAfter = runQuery(queryHandler, g.measurement, g.field, first, last, intervalStr, w.method);
    if (!shapedAfter.ok) {
        return "shaped/after: " + shapedAfter.error;
    }
    if (auto e = checkLengthCoherence(shapedAfter, "shaped/after")) {
        return e;
    }
    if (auto e = checkBucketWellFormed(shapedAfter, intervalNs, first, last, "shaped/after")) {
        return e;
    }
    if (auto e = expectEqual(
            shapedAfter.timestamps, shapedBefore.timestamps,
            std::string("timestamps for method '") + methodName(w.method) + "' changed across a TSM flush")) {
        return e;
    }
    if (auto e =
            compareValues(shapedAfter.values, shapedBefore.values, w.method,
                          std::string("values for method '") + methodName(w.method) + "' changed across a TSM flush")) {
        return e;
    }

    // ---- P3 again, now multi-block on disk ----
    if (auto e = checkSplit(rawAfter, "after flush")) {
        return e;
    }

    return std::nullopt;
}

// ---------------------------------------------------------------------------
// Shrinking
//
// A failure over 3000 random points is not actionable. Minimise one field at a
// time, keeping any change that still fails, and report only the reduced case.
//
// Every probe rebuilds an engine, so the budget is bounded; the count search is
// a binary search rather than linear because count is the field that dominates
// both the reproducer's size and the run time.
// ---------------------------------------------------------------------------
struct ShrinkStats {
    size_t probes = 0;
    size_t budget = 40;
    bool exhausted() const { return probes >= budget; }
};

// Digits vary between runs ("got 3000 values, expected 3001"), so compare the
// SHAPE of a failure rather than its text.
std::string failureSignature(const std::string& msg) {
    std::string out;
    bool inNumber = false;
    for (char c : msg) {
        if (std::isdigit(static_cast<unsigned char>(c))) {
            if (!inNumber) {
                out += '#';
                inNumber = true;
            }
        } else {
            out += c;
            inNumber = false;
        }
    }
    return out;
}

// A candidate counts as "still failing" only when it fails THE SAME WAY.
// Accepting any failure lets the shrinker walk into an unrelated bug and report
// a minimal case that never demonstrated the original problem -- which is
// exactly what happened before this check existed.
bool stillFails(const Workload& w, const std::string& wantedSignature, ShrinkStats& stats) {
    if (stats.exhausted()) {
        return false;
    }
    ++stats.probes;
    auto result = runWorkload(w);
    return result.has_value() && failureSignature(*result) == wantedSignature;
}

Workload shrinkWorkload(const Workload& failing, const std::string& wantedSignature, ShrinkStats& stats) {
    Workload best = failing;

    // 1. count -- binary search for the smallest count that still fails. This
    //    also LOCATES the boundary: a minimum that lands just past
    //    MaxPointsPerBlock() is itself the diagnosis (multi-block).
    if (best.count > 1) {
        size_t lo = 1, hi = best.count;
        while (lo < hi && !stats.exhausted()) {
            const size_t mid = lo + (hi - lo) / 2;
            Workload cand = best;
            cand.count = mid;
            if (stillFails(cand, wantedSignature, stats)) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (hi < best.count) {
            Workload cand = best;
            cand.count = hi;
            if (stillFails(cand, wantedSignature, stats)) {
                best = cand;
            }
        }
    }

    // 2. irregular spacing -> regular
    if (best.irregular) {
        Workload cand = best;
        cand.irregular = false;
        if (stillFails(cand, wantedSignature, stats)) {
            best = cand;
        }
    }

    // 3. step -> 1s
    if (best.baseStepSec > 1) {
        Workload cand = best;
        cand.baseStepSec = 1;
        if (stillFails(cand, wantedSignature, stats)) {
            best = cand;
        }
    }

    // 4. string cardinality -> 1 (a single repeated value is the simplest case,
    //    and also forces the STR2 dictionary path)
    if (best.type == FieldType::String && best.stringCardinality > 1) {
        Workload cand = best;
        cand.stringCardinality = 1;
        if (stillFails(cand, wantedSignature, stats)) {
            best = cand;
        }
    }

    // 5. simplest values
    if (best.valueSeed != 0) {
        Workload cand = best;
        cand.valueSeed = 0;
        if (stillFails(cand, wantedSignature, stats)) {
            best = cand;
        }
    }

    return best;
}

}  // namespace

class StackPropertyFuzzTest : public ::testing::Test {
protected:
    void SetUp() override { cleanTestShardDirectories(); }
    void TearDown() override { cleanTestShardDirectories(); }
};

// One iteration: generate a workload, run every property, and on failure shrink
// it before reporting, so the message names a minimal case rather than the
// random one that happened to trip.
TEST_F(StackPropertyFuzzTest, WellFormedDataSurvivesPlacementAndRangeSplitting) {
    const size_t seed = envSize("TIMESTAR_FUZZ_SEED", 20260720);
    // Default covers the full (type x count) cross-product exactly once.
    const size_t iterations = envSize("TIMESTAR_FUZZ_ITERATIONS", 4 * interestingCounts().size());

    seastar::thread([seed, iterations] {
        for (size_t iter = 0; iter < iterations; ++iter) {
            const Workload w = generateWorkload(iter, seed);
            auto failure = runWorkload(w);
            if (!failure) {
                continue;
            }

            ShrinkStats stats;
            const Workload minimal = shrinkWorkload(w, failureSignature(*failure), stats);
            auto minimalFailure = runWorkload(minimal);

            ADD_FAILURE() << "property violated (seed=" << seed << " iteration=" << iter << ")\n"
                          << "  original: " << w.describe() << "\n"
                          << "    -> " << *failure << "\n"
                          << "  shrunk:   " << minimal.describe() << " (" << stats.probes << " shrink probes)\n"
                          << "    -> " << (minimalFailure ? *minimalFailure : std::string("(no longer fails)")) << "\n"
                          << minimal.reproducer() << "Replay the whole run with: TIMESTAR_FUZZ_SEED=" << seed << "\n";
            return;  // one failure is enough; continuing only buries it
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
