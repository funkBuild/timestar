// Regression tests for measurement / field / tag names containing spaces (and
// other structural characters).  Prior to the escaping fix, buildSeriesKey used
// unescaped ',', '=', ' ' delimiters, so:
//   - names with a space were REJECTED at write validation, and
//   - had they been allowed, distinct series could collide onto one key
//     (e.g. ("a b",{},"c") and ("a",{},"b c") both -> "a b c").
//
// These tests pin the two guarantees the fix provides:
//   1. buildSeriesKey is bijective (distinct tuples -> distinct keys), and
//   2. SeriesKeyParser(buildSeriesKey(m,t,f)) round-trips back to (m,t,f),
//      which the WAL-replay / delete-by-key paths rely on.

#include "line_parser.hpp"
#include "series_key.hpp"

#include <gtest/gtest.h>

#include <map>
#include <set>
#include <string>
#include <vector>

using timestar::buildSeriesKey;

namespace {

// Verbatim from the production logs that surfaced the bug.
const std::vector<std::string> kSpacedMeasurements = {
    "Transfer Pump", "weather station", "Chiller System", "Wet Well Level", "ATS Stats", "Wash Down Bay System",
};
const std::vector<std::string> kSpacedFields = {
    "Home 5 Flow",
    "Hill 1 Flow",
    "Home 1 Flow",
    "Hill 6 Flow",
};

using Tags = std::map<std::string, std::string>;

// Build a key from (m, tags, f), parse it back, and assert every component is
// reconstructed exactly.
void expectRoundTrip(const std::string& measurement, const Tags& tags, const std::string& field) {
    const std::string key = buildSeriesKey(measurement, tags, field);
    SeriesKeyParser parser(key);
    EXPECT_EQ(parser.measurement, measurement) << "key=[" << key << "]";
    EXPECT_EQ(parser.field, field) << "key=[" << key << "]";
    ASSERT_EQ(parser.tags.size(), tags.size()) << "key=[" << key << "]";
    for (const auto& [k, v] : tags) {
        auto it = parser.tags.find(k);
        ASSERT_NE(it, parser.tags.end()) << "missing tag key [" << k << "] in [" << key << "]";
        EXPECT_EQ(it->second, v) << "tag [" << k << "] in [" << key << "]";
    }
}

}  // namespace

// --- Round-trip: the exact strings from the logs -----------------------------

TEST(SeriesKeyEscapingTest, RoundTripSpacedMeasurements) {
    for (const auto& m : kSpacedMeasurements) {
        expectRoundTrip(m, {}, "value");
    }
}

TEST(SeriesKeyEscapingTest, RoundTripSpacedFields) {
    for (const auto& f : kSpacedFields) {
        expectRoundTrip("cpu", {}, f);
    }
}

TEST(SeriesKeyEscapingTest, RoundTripSpacedMeasurementAndField) {
    for (const auto& m : kSpacedMeasurements) {
        for (const auto& f : kSpacedFields) {
            expectRoundTrip(m, {}, f);
        }
    }
}

TEST(SeriesKeyEscapingTest, RoundTripSpacedMeasurementFieldAndTags) {
    for (const auto& m : kSpacedMeasurements) {
        for (const auto& f : kSpacedFields) {
            expectRoundTrip(m, {{"site name", "North Plant"}, {"unit", "Pump 3"}}, f);
        }
    }
}

// --- Injectivity: no collisions ---------------------------------------------

TEST(SeriesKeyEscapingTest, SpaceAmbiguityDoesNotCollide) {
    // The canonical collision the old unescaped format produced.
    const std::string a = buildSeriesKey("a b", {}, "c");
    const std::string b = buildSeriesKey("a", {}, "b c");
    EXPECT_NE(a, b);
    // Both still round-trip to their own components.
    expectRoundTrip("a b", {}, "c");
    expectRoundTrip("a", {}, "b c");
}

TEST(SeriesKeyEscapingTest, CommaAmbiguityDoesNotCollide) {
    // Measurement with a comma vs. a measurement + tag.
    const std::string a = buildSeriesKey("a,b=c", {}, "f");
    const std::string b = buildSeriesKey("a", {{"b", "c"}}, "f");
    EXPECT_NE(a, b);
    expectRoundTrip("a,b=c", {}, "f");
}

TEST(SeriesKeyEscapingTest, AllLoggedCombinationsAreUnique) {
    // Every (measurement, field) pair from the logs must map to a distinct key.
    std::set<std::string> keys;
    size_t count = 0;
    for (const auto& m : kSpacedMeasurements) {
        for (const auto& f : kSpacedFields) {
            keys.insert(buildSeriesKey(m, {}, f));
            ++count;
        }
    }
    EXPECT_EQ(keys.size(), count) << "collision among logged measurement/field combinations";
}

// --- Other structural characters --------------------------------------------

TEST(SeriesKeyEscapingTest, RoundTripStructuralCharsInNames) {
    // comma, equals, backslash, quote, and mixes — all must round-trip.
    expectRoundTrip("comma,measure", {}, "field,name");
    expectRoundTrip("eq=measure", {}, "eq=field");
    expectRoundTrip("back\\slash", {}, "field\\x");
    expectRoundTrip("quote\"measure", {}, "quote\"field");
    expectRoundTrip("all ,=\"\\ chars", {{"t ,=", "v ,=\""}}, "f ,=\"\\");
}

TEST(SeriesKeyEscapingTest, RoundTripTagValuesWithSpaces) {
    // Tag values already accepted spaces before the fix, but produced ambiguous
    // keys.  They must now round-trip and be collision-free.
    expectRoundTrip("cpu", {{"host", "my server 01"}}, "value");
    const std::string a = buildSeriesKey("cpu", {{"host", "b c"}}, "d");
    const std::string b = buildSeriesKey("cpu", {{"host", "b"}}, "c d");
    EXPECT_NE(a, b);
}

// --- Backward compatibility: no data migration ------------------------------

TEST(SeriesKeyEscapingTest, PlainNamesProduceIdenticalKey) {
    // Names without structural characters must serialize byte-identically to the
    // pre-fix format, so existing series hash to the same SeriesId128.
    EXPECT_EQ(buildSeriesKey("cpu", {}, "value"), "cpu value");
    EXPECT_EQ(buildSeriesKey("cpu", {{"host", "server01"}}, "value"), "cpu,host=server01 value");
    EXPECT_EQ(buildSeriesKey("cpu", {{"host", "server01"}, {"region", "us-west"}}, "value"),
              "cpu,host=server01,region=us-west value");
}
