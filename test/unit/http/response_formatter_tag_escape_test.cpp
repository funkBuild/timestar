#include "../../../lib/http/response_formatter.hpp"
#include "../../../lib/http/http_query_handler.hpp"

#include <glaze/glaze.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace timestar;

// ── Glaze structures for round-trip parsing ──────────────────────────

struct FieldDataParsed {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

template <>
struct glz::meta<FieldDataParsed> {
    using T = FieldDataParsed;
    static constexpr auto value = object("timestamps", &T::timestamps, "values", &T::values);
};

struct SeriesParsed {
    std::string measurement;
    std::vector<std::string> groupTags;
    std::map<std::string, FieldDataParsed> fields;
};

template <>
struct glz::meta<SeriesParsed> {
    using T = SeriesParsed;
    static constexpr auto value = object("measurement", &T::measurement, "groupTags", &T::groupTags, "fields", &T::fields);
};

struct StatsParsed {
    int64_t series_count = 0;
    int64_t point_count = 0;
    double execution_time_ms = 0.0;
};

template <>
struct glz::meta<StatsParsed> {
    using T = StatsParsed;
    static constexpr auto value =
        object("series_count", &T::series_count, "point_count", &T::point_count, "execution_time_ms", &T::execution_time_ms);
};

struct FullResponseParsed {
    std::string status;
    std::vector<SeriesParsed> series;
    StatsParsed statistics;
};

template <>
struct glz::meta<FullResponseParsed> {
    using T = FullResponseParsed;
    static constexpr auto value = object("status", &T::status, "series", &T::series, "statistics", &T::statistics);
};

// ── Helpers ──────────────────────────────────────────────────────────

// Build a minimal QueryResponse with one series containing the given tags
// and a single data point so the series is non-empty.
static QueryResponse buildResponseWithTags(std::map<std::string, std::string> tags) {
    QueryResponse resp;
    resp.success = true;
    resp.statistics.seriesCount = 1;
    resp.statistics.pointCount = 1;
    resp.statistics.executionTimeMs = 1.0;

    SeriesResult sr;
    sr.measurement = "test_metric";
    sr.tags = std::move(tags);
    sr.fields["value"] = {std::vector<uint64_t>{1000000000ULL}, std::vector<double>{42.0}};

    resp.series.push_back(std::move(sr));
    return resp;
}

// Parse a "key=value" groupTag string at the first unescaped '=' after
// JSON-level unescaping (which glaze already did for us).
static std::pair<std::string, std::string> splitGroupTag(const std::string& tag) {
    auto eq = tag.find('=');
    if (eq == std::string::npos) return {tag, ""};
    return {tag.substr(0, eq), tag.substr(eq + 1)};
}

// ── Tests ────────────────────────────────────────────────────────────

class ResponseFormatterTagEscapeTest : public ::testing::Test {};

TEST_F(ResponseFormatterTagEscapeTest, TagValueWithDoubleQuoteProducesValidJson) {
    auto resp = buildResponseWithTags({{"host", "server-\"01\""}});
    std::string json = ResponseFormatter::format(resp);

    // Must parse as valid JSON
    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with quoted tag value failed to parse: " << json;

    // Round-trip: verify the tag value survived
    ASSERT_EQ(parsed.series.size(), 1u);
    ASSERT_EQ(parsed.series[0].groupTags.size(), 1u);
    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, "host");
    EXPECT_EQ(value, "server-\"01\"");
}

TEST_F(ResponseFormatterTagEscapeTest, TagValueWithBackslashProducesValidJson) {
    auto resp = buildResponseWithTags({{"path", R"(C:\data\tsm)"}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with backslash tag value failed to parse: " << json;

    ASSERT_EQ(parsed.series[0].groupTags.size(), 1u);
    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, "path");
    EXPECT_EQ(value, R"(C:\data\tsm)");
}

TEST_F(ResponseFormatterTagEscapeTest, TagValueWithControlCharsProducesValidJson) {
    std::string malicious = "val\x01ue\twith\nnewline";
    auto resp = buildResponseWithTags({{"tag", malicious}});
    std::string json = ResponseFormatter::format(resp);

    // Raw control characters must not appear in the JSON output
    EXPECT_EQ(json.find('\x01'), std::string::npos) << "Raw 0x01 must be escaped";
    EXPECT_EQ(json.find('\n'), std::string::npos) << "Raw newline must be escaped";

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with control-char tag value failed to parse: " << json;

    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, "tag");
    EXPECT_EQ(value, malicious);
}

TEST_F(ResponseFormatterTagEscapeTest, TagKeyWithSpecialCharsProducesValidJson) {
    // Tag KEY containing quotes and backslashes
    auto resp = buildResponseWithTags({{R"(my"key\here)", "normal_value"}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with special-char tag key failed to parse: " << json;

    ASSERT_EQ(parsed.series[0].groupTags.size(), 1u);
    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, R"(my"key\here)");
    EXPECT_EQ(value, "normal_value");
}

TEST_F(ResponseFormatterTagEscapeTest, BothKeyAndValueNeedEscaping) {
    auto resp = buildResponseWithTags({{R"(k"1)", R"(v"2\3)"}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with both key+value needing escaping failed to parse: " << json;

    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, R"(k"1)");
    EXPECT_EQ(value, R"(v"2\3)");
}

TEST_F(ResponseFormatterTagEscapeTest, MultipleTagsSomeNeedEscaping) {
    std::map<std::string, std::string> tags = {
        {"clean_key", "clean_value"},
        {"dirty\"key", "dirty\\value"},
        {"location", "us-west"},
    };
    auto resp = buildResponseWithTags(tags);
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with mixed clean/dirty tags failed to parse: " << json;

    // std::map iterates in sorted order: clean_key, dirty"key, location
    ASSERT_EQ(parsed.series[0].groupTags.size(), 3u);

    // Verify all three round-trip correctly
    std::map<std::string, std::string> recovered;
    for (const auto& gt : parsed.series[0].groupTags) {
        auto [k, v] = splitGroupTag(gt);
        recovered[k] = v;
    }
    EXPECT_EQ(recovered["clean_key"], "clean_value");
    EXPECT_EQ(recovered["dirty\"key"], "dirty\\value");
    EXPECT_EQ(recovered["location"], "us-west");
}

TEST_F(ResponseFormatterTagEscapeTest, TagValueIsEntirelyQuotes) {
    auto resp = buildResponseWithTags({{"x", "\"\"\""}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "JSON with all-quotes tag value failed to parse: " << json;

    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, "x");
    EXPECT_EQ(value, "\"\"\"");
}

TEST_F(ResponseFormatterTagEscapeTest, InjectionAttemptCannotBreakJson) {
    // Attempt to inject a new JSON key via tag value
    std::string injection = R"(val","injected":"evil)";
    auto resp = buildResponseWithTags({{"tag", injection}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "Injection attempt produced invalid JSON: " << json;

    // The injected key must NOT appear as a real JSON key
    auto [key, value] = splitGroupTag(parsed.series[0].groupTags[0]);
    EXPECT_EQ(key, "tag");
    EXPECT_EQ(value, injection) << "Injection payload must be preserved verbatim as a value, not interpreted as JSON structure";

    // Double-check: there should be exactly one series with exactly one groupTag
    EXPECT_EQ(parsed.series.size(), 1u);
    EXPECT_EQ(parsed.series[0].groupTags.size(), 1u);
}

TEST_F(ResponseFormatterTagEscapeTest, NormalTagsStillWorkAfterFix) {
    // Regression check: normal ASCII tags must still format correctly
    auto resp = buildResponseWithTags({{"host", "server-01"}, {"region", "us-east-1"}});
    std::string json = ResponseFormatter::format(resp);

    FullResponseParsed parsed;
    auto err = glz::read_json(parsed, json);
    ASSERT_FALSE(bool(err)) << "Normal tags failed to parse after escaping fix: " << json;

    ASSERT_EQ(parsed.series[0].groupTags.size(), 2u);
    std::map<std::string, std::string> recovered;
    for (const auto& gt : parsed.series[0].groupTags) {
        auto [k, v] = splitGroupTag(gt);
        recovered[k] = v;
    }
    EXPECT_EQ(recovered["host"], "server-01");
    EXPECT_EQ(recovered["region"], "us-east-1");
}
