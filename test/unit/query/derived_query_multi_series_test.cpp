#include "derived_query.hpp"
#include "derived_query_executor.hpp"
#include "http_query_handler.hpp"

#include <gtest/gtest.h>

#include <cmath>
#include <set>
#include <string>

using namespace timestar;

class DerivedQueryMultiSeriesTest : public ::testing::Test {
protected:
    // Helper: create a DerivedQueryExecutor with a null engine
    // (convertQueryResponse doesn't use it)
    DerivedQueryExecutor makeExecutor() { return DerivedQueryExecutor(nullptr); }

    // Helper: create a simple QueryRequest
    QueryRequest makeQuery(const std::string& measurement, const std::string& field) {
        QueryRequest req;
        req.measurement = measurement;
        req.fields.push_back(field);
        req.aggregation = AggregationMethod::AVG;
        req.startTime = 1000000000;
        req.endTime = 2000000000;
        return req;
    }

    // Helper: create a SeriesResult with double values
    SeriesResult makeSeries(const std::string& measurement, const std::map<std::string, std::string>& tags,
                            const std::string& fieldName, const std::vector<uint64_t>& timestamps,
                            const std::vector<double>& values) {
        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        FieldValues fv = values;
        sr.fields[fieldName] = std::make_pair(timestamps, fv);
        return sr;
    }

    // Expose the private convertQueryResponse for testing via friend access.
    // convertQueryResponse consumes its input (rvalue ref); copy here so the
    // tests can keep passing lvalues.
    SubQueryResult callConvert(DerivedQueryExecutor& executor, const std::string& name, const QueryRequest& query,
                               const std::vector<SeriesResult>& results) {
        auto owned = results;
        return executor.convertQueryResponse(name, query, std::move(owned));
    }

    // ---- Multi-series helpers (Phase 2 group model) ----

    DerivedQueryExecutor makeExecutorWithCap(size_t cap) {
        DerivedQueryConfig config;
        config.maxSeriesPerLeg = cap;
        return DerivedQueryExecutor(nullptr, config);
    }

    // A query naming several fields, in the order the query string named them.
    QueryRequest makeQueryFields(const std::string& measurement, const std::vector<std::string>& fields) {
        QueryRequest req;
        req.measurement = measurement;
        req.fields = fields;
        req.aggregation = AggregationMethod::AVG;
        req.startTime = 1000000000;
        req.endTime = 2000000000;
        return req;
    }

    // A "()" query: no explicit fields, so every field of every series.
    QueryRequest makeAllFieldsQuery(const std::string& measurement) { return makeQueryFields(measurement, {}); }

    // Attach one more field (any FieldValues alternative) to an existing series.
    void addField(SeriesResult& sr, const std::string& fieldName, const std::vector<uint64_t>& timestamps,
                  const FieldValues& values) {
        sr.fields[fieldName] = std::make_pair(timestamps, values);
    }

    SeriesResult makeSeriesNoFields(const std::string& measurement, const std::map<std::string, std::string>& tags) {
        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        return sr;
    }

    MultiSeriesSubQueryResult callConvertMulti(DerivedQueryExecutor& executor, const std::string& name,
                                               const QueryRequest& query, const std::vector<SeriesResult>& results) {
        auto owned = results;
        return executor.convertQueryResponseMulti(name, query, std::move(owned));
    }

    // ---- Shared-axis alignment (Phase 3 fan-out) ----
    //
    // DerivedQueryExecutor::AlignedSubQueryGroups is private, and friendship is
    // not inherited by the classes TEST_F generates, so mirror it here and hand
    // the pieces out from a fixture member (which IS a friend).
    struct AlignedView {
        std::vector<uint64_t> times;
        std::vector<std::vector<double>> values;
        std::vector<std::vector<std::string>> groupTags;
    };

    // Aligns with the fan-out output bound DISABLED, so the alignment tests
    // pin alignment only.  The bound has its own tests below.
    static AlignedView callAlign(MultiSeriesSubQueryResult multi) { return callAlignBounded(std::move(multi), 0); }

    static AlignedView callAlignBounded(MultiSeriesSubQueryResult multi, size_t maxFanOutPoints) {
        auto aligned = DerivedQueryExecutor::alignSubQueryGroups(std::move(multi), maxFanOutPoints);
        return AlignedView{std::move(aligned.times), std::move(aligned.values), std::move(aligned.groupTags)};
    }

    // Build a multi-series result directly, bypassing the converter.
    static MultiSeriesSubQueryResult makeMulti(const std::vector<SubQuerySeries>& series) {
        MultiSeriesSubQueryResult multi;
        multi.queryName = "a";
        multi.measurement = "motor";
        multi.series = series;
        return multi;
    }

    static SubQuerySeries makeGroup(const std::map<std::string, std::string>& tags, const std::string& field,
                                    const std::vector<uint64_t>& timestamps, const std::vector<double>& values) {
        SubQuerySeries group;
        group.tags = tags;
        group.field = field;
        group.timestamps = timestamps;
        group.values = values;
        return group;
    }

    static size_t countNaN(const std::vector<double>& values) {
        size_t n = 0;
        for (double v : values) {
            if (std::isnan(v)) {
                ++n;
            }
        }
        return n;
    }

    // The (tags, field) sequence of a multi-series result — the thing the
    // ordering rule pins.
    static std::vector<std::pair<std::map<std::string, std::string>, std::string>> keysOf(
        const MultiSeriesSubQueryResult& multi) {
        std::vector<std::pair<std::map<std::string, std::string>, std::string>> keys;
        keys.reserve(multi.series.size());
        for (const auto& s : multi.series) {
            keys.emplace_back(s.tags, s.field);
        }
        return keys;
    }
};

// ==================== Empty Results ====================

TEST_F(DerivedQueryMultiSeriesTest, EmptyResultsReturnEmptySubQueryResult) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");
    std::vector<SeriesResult> emptyResults;

    auto result = callConvert(executor, "a", query, emptyResults);

    EXPECT_EQ(result.queryName, "a");
    EXPECT_EQ(result.measurement, "cpu");
    EXPECT_TRUE(result.timestamps.empty());
    EXPECT_TRUE(result.values.empty());
    EXPECT_TRUE(result.tags.empty());
    EXPECT_TRUE(result.field.empty());
}

// ==================== Single Series (Happy Path) ====================

TEST_F(DerivedQueryMultiSeriesTest, SingleSeriesResultWorkCorrectly) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "server1"}}, "usage", {1000, 2000, 3000}, {10.0, 20.0, 30.0}));

    auto result = callConvert(executor, "a", query, results);

    EXPECT_EQ(result.queryName, "a");
    EXPECT_EQ(result.measurement, "cpu");
    EXPECT_EQ(result.field, "usage");
    EXPECT_EQ(result.tags.at("host"), "server1");
    ASSERT_EQ(result.timestamps.size(), 3);
    EXPECT_EQ(result.timestamps[0], 1000);
    EXPECT_EQ(result.timestamps[2], 3000);
    ASSERT_EQ(result.values.size(), 3);
    EXPECT_DOUBLE_EQ(result.values[0], 10.0);
    EXPECT_DOUBLE_EQ(result.values[2], 30.0);
}

// ==================== Multiple Series (Bug) ====================

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesThrowsDerivedQueryException) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "server1"}}, "usage", {1000, 2000}, {10.0, 20.0}));
    results.push_back(makeSeries("cpu", {{"host", "server2"}}, "usage", {1000, 2000}, {30.0, 40.0}));

    EXPECT_THROW(callConvert(executor, "my_query", query, results), DerivedQueryException);
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageContainsQueryName) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "server1"}}, "usage", {1000}, {10.0}));
    results.push_back(makeSeries("cpu", {{"host", "server2"}}, "usage", {1000}, {20.0}));

    try {
        callConvert(executor, "error_rate", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("error_rate"), std::string::npos)
            << "Error message should contain the query name 'error_rate', got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageContainsSeriesCount) {
    auto executor = makeExecutor();
    auto query = makeQuery("temperature", "value");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("temperature", {{"location", "us-west"}}, "value", {1000}, {72.0}));
    results.push_back(makeSeries("temperature", {{"location", "us-east"}}, "value", {1000}, {68.0}));
    results.push_back(makeSeries("temperature", {{"location", "eu-west"}}, "value", {1000}, {65.0}));

    try {
        callConvert(executor, "temp_avg", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        // Should mention 3 series were returned
        EXPECT_NE(msg.find("3"), std::string::npos) << "Error message should contain the count '3', got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, MultipleSeriesErrorMessageSuggestsScopeFilters) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "a"}}, "usage", {1000}, {1.0}));
    results.push_back(makeSeries("cpu", {{"host", "b"}}, "usage", {1000}, {2.0}));

    try {
        callConvert(executor, "load", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        // Should suggest adding scope filters to narrow the result
        EXPECT_NE(msg.find("scope"), std::string::npos) << "Error message should mention scope filters, got: " << msg;
    }
}

TEST_F(DerivedQueryMultiSeriesTest, ExactlyTwoSeriesAlsoThrows) {
    auto executor = makeExecutor();
    auto query = makeQuery("disk", "free");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("disk", {{"device", "sda"}}, "free", {1000, 2000}, {100.0, 200.0}));
    results.push_back(makeSeries("disk", {{"device", "sdb"}}, "free", {1000, 2000}, {300.0, 400.0}));

    EXPECT_THROW(callConvert(executor, "disk_check", query, results), DerivedQueryException);
}

// ===========================================================================
// Phase 2: the multi-series group model (convertQueryResponseMulti)
//
// The single-series converter above stays exactly as it is -- it still refuses
// a leg that resolved to more than one series, and the five tests that pin
// that refusal still pass.  convertQueryResponseMulti() is the shape that
// lifts the restriction: one entry per (series tag set x numeric field) pair,
// which is what ForecastExecutor::executeMulti / AnomalyExecutor::executeMulti
// already consume.  It is deliberately NOT wired into any request path yet.
//
// Ordering rule under test throughout: tag set ascending, then field rank
// (requested order for an explicit field list, ascending field name for "()"),
// then input index.
// ===========================================================================

// ==================== Empty input ====================

TEST_F(DerivedQueryMultiSeriesTest, MultiEmptyResultsReturnEmptySeriesVector) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    auto multi = callConvertMulti(executor, "a", query, {});

    EXPECT_EQ(multi.queryName, "a");
    EXPECT_EQ(multi.measurement, "cpu");
    EXPECT_TRUE(multi.series.empty());
    EXPECT_TRUE(multi.empty());
    EXPECT_EQ(multi.size(), 0u);
}

// ==================== Equivalence with the single-series converter =========

// The single-series semantics must be exactly reproducible from the model:
// for one input series, entry[0] of the multi result carries the same tags,
// field, timestamps and values that convertQueryResponse() returns.
TEST_F(DerivedQueryMultiSeriesTest, MultiReproducesSingleSeriesConversionForExplicitField) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "server1"}}, "usage", {1000, 2000, 3000}, {10.0, 20.0, 30.0}));

    auto single = callConvert(executor, "a", query, results);
    auto multi = callConvertMulti(executor, "a", query, results);

    EXPECT_EQ(multi.queryName, single.queryName);
    EXPECT_EQ(multi.measurement, single.measurement);
    ASSERT_EQ(multi.series.size(), 1u);
    EXPECT_EQ(multi.series[0].tags, single.tags);
    EXPECT_EQ(multi.series[0].field, single.field);
    EXPECT_EQ(multi.series[0].timestamps, single.timestamps);
    EXPECT_EQ(multi.series[0].values, single.values);
}

// Same claim for "()", where the single-series converter takes whichever field
// comes first in the series' std::map.  Entry[0] of the multi result is that
// same field, because "()" emits in the series' own map order.
TEST_F(DerivedQueryMultiSeriesTest, MultiReproducesSingleSeriesConversionForAllFieldsQuery) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("cpu");

    auto series = makeSeries("cpu", {{"host", "server1"}}, "usage", {1000, 2000}, {10.0, 20.0});
    addField(series, "idle", {1000, 2000}, std::vector<double>{90.0, 80.0});
    std::vector<SeriesResult> results{series};

    auto single = callConvert(executor, "a", query, results);
    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 2u);
    EXPECT_EQ(single.field, "idle") << "std::map order: 'idle' sorts before 'usage'";
    EXPECT_EQ(multi.series[0].field, single.field);
    EXPECT_EQ(multi.series[0].timestamps, single.timestamps);
    EXPECT_EQ(multi.series[0].values, single.values);
    // ...and the field the single-series converter DISCARDED is still here.
    EXPECT_EQ(multi.series[1].field, "usage");
}

// ==================== Fan-out: N series x M fields =========================

TEST_F(DerivedQueryMultiSeriesTest, MultiFansOutSeriesTimesFields) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz", "20hz", "30hz"});

    std::vector<SeriesResult> results;
    for (const char* device : {"DEV-A", "DEV-B"}) {
        auto sr = makeSeries("motor", {{"deviceId", device}}, "10hz", {1000, 2000}, {1.0, 2.0});
        addField(sr, "20hz", {1000, 2000}, std::vector<double>{3.0, 4.0});
        addField(sr, "30hz", {1000, 2000}, std::vector<double>{5.0, 6.0});
        results.push_back(std::move(sr));
    }

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 6u) << "2 devices x 3 fields";
    // Tag-major: a device's three fields stay contiguous.
    EXPECT_EQ(multi.series[0].tags.at("deviceId"), "DEV-A");
    EXPECT_EQ(multi.series[1].tags.at("deviceId"), "DEV-A");
    EXPECT_EQ(multi.series[2].tags.at("deviceId"), "DEV-A");
    EXPECT_EQ(multi.series[3].tags.at("deviceId"), "DEV-B");
    EXPECT_EQ(multi.series[5].tags.at("deviceId"), "DEV-B");
    // Each entry keeps its own data.
    EXPECT_EQ(multi.series[0].field, "10hz");
    EXPECT_EQ(multi.series[0].values, (std::vector<double>{1.0, 2.0}));
    EXPECT_EQ(multi.series[2].field, "30hz");
    EXPECT_EQ(multi.series[2].values, (std::vector<double>{5.0, 6.0}));
    EXPECT_EQ(multi.series[0].timestamps, (std::vector<uint64_t>{1000, 2000}));
}

// ==================== Requested-field order ================================

// The order is the one the QUERY STRING named, not std::map order.  The
// single-series converter's "keep fields[0]" is query-string order too, so the
// model must not silently switch to alphabetical.
TEST_F(DerivedQueryMultiSeriesTest, MultiPreservesRequestedFieldOrder) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"zeta", "alpha", "mid"});

    auto sr = makeSeries("motor", {{"deviceId", "DEV-A"}}, "alpha", {1000}, {1.0});
    addField(sr, "mid", {1000}, std::vector<double>{2.0});
    addField(sr, "zeta", {1000}, std::vector<double>{3.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 3u);
    EXPECT_EQ(multi.series[0].field, "zeta");
    EXPECT_EQ(multi.series[1].field, "alpha");
    EXPECT_EQ(multi.series[2].field, "mid");
    // The values travel with their own field, not with the position.
    EXPECT_EQ(multi.series[0].values, (std::vector<double>{3.0}));
    EXPECT_EQ(multi.series[1].values, (std::vector<double>{1.0}));
}

TEST_F(DerivedQueryMultiSeriesTest, MultiIgnoresFieldsTheQueryDidNotRequest) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz"});

    auto sr = makeSeries("motor", {{"deviceId", "DEV-A"}}, "10hz", {1000}, {1.0});
    addField(sr, "20hz", {1000}, std::vector<double>{2.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 1u);
    EXPECT_EQ(multi.series[0].field, "10hz");
}

// ==================== "()" — all fields ====================================

TEST_F(DerivedQueryMultiSeriesTest, MultiAllFieldsQueryEmitsEveryFieldInMapOrder) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("motor");

    auto sr = makeSeries("motor", {{"deviceId", "DEV-A"}}, "zeta", {1000}, {3.0});
    addField(sr, "alpha", {1000}, std::vector<double>{1.0});
    addField(sr, "mid", {1000}, std::vector<double>{2.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 3u);
    EXPECT_EQ(multi.series[0].field, "alpha");
    EXPECT_EQ(multi.series[1].field, "mid");
    EXPECT_EQ(multi.series[2].field, "zeta");
}

// ==================== Determinism ==========================================

// Shard fan-out decides the order `results` arrives in; the model's order must
// not.  Reversing the input must produce the identical output sequence.
TEST_F(DerivedQueryMultiSeriesTest, MultiOrderingIsIndependentOfInputSeriesOrder) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz", "20hz"});

    std::vector<SeriesResult> ascending;
    for (const char* device : {"DEV-A", "DEV-B", "DEV-C"}) {
        auto sr = makeSeries("motor", {{"deviceId", device}}, "10hz", {1000}, {1.0});
        addField(sr, "20hz", {1000}, std::vector<double>{2.0});
        ascending.push_back(std::move(sr));
    }
    std::vector<SeriesResult> reversed(ascending.rbegin(), ascending.rend());

    auto fromAscending = callConvertMulti(executor, "a", query, ascending);
    auto fromReversed = callConvertMulti(executor, "a", query, reversed);

    ASSERT_EQ(fromAscending.series.size(), 6u);
    EXPECT_EQ(keysOf(fromAscending), keysOf(fromReversed));
    EXPECT_EQ(fromAscending.series[0].tags.at("deviceId"), "DEV-A");
    EXPECT_EQ(fromAscending.series[4].tags.at("deviceId"), "DEV-C");
}

TEST_F(DerivedQueryMultiSeriesTest, MultiRepeatedCallsProduceTheIdenticalSequence) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"20hz", "10hz"});

    std::vector<SeriesResult> results;
    for (const char* rack : {"r2", "r1"}) {
        for (const char* device : {"d2", "d1"}) {
            auto sr = makeSeries("motor", {{"rack", rack}, {"deviceId", device}}, "10hz", {1000}, {1.0});
            addField(sr, "20hz", {1000}, std::vector<double>{2.0});
            results.push_back(std::move(sr));
        }
    }

    auto first = callConvertMulti(executor, "a", query, results);
    for (int i = 0; i < 5; ++i) {
        auto again = callConvertMulti(executor, "a", query, results);
        EXPECT_EQ(keysOf(again), keysOf(first)) << "iteration " << i;
    }

    // Tag sets compare as std::map does: by (key, value) pairs in key order,
    // so deviceId (the first key) is the primary sort.
    ASSERT_EQ(first.series.size(), 8u);
    EXPECT_EQ(first.series[0].tags.at("deviceId"), "d1");
    EXPECT_EQ(first.series[0].tags.at("rack"), "r1");
    EXPECT_EQ(first.series[0].field, "20hz") << "requested order, not map order";
    EXPECT_EQ(first.series[7].tags.at("deviceId"), "d2");
    EXPECT_EQ(first.series[7].tags.at("rack"), "r2");
}

// ==================== Tag fidelity =========================================

// Tags come from SeriesResult's internal std::map, not the "k=v" strings the
// /query JSON serializer flattens them into.  Every entry of a series carries
// the full map.
TEST_F(DerivedQueryMultiSeriesTest, MultiPreservesTheFullTagMapOnEveryEntry) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz", "20hz"});

    const std::map<std::string, std::string> tags{
        {"deviceId", "DEV-A"}, {"rack", "r1"}, {"site", "us-west"}, {"empty", ""}};
    auto sr = makeSeries("motor", tags, "10hz", {1000}, {1.0});
    addField(sr, "20hz", {1000}, std::vector<double>{2.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 2u);
    EXPECT_EQ(multi.series[0].tags, tags);
    EXPECT_EQ(multi.series[1].tags, tags) << "the second field must not lose the tag map";
}

// ==================== Value types ==========================================

TEST_F(DerivedQueryMultiSeriesTest, MultiStringValuesThrowDerivedQueryException) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("logs", {"message"});

    auto sr = makeSeriesNoFields("logs", {{"host", "h1"}});
    addField(sr, "message", {1000}, std::vector<std::string>{"hello"});
    std::vector<SeriesResult> results{sr};

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        EXPECT_NE(std::string(e.what()).find("non-numeric"), std::string::npos) << e.what();
    }
}

// Booleans are NOT numeric in this codebase (CLAUDE.md, "Non-Numeric Fields in
// Queries").  The group model must refuse them exactly as strings, not revive
// the old 1.0/0.0 coercion.
TEST_F(DerivedQueryMultiSeriesTest, MultiBooleanValuesThrowDerivedQueryException) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("switch", {"state"});

    auto sr = makeSeriesNoFields("switch", {{"host", "h1"}});
    addField(sr, "state", {1000, 2000}, std::vector<bool>{true, false});
    std::vector<SeriesResult> results{sr};

    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);
}

// A non-numeric field must fail the leg even when a numeric one sits beside it:
// a short answer is not an acceptable substitute for a failure.
TEST_F(DerivedQueryMultiSeriesTest, MultiNonNumericFieldThrowsEvenAlongsideNumericFields) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("mixed", {"value", "label"});

    auto sr = makeSeries("mixed", {{"host", "h1"}}, "value", {1000}, {1.0});
    addField(sr, "label", {1000}, std::vector<std::string>{"x"});
    std::vector<SeriesResult> results{sr};

    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);
}

TEST_F(DerivedQueryMultiSeriesTest, MultiWidensInt64ValuesToDouble) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("counters", {"requests"});

    auto sr = makeSeriesNoFields("counters", {{"host", "h1"}});
    addField(sr, "requests", {1000, 2000, 3000}, std::vector<int64_t>{-7, 0, 9007199254740993LL});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 1u);
    ASSERT_EQ(multi.series[0].values.size(), 3u);
    EXPECT_DOUBLE_EQ(multi.series[0].values[0], -7.0);
    EXPECT_DOUBLE_EQ(multi.series[0].values[1], 0.0);
    EXPECT_DOUBLE_EQ(multi.series[0].values[2], static_cast<double>(9007199254740993LL));
}

// ==================== Missing requested field ==============================

// Same contract as the single-series converter: a series that carries data but
// none of the requested fields is a failed read, not an empty one.
TEST_F(DerivedQueryMultiSeriesTest, MultiMissingRequestedFieldThrowsLikeSingleSeries) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    auto sr = makeSeries("cpu", {{"host", "h1"}}, "idle", {1000}, {1.0});
    std::vector<SeriesResult> results{sr};

    std::string singleMsg;
    try {
        callConvert(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException from the single-series converter";
    } catch (const DerivedQueryException& e) {
        singleMsg = e.what();
    }

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException from the multi-series converter";
    } catch (const DerivedQueryException& e) {
        EXPECT_EQ(std::string(e.what()), singleMsg);
    }
}

// ==================== Per-leg series cap ===================================

// Fan-out is the unit that costs: each (tags x field) entry is fitted
// independently and emits its own response pieces.  The cap counts entries.

TEST_F(DerivedQueryMultiSeriesTest, MultiCapOneUnderTheLimitPasses) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeQueryFields("motor", {"10hz"});

    std::vector<SeriesResult> results;
    for (int i = 0; i < 4; ++i) {
        results.push_back(makeSeries("motor", {{"deviceId", "d" + std::to_string(i)}}, "10hz", {1000}, {1.0}));
    }

    auto multi = callConvertMulti(executor, "a", query, results);
    EXPECT_EQ(multi.series.size(), 4u);
}

TEST_F(DerivedQueryMultiSeriesTest, MultiCapExactlyAtTheLimitPasses) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeQueryFields("motor", {"10hz"});

    std::vector<SeriesResult> results;
    for (int i = 0; i < 5; ++i) {
        results.push_back(makeSeries("motor", {{"deviceId", "d" + std::to_string(i)}}, "10hz", {1000}, {1.0}));
    }

    auto multi = callConvertMulti(executor, "a", query, results);
    EXPECT_EQ(multi.series.size(), 5u);
}

TEST_F(DerivedQueryMultiSeriesTest, MultiCapOneOverTheLimitThrowsWithLegCountAndCap) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeQueryFields("motor", {"10hz"});

    std::vector<SeriesResult> results;
    for (int i = 0; i < 6; ++i) {
        results.push_back(makeSeries("motor", {{"deviceId", "d" + std::to_string(i)}}, "10hz", {1000}, {1.0}));
    }

    try {
        callConvertMulti(executor, "fleet_forecast", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("fleet_forecast"), std::string::npos) << "must name the leg: " << msg;
        EXPECT_NE(msg.find("6"), std::string::npos) << "must state the actual count: " << msg;
        EXPECT_NE(msg.find("5"), std::string::npos) << "must state the cap: " << msg;
        EXPECT_NE(msg.find("scope"), std::string::npos) << "must suggest narrowing the scope: " << msg;
    }
}

// The cap counts (tags x field) entries, so a narrow-but-wide leg -- few
// series, many fields -- is bounded too.
TEST_F(DerivedQueryMultiSeriesTest, MultiCapCountsTagsTimesFieldsNotSeries) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeQueryFields("motor", {"f1", "f2", "f3"});

    std::vector<SeriesResult> results;
    for (const char* device : {"DEV-A", "DEV-B"}) {
        auto sr = makeSeries("motor", {{"deviceId", device}}, "f1", {1000}, {1.0});
        addField(sr, "f2", {1000}, std::vector<double>{2.0});
        addField(sr, "f3", {1000}, std::vector<double>{3.0});
        results.push_back(std::move(sr));
    }

    // 2 series only, but 6 entries -- over a cap of 5.
    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);
}

// The default is the documented 200, not "unbounded".
//
// It was 50 through Phase 3.5.  50 counted (tag set x field), so the reporter's
// most natural follow-up query -- a two-metric per-device dashboard,
// `avg:m(v,w){} by {dev}` over a 50-device fleet -- resolved to 100 and was
// refused, while asking for a few thousand cells.  maxFanOutPoints does the
// memory guarding now (it binds at 200 groups from a 1250-slot axis upwards),
// so this one is left as the cardinality sanity check it always was.
TEST_F(DerivedQueryMultiSeriesTest, MultiCapDefaultsToTwoHundred) {
    DerivedQueryConfig defaults;
    EXPECT_EQ(defaults.maxSeriesPerLeg, 200u);

    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz"});

    std::vector<SeriesResult> results;
    for (int i = 0; i < 201; ++i) {
        results.push_back(makeSeries("motor", {{"deviceId", "d" + std::to_string(i)}}, "10hz", {1000}, {1.0}));
    }

    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);

    results.pop_back();
    auto multi = callConvertMulti(executor, "a", query, results);
    EXPECT_EQ(multi.series.size(), 200u);
}

// The two-metric per-device dashboard the 50-series cap used to refuse: 50
// devices x 2 fields == 100 entries, comfortably under the raised cap, and it
// must resolve to all 100 groups rather than throw.
TEST_F(DerivedQueryMultiSeriesTest, ATwoFieldFiftyDeviceDashboardIsUnderTheDefaultCap) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"v", "w"});

    std::vector<SeriesResult> results;
    for (int i = 0; i < 50; ++i) {
        auto sr = makeSeries("motor", {{"deviceId", "d" + std::to_string(i)}}, "v", {1000}, {1.0});
        addField(sr, "w", {1000}, std::vector<double>{2.0});
        results.push_back(std::move(sr));
    }

    auto multi = callConvertMulti(executor, "a", query, results);
    EXPECT_EQ(multi.series.size(), 100u) << "REGRESSION: a 50-device two-metric dashboard was refused again";
}

// ==================== The single-series converter is UNCHANGED =============

// Phase 2 adds the model without wiring it up: the same input that the group
// model happily fans out is still refused by the converter /derived actually
// calls.  This is the byte-identical-behaviour guarantee, in test form.
TEST_F(DerivedQueryMultiSeriesTest, MultiSeriesModelDoesNotRelaxTheSingleSeriesRefusal) {
    auto executor = makeExecutor();
    auto query = makeQuery("cpu", "usage");

    std::vector<SeriesResult> results;
    results.push_back(makeSeries("cpu", {{"host", "server1"}}, "usage", {1000}, {10.0}));
    results.push_back(makeSeries("cpu", {{"host", "server2"}}, "usage", {1000}, {20.0}));

    EXPECT_EQ(callConvertMulti(executor, "a", query, results).series.size(), 2u);
    EXPECT_THROW(callConvert(executor, "a", query, results), DerivedQueryException);
}

// ==========================================================================
// Phase 3 Part A — defects found reviewing the Phase 2 group model
// ==========================================================================

// ---- D1: a duplicated field name ----------------------------------------
//
// QueryParser::parseFields() does not dedupe, so `avg:m(10hz,10hz)` reaches
// the converter as {"10hz","10hz"}.  Both requests resolved to the SAME field
// iterator, the emit loop moved the column out for the first entry, and the
// second read the moved-from vectors -- yielding a phantom 0-point entry with
// no exception at all (a moved-from FieldValues still holds a vector<double>,
// so the numeric extractor never noticed).
TEST_F(DerivedQueryMultiSeriesTest, MultiDeduplicatesRepeatedRequestedFields) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor.vibration", {"10hz", "10hz"});

    std::vector<SeriesResult> results{
        makeSeries("motor.vibration", {{"deviceId", "DEV-A"}}, "10hz", {1000, 2000}, {1.0, 2.0})};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 1u) << "the repeated name must not fan out into two entries";
    EXPECT_EQ(multi.series[0].field, "10hz");
    EXPECT_EQ(multi.series[0].timestamps, (std::vector<uint64_t>{1000, 2000}));
    EXPECT_EQ(multi.series[0].values, (std::vector<double>{1.0, 2.0}));
}

// The dedupe keeps FIRST-occurrence order, so it cannot disturb the requested
// ordering rule the surviving names are sorted by.
TEST_F(DerivedQueryMultiSeriesTest, MultiDedupeKeepsFirstOccurrenceOrder) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"zeta", "alpha", "zeta", "mid", "alpha"});

    auto sr = makeSeries("motor", {{"deviceId", "DEV-A"}}, "alpha", {1000}, {1.0});
    addField(sr, "mid", {1000}, std::vector<double>{2.0});
    addField(sr, "zeta", {1000}, std::vector<double>{3.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 3u);
    EXPECT_EQ(multi.series[0].field, "zeta");
    EXPECT_EQ(multi.series[1].field, "alpha");
    EXPECT_EQ(multi.series[2].field, "mid");
    EXPECT_EQ(multi.series[0].values, (std::vector<double>{3.0}));
    EXPECT_EQ(multi.series[1].values, (std::vector<double>{1.0}));
    EXPECT_EQ(multi.series[2].values, (std::vector<double>{2.0}));
}

// A duplicated name on a "()" leg is impossible (no list to duplicate), but a
// duplicate that resolves to nothing must still not fabricate an entry.
TEST_F(DerivedQueryMultiSeriesTest, MultiDedupeDoesNotMaskAMissingField) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("cpu", {"usage", "usage"});

    std::vector<SeriesResult> results{makeSeries("cpu", {{"host", "h1"}}, "idle", {1000}, {1.0})};

    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);
}

// ---- D2: a series missing SOME of the requested fields -------------------
//
// RULE (fan-out only): a series must contribute at least ONE requested field.
// Missing SOME of them simply omits those (tags x field) pairs; missing ALL of
// them throws.  This mirrors /query -- `avg:m(a,b){} by {dev}` where DEV-2 has
// no `a` returns DEV-1/a, DEV-1/b, DEV-2/b and no error -- and it is not the
// silent short answer CLAUDE.md forbids, because under fan-out every emitted
// group is labelled with its own tags and field.
TEST_F(DerivedQueryMultiSeriesTest, MultiSeriesMissingSomeRequestedFieldsEmitsTheOnesItHas) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz", "20hz"});

    auto full = makeSeries("motor", {{"deviceId", "DEV-1"}}, "10hz", {1000}, {1.0});
    addField(full, "20hz", {1000}, std::vector<double>{2.0});
    auto partial = makeSeries("motor", {{"deviceId", "DEV-2"}}, "20hz", {1000}, {3.0});

    std::vector<SeriesResult> results{full, partial};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 3u) << "DEV-1/10hz, DEV-1/20hz, DEV-2/20hz";
    EXPECT_EQ(multi.series[0].tags.at("deviceId"), "DEV-1");
    EXPECT_EQ(multi.series[0].field, "10hz");
    EXPECT_EQ(multi.series[1].tags.at("deviceId"), "DEV-1");
    EXPECT_EQ(multi.series[1].field, "20hz");
    EXPECT_EQ(multi.series[2].tags.at("deviceId"), "DEV-2");
    EXPECT_EQ(multi.series[2].field, "20hz") << "the field DEV-2 does carry must still be emitted";
}

// ...but a series carrying NONE of them is still a failed read, with the
// single-series converter's exact message.
TEST_F(DerivedQueryMultiSeriesTest, MultiSeriesMissingAllRequestedFieldsStillThrows) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"10hz", "20hz"});

    auto good = makeSeries("motor", {{"deviceId", "DEV-1"}}, "10hz", {1000}, {1.0});
    auto none = makeSeries("motor", {{"deviceId", "DEV-2"}}, "99hz", {1000}, {3.0});
    std::vector<SeriesResult> results{good, none};

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("'10hz'"), std::string::npos) << msg;
        EXPECT_NE(msg.find("not found"), std::string::npos) << msg;
    }
}

// ---- D3: ordering when two series share a tag set ------------------------
//
// The comparator's key was (tags, fieldRank, arrivalIndex).  Two SeriesResults
// carrying the SAME tag set each rank their own first field 0, so the tie fell
// through to arrival order and the sequence FLIPPED when the shards answered
// in a different order -- exactly what the tag-major rule exists to prevent.
// The field NAME is what actually separates them.
TEST_F(DerivedQueryMultiSeriesTest, MultiOrderingIsStableForTwoSeriesSharingATagSet) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("motor");

    const std::map<std::string, std::string> tags{{"deviceId", "D1"}};
    std::vector<SeriesResult> forward{makeSeries("motor", tags, "a", {1000}, {1.0}),
                                      makeSeries("motor", tags, "z", {1000}, {26.0})};
    std::vector<SeriesResult> reversed(forward.rbegin(), forward.rend());

    auto fromForward = callConvertMulti(executor, "a", query, forward);
    auto fromReversed = callConvertMulti(executor, "a", query, reversed);

    ASSERT_EQ(fromForward.series.size(), 2u);
    EXPECT_EQ(keysOf(fromForward), keysOf(fromReversed)) << "the order must not depend on shard arrival order";
    EXPECT_EQ(fromForward.series[0].field, "a");
    EXPECT_EQ(fromForward.series[1].field, "z");
    // ...and the values travel with their own field in both directions.
    EXPECT_EQ(fromReversed.series[0].values, (std::vector<double>{1.0}));
    EXPECT_EQ(fromReversed.series[1].values, (std::vector<double>{26.0}));
}

// Same, with an explicit field list: the requested rank still wins over the
// name, so the name is only ever the TIE-break.
TEST_F(DerivedQueryMultiSeriesTest, MultiFieldNameTieBreakDoesNotOverrideRequestedOrder) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("motor", {"z", "a"});

    const std::map<std::string, std::string> tags{{"deviceId", "D1"}};
    auto sr = makeSeries("motor", tags, "a", {1000}, {1.0});
    addField(sr, "z", {1000}, std::vector<double>{26.0});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 2u);
    EXPECT_EQ(multi.series[0].field, "z") << "requested order, not alphabetical";
    EXPECT_EQ(multi.series[1].field, "a");
}

// ---- D4: the cap counts EMITTED entries ---------------------------------
//
// Counting the raw (tag set x field) product let the cap error pre-empt a
// request that was genuinely under the limit: a "()" leg on a measurement with
// 4 numeric and 3 string fields reported "resolved to 7 series ... limit is 5.
// Add more specific scope filters" -- advice the caller cannot act on, for a
// leg that actually resolves to 4.
TEST_F(DerivedQueryMultiSeriesTest, MultiCapCountsOnlyEntriesThatWillBeEmitted) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeAllFieldsQuery("mixed");

    auto sr = makeSeriesNoFields("mixed", {{"host", "h1"}});
    for (int i = 0; i < 4; ++i) {
        addField(sr, "num" + std::to_string(i), {1000}, std::vector<double>{static_cast<double>(i)});
    }
    for (int i = 0; i < 3; ++i) {
        addField(sr, "str" + std::to_string(i), {1000}, std::vector<std::string>{"x"});
    }
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 4u) << "4 numeric fields; the 3 string fields are not emitted, so do not count";
    for (const auto& s : multi.series) {
        EXPECT_EQ(s.field.rfind("num", 0), 0u) << s.field;
    }
}

// ...and once the EMITTED count really is over the cap, it still throws.
TEST_F(DerivedQueryMultiSeriesTest, MultiCapStillThrowsOnEmittedEntriesOverTheLimit) {
    auto executor = makeExecutorWithCap(5);
    auto query = makeAllFieldsQuery("mixed");

    auto sr = makeSeriesNoFields("mixed", {{"host", "h1"}});
    for (int i = 0; i < 6; ++i) {
        addField(sr, "num" + std::to_string(i), {1000}, std::vector<double>{static_cast<double>(i)});
    }
    addField(sr, "label", {1000}, std::vector<std::string>{"x"});
    std::vector<SeriesResult> results{sr};

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("6"), std::string::npos) << "must report the EMITTED count, not 7: " << msg;
    }
}

// ---- D5: explicit vs implicit non-numeric fields -------------------------
//
// An EXPLICITLY named non-numeric field throws: that is the single-series
// contract and CLAUDE.md's numeric-only rule for formula operands.
TEST_F(DerivedQueryMultiSeriesTest, MultiExplicitNonNumericFieldThrowsAndNamesTheField) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("mixed", {"label"});

    auto sr = makeSeriesNoFields("mixed", {{"host", "h1"}});
    addField(sr, "label", {1000}, std::vector<std::string>{"x"});
    addField(sr, "value", {1000}, std::vector<double>{1.0});
    std::vector<SeriesResult> results{sr};

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("non-numeric"), std::string::npos) << msg;
        EXPECT_NE(msg.find("label"), std::string::npos) << "must name the offending field: " << msg;
    }
}

// Booleans are non-numeric (CLAUDE.md), so naming one explicitly throws too --
// the 1.0/0.0 coercion must not come back through the group model.
TEST_F(DerivedQueryMultiSeriesTest, MultiExplicitBooleanFieldThrows) {
    auto executor = makeExecutor();
    auto query = makeQueryFields("switch", {"state"});

    auto sr = makeSeriesNoFields("switch", {{"host", "h1"}});
    addField(sr, "state", {1000, 2000}, std::vector<bool>{true, false});
    std::vector<SeriesResult> results{sr};

    EXPECT_THROW(callConvertMulti(executor, "a", query, results), DerivedQueryException);
}

// An IMPLICIT "()" leg selects only the numeric fields.  One device carrying a
// string label must not kill a fleet-wide forecast.  Booleans are skipped on
// the same rule.
TEST_F(DerivedQueryMultiSeriesTest, MultiAllFieldsQuerySkipsNonNumericFields) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("mixed");

    auto sr = makeSeriesNoFields("mixed", {{"host", "h1"}});
    addField(sr, "aaa_label", {1000}, std::vector<std::string>{"x"});  // sorts FIRST in std::map
    addField(sr, "bbb_flag", {1000}, std::vector<bool>{true});
    addField(sr, "ccc_value", {1000}, std::vector<double>{1.0});
    addField(sr, "ddd_count", {1000}, std::vector<int64_t>{7});
    std::vector<SeriesResult> results{sr};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 2u) << "only the numeric fields";
    EXPECT_EQ(multi.series[0].field, "ccc_value");
    EXPECT_EQ(multi.series[1].field, "ddd_count");
    EXPECT_EQ(multi.series[1].values, (std::vector<double>{7.0})) << "int64 is numeric and is widened";
}

// A "()" leg where ONE device carries an extra string field still forecasts
// every numeric channel of every device.
TEST_F(DerivedQueryMultiSeriesTest, MultiAllFieldsQueryOneDeviceWithAStringLabelDoesNotKillTheLeg) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("motor");

    auto clean = makeSeries("motor", {{"deviceId", "DEV-A"}}, "10hz", {1000}, {1.0});
    auto labelled = makeSeries("motor", {{"deviceId", "DEV-B"}}, "10hz", {1000}, {2.0});
    addField(labelled, "firmware", {1000}, std::vector<std::string>{"v2"});
    std::vector<SeriesResult> results{clean, labelled};

    auto multi = callConvertMulti(executor, "a", query, results);

    ASSERT_EQ(multi.series.size(), 2u);
    EXPECT_EQ(multi.series[0].tags.at("deviceId"), "DEV-A");
    EXPECT_EQ(multi.series[0].field, "10hz");
    EXPECT_EQ(multi.series[1].tags.at("deviceId"), "DEV-B");
    EXPECT_EQ(multi.series[1].field, "10hz");
}

// ...but a "()" leg with NO numeric field anywhere is a real failure, and the
// message names what was found instead.
TEST_F(DerivedQueryMultiSeriesTest, MultiAllFieldsQueryWithNoNumericFieldThrowsNamingWhatWasFound) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("logs");

    auto sr = makeSeriesNoFields("logs", {{"host", "h1"}});
    addField(sr, "message", {1000}, std::vector<std::string>{"x"});
    addField(sr, "healthy", {1000}, std::vector<bool>{true});
    std::vector<SeriesResult> results{sr};

    try {
        callConvertMulti(executor, "a", query, results);
        FAIL() << "Expected DerivedQueryException";
    } catch (const DerivedQueryException& e) {
        std::string msg = e.what();
        EXPECT_NE(msg.find("non-numeric"), std::string::npos) << msg;
        EXPECT_NE(msg.find("message"), std::string::npos) << "must name what was found: " << msg;
        EXPECT_NE(msg.find("healthy"), std::string::npos) << "must name what was found: " << msg;
    }
}

// A "()" leg over series that carry no fields at all is still an empty result,
// not an error -- there is nothing to report as non-numeric.
TEST_F(DerivedQueryMultiSeriesTest, MultiAllFieldsQueryWithNoFieldsAtAllReturnsEmpty) {
    auto executor = makeExecutor();
    auto query = makeAllFieldsQuery("motor");

    std::vector<SeriesResult> results{makeSeriesNoFields("motor", {{"deviceId", "DEV-A"}})};

    auto multi = callConvertMulti(executor, "a", query, results);
    EXPECT_TRUE(multi.series.empty());
}

// ==========================================================================
// Phase 3 Part B — the shared time axis the fan-out projects every group onto
//
// ForecastQueryResult / AnomalyQueryResult carry ONE `times` vector for all
// series pieces, so N groups must be reduced to one axis before they can be
// fitted.  The axis is the sorted union of every group's timestamps and a gap
// is filled with NaN, which means "missing" here (docs/nan_policy.md) and is
// what every forecaster and detector already skips.
// ==========================================================================

// The common case: a bucketed leg gives every group the identical axis.  The
// union must then be exactly that axis, and NOT ONE NaN may be introduced --
// otherwise ordinary per-device forecasting would pay for a facility only
// ragged data needs.
TEST_F(DerivedQueryMultiSeriesTest, AlignSharedAxisIntroducesNoNaN) {
    const std::vector<uint64_t> axis{1000, 2000, 3000, 4000};
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}}, "10hz", axis, {1.0, 2.0, 3.0, 4.0}),
                            makeGroup({{"deviceId", "DEV-B"}}, "10hz", axis, {5.0, 6.0, 7.0, 8.0})});

    auto aligned = callAlign(multi);

    EXPECT_EQ(aligned.times, axis) << "the union of one repeated axis is that axis";
    ASSERT_EQ(aligned.values.size(), 2u);
    EXPECT_EQ(aligned.values[0], (std::vector<double>{1.0, 2.0, 3.0, 4.0}));
    EXPECT_EQ(aligned.values[1], (std::vector<double>{5.0, 6.0, 7.0, 8.0}));
    EXPECT_EQ(countNaN(aligned.values[0]) + countNaN(aligned.values[1]), 0u) << "no NaN on the shared-axis path";
}

// Ragged input: two groups sampled on disjoint offsets (the raw, un-bucketed
// case -- DEV-A on the hour, DEV-B at :30).  The axis is the union and each
// group carries NaN exactly where it has no point.
TEST_F(DerivedQueryMultiSeriesTest, AlignBuildsUnionAxisAndFillsGapsWithNaN) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}}, "10hz", {1000, 3000, 5000}, {1.0, 3.0, 5.0}),
                            makeGroup({{"deviceId", "DEV-B"}}, "10hz", {2000, 4000}, {2.0, 4.0})});

    auto aligned = callAlign(multi);

    EXPECT_EQ(aligned.times, (std::vector<uint64_t>{1000, 2000, 3000, 4000, 5000}));
    ASSERT_EQ(aligned.values.size(), 2u);
    ASSERT_EQ(aligned.values[0].size(), 5u);
    ASSERT_EQ(aligned.values[1].size(), 5u);

    EXPECT_DOUBLE_EQ(aligned.values[0][0], 1.0);
    EXPECT_TRUE(std::isnan(aligned.values[0][1]));
    EXPECT_DOUBLE_EQ(aligned.values[0][2], 3.0);
    EXPECT_TRUE(std::isnan(aligned.values[0][3]));
    EXPECT_DOUBLE_EQ(aligned.values[0][4], 5.0);

    EXPECT_TRUE(std::isnan(aligned.values[1][0]));
    EXPECT_DOUBLE_EQ(aligned.values[1][1], 2.0);
    EXPECT_TRUE(std::isnan(aligned.values[1][2]));
    EXPECT_DOUBLE_EQ(aligned.values[1][3], 4.0);
    EXPECT_TRUE(std::isnan(aligned.values[1][4]));
}

// Partial overlap: shared timestamps are NOT duplicated in the union, and a
// value never slides into a neighbouring slot.
TEST_F(DerivedQueryMultiSeriesTest, AlignUnionDeduplicatesSharedTimestamps) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}}, "10hz", {1000, 2000, 3000}, {1.0, 2.0, 3.0}),
                            makeGroup({{"deviceId", "DEV-B"}}, "10hz", {2000, 3000, 4000}, {20.0, 30.0, 40.0})});

    auto aligned = callAlign(multi);

    EXPECT_EQ(aligned.times, (std::vector<uint64_t>{1000, 2000, 3000, 4000}));
    EXPECT_TRUE(std::isnan(aligned.values[0][3]));
    EXPECT_TRUE(std::isnan(aligned.values[1][0]));
    EXPECT_DOUBLE_EQ(aligned.values[0][1], 2.0);
    EXPECT_DOUBLE_EQ(aligned.values[1][1], 20.0);
}

TEST_F(DerivedQueryMultiSeriesTest, AlignSingleGroupIsItsOwnAxis) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}}, "10hz", {1000, 2000}, {1.0, 2.0})});

    auto aligned = callAlign(multi);

    EXPECT_EQ(aligned.times, (std::vector<uint64_t>{1000, 2000}));
    ASSERT_EQ(aligned.values.size(), 1u);
    EXPECT_EQ(aligned.values[0], (std::vector<double>{1.0, 2.0}));
    EXPECT_EQ(countNaN(aligned.values[0]), 0u);
}

TEST_F(DerivedQueryMultiSeriesTest, AlignEmptyResultProducesEmptyAxis) {
    auto aligned = callAlign(makeMulti({}));
    EXPECT_TRUE(aligned.times.empty());
    EXPECT_TRUE(aligned.values.empty());
    EXPECT_TRUE(aligned.groupTags.empty());
}

// ---- group_tags ----------------------------------------------------------

// A SINGLE-field leg must produce byte-identical group_tags to the pre-fan-out
// code: the tag map flattened to "k=v" in tag-key order, and nothing else.
// This is the regression guard for every existing client.
TEST_F(DerivedQueryMultiSeriesTest, AlignSingleFieldLegOmitsTheFieldLabel) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}, {"rack", "r1"}}, "10hz", {1000}, {1.0}),
                            makeGroup({{"deviceId", "DEV-B"}, {"rack", "r2"}}, "10hz", {1000}, {2.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 2u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"deviceId=DEV-A", "rack=r1"}));
    EXPECT_EQ(aligned.groupTags[1], (std::vector<std::string>{"deviceId=DEV-B", "rack=r2"}));
}

// A MULTI-field leg appends "_field=<name>" LAST, after the tags, so the tag
// sequence a client already parses is untouched and the synthetic entry is
// trivially separable.
TEST_F(DerivedQueryMultiSeriesTest, AlignMultiFieldLegAppendsFieldLabelLast) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}, {"rack", "r1"}}, "10hz", {1000}, {1.0}),
                            makeGroup({{"deviceId", "DEV-A"}, {"rack", "r1"}}, "20hz", {1000}, {2.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 2u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"deviceId=DEV-A", "rack=r1", "_field=10hz"}));
    EXPECT_EQ(aligned.groupTags[1], (std::vector<std::string>{"deviceId=DEV-A", "rack=r1", "_field=20hz"}));
    EXPECT_EQ(aligned.groupTags[0].back().rfind("_field=", 0), 0u) << "the field label sorts LAST, after every tag";
}

// A leg with no tags at all (the "{}"-scoped, cross-series-merged case) and one
// field carries an EMPTY group_tags list, exactly as before fan-out.
TEST_F(DerivedQueryMultiSeriesTest, AlignUntaggedSingleFieldLegHasEmptyGroupTags) {
    auto multi = makeMulti({makeGroup({}, "10hz", {1000}, {1.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 1u);
    EXPECT_TRUE(aligned.groupTags[0].empty());
}

// ...and with several fields it carries only the field label.
TEST_F(DerivedQueryMultiSeriesTest, AlignUntaggedMultiFieldLegCarriesOnlyTheFieldLabel) {
    auto multi = makeMulti({makeGroup({}, "10hz", {1000}, {1.0}), makeGroup({}, "20hz", {1000}, {2.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 2u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"_field=10hz"}));
    EXPECT_EQ(aligned.groupTags[1], (std::vector<std::string>{"_field=20hz"}));
}

// ===========================================================================
// D-E: the synthetic field label must not collide with a real tag KEY.
//
// The label started life as "field=<name>", which is exactly what a series
// carrying a tag literally named `field` already flattens to.  `by {field}` is
// in the reporter's own vocabulary, so this is reachable rather than
// theoretical.  The prefixed spelling follows InfluxDB's reserved-column
// convention (_field / _measurement), which this project already follows on
// the write API and the default port.
// ===========================================================================

// A real tag named `field` and the synthetic label now occupy different keys,
// so a client parsing "k=v" into a map keeps both.
TEST_F(DerivedQueryMultiSeriesTest, AlignSyntheticFieldLabelDoesNotCollideWithATagNamedField) {
    auto multi = makeMulti(
        {makeGroup({{"field", "TAGVAL"}}, "a", {1000}, {1.0}), makeGroup({{"field", "TAGVAL"}}, "b", {1000}, {2.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 2u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"field=TAGVAL", "_field=a"}));
    EXPECT_EQ(aligned.groupTags[1], (std::vector<std::string>{"field=TAGVAL", "_field=b"}));

    // The point of the rename: no duplicated KEY anywhere in the list.
    for (const auto& tags : aligned.groupTags) {
        std::set<std::string> keys;
        for (const auto& kv : tags) {
            EXPECT_TRUE(keys.insert(kv.substr(0, kv.find('='))).second)
                << "duplicate key in group_tags: " << kv << " -- a client parsing k=v loses one";
        }
    }
}

// ...and the single-field variant of the same query is no longer ambiguous
// with a synthetic label: a lone "field=TAGVAL" can now only be the real tag.
TEST_F(DerivedQueryMultiSeriesTest, AlignSingleFieldLegWithATagNamedFieldIsUnambiguous) {
    auto multi = makeMulti({makeGroup({{"field", "TAGVAL"}}, "a", {1000}, {1.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 1u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"field=TAGVAL"}));
}

// DOCUMENTED PATHOLOGICAL CASE: a tag literally named `_field` still collides.
// Position is the only discriminator -- the real tag keeps its tag position,
// the synthetic label is always last -- which is why that position is a
// contract rather than an implementation detail.  Pinned so a future change
// that reorders group_tags has to confront it.
TEST_F(DerivedQueryMultiSeriesTest, AlignTagLiterallyNamedUnderscoreFieldStillCollidesButOrderIsPinned) {
    auto multi = makeMulti({makeGroup({{"_field", "TAGVAL"}, {"deviceId", "DEV-A"}}, "a", {1000}, {1.0}),
                            makeGroup({{"_field", "TAGVAL"}, {"deviceId", "DEV-A"}}, "b", {1000}, {2.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 2u);
    EXPECT_EQ(aligned.groupTags[0], (std::vector<std::string>{"_field=TAGVAL", "deviceId=DEV-A", "_field=a"}));
    EXPECT_EQ(aligned.groupTags[1], (std::vector<std::string>{"_field=TAGVAL", "deviceId=DEV-A", "_field=b"}));
    EXPECT_EQ(aligned.groupTags[0].back(), "_field=a") << "the synthetic label is ALWAYS last";
}

// ===========================================================================
// D-A: the fan-out output bound.
//
// maxSeriesPerLeg bounds the number of GROUPS; nothing bounded their product
// with the shared axis, and that product is what the response size tracks.
// Groups sampled at staggered offsets widen the union axis as they are added,
// so the dense matrix is O(G^2 * N) in the stored point count: 50 devices x
// 2000 staggered points (100k stored, 2.6 MB from /query) produced 40M cells
// and a 402 MB HTTP 200 from /derived.
// ===========================================================================

// Under the bound: aligned normally.
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutUnderTheBoundSucceeds) {
    // 3 groups x 4 union timestamps = 12 cells, bound 12.
    auto multi = makeMulti({makeGroup({{"d", "A"}}, "v", {1000, 2000}, {1.0, 2.0}),
                            makeGroup({{"d", "B"}}, "v", {1500, 2500}, {3.0, 4.0}),
                            makeGroup({{"d", "C"}}, "v", {1000, 2500}, {5.0, 6.0})});

    auto aligned = callAlignBounded(multi, 12);

    EXPECT_EQ(aligned.times.size(), 4u);
    ASSERT_EQ(aligned.values.size(), 3u);
    EXPECT_EQ(aligned.values[0].size(), 4u);
}

// One cell over: refused, and the message names BOTH factors and the limit.
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutOverTheBoundThrowsNamingGroupsAxisAndLimit) {
    auto multi = makeMulti({makeGroup({{"d", "A"}}, "v", {1000, 2000}, {1.0, 2.0}),
                            makeGroup({{"d", "B"}}, "v", {1500, 2500}, {3.0, 4.0}),
                            makeGroup({{"d", "C"}}, "v", {1000, 2500}, {5.0, 6.0})});

    try {
        callAlignBounded(multi, 11);
        FAIL() << "expected the fan-out output bound to reject 3 groups x 4 timestamps against a limit of 11";
    } catch (const DerivedQueryException& e) {
        const std::string msg = e.what();
        EXPECT_NE(msg.find("3"), std::string::npos) << msg;   // groups
        EXPECT_NE(msg.find("4"), std::string::npos) << msg;   // axis length
        EXPECT_NE(msg.find("11"), std::string::npos) << msg;  // the limit
        // The actionable advice: an interval collapses the staggered union,
        // which is the actual amplifier.
        EXPECT_NE(msg.find("aggregationInterval"), std::string::npos) << msg;
        EXPECT_NE(msg.find("scope"), std::string::npos) << msg;
    }
}

// Beyond one group the bound applies to a SHARED axis too.  Moving the columns
// costs nothing, but the RESULT is still groups x axis -- the forecast path
// expands each group into four pieces spanning it -- so several groups over a
// huge shared axis amplify just as hard as a staggered leg does.
//
// ...and a SINGLE-group leg is EXEMPT, however long its axis.  That case is
// the pre-fan-out behaviour this work must not change: it is linear in N
// rather than quadratic, and it was already unbounded (subject only to
// http.max_total_points) before the fan-out existed.  Both halves are pinned
// here, on the same axis and against the same limit, so the exemption cannot
// be mistaken for the axis being short enough.
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutBoundExemptsOneGroupAndAppliesToTheSharedAxisFastPath) {
    std::vector<uint64_t> axis;
    std::vector<double> values;
    for (uint64_t i = 0; i < 10; ++i) {
        axis.push_back(1000 + i);
        values.push_back(static_cast<double>(i));
    }

    // ONE group, axis of 10, limit of 1: far over the limit, and allowed.
    auto single = makeMulti({makeGroup({{"d", "A"}}, "v", axis, values)});
    AlignedView aligned;
    ASSERT_NO_THROW({ aligned = callAlignBounded(single, 1); })
        << "a single-group leg must behave exactly as it did before the fan-out existed";
    EXPECT_EQ(aligned.times.size(), 10u);
    ASSERT_EQ(aligned.values.size(), 1u);
    EXPECT_EQ(aligned.values[0].size(), 10u);

    // TWO groups over the SAME axis and the same limit: refused.
    auto pair = makeMulti({makeGroup({{"d", "A"}}, "v", axis, values), makeGroup({{"d", "B"}}, "v", axis, values)});
    EXPECT_THROW(callAlignBounded(pair, 1), DerivedQueryException)
        << "the exemption is for ONE group only -- two groups is where the amplification starts";

    // ...and the exact boundary still holds for several groups on the fast path.
    auto three = makeMulti({makeGroup({{"d", "A"}}, "v", axis, values), makeGroup({{"d", "B"}}, "v", axis, values),
                            makeGroup({{"d", "C"}}, "v", axis, values)});
    EXPECT_THROW(callAlignBounded(three, 29), DerivedQueryException);  // 3 x 10 = 30
    EXPECT_NO_THROW(callAlignBounded(three, 30));
}

// The exemption is decided by the GROUP count, not by the leg looking small:
// one group carrying an enormous axis is still exempt, and it is the group
// count alone that flips the bound on.
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutBoundExemptionIsDecidedByGroupCountAloneNotAxisLength) {
    constexpr size_t kLongAxis = 5000;
    std::vector<uint64_t> axis;
    std::vector<double> values;
    axis.reserve(kLongAxis);
    values.reserve(kLongAxis);
    for (uint64_t i = 0; i < kLongAxis; ++i) {
        axis.push_back(1000 + i);
        values.push_back(static_cast<double>(i));
    }

    // 1 x 5000 cells against a 100-cell limit: exempt.
    EXPECT_NO_THROW(callAlignBounded(makeMulti({makeGroup({{"d", "A"}}, "v", axis, values)}), 100));

    // Adding a SECOND group -- and nothing else -- turns the same leg into an
    // error.  Both groups share the axis, so the axis length is unchanged.
    EXPECT_THROW(
        callAlignBounded(
            makeMulti({makeGroup({{"d", "A"}}, "v", axis, values), makeGroup({{"d", "B"}}, "v", axis, values)}), 100),
        DerivedQueryException);
}

// The customer's actual use case must not be blocked: bucketing collapses the
// staggered union onto one grid, and 50 devices over a day of one-minute
// buckets is 72k cells -- an order of magnitude under the default bound.
TEST_F(DerivedQueryMultiSeriesTest, AlignBucketedFanOutAtTheSeriesCapStaysUnderTheDefaultBound) {
    const size_t kGroups = 50;     // the reporter's fleet size, well under maxSeriesPerLeg
    const size_t kBuckets = 1440;  // one day of one-minute buckets

    std::vector<uint64_t> axis;
    std::vector<double> values;
    axis.reserve(kBuckets);
    values.reserve(kBuckets);
    for (size_t i = 0; i < kBuckets; ++i) {
        axis.push_back(1000000000ULL + static_cast<uint64_t>(i) * 60000000000ULL);
        values.push_back(static_cast<double>(i));
    }

    std::vector<SubQuerySeries> groups;
    for (size_t g = 0; g < kGroups; ++g) {
        groups.push_back(makeGroup({{"deviceId", "DEV-" + std::to_string(g)}}, "v", axis, values));
    }

    const size_t defaultBound = DerivedQueryConfig{}.maxFanOutPoints;
    EXPECT_LT(kGroups * kBuckets, defaultBound);
    EXPECT_NO_THROW(callAlignBounded(makeMulti(groups), defaultBound));
}

// With maxSeriesPerLeg raised to 200, the CELL bound is what binds on a large
// leg: a leg at the full series cap is refused as soon as its axis passes
// maxFanOutPoints / 200 slots, long before anything else notices.  This is the
// argument for raising the series cap in test form -- the memory guard is the
// cell bound, and it still fires.
TEST_F(DerivedQueryMultiSeriesTest, AtTheRaisedSeriesCapTheCellBoundIsWhatBinds) {
    const size_t kGroups = DerivedQueryConfig{}.maxSeriesPerLeg;  // 200
    const size_t bound = DerivedQueryConfig{}.maxFanOutPoints;    // 250,000
    const size_t kAtLimit = bound / kGroups;                      // 1250 slots

    auto buildLeg = [](size_t groups, size_t slots) {
        std::vector<uint64_t> axis;
        std::vector<double> values;
        axis.reserve(slots);
        values.reserve(slots);
        for (size_t i = 0; i < slots; ++i) {
            axis.push_back(1000000000ULL + static_cast<uint64_t>(i) * 60000000000ULL);
            values.push_back(static_cast<double>(i));
        }
        std::vector<SubQuerySeries> out;
        out.reserve(groups);
        for (size_t g = 0; g < groups; ++g) {
            out.push_back(makeGroup({{"deviceId", "DEV-" + std::to_string(g)}}, "v", axis, values));
        }
        return out;
    };

    EXPECT_NO_THROW(callAlignBounded(makeMulti(buildLeg(kGroups, kAtLimit)), bound));
    EXPECT_THROW(callAlignBounded(makeMulti(buildLeg(kGroups, kAtLimit + 1)), bound), DerivedQueryException);
}

// A bound of 0 disables the check (used by the alignment tests above).
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutBoundOfZeroIsDisabled) {
    auto multi = makeMulti({makeGroup({{"d", "A"}}, "v", {1000, 2000}, {1.0, 2.0}),
                            makeGroup({{"d", "B"}}, "v", {1500, 2500}, {3.0, 4.0})});

    EXPECT_NO_THROW(callAlignBounded(multi, 0));
}

// An empty leg has no axis and no groups; the bound must not fire (nor divide
// by a zero axis length).
TEST_F(DerivedQueryMultiSeriesTest, AlignFanOutBoundIgnoresAnEmptyLeg) {
    MultiSeriesSubQueryResult empty;
    empty.queryName = "a";
    EXPECT_NO_THROW(callAlignBounded(std::move(empty), 1));
}

// The label is decided by the number of DISTINCT fields across the leg, not by
// the number of groups: N devices all reporting the same single field stay
// unlabelled.
TEST_F(DerivedQueryMultiSeriesTest, AlignFieldLabelTracksDistinctFieldsNotGroupCount) {
    auto multi = makeMulti({makeGroup({{"deviceId", "DEV-A"}}, "10hz", {1000}, {1.0}),
                            makeGroup({{"deviceId", "DEV-B"}}, "10hz", {1000}, {2.0}),
                            makeGroup({{"deviceId", "DEV-C"}}, "10hz", {1000}, {3.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.groupTags.size(), 3u);
    for (const auto& tags : aligned.groupTags) {
        ASSERT_EQ(tags.size(), 1u);
        EXPECT_EQ(tags[0].rfind("deviceId=", 0), 0u) << tags[0];
    }
}

// ===========================================================================
// R6: the shared-axis FAST PATH must normalise its row to the axis width.
//
// It is the one site that pushes a value column it did not build itself: the
// union path always sizes its row to aligned.times.size() and clamps the
// source with std::min, but the fast path moves the caller's column straight
// onto the axis.  A column shorter than its own timestamp vector then leaves a
// row narrower than `times`, and every consumer indexes a row against `times`
// (the forecasters, the detectors, the history trim).
//
// Not demonstrated reachable -- convertQueryResponseMulti() moves a /query
// response's PAIRED timestamp and value columns, so they match -- but the
// alignment layer is where the invariant belongs, and NaN is exactly what a
// slot with no value means (docs/nan_policy.md).
// ===========================================================================
TEST_F(DerivedQueryMultiSeriesTest, AlignSharedAxisPadsAShortValueColumnToTheAxis) {
    const std::vector<uint64_t> axis = {1000, 2000, 3000, 4000};

    // Identical timestamps -> the shared-axis fast path.  The first group's
    // value column is two short.
    auto multi = makeMulti(
        {makeGroup({{"d", "A"}}, "v", axis, {1.0, 2.0}), makeGroup({{"d", "B"}}, "v", axis, {10.0, 20.0, 30.0, 40.0})});

    auto aligned = callAlign(multi);

    ASSERT_EQ(aligned.times.size(), 4u);
    ASSERT_EQ(aligned.values.size(), 2u);
    for (const auto& row : aligned.values) {
        EXPECT_EQ(row.size(), aligned.times.size()) << "every row must span the axis";
    }
    EXPECT_DOUBLE_EQ(aligned.values[0][0], 1.0);
    EXPECT_DOUBLE_EQ(aligned.values[0][1], 2.0);
    EXPECT_TRUE(std::isnan(aligned.values[0][2])) << "a slot with no value is missing, not zero";
    EXPECT_TRUE(std::isnan(aligned.values[0][3]));
    EXPECT_DOUBLE_EQ(aligned.values[1][3], 40.0);
}
