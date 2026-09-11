#pragma once

#include "anomaly/anomaly_executor.hpp"
#include "anomaly/anomaly_result.hpp"
#include "derived_query.hpp"
#include "expression_evaluator.hpp"
#include "expression_parser.hpp"
#include "forecast/forecast_executor.hpp"
#include "forecast/forecast_result.hpp"
#include "http_query_handler.hpp"
#include "query_parser.hpp"
#include "series_aligner.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <string>
#include <variant>

// Forward declarations
class Engine;
class DerivedQueryMultiSeriesTest;

namespace timestar {

// Configuration for derived query execution
struct DerivedQueryConfig {
    // Alignment strategy for combining series
    AlignmentStrategy alignmentStrategy = AlignmentStrategy::INNER;

    // Interpolation method for missing values
    InterpolationMethod interpolationMethod = InterpolationMethod::LINEAR;

    // Maximum number of sub-queries allowed
    size_t maxSubQueries = 10;

    // Maximum number of series -- one per (tag set x numeric field) -- that a
    // single sub-query leg may resolve to on the multi-series path.
    //
    // Same reasoning as the kMaxConcurrentSubQueries fan-out ceiling in
    // executeAllSubQueries(): a leg is bounded only by http.max_total_points,
    // and every group it resolves to is fitted independently and emits four
    // response pieces of its own, so an unscoped leg over a high-cardinality
    // measurement is an unbounded memory/CPU multiplier. Bound it where the
    // fan-out is first known instead of discovering it during the fit.
    //
    // This is a CARDINALITY sanity bound, not the memory bound: the memory
    // bound is maxFanOutPoints (below), which counts groups x axis and is the
    // constraint that actually binds on any leg wide enough to hurt.  Held at
    // 50 it refused shapes that cost nothing -- the count is (tag set x field),
    // so a two-metric per-device dashboard, `avg:m(v,w){} by {dev}` over a
    // 50-device fleet, resolved to 100 and was rejected while asking for a few
    // thousand cells.  200 covers the fleet-sized dashboards this endpoint
    // exists to serve; anything genuinely large hits the cell bound first (200
    // groups reach 250,000 cells at a 1250-slot axis, a shape the cell bound
    // was measured against).
    size_t maxSeriesPerLeg = 200;

    // Maximum number of CELLS -- groups x shared-axis length -- in the dense
    // matrix the fan-out builds, and therefore the bound on the response
    // computed from it.
    //
    // maxSeriesPerLeg (above) bounds the number of GROUPS; nothing bounded
    // their product with the axis, and that product is where the amplification
    // lives.  Groups sampled at staggered offsets do not share a time axis, so
    // the union axis grows with the group count and EVERY group is then
    // densified across the whole of it: the matrix is O(G^2 * N) in the stored
    // point count, not O(G * N).  Measured at the then-50-group maxSeriesPerLeg
    // ceiling on a trivially small dataset -- 50 devices x 2000 points, 100k
    // points stored,
    // 2.6 MB from POST /query -- the fan-out built 40M cells and POST /derived
    // answered HTTP 200 with a 402 MB body, a 400x amplification, all of it
    // allocated in the reactor's heap.  http.max_total_points bounds the LEG
    // (what was read); this bounds the RESULT (what is computed and returned).
    //
    // WHAT A CELL COSTS, measured rather than assumed.  The bound counts the
    // INPUT matrix, but what matters is the OUTPUT it is turned into, and the
    // expansion factor is 8x, not 4x:
    //
    //   * executeForecast() OVERRIDES the auto horizon (which is capped at
    //     2000 points) with `duration / interval`, where interval is the
    //     axis's own sampling interval.  Over a window the leg actually covers,
    //     duration / interval IS the number of axis slots -- so the horizon is
    //     ~N, on a bucketed leg exactly as much as on a raw one, and the
    //     response axis is ~2N rather than N.  Over a window far WIDER than the
    //     leg's span the horizon is unrelated to N and can be arbitrarily
    //     larger; that case is not a function of the input matrix at all and is
    //     bounded by maxForecastOutputPoints (below), not here.
    //   * each group emits FOUR pieces (past, forecast, upper, lower), each
    //     one 2N slots wide.
    //
    // So one cell of the input matrix becomes ~8 slots of
    // std::vector<std::optional<double>> (16 B each on this ABI) plus its JSON
    // rendering.  At a 1,000,000-cell bound that is a MEASURED 79.6 MB body,
    // ~124 MB of piece storage live at the same time, and a 66 ms reactor
    // stall on a single request -- for an endpoint whose own request body is
    // capped at 1 MB.  (An earlier version of this comment claimed ~40 MB at
    // the bound; it had assumed the 2000-point auto horizon, which this path
    // never uses.)
    //
    // Hence 250,000 rather than 1,000,000.  MEASURED at exactly that ceiling
    // (50 groups x a 5000-bucket shared axis, one-minute buckets): 200 pieces
    // over a 10,000-slot axis == 2,000,000 slots, confirming the 8x factor to
    // the point; a 20.1 MB body and 43 ms of server time.  That is still not
    // cheap, and it is meant to be the WORST case rather than a target -- this
    // project treats reactor stalls as defects (see
    // test/performance/aggregation_stall_benchmark.cpp).  It clears the
    // legitimate per-device dashboard shapes by 2.5-3.5x: the reporter's own
    // 50 devices x 1440 one-minute buckets is 72k cells (~5.8 MB, ~12 ms), 50
    // devices x 5m buckets over a week is 101k, 20 devices x 10s buckets over
    // a day is 173k.  What it now refuses -- 50 devices at one-minute
    // resolution over a week, 504k cells -- is a request whose honest answer
    // is a ~40 MB synchronous JSON body, which the caller should be splitting
    // anyway; the error message says how (bucket coarser, or narrow the
    // scope).
    //
    // An aggregationInterval collapses a staggered union onto one shared
    // bucket grid, which removes the quadratic term outright -- hence the
    // advice the error message gives.  It does NOT reduce the 8x expansion,
    // which is why the bound is on cells and not on the union's shape.
    //
    // A leg that resolves to exactly ONE group is exempt: that case is the
    // pre-fan-out behaviour this work must not change, it is linear in N
    // rather than quadratic, and it was already unbounded (subject only to
    // http.max_total_points) before this campaign.  See the check itself in
    // alignSubQueryGroups().  That exemption does NOT extend to
    // maxForecastOutputPoints below, which counts fabricated slots rather than
    // stored ones -- see there.
    //
    // SHARED with multi-series ARITHMETIC derived (the "multiSeries" opt-in),
    // where it bounds the same quantity -- groups x per-group axis, i.e. the
    // total points the response carries -- for the same reason, and where the
    // one-group exemption again keeps the pre-campaign single-series answer
    // exactly as it was.  Deliberately the SAME constant rather than a fourth
    // one: the arithmetic path emits ONE (timestamp, value) pair per cell where
    // the forecast path emits ~8 slots, so at an identical cell count it is
    // strictly the cheaper of the two and 250,000 is if anything generous
    // there (~250k points, ~10 MB of JSON).
    //
    // On that path it is checked BEFORE each group is materialised, from
    // SeriesAligner::projectedOutputSize() -- which sizes the axis from the
    // inputs' lengths and end timestamps without building anything -- and again
    // after the group as a backstop.  An earlier revision checked only
    // afterwards, which meant the FIRST group was never checked at all: four
    // stored points with a one-second aggregationInterval over a thirty-day
    // window materialised 2,592,001 points, 10.4x this budget, with a 33 ms
    // reactor stall, before being refused.
    //
    // maxForecastOutputPoints (below) does not apply to the arithmetic path --
    // it counts a horizon the path has none of -- but do NOT read that as "an
    // arithmetic formula fabricates no points".  It does: with an
    // aggregationInterval, SeriesAligner::resampleTimestamps() builds a DENSE
    // grid from the first aligned timestamp to the last, so 4 stored points over
    // a 30-day window at a one-second interval emit 2,592,001 (a measured
    // 56.85 MB body and two reactor stalls, 67 ms and 128 ms).  That behaviour
    // predates this campaign -- the single-series path does exactly the same,
    // byte for byte -- and what bounds it here is this constant, enforced by the
    // pre-check above.  A ONE-GROUP result stays exempt and therefore stays
    // unbounded, deliberately: that is the pre-flag answer, and the exemption
    // exists so the flag cannot make a working query start failing.
    size_t maxFanOutPoints = 250000;

    // Maximum number of OUTPUT points a forecast() leg may produce:
    // `groups * (historical axis + forecast horizon)`, checked before the
    // forecast runs.  Exceeding it is a 400 naming the numbers.
    //
    // The horizon on this path is `duration / interval` -- the number of slots
    // the requested WINDOW holds at the leg's own sampling rate.  When the data
    // spans the window that is just the axis length, and everything here is
    // inert.  When the data covers only a SLIVER of the window the two diverge
    // without limit: 60 points at one-second spacing inside a 30-day window
    // asks for 2,592,000 forecast points and produced a MEASURED ~200 MB
    // synchronous JSON body from ~480 bytes of stored data, in 114 ms of server
    // time, with reactor stalls of 442, 233, 124, 66 and 66 ms.  (That body
    // size is FIXTURE-SPECIFIC -- an independent reproduction of the same
    // 60@1s-in-30-days shape measured 122.19 MB.  Each of the 2.59M slots is
    // rendered as text, so the total tracks the DECIMAL WIDTH of the stored
    // values; the POINT COUNT, which is what this bound counts, does not vary.)
    // maxFanOutPoints
    // cannot catch that -- the amplification is not a function of the input
    // matrix -- so a caller refused by the cell bound could reach the same cost
    // through a single-group leg.
    //
    // REFUSE, never TRUNCATE.  An earlier version of this fix clamped the
    // horizon to max(2000, N) instead.  That silently shortened legitimate
    // forecasts with nothing in the response to say so: 1440 one-minute points
    // projected over a 30-day window went from 43,200 forecast points to 2,000
    // (33 hours of a 30-day ask), 7 days of minute data over a 90-day window
    // from 129,600 to 10,080, and the same leg with history='1h' to 2,000
    // (the trim drops N to 61, so the clamp collapses to its floor).  A fixed
    // horizon ceiling cannot tell an ordinary long projection from 2.59M points
    // extrapolated out of 60 observations -- only the OUTPUT SIZE can, because
    // that is the thing that actually hurts.  So: answer, or refuse with a
    // reason the caller can act on.
    //
    // Applies to EVERY leg, single-group included -- unlike maxFanOutPoints.
    // The single-group exemption there exists to keep returning data the user
    // actually STORED; forecast slots are fabricated, and a one-device leg
    // fabricating 2.6M of them is exactly the case measured above.
    //
    // 500,000: TWICE maxFanOutPoints, and deliberately NOT the same number.
    // Do not "harmonise" the two constants -- they count different things and
    // the factor of two is what keeps them from colliding.
    //
    // The cell bound counts `groups x axis`.  This one counts
    // `groups x (axis + horizon)`, and for the ordinary leg -- one that SPANS
    // its window -- `horizon ~= axis`, so this quantity is about 2 x cells.
    // Set to the same constant, the output bound is therefore strictly tighter
    // on every forecast path and silently SUBSUMES the cell bound: it starts
    // refusing the shapes maxFanOutPoints was measured and calibrated to
    // admit.  At 250,000 it refused 200 groups over a 1250-slot axis with the
    // window equal to the data span (499,800 -- the cell bound's own ceiling
    // shape) and 20 devices at 10-second buckets over a day (345,580), which
    // the maxFanOutPoints comment above lists as a legitimate per-device
    // dashboard.  A guard that rejects a real dashboard is worse than one that
    // admits a large response.
    //
    // At 2 x the cell budget the two bounds sit in their intended relationship:
    // the cell bound governs the INPUT matrix (and is the only one that binds
    // on anomalies(), which has no horizon), while this one governs the
    // fabricated output and binds only when the horizon is out of proportion to
    // the data -- which is the case it was introduced for.
    //
    // Shapes admitted (all MEASURED): 1440 one-minute points over a 30-day
    // window (44,640), 7 days of minute data over 90 days (139,680), 120 daily
    // buckets over 120 days (239), 50 devices x 1440 one-minute buckets
    // (143,950), 20 devices x 10-second buckets over a day (345,580 -- 13.19 MB,
    // 20-32 ms, NO reactor stall over three runs), 200 groups over a 1250-slot
    // axis with the window equal to the span (499,800 -- 18.67 MB, 30-40 ms).
    // Shapes refused: the 60@1s sliver above (2,592,060) and 200 groups over a
    // 1250-slot axis with a sliver window (8,890,000).  0 disables the bound.
    //
    // KNOWN, and deliberately not fixed here: that last ADMITTED shape stalls
    // the reactor -- 2 stalls of 65 ms across 3 runs, with zero during the
    // seeding of the same data, so it is the forecast and not the write path.
    // This project treats reactor stalls as defects (see
    // test/performance/aggregation_stall_benchmark.cpp), so it is a real one.
    //
    // An earlier version of this note argued the stall was "not this constant's
    // to fix" because 200 groups x 1250 slots is exactly maxFanOutPoints' own
    // 250,000-cell ceiling, "which admits the shape independently".  That
    // argument is REFUTED: a SINGLE-GROUP leg at 499,680 output points stalls
    // identically (68 ms), and the cell bound EXEMPTS single-group legs -- so on
    // that shape this constant is the only guard there is, and the stall cannot
    // be laid at the cell bound's door.
    //
    // The CONCLUSION survives on different grounds: the stall is PRE-EXISTING.
    // It reproduces on the pristine pre-campaign binary at 69 ms for the same
    // 37.57 MB body, so it is neither introduced nor made reachable by the
    // fan-out work.  Symbolized, the time is in JSON serialization
    // (glz::write_json<GlazeForecastResponse>) and in the basic_sstring copy of
    // the finished body into the reply -- it is a function of RESPONSE BYTES,
    // not of group count or fit cost, and it appears only on a cold heap.
    // Lowering this bound under 499,800 would therefore trade a real dashboard
    // (see the admitted shapes above) for a partial mitigation of a cost that
    // large single-group responses incur anyway.  The fix is chunked or
    // yielding serialization in the response path, not a smaller constant.
    size_t maxForecastOutputPoints = 500000;
};

// Result type that can be regular, anomaly, or forecast result
using DerivedQueryResultVariant =
    std::variant<DerivedQueryResult, anomaly::AnomalyQueryResult, forecast::ForecastQueryResult>;

// Executes derived metric queries
class DerivedQueryExecutor {
    friend class ::DerivedQueryMultiSeriesTest;

public:
    DerivedQueryExecutor(seastar::sharded<Engine>* engine, DerivedQueryConfig config = {});

    // Execute a derived query
    seastar::future<DerivedQueryResult> execute(const DerivedQueryRequest& request);

    // Execute from JSON request body
    seastar::future<DerivedQueryResult> executeFromJson(const std::string& jsonBody);

    // Execute with support for anomaly detection
    seastar::future<DerivedQueryResultVariant> executeWithAnomaly(const DerivedQueryRequest& request);

    // Execute from JSON with anomaly support
    seastar::future<DerivedQueryResultVariant> executeFromJsonWithAnomaly(const std::string& jsonBody);

    // Format result as JSON response
    std::string formatResponse(const DerivedQueryResult& result);

    // Format anomaly result as JSON response
    std::string formatAnomalyResponse(const anomaly::AnomalyQueryResult& result);

    // Format forecast result as JSON response
    std::string formatForecastResponse(const forecast::ForecastQueryResult& result);

    // Format variant result as JSON response
    std::string formatResponseVariant(const DerivedQueryResultVariant& result);

    // Create error response JSON
    static std::string createErrorResponse(const std::string& code, const std::string& message);

    // Check if a formula is an anomaly function
    static bool isAnomalyFormula(const std::string& formula);

    // Check if a formula is a forecast function
    static bool isForecastFormula(const std::string& formula);

private:
    friend class DerivedQueryCachedAstSafetyTest;  // Test access to cachedAst_

    seastar::sharded<Engine>* engine_;
    DerivedQueryConfig config_;
    const ExpressionNode* cachedAst_ = nullptr;  // Avoids re-parsing in executeWithAnomaly→execute

    // Execute a single sub-query.  Takes the QueryRequest by value: callers
    // may pass a temporary (e.g. with the request-level aggregation interval
    // applied), and a by-value coroutine parameter is copied into the
    // coroutine frame, so it cannot dangle across suspension points.
    seastar::future<SubQueryResult> executeSubQuery(const std::string& name, QueryRequest query);

    // Execute a single sub-query on the MULTI-series path: the leg may resolve
    // to any number of (tag set x numeric field) groups, up to
    // config_.maxSeriesPerLeg.  Used by forecast() and anomalies(), which fit
    // each group independently; the arithmetic formula path still goes through
    // executeSubQuery() above, which folds a leg to one series or refuses it.
    seastar::future<MultiSeriesSubQueryResult> executeSubQueryMulti(const std::string& name, QueryRequest query);

    // Apply the request-level aggregation interval to one sub-query leg and
    // return the leg to execute.  A leg that already carries its own interval
    // keeps it (the correct precedence rule should the query grammar ever gain
    // a per-leg interval); otherwise it inherits the request's.
    //
    // EVERY executeSubQuery() call site must route through this.  The forecast
    // and anomaly paths used to pass the raw leg straight out of the request,
    // so forecast() / anomalies() silently ran at raw resolution while the
    // identical request with an arithmetic formula honoured the interval.
    static QueryRequest applyAggregationInterval(const DerivedQueryRequest& request, const QueryRequest& leg);

    // Execute all sub-queries in parallel
    seastar::future<std::map<std::string, SubQueryResult>> executeAllSubQueries(const DerivedQueryRequest& request);

    // Same, on the MULTI-series path: every referenced leg may resolve to any
    // number of (tag set x field) groups.  Same referenced-only filter and same
    // concurrency ceiling as executeAllSubQueries().
    seastar::future<std::map<std::string, MultiSeriesSubQueryResult>> executeAllSubQueriesMulti(
        const DerivedQueryRequest& request);

    // Evaluate an ARITHMETIC formula ONCE PER GROUP -- the "multiSeries"
    // opt-in.  Reached only from execute() and only when
    // DerivedQueryRequest::multiSeries is set; without the flag the flat
    // single-series path is untouched.
    //
    // The rules, in the order they are applied:
    //
    //  1. PAIRING.  A group's identity is its (tag set, field) key -- exactly
    //     the key convertQueryResponseMulti() already emits.  Legs are paired
    //     key by key and the formula is evaluated over each pair independently,
    //     reusing SeriesAligner and ExpressionEvaluator unchanged (one
    //     map<legName, SubQueryResult> per group, i.e. precisely the input the
    //     single-series path builds).
    //
    //     EXCEPT when every leg resolves to exactly one distinct field name, in
    //     which case the key is the TAG SET alone.  The field then distinguishes
    //     nothing within a leg, while including it makes the unambiguous
    //     cross-measurement pairing impossible: `cpu.user / mem.used by {host}`
    //     is one series per host on each side and was refused with advice no
    //     caller could act on.
    //
    //  2. BROADCAST.  A leg resolving to exactly ONE group is paired with EVERY
    //     group of the others -- provided its own tag set is EMPTY or a subset
    //     of every output group's.  That covers `per_device_bytes /
    //     fleet_total` (an untagged total) and a leg scoped to a tag the output
    //     groups all share, and it keeps every formula that worked before this
    //     flag working unchanged (all legs single -> one group out).
    //
    //     Arity alone is NOT sufficient and treating it as sufficient was a
    //     silent wrong answer: a leg scoped to `{dev:DEV-A}` broadcast across
    //     DEV-A/B/C answered 200 with a group LABELLED dev=DEV-B whose value
    //     came from DEV-A.  A tagged single group that is not a subset is a 400
    //     of its own -- the cause is an over-narrow scope filter rather than the
    //     disagreeing key sets of rule 3, and the remedy differs.
    //
    //     The subset test applies only where there IS fan-out.  When EVERY leg
    //     resolves to one group the legs pair regardless of tags -- comparing
    //     two named hosts is a legitimate query and answers as it does without
    //     the flag -- and the only thing at risk is the label, so the single
    //     output group carries NO tags unless every leg agreed on them.  One
    //     group standing in for many is the defect; one group paired with one
    //     group is not.
    //
    //  3. MISMATCH IS A 400.  Two legs that BOTH resolve to more than one group
    //     must resolve to the SAME keys.  A silent intersection would answer a
    //     fleet-wide question with a subset of the fleet and look successful, so
    //     the difference is named in both directions instead.
    //
    //  4. ORDER.  The output follows the REFERENCE leg's group order -- the
    //     first leg (by sub-query name, i.e. std::map order) that resolves to
    //     more than one group, or the first leg at all when none does.  Each
    //     leg's own order is already the fan-out convention (tags ascending,
    //     then field rank, then field name, then input index), so the output
    //     inherits it; taking it from a NAMED leg rather than re-deriving it is
    //     what makes the sequence deterministic when two legs disagree about
    //     field order (`a = m(x,y)` against `b = m(y,x)`).
    //
    //  5. SHAPE.  The answer is DerivedQueryResult::series and nothing else, at
    //     every group count including one; the flat timestamps/values stay
    //     empty.  Filling both for a one-group answer (an earlier revision) let
    //     a client adopt the flag before the array, but doubled the body for
    //     good -- a measured 56.85 MB became 113.71 MB -- and the one-group case
    //     is exempt from maxFanOutPoints, so nothing capped it.
    //
    //  6. BUDGET.  maxFanOutPoints bounds groups x per-group axis, checked
    //     BEFORE each group is materialised (see the call to
    //     SeriesAligner::projectedOutputSize()) and again afterwards as a
    //     backstop.  A one-group result is exempt.
    //
    // Sets everything on the result except `formula` and the execution time,
    // which execute() owns so that both paths time the same span.
    seastar::future<DerivedQueryResult> executeMultiSeries(const DerivedQueryRequest& request);

    // Convert HTTP query handler results to SubQueryResult.
    // Takes the results by rvalue reference and moves the timestamp/value
    // vectors out (the response is locally owned and discarded by the caller).
    SubQueryResult convertQueryResponse(const std::string& name, const QueryRequest& query,
                                        std::vector<SeriesResult>&& results);

    // Convert HTTP query handler results to a MULTI-series sub-query result:
    // one entry per (series tag set x numeric field) pair, the group model
    // behind per-device forecast()/anomalies().  Unlike convertQueryResponse()
    // it neither refuses a leg that resolved to several series nor silently
    // keeps only the first requested field.
    //
    // Takes the results by rvalue reference and moves the timestamp/value
    // vectors out, exactly as the single-series converter does.
    MultiSeriesSubQueryResult convertQueryResponseMulti(const std::string& name, const QueryRequest& query,
                                                        std::vector<SeriesResult>&& results);

    // A leg's groups projected onto ONE shared time axis, which is the shape
    // ForecastQueryResult / AnomalyQueryResult carry (a single `times` vector
    // for every series piece) and what ForecastExecutor::executeMulti() and
    // AnomalyExecutor::executeMulti() consume.
    struct AlignedSubQueryGroups {
        std::vector<uint64_t> times;                      // sorted, unique union of every group's timestamps
        std::vector<std::vector<double>> values;          // one row per group, times.size() wide
        std::vector<std::vector<std::string>> groupTags;  // one row per group, parallel to `values`
    };

    // Build the shared axis and fill each group against it, using NaN for the
    // timestamps a group does not carry.  NaN means "missing" here (canonical,
    // docs/nan_policy.md), and every consumer of these vectors treats it that
    // way: SeasonalForecaster replaces non-finite values with the mean of the
    // finite ones (and refuses an all-NaN input outright), BasicDetector and
    // RobustDetector score a NaN input as missing data rather than an anomaly,
    // and LinearForecaster::fitLinearRegression skips non-finite points --
    // which it only genuinely started doing in this change: it used to zero
    // their WEIGHT and multiply anyway, and 0.0 * NaN is NaN, so one gap made
    // the whole fit NaN.
    //
    // In the normal bucketed case every group already shares one axis, and then
    // the union IS that axis and not a single NaN is introduced.
    //
    // Takes the groups by rvalue and MOVES each value column onto the axis
    // where it can (the shared-axis fast path), matching what the pre-fan-out
    // code did with its single series -- a leg is bounded only by
    // http.max_total_points, so copying every column would be a real cost.
    //
    // `maxFanOutPoints` bounds groups x axis length and is checked BEFORE the
    // dense matrix is materialised -- both factors are known once the axis is
    // built, so the over-wide case is refused rather than allocated and then
    // regretted.  0 disables the bound (tests that deliberately build a wide
    // matrix); see DerivedQueryConfig::maxFanOutPoints for why it exists.
    static AlignedSubQueryGroups alignSubQueryGroups(MultiSeriesSubQueryResult&& multi, size_t maxFanOutPoints);

    // Validate request before execution
    void validateRequest(const DerivedQueryRequest& request);

    // Execute anomaly detection on query results
    seastar::future<anomaly::AnomalyQueryResult> executeAnomalyDetection(const DerivedQueryRequest& request,
                                                                         const ExpressionNode& anomalyNode);

    // Execute forecast on query results
    seastar::future<forecast::ForecastQueryResult> executeForecast(const DerivedQueryRequest& request,
                                                                   const ExpressionNode& forecastNode);
};

// JSON structures for derived query API (using Glaze)
struct GlazeDerivedQueryRequest {
    std::map<std::string, std::string> queries;  // name -> query string
    std::string formula;
    uint64_t startTime = 0;
    uint64_t endTime = 0;
    // Accepts both JSON spellings, exactly like POST /query: a JSON number is
    // nanoseconds, a JSON string goes through HttpQueryHandler::parseInterval
    // ("5m", "1h", or a bare numeric string which also means nanoseconds).
    //
    // THE ONE QUALIFICATION on this campaign's byte-identity claims, which are
    // otherwise about the RESPONSE (see DerivedQueryResult::series and the
    // Glaze structs below): the REQUEST surface did widen here.  The
    // pre-campaign field was a bare std::string, so a NUMERIC
    // aggregationInterval was a 400 Glaze parse error ("expected_quote") even
    // though the API has always documented the type as string/uint64.  Only
    // bodies that were previously REJECTED changed meaning -- every body that
    // used to be accepted parses to the same interval -- so no working client
    // can break on it.  Deliberate, and the one place where "identical to
    // before" needs an asterisk.
    std::optional<std::variant<uint64_t, std::string>> aggregationInterval;
    // Opt in to per-group evaluation of an arithmetic formula; see
    // DerivedQueryRequest::multiSeries.  This struct is parsed STRICTLY (an
    // unknown key is a 400), so the flag has to exist here AND in the
    // glz::meta below or it is unreachable from JSON.
    bool multiSeries = false;
};

// One series of a multi-series derived response.  `values` is a plain
// vector<double> rather than the vector<optional<double>> the forecast and
// anomaly pieces use, so that a group renders EXACTLY as the flat
// timestamps/values pair a single-series answer carries -- the two forms are
// alternatives, never both, but a client moving between them must not have to
// re-learn how a value is spelled.
struct GlazeDerivedSeries {
    std::vector<std::string> group_tags;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
};

struct GlazeDerivedQueryResponse {
    std::string status;
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    std::string formula;
    // Per-group results.  std::optional and therefore ADDITIVE: Glaze skips
    // null members, so every response that predates the multiSeries flag --
    // which is every response that does not set it -- is byte-identical to
    // before, rather than growing an empty `"series":[]`.  An opted-in request
    // that matched nothing DOES emit `[]`: absent means "this server has no such
    // flag", empty means "no groups".
    std::optional<std::vector<GlazeDerivedSeries>> series;

    struct Statistics {
        size_t pointCount = 0;
        double executionTimeMs = 0.0;
        size_t subQueriesExecuted = 0;
        // SUM across groups on the multi-series path -- each group is aligned on
        // its own, so this is a total and not a per-group figure.
        size_t pointsDroppedDueToAlignment = 0;
        // Groups the formula was evaluated over -- what `series_count` is to a
        // forecast response.  std::optional and therefore ADDITIVE: emitted only
        // when the request opted in, so a response that predates the flag is
        // byte-identical to before.
        std::optional<size_t> groupCount;
    } statistics;

    struct Error {
        std::string code;
        std::string message;
    } error;
};

// Glaze-serializable struct for anomaly response
struct GlazeAnomalySeriesPiece {
    std::string piece;
    std::vector<std::string> group_tags;
    std::vector<std::optional<double>> values;  // NaN/Inf converted to nullopt (JSON null)
    std::optional<double> alert_value;
};

struct GlazeAnomalyStatistics {
    std::string algorithm;
    double bounds = 0.0;
    std::string seasonality;
    size_t anomaly_count = 0;
    size_t total_points = 0;
    double execution_time_ms = 0.0;
    // Groups resolved but declined for want of observations.  std::optional
    // and therefore ADDITIVE: Glaze skips null members, so a response with
    // nothing declined -- every response that existed before this field -- is
    // byte-identical to before.  That also matches what the protobuf encoding
    // does on its own, where a 0 uint64 is not put on the wire, so the two
    // transports agree on "absent means none".
    std::optional<size_t> declined_series_count;
};

struct GlazeAnomalyResponse {
    std::string status;
    std::vector<uint64_t> times;
    std::vector<GlazeAnomalySeriesPiece> series;
    GlazeAnomalyStatistics statistics;
    struct Error {
        std::string message;
    } error;
};

// Glaze-serializable struct for forecast response
struct GlazeForecastSeriesPiece {
    std::string piece;
    std::vector<std::string> group_tags;
    std::vector<std::optional<double>> values;
};

struct GlazeForecastStatistics {
    std::string algorithm;
    double deviations = 0.0;
    std::string seasonality;
    double slope = 0.0;
    double intercept = 0.0;
    double r_squared = 0.0;
    double residual_std_dev = 0.0;
    size_t historical_points = 0;
    size_t forecast_points = 0;
    // Groups actually EMITTED under `series`, not the number the leg resolved
    // to; see ForecastStatistics::seriesCount.
    size_t series_count = 0;
    double execution_time_ms = 0.0;
    // Groups resolved but declined for want of observations.  std::optional
    // and therefore ADDITIVE: Glaze skips null members, so a response with
    // nothing declined -- every response that existed before this field -- is
    // byte-identical to before.  That also matches what the protobuf encoding
    // does on its own, where a 0 uint64 is not put on the wire, so the two
    // transports agree on "absent means none".
    std::optional<size_t> declined_series_count;
};

struct GlazeForecastResponse {
    std::string status;
    std::vector<uint64_t> times;
    size_t forecast_start_index = 0;
    std::vector<GlazeForecastSeriesPiece> series;
    GlazeForecastStatistics statistics;
    struct Error {
        std::string message;
    } error;
};

}  // namespace timestar
