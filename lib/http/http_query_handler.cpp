#include "http_query_handler.hpp"

#include "aggregator.hpp"
#include "block_aggregator.hpp"
#include "content_negotiation.hpp"
#include "engine.hpp"
#include "group_key.hpp"
#include "http_auth.hpp"
#include "http_routes.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "proto_converters.hpp"
#include "query_parser.hpp"
#include "query_runner.hpp"
#include "response_formatter.hpp"
#include "series_key.hpp"
#include "series_matcher.hpp"
#include "yield_policy.hpp"

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <limits>
#include <numeric>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

// Glaze-compatible structures for JSON parsing
struct GlazeQueryRequest {
    std::string query;
    std::variant<uint64_t, std::string> startTime;
    std::variant<uint64_t, std::string> endTime;
    std::optional<std::variant<uint64_t, std::string>> aggregationInterval;
    // "epoch" (default) or "start" — see QueryRequest::bucketAnchor.
    std::optional<std::string> bucketAlignment;
    // Treat booleans as numeric 1.0/0.0 — see QueryRequest::booleansAsNumeric.
    std::optional<bool> booleansAsNumeric;
};

template <>
struct glz::meta<GlazeQueryRequest> {
    using T = GlazeQueryRequest;
    static constexpr auto value = object("query", &T::query, "startTime", &T::startTime, "endTime", &T::endTime,
                                         "aggregationInterval", &T::aggregationInterval, "bucketAlignment",
                                         &T::bucketAlignment, "booleansAsNumeric", &T::booleansAsNumeric);
};

namespace timestar::http {

// Struct holding pre-parsed series metadata for per-shard query execution.
// Includes pre-computed SeriesId128 to eliminate redundant hash computations.
// Defined at namespace level so streaming group-by helpers can reference it.
struct SeriesQueryContext {
    std::string seriesKey;
    SeriesId128 seriesId;  // Pre-computed from seriesKey — avoids rehashing in QueryRunner
    std::string field;
    std::map<std::string, std::string> tags;
};

// ---------------------------------------------------------------------------
// Streaming group-by aggregation
// ---------------------------------------------------------------------------
// For group-by queries with streamable methods (avg, sum, min, max, count,
// spread, stddev, stdvar, latest, first), fold TSM blocks one-at-a-time into
// per-group accumulators instead of materializing all raw data.  Memory goes
// from O(points) to O(groups x buckets).

// Returns true when a group-by query can use the streaming path.
// MEDIAN and EXACT_MEDIAN are NOT streamable (both need raw values).
// Returns true when the query can use streaming aggregation (fold per-block
// into accumulators instead of materializing all raw data).
//
// Requires EITHER group-by tags OR an aggregation interval — without both,
// the old path returns raw timestamp/value pairs which internal callers
// (DerivedQueryExecutor, ForecastExecutor) depend on for formula evaluation.
static bool canStreamAggregation(AggregationMethod method, const std::vector<std::string>& groupByTags,
                                 uint64_t aggregationInterval) {
    if (!isStreamableMethod(method))
        return false;
    // Need at least one of: group-by (fold into groups) or bucketing (fold into buckets)
    return !groupByTags.empty() || aggregationInterval > 0;
}

// ---------------------------------------------------------------------------
// Non-numeric field semantics (canonical, see CLAUDE.md "Non-numeric fields in
// queries"): strings and booleans never aggregate numerically.  Without an
// aggregationInterval they pass through raw; with an interval they are reduced
// to LATEST-per-bucket (bucket-start timestamps, matching the numeric bucket
// layout).  The aggregation method named in the query is ignored for them, and
// values are returned in the type they were written in.
// ---------------------------------------------------------------------------

// Reduce one raw non-numeric field (sorted ascending by timestamp) to
// LATEST-per-bucket in place.  Bucket boundaries are epoch-aligned by default
// (anchor == 0: ts / interval * interval, identical to the numeric fold in
// BlockAggregator / foldPoint below); a non-zero anchor shifts the grid to
// start-aligned buckets (QueryRequest::bucketAnchor).
//
// A coroutine with a chunked maybe_yield: this loop runs on the reactor for
// every non-numeric series of a bucketed query, and at production sizes
// (millions of raw bool points per series) the synchronous version WAS the
// reactor stall — the Jul 22 v1.3.0 backtraces symbolize to exactly this
// loop.  Yield cadence per lib/core/yield_policy.hpp.
template <typename V>
static seastar::future<> bucketNonNumericFieldLatest(std::vector<uint64_t>& timestamps, std::vector<V>& values,
                                                     uint64_t interval, uint64_t anchor) {
    if (interval == 0 || timestamps.empty()) {
        co_return;
    }
    // This loop indexes values[i] by a TIMESTAMP index, so a desynced pair would
    // be an out-of-bounds read. It cannot get one: TSM now refuses to emit a
    // block whose value count does not match its timestamp count, failing the
    // query instead (see BlockDecodeError).
    //
    // A clamp used to live here as a second line of defence. It was removed
    // because truncating to min(size) does not make a desynced pair safe -- it
    // MISPAIRS it. With 3001 timestamps and 1 surviving value, clamping emits
    // timestamps[0] paired with a value belonging to point 3000: one
    // confidently-wrong point presented as valid, which is worse than the
    // out-of-bounds read it was preventing and far worse than a failed query.
    if (timestamps.size() != values.size()) {
        throw timestar::BlockDecodeError("non-numeric field has " + std::to_string(timestamps.size()) +
                                         " timestamps but " + std::to_string(values.size()) +
                                         " values; refusing to emit mispaired points");
    }
    const size_t n = timestamps.size();
    std::vector<uint64_t> outTs;
    std::vector<V> outVals;
    size_t sinceYield = 0;
    for (size_t i = 0; i < n; ++i) {
        // anchor == 0 is the epoch grid; a non-zero anchor shifts the grid to
        // start-aligned buckets (QueryRequest::bucketAnchor).  Range filtering
        // guarantees ts >= anchor; clamp defensively regardless.
        const uint64_t rel = timestamps[i] >= anchor ? timestamps[i] - anchor : 0;
        const uint64_t bucket = anchor + (rel / interval) * interval;
        if (!outTs.empty() && outTs.back() == bucket) {
            // Same bucket: ascending input order means later value = latest.
            outVals.back() = std::move(values[i]);
        } else {
            outTs.push_back(bucket);
            outVals.push_back(std::move(values[i]));
        }
        if (++sinceYield >= timestar::kYieldChunkPoints) {
            sinceYield = 0;
            co_await seastar::coroutine::maybe_yield();
        }
    }
    timestamps = std::move(outTs);
    values = std::move(outVals);
}

// Apply LATEST-per-bucket to every non-numeric field of a SeriesResult.
static seastar::future<> bucketNonNumericResultLatest(timestar::SeriesResult& sr, uint64_t interval, uint64_t anchor) {
    for (auto& [fieldName, fieldData] : sr.fields) {
        if (auto* strs = std::get_if<std::vector<std::string>>(&fieldData.second)) {
            co_await bucketNonNumericFieldLatest(fieldData.first, *strs, interval, anchor);
        } else if (auto* bools = std::get_if<std::vector<bool>>(&fieldData.second)) {
            co_await bucketNonNumericFieldLatest(fieldData.first, *bools, interval, anchor);
        }
    }
}

// Bounded recovery for a series whose single-shot read exhausted memory.
//
// A non-numeric field queried WITH an aggregationInterval reduces to
// LATEST-per-bucket, so its RESULT is O(buckets) -- but assembly asked
// engine.query() for the whole range first, making peak memory O(points in
// range).  A `latest:` over a multi-million-point string series therefore threw
// std::bad_alloc while the answer it was building was a handful of values: the
// output was bounded and only the assembly was not.
//
// Re-read the range in bucket-aligned chunks, reducing each chunk to
// LATEST-per-bucket before the next is read, so peak memory is O(points in one
// chunk) instead of O(points in range).  Chunk boundaries are multiples of the
// interval, so a bucket never spans two chunks and the per-chunk reductions
// compose by plain concatenation -- ascending chunks yield ascending, distinct
// bucket starts.  A chunk that still cannot be allocated halves the width and
// retries, down to a floor of one bucket.
//
// This runs ONLY after a failed single-shot attempt, so a query that fits today
// keeps its exact current plan and cost.
//
// Returns nullopt when the series turns out to be NUMERIC: those fold through
// AggregationState/BlockAggregator rather than this reduction, so the caller
// records the drop exactly as before.  A returned SeriesResult with no fields
// means "recovered, but the range holds no data" -- not a failure.
namespace detail {

seastar::future<std::optional<timestar::SeriesResult>> queryNonNumericBucketedChunked(
    Engine& engine, std::string seriesKey, SeriesId128 seriesId, std::string field,
    std::map<std::string, std::string> tags, std::string measurement, uint64_t startTime, uint64_t endTime,
    uint64_t interval, uint64_t initialChunkWidth, uint64_t bucketAnchor) {
    if (interval == 0 || startTime > endTime) {
        co_return std::nullopt;
    }

    // Chunk width is in TIME and subdivides freely -- it is NOT floored at one
    // bucket.  A single bucket can itself hold millions of points (a 1h bucket
    // over 1ms data holds 3.6M), so a one-bucket floor still cannot be
    // materialised on a small shard, and whether the query survived came down to
    // allocator luck rather than the query.
    //
    // Splitting inside a bucket is safe because the per-chunk reductions are
    // merged by BUCKET with later-wins below, not concatenated: chunks ascend, so
    // a later sub-chunk carrying the same bucket simply replaces that bucket's
    // value, which is exactly LATEST-per-bucket.
    //
    // Start at 1/16 of the range and halve on failure. The initial split is only
    // a guess -- correctness does not depend on it, because a chunk that cannot
    // be allocated is retried narrower.
    uint64_t chunkWidth =
        initialChunkWidth > 0 ? initialChunkWidth : std::max<uint64_t>(1, (endTime - startTime) / 16 + 1);

    std::vector<uint64_t> outTs;
    std::vector<std::string> outStrs;
    std::vector<bool> outBools;
    bool sawString = false;
    bool sawBool = false;

    // Append one already-bucketed (timestamp, value) pair, merging into the last
    // bucket when a chunk boundary split it.  Ascending chunks => later wins.
    auto appendBucket = [&](uint64_t bucket, auto&& value) {
        using V = std::decay_t<decltype(value)>;
        if (!outTs.empty() && outTs.back() == bucket) {
            if constexpr (std::is_same_v<V, std::string>) {
                outStrs.back() = std::forward<decltype(value)>(value);
            } else {
                outBools.back() = value;
            }
            return;
        }
        outTs.push_back(bucket);
        if constexpr (std::is_same_v<V, std::string>) {
            outStrs.push_back(std::forward<decltype(value)>(value));
        } else {
            outBools.push_back(value);
        }
    };

    uint64_t cur = startTime;
    while (cur <= endTime) {
        // chunkWidth >= 1 guarantees forward progress.
        const uint64_t remaining = endTime - cur;
        const uint64_t chunkEnd = (chunkWidth - 1 >= remaining) ? endTime : cur + chunkWidth - 1;

        std::optional<VariantQueryResult> chunk;
        try {
            chunk = co_await engine.query(seriesKey, seriesId, cur, chunkEnd);
        } catch (const std::bad_alloc&) {
            if (chunkWidth > 1) {
                chunkWidth /= 2;
                continue;  // retry the same `cur` with a narrower chunk
            }
            throw;  // a single time unit will not fit; the caller reports the drop
        }

        if (!chunk.has_value()) {
            cur = chunkEnd + 1;
            continue;
        }

        // Typed branches instead of std::visit: the reduction is a coroutine
        // now (chunked yields) and co_await is not usable inside a visitor.
        if (auto* strRes = std::get_if<QueryResult<std::string>>(&*chunk)) {
            sawString = true;
            co_await bucketNonNumericFieldLatest(strRes->timestamps, strRes->values, interval, bucketAnchor);
            for (size_t i = 0; i < strRes->timestamps.size(); ++i) {
                appendBucket(strRes->timestamps[i], std::move(strRes->values[i]));
            }
        } else if (auto* boolRes = std::get_if<QueryResult<bool>>(&*chunk)) {
            sawBool = true;
            co_await bucketNonNumericFieldLatest(boolRes->timestamps, boolRes->values, interval, bucketAnchor);
            for (size_t i = 0; i < boolRes->timestamps.size(); ++i) {
                appendBucket(boolRes->timestamps[i], static_cast<bool>(boolRes->values[i]));
            }
        } else {
            co_return std::nullopt;  // numeric series — not this path's job
        }
        cur = chunkEnd + 1;
    }

    timestar::SeriesResult sr;
    sr.measurement = std::move(measurement);
    sr.tags = std::move(tags);
    // A series carries one value type (enforced on write), so exactly one of
    // these is populated; a mixed pair would mean the type binding was violated.
    if (sawString && !sawBool && !outTs.empty()) {
        sr.fields[field] = std::make_pair(std::move(outTs), FieldValues(std::move(outStrs)));
    } else if (sawBool && !sawString && !outTs.empty()) {
        sr.fields[field] = std::make_pair(std::move(outTs), FieldValues(std::move(outBools)));
    }
    co_return sr;
}

// Bucketed LATEST for a BOOLEAN series without materialisation: rides the
// numeric bucketed-LATEST pushdown (reverse block scan with filledBuckets
// early termination, sparse-stat single-point resolution — see
// QueryRunner::queryTsmAggregated boolLatestAsNumeric) by folding true/false
// as 1.0/0.0, then converts the selected per-bucket values back to bool.
// LATEST only SELECTS a stored value, it never computes, so the round-trip is
// exact and the response type is unchanged.
//
// This is what keeps a bool status series from materialising millions of raw
// points to answer a handful of buckets: the Jul 22 production incident shape
// (multi-second bucketed queries over bool-heavy measurements, reactor stalls
// in the reduction loop).
//
// Returns nullopt when the fast path is not applicable (LWW overlap between
// files or with memory data, string series, unknown series); the caller then
// takes the bounded chunked read.  A returned SeriesResult with no fields
// means "resolved: the range holds no data".
seastar::future<std::optional<timestar::SeriesResult>> queryBoolLatestBucketed(
    Engine& engine, const std::string& seriesKey, SeriesId128 seriesId, const std::string& field,
    std::map<std::string, std::string> tags, std::string measurement, uint64_t startTime, uint64_t endTime,
    uint64_t interval) {
    auto pr = co_await engine.queryAggregated(seriesKey, seriesId, startTime, endTime, interval,
                                              timestar::AggregationMethod::LATEST,
                                              /*foldNoInterval=*/false, /*boolLatestAsNumeric=*/true);
    if (!pr.has_value()) {
        co_return std::nullopt;
    }

    std::vector<std::pair<uint64_t, bool>> buckets;
    buckets.reserve(pr->bucketStates.size());
    size_t sinceYield = 0;
    for (auto& [bucketTs, state] : pr->bucketStates) {
        if (state.count == 0) {
            continue;  // empty buckets are omitted (no gap filling)
        }
        buckets.emplace_back(bucketTs, state.latest != 0.0);
        if (++sinceYield >= timestar::kYieldChunkPoints) {
            sinceYield = 0;
            co_await seastar::coroutine::maybe_yield();
        }
    }
    std::sort(buckets.begin(), buckets.end());

    timestar::SeriesResult sr;
    sr.measurement = std::move(measurement);
    sr.tags = std::move(tags);
    if (!buckets.empty()) {
        std::vector<uint64_t> ts;
        std::vector<bool> vals;
        ts.reserve(buckets.size());
        vals.reserve(buckets.size());
        for (auto& [bucket, val] : buckets) {
            ts.push_back(bucket);
            vals.push_back(val);
        }
        sr.fields[field] = std::make_pair(std::move(ts), FieldValues(std::move(vals)));
    }
    co_return sr;
}

}  // namespace detail

// Streaming group-by coroutine.  Iterates each series context, folds the
// per-series PushdownResult into a per-group accumulator, and returns
// PartialAggregationResults in the same format the rest of the pipeline
// expects.  String and boolean series cannot fold numerically; they are
// appended to `nonNumericResults` raw (the caller applies the interval
// reduction).
//
// Uses direct AggregationState accumulators (not BlockAggregator) so that
// PushdownResult states can be merged in without converting back to raw
// points.
//
// Key invariant: Seastar is single-threaded per shard, so the group
// accumulators are never accessed concurrently.  After each co_await the
// accumulators are safe to mutate.
static seastar::future<std::vector<PartialAggregationResult>> streamingGroupByAggregation(
    Engine& engine, std::vector<SeriesQueryContext>& contexts, const std::string& measurement, uint64_t startTime,
    uint64_t endTime, AggregationMethod aggregation, uint64_t aggregationInterval,
    const std::vector<std::string>& groupByTags, std::vector<timestar::SeriesResult>& nonNumericResults,
    size_t& droppedSeriesOut, std::string& firstDropReasonOut) {
    // ---- Phase 1: Pre-group series by groupKey ----
    struct GroupAccumulator {
        // One AggregationState per bucket; with interval == 0 the "bucket" is
        // the point's own timestamp, so every distinct timestamp survives.
        std::unordered_map<uint64_t, AggregationState> bucketStates;
        // Metadata
        std::string groupKey;
        size_t groupKeyHash = 0;
        std::map<std::string, std::string> cachedTags;
        std::string fieldName;
        size_t totalPoints = 0;
    };

    // Use PrehashedString for O(1) lookups
    std::unordered_map<PrehashedString, std::unique_ptr<GroupAccumulator>, PrehashedStringHash, PrehashedStringEqual>
        groupMap;
    groupMap.reserve(contexts.size());  // upper-bound

    // Map each context to its group accumulator
    std::vector<GroupAccumulator*> contextGroupPtrs;
    contextGroupPtrs.reserve(contexts.size());

    // Shard-wide ceiling on speculative bucket-map preallocation across ALL
    // groups in this query. 1M slots is ~8 MB of hash-table storage, enough that
    // a handful of wide-range groups still get their full reserve while a
    // thousand-group query cannot allocate hundreds of MB of empty tables.
    constexpr uint64_t kMaxTotalPreallocatedBuckets = 1'000'000;
    uint64_t totalPreallocatedBuckets = 0;

    for (auto& ctx : contexts) {
        auto gkr = buildGroupKeyDirect(measurement, ctx.field, ctx.tags, groupByTags);
        PrehashedString pkey(gkr.key, gkr.hash);

        auto [it, inserted] = groupMap.try_emplace(pkey, nullptr);
        if (inserted) {
            it->second = std::make_unique<GroupAccumulator>();
            it->second->groupKey = std::move(gkr.key);
            it->second->groupKeyHash = gkr.hash;
            it->second->cachedTags = std::move(gkr.tags);
            it->second->fieldName = ctx.field;
            // Pre-reserve bucket map for bucketed queries.
            //
            // The bucket count is a property of the query RANGE, not of how much
            // data this group actually holds, and this reserve is PER GROUP. A
            // day-long range at a 1s interval is 86,400 buckets = ~691 KB of
            // hash-table slots; with a `by {host}` clause over 1,000 hosts that
            // is ~691 MB of empty tables allocated before a single point is
            // folded, and most groups never come close to filling them.
            //
            // Cap the total across groups rather than reserving the full range
            // for each. Groups beyond the budget still work -- unordered_map
            // grows on demand -- they just pay incremental rehashing, which is
            // the right trade when there are enough groups for it to matter.
            if (aggregationInterval > 0 && endTime > startTime &&
                totalPreallocatedBuckets < kMaxTotalPreallocatedBuckets) {
                uint64_t range = endTime - startTime;
                uint64_t bucketCount = (range + aggregationInterval - 1) / aggregationInterval;
                if (bucketCount > BlockAggregator::MAX_PREALLOCATED_BUCKETS) {
                    bucketCount = BlockAggregator::MAX_PREALLOCATED_BUCKETS;
                }
                bucketCount = std::min<uint64_t>(bucketCount, kMaxTotalPreallocatedBuckets - totalPreallocatedBuckets);
                if (bucketCount > 0) {
                    it->second->bucketStates.reserve(static_cast<size_t>(bucketCount));
                    totalPreallocatedBuckets += bucketCount;
                }
            }
        }
        contextGroupPtrs.push_back(it->second.get());
    }

    // ---- Phase 2: Prefetch TSM indices ----
    {
        std::vector<SeriesId128> prefetchIds;
        prefetchIds.reserve(contexts.size());
        for (const auto& ctx : contexts) {
            prefetchIds.push_back(ctx.seriesId);
        }
        co_await engine.prefetchSeriesIndices(prefetchIds);
    }

    // ---- Phase 3: Helper lambdas for folding values into groups ----

    // Fold a single (timestamp, value) pair into a group accumulator.
    //
    // NO method collapses a time range here: without an aggregationInterval
    // every distinct timestamp survives, for LATEST/FIRST exactly as for
    // avg/min/max (CLAUDE.md "Aggregation Result Shape").  Grouping decides
    // only which series fold together at equal timestamps.
    auto foldPoint = [&](GroupAccumulator& group, uint64_t ts, double val) {
        // interval == 0 buckets per distinct timestamp.
        const uint64_t bucket = aggregationInterval > 0 ? (ts / aggregationInterval) * aggregationInterval : ts;
        group.bucketStates[bucket].addValueForMethod(val, ts, aggregation);
    };

    // Merge a PushdownResult into a group accumulator.
    auto mergePushdownIntoGroup = [&](GroupAccumulator& group, PushdownResult& pr) {
        group.totalPoints += pr.totalPoints;

        if (aggregationInterval > 0 && !pr.bucketStates.empty()) {
            // Bucketed: merge each bucket state
            for (auto& [bucketTs, state] : pr.bucketStates) {
                group.bucketStates[bucketTs].mergeForMethod(state, aggregation);
            }
        } else if (!pr.sortedTimestamps.empty()) {
            // Raw values from pushdown: fold each value
            for (size_t i = 0; i < pr.sortedTimestamps.size(); ++i) {
                foldPoint(group, pr.sortedTimestamps[i], pr.sortedValues[i]);
            }
        }
    };

    // ---- Phase 4: Iterate series with bounded concurrency ----
    // Build (context*, group*) pairs for index-free iteration.
    struct ContextGroupPair {
        SeriesQueryContext* ctx;
        GroupAccumulator* group;
    };
    std::vector<ContextGroupPair> pairs;
    pairs.reserve(contexts.size());
    for (size_t i = 0; i < contexts.size(); ++i) {
        pairs.push_back({&contexts[i], contextGroupPtrs[i]});
    }

    static constexpr size_t MAX_CONCURRENT_SERIES_QUERIES = 64;

    co_await seastar::max_concurrent_for_each(
        pairs, MAX_CONCURRENT_SERIES_QUERIES, [&](ContextGroupPair& pair) -> seastar::future<> {
            auto& ctx = *pair.ctx;
            auto* group = pair.group;

            // --- Non-numeric bounded path (interval > 0) ---
            // Same routing as the standard path: bool/string series reduce to
            // LATEST-per-bucket, so answer them from the bucketed-LATEST
            // pushdown (bool) or the bounded chunked reader instead of
            // materialising the full range below.  This path only runs with
            // bucketAnchor == 0 and booleansAsNumeric == false (see the
            // executeShardQuery dispatch), so no flag checks are needed.
            if (aggregationInterval > 0) {
                const auto localType = engine.localSeriesValueType(ctx.seriesId);
                if (localType.has_value() && isNonNumericValueType(*localType)) {
                    bool routedFailed = false;
                    std::string routedReason;
                    try {
                        std::optional<timestar::SeriesResult> reduced;
                        if (*localType == TSMValueType::Boolean) {
                            reduced = co_await detail::queryBoolLatestBucketed(engine, ctx.seriesKey, ctx.seriesId,
                                                                               ctx.field, ctx.tags, measurement,
                                                                               startTime, endTime, aggregationInterval);
                        }
                        if (!reduced.has_value()) {
                            const uint64_t range = endTime - startTime;
                            const uint64_t fullWidth =
                                (range == std::numeric_limits<uint64_t>::max()) ? range : range + 1;
                            reduced = co_await detail::queryNonNumericBucketedChunked(
                                engine, ctx.seriesKey, ctx.seriesId, ctx.field, ctx.tags, measurement, startTime,
                                endTime, aggregationInterval, fullWidth);
                        }
                        if (reduced.has_value()) {
                            if (!reduced->fields.empty()) {
                                nonNumericResults.push_back(std::move(*reduced));
                            }
                            co_return;
                        }
                        // Series turned out numeric — fall through to the
                        // standard flow below, which handles every type.
                    } catch (const std::exception& e) {
                        routedFailed = true;
                        routedReason = e.what();
                    }
                    if (routedFailed) {
                        timestar::http_log.error(
                            "[QUERY] Dropping series '{}' from result: {}. The response will be INCOMPLETE.",
                            ctx.seriesKey, routedReason);
                        ++droppedSeriesOut;
                        if (firstDropReasonOut.empty()) {
                            firstDropReasonOut = routedReason;
                        }
                        co_return;
                    }
                }
            }

            // --- Try pushdown path ---
            // foldNoInterval=false: with interval == 0 the group's time axis is
            // preserved, so the pushdown must hand back raw sorted vectors to be
            // folded per timestamp.  LATEST/FIRST collapse inside the runner
            // regardless of this flag, which is exactly collapseRange's shape.
            auto pushdownResult = co_await engine.queryAggregated(ctx.seriesKey, ctx.seriesId, startTime, endTime,
                                                                  aggregationInterval, aggregation,
                                                                  /*foldNoInterval=*/false);

            if (pushdownResult.has_value()) {
                mergePushdownIntoGroup(*group, *pushdownResult);
                co_return;
            }

            // --- Fallback path ---
            // Pushdown not applicable (non-float, memory data, overlap).
            // Query the full data for this single series and fold.
            // Memory store data is bounded in size so materializing one
            // series at a time is fine.
            std::optional<VariantQueryResult> optResult;
            // co_await is not permitted inside a catch handler, so the failure is
            // recorded here and handled after the try/catch.
            bool readFailed = false;
            std::string failReason;
            try {
                optResult = co_await engine.query(ctx.seriesKey, ctx.seriesId, startTime, endTime);
            } catch (const SeriesNotFoundException&) {
                // Expected: the series genuinely has no data here.
                co_return;
            } catch (const std::exception& e) {
                readFailed = true;
                failReason = e.what();
            }

            if (readFailed) {
                // Before giving up: a non-numeric field with an interval has a
                // bounded RESULT even when its raw range does not fit, so retry
                // it in bucket-aligned chunks rather than dropping it.
                if (aggregationInterval > 0) {
                    bool recoveredOk = false;
                    try {
                        auto recovered = co_await detail::queryNonNumericBucketedChunked(
                            engine, ctx.seriesKey, ctx.seriesId, ctx.field, ctx.tags, measurement, startTime, endTime,
                            aggregationInterval);
                        if (recovered.has_value()) {
                            if (!recovered->fields.empty()) {
                                nonNumericResults.push_back(std::move(*recovered));
                            }
                            recoveredOk = true;
                        }
                    } catch (const std::exception&) {
                        // Still not satisfiable -- fall through and report it.
                    }
                    if (recoveredOk) {
                        co_return;
                    }
                }
                // NOT expected. Dropping a series here removes real data from an
                // otherwise-successful response, so this must never be silent:
                // it was previously logged at debug level and therefore invisible
                // in production, which made a query that returned an empty result
                // indistinguishable from one that legitimately found nothing.
                timestar::http_log.error(
                    "[QUERY] Dropping series '{}' from result: {}. The response will be INCOMPLETE.", ctx.seriesKey,
                    failReason);
                ++droppedSeriesOut;
                if (firstDropReasonOut.empty()) {
                    firstDropReasonOut = failReason;
                }
                co_return;
            }

            if (!optResult.has_value()) {
                co_return;
            }

            std::visit(
                [&](auto&& result) {
                    using T = std::decay_t<decltype(result)>;
                    if constexpr (std::is_same_v<T, QueryResult<double>>) {
                        group->totalPoints += result.timestamps.size();
                        // Batch fold through BlockAggregator: sorted-run bucket
                        // batching + SIMD reductions instead of per-point
                        // division + hash lookup + method switch. Only streamable
                        // methods reach this function (gated by the caller), so
                        // BlockAggregator's fold semantics match foldPoint's.
                        if (aggregationInterval > 0) {
                            BlockAggregator agg(aggregationInterval, startTime, endTime, aggregation, true);
                            agg.addPoints(result.timestamps, result.values);
                            for (auto& [bucketTs, state] : agg.takeBucketStates()) {
                                group->bucketStates[bucketTs].mergeForMethod(std::move(state), aggregation);
                            }
                        } else {
                            // interval == 0, method keeps the time axis: one state
                            // per distinct timestamp.  BlockAggregator's bucketed
                            // fold needs interval > 0, so fold directly.
                            for (size_t i = 0; i < result.timestamps.size(); ++i) {
                                foldPoint(*group, result.timestamps[i], result.values[i]);
                            }
                        }
                    } else if constexpr (std::is_same_v<T, QueryResult<int64_t>>) {
                        group->totalPoints += result.timestamps.size();
                        for (size_t i = 0; i < result.timestamps.size(); ++i) {
                            foldPoint(*group, result.timestamps[i], static_cast<double>(result.values[i]));
                        }
                    } else if constexpr (std::is_same_v<T, QueryResult<bool>> ||
                                         std::is_same_v<T, QueryResult<std::string>>) {
                        // Strings and booleans can't aggregate numerically —
                        // pass them through per-series in their written type
                        // (they bypass grouping, like the standard path).  The
                        // caller reduces them to LATEST-per-bucket when an
                        // interval is set.
                        if (!result.timestamps.empty()) {
                            timestar::SeriesResult sr;
                            sr.measurement = measurement;
                            sr.tags = ctx.tags;
                            sr.fields[ctx.field] =
                                std::make_pair(std::move(result.timestamps), FieldValues(std::move(result.values)));
                            nonNumericResults.push_back(std::move(sr));
                        }
                    }
                },
                *optResult);
        });

    // ---- Phase 5: Convert group accumulators to PartialAggregationResults ----
    std::vector<PartialAggregationResult> results;
    results.reserve(groupMap.size());

    for (auto& [pkey, groupPtr] : groupMap) {
        PartialAggregationResult partial;
        partial.measurement = measurement;
        partial.fieldName = groupPtr->fieldName;
        partial.groupKey = groupPtr->groupKey;
        partial.groupKeyHash = groupPtr->groupKeyHash;
        partial.cachedTags = std::move(groupPtr->cachedTags);
        partial.totalPoints = groupPtr->totalPoints;

        if (groupPtr->bucketStates.empty())
            continue;  // No data for this group
        partial.bucketStates = std::move(groupPtr->bucketStates);

        results.push_back(std::move(partial));
    }

    co_return results;
}

// Finalize partial aggregation results from a single shard directly into the query response,
// bypassing the mergePartialAggregationsGrouped() → GroupedAggregationResult pipeline.
// Precondition: each partial must have a unique groupKey (i.e. no two partials share the
// same measurement+field+tags combination). When multiple series produce partials with the
// same groupKey (e.g. non-group-by queries with multiple matching series), callers must use
// the merge fallback path instead.
seastar::future<> HttpQueryHandler::finalizeSingleShardPartials(std::vector<PartialAggregationResult>& partials,
                                                                AggregationMethod method, QueryResponse& response) {
    response.series.reserve(partials.size());

    // Yield cadence: see lib/core/yield_policy.hpp (reactor-stall prevention;
    // chunked, never per-point).
    size_t pointsSinceYield = 0;

    for (auto& partial : partials) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;

        if (!partial.bucketStates.empty()) {
            // Bucketed: extract values directly into a sortable structure.
            // Use vector<pair<ts,value>> — avoids the indirection through state pointers
            // and getValue() is called during extraction, not during the sort.
            size_t n = partial.bucketStates.size();
            std::vector<std::pair<uint64_t, double>> tvPairs;
            tvPairs.reserve(n);
            for (auto& [ts, state] : partial.bucketStates) {
                tvPairs.emplace_back(ts, state.getValue(method));
                if (++pointsSinceYield >= kYieldChunkPoints) {
                    pointsSinceYield = 0;
                    co_await seastar::coroutine::maybe_yield();
                }
            }
            std::sort(tvPairs.begin(), tvPairs.end());
            timestamps.reserve(n);
            values.reserve(n);
            for (auto& [ts, val] : tvPairs) {
                timestamps.push_back(ts);
                values.push_back(val);
                if (++pointsSinceYield >= kYieldChunkPoints) {
                    pointsSinceYield = 0;
                    co_await seastar::coroutine::maybe_yield();
                }
            }
        } else if (partial.collapsedState.has_value()) {
            // Non-bucketed streaming pushdown — single collapsed AggregationState
            auto& state = *partial.collapsedState;
            if (state.count > 0) {
                timestamps.push_back(state.getTimestamp(method));
                values.push_back(state.getValue(method));
            }
        } else if (!partial.sortedValues.empty()) {
            // Non-bucketed raw values from pushdown.  Each timestamp holds ONE
            // value here, so the per-timestamp aggregate is the fold of a
            // single-element set — which is the value itself only for methods
            // where fold-of-one is the identity (methodCanFoldRaw).  COUNT is 1
            // per point; SPREAD/STDDEV/STDVAR are 0 over one value, and passing
            // the raw value through instead made this path disagree with the
            // grouped and multi-shard paths, which fold properly.
            timestamps = std::move(partial.sortedTimestamps);
            if (method == AggregationMethod::COUNT) {
                // COUNT counts only non-NaN values (docs/nan_policy.md): a lone
                // NaN point is an empty per-timestamp set → NaN, matching
                // getValue() on a count-0 state.
                values.reserve(timestamps.size());
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    values.push_back(std::isnan(partial.sortedValues[i]) ? std::numeric_limits<double>::quiet_NaN()
                                                                         : 1.0);
                    if (++pointsSinceYield >= kYieldChunkPoints) {
                        pointsSinceYield = 0;
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
            } else if (methodCanFoldRaw(method)) {
                values = std::move(partial.sortedValues);
            } else {
                values.reserve(timestamps.size());
                for (size_t i = 0; i < timestamps.size(); ++i) {
                    AggregationState s;
                    s.collectRaw = (method == AggregationMethod::MEDIAN || method == AggregationMethod::EXACT_MEDIAN);
                    s.addValue(partial.sortedValues[i], timestamps[i]);
                    values.push_back(s.getValue(method));
                    if (++pointsSinceYield >= kYieldChunkPoints) {
                        pointsSinceYield = 0;
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
            }
        } else if (!partial.sortedStates.empty()) {
            // Non-bucketed states from fallback aggregation
            timestamps.reserve(partial.sortedStates.size());
            values.reserve(partial.sortedStates.size());
            for (size_t i = 0; i < partial.sortedStates.size(); ++i) {
                timestamps.push_back(partial.sortedTimestamps[i]);
                values.push_back(partial.sortedStates[i].getValue(method));
                if (++pointsSinceYield >= kYieldChunkPoints) {
                    pointsSinceYield = 0;
                    co_await seastar::coroutine::maybe_yield();
                }
            }
        } else {
            continue;
        }

        if (timestamps.empty())
            continue;

        // Both pushdown and fallback paths now populate cachedTags via buildGroupKeyDirect()
        auto tags = std::move(partial.cachedTags);

        // Each partial produces its own series entry (one field per series).
        // The API returns each field as a separate series — clients merge if needed.
        SeriesResult series;
        series.measurement = std::move(partial.measurement);
        series.tags = std::move(tags);
        series.fields[std::move(partial.fieldName)] =
            std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

        response.series.push_back(std::move(series));

        // Partial boundary: cheap preemption check — covers the zero-copy move
        // branch, whose per-point loops above never run.
        co_await seastar::coroutine::maybe_yield();
    }
}

// Timing structure to track query execution steps
struct QueryTimingInfo {
    std::chrono::high_resolution_clock::time_point startTime;
    std::chrono::high_resolution_clock::time_point endTime;

    // Step timings
    double findSeriesMs = 0.0;
    double shardQueriesMs = 0.0;
    std::vector<std::pair<unsigned, double>> perShardQueryMs;
    double resultCollectionMs = 0.0;  // time to move shard results into local vectors
    double aggregationMs = 0.0;       // time for merge + reduce + response building
    double totalMs = 0.0;

    // Statistics
    size_t seriesFound = 0;
    size_t shardsQueried = 0;
    size_t totalPointsRetrieved = 0;
    size_t finalPointsReturned = 0;

    std::string toString() const {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2);
        ss << "\n==================== Query Timing Breakdown ===================\n";
        ss << "Find Series:          " << std::setw(8) << findSeriesMs << " ms\n";
        ss << "Shard Queries:        " << std::setw(8) << shardQueriesMs << " ms\n";
        if (!perShardQueryMs.empty()) {
            ss << "  Per-shard breakdown:\n";
            for (const auto& [shardId, ms] : perShardQueryMs) {
                ss << "    Shard " << shardId << ":         " << std::setw(8) << ms << " ms\n";
            }
        }
        ss << "Result Collection:    " << std::setw(8) << resultCollectionMs << " ms\n";
        ss << "Aggregation:          " << std::setw(8) << aggregationMs << " ms\n";
        ss << "----------------------------------------------------------------\n";
        ss << "Total Execution:      " << std::setw(8) << totalMs << " ms\n";
        ss << "\n";
        ss << "Series Found:         " << seriesFound << "\n";
        ss << "Shards Queried:       " << shardsQueried << "\n";
        ss << "Points Retrieved:     " << totalPointsRetrieved << "\n";
        ss << "Points Returned:      " << finalPointsReturned << "\n";
        ss << "================================================================\n";
        return ss.str();
    }
};

std::unique_ptr<seastar::http::reply> HttpQueryHandler::validateRequest(const seastar::http::request& req) const {
    auto resFmt = timestar::http::responseFormat(req);

    // Check body size limit
    if (req.content.size() > maxQueryBodySize()) {
        auto rep = std::make_unique<seastar::http::reply>();
        rep->set_status(seastar::http::reply::status_type::payload_too_large);
        auto maxBytes = maxQueryBodySize();
        auto msg = "Request body too large (max " + std::to_string(maxBytes / 1024) + "KB)";
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatQueryError("PAYLOAD_TOO_LARGE", msg);
        } else {
            auto errObj = glz::obj{"status", "error", "message", msg, "error", msg};
            rep->_content = glz::write_json(errObj).value_or("{\"status\":\"error\"}");
        }
        timestar::http::setContentType(*rep, resFmt);
        return rep;
    }

    // Content-Type validation is handled by the content negotiation layer
    // (timestar::http::requestFormat), which parses the header case-insensitively
    // and defaults unknown types to JSON for backward compatibility.

    return nullptr;  // Validation passed
}

seastar::future<std::unique_ptr<seastar::http::reply>> HttpQueryHandler::handleQuery(
    std::unique_ptr<seastar::http::request> req) {
    auto rep = std::make_unique<seastar::http::reply>();
    auto reqFmt = timestar::http::requestFormat(*req);
    auto resFmt = timestar::http::responseFormat(*req);

    try {
        // Validate request body size
        auto validationError = validateRequest(*req);
        if (validationError) {
            co_return validationError;
        }

        // Parse request body
        QueryRequest queryRequest;
        std::string queryStr;  // For logging

        if (timestar::http::isProtobuf(reqFmt)) {
            // Parse protobuf request
            auto parsed = timestar::proto::parseQueryRequest(req->content.data(), req->content.size());
            queryStr = parsed.query;

            // Build a GlazeQueryRequest-compatible struct to reuse parseQueryRequest()
            GlazeQueryRequest glazeRequest;
            glazeRequest.query = std::move(parsed.query);
            glazeRequest.startTime = parsed.startTime;
            glazeRequest.endTime = parsed.endTime;
            if (!parsed.aggregationInterval.empty()) {
                glazeRequest.aggregationInterval = parsed.aggregationInterval;
            }
            if (!parsed.bucketAlignment.empty()) {
                glazeRequest.bucketAlignment = parsed.bucketAlignment;
            }
            if (parsed.booleansAsNumeric) {
                glazeRequest.booleansAsNumeric = true;
            }

            try {
                queryRequest = parseQueryRequest(glazeRequest);
            } catch (const QueryParseException& e) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError("INVALID_QUERY", e.what());
                } else {
                    rep->_content = createErrorResponse("INVALID_QUERY", e.what());
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        } else {
            // Parse JSON request body using Glaze
            GlazeQueryRequest glazeRequest;
            auto parse_error = glz::read_json(glazeRequest, req->content);

            if (parse_error) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError(
                        "INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
                } else {
                    rep->_content = createErrorResponse(
                        "INVALID_JSON", "Failed to parse JSON request: " + std::string(glz::format_error(parse_error)));
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }

            queryStr = glazeRequest.query;

            // Convert to QueryRequest
            try {
                queryRequest = parseQueryRequest(glazeRequest);
            } catch (const QueryParseException& e) {
                rep->set_status(seastar::http::reply::status_type::bad_request);
                if (timestar::http::isProtobuf(resFmt)) {
                    rep->_content = timestar::proto::formatQueryError("INVALID_QUERY", e.what());
                } else {
                    rep->_content = createErrorResponse("INVALID_QUERY", e.what());
                }
                timestar::http::setContentType(*rep, resFmt);
                co_return rep;
            }
        }

        // Execute query
        auto startTime = std::chrono::high_resolution_clock::now();
        QueryResponse response = co_await executeQuery(queryRequest);
        auto endTime = std::chrono::high_resolution_clock::now();

        // Calculate execution time
        response.statistics.executionTimeMs = std::chrono::duration<double, std::milli>(endTime - startTime).count();

        // Format response
        if (response.success) {
            rep->set_status(seastar::http::reply::status_type::ok);
            if (timestar::http::isProtobuf(resFmt)) {
                // Build QueryResponseData from internal QueryResponse
                timestar::proto::QueryResponseData protoResp;
                protoResp.success = true;
                protoResp.statistics.seriesCount = response.statistics.seriesCount;
                protoResp.statistics.pointCount = response.statistics.pointCount;
                protoResp.statistics.failedSeriesCount = response.statistics.failedSeriesCount;
                protoResp.statistics.executionTimeMs = response.statistics.executionTimeMs;
                protoResp.statistics.shardsQueried = response.statistics.shardsQueried;
                protoResp.statistics.truncated = response.statistics.truncated;
                protoResp.statistics.truncationReason = response.statistics.truncationReason;

                for (auto& sr : response.series) {
                    timestar::proto::SeriesResultData srd;
                    srd.measurement = std::move(sr.measurement);
                    srd.tags = std::move(sr.tags);
                    for (auto& [fieldName, fieldData] : sr.fields) {
                        auto& [timestamps, values] = fieldData;
                        timestar::proto::FieldValues protoValues;
                        std::visit([&protoValues](auto& vec) { protoValues = std::move(vec); }, values);
                        srd.fields[std::move(fieldName)] =
                            std::make_pair(std::move(timestamps), std::move(protoValues));
                    }
                    protoResp.series.push_back(std::move(srd));

                    // Series boundary: cheap preemption check — a multi-million
                    // point response must not build in one reactor task.
                    co_await seastar::coroutine::maybe_yield();
                }

                rep->_content = co_await timestar::proto::formatQueryResponseYielding(protoResp);
            } else {
                rep->_content = co_await formatQueryResponse(response);
            }
        } else {
            rep->set_status(seastar::http::reply::status_type::internal_server_error);
            if (timestar::http::isProtobuf(resFmt)) {
                rep->_content = timestar::proto::formatQueryError(response.errorCode, response.errorMessage);
            } else {
                rep->_content = createErrorResponse(response.errorCode, response.errorMessage);
            }
        }

        timestar::http::setContentType(*rep, resFmt);

        // Log query summary after response is ready (controlled by TIMESTAR_LOG_QUERY_PATH)
        LOG_QUERY_PATH(timestar::http_log, info,
                       "[QUERY_SUMMARY] Query: '{}' | StartTime: {} | EndTime: {} | AggregationInterval: {} | "
                       "ExecutionTime: {:.2f}ms",
                       queryStr, queryRequest.startTime, queryRequest.endTime,
                       queryRequest.aggregationInterval ? std::to_string(queryRequest.aggregationInterval) : "none",
                       response.statistics.executionTimeMs);

    } catch (const std::exception& e) {
        if (engineSharded) {
            ++engineSharded->local().metrics().query_errors_total;
        }
        timestar::http_log.error("[QUERY] Error handling query request: {}", e.what());
        rep->set_status(seastar::http::reply::status_type::internal_server_error);
        if (timestar::http::isProtobuf(resFmt)) {
            rep->_content = timestar::proto::formatQueryError("INTERNAL_ERROR", "Internal query error");
        } else {
            rep->_content = createErrorResponse("INTERNAL_ERROR", "Internal query error");
        }
        timestar::http::setContentType(*rep, resFmt);
    }

    co_return rep;
}

void HttpQueryHandler::registerRoutes(seastar::httpd::routes& r, std::string_view authToken) {
    // addJsonRoute applies timestar::http::wrapWithAuth per route.
    timestar::http::addJsonRoute(
        r, seastar::httpd::operation_type::POST, "/query", authToken,
        [this](std::unique_ptr<seastar::http::request> req, std::unique_ptr<seastar::http::reply>)
            -> seastar::future<std::unique_ptr<seastar::http::reply>> { return handleQuery(std::move(req)); });

    timestar::http_log.info("Registered HTTP query endpoint at /query{}", authToken.empty() ? "" : " (auth required)");
}

QueryRequest HttpQueryHandler::parseQueryRequest(const GlazeQueryRequest& glazeReq) {
    // Parse the query string
    std::string queryStr = glazeReq.query;

    // Handle both numeric and string timestamps for backward compatibility
    uint64_t startTime, endTime;

    // Parse startTime - can be number or string
    if (std::holds_alternative<uint64_t>(glazeReq.startTime)) {
        startTime = std::get<uint64_t>(glazeReq.startTime);
    } else {
        std::string timeStr = std::get<std::string>(glazeReq.startTime);
        try {
            if (!timeStr.empty() && timeStr[0] == '-')
                throw std::invalid_argument("Negative timestamps are not supported");
            startTime = std::stoull(timeStr);
        } catch (const std::invalid_argument&) {
            // Fall back to date string parsing (legacy support)
            startTime = QueryParser::parseTime(timeStr);
        }
    }

    // Parse endTime - can be number or string
    if (std::holds_alternative<uint64_t>(glazeReq.endTime)) {
        endTime = std::get<uint64_t>(glazeReq.endTime);
    } else {
        std::string timeStr = std::get<std::string>(glazeReq.endTime);
        try {
            if (!timeStr.empty() && timeStr[0] == '-')
                throw std::invalid_argument("Negative timestamps are not supported");
            endTime = std::stoull(timeStr);
        } catch (const std::invalid_argument&) {
            // Fall back to date string parsing (legacy support)
            endTime = QueryParser::parseTime(timeStr);
        }
    }

    // Validate time range
    if (startTime >= endTime) {
        throw QueryParseException("startTime must be less than endTime");
    }

    // Parse aggregation interval if provided
    uint64_t aggregationInterval = 0;
    if (glazeReq.aggregationInterval) {
        const auto& interval = *glazeReq.aggregationInterval;
        if (std::holds_alternative<uint64_t>(interval)) {
            aggregationInterval = std::get<uint64_t>(interval);
        } else {
            std::string intervalStr = std::get<std::string>(interval);
            // Use the proper parseInterval function that supports decimals and all units
            aggregationInterval = parseInterval(intervalStr);
        }
    }

    LOG_QUERY_PATH(timestar::http_log, debug, "[QUERY] Parsed request - Query: '{}', Start: {}, End: {}, Interval: {}",
                   queryStr, startTime, endTime, aggregationInterval ? std::to_string(aggregationInterval) : "none");

    // Parse the query string to extract components
    QueryRequest request = QueryParser::parseQueryString(queryStr);
    request.startTime = startTime;
    request.endTime = endTime;
    request.aggregationInterval = aggregationInterval;

    // Bucket alignment: "epoch" (default) keeps the canonical epoch grid;
    // "start" anchors the grid at startTime (rollup.js-compatible reads).
    // Anchoring is meaningful only with an interval — without one there are
    // no buckets, so the anchor stays 0 and the request is a plain raw read.
    if (glazeReq.bucketAlignment.has_value() && !glazeReq.bucketAlignment->empty()) {
        const std::string& alignment = *glazeReq.bucketAlignment;
        if (alignment == "start") {
            if (aggregationInterval > 0) {
                request.bucketAnchor = startTime;
            }
        } else if (alignment != "epoch") {
            throw QueryParseException("Invalid bucketAlignment '" + alignment + "': expected \"epoch\" or \"start\"");
        }
    }
    request.booleansAsNumeric = glazeReq.booleansAsNumeric.value_or(false);

    return request;
}

// ---------------------------------------------------------------------------
// executeQuery phase types
// ---------------------------------------------------------------------------
// executeQuery() is decomposed into phase methods (following the extraction
// pattern used by streamingGroupByAggregation() and finalizeSingleShardPartials()
// above):
//
//   1. discoverSeriesAcrossShards()  — scatter-gather series discovery
//   2. fanOutShardQueries()          — per-shard query execution via
//      executeShardQuery(), which runs on the owning shard
//   3. finalizeSingleShardResponse() / finalizeMultiShardResponse() — partial
//      merge + aggregation finalize into the QueryResponse
//   4. logQueryCompletion()          — timing breakdown + slow-query logging

// Per-shard discovery result
struct PerShardDiscovery {
    std::vector<SeriesQueryContext> contexts;
    bool limitExceeded = false;
    size_t discovered = 0;
    size_t limit = 0;
};

// Combined result of the discovery phase across all shards.
struct SeriesDiscoveryResult {
    std::vector<std::vector<SeriesQueryContext>> seriesByShard;
    bool limitExceeded = false;
    size_t discovered = 0;
    size_t limit = 0;
    bool timedOut = false;  // discovery exceeded defaultQueryTimeout()
};

// Struct to hold shard query results including both aggregatable and non-aggregatable data
struct ShardQueryResult {
    std::vector<PartialAggregationResult> partialResults;
    std::vector<SeriesResult> nonNumericResults;  // String/bool fields bypass aggregation
    double shardMs = 0.0;
    // Series this shard could not read and therefore OMITTED from the result.
    // Must be surfaced: a response missing series is not a successful response,
    // and reporting one as success is indistinguishable from "no data".
    size_t droppedSeries = 0;
    std::string firstDropReason;
};

// Result of the shard query fan-out phase.
struct ShardFanOutResult {
    std::vector<std::pair<unsigned, ShardQueryResult>> shardResults;
    // Aggregated across shards; see ShardQueryResult::droppedSeries.
    bool timedOut = false;  // shard queries exceeded defaultQueryTimeout()
};

// Execute the query for every series context owned by one shard.  Runs on the
// owning shard (dispatched via invoke_on from fanOutShardQueries()).  Parameters
// are taken by value: they are moved into the coroutine frame at invocation, so
// they outlive every suspension point regardless of the caller's closure lifetime.
static seastar::future<ShardQueryResult> executeShardQuery(
    Engine& engine, unsigned shardId, std::vector<SeriesQueryContext> contexts, std::string measurement,
    uint64_t startTime, uint64_t endTime, AggregationMethod aggregation, uint64_t aggregationInterval,
    uint64_t bucketAnchor, bool booleansAsNumeric, std::vector<std::string> groupByTags) {
    auto shardStart = std::chrono::high_resolution_clock::now();
    LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Shard {} querying {} series keys in parallel", shardId,
                   contexts.size());

    // Series this shard failed to read. Tracked so the caller can refuse to
    // report an incomplete result as a success.
    size_t droppedSeries = 0;
    std::string firstDropReason;

    // Containers for pushdown results and fallback results
    std::vector<PartialAggregationResult> pushdownPartials;
    std::vector<timestar::SeriesResult> fallbackResults;
    fallbackResults.reserve(contexts.size());
    // String and boolean fields bypass numeric aggregation on every path
    // (canonical rule: raw passthrough without an interval, LATEST-per-bucket
    // with one), and are returned in the type they were written in.
    std::vector<timestar::SeriesResult> nonNumericResults;

    const bool isLatestOrFirst = isLatestOrFirstMethod(aggregation);

    // ---- BATCH LATEST/FIRST FAST PATH ----
    // For non-bucketed LATEST/FIRST, resolve all series in a single
    // pass over TSM sparse indices and memory stores.  This avoids
    // per-series: file snapshot, sort, coroutine overhead, and
    // intermediate PushdownResult/BlockAggregator allocations.
    //
    // ONLY for a bucketed LATEST/FIRST whose whole range falls inside one
    // epoch-aligned bucket — then "latest per bucket" names exactly one point
    // per series and a single sparse lookup resolves it.  Testing
    // `interval >= range` is NOT sufficient: a range shorter than the interval
    // still straddles two buckets when it crosses an epoch boundary (see
    // CLAUDE.md "Aggregation Result Shape").  The winning point is stamped with
    // its bucket start below, matching every other interval query.
    //
    // interval == 0 is NOT eligible: LATEST/FIRST no longer collapse a range,
    // so there is no single point to seek to — every timestamp survives and
    // those queries take the normal per-series path.
    // bucketAnchor != 0 is excluded: this fast path's single-bucket test and
    // bucket stamping use the epoch grid.  Anchored queries take the standard
    // fallback path below, which is anchor-aware.
    const bool singleBucket = aggregationInterval > 0 && bucketAnchor == 0 &&
                              (startTime / aggregationInterval) == (endTime / aggregationInterval);
    if (isLatestOrFirst && singleBucket) {
        const bool wantFirst = (aggregation == AggregationMethod::FIRST);

        // Build batch entries
        std::vector<Engine::BatchLatestEntry> batchEntries;
        batchEntries.resize(contexts.size());
        for (size_t i = 0; i < contexts.size(); ++i) {
            batchEntries[i].seriesId = contexts[i].seriesId;
        }

        co_await engine.batchLatest(batchEntries, startTime, endTime, wantFirst);

        // Convert resolved entries to PartialAggregationResults
        pushdownPartials.reserve(batchEntries.size());
        for (size_t i = 0; i < batchEntries.size(); ++i) {
            if (!batchEntries[i].resolved)
                continue;

            PartialAggregationResult partial;
            partial.measurement = measurement;
            partial.fieldName = contexts[i].field;
            partial.totalPoints = 1;

            auto gkr = timestar::buildGroupKeyDirect(measurement, contexts[i].field, contexts[i].tags, groupByTags);
            partial.groupKey = std::move(gkr.key);
            partial.groupKeyHash = gkr.hash;
            partial.cachedTags = std::move(gkr.tags);

            AggregationState state;
            state.addValue(batchEntries[i].value, batchEntries[i].timestamp);
            // Stamp the bucket start, not the point's own timestamp
            // (aggregationInterval > 0 is guaranteed by singleBucket).
            const uint64_t bucket = (batchEntries[i].timestamp / aggregationInterval) * aggregationInterval;
            partial.bucketStates.emplace(bucket, std::move(state));

            pushdownPartials.push_back(std::move(partial));
        }

        // Collect unresolved series (e.g. string types) for
        // standard per-series fallback below.
        std::vector<SeriesQueryContext> unresolvedContexts;
        for (size_t i = 0; i < batchEntries.size(); ++i) {
            if (!batchEntries[i].resolved) {
                unresolvedContexts.push_back(std::move(contexts[i]));
            }
        }
        contexts = std::move(unresolvedContexts);
    }

    if (contexts.empty()) {
        // All series resolved by batch or streaming path — skip standard path.
    } else if (bucketAnchor == 0 && !booleansAsNumeric &&
               canStreamAggregation(aggregation, groupByTags, aggregationInterval)) {
        // ---- STREAMING AGGREGATION PATH ----
        // Fold per-series results into accumulators without
        // materializing all raw data.  O(groups x buckets)
        // memory instead of O(points).  Works for both
        // group-by and non-group-by queries.
        //
        // APPEND, never assign: the batch LATEST/FIRST path above may already
        // have resolved the numeric series into pushdownPartials and left only
        // the non-numeric ones in `contexts`.  Overwriting here silently
        // dropped every numeric field from such a response.
        auto streamedPartials = co_await streamingGroupByAggregation(engine, contexts, measurement, startTime, endTime,
                                                                     aggregation, aggregationInterval, groupByTags,
                                                                     nonNumericResults, droppedSeries, firstDropReason);
        pushdownPartials.insert(pushdownPartials.end(), std::make_move_iterator(streamedPartials.begin()),
                                std::make_move_iterator(streamedPartials.end()));

    } else {
        // ---- STANDARD PER-SERIES PATH ----

        // Prefetch TSM index entries for all series on this shard.
        // Warms the full-index cache in parallel so that per-series
        // queries hit cache instead of issuing individual DMA reads.
        {
            std::vector<SeriesId128> prefetchIds;
            prefetchIds.reserve(contexts.size());
            for (const auto& ctx : contexts) {
                prefetchIds.push_back(ctx.seriesId);
            }
            co_await engine.prefetchSeriesIndices(prefetchIds);
        }

        // Use max_concurrent_for_each to avoid creating thousands of
        // concurrent futures at once. Bounded concurrency reduces memory
        // pressure and context thrashing.
        {
            static constexpr size_t MAX_CONCURRENT_SERIES_QUERIES = 64;

            co_await seastar::max_concurrent_for_each(
                contexts, MAX_CONCURRENT_SERIES_QUERIES,
                [&engine, &pushdownPartials, &fallbackResults, &nonNumericResults, &measurement, startTime, endTime,
                 shardId, aggregationInterval, bucketAnchor, booleansAsNumeric, aggregation, &groupByTags,
                 &droppedSeries, &firstDropReason](SeriesQueryContext& ctx) -> seastar::future<> {
                    // ---- NON-NUMERIC BOUNDED PATH (interval > 0) ----
                    // A bool/string series with an interval reduces to
                    // LATEST-per-bucket — an O(buckets) answer — yet the
                    // fallback below materialises O(points in range) raw data
                    // first and reduces afterwards.  At production sizes that
                    // materialise-then-reduce WAS the Jul 22 incident: seconds
                    // of decode plus a reactor-stalling reduction per bool
                    // series.  Probe the type without I/O and take a bounded
                    // path instead: the bucketed-LATEST pushdown for booleans
                    // (reads only the blocks that decide buckets), else the
                    // chunked reader (same total I/O as the single shot, but
                    // peak memory of one chunk and yields throughout).
                    //
                    // Booleans under booleansAsNumeric skip this: they
                    // aggregate arithmetically with the requested method, so
                    // they need the numeric flow below.  Strings route here
                    // regardless of that flag (they stay non-numeric).
                    if (aggregationInterval > 0) {
                        const auto localType = engine.localSeriesValueType(ctx.seriesId);
                        const bool routeBool =
                            localType.has_value() && *localType == TSMValueType::Boolean && !booleansAsNumeric;
                        const bool routeString = localType.has_value() && *localType == TSMValueType::String;
                        if (routeBool || routeString) {
                            bool routedFailed = false;
                            std::string routedReason;
                            try {
                                std::optional<timestar::SeriesResult> reduced;
                                if (routeBool && bucketAnchor == 0) {
                                    reduced = co_await detail::queryBoolLatestBucketed(
                                        engine, ctx.seriesKey, ctx.seriesId, ctx.field, ctx.tags, measurement,
                                        startTime, endTime, aggregationInterval);
                                }
                                if (!reduced.has_value()) {
                                    // Full-range first chunk = the old single-shot
                                    // read profile; bad_alloc halves it from there.
                                    const uint64_t range = endTime - startTime;
                                    const uint64_t fullWidth =
                                        (range == std::numeric_limits<uint64_t>::max()) ? range : range + 1;
                                    reduced = co_await detail::queryNonNumericBucketedChunked(
                                        engine, ctx.seriesKey, ctx.seriesId, ctx.field, ctx.tags, measurement,
                                        startTime, endTime, aggregationInterval, fullWidth, bucketAnchor);
                                }
                                if (reduced.has_value()) {
                                    if (!reduced->fields.empty()) {
                                        nonNumericResults.push_back(std::move(*reduced));
                                    }
                                    co_return;
                                }
                                // The read says the series is numeric after all
                                // (type probe raced a concurrent write?) — fall
                                // through to the standard flow, which handles
                                // every type.
                            } catch (const std::exception& e) {
                                routedFailed = true;
                                routedReason = e.what();
                            }
                            if (routedFailed) {
                                timestar::http_log.error(
                                    "[QUERY] Dropping series '{}' on shard {} from result: {}. The response will "
                                    "be INCOMPLETE.",
                                    ctx.seriesKey, shardId, routedReason);
                                ++droppedSeries;
                                if (firstDropReason.empty()) {
                                    firstDropReason = routedReason;
                                }
                                co_return;
                            }
                        }
                    }

                    // ---- PUSHDOWN PATH ----
                    // Try aggregating directly from TSM blocks, skipping the
                    // full TSMResult → QueryResult → SeriesResult pipeline.
                    //
                    // foldNoInterval=false: this standard path handles the
                    // no-group-by, no-interval case whose canonical response
                    // shape is per-timestamp aggregation across series (N
                    // points), so the pushdown must return raw sorted vectors
                    // — never a collapsed single state.  Collapsing here used
                    // to make the response shape depend on data placement
                    // (memstore raw vs TSM collapsed).  LATEST/FIRST are
                    // handled by the batch fast path above and always collapse.
                    // Anchored interval buckets bypass pushdown entirely: every
                    // pushdown bucket computation (BlockAggregator, TSM block
                    // stats, memory folds) uses the epoch grid.  Skipping the
                    // attempt — not just its result — keeps anchored queries on
                    // ONE path for every data placement.
                    std::optional<timestar::PushdownResult> pushdownResult;
                    if (bucketAnchor == 0 || aggregationInterval == 0) {
                        pushdownResult =
                            co_await engine.queryAggregated(ctx.seriesKey, ctx.seriesId, startTime, endTime,
                                                            aggregationInterval, aggregation, /*foldNoInterval=*/false);
                    }

                    if (pushdownResult.has_value()) {
                        // Build PartialAggregationResult directly from PushdownResult
                        PartialAggregationResult partial;
                        partial.measurement = measurement;
                        partial.fieldName = ctx.field;
                        partial.totalPoints = pushdownResult->totalPoints;

                        // Build composite groupKey (same format as createPartialAggregations)
                        auto gkr = timestar::buildGroupKeyDirect(measurement, ctx.field, ctx.tags, groupByTags);
                        partial.groupKey = std::move(gkr.key);
                        partial.groupKeyHash = gkr.hash;
                        partial.cachedTags = std::move(gkr.tags);

                        if (aggregationInterval > 0) {
                            partial.bucketStates = std::move(pushdownResult->bucketStates);
                        } else if (pushdownResult->aggregatedState.has_value()) {
                            partial.collapsedState = std::move(pushdownResult->aggregatedState);
                        } else {
                            partial.sortedTimestamps = std::move(pushdownResult->sortedTimestamps);
                            partial.sortedValues = std::move(pushdownResult->sortedValues);
                        }

                        pushdownPartials.push_back(std::move(partial));
                        co_return;
                    }

                    // ---- FALLBACK PATH ----
                    // Pushdown not applicable (non-float, memory data, overlap).
                    std::optional<VariantQueryResult> optResult;
                    // co_await is not permitted inside a catch handler, so the
                    // failure is recorded here and handled after the try/catch.
                    bool readFailed = false;
                    std::string failReason;
                    try {
                        optResult = co_await engine.query(ctx.seriesKey, ctx.seriesId, startTime, endTime);
                    } catch (const SeriesNotFoundException&) {
                        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Series '{}' not found on shard {} - skipping",
                                       ctx.seriesKey, shardId);
                        co_return;
                    } catch (const std::exception& e) {
                        readFailed = true;
                        failReason = e.what();
                    }

                    if (readFailed) {
                        // Bounded retry before dropping -- see the streaming path
                        // above and queryNonNumericBucketedChunked().
                        if (aggregationInterval > 0) {
                            bool recoveredOk = false;
                            try {
                                auto recovered = co_await detail::queryNonNumericBucketedChunked(
                                    engine, ctx.seriesKey, ctx.seriesId, ctx.field, ctx.tags, measurement, startTime,
                                    endTime, aggregationInterval, /*initialChunkWidth=*/0, bucketAnchor);
                                if (recovered.has_value()) {
                                    // Under booleansAsNumeric a boolean series must
                                    // aggregate numerically with the requested
                                    // method; this recovery reduces to
                                    // LATEST-per-bucket, which would silently
                                    // substitute the wrong answer.  Let such a
                                    // series drop to QUERY_INCOMPLETE instead
                                    // (strings still recover — they stay
                                    // non-numeric under the flag).
                                    bool boolUnderNumericFlag = false;
                                    if (booleansAsNumeric) {
                                        for (auto& [fn, fd] : recovered->fields) {
                                            if (std::holds_alternative<std::vector<bool>>(fd.second)) {
                                                boolUnderNumericFlag = true;
                                            }
                                        }
                                    }
                                    if (!boolUnderNumericFlag) {
                                        if (!recovered->fields.empty()) {
                                            nonNumericResults.push_back(std::move(*recovered));
                                        }
                                        recoveredOk = true;
                                    }
                                }
                            } catch (const std::exception&) {
                                // Still not satisfiable -- fall through and report it.
                            }
                            if (recoveredOk) {
                                co_return;
                            }
                        }
                        // Unconditional, NOT LOG_QUERY_PATH: that macro compiles to
                        // a no-op unless TIMESTAR_LOG_QUERY_PATH is set, so this
                        // warning did not exist in a normal build and the series
                        // vanished from the response with no trace at all.
                        timestar::http_log.error(
                            "[QUERY] Dropping series '{}' on shard {} from result: {}. The response will be "
                            "INCOMPLETE.",
                            ctx.seriesKey, shardId, failReason);
                        ++droppedSeries;
                        if (firstDropReason.empty()) {
                            firstDropReason = failReason;
                        }
                        co_return;
                    }

                    if (!optResult.has_value()) {
                        co_return;
                    }

                    auto variantResult = std::move(optResult.value());

                    timestar::SeriesResult seriesResult;
                    seriesResult.measurement = measurement;
                    seriesResult.tags = std::move(ctx.tags);

                    std::visit(
                        [&seriesResult, &ctx](auto&& result) {
                            using T = std::decay_t<decltype(result)>;
                            if constexpr (std::is_same_v<T, QueryResult<double>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(
                                        std::move(result.timestamps), FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<bool>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(
                                        std::move(result.timestamps), FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<std::string>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(
                                        std::move(result.timestamps), FieldValues(std::move(result.values)));
                                }
                            } else if constexpr (std::is_same_v<T, QueryResult<int64_t>>) {
                                if (!result.timestamps.empty()) {
                                    seriesResult.fields[ctx.field] = std::make_pair(
                                        std::move(result.timestamps), FieldValues(std::move(result.values)));
                                }
                            }
                        },
                        variantResult);

                    if (!seriesResult.fields.empty()) {
                        fallbackResults.push_back(std::move(seriesResult));
                    }
                });
        }  // end of non-batch per-series loop
    }  // end of batch-vs-standard if/else

    // Separate non-numeric (string, boolean) fallback results from numeric.
    // Non-numeric data bypasses aggregation entirely (raw, or LATEST-per-bucket
    // when an interval is set — see below) and is returned in its written type.
    // The pushdown paths refuse these types for the same reason, so the result
    // does not depend on where the data happens to sit (memory vs TSM).
    std::vector<timestar::SeriesResult> numericResults;
    for (auto& sr : fallbackResults) {
        // Migration compat (QueryRequest::booleansAsNumeric): booleans become
        // numeric 1.0/0.0 here — before the numeric/non-numeric split — so
        // they aggregate arithmetically on every method and bucket grid,
        // exactly like a rollup.js reader expects.  Strings are unaffected.
        if (booleansAsNumeric) {
            for (auto& [fn, fd] : sr.fields) {
                if (auto* boolVals = std::get_if<std::vector<bool>>(&fd.second)) {
                    std::vector<double> numeric;
                    numeric.reserve(boolVals->size());
                    for (bool b : *boolVals) {
                        numeric.push_back(b ? 1.0 : 0.0);
                    }
                    fd.second = FieldValues(std::move(numeric));
                }
            }
            co_await seastar::coroutine::maybe_yield();
        }
        bool hasNonNumericField = false;
        for (auto& [fn, fd] : sr.fields) {
            // Mirrors timestar::isNonNumericValueType at the variant level —
            // these two must never disagree about what "non-numeric" means.
            if (std::holds_alternative<std::vector<std::string>>(fd.second) ||
                std::holds_alternative<std::vector<bool>>(fd.second)) {
                hasNonNumericField = true;
            }
        }
        if (hasNonNumericField) {
            nonNumericResults.push_back(std::move(sr));
        } else {
            numericResults.push_back(std::move(sr));
        }
    }

    // Canonical non-numeric rule for interval queries: LATEST-per-bucket,
    // aligned to the same epoch buckets as the numeric aggregation.  Applied
    // uniformly to values collected on the streaming path and the standard
    // fallback path so the result shape does not depend on the query plan.
    // Series routed through the bounded non-numeric paths arrive already
    // bucketed; re-reducing bucketed data is the identity, so applying it
    // uniformly here stays correct and keeps one rule for every source.
    if (aggregationInterval > 0) {
        for (auto& sr : nonNumericResults) {
            co_await bucketNonNumericResultLatest(sr, aggregationInterval, bucketAnchor);
        }
    }

    // Run partial aggregation only on fallback numeric results
    auto partialAggStart = std::chrono::high_resolution_clock::now();
    auto partialResults = co_await Aggregator::createPartialAggregations(
        std::move(numericResults), aggregation, aggregationInterval, groupByTags, bucketAnchor);
    // Combine pushdown partials with fallback partials
    partialResults.insert(partialResults.end(), std::make_move_iterator(pushdownPartials.begin()),
                          std::make_move_iterator(pushdownPartials.end()));
    auto partialAggEnd = std::chrono::high_resolution_clock::now();
    [[maybe_unused]] double partialAggMs =
        std::chrono::duration<double, std::milli>(partialAggEnd - partialAggStart).count();  // used by LOG_QUERY_PATH

    auto shardEnd = std::chrono::high_resolution_clock::now();
    double shardMs = std::chrono::duration<double, std::milli>(shardEnd - shardStart).count();
    LOG_QUERY_PATH(timestar::http_log, info,
                   "[QUERY] Shard {} completed {} parallel queries + partial aggregation in "
                   "{:.2f} ms (aggregation: {:.2f} ms)",
                   shardId, contexts.size(), shardMs, partialAggMs);
    ShardQueryResult sqr;
    sqr.partialResults = std::move(partialResults);
    sqr.nonNumericResults = std::move(nonNumericResults);
    sqr.shardMs = shardMs;
    sqr.droppedSeries = droppedSeries;
    sqr.firstDropReason = std::move(firstDropReason);
    co_return sqr;
}

// Phase 1: scatter-gather series discovery.  Each shard discovers its own
// series from its local index; results are combined per shard.  No centralized
// shard-0 bottleneck.  Sets timedOut when the discovery phase exceeds the
// query timeout, and limitExceeded when a shard hit the series limit.
seastar::future<SeriesDiscoveryResult> HttpQueryHandler::discoverSeriesAcrossShards(const QueryRequest& request) {
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0)
        shardCount = 1;

    // Split scopes into exact-match (drives the postings-bitmap intersect in
    // the index) and pattern (wildcard / ~regex / /regex/) scopes. Pattern
    // scopes cannot be looked up as literal bitmap keys — doing so returns
    // zero series — so they are applied as a post-filter on resolved series
    // metadata via SeriesMatcher. The index discovery only sees exactScopes.
    std::map<std::string, std::string> exactScopes;
    std::map<std::string, std::string> patternScopes;
    for (const auto& [k, v] : request.scopes) {
        if (SeriesMatcher::classifyScope(v) == ScopeMatchType::EXACT)
            exactScopes.emplace(k, v);
        else
            patternScopes.emplace(k, v);
    }

    // Fan out discovery to all shards in parallel
    std::vector<seastar::future<std::pair<unsigned, PerShardDiscovery>>> discoveryFutures;
    discoveryFutures.reserve(shardCount);
    for (unsigned s = 0; s < shardCount; ++s) {
        auto f =
            engineSharded
                ->invoke_on(
                    s,
                    [measurement = request.measurement, scopes = exactScopes, patternScopes = patternScopes,
                     fields = request.fields, startTime = request.startTime, endTime = request.endTime,
                     maxSeries = maxSeriesCount()](Engine& engine) -> seastar::future<PerShardDiscovery> {
                        auto& index = engine.getIndex();
                        std::unordered_set<std::string> fieldFilter(fields.begin(), fields.end());
                        // Wide-range optimisation: use discovery cache for ranges > 365 days.
                        static constexpr uint64_t NS_PER_DAY = 86400ULL * 1'000'000'000ULL;
                        static constexpr uint64_t WIDE_RANGE_THRESHOLD = 365ULL * NS_PER_DAY;
                        const bool wideRange = (endTime > startTime) && (endTime - startTime) > WIDE_RANGE_THRESHOLD;

                        const std::vector<IndexBackend::SeriesWithMetadata>* swmPtr = nullptr;
                        std::shared_ptr<const std::vector<IndexBackend::SeriesWithMetadata>> cachedPtr;

                        PerShardDiscovery result;

                        if (wideRange) {
                            auto cr = co_await index.findSeriesWithMetadataCached(measurement, scopes, fieldFilter,
                                                                                  maxSeries);
                            if (!cr.has_value()) {
                                result.limitExceeded = true;
                                result.discovered = cr.error().discovered;
                                result.limit = cr.error().limit;
                                co_return result;
                            }
                            cachedPtr = std::move(*cr);
                            swmPtr = cachedPtr.get();
                        } else {
                            // Day-scoped discovery is cached too (shared
                            // immutable vector — no per-query metadata copies).
                            auto findResult = co_await index.findSeriesWithMetadataTimeScopedCached(
                                measurement, scopes, fieldFilter, startTime, endTime, maxSeries);
                            if (!findResult.has_value()) {
                                result.limitExceeded = true;
                                result.discovered = findResult.error().discovered;
                                result.limit = findResult.error().limit;
                                co_return result;
                            }
                            cachedPtr = std::move(*findResult);
                            swmPtr = cachedPtr.get();
                        }

                        result.contexts.reserve(swmPtr->size());

                        // Context building copies strings + a tag map per series;
                        // at high cardinality (thousands of series) this loop was
                        // an observed 150ms reactor stall — yield periodically.
                        size_t sinceYield = 0;
                        for (const auto& swm : *swmPtr) {
                            // Apply wildcard/regex scopes that the bitmap intersect could not.
                            if (!patternScopes.empty() && !SeriesMatcher::matches(swm.metadata.tags, patternScopes)) {
                                continue;
                            }
                            SeriesQueryContext ctx;
                            ctx.seriesKey =
                                buildSeriesKey(swm.metadata.measurement, swm.metadata.tags, swm.metadata.field);
                            ctx.seriesId = swm.seriesId;
                            ctx.field = swm.metadata.field;
                            ctx.tags = swm.metadata.tags;
                            result.contexts.push_back(std::move(ctx));

                            if (++sinceYield >= 512) {
                                sinceYield = 0;
                                co_await seastar::coroutine::maybe_yield();
                            }
                        }

                        co_return result;
                    })
                .then([s](PerShardDiscovery result) { return std::make_pair(s, std::move(result)); });
        discoveryFutures.push_back(std::move(f));
    }

    // Wait for all shards to complete discovery, with timeout to prevent indefinite hangs
    auto discoveryDeadline = seastar::lowres_clock::now() + defaultQueryTimeout();
    std::vector<std::pair<unsigned, PerShardDiscovery>> discoveryResults;
    SeriesDiscoveryResult discoveryResult;
    try {
        discoveryResults = co_await seastar::with_timeout(
            discoveryDeadline, seastar::when_all_succeed(discoveryFutures.begin(), discoveryFutures.end()));
    } catch (seastar::timed_out_error&) {
        discoveryResult.timedOut = true;
        co_return discoveryResult;
    }

    // Combine discovery results into seriesByShard
    discoveryResult.seriesByShard.resize(shardCount);

    for (auto& [s, perShard] : discoveryResults) {
        if (perShard.limitExceeded) {
            discoveryResult.limitExceeded = true;
            discoveryResult.discovered = perShard.discovered;
            discoveryResult.limit = perShard.limit;
            break;
        }
        discoveryResult.seriesByShard[s] = std::move(perShard.contexts);
    }

    // A grouping key must resolve to a real tag key, exactly as a scope key
    // does (an unknown scope key finds no postings bitmap, hence no series).
    // Without this, buildGroupKeyDirect matches nothing for an unrecognised
    // key and produces the same group key as an ungrouped query, silently
    // answering `by {devicId}` with a fleet-wide aggregate — a plausible but
    // wrong number, with nothing in the response signalling the grouping was
    // dropped.  Resolved against the discovered series rather than the tag
    // index so a schema broadcast still in flight cannot turn a valid grouping
    // into an empty result.
    if (!discoveryResult.limitExceeded && !request.groupByTags.empty()) {
        for (const auto& groupByTag : request.groupByTags) {
            bool known = false;
            for (const auto& contexts : discoveryResult.seriesByShard) {
                for (const auto& ctx : contexts) {
                    if (ctx.tags.contains(groupByTag)) {
                        known = true;
                        break;
                    }
                }
                if (known)
                    break;
            }
            if (!known) {
                discoveryResult.seriesByShard.assign(shardCount, {});
                co_return discoveryResult;
            }
        }
    }

    co_return discoveryResult;
}

// Phase 2: fan out query execution to every shard that owns series.
// Consumes (moves from) the per-shard context vectors in seriesByShard.
// Sets timedOut when the shard query phase exceeds the query timeout.
seastar::future<ShardFanOutResult> HttpQueryHandler::fanOutShardQueries(
    const QueryRequest& request, std::vector<std::vector<SeriesQueryContext>>& seriesByShard) {
    std::vector<seastar::future<std::pair<unsigned, ShardQueryResult>>> futures;
    for (unsigned shardId = 0; shardId < seriesByShard.size(); ++shardId) {
        if (seriesByShard[shardId].empty())
            continue;
        auto& shardContexts = seriesByShard[shardId];
        auto f =
            engineSharded
                ->invoke_on(shardId,
                            [shardId, contexts = std::move(shardContexts), startTime = request.startTime,
                             endTime = request.endTime, measurement = request.measurement,
                             aggregation = request.aggregation, aggregationInterval = request.aggregationInterval,
                             bucketAnchor = request.bucketAnchor, booleansAsNumeric = request.booleansAsNumeric,
                             groupByTags = request.groupByTags](Engine& engine) mutable {
                                // executeShardQuery takes its parameters by value: they are moved
                                // into the coroutine frame at invocation, so the closure (and its
                                // captures) need not outlive the returned future.
                                return executeShardQuery(engine, shardId, std::move(contexts), std::move(measurement),
                                                         startTime, endTime, aggregation, aggregationInterval,
                                                         bucketAnchor, booleansAsNumeric, std::move(groupByTags));
                            })
                .then([shardId](ShardQueryResult result) { return std::make_pair(shardId, std::move(result)); });
        futures.push_back(std::move(f));
    }

    // Wait for all shards to complete, with timeout to prevent indefinite hangs
    auto deadline = seastar::lowres_clock::now() + defaultQueryTimeout();
    ShardFanOutResult fanOut;
    try {
        fanOut.shardResults =
            co_await seastar::with_timeout(deadline, seastar::when_all_succeed(futures.begin(), futures.end()));
    } catch (seastar::timed_out_error&) {
        fanOut.timedOut = true;
    }
    co_return fanOut;
}

// Phase 3a: single-shard fast path finalize.
// All matching series live on one shard — skip the cross-shard merge pipeline.
// finalizeSingleShardPartials() produces SeriesResult entries directly from
// PartialAggregationResult, bypassing mergePartialAggregationsGrouped() and
// the GroupedAggregationResult intermediary.
//
// Fills `response` in place and returns std::nullopt on success; returns a
// complete error QueryResponse when a limit was exceeded (the caller returns
// it as-is).
// Consolidate series that share the same measurement+tags into a single
// SeriesResult with multiple fields.  The aggregation paths emit one series
// per field; the HTTP response format groups fields under a single series
// entry.  Called by BOTH finalize paths so the response shape does not depend
// on which shards the series landed on.
// Uses hash-indexed lookup for O(N) instead of O(N²) flat scan.
void HttpQueryHandler::consolidateSeriesFields(std::vector<SeriesResult>& seriesList) {
    // Multimap from (measurement+tags) hash to candidate indices in `merged`.
    // Same hash with different tag-sets must coexist; without this, a hash
    // collision overwrote the bucket and subsequent merges for the first
    // tag-set would miss its existing entry.
    std::unordered_multimap<size_t, size_t> tagHashToIdx;
    tagHashToIdx.reserve(seriesList.size());
    std::vector<SeriesResult> merged;
    merged.reserve(seriesList.size());

    for (auto& s : seriesList) {
        size_t h = std::hash<std::string>{}(s.measurement);
        for (const auto& [k, v] : s.tags) {
            h ^= std::hash<std::string>{}(k) * 31 + std::hash<std::string>{}(v);
        }

        bool mergedIn = false;
        auto [lo, hi] = tagHashToIdx.equal_range(h);
        for (auto it = lo; it != hi; ++it) {
            auto& existing = merged[it->second];
            if (existing.measurement == s.measurement && existing.tags == s.tags) {
                // Merging must never DESTROY data.  A field name is not unique
                // across a measurement's series (the index records a field's
                // type first-writer-wins and never rejects a conflicting one),
                // so two series sharing measurement+tags can both carry field
                // "v" — e.g. a numeric aggregate whose group-by tags happen to
                // equal a non-numeric series' full tag set.  Blindly assigning
                // here dropped the first one, silently and deterministically.
                // Keep this series separate instead; a caller seeing two
                // entries is correct, losing a field is not.
                bool collides = false;
                for (const auto& [fname, fdata] : s.fields) {
                    if (existing.fields.contains(fname)) {
                        collides = true;
                        break;
                    }
                }
                if (collides) {
                    continue;  // try the next candidate with this tag hash
                }
                for (auto& [fname, fdata] : s.fields) {
                    existing.fields[std::move(fname)] = std::move(fdata);
                }
                mergedIn = true;
                break;
            }
        }
        if (!mergedIn) {
            tagHashToIdx.emplace(h, merged.size());
            merged.push_back(std::move(s));
        }
    }
    seriesList = std::move(merged);
}

seastar::future<std::optional<QueryResponse>> HttpQueryHandler::finalizeSingleShardResponse(
    const QueryRequest& request, std::pair<unsigned, ShardQueryResult>& shardResult, QueryTimingInfo& timing,
    QueryResponse& response) {
    auto& [singleShardId, sqr] = shardResult;
    timing.perShardQueryMs.push_back({singleShardId, sqr.shardMs});

    for (const auto& partial : sqr.partialResults) {
        timing.totalPointsRetrieved += partial.totalPoints;
    }
    timing.resultCollectionMs = 0.0;

    // Only bucketing reduces the point count.  Neither group-by nor
    // LATEST/FIRST does: without an aggregationInterval every distinct
    // timestamp survives, whatever the method.
    bool aggregationReducesOutput = request.aggregationInterval > 0;
    if (!aggregationReducesOutput) {
        for (const auto& p : sqr.partialResults) {
            if (p.collapsedState.has_value()) {
                aggregationReducesOutput = true;
                break;
            }
        }
    }
    if (!aggregationReducesOutput && timing.totalPointsRetrieved > maxTotalPoints()) {
        QueryResponse limitResponse;
        limitResponse.success = false;
        limitResponse.errorCode = "TOO_MANY_POINTS";
        limitResponse.errorMessage = "Total points " + std::to_string(timing.totalPointsRetrieved) +
                                     " exceeds limit of " + std::to_string(maxTotalPoints());
        limitResponse.statistics.truncated = true;
        limitResponse.statistics.truncationReason = limitResponse.errorMessage;
        co_return limitResponse;
    }

    auto aggregationStart = std::chrono::high_resolution_clock::now();
    std::unordered_set<std::string> requestedFieldSet(request.fields.begin(), request.fields.end());
    if (!sqr.partialResults.empty()) {
        // Check for duplicate groupKeys: non-group-by queries with multiple matching
        // series produce partials that share the same groupKey. finalizeSingleShardPartials
        // would overwrite earlier data, so we fall back to the merge path for correctness.
        bool hasDuplicateGroupKeys = false;
        if (sqr.partialResults.size() > 1) {
            std::unordered_set<std::string_view> seen;
            seen.reserve(sqr.partialResults.size());
            for (const auto& p : sqr.partialResults) {
                if (!seen.insert(p.groupKey).second) {
                    hasDuplicateGroupKeys = true;
                    break;
                }
            }
        }

        if (!hasDuplicateGroupKeys) {
            // All groupKeys unique — direct finalization (no merge needed)
            co_await finalizeSingleShardPartials(sqr.partialResults, request.aggregation, response);
        } else {
            // Duplicate groupKeys — must merge partials before finalizing
            auto groupedResults =
                co_await Aggregator::mergePartialAggregationsGrouped(sqr.partialResults, request.aggregation);

            response.series.reserve(groupedResults.size());
            for (auto& groupedResult : groupedResults) {
                std::vector<uint64_t> timestamps;
                std::vector<double> values;
                if (!groupedResult.rawTimestamps.empty()) {
                    timestamps = std::move(groupedResult.rawTimestamps);
                    values = std::move(groupedResult.rawValues);
                } else {
                    timestamps.reserve(groupedResult.points.size());
                    values.reserve(groupedResult.points.size());
                    size_t sinceYield = 0;
                    for (const auto& point : groupedResult.points) {
                        timestamps.push_back(point.timestamp);
                        values.push_back(point.value);
                        if (++sinceYield >= kYieldChunkPoints) {
                            sinceYield = 0;
                            co_await seastar::coroutine::maybe_yield();
                        }
                    }
                }

                // Each grouped result produces its own series (one field per series)
                SeriesResult series;
                series.measurement = std::move(groupedResult.measurement);
                series.tags = std::move(groupedResult.tags);
                std::string fieldName = std::move(groupedResult.fieldName);
                series.fields[fieldName] = std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

                response.series.push_back(std::move(series));

                // Series boundary: cheap preemption check (reactor-stall prevention)
                co_await seastar::coroutine::maybe_yield();
            }
        }
    }

    // Non-numeric (string/bool) results arrive one field per series.  Append
    // them BEFORE consolidating so they merge by measurement+tags like every
    // other field: "one series per measurement+tags" is a property of the
    // response, not of a field's type.  Appending after consolidation left a
    // measurement's string/bool fields stranded in their own series entries.
    for (auto& nonNumeric : sqr.nonNumericResults) {
        response.series.push_back(std::move(nonNumeric));
    }

    // Same consolidation as the multi-shard path: without it, the response
    // shape depended on shard placement (multi-field series consolidated
    // only when the fields happened to land on different shards).
    consolidateSeriesFields(response.series);

    if (!requestedFieldSet.empty()) {
        for (auto& s : response.series) {
            std::erase_if(s.fields, [&](const auto& kv) { return !requestedFieldSet.contains(kv.first); });
        }
        std::erase_if(response.series, [](const SeriesResult& s) { return s.fields.empty(); });
    }

    response.statistics.pointCount = 0;
    timing.finalPointsReturned = 0;
    for (const auto& series : response.series) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            size_t points = fieldData.first.size();
            response.statistics.pointCount += points;
            timing.finalPointsReturned += points;
        }
    }
    if (response.statistics.pointCount > maxTotalPoints()) {
        response.statistics.truncated = true;
        response.statistics.truncationReason = "Total points " + std::to_string(response.statistics.pointCount) +
                                               " exceeds limit of " + std::to_string(maxTotalPoints());
        response.success = false;
        response.errorCode = "TOO_MANY_POINTS";
        response.errorMessage = response.statistics.truncationReason;
        co_return std::move(response);
    }

    auto aggregationEnd = std::chrono::high_resolution_clock::now();
    timing.aggregationMs = std::chrono::duration<double, std::milli>(aggregationEnd - aggregationStart).count();

    co_return std::nullopt;
}

// Phase 3b: multi-shard merge + aggregation finalize.
// Collects partial aggregations from all shards, merges them grouped, and
// builds the response series.  Fills `response` in place and returns
// std::nullopt on success; returns a complete error QueryResponse when a
// limit was exceeded (the caller returns it as-is).
seastar::future<std::optional<QueryResponse>> HttpQueryHandler::finalizeMultiShardResponse(
    const QueryRequest& request, std::vector<std::pair<unsigned, ShardQueryResult>>& shardResults,
    QueryTimingInfo& timing, QueryResponse& response) {
    auto mergeStart = std::chrono::high_resolution_clock::now();

    std::vector<PartialAggregationResult> allPartialResults;
    std::vector<SeriesResult> allNonNumericResults;
    size_t totalPartialResults = 0;
    for (const auto& sr : shardResults) {
        totalPartialResults += sr.second.partialResults.size();
    }
    allPartialResults.reserve(totalPartialResults);

    for (auto& shardResult : shardResults) {
        unsigned shardId = shardResult.first;
        auto& sqr = shardResult.second;
        timing.perShardQueryMs.push_back({shardId, sqr.shardMs});

        // Count points from partial results
        for (const auto& partial : sqr.partialResults) {
            timing.totalPointsRetrieved += partial.totalPoints;
        }

        // Collect all partial results
        allPartialResults.insert(allPartialResults.end(), std::make_move_iterator(sqr.partialResults.begin()),
                                 std::make_move_iterator(sqr.partialResults.end()));

        // Collect string results (these bypass aggregation)
        allNonNumericResults.insert(allNonNumericResults.end(), std::make_move_iterator(sqr.nonNumericResults.begin()),
                                    std::make_move_iterator(sqr.nonNumericResults.end()));
    }

    auto mergeEnd = std::chrono::high_resolution_clock::now();
    timing.resultCollectionMs = std::chrono::duration<double, std::milli>(mergeEnd - mergeStart).count();

    // Early point-count check: totalPointsRetrieved is an upper bound on
    // the final output (merging can only reduce counts via timestamp dedup).
    // Fail fast before the expensive merge + JSON serialization phase.
    //
    // Skip when bucketing is active: that pipeline (including pushdown) reduces
    // output far below the raw point count, so totalPointsRetrieved would be a
    // massive overcount.  The final output limit is still enforced after
    // aggregation (line ~884).  Neither group-by nor LATEST/FIRST reduces
    // output — without an interval every timestamp survives.
    bool aggregationReducesOutput = request.aggregationInterval > 0;
    if (!aggregationReducesOutput) {
        for (const auto& p : allPartialResults) {
            if (p.collapsedState.has_value()) {
                aggregationReducesOutput = true;
                break;
            }
        }
    }
    if (!aggregationReducesOutput && timing.totalPointsRetrieved > maxTotalPoints()) {
        QueryResponse limitResponse;
        limitResponse.success = false;
        limitResponse.errorCode = "TOO_MANY_POINTS";
        limitResponse.errorMessage = "Total points " + std::to_string(timing.totalPointsRetrieved) +
                                     " exceeds limit of " + std::to_string(maxTotalPoints());
        limitResponse.statistics.truncated = true;
        limitResponse.statistics.truncationReason = limitResponse.errorMessage;
        co_return limitResponse;
    }

    // Merge partial aggregations from all shards into final aggregated points
    auto aggregationStart = std::chrono::high_resolution_clock::now();

    // Build field filter set once, shared by both numeric and string result paths
    std::unordered_set<std::string> requestedFieldSet(request.fields.begin(), request.fields.end());

    if (!allPartialResults.empty()) {
        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Merging {} partial aggregations from {} shards",
                       allPartialResults.size(), timing.shardsQueried);

        // OPTIMIZATION & FIX: Use grouped merge to preserve metadata associations
        auto groupedResults =
            co_await Aggregator::mergePartialAggregationsGrouped(allPartialResults, request.aggregation);

        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Merged into {} grouped results", groupedResults.size());

        // Each grouped result produces its own series (one field per series).
        response.series.reserve(groupedResults.size());

        for (auto& groupedResult : groupedResults) {
            // Build timestamps and values for this field.
            // Fast path: if the merge returned raw vectors (single-partial
            // pushdown), move them directly without the AggregatedPoint split.
            std::vector<uint64_t> timestamps;
            std::vector<double> values;
            if (!groupedResult.rawTimestamps.empty()) {
                timestamps = std::move(groupedResult.rawTimestamps);
                values = std::move(groupedResult.rawValues);
            } else {
                timestamps.reserve(groupedResult.points.size());
                values.reserve(groupedResult.points.size());
                size_t sinceYield = 0;
                for (const auto& point : groupedResult.points) {
                    timestamps.push_back(point.timestamp);
                    values.push_back(point.value);
                    if (++sinceYield >= kYieldChunkPoints) {
                        sinceYield = 0;
                        co_await seastar::coroutine::maybe_yield();
                    }
                }
            }

            // Each grouped result produces its own series (one field per series)
            SeriesResult series;
            series.measurement = std::move(groupedResult.measurement);
            series.tags = std::move(groupedResult.tags);
            series.fields[std::move(groupedResult.fieldName)] =
                std::make_pair(std::move(timestamps), FieldValues(std::move(values)));

            response.series.push_back(std::move(series));

            // Series boundary: cheap preemption check (reactor-stall prevention)
            co_await seastar::coroutine::maybe_yield();
        }
    }

    // Non-numeric (string/bool) results bypassed aggregation and arrive one
    // field per series.  Append them BEFORE consolidating so they merge by
    // measurement+tags like every other field — "one series per
    // measurement+tags" is a property of the response, not of a field's type.
    for (auto& nonNumeric : allNonNumericResults) {
        response.series.push_back(std::move(nonNumeric));
    }

    // Consolidate series that share the same measurement+tags into
    // a single SeriesResult with multiple fields.  The aggregation
    // loop above emits one SeriesResult per field; the HTTP response
    // format groups fields under a single series entry.
    consolidateSeriesFields(response.series);

    // Filter fields: remove non-requested fields from each series, then remove empty series.
    if (!requestedFieldSet.empty()) {
        for (auto& s : response.series) {
            std::erase_if(s.fields, [&](const auto& kv) { return !requestedFieldSet.contains(kv.first); });
        }
        std::erase_if(response.series, [](const SeriesResult& s) { return s.fields.empty(); });
    }

    // Update statistics and enforce maxTotalPoints() (covers both numeric and string results)
    response.statistics.pointCount = 0;
    timing.finalPointsReturned = 0;
    for (const auto& series : response.series) {
        for (const auto& [fieldName, fieldData] : series.fields) {
            size_t points = fieldData.first.size();
            response.statistics.pointCount += points;
            timing.finalPointsReturned += points;
        }
    }

    // Enforce maxTotalPoints() limit
    if (response.statistics.pointCount > maxTotalPoints()) {
        response.statistics.truncated = true;
        response.statistics.truncationReason = "Total points " + std::to_string(response.statistics.pointCount) +
                                               " exceeds limit of " + std::to_string(maxTotalPoints());
        response.success = false;
        response.errorCode = "TOO_MANY_POINTS";
        response.errorMessage = response.statistics.truncationReason;
        co_return std::move(response);
    }

    auto aggregationEnd = std::chrono::high_resolution_clock::now();
    timing.aggregationMs = std::chrono::duration<double, std::milli>(aggregationEnd - aggregationStart).count();

    co_return std::nullopt;
}

// Phase 4: timing breakdown + slow-query logging.
void HttpQueryHandler::logQueryCompletion(const QueryRequest& request, QueryTimingInfo& timing) {
    // Calculate total timing
    timing.endTime = std::chrono::high_resolution_clock::now();
    timing.totalMs = std::chrono::duration<double, std::milli>(timing.endTime - timing.startTime).count();

    // Print timing information
    LOG_QUERY_PATH(timestar::http_log, info, "{}", timing.toString());

    // Slow query detection (always compiled in, runtime configurable)
    const auto slowThresholdMs = timestar::config().http.slow_query_threshold_ms;
    if (slowThresholdMs > 0 && timing.totalMs > static_cast<double>(slowThresholdMs)) {
        if (engineSharded)
            ++engineSharded->local().metrics().slow_queries_total;
        timestar::query_log.warn(
            "[SLOW_QUERY] {:.1f}ms (threshold {}ms) | "
            "measurement={} series={} points={} shards={} | "
            "discovery={:.1f}ms shard_queries={:.1f}ms aggregation={:.1f}ms",
            timing.totalMs, slowThresholdMs, request.measurement, timing.seriesFound, timing.finalPointsReturned,
            timing.shardsQueried, timing.findSeriesMs, timing.shardQueriesMs, timing.aggregationMs);

        // Per-shard breakdown for slow queries (only log shards that took >50% of threshold)
        const double shardWarnMs = static_cast<double>(slowThresholdMs) / 2.0;
        for (const auto& [shardId, ms] : timing.perShardQueryMs) {
            if (ms > shardWarnMs) {
                timestar::query_log.warn("[SLOW_QUERY]   shard {} took {:.1f}ms", shardId, ms);
            }
        }
    }
}

seastar::future<QueryResponse> HttpQueryHandler::executeQuery(QueryRequest request) {
    // `request` is BY VALUE (see the header): a reference parameter would be
    // read after every co_await while the caller's object may already be gone
    // — the SSE backfill loop passes a loop-body local through
    // seastar::with_timeout, which does NOT cancel the inner future, so a
    // backfill timeout destroys it while this coroutine is still suspended.
    // Do not "optimise" this back into a const&.

    // NOTE: LATEST/FIRST whose interval covers the whole range used to have
    // their aggregationInterval zeroed here, to reach the non-bucketed sparse
    // fast path.  That silently changed the ANSWER, not just the plan:
    //   - non-numeric fields flipped from LATEST-per-bucket (one value) to raw
    //     passthrough (every value), and
    //   - numeric points came back stamped with their raw timestamp instead of
    //     the bucket start every other interval query returns.
    // The fast path is now selected inside executeShardQuery (see the
    // singleBucket gate), which keeps the optimisation without touching the
    // requested semantics.

    LOG_QUERY_PATH(timestar::http_log, info,
                   "[QUERY] Executing query - Measurement: {}, Fields: {}, Scopes: {}, Start: {}, End: {}",
                   request.measurement, request.fields.size(), request.scopes.size(), request.startTime,
                   request.endTime);

    QueryResponse response;
    response.success = true;

    // Initialize timing tracker
    QueryTimingInfo timing;
    timing.startTime = std::chrono::high_resolution_clock::now();

    // Increment query counter on the local shard
    if (engineSharded) {
        ++engineSharded->local().metrics().queries_total;
    }

    try {
        LOG_QUERY_PATH(timestar::http_log, info, "[QUERY] Checking pointer - engineSharded: {}",
                       static_cast<const void*>(engineSharded));

        if (!engineSharded) {
            LOG_QUERY_PATH(timestar::http_log, error, "[QUERY] engineSharded is NULL!");
            response.success = false;
            response.errorCode = "NULL_ENGINE";
            response.errorMessage = "Engine pointer is null";
            co_return response;
        }

        // Scatter-gather: each shard discovers its own series from its local index,
        // then we combine the results. No centralized shard-0 bottleneck.
        auto findSeriesStart = std::chrono::high_resolution_clock::now();

        SeriesDiscoveryResult discoveryResult = co_await discoverSeriesAcrossShards(request);

        if (discoveryResult.timedOut) {
            QueryResponse timeoutResponse;
            timeoutResponse.success = false;
            timeoutResponse.errorCode = "QUERY_TIMEOUT";
            timeoutResponse.errorMessage =
                "Discovery phase timed out after " + std::to_string(defaultQueryTimeout().count()) + " seconds";
            co_return timeoutResponse;
        }

        auto findSeriesEnd = std::chrono::high_resolution_clock::now();
        timing.findSeriesMs = std::chrono::duration<double, std::milli>(findSeriesEnd - findSeriesStart).count();

        // Check if the series limit was exceeded in the index layer (early bailout)
        if (discoveryResult.limitExceeded) {
            response.success = false;
            response.errorCode = "TOO_MANY_SERIES";
            response.errorMessage = "Too many series: " + std::to_string(discoveryResult.discovered) +
                                    " exceeds limit of " + std::to_string(discoveryResult.limit);
            co_return response;
        }

        auto& seriesByShard = discoveryResult.seriesByShard;

        // Compute total series count from the grouped buckets
        size_t totalSeriesFound = 0;
        size_t shardsWithData = 0;
        for (const auto& contexts : seriesByShard) {
            if (!contexts.empty()) {
                totalSeriesFound += contexts.size();
                ++shardsWithData;
            }
        }
        timing.seriesFound = totalSeriesFound;
        timing.shardsQueried = shardsWithData;

        LOG_QUERY_PATH(timestar::http_log, info,
                       "[QUERY] Total series from centralized metadata: {} across {} shards (took {:.2f} ms)",
                       totalSeriesFound, shardsWithData, timing.findSeriesMs);

        // Safety net: enforce maxSeriesCount() limit (should rarely trigger now
        // that the index layer checks early, but kept for defense-in-depth)
        if (totalSeriesFound > maxSeriesCount()) {
            response.success = false;
            response.errorCode = "TOO_MANY_SERIES";
            response.errorMessage = "Too many series: " + std::to_string(totalSeriesFound) + " exceeds limit of " +
                                    std::to_string(maxSeriesCount());
            co_return response;
        }

        // Execute queries on each shard that has series
        auto shardQueriesStart = std::chrono::high_resolution_clock::now();
        ShardFanOutResult fanOut = co_await fanOutShardQueries(request, seriesByShard);

        // Refuse to report an incomplete result as a success. A shard that could
        // not read a series omits it entirely, so the response would otherwise be
        // a plausible-looking answer computed over a subset of the data -- or, if
        // every series failed, an empty result indistinguishable from "no data".
        // That is exactly what a query spanning duplicate-timestamp data used to
        // return: the aggregation pushdown declines when timestamps repeat across
        // files, the fallback materialises the whole series to dedup it, and on a
        // memory-constrained shard that throws std::bad_alloc.
        {
            size_t totalDropped = 0;
            std::string reason;
            for (const auto& [shardId, sr] : fanOut.shardResults) {
                totalDropped += sr.droppedSeries;
                if (reason.empty() && !sr.firstDropReason.empty()) {
                    reason = sr.firstDropReason;
                }
            }
            if (totalDropped > 0) {
                QueryResponse incomplete;
                incomplete.success = false;
                incomplete.errorCode = "QUERY_INCOMPLETE";
                incomplete.errorMessage = "Query could not read " + std::to_string(totalDropped) +
                                          " series and would have returned an incomplete result" +
                                          (reason.empty() ? std::string() : (": " + reason));
                co_return incomplete;
            }
        }

        if (fanOut.timedOut) {
            QueryResponse timeoutResponse;
            timeoutResponse.success = false;
            timeoutResponse.errorCode = "QUERY_TIMEOUT";
            timeoutResponse.errorMessage =
                "Query timed out after " + std::to_string(defaultQueryTimeout().count()) + " seconds";
            co_return timeoutResponse;
        }
        auto shardQueriesEnd = std::chrono::high_resolution_clock::now();
        timing.shardQueriesMs = std::chrono::duration<double, std::milli>(shardQueriesEnd - shardQueriesStart).count();

        auto& shardResults = fanOut.shardResults;
        if (shardResults.size() == 1) {
            // === SINGLE-SHARD FAST PATH ===
            if (auto earlyReturn = co_await finalizeSingleShardResponse(request, shardResults[0], timing, response)) {
                co_return std::move(*earlyReturn);
            }
        } else {
            // === MULTI-SHARD GENERAL PATH ===
            if (auto earlyReturn = co_await finalizeMultiShardResponse(request, shardResults, timing, response)) {
                co_return std::move(*earlyReturn);
            }
        }

        response.statistics.seriesCount = response.series.size();

        logQueryCompletion(request, timing);

    } catch (const std::exception& e) {
        response.success = false;
        response.errorCode = "QUERY_EXECUTION_ERROR";
        response.errorMessage = "Query execution failed";
        timestar::http_log.error("Query execution failed: {}", e.what());
    }

    co_return response;
}

seastar::future<std::string> HttpQueryHandler::formatQueryResponse(QueryResponse& response) {
    return ResponseFormatter::format(response);
}

std::string HttpQueryHandler::createErrorResponse(const std::string& code, const std::string& message) {
    return ResponseFormatter::formatError(message, code);
}

// Test-only utility: production query path iterates shards inline in executeQuery().
std::vector<unsigned> HttpQueryHandler::determineTargetShards(const QueryRequest& /*request*/) {
    unsigned shardCount = seastar::smp::count;
    if (shardCount == 0)
        shardCount = 1;  // test environment default
    std::vector<unsigned> shards(shardCount);
    std::iota(shards.begin(), shards.end(), 0u);
    return shards;
}

// Test-only utility: flat concat. Production uses Aggregator::mergePartialAggregationsGrouped().
std::vector<SeriesResult> HttpQueryHandler::mergeResults(std::vector<std::vector<SeriesResult>> shardResults) {
    std::vector<SeriesResult> merged;
    for (auto& shardResult : shardResults) {
        merged.insert(merged.end(), std::make_move_iterator(shardResult.begin()),
                      std::make_move_iterator(shardResult.end()));
    }
    return merged;
}

uint64_t HttpQueryHandler::parseInterval(const std::string& interval) {
    if (interval.empty()) {
        throw QueryParseException("Interval string cannot be empty");
    }

    // Find where the numeric part ends
    size_t unitPos = 0;
    bool hasDecimal = false;
    for (size_t i = 0; i < interval.length(); ++i) {
        if (std::isdigit(interval[i]) || (interval[i] == '.' && !hasDecimal)) {
            if (interval[i] == '.')
                hasDecimal = true;
            unitPos = i + 1;
        } else {
            break;
        }
    }

    if (unitPos == 0) {
        throw QueryParseException("Invalid interval format: no numeric value");
    }

    std::string valueStr = interval.substr(0, unitPos);
    std::string unit = interval.substr(unitPos);

    // Convert to nanoseconds based on unit
    uint64_t multiplier = 0;
    if (unit == "ns") {
        multiplier = 1;
    } else if (unit == "us" || unit == "µs") {
        multiplier = 1000;
    } else if (unit == "ms") {
        multiplier = 1000000;
    } else if (unit == "s") {
        multiplier = 1000000000;
    } else if (unit == "m") {
        multiplier = 60ULL * 1000000000;
    } else if (unit == "h") {
        multiplier = 3600ULL * 1000000000;
    } else if (unit == "d") {
        multiplier = 86400ULL * 1000000000;
    } else if (unit.empty()) {
        // Bare numeric interval = nanoseconds.  Keeps string-typed interval
        // fields (protobuf QueryRequest, string JSON values) consistent with
        // the documented JSON numeric form, which has always meant ns.
        multiplier = 1;
    } else {
        throw QueryParseException("Unknown time unit: '" + unit + "'. Supported units: ns, us, ms, s, m, h, d");
    }

    // Use integer arithmetic when possible to avoid floating-point precision loss
    if (hasDecimal) {
        double value;
        try {
            value = std::stod(valueStr);
        } catch (const std::invalid_argument&) {
            throw QueryParseException("Invalid interval format: '" + valueStr + "' is not a valid number");
        } catch (const std::out_of_range&) {
            throw QueryParseException("Interval value overflow: " + interval +
                                      " exceeds maximum representable nanoseconds");
        }
        // Guard against NaN or Inf before any arithmetic: casting a non-finite
        // double to uint64_t is undefined behavior in C++.
        if (!std::isfinite(value) || value < 0) {
            throw QueryParseException("Invalid interval: '" + interval + "' must be a finite positive number");
        }
        double result = value * static_cast<double>(multiplier);
        // Clamp to UINT64_MAX on overflow — any interval larger than the query
        // range simply produces a single bucket, so saturation is correct.
        if (!std::isfinite(result) || result > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
            return std::numeric_limits<uint64_t>::max();
        }
        return static_cast<uint64_t>(result);
    } else {
        uint64_t value;
        try {
            value = std::stoull(valueStr);
        } catch (const std::out_of_range&) {
            throw QueryParseException("Interval value overflow: " + interval);
        }
        // Clamp to UINT64_MAX on overflow (same rationale as decimal path).
        if (multiplier > 1 && value > std::numeric_limits<uint64_t>::max() / multiplier) {
            return std::numeric_limits<uint64_t>::max();
        }
        return value * multiplier;
    }
}

}  // namespace timestar::http
