#pragma once

#include "block_aggregator.hpp"
#include "query_parser.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "tsm_file_manager.hpp"
#include "wal_file_manager.hpp"

#include <optional>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <string>

// Thrown when a query references a series that does not exist in the database.
class SeriesNotFoundException : public std::runtime_error {
public:
    explicit SeriesNotFoundException(const std::string& series) : std::runtime_error("Series not found: " + series) {}
};

class QueryRunner {
private:
    TSMFileManager* fileManager;
    WALFileManager* walFileManager;

    template <class T>
    seastar::future<QueryResult<T>> queryTsm(const std::string& series, SeriesId128 seriesId, uint64_t startTime,
                                             uint64_t endTime);

public:
    QueryRunner(TSMFileManager* _fileManager, WALFileManager* _walFileManager)
        : fileManager(_fileManager), walFileManager(_walFileManager) {}

    seastar::future<VariantQueryResult> runQuery(const std::string& series, SeriesId128 seriesId, uint64_t startTime,
                                                 uint64_t endTime);
    // Convenience overload: computes SeriesId128 internally
    seastar::future<VariantQueryResult> runQuery(const std::string& series, uint64_t startTime, uint64_t endTime);

    // Pushdown aggregation: fold TSM float data directly into AggregationState,
    // bypassing the full QueryResult materialisation pipeline.
    // Returns nullopt when pushdown is not applicable (non-float type, memory
    // store data present, or cross-TSM block time overlap).
    //
    // foldNoInterval controls the aggregationInterval == 0 result shape for
    // streamable non-LATEST/FIRST methods: true folds all points into a single
    // collapsed AggregationState; false returns raw sorted (timestamp, value)
    // vectors so the caller can produce per-timestamp results.  The choice must
    // be made by the caller from the QUERY alone — never from data placement —
    // so the response shape stays deterministic (see CLAUDE.md "Aggregation
    // Result Shape").  LATEST/FIRST always collapse regardless of this flag.
    //
    // Defaults to FALSE: collapsing a range the caller did not ask to collapse
    // violates the canonical rules — grouping must never alter the time axis.
    // Group-by callers previously passed true (they collapsed per group); that
    // was the T1 defect and both production call sites now pass false.
    //
    // boolLatestAsNumeric relaxes the non-numeric gates for BOOLEAN series
    // only, folding true/false as 1.0/0.0 through the bucketed LATEST fast
    // path (reverse block scan, filledBuckets early termination, sparse-stat
    // single-point resolution).  This exists for exactly one caller shape —
    // the canonical non-numeric interval reduction, which is LATEST-per-bucket
    // by definition — so it is honoured only when method == LATEST and
    // aggregationInterval > 0; any other combination ignores the flag.  The
    // caller converts the resulting per-bucket 1.0/0.0 states back to
    // true/false, so the response type is unchanged.  String series are still
    // refused (their values cannot round-trip through a double).
    seastar::future<std::optional<timestar::PushdownResult>> queryTsmAggregated(
        SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
        timestar::AggregationMethod method = timestar::AggregationMethod::AVG, bool foldNoInterval = false,
        bool boolLatestAsNumeric = false);

    // No-I/O type probe: the series' value type as recorded in the pinned
    // memory stores or any TSM file's sparse index on this shard.  nullopt when
    // the series is unknown here.  Lets the query handler route non-numeric
    // series to their bounded read paths WITHOUT first materialising the full
    // range just to discover the type.
    std::optional<TSMValueType> localSeriesValueType(const SeriesId128& seriesId);
};
