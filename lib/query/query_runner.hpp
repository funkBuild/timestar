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
    // collapsed AggregationState (used by group-by callers, which collapse per
    // group anyway); false returns raw sorted (timestamp, value) vectors so
    // the caller can produce per-timestamp results.  The choice must be made
    // by the caller from the QUERY alone — never from data placement — so the
    // response shape stays deterministic (see CLAUDE.md "no-interval query
    // semantics").  LATEST/FIRST always collapse regardless of this flag.
    seastar::future<std::optional<timestar::PushdownResult>> queryTsmAggregated(
        SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
        timestar::AggregationMethod method = timestar::AggregationMethod::AVG, bool foldNoInterval = true);
};
