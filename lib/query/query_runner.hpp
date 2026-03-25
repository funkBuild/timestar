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
    seastar::future<std::optional<timestar::PushdownResult>> queryTsmAggregated(
        std::string seriesKey, SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
        timestar::AggregationMethod method = timestar::AggregationMethod::AVG);

    // Multi-field batch aggregation: fold N series through TSM files and memory
    // stores in a single pass instead of N independent passes.  Each entry in
    // the input vector is a (seriesKey, seriesId) pair.  Returns a vector of
    // the same length, where each element is either a PushdownResult (success)
    // or nullopt (pushdown not applicable for that field).
    seastar::future<std::vector<std::optional<timestar::PushdownResult>>> queryTsmAggregatedBatch(
        const std::vector<std::pair<std::string, SeriesId128>>& seriesEntries, uint64_t startTime, uint64_t endTime,
        uint64_t aggregationInterval, timestar::AggregationMethod method);
};
