#pragma once

#include "block_aggregator.hpp"
#include "query_parser.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "tsm_file_manager.hpp"
#include "wal_file_manager.hpp"

#include <optional>
#include <seastar/core/coroutine.hh>
#include <string>

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

    seastar::future<VariantQueryResult> runQuery(std::string series, SeriesId128 seriesId, uint64_t startTime,
                                                 uint64_t endTime);
    // Convenience overload: computes SeriesId128 internally
    seastar::future<VariantQueryResult> runQuery(std::string series, uint64_t startTime, uint64_t endTime);

    // Pushdown aggregation: fold TSM float data directly into AggregationState,
    // bypassing the full QueryResult materialisation pipeline.
    // Returns nullopt when pushdown is not applicable (non-float type, memory
    // store data present, or cross-TSM block time overlap).
    seastar::future<std::optional<timestar::PushdownResult>> queryTsmAggregated(
        std::string seriesKey, SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
        timestar::AggregationMethod method = timestar::AggregationMethod::AVG);
};
