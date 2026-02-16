#include "query_runner.hpp"
#include "query_result.hpp"
#include "tsm_result.hpp"
#include "memory_store.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"
#include <map>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <optional>

#include <seastar/core/distributed.hh>


typedef std::chrono::high_resolution_clock Clock;

template <class T>
seastar::future<QueryResult<T>> QueryRunner::queryTsm(std::string series, uint64_t startTime, uint64_t endTime){
  LOG_QUERY_PATH(tsdb::query_log, debug, "QueryRunner: Querying TSM files for series={}, startTime={}, endTime={}", series, startTime, endTime);

  // Pre-allocate indexed slots to avoid concurrent push_back on a shared vector.
  // Each coroutine writes to its own slot, eliminating the race condition where
  // parallel_for_each coroutines could push_back concurrently after co_await points.
  std::vector<std::optional<TSMResult<T>>> tsmSlots(fileManager->sequencedTsmFiles.size());

  // First query TSM files
  size_t slotIdx = 0;
  co_await seastar::parallel_for_each(
    fileManager->sequencedTsmFiles.begin(),
    fileManager->sequencedTsmFiles.end(),
    [&tsmSlots, &series, startTime, endTime, &slotIdx] (std::pair<unsigned int, seastar::shared_ptr<TSM>> tsmTuple) -> seastar::future<> {
      // Assign index before any suspension point - safe in cooperative scheduling
      // because parallel_for_each invokes the lambda sequentially before yielding
      size_t myIdx = slotIdx++;
      auto [tsmRank, tsmFile] = tsmTuple;

      // Use queryWithTombstones to automatically filter out deleted data
      // Convert series key to SeriesId128
      SeriesId128 seriesId = SeriesId128::fromSeriesKey(series);
      TSMResult<T> results = co_await tsmFile.get()->queryWithTombstones<T>(seriesId, startTime, endTime);

      if(results.empty())
        co_return;

      results.sort();
      tsmSlots[myIdx] = std::move(results);
    }
  );

  // Collect non-empty results from the slots
  std::vector<TSMResult<T>> tsmResults;
  tsmResults.reserve(tsmSlots.size());
  for (auto& slot : tsmSlots) {
    if (slot.has_value()) {
      tsmResults.push_back(std::move(*slot));
    }
  }

  // Sort by rank in descending order
  std::sort(tsmResults.begin(), tsmResults.end(), [](const auto& lhs, const auto& rhs){
    return lhs.rank > rhs.rank;
  });

  // Now also query memory stores from WAL.
  // With background TSM conversion, multiple memory stores may hold data
  // for the same series, so we query all of them.
  QueryResult<T> result = QueryResult<T>::fromTsmResults(tsmResults);

  auto memoryMatches = walFileManager->queryAllMemoryStores<T>(series);
  for (const auto* memoryData : memoryMatches) {
    const auto& storeData = *memoryData;

    result.timestamps.reserve(result.timestamps.size() + storeData.timestamps.size());
    result.values.reserve(result.values.size() + storeData.values.size());

    for (size_t i = 0; i < storeData.timestamps.size(); ++i) {
      if (storeData.timestamps[i] >= startTime && storeData.timestamps[i] <= endTime) {
        result.timestamps.push_back(storeData.timestamps[i]);
        result.values.push_back(storeData.values[i]);
      }
    }
  }

  // Sort combined results by timestamp and deduplicate.
  // Deduplication is needed because during background TSM conversion, the same
  // data may exist in both a TSM file and an in-memory store briefly.
  if (!result.timestamps.empty()) {
    // Create index array
    std::vector<size_t> indices(result.timestamps.size());
    std::iota(indices.begin(), indices.end(), 0);

    // Sort indices by timestamp
    std::sort(indices.begin(), indices.end(),
      [&timestamps = result.timestamps](size_t a, size_t b) {
        return timestamps[a] < timestamps[b];
      });

    // Apply permutation and deduplicate by timestamp in one pass
    std::vector<uint64_t> sortedTimestamps;
    std::vector<T> sortedValues;
    sortedTimestamps.reserve(result.timestamps.size());
    sortedValues.reserve(result.values.size());

    for (size_t idx : indices) {
      // Skip duplicate timestamps (keep first occurrence, which is from TSM/higher priority)
      if (!sortedTimestamps.empty() && sortedTimestamps.back() == result.timestamps[idx]) {
        continue;
      }
      sortedTimestamps.push_back(result.timestamps[idx]);
      sortedValues.push_back(std::move(result.values[idx]));
    }

    result.timestamps = std::move(sortedTimestamps);
    result.values = std::move(sortedValues);
  }

  co_return std::move(result);
}



seastar::future<VariantQueryResult> QueryRunner::runQuery(std::string seriesKey, uint64_t startTime, uint64_t endTime){
  LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Running query for series='{}', startTime={}, endTime={}", seriesKey, startTime, endTime);
  
  // Get the type of the series
  LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Getting series type from WAL for series: '{}'", seriesKey);
  std::optional<TSMValueType> seriesType = walFileManager->getSeriesType(seriesKey);
  
  // std::cerr << "[QUERY_DEBUG] Series '" << seriesKey << "' type from WAL: " 
  //           << (seriesType.has_value() ? std::to_string(static_cast<int>(*seriesType)) : "none") << std::endl;

  if(!seriesType.has_value()) {
    LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Series type not found in WAL, trying TSM files for series: '{}'", seriesKey);
    seriesType = fileManager->getSeriesType(seriesKey);
  }
  
  // std::cerr << "[QUERY_DEBUG] Series '" << seriesKey << "' final type: " 
  //           << (seriesType.has_value() ? std::to_string(static_cast<int>(*seriesType)) : "NONE") << std::endl;

  // If there's no type, the series doesn't exist
  if(!seriesType.has_value()) {
    LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Series type not found anywhere for series: '{}' - series doesn't exist", seriesKey);
    // std::cerr << "[QUERY_DEBUG] Series '" << seriesKey << "' NOT FOUND!" << std::endl;
    throw std::runtime_error("Series not found");
  }
  
  LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Found series type: {} for series: '{}'", 
                 static_cast<int>(seriesType.value()), seriesKey);

  VariantQueryResult results;

  switch(seriesType.value()){
    case TSMValueType::Boolean:
      LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Querying boolean series: '{}'", seriesKey);
      results = co_await queryTsm<bool>(seriesKey, startTime, endTime);
      break;
    case TSMValueType::Float:
      LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Querying float series: '{}'", seriesKey);
      results = co_await queryTsm<double>(seriesKey, startTime, endTime);
      break;
    case TSMValueType::String:
      LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Querying string series: '{}'", seriesKey);
      results = co_await queryTsm<std::string>(seriesKey, startTime, endTime);
      break;
    default:
      LOG_QUERY_PATH(tsdb::query_log, debug, "[QUERYRUNNER] Unknown series type {} for series: '{}'", 
                     static_cast<int>(seriesType.value()), seriesKey);
      throw std::runtime_error("Unknown series type");
  }

  co_return std::move(results);
};

// Template instantiations
template seastar::future<QueryResult<bool>> QueryRunner::queryTsm<bool>(std::string series, uint64_t startTime, uint64_t endTime);
template seastar::future<QueryResult<double>> QueryRunner::queryTsm<double>(std::string series, uint64_t startTime, uint64_t endTime);
template seastar::future<QueryResult<std::string>> QueryRunner::queryTsm<std::string>(std::string series, uint64_t startTime, uint64_t endTime);
