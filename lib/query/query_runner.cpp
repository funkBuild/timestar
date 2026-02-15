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

#include <seastar/core/distributed.hh>


typedef std::chrono::high_resolution_clock Clock;

template <class T>
seastar::future<QueryResult<T>> QueryRunner::queryTsm(std::string series, uint64_t startTime, uint64_t endTime){
  LOG_QUERY_PATH(tsdb::query_log, debug, "QueryRunner: Querying TSM files for series={}, startTime={}, endTime={}", series, startTime, endTime);
  std::vector<TSMResult<T>> tsmResults;

  // Pre-allocate for expected TSM files to avoid reallocations
  tsmResults.reserve(fileManager->sequencedTsmFiles.size());

  // First query TSM files
  co_await seastar::parallel_for_each(
    fileManager->sequencedTsmFiles.begin(),
    fileManager->sequencedTsmFiles.end(),
    [&] (std::pair<unsigned int, seastar::shared_ptr<TSM>> tsmTuple) -> seastar::future<> {
      auto [tsmRank, tsmFile] = tsmTuple;

      // Use queryWithTombstones to automatically filter out deleted data
      // Convert series key to SeriesId128
      SeriesId128 seriesId = SeriesId128::fromSeriesKey(series);
      TSMResult<T> results = co_await tsmFile.get()->queryWithTombstones<T>(seriesId, startTime, endTime);

      if(results.empty())
        co_return;
      
      results.sort();
      tsmResults.push_back(std::move(results));
    }
  );

  // Sort by rank in descending order
  std::sort(tsmResults.begin(), tsmResults.end(), [](const auto& lhs, const auto& rhs){
    return lhs.rank > rhs.rank;
  });

  // Now also query memory stores from WAL
  QueryResult<T> result = QueryResult<T>::fromTsmResults(tsmResults);
  
  // Query memory stores - WAL replay ensures correct state without filtering
  const auto* memoryData = walFileManager->queryMemoryStores<T>(series);
  if (memoryData != nullptr) {
    // Filter by time range and add to results
    const auto& storeData = *memoryData;

    // Pre-allocate space to avoid reallocations
    result.timestamps.reserve(result.timestamps.size() + storeData.timestamps.size());
    result.values.reserve(result.values.size() + storeData.values.size());

    for (size_t i = 0; i < storeData.timestamps.size(); ++i) {
      if (storeData.timestamps[i] >= startTime && storeData.timestamps[i] <= endTime) {
        result.timestamps.push_back(storeData.timestamps[i]);
        result.values.push_back(storeData.values[i]);
      }
    }
  }

  // OPTIMIZATION: Sort combined results by timestamp using index-based sorting
  // This avoids creating a temporary vector of pairs, reducing allocations by half
  if (!result.timestamps.empty()) {
    // Create index array
    std::vector<size_t> indices(result.timestamps.size());
    std::iota(indices.begin(), indices.end(), 0);

    // Sort indices by timestamp - only sorts small integers, not the actual data
    std::sort(indices.begin(), indices.end(),
      [&timestamps = result.timestamps](size_t a, size_t b) {
        return timestamps[a] < timestamps[b];
      });

    // Apply permutation in-place using index array
    // This is more cache-friendly than the pair approach
    std::vector<uint64_t> sortedTimestamps;
    std::vector<T> sortedValues;
    sortedTimestamps.reserve(result.timestamps.size());
    sortedValues.reserve(result.values.size());

    for (size_t idx : indices) {
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
