#include "query_runner.hpp"
#include "query_result.hpp"

#include <iostream>

#include <chrono>


typedef std::chrono::high_resolution_clock Clock;

template <class T>
QueryResult<T> QueryRunner::queryTsm(std::string series, uint64_t startTime, uint64_t endTime){
  QueryResult<T> results;

  for (auto const &[seqNum, tsmFile] : fileManager->sequencedTsmFiles){
    QueryResult<T> tsmResults;
    tsmFile.get()->readSeries(series, startTime, endTime, tsmResults);
    results.mergeQueryResult(tsmResults);
  }

  return std::move(results);
}

VariantQueryResult QueryRunner::runQuery(std::string series, uint64_t startTime, uint64_t endTime){
  // Get the type of the series
  std::optional<TSMValueType> seriesType;
  for (auto const &[seqNum, tsmFile] : fileManager->sequencedTsmFiles){
    seriesType = tsmFile.get()->getSeriesType(series);

    if(seriesType)
      break;
  }

  // If there's no type, the series doesn't exist
  if(!seriesType)
    throw std::runtime_error("Series not found");

  VariantQueryResult results;

  switch(seriesType.value()){
    case TSMValueType::Boolean:
      results = queryTsm<bool>(series, startTime, endTime);
      break;
    case TSMValueType::Float:
      results = queryTsm<double>(series, startTime, endTime);
      break;
  }

  return std::move(results);
};
