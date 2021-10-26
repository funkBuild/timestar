#ifndef __QUERY_RUNNER_H_INCLUDED__
#define __QUERY_RUNNER_H_INCLUDED__

#include "tsm_file_manager.hpp"
#include "query_result.hpp"

#include <string>

class QueryRunner {
private:
  TSMFileManager *fileManager;

  template <class T>
  QueryResult<T> queryTsm(std::string series, uint64_t startTime, uint64_t endTime);
public:
  QueryRunner(TSMFileManager *_fileManager) : fileManager(_fileManager) {};

  VariantQueryResult runQuery(std::string series, uint64_t startTime, uint64_t endTime);
};

#endif
