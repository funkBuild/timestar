#ifndef QUERY_RUNNER_H_INCLUDED
#define QUERY_RUNNER_H_INCLUDED

#include "tsm_file_manager.hpp"
#include "wal_file_manager.hpp"
#include "query_result.hpp"

#include <seastar/core/coroutine.hh>

#include <string>

class QueryRunner {
private:
  TSMFileManager *fileManager;
  WALFileManager *walFileManager;

  template <class T>
  seastar::future<QueryResult<T>> queryTsm(std::string series, uint64_t startTime, uint64_t endTime);
public:
  QueryRunner(TSMFileManager* _fileManager, WALFileManager* _walFileManager) : fileManager(_fileManager), walFileManager(_walFileManager) {};

  seastar::future<VariantQueryResult> runQuery(std::string series, uint64_t startTime, uint64_t endTime);
};

#endif
