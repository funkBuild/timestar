#ifndef __MEMORY_STORE_H_INCLUDED__
#define __MEMORY_STORE_H_INCLUDED__

#include <memory>
#include <seastar/core/coroutine.hh>
#include <unordered_map>
#include <variant>
#include <vector>

#include "logger.hpp"
#include "tsdb_value.hpp"
#include "tsm.hpp"
#include "wal.hpp"
#include "series_id.hpp"

class WAL;

template <class T> class InMemorySeries {
public:
  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  void insert(TSDBInsert<T> &insertRequest);
  void sort();
};

// Order of variants must match TSMValueType order
using VariantInMemorySeries =
    std::variant<InMemorySeries<double>, InMemorySeries<bool>,
                 InMemorySeries<std::string>>;

class MemoryStore {
private:
  std::unique_ptr<WAL> wal;
  bool closed = false;

public:
  // 16MB threshold for actual WAL file size on disk (uncompressed)
  static constexpr size_t WAL_SIZE_THRESHOLD = 64 * 1024 * 1024; // 16MB
  const unsigned int sequenceNumber;
  std::unordered_map<SeriesId128, VariantInMemorySeries, SeriesId128::Hash> series;

  MemoryStore(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {
    tsdb::memory_log.debug("Memory store {} created", sequenceNumber);
  };
  ~MemoryStore() {
    tsdb::memory_log.debug("Memory store {} removed", sequenceNumber);
  };

  seastar::future<> initWAL();
  seastar::future<> removeWAL();
  seastar::future<> initFromWAL(std::string filename);
  seastar::future<> close();
  template <class T> void insertMemory(TSDBInsert<T> &insertRequest);
  template <class T>
  seastar::future<bool>
  insert(TSDBInsert<T> &insertRequest); // Returns true if WAL needs rollover
  
  template <class T>
  seastar::future<bool>
  insertBatch(std::vector<TSDBInsert<T>> &insertRequests); // Batch insert - returns true if WAL needs rollover
  seastar::future<bool> isFull();
  template <class T> bool wouldExceedThreshold(TSDBInsert<T> &insertRequest);
  template <class T> bool wouldBatchExceedThreshold(std::vector<TSDBInsert<T>> &insertRequests);
  bool isClosed() { return closed; }
  bool isEmpty() { return series.size() == 0; }
  std::optional<TSMValueType> getSeriesType(const SeriesId128 &seriesId);
  WAL *getWAL() { return wal.get(); }

  // Query method to get data for a series
  template <class T>
  std::optional<InMemorySeries<T>> querySeries(const SeriesId128 &seriesId) {
    auto it = series.find(seriesId);
    if (it != series.end()) {
      return std::visit(
          [](auto &&arg) -> std::optional<InMemorySeries<T>> {
            using SeriesType = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<SeriesType, InMemorySeries<T>>) {
              return arg;
            }
            return std::nullopt;
          },
          it->second);
    }
    return std::nullopt;
  }

  // Delete data in a time range for a series
  void deleteRange(const SeriesId128 &seriesId, uint64_t startTime,
                   uint64_t endTime);
};

#endif
