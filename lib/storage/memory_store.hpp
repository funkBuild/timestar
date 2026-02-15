#ifndef MEMORY_STORE_H_INCLUDED
#define MEMORY_STORE_H_INCLUDED

#include <memory>
#include <seastar/core/coroutine.hh>
#include <variant>
#include <vector>
#include <tsl/robin_map.h>

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
  static constexpr size_t WAL_SIZE_THRESHOLD = 16 * 1024 * 1024; // 16MB
  const unsigned int sequenceNumber;
  // Use robin_map for O(1) lookups with better cache locality than std::unordered_map
  tsl::robin_map<SeriesId128, VariantInMemorySeries, SeriesId128::Hash> series;

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

  // Query method to get data for a series.
  // Returns a const pointer to avoid copying the entire series data.
  // Returns nullptr if the series is not found or has a different type.
  template <class T>
  const InMemorySeries<T>* querySeries(const SeriesId128 &seriesId) const {
    auto it = series.find(seriesId);
    if (it != series.end()) {
      return std::visit(
          [](const auto &arg) -> const InMemorySeries<T>* {
            using SeriesType = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<SeriesType, InMemorySeries<T>>) {
              return &arg;
            }
            return nullptr;
          },
          it->second);
    }
    return nullptr;
  }

  // Delete data in a time range for a series
  void deleteRange(const SeriesId128 &seriesId, uint64_t startTime,
                   uint64_t endTime);
};

#endif
