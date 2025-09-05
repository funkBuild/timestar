#ifndef __MEMORY_STORE_H_INCLUDED__
#define __MEMORY_STORE_H_INCLUDED__

#include <unordered_map>
#include <vector>
#include <memory>
#include <variant>
#include <seastar/core/coroutine.hh>

#include "tsdb_value.hpp"
#include "tsm.hpp"
#include "wal.hpp"
#include "logger.hpp"

class WAL;

template <class T>
class InMemorySeries {
public:
  std::vector<uint64_t> timestamps;
  std::vector<T> values;

  void insert(TSDBInsert<T> &insertRequest);
  void sort();
};

// Order of variants must match TSMValueType order
using VariantInMemorySeries = std::variant<InMemorySeries<double>, InMemorySeries<bool>, InMemorySeries<std::string>>;

class MemoryStore {
private:
  std::unique_ptr<WAL> wal;
  bool closed = false;
  // 16MB threshold for actual WAL file size on disk (uncompressed)
  static constexpr size_t WAL_SIZE_THRESHOLD = 16 * 1024 * 1024; // 16MB

public:
  const unsigned int sequenceNumber;
  std::unordered_map<std::string, VariantInMemorySeries> series;


  MemoryStore(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {
    tsdb::memory_log.debug("Memory store {} created", sequenceNumber);
  };
  ~MemoryStore() { tsdb::memory_log.debug("Memory store {} removed", sequenceNumber); };

  seastar::future<> initWAL();
  seastar::future<> removeWAL();
  seastar::future<> initFromWAL(std::string filename);
  seastar::future<> close();
  template <class T>
  void insertMemory(TSDBInsert<T> &insertRequest);
  template <class T>
  seastar::future<bool> insert(TSDBInsert<T> &insertRequest); // Returns true if WAL needs rollover
  seastar::future<bool> isFull();
  template <class T>
  bool wouldExceedThreshold(TSDBInsert<T> &insertRequest);
  bool isClosed() { return closed; }
  bool isEmpty(){ return series.size() == 0; }
  std::optional<TSMValueType> getSeriesType(std::string &seriesKey);
  WAL* getWAL() { return wal.get(); }
  
  // Query method to get data for a series
  template <class T>
  std::optional<InMemorySeries<T>> querySeries(const std::string& seriesKey) {
    auto it = series.find(seriesKey);
    if (it != series.end()) {
      return std::visit([](auto&& arg) -> std::optional<InMemorySeries<T>> {
        using SeriesType = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<SeriesType, InMemorySeries<T>>) {
          return arg;
        }
        return std::nullopt;
      }, it->second);
    }
    return std::nullopt;
  }
};

#endif
