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
  
  // Track deleted time ranges for each series
  std::unordered_map<std::string, std::vector<std::pair<uint64_t, uint64_t>>> deletedRanges;

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
  
  // Delete data in a time range for a series
  void deleteRange(const std::string& seriesKey, uint64_t startTime, uint64_t endTime);
  
  // Query method that filters out deleted data
  template <class T>
  std::optional<InMemorySeries<T>> querySeriesFiltered(const std::string& seriesKey) {
    std::cerr << "[MEMORY_STORE_FILTER] querySeriesFiltered called for series: " << seriesKey << std::endl;
    
    auto result = querySeries<T>(seriesKey);
    if (!result.has_value()) {
      std::cerr << "[MEMORY_STORE_FILTER] No data found for series: " << seriesKey << std::endl;
      return std::nullopt;
    }
    
    std::cerr << "[MEMORY_STORE_FILTER] Found " << result.value().timestamps.size() 
              << " points for series: " << seriesKey << std::endl;
    
    // Check if there are any deleted ranges for this series
    auto deletedIt = deletedRanges.find(seriesKey);
    if (deletedIt == deletedRanges.end() || deletedIt->second.empty()) {
      std::cerr << "[MEMORY_STORE_FILTER] No deleted ranges for series: " << seriesKey << std::endl;
      return result; // No deletions, return as-is
    }
    
    std::cerr << "[MEMORY_STORE_FILTER] Found " << deletedIt->second.size() 
              << " deleted ranges for series: " << seriesKey << std::endl;
    for (const auto& [start, end] : deletedIt->second) {
      std::cerr << "[MEMORY_STORE_FILTER]   Range: [" << start << ", " << end << "]" << std::endl;
    }
    
    // Filter out deleted data
    InMemorySeries<T> filtered;
    const auto& original = result.value();
    const auto& delRanges = deletedIt->second;
    
    int deletedCount = 0;
    for (size_t i = 0; i < original.timestamps.size(); ++i) {
      uint64_t ts = original.timestamps[i];
      bool isDeleted = false;
      
      // Check if this timestamp falls in any deleted range
      for (const auto& [delStart, delEnd] : delRanges) {
        if (ts >= delStart && ts <= delEnd) {
          isDeleted = true;
          deletedCount++;
          break;
        }
      }
      
      if (!isDeleted) {
        filtered.timestamps.push_back(original.timestamps[i]);
        filtered.values.push_back(original.values[i]);
      }
    }
    
    std::cerr << "[MEMORY_STORE_FILTER] Filtered out " << deletedCount 
              << " deleted points, returning " << filtered.timestamps.size() 
              << " points for series: " << seriesKey << std::endl;
    
    // If all data was filtered out, return nullopt instead of empty series
    if (filtered.timestamps.empty()) {
      std::cerr << "[MEMORY_STORE_FILTER] All data filtered out, returning nullopt for series: " 
                << seriesKey << std::endl;
      return std::nullopt;
    }
    
    return filtered;
  }
};

#endif
