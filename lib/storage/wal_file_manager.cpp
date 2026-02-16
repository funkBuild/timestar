#include "wal_file_manager.hpp"
#include "engine.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"

#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <vector>

namespace fs = std::filesystem;

WALFileManager::WALFileManager() {
  shardId = seastar::this_shard_id();
}

seastar::future<> WALFileManager::init(Engine &engine,
                                       TSMFileManager &_tsmFileManager) {
  tsdb::wal_log.info("WALFileManager::init starting for shard {}", shardId);
  tsmFileManager = &_tsmFileManager;

  // Search for existing WAL's
  std::string path = engine.basePath() + '/'; // TODO: Get WAL path from config
  tsdb::wal_log.debug("Scanning for WAL files in {} on shard {}", path,
                      shardId);

  std::vector<std::string> walFiles;

  // Wrap blocking std::filesystem calls in seastar::async to avoid
  // blocking the Seastar reactor thread (important for NFS / high I/O).
  walFiles = co_await seastar::async([&path]() {
    std::vector<std::string> files;
    if (fs::exists(path)) {
      for (const auto &entry : fs::directory_iterator(path)) {
        if (endsWith(entry.path(), ".wal"))
          files.push_back(entry.path());
      }
    }
    return files;
  });

  if (!walFiles.empty()) {
    tsdb::wal_log.info(
        "Found {} existing WAL files on shard {} - converting to TSM",
        walFiles.size(), shardId);

    // Log system memory before recovery
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo.is_open()) {
      std::string line;
      while (std::getline(meminfo, line)) {
        if (line.find("MemFree:") != std::string::npos ||
            line.find("MemAvailable:") != std::string::npos) {
          tsdb::wal_log.info("Before WAL recovery - {}", line);
        }
      }
      meminfo.close();
    }
  }

  // Convert them to TSM's if they exist and are closed
  for (const auto &walFilename : walFiles) {
    size_t filenameEndIndex = walFilename.find_last_of(".");
    size_t filenameStartIndex = walFilename.find_last_of("/") + 1;

    int seqNum = std::stoi(walFilename.substr(
        filenameStartIndex, filenameEndIndex - filenameStartIndex));

    if (seqNum > currentWalSequenceNumber)
      currentWalSequenceNumber = seqNum;

    tsdb::wal_log.debug(
        "Creating recovery store for WAL sequence {} on shard {}", seqNum,
        shardId);
    seastar::shared_ptr store = seastar::make_shared<MemoryStore>(seqNum);
    tsdb::wal_log.debug("Reading WAL file {} on shard {}", walFilename,
                        shardId);
    co_await store->initFromWAL(walFilename);
    tsdb::wal_log.debug("WAL recovery complete for sequence {} on shard {}",
                        seqNum, shardId);

    // Write to TSM if there's data
    // NOTE: We always have to write the WAL to TSM since WAL's can't be resumed
    // due to make_file_output_stream
    bool conversionSucceeded = false;
    if (!store->isEmpty()) {
      tsdb::wal_log.info("Writing memory store {} to TSM on shard {}", seqNum,
                         shardId);
      try {
        co_await convertWalToTsm(store);
        tsdb::wal_log.info("Successfully converted WAL {} to TSM on shard {}",
                           seqNum, shardId);
        conversionSucceeded = true;
      } catch (const std::bad_alloc &e) {
        tsdb::wal_log.error(
            "Failed to convert WAL {} to TSM on shard {} - bad_alloc", seqNum,
            shardId);
        // Log memory info
        std::ifstream meminfo("/proc/meminfo");
        if (meminfo.is_open()) {
          std::string line;
          while (std::getline(meminfo, line)) {
            if (line.find("MemFree:") != std::string::npos ||
                line.find("MemAvailable:") != std::string::npos) {
              tsdb::wal_log.error("System memory at failure: {}", line);
            }
          }
          meminfo.close();
        }
        throw;
      } catch (const std::exception &e) {
        tsdb::wal_log.error("Failed to convert WAL {} to TSM on shard {}: {}",
                            seqNum, shardId, e.what());
        // Preserve WAL file so it can be recovered on next restart
        tsdb::wal_log.warn(
            "Preserving WAL file {} for recovery on next restart",
            walFilename);
      }
    } else {
      tsdb::wal_log.info(
          "WAL {} is empty, removing without creating TSM on shard {}", seqNum,
          shardId);
      conversionSucceeded = true; // Empty WAL, safe to remove
    }

    // Only remove WAL file if conversion succeeded or store was empty
    if (conversionSucceeded) {
      co_await seastar::remove_file(walFilename);
    }

    // Explicitly release the temporary memory store to free resources
    store = nullptr;
  }

  if (memoryStores.size() == 0) {
    seastar::shared_ptr store =
        seastar::make_shared<MemoryStore>(++currentWalSequenceNumber);
    co_await store->initWAL();
    memoryStores.push_back(store);
  }

  tsdb::wal_log.info("WAL file manager initialization complete for shard {}",
                     shardId);

  // Log system memory after recovery
  if (!walFiles.empty()) {
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo.is_open()) {
      std::string line;
      while (std::getline(meminfo, line)) {
        if (line.find("MemFree:") != std::string::npos ||
            line.find("MemAvailable:") != std::string::npos) {
          tsdb::wal_log.info("After WAL recovery - {}", line);
        }
      }
      meminfo.close();
    }
  }
}

template <class T>
seastar::future<> WALFileManager::insert(TSDBInsert<T> &insertRequest) {
  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "[WAL] Insert called for series: '{}', {} values",
                  insertRequest.seriesKey(), insertRequest.values.size());

  // Ensure we have at least one memory store
  if (memoryStores.empty()) {
    throw std::runtime_error("No memory stores available for insert");
  }

  // First, estimate the size of this insert to check if it exceeds 16MB
  if (memoryStores[0] && memoryStores[0]->getWAL()) {
    size_t estimatedSize =
        memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
    if (estimatedSize > MemoryStore::WAL_SIZE_THRESHOLD) {
      // This single insert exceeds the entire 16MB WAL limit
      tsdb::wal_log.error(
          "Insert request of {} bytes exceeds maximum WAL size of {} bytes",
          estimatedSize, MemoryStore::WAL_SIZE_THRESHOLD);
      throw std::runtime_error("Insert batch too large - requested " +
                               std::to_string(estimatedSize) +
                               " bytes, exceeds 16MB WAL "
                               "limit. Please reduce batch size.");
    }
  }

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "[WAL] Inserting into memory store for series: '{}'",
                  insertRequest.seriesKey());
  // Try to insert - returns true if rollover is needed
  bool needsRollover = co_await memoryStores[0]->insert(insertRequest);

  if (needsRollover) {
    LOG_INSERT_PATH(tsdb::wal_log, debug,
                    "[WAL] Memory store rollover needed for series: '{}'",
                    insertRequest.seriesKey());
    // Rollover the WAL
    co_await rolloverMemoryStore();

    // Now retry the insert with the new memory store
    bool retryResult = co_await memoryStores[0]->insert(insertRequest);
    if (retryResult) {
      // The insert still doesn't fit in a fresh WAL - it's too large
      size_t estimatedSize =
          memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
      tsdb::wal_log.error(
          "Insert batch of {} bytes too large for fresh 16MB WAL",
          estimatedSize);
      throw std::runtime_error("Insert batch too large - requested " +
                               std::to_string(estimatedSize) +
                               " bytes, exceeds 16MB WAL "
                               "limit. Please reduce batch size.");
    }
  }
}

template <class T>
seastar::future<>
WALFileManager::insertBatch(std::vector<TSDBInsert<T>> &insertRequests) {
  if (insertRequests.empty()) {
    co_return; // No work to do
  }

  auto start_wal_batch = std::chrono::high_resolution_clock::now();
  LOG_INSERT_PATH(tsdb::wal_log, info,
                  "[PERF] [WAL] Batch insert started for {} requests",
                  insertRequests.size());

  // Ensure we have at least one memory store
  if (memoryStores.empty()) {
    throw std::runtime_error("No memory stores available for batch insert");
  }

  // First, estimate the total size of this batch to check if it exceeds
  // threshold
  if (memoryStores[0] && memoryStores[0]->getWAL()) {
    size_t totalEstimatedSize = 0;
    for (auto &insertRequest : insertRequests) {
      totalEstimatedSize +=
          memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
    }

    if (totalEstimatedSize > MemoryStore::WAL_SIZE_THRESHOLD) {
      // This batch exceeds the entire WAL limit
      tsdb::wal_log.error("Batch insert request of {} bytes exceeds maximum "
                          "WAL size of {} bytes",
                          totalEstimatedSize, MemoryStore::WAL_SIZE_THRESHOLD);
      throw std::runtime_error(
          "Insert batch too large - requested " +
          std::to_string(totalEstimatedSize) +
          " bytes, exceeds WAL limit. Please reduce batch size.");
    }
  }

  LOG_INSERT_PATH(tsdb::wal_log, debug,
                  "[WAL] Inserting batch into memory store for {} requests",
                  insertRequests.size());

  auto start_memory_batch = std::chrono::high_resolution_clock::now();
  // Try to insert batch - returns true if rollover is needed
  bool needsRollover = co_await memoryStores[0]->insertBatch(insertRequests);
  auto end_memory_batch = std::chrono::high_resolution_clock::now();

  if (needsRollover) {
    LOG_INSERT_PATH(
        tsdb::wal_log, debug,
        "[WAL] Memory store rollover needed for batch of {} requests",
        insertRequests.size());
    // Rollover the WAL
    co_await rolloverMemoryStore();

    // Now retry the batch insert with the new memory store
    bool retryResult = co_await memoryStores[0]->insertBatch(insertRequests);
    if (retryResult) {
      // The batch still doesn't fit in a fresh WAL - it's too large
      size_t totalEstimatedSize = 0;
      for (auto &insertRequest : insertRequests) {
        totalEstimatedSize +=
            memoryStores[0]->getWAL()->estimateInsertSize(insertRequest);
      }

      tsdb::wal_log.error("Batch insert of {} bytes too large for fresh WAL",
                          totalEstimatedSize);
      throw std::runtime_error(
          "Insert batch too large - requested " +
          std::to_string(totalEstimatedSize) +
          " bytes, exceeds WAL limit. Please reduce batch size.");
    }
  }

  auto end_wal_batch = std::chrono::high_resolution_clock::now();
  auto memory_batch_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end_memory_batch -
                                                            start_memory_batch);
  auto wal_batch_duration =
      std::chrono::duration_cast<std::chrono::microseconds>(end_wal_batch -
                                                            start_wal_batch);

  LOG_INSERT_PATH(tsdb::wal_log, info, "[PERF] [WAL] Memory batch insert: {}μs",
                  memory_batch_duration.count());
  LOG_INSERT_PATH(tsdb::wal_log, info, "[PERF] [WAL] Total batch insert: {}μs",
                  wal_batch_duration.count());
}

seastar::future<> WALFileManager::rolloverMemoryStore() {
  // Use compaction semaphore to ensure only one compaction at a time
  auto units = co_await seastar::get_units(compactionSemaphore, 1);

  // Don't check isFull() here - if rolloverMemoryStore() was called,
  // it means wouldExceedThreshold() returned true, so we need to rollover.
  // The isFull() check could return false if the WAL is just below the
  // threshold, causing the rollover to be skipped and the insert to fail.
  // Removing this check ensures consistency with the threshold logic.

  auto previousStore = memoryStores[0];
  tsdb::wal_log.info(
      "Memory store {} full (16MB threshold reached), rolling over",
      previousStore->sequenceNumber);

  // Create and init the new store FIRST, before closing the old one.
  // This ensures memoryStores[0] always points to an open store,
  // even if another insert coroutine runs during a co_await yield.
  auto store = seastar::make_shared<MemoryStore>(++currentWalSequenceNumber);
  co_await store->initWAL();
  memoryStores.insert(memoryStores.begin(), store);

  tsdb::wal_log.info("New memory store {} created for shard {}",
                     store->sequenceNumber, shardId);

  // Do NOT close the old store here — in-flight writes may still be
  // completing on its WAL (holding _io_gate, waiting on _io_sem).
  // The background conversion task closes the store after all pending
  // I/O drains via WAL::close() -> _io_gate.close().

  // Launch TSM conversion as a background task so writes are not blocked.
  // The gate tracks the in-flight conversion; close() drains it at shutdown.
  // The conversion semaphore serializes conversions to prevent compaction races
  // (writeMemstore triggers inline compaction which is not reentrant).
  if (!previousStore->isEmpty()) {
    auto sid = shardId;
    auto seqNum = previousStore->sequenceNumber;
    _backgroundGate.enter();
    // Acquire the semaphore to serialize with other conversions, then
    // close the store (drains any in-flight WAL writes) and convert.
    // The chained .finally() releases the gate when the task completes.
    (void)seastar::get_units(_conversionSemaphore, 1).then(
        [this, store = previousStore](auto units) {
          return store->close().then([this, store, units = std::move(units)]() mutable {
            return convertWalToTsm(store).finally([units = std::move(units)] {});
          });
        }).handle_exception([sid, seqNum](auto ep) {
          try {
            std::rethrow_exception(ep);
          } catch (const std::exception& e) {
            tsdb::wal_log.error(
                "[BG_CONVERT] Background TSM conversion failed for store {} on shard {}: {}",
                seqNum, sid, e.what());
          }
        }).finally([this] {
          _backgroundGate.leave();
        });
  } else {
    // Empty store — close and remove the WAL file
    co_await previousStore->close();
    co_await previousStore->removeWAL();
    auto it = std::find(memoryStores.begin(), memoryStores.end(), previousStore);
    if (it != memoryStores.end()) {
      memoryStores.erase(it);
    }
  }

  tsdb::wal_log.info(
      "Rollover complete, new memory store {} created for shard {}",
      store->sequenceNumber, shardId);
}

seastar::future<>
WALFileManager::convertWalToTsm(seastar::shared_ptr<MemoryStore> store) {
  tsdb::wal_log.info(
      "[CONVERT_WAL_TO_TSM] Starting conversion of WAL {} to TSM on shard {}",
      store->sequenceNumber, shardId);

  // Count total data points in memory store for debugging
  size_t totalPoints = 0;
  size_t totalSeries = store->series.size();
  size_t totalMemoryEstimate = 0;
  size_t largestSeriesPoints = 0;
  std::string largestSeriesKey;

  for (const auto &[key, series] : store->series) {
    size_t seriesPoints =
        std::visit([](const auto &s) { return s.timestamps.size(); }, series);
    totalPoints += seriesPoints;

    if (seriesPoints > largestSeriesPoints) {
      largestSeriesPoints = seriesPoints;
      largestSeriesKey = key.toHex();
    }

    // Estimate memory usage for this series
    size_t seriesMemory = std::visit(
        [&seriesPoints](const auto &s) -> size_t {
          using T = typename std::decay_t<decltype(s.values)>::value_type;
          size_t mem = seriesPoints * sizeof(uint64_t); // timestamps
          if constexpr (std::is_same_v<T, double>) {
            mem += seriesPoints * sizeof(double);
          } else if constexpr (std::is_same_v<T, bool>) {
            mem += seriesPoints * sizeof(bool);
          } else if constexpr (std::is_same_v<T, std::string>) {
            // Estimate string memory
            size_t stringMem = 0;
            for (const auto &str : s.values) {
              stringMem += str.size() + sizeof(std::string);
            }
            mem += stringMem;
          }
          return mem;
        },
        series);
    totalMemoryEstimate += seriesMemory;

    // Warn about large series
    if (seriesPoints > 100000) {
      tsdb::wal_log.warn("[LARGE_SERIES] Series '{}' with {} points (~{} MB) "
                         "in memory store {}",
                         key, seriesPoints, seriesMemory / (1024 * 1024),
                         store->sequenceNumber);
    }
  }

  tsdb::wal_log.info("[MEMORY_STORE_STATS] Store {} on shard {}: {} series, {} "
                     "total points, ~{} MB estimated memory",
                     store->sequenceNumber, shardId, totalSeries, totalPoints,
                     totalMemoryEstimate / (1024 * 1024));
  tsdb::wal_log.info("[LARGEST_SERIES] Largest series: '{}' with {} points",
                     largestSeriesKey, largestSeriesPoints);

  try {
    tsdb::wal_log.debug(
        "[TSM_WRITE_START] Calling tsmFileManager->writeMemstore for store {} "
        "on shard {}",
        store->sequenceNumber, shardId);
    co_await tsmFileManager->writeMemstore(store);
    tsdb::wal_log.debug(
        "[TSM_WRITE_SUCCESS] Successfully wrote TSM for store {} on shard {}",
        store->sequenceNumber, shardId);
  } catch (const std::bad_alloc &e) {
    tsdb::wal_log.error("[BAD_ALLOC] Memory allocation failed when writing TSM "
                        "for store {} on shard {}",
                        store->sequenceNumber, shardId);
    tsdb::wal_log.error("[BAD_ALLOC] Stats: {} series, {} points, ~{} MB "
                        "estimated, largest series: '{}' ({} points)",
                        totalSeries, totalPoints,
                        totalMemoryEstimate / (1024 * 1024), largestSeriesKey,
                        largestSeriesPoints);

    // Log system memory info if available
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo.is_open()) {
      std::string line;
      while (std::getline(meminfo, line)) {
        if (line.find("MemFree:") != std::string::npos ||
            line.find("MemAvailable:") != std::string::npos ||
            line.find("MemTotal:") != std::string::npos) {
          tsdb::wal_log.error("[SYSTEM_MEMORY] {}", line);
        }
      }
      meminfo.close();
    }
    throw;
  } catch (const std::exception &e) {
    tsdb::wal_log.error(
        "[TSM_WRITE_ERROR] Failed to write TSM for store {} on shard {}: {}",
        store->sequenceNumber, shardId, e.what());
    throw;
  }

  // Remove from memoryStores immediately after successful TSM write,
  // BEFORE the removeWAL yield point, to prevent duplicate query results.
  // The shared_ptr `store` keeps data alive for the removeWAL call below.
  auto it = std::find(memoryStores.begin(), memoryStores.end(), store);
  if (it != memoryStores.end()) {
    memoryStores.erase(it);
  }

  co_await store->removeWAL();
  tsdb::wal_log.info("Successfully converted WAL {} to TSM on shard {}",
                     store->sequenceNumber, shardId);
}


std::optional<TSMValueType>
WALFileManager::getSeriesType(const std::string &seriesKey) {
  std::optional<TSMValueType> seriesType;
  SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);

  for (auto const &memoryStore : memoryStores) {
    seriesType = memoryStore.get()->getSeriesType(seriesId);

    if (seriesType.has_value())
      return seriesType;
  }

  seriesType.reset();
  return seriesType;
}

seastar::future<>
WALFileManager::deleteFromMemoryStores(const std::string &seriesKey,
                                       uint64_t startTime, uint64_t endTime) {
  // Check if the series exists in any memory store before writing to WAL
  bool seriesExists = false;
  for (const auto &memStore : memoryStores) {
    auto seriesType =
        memStore->getSeriesType(SeriesId128::fromSeriesKey(seriesKey));
    if (seriesType.has_value()) {
      seriesExists = true;
      break;
    }
  }

  // Write deletion to WAL and apply to memory stores
  // We always write to WAL to ensure deletions are persisted
  if (!memoryStores.empty() && memoryStores[0]) {
    // Get the current WAL from the first memory store
    auto wal = memoryStores[0]->getWAL();
    if (wal) {
      SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
      co_await wal->deleteRange(seriesId, startTime, endTime);
      tsdb::wal_log.debug(
          "Wrote deleteRange to WAL: series={}, startTime={}, endTime={}",
          seriesKey, startTime, endTime);
    }
  }

  // Apply deletion to all memory stores
  // This ensures tombstones are applied even if data arrives later
  for (auto &memStore : memoryStores) {
    memStore->deleteRange(SeriesId128::fromSeriesKey(seriesKey), startTime,
                          endTime);
  }

  if (!seriesExists) {
    tsdb::wal_log.debug(
        "Series '{}' not found in memory stores but deletion recorded",
        seriesKey);
  }

  tsdb::wal_log.debug("Applied deleteRange to {} memory stores",
                      memoryStores.size());

  co_return;
}

template seastar::future<>
WALFileManager::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<>
WALFileManager::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<>
WALFileManager::insert<std::string>(TSDBInsert<std::string> &insertRequest);

template seastar::future<> WALFileManager::insertBatch<bool>(
    std::vector<TSDBInsert<bool>> &insertRequests);
template seastar::future<> WALFileManager::insertBatch<double>(
    std::vector<TSDBInsert<double>> &insertRequests);
template seastar::future<> WALFileManager::insertBatch<std::string>(
    std::vector<TSDBInsert<std::string>> &insertRequests);