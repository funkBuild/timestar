#include "wal_file_manager.hpp"
#include "engine.hpp"
#include "logger.hpp"
#include "logging_config.hpp"

#include <seastar/core/reactor.hh>

#include <filesystem>
#include <vector>
#include <algorithm>

namespace fs = std::filesystem;

WALFileManager::WALFileManager() : pendingWrites(100) {
  shardId = seastar::this_shard_id();
}

seastar::future<> WALFileManager::init(Engine &engine,
                                       TSMFileManager &_tsmFileManager) {
  tsmFileManager = &_tsmFileManager;

  // Search for existing WAL's
  std::string path = engine.basePath() + '/'; // TODO: Get WAL path from config

  std::vector<std::string> walFiles;

  if (fs::exists(path)) {
    for (const auto &entry : fs::directory_iterator(path)) {
      if (endsWith(entry.path(), ".wal"))
        walFiles.push_back(entry.path());
    }
  }

  if (!walFiles.empty()) {
    tsdb::wal_log.info("Found {} existing WAL files on shard {} - converting to TSM", 
                       walFiles.size(), shardId);
  }

  // Convert them to TSM's if they exist and are closed
  for (const auto &walFilename : walFiles) {
    size_t filenameEndIndex = walFilename.find_last_of(".");
    size_t filenameStartIndex = walFilename.find_last_of("/") + 1;

    int seqNum = std::stoi(walFilename.substr(
        filenameStartIndex, filenameEndIndex - filenameStartIndex));

    if (seqNum > currentWalSequenceNumber)
      currentWalSequenceNumber = seqNum;

    seastar::shared_ptr store = seastar::make_shared<MemoryStore>(seqNum);
    co_await store->initFromWAL(walFilename);

    // Write to TSM
    // NOTE: We always have to write the WAL to TSM since WAL's can't be resumed due to make_file_output_stream 
    if (!store->isEmpty()) {
      tsdb::wal_log.info("Writing memory store {} to TSM", seqNum);
      co_await convertWalToTsm(store);
    }

    co_await store->removeWAL();
  }

  if(memoryStores.size() == 0){
    seastar::shared_ptr store =
        seastar::make_shared<MemoryStore>(++currentWalSequenceNumber);
    co_await store->initWAL();
    memoryStores.push_back(store);
  }

  tsdb::wal_log.info("WAL file manager initialization complete");
}

template <class T>
seastar::future<> WALFileManager::insert(TSDBInsert<T> &insertRequest) {
  LOG_INSERT_PATH(tsdb::wal_log, debug, "[WAL] Insert called for series: '{}', {} values", 
                  insertRequest.seriesKey(), insertRequest.values.size());
  
  // Ensure we have at least one memory store
  if (memoryStores.empty()) {
    throw std::runtime_error("No memory stores available for insert");
  }
  
  LOG_INSERT_PATH(tsdb::wal_log, debug, "[WAL] Inserting into memory store for series: '{}'", insertRequest.seriesKey());
  // Try to insert - returns true if rollover is needed
  bool needsRollover = co_await memoryStores[0]->insert(insertRequest);
  
  if (needsRollover) {
    LOG_INSERT_PATH(tsdb::wal_log, debug, "[WAL] Memory store rollover needed for series: '{}'", insertRequest.seriesKey());
    // Rollover the WAL
    co_await rolloverMemoryStore();
    
    // Now retry the insert with the new memory store
    bool retryResult = co_await memoryStores[0]->insert(insertRequest);
    if (retryResult) {
      // This should not happen - the new WAL should have room
      throw std::runtime_error("New WAL immediately full - insert too large");
    }
  }
}

seastar::future<> WALFileManager::rolloverMemoryStore() {
  // Use compaction semaphore to ensure only one compaction at a time
  auto units = co_await seastar::get_units(compactionSemaphore, 1);

  bool isFull = co_await memoryStores[0]->isFull();
  if (!isFull)
    co_return;

  auto previousStore = memoryStores[0];
  tsdb::wal_log.info("Memory store {} full (16MB threshold reached), rolling over", previousStore->sequenceNumber);
  co_await previousStore->close();

  auto store = seastar::make_shared<MemoryStore>(++currentWalSequenceNumber);
  co_await store->initWAL();
  memoryStores.insert(memoryStores.begin(), store);

  // Send the closed store to the background task to write it to a TSM (level 0)
  co_await pendingWrites.writer.write(std::move(previousStore));
}

seastar::future<> WALFileManager::convertWalToTsm(seastar::shared_ptr<MemoryStore> store) {
    tsdb::wal_log.info("Converting WAL {} to TSM on shard {}", store->sequenceNumber, shardId);
    co_await tsmFileManager->writeMemstore(store);
    co_await store->removeWAL();
    tsdb::wal_log.info("Successfully converted WAL {} to TSM on shard {}", store->sequenceNumber, shardId);

    // Only remove from memoryStores if it's actually in there
    // During initialization, temporary stores are not in the vector
    auto it = std::find(memoryStores.begin(), memoryStores.end(), store);
    if (it != memoryStores.end()) {
        memoryStores.erase(it);
    }
}

seastar::future<> WALFileManager::startTsmWriter() {
  tsdb::wal_log.info("Background TSM writer started on shard {}", shardId);
  
  for(;;){
    std::optional<seastar::shared_ptr<MemoryStore>> pipeStore = co_await pendingWrites.reader.read();

    if(!pipeStore.has_value() || pipeStore.value().use_count() == 0) {
      tsdb::wal_log.info("Background TSM writer stopping on shard {}", shardId);
      break;
    }

    co_await convertWalToTsm(pipeStore.value());
  }
}

std::optional<TSMValueType> WALFileManager::getSeriesType(std::string &seriesKey){
  std::optional<TSMValueType> seriesType;

  for (auto const &memoryStore : memoryStores){
    seriesType = memoryStore.get()->getSeriesType(seriesKey);

    if(seriesType.has_value())
      return seriesType;
  }

  seriesType.reset();
  return seriesType;
}

seastar::future<> WALFileManager::deleteFromMemoryStores(const std::string& seriesKey,
                                                        uint64_t startTime,
                                                        uint64_t endTime) {
  // Write deletion to WAL for durability
  if (!memoryStores.empty() && memoryStores[0]) {
    // Get the current WAL from the first memory store
    auto wal = memoryStores[0]->getWAL();
    if (wal) {
      co_await wal->deleteRange(seriesKey, startTime, endTime);
      tsdb::wal_log.debug("Wrote deleteRange to WAL: series={}, startTime={}, endTime={}",
                         seriesKey, startTime, endTime);
    }
  }
  
  // TODO: In a full implementation, we would also mark these ranges as deleted
  // in the memory stores so queries filter them out properly
  // For now, this just ensures the deletion is persisted to WAL
  
  co_return;
}

template seastar::future<>
WALFileManager::insert<bool>(TSDBInsert<bool> &insertRequest);
template seastar::future<>
WALFileManager::insert<double>(TSDBInsert<double> &insertRequest);
template seastar::future<>
WALFileManager::insert<std::string>(TSDBInsert<std::string> &insertRequest);