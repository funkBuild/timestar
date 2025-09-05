#include "tsm_file_manager.hpp"
#include "tsm_compactor.hpp"
#include "tsm_writer.hpp"
#include "util.hpp"
#include "logger.hpp"

#include <seastar/core/thread.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>

#include <filesystem>

namespace fs = std::filesystem;

TSMFileManager::TSMFileManager(){
  shardId = seastar::this_shard_id();
}

std::string TSMFileManager::basePath(){
  return std::string("shard_" + std::to_string(shardId) + "/tsm/");
}

seastar::future<> TSMFileManager::init(){
  tsdb::tsm_log.info("TSMFileManager init. shardId={}", shardId);
  
  // Initialize compactor
  compactor = std::make_shared<TSMCompactor>(this);
  
  // Scan the TSM folder for files if it exists
  if (fs::exists(basePath())) {
    for (const auto &entry : fs::directory_iterator(basePath()))
    {
      if (endsWith(entry.path(), ".tsm")){
        std::string absolutePath = fs::canonical(fs::absolute(entry.path()));
        co_await openTsmFile(absolutePath);
      }
    }
  }
}

seastar::future<> TSMFileManager::openTsmFile(std::string path){
  tsdb::tsm_log.debug("Opening TSM file: {}", path);

  try {
    seastar::shared_ptr<TSM> tsmFile = seastar::make_shared<TSM>(path);
    co_await tsmFile->open();

    tsmFiles.push_back(tsmFile);
    
    // Add to tier tracking
    uint64_t tier = tsmFile->tierNum;
    if(tier < MAX_TIERS) {
      tiers[tier].push_back(tsmFile);
    }

    unsigned int tsmSeqNum = tsmFile.get()->rankAsInteger();

    // TODO: Handle conflicting sequence number, or fail loudly
    sequencedTsmFiles.insert({tsmSeqNum, tsmFile });

    if(tsmFile->seqNum >= nextSequenceId){
      nextSequenceId = tsmFile->seqNum + 1;
    }
  } catch(const std::runtime_error&) {
    co_return;
  }
}

seastar::future<> TSMFileManager::writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier){
  auto seqNum = nextSequenceId++;

  std::string filename = "shard_" + std::to_string(shardId) + "/tsm/" + std::to_string(tier) + "_" + std::to_string(seqNum) + ".tsm";

  // TODO: Convert to coroutine using Seastar file accesses
  TSMWriter::run(memStore, filename);
  co_await openTsmFile(filename);
  
  // Check if this tier needs compaction after adding the new file
  co_await checkAndTriggerCompaction();

  co_return;
}

std::optional<TSMValueType> TSMFileManager::getSeriesType(std::string &seriesKey){
  std::optional<TSMValueType> seriesType;

  for (auto const &[seqNum, tsmFile] : sequencedTsmFiles){
    seriesType = tsmFile.get()->getSeriesType(seriesKey);

    if(seriesType)
      return seriesType;
  }

  return seriesType;
}

std::vector<seastar::shared_ptr<TSM>> TSMFileManager::getFilesInTier(uint64_t tier) const {
  if(tier >= MAX_TIERS) {
    return {};
  }
  return tiers[tier];
}

size_t TSMFileManager::getFileCountInTier(uint64_t tier) const {
  if(tier >= MAX_TIERS) {
    return 0;
  }
  return tiers[tier].size();
}

bool TSMFileManager::shouldCompactTier(uint64_t tier) const {
  if(tier >= MAX_TIERS) {
    return false;
  }
  
  size_t fileCount = tiers[tier].size();
  
  // Compact when we have at least FILES_PER_COMPACTION files
  return fileCount >= FILES_PER_COMPACTION;
}

seastar::future<> TSMFileManager::addTSMFile(seastar::shared_ptr<TSM> file) {
  tsmFiles.push_back(file);
  
  uint64_t tier = file->tierNum;
  if(tier < MAX_TIERS) {
    tiers[tier].push_back(file);
  }
  
  unsigned int tsmSeqNum = file->rankAsInteger();
  sequencedTsmFiles.insert({tsmSeqNum, file});
  
  if(file->seqNum >= nextSequenceId){
    nextSequenceId = file->seqNum + 1;
  }
  
  co_return;
}

seastar::future<> TSMFileManager::removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files) {
  for(const auto& file : files) {
    // Remove from main list
    tsmFiles.erase(std::remove(tsmFiles.begin(), tsmFiles.end(), file), tsmFiles.end());
    
    // Remove from tier tracking
    uint64_t tier = file->tierNum;
    if(tier < MAX_TIERS) {
      tiers[tier].erase(std::remove(tiers[tier].begin(), tiers[tier].end(), file), 
                        tiers[tier].end());
    }
    
    // Remove from sequenced map
    unsigned int tsmSeqNum = file->rankAsInteger();
    sequencedTsmFiles.erase(tsmSeqNum);
    
    // Mark file for deletion
    file->markForDeletion();
  }
  
  co_return;
}

seastar::future<> TSMFileManager::checkAndTriggerCompaction() {
  // Check each tier for compaction needs
  for(uint64_t tier = 0; tier < MAX_TIERS - 1; tier++) {
    if(shouldCompactTier(tier)) {
      tsdb::compactor_log.info("Tier {} needs compaction ({} files)", 
                                tier, getFileCountInTier(tier));
      
      // Plan and execute compaction
      auto plan = compactor->planCompaction(tier);
      if(plan.isValid()) {
        auto stats = co_await compactor->executeCompaction(plan);
        tsdb::compactor_log.info("Compacted {} files from tier {} to tier {} in {}ms",
                                  stats.filesCompacted, tier, plan.targetTier, 
                                  stats.duration.count());
      }
    }
  }
  
  co_return;
}

seastar::future<> TSMFileManager::startCompactionLoop() {
  if(compactor) {
    compactionTask = compactor->runCompactionLoop();
  }
  co_return;
}

seastar::future<> TSMFileManager::stopCompactionLoop() {
  if(compactor) {
    compactor->stopCompaction();
    if(compactionTask.has_value()) {
      co_await std::move(compactionTask.value());
    }
  }
  co_return;
}