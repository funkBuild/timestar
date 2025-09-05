#include "tsm_compactor.hpp"
#include "tsm_file_manager.hpp"
#include "tsm_writer.hpp"
#include "logger.hpp"
#include <filesystem>
#include <set>
#include <chrono>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/reactor.hh>

namespace fs = std::filesystem;

TSMCompactor::TSMCompactor(TSMFileManager* manager) 
    : fileManager(manager),
      strategy(std::make_unique<LeveledCompactionStrategy>()) {
}

std::string TSMCompactor::generateCompactedFilename(uint64_t tier, uint64_t seqNum) {
    int shardId = seastar::this_shard_id();
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "shard_%d/tsm/%lu_%lu.tsm", shardId, tier, seqNum);
    return std::string(buffer);
}

std::set<std::string> TSMCompactor::getAllSeriesKeys(
    const std::vector<seastar::shared_ptr<TSM>>& files) {
    
    std::set<std::string> allKeys;
    
    // Collect all unique series keys from all files
    for (const auto& file : files) {
        auto keys = file->getSeriesKeys();
        allKeys.insert(keys.begin(), keys.end());
    }
    
    return allKeys;
}

template<typename T>
seastar::future<> TSMCompactor::mergeSeries(
    const std::string& seriesKey,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats) {
    
    // Create merge iterator for this series
    TSMMergeIterator<T> merger(seriesKey, sources);
    co_await merger.init();
    
    // Collect all tombstones for this series from all source files
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                uint64_t seriesId = file->getSeriesId(seriesKey);
                auto ranges = tombstones->getTombstoneRanges(seriesId);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }
    
    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());
        
        // Merge overlapping ranges
        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);
        
        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];
            
            if (current.first <= last.second + 1) {
                // Overlapping or adjacent, merge
                last.second = std::max(last.second, current.second);
            } else {
                // Non-overlapping, add as new range
                mergedRanges.push_back(current);
            }
        }
        
        tombstoneRanges = std::move(mergedRanges);
    }
    
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    timestamps.reserve(BATCH_SIZE);
    values.reserve(BATCH_SIZE);
    
    uint64_t lastTimestamp = 0;
    size_t tombstonesFiltered = 0;
    
    // Process in batches
    while (merger.hasNext()) {
        auto batch = merger.nextBatch(BATCH_SIZE);
        
        for (const auto& [ts, val] : batch) {
            stats.pointsRead++;
            
            // Check if this point is tombstoned
            bool isTombstoned = false;
            for (const auto& [startTime, endTime] : tombstoneRanges) {
                if (ts >= startTime && ts <= endTime) {
                    isTombstoned = true;
                    tombstonesFiltered++;
                    break;
                }
                // Early exit if we've passed this tombstone range
                if (startTime > ts) {
                    break;
                }
            }
            
            if (isTombstoned) {
                continue; // Skip tombstoned data
            }
            
            // Deduplicate - skip if same timestamp
            if (ts != lastTimestamp) {
                timestamps.push_back(ts);
                values.push_back(val);
                lastTimestamp = ts;
                stats.pointsWritten++;
            } else {
                stats.duplicatesRemoved++;
            }
            
            // Write when batch is full
            if (timestamps.size() >= BATCH_SIZE) {
                writer.writeSeries(TSM::getValueType<T>(), seriesKey, timestamps, values);
                timestamps.clear();
                values.clear();
            }
        }
    }
    
    // Write remaining data
    if (!timestamps.empty()) {
        writer.writeSeries(TSM::getValueType<T>(), seriesKey, timestamps, values);
    }
    
    // Log tombstone filtering stats if any points were filtered
    if (tombstonesFiltered > 0) {
        tsdb::compactor_log.info("Compaction: Filtered {} tombstoned points for series {}",
                                 tombstonesFiltered, seriesKey);
    }
    
    co_return;
}

seastar::future<std::string> TSMCompactor::compact(
    const std::vector<seastar::shared_ptr<TSM>>& files) {
    
    if (files.empty()) {
        co_return std::string();
    }
    
    // Determine output tier and filename
    uint64_t maxTier = 0;
    uint64_t maxSeq = 0;
    for (const auto& file : files) {
        maxTier = std::max(maxTier, file->tierNum);
        maxSeq = std::max(maxSeq, file->seqNum);
    }
    
    uint64_t targetTier = strategy->getTargetTier(maxTier, files.size());
    uint64_t targetSeq = maxSeq + 1;
    std::string outputPath = generateCompactedFilename(targetTier, targetSeq);
    
    // Create temporary file for writing
    std::string tempPath = outputPath + ".tmp";
    TSMWriter writer(tempPath);
    
    CompactionStats stats;
    stats.filesCompacted = files.size();
    auto startTime = std::chrono::steady_clock::now();
    
    // Get all unique series across files
    auto allSeries = getAllSeriesKeys(files);
    
    // Merge each series
    for (const std::string& seriesKey : allSeries) {
        // Determine the type of this series
        std::string key = seriesKey;
        auto seriesType = files[0]->getSeriesType(key);
        
        if (!seriesType.has_value()) {
            continue;
        }
        
        // Merge based on type
        if (seriesType.value() == TSMValueType::Float) {
            co_await mergeSeries<double>(seriesKey, files, writer, stats);
        } else if (seriesType.value() == TSMValueType::Boolean) {
            co_await mergeSeries<bool>(seriesKey, files, writer, stats);
        } else if (seriesType.value() == TSMValueType::String) {
            co_await mergeSeries<std::string>(seriesKey, files, writer, stats);
        }
    }
    
    // Finalize the file
    writer.writeIndex();
    writer.close();
    
    // Calculate statistics
    auto endTime = std::chrono::steady_clock::now();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - startTime);
    
    // Atomic rename from temp to final
    fs::rename(tempPath, outputPath);
    
    co_return outputPath;
}

CompactionPlan TSMCompactor::planCompaction(uint64_t tier) {
    CompactionPlan plan;
    plan.targetTier = tier;
    
    // Get files from file manager using new tier tracking
    std::vector<seastar::shared_ptr<TSM>> tierFiles = fileManager->getFilesInTier(tier);
    
    // Use strategy to select files
    plan.sourceFiles = strategy->selectFiles(tierFiles, tier);
    
    if (!plan.sourceFiles.empty()) {
        // Calculate target tier and sequence
        uint64_t maxSeq = 0;
        for (const auto& file : plan.sourceFiles) {
            maxSeq = std::max(maxSeq, file->seqNum);
        }
        
        plan.targetTier = strategy->getTargetTier(tier, plan.sourceFiles.size());
        plan.targetSeqNum = maxSeq + 1;
        plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);
        
        // Estimate output size (rough estimate - 70% of input due to compression)
        plan.estimatedSize = 0;
        for (const auto& file : plan.sourceFiles) {
            plan.estimatedSize += file->getFileSize();
        }
        plan.estimatedSize = plan.estimatedSize * 0.7;
    }
    
    return plan;
}

seastar::future<CompactionStats> TSMCompactor::executeCompaction(const CompactionPlan& plan) {
    if (!plan.isValid()) {
        co_return CompactionStats{};
    }
    
    // Acquire semaphore to limit concurrent compactions
    auto units = co_await seastar::get_units(compactionSemaphore, 1);
    
    // Track this compaction
    ActiveCompaction active;
    active.plan = plan;
    active.startTime = std::chrono::steady_clock::now();
    activeCompactions.push_back(active);
    
    // Perform compaction
    std::string newFile = co_await compact(plan.sourceFiles);
    
    if (!newFile.empty()) {
        // Open the new file
        auto newTSM = seastar::make_shared<TSM>(newFile);
        co_await newTSM->open();
        
        // Add to file manager
        co_await fileManager->addTSMFile(newTSM);
        
        // Remove old files from manager and mark for deletion
        co_await fileManager->removeTSMFiles(plan.sourceFiles);
        
        // Delete tombstone files for the compacted TSM files
        for (const auto& file : plan.sourceFiles) {
            if (file->hasTombstones()) {
                co_await file->deleteTombstoneFile();
            }
        }
    }
    
    // Update stats
    auto endTime = std::chrono::steady_clock::now();
    active.stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - active.startTime);
    active.stats.filesCompacted = plan.sourceFiles.size();
    
    // Remove from active list
    activeCompactions.erase(
        std::remove_if(activeCompactions.begin(), activeCompactions.end(),
                      [&](const ActiveCompaction& a) {
                          return a.plan.targetPath == plan.targetPath;
                      }),
        activeCompactions.end());
    
    co_return active.stats;
}

bool TSMCompactor::shouldCompact(uint64_t tier) const {
    // Use the file manager's method to check
    return fileManager->shouldCompactTier(tier);
}

seastar::future<> TSMCompactor::runCompactionLoop() {
    while (compactionEnabled) {
        bool didCompaction = false;
        
        // Check each tier for compaction
        for (uint64_t tier = 0; tier < 4; tier++) {
            if (shouldCompact(tier)) {
                auto plan = planCompaction(tier);
                if (plan.isValid()) {
                    auto stats = co_await executeCompaction(plan);
                    didCompaction = true;
                    
                    tsdb::compactor_log.info("Compacted {} files in tier {}, removed {} duplicates in {}ms",
                                            stats.filesCompacted, tier, stats.duplicatesRemoved,
                                            stats.duration.count());
                }
            }
        }
        
        // Sleep before next check
        if (!didCompaction) {
            co_await seastar::sleep(std::chrono::seconds(30));
        } else {
            // Short sleep after compaction to allow system to stabilize
            co_await seastar::sleep(std::chrono::seconds(5));
        }
    }
}

seastar::future<> TSMCompactor::forceFullCompaction() {
    tsdb::compactor_log.info("Starting forced full compaction...");
    
    // Compact each tier from bottom up
    for (uint64_t tier = 0; tier < 3; tier++) {
        // Get all files in this tier
        std::vector<seastar::shared_ptr<TSM>> tierFiles;
        for (const auto& [seq, file] : fileManager->sequencedTsmFiles) {
            if (file->tierNum == tier) {
                tierFiles.push_back(file);
            }
        }
        
        if (tierFiles.size() > 1) {
            tsdb::compactor_log.info("Compacting {} files in tier {}", tierFiles.size(), tier);
            
            CompactionPlan plan;
            plan.sourceFiles = tierFiles;
            plan.targetTier = tier + 1;
            plan.targetSeqNum = 0; // Will be set properly
            plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);
            
            auto stats = co_await executeCompaction(plan);
            
            std::cout << "Compacted tier " << tier << ": " 
                     << stats.filesCompacted << " files, "
                     << stats.pointsWritten << " points written, "
                     << stats.duplicatesRemoved << " duplicates removed"
                     << std::endl;
        }
    }
    
    std::cout << "Full compaction complete" << std::endl;
}

std::vector<CompactionStats> TSMCompactor::getActiveCompactionStats() const {
    std::vector<CompactionStats> stats;
    for (const auto& active : activeCompactions) {
        stats.push_back(active.stats);
    }
    return stats;
}

// Explicit template instantiations
template seastar::future<> TSMCompactor::mergeSeries<double>(
    const std::string& seriesKey,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

template seastar::future<> TSMCompactor::mergeSeries<bool>(
    const std::string& seriesKey,
    const std::vector<seastar::shared_ptr<TSM>>& sources,
    TSMWriter& writer,
    CompactionStats& stats);

// TimeBasedCompactionStrategy implementation
bool TimeBasedCompactionStrategy::shouldCompact(uint64_t tier, 
                                               size_t fileCount, 
                                               size_t totalSize) const {
    // For time-based, compact if we have old files
    // This would need file creation time tracking
    return fileCount >= 2; // Simple check for now
}

std::vector<seastar::shared_ptr<TSM>> TimeBasedCompactionStrategy::selectFiles(
    const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
    uint64_t tier) const {
    
    // Select oldest files
    std::vector<seastar::shared_ptr<TSM>> selected = availableFiles;
    
    // Sort by sequence number (assuming older = lower seq)
    std::sort(selected.begin(), selected.end(),
             [](const auto& a, const auto& b) {
                 return a->seqNum < b->seqNum;
             });
    
    // Take first half of files
    if (selected.size() > 2) {
        selected.resize(selected.size() / 2);
    }
    
    return selected;
}