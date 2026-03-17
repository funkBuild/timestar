#include "tsm_file_manager.hpp"

#include "logger.hpp"
#include "series_id.hpp"
#include "tsm_compactor.hpp"
#include "tsm_writer.hpp"
#include "util.hpp"

#include <filesystem>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/with_scheduling_group.hh>

namespace fs = std::filesystem;

TSMFileManager::TSMFileManager() {
    shardId = seastar::this_shard_id();
}

// Destructor defined here where TSMCompactor is a complete type,
// so std::unique_ptr<TSMCompactor> can call delete.
TSMFileManager::~TSMFileManager() = default;

std::string TSMFileManager::basePath() {
    return std::string("shard_" + std::to_string(shardId) + "/tsm/");
}

seastar::future<> TSMFileManager::init() {
    timestar::tsm_log.info("TSMFileManager init. shardId={}", shardId);

    // Initialize compactor
    compactor = std::make_unique<TSMCompactor>(this);

    // Scan the TSM folder for files if it exists.
    // std::filesystem calls are blocking, so run them off the reactor thread.
    auto tsmPaths = co_await seastar::async([this] {
        std::vector<std::string> paths;
        auto base = basePath();
        if (fs::exists(base)) {
            for (const auto& entry : fs::directory_iterator(base)) {
                if (endsWith(entry.path(), ".tsm")) {
                    paths.push_back(fs::canonical(fs::absolute(entry.path())).string());
                } else if (endsWith(entry.path(), ".tmp")) {
                    // Orphaned .tmp files from a previous crash (compaction wrote the
                    // file but died before rename to .tsm). Safe to remove.
                    std::error_code ec;
                    fs::remove(entry.path(), ec);
                    if (ec) {
                        timestar::tsm_log.warn("Failed to remove orphaned tmp file {}: {}", entry.path().string(),
                                               ec.message());
                    } else {
                        timestar::tsm_log.info("Cleaned up orphaned tmp file: {}", entry.path().string());
                    }
                }
            }
        }
        return paths;
    });

    // Open TSM files using Seastar async I/O (must run on reactor thread)
    for (const auto& path : tsmPaths) {
        co_await openTsmFile(path);
    }
}

seastar::future<> TSMFileManager::openTsmFile(std::string path) {
    timestar::tsm_log.debug("Opening TSM file: {}", path);

    try {
        seastar::shared_ptr<TSM> tsmFile = seastar::make_shared<TSM>(path);
        co_await tsmFile->open();

        tsmFiles.push_back(tsmFile);

        // Add to tier tracking
        uint64_t tier = tsmFile->tierNum;
        if (tier < MAX_TIERS) {
            tiers[tier].push_back(tsmFile);
        }

        uint64_t tsmSeqNum = tsmFile.get()->rankAsInteger();

        auto [it, inserted] = sequencedTsmFiles.insert({tsmSeqNum, tsmFile});
        if (!inserted) {
            timestar::tsm_log.warn("Duplicate sequence number {} for TSM file: {}, existing file takes precedence",
                                   tsmSeqNum, path);
        }

        if (tsmFile->seqNum >= nextSequenceId) {
            nextSequenceId = tsmFile->seqNum + 1;
        }
    } catch (const std::runtime_error& e) {
        timestar::tsm_log.error("Failed to open TSM file {}: {}", path, e.what());
        co_return;
    }
}

seastar::future<> TSMFileManager::writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier) {
    auto seqNum = nextSequenceId++;

    std::string filename =
        "shard_" + std::to_string(shardId) + "/tsm/" + std::to_string(tier) + "_" + std::to_string(seqNum) + ".tsm";

    // TSMWriter::runAsync() uses Seastar DMA I/O for non-blocking writes.
    // All in-memory buffer construction is synchronous (CPU-bound, fast),
    // and only the final file write is async via open_file_dma + dma_write.
    co_await TSMWriter::runAsync(memStore, filename);
    co_await openTsmFile(filename);

    co_await checkAndTriggerCompaction();
}

std::optional<TSMValueType> TSMFileManager::getSeriesType(const std::string& seriesKey) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    return getSeriesType(seriesId);
}

std::optional<TSMValueType> TSMFileManager::getSeriesType(const SeriesId128& seriesId) {
    std::optional<TSMValueType> seriesType;

    for (auto const& [seqNum, tsmFile] : sequencedTsmFiles) {
        seriesType = tsmFile.get()->getSeriesType(seriesId);

        if (seriesType)
            return seriesType;
    }

    return seriesType;
}

std::vector<seastar::shared_ptr<TSM>> TSMFileManager::getFilesInTier(uint64_t tier) const {
    if (tier >= MAX_TIERS) {
        return {};
    }
    return tiers[tier];
}

size_t TSMFileManager::getFileCountInTier(uint64_t tier) const {
    if (tier >= MAX_TIERS) {
        return 0;
    }
    return tiers[tier].size();
}

bool TSMFileManager::shouldCompactTier(uint64_t tier) const {
    if (tier >= MAX_TIERS) {
        return false;
    }

    size_t fileCount = tiers[tier].size();

    // Compact when we have at least filesPerCompaction() files
    return fileCount >= filesPerCompaction();
}

seastar::future<> TSMFileManager::addTSMFile(seastar::shared_ptr<TSM> file) {
    tsmFiles.push_back(file);

    uint64_t tier = file->tierNum;
    if (tier < MAX_TIERS) {
        tiers[tier].push_back(file);
    }

    uint64_t tsmSeqNum = file->rankAsInteger();
    auto [it, inserted] = sequencedTsmFiles.insert({tsmSeqNum, file});
    if (!inserted) {
        timestar::tsm_log.warn("Duplicate sequence number {} when adding TSM file, existing file takes precedence",
                               tsmSeqNum);
    }

    if (file->seqNum >= nextSequenceId) {
        nextSequenceId = file->seqNum + 1;
    }

    co_return;
}

seastar::future<> TSMFileManager::removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files) {
    for (const auto& file : files) {
        // Remove from main list
        tsmFiles.erase(std::remove(tsmFiles.begin(), tsmFiles.end(), file), tsmFiles.end());

        // Remove from tier tracking
        uint64_t tier = file->tierNum;
        if (tier < MAX_TIERS) {
            tiers[tier].erase(std::remove(tiers[tier].begin(), tiers[tier].end(), file), tiers[tier].end());
        }

        // Remove from sequenced map
        uint64_t tsmSeqNum = file->rankAsInteger();
        sequencedTsmFiles.erase(tsmSeqNum);

        // Delete the tombstone file first (if any), then the TSM file itself
        co_await file->deleteTombstoneFile();
        co_await file->scheduleDelete();
    }

    co_return;
}

seastar::future<> TSMFileManager::checkAndTriggerCompaction() {
    for (uint64_t tier = 0; tier < MAX_TIERS - 1; tier++) {
        if (shouldCompactTier(tier)) {
            timestar::compactor_log.info("Tier {} needs compaction ({} files)", tier, getFileCountInTier(tier));

            auto plan = compactor->planCompaction(tier);
            if (plan.isValid()) {
                try {
                    CompactionStats stats;
                    if (_compactionGroupSet) {
                        // Run compaction under the low-priority scheduling group so
                        // query I/O gets preferential disk bandwidth. Pass plan as
                        // an argument (not capture) to avoid dangling references.
                        stats = co_await seastar::with_scheduling_group(
                            _compactionGroup, [this](CompactionPlan p) { return compactor->executeCompaction(p); },
                            std::move(plan));
                    } else {
                        stats = co_await compactor->executeCompaction(plan);
                    }
                    timestar::compactor_log.info("Compacted {} files from tier {} to tier {} in {}ms",
                                                 stats.filesCompacted, tier, tier + 1, stats.duration.count());
                } catch (const std::exception& e) {
                    timestar::compactor_log.error("Compaction failed for tier {}: {}. Will retry later.", tier,
                                                  e.what());
                }
            }
        }
    }
    co_return;
}

seastar::future<> TSMFileManager::startCompactionLoop() {
    if (compactor) {
        compactionTask = compactor->runCompactionLoop();
    }
    co_return;
}

seastar::future<> TSMFileManager::stopCompactionLoop() {
    if (compactor) {
        compactor->stopCompaction();
        if (compactionTask.has_value()) {
            co_await std::move(compactionTask.value());
        }
    }
    co_return;
}