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

seastar::future<> TSMFileManager::stop() {
    // Stop compaction before closing files to prevent reads from closed handles
    co_await stopCompactionLoop();

    // Close all open TSM file handles to prevent resource leaks.
    // Seastar asserts on leaked file handles in debug builds.
    for (auto& [seqNum, tsmFile] : sequencedTsmFiles) {
        try {
            co_await tsmFile->close();
        } catch (const std::exception& e) {
            timestar::tsm_log.warn("Failed to close TSM file (seq={}): {}", seqNum, e.what());
        }
    }
    sequencedTsmFiles.clear();
    for (size_t i = 0; i < MAX_TIERS; ++i) {
        tiers[i].clear();
    }
    timestar::tsm_log.info("TSMFileManager stopped on shard {}", shardId);
}

seastar::future<> TSMFileManager::openTsmFile(std::string path) {
    timestar::tsm_log.debug("Opening TSM file: {}", path);

    // Parse sequence number from filename before attempting open, so that
    // nextSequenceId is updated even if open() fails on a corrupt file.
    // This prevents sequence number reuse on the next writeMemstore().
    seastar::shared_ptr<TSM> tsmFile = seastar::make_shared<TSM>(path);
    if (tsmFile->seqNum >= nextSequenceId) {
        nextSequenceId = tsmFile->seqNum + 1;
    }

    try {
        co_await tsmFile->open();

        uint64_t tsmSeqNum = tsmFile->rankAsInteger();

        auto [it, inserted] = sequencedTsmFiles.insert({tsmSeqNum, tsmFile});
        if (!inserted) {
            timestar::tsm_log.warn("Duplicate sequence number {} for TSM file: {}, existing file takes precedence",
                                   tsmSeqNum, path);
            co_await tsmFile->close();
        } else {
            uint64_t tier = tsmFile->tierNum;
            if (tier < MAX_TIERS) {
                tiers[tier].push_back(tsmFile);
            }
        }
    } catch (const std::exception& e) {
        timestar::tsm_log.error("Failed to open TSM file {}: {}", path, e.what());
        co_return;
    }
}

seastar::future<> TSMFileManager::writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier) {
    auto seqNum = nextSequenceId++;

    std::string filename =
        "shard_" + std::to_string(shardId) + "/tsm/" + std::to_string(tier) + "_" + std::to_string(seqNum) + ".tsm";

    // Write to a .tmp file first, then rename atomically on success.
    // If TSMWriter::runAsync() throws, the orphaned .tmp file will be
    // cleaned up on the next startup by init() (which removes all .tmp files).
    // Note: TSMWriter::runAsync() calls closeDMA(), which flushes the file
    // (co_await file.flush()) before closing. No additional flush needed here.
    auto tmpFilename = filename + ".tmp";
    co_await TSMWriter::runAsync(memStore, tmpFilename);
    co_await seastar::rename_file(tmpFilename, filename);

    // Fsync the parent directory to ensure the rename (directory entry update)
    // is durable.  Without this, a crash could lose the rename even though
    // the file data is already on disk.
    auto slash = filename.rfind('/');
    std::string parentDir = (slash != std::string::npos) ? filename.substr(0, slash) : ".";
    try {
        auto dirFile = co_await seastar::open_directory(parentDir);
        co_await dirFile.flush();
        co_await dirFile.close();
    } catch (...) {
        timestar::tsm_log.warn("Failed to fsync parent directory after memstore write rename: {}", parentDir);
    }

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
    uint64_t tsmSeqNum = file->rankAsInteger();
    auto [it, inserted] = sequencedTsmFiles.insert({tsmSeqNum, file});
    if (!inserted) {
        timestar::tsm_log.warn("Duplicate sequence number {} when adding TSM file, existing file takes precedence",
                               tsmSeqNum);
        co_await file->close();
    } else {
        uint64_t tier = file->tierNum;
        if (tier < MAX_TIERS) {
            tiers[tier].push_back(file);
        }
    }

    if (file->seqNum >= nextSequenceId) {
        if (file->seqNum == UINT64_MAX) [[unlikely]] {
            throw std::overflow_error("TSM sequence number exhausted");
        }
        nextSequenceId = file->seqNum + 1;
    }

    co_return;
}

seastar::future<> TSMFileManager::removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files) {
    for (const auto& file : files) {
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
                    ++completedCompactions_;
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
    if (compactor && !compactionTask.has_value()) {
        compactionTask = compactor->runCompactionLoop();
    }
    co_return;
}

seastar::future<> TSMFileManager::stopCompactionLoop() {
    if (compactor) {
        compactor->stopCompaction();
        if (compactionTask.has_value()) {
            co_await std::move(compactionTask.value());
            compactionTask.reset();
        }
    }
    co_return;
}