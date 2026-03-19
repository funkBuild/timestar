#include "shard_rebalancer.hpp"

#include "logger.hpp"
#include "memory_store.hpp"
#include "placement_table.hpp"
#include "series_id.hpp"
#include "tsm.hpp"
#include "tsm_writer.hpp"

#include <algorithm>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <limits>
#include <regex>
#include <unistd.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>
#include <unordered_map>
#include <unordered_set>

namespace fs = std::filesystem;

namespace timestar {

// ---------------------------------------------------------------------------
// Construction & path helpers
// ---------------------------------------------------------------------------

ShardRebalancer::ShardRebalancer(const std::string& dataDir) : _dataDir(dataDir.empty() ? "." : dataDir) {}

std::string ShardRebalancer::shardDir(unsigned shard) const {
    return _dataDir + "/shard_" + std::to_string(shard);
}

std::string ShardRebalancer::shardDirNew(unsigned shard) const {
    return _dataDir + "/shard_" + std::to_string(shard) + "_new";
}

std::string ShardRebalancer::shardDirOld(unsigned shard) const {
    return _dataDir + "/shard_" + std::to_string(shard) + "_old";
}

std::string ShardRebalancer::metaFilePath() const {
    return _dataDir + "/shard_count.meta";
}

std::string ShardRebalancer::stateFilePath() const {
    return _dataDir + "/rebalance.state";
}

// ---------------------------------------------------------------------------
// shard_count.meta persistence
// ---------------------------------------------------------------------------

void ShardRebalancer::writeShardCountMeta(const std::string& dataDir, unsigned shardCount) {
    std::string path = (dataDir.empty() ? "." : dataDir) + "/shard_count.meta";
    std::string tmpPath = path + ".tmp";
    std::ofstream ofs(tmpPath, std::ios::trunc);
    if (!ofs) {
        throw std::runtime_error("Failed to write shard_count.meta.tmp: " + tmpPath);
    }
    ofs << shardCount << "\n";
    ofs.flush();
    ofs.close();
    // fsync the temp file to ensure data reaches disk before rename
    int fd = ::open(tmpPath.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
    // Atomic rename replaces old file — no window where truncation loses data
    fs::rename(tmpPath, path);
    // fsync parent directory to persist the rename
    std::string dir = fs::path(path).parent_path().string();
    int dirfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirfd >= 0) {
        ::fsync(dirfd);
        ::close(dirfd);
    }
}

unsigned ShardRebalancer::readShardCountMeta(const std::string& dataDir) {
    std::string path = (dataDir.empty() ? "." : dataDir) + "/shard_count.meta";
    std::ifstream ifs(path);
    if (!ifs)
        return 0;
    unsigned count = 0;
    ifs >> count;
    return count;
}

// ---------------------------------------------------------------------------
// Rebalance state file (crash safety)
// ---------------------------------------------------------------------------

void ShardRebalancer::writeState(const RebalanceState& state) {
    auto path = stateFilePath();
    std::string tmpPath = path + ".tmp";
    std::ofstream ofs(tmpPath, std::ios::trunc);
    if (!ofs) {
        throw std::runtime_error("Failed to write rebalance.state.tmp");
    }
    // Simple text format: phase oldCount newCount
    int phaseInt = static_cast<int>(state.phase);
    ofs << phaseInt << " " << state.oldShardCount << " " << state.newShardCount << "\n";
    ofs.flush();
    ofs.close();
    // fsync the temp file to ensure data reaches disk before rename
    int fd = ::open(tmpPath.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
    // Atomic rename replaces old file — no window where truncation loses data
    fs::rename(tmpPath, path);
    // fsync parent directory to persist the rename
    std::string dir = fs::path(path).parent_path().string();
    int dirfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirfd >= 0) {
        ::fsync(dirfd);
        ::close(dirfd);
    }
}

RebalanceState ShardRebalancer::readState() {
    std::ifstream ifs(stateFilePath());
    if (!ifs)
        return {};
    RebalanceState state;
    int phaseInt = 0;
    ifs >> phaseInt >> state.oldShardCount >> state.newShardCount;
    if (ifs.fail())
        return {};
    if (phaseInt < 0 || phaseInt > 3) {
        engine_log.error("[REBALANCE] Invalid phase {} in state file, ignoring", phaseInt);
        return {};
    }
    state.phase = static_cast<RebalancePhase>(phaseInt);
    return state;
}

void ShardRebalancer::removeState() {
    std::error_code ec;
    fs::remove(stateFilePath(), ec);
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

unsigned ShardRebalancer::detectShardCountFromDirs() const {
    unsigned maxShard = 0;
    bool found = false;
    std::regex shardPattern("shard_(\\d+)");

    if (!fs::exists(_dataDir))
        return 0;

    for (const auto& entry : fs::directory_iterator(_dataDir)) {
        if (!entry.is_directory())
            continue;
        std::string name = entry.path().filename().string();
        std::smatch m;
        if (std::regex_match(name, m, shardPattern)) {
            unsigned id = std::stoul(m[1].str());
            if (id >= maxShard) {
                maxShard = id + 1;
                found = true;
            }
        }
    }
    return found ? maxShard : 0;
}

bool ShardRebalancer::isRebalanceNeeded(unsigned newShardCount) {
    // First check if there's a partial rebalance to recover
    auto state = readState();
    if (state.phase != RebalancePhase::None) {
        _oldShardCount = state.oldShardCount;
        return true;
    }

    // Check shard_count.meta
    _oldShardCount = readShardCountMeta(_dataDir);
    if (_oldShardCount == 0) {
        // No meta file — scan directories
        _oldShardCount = detectShardCountFromDirs();
        if (_oldShardCount == 0) {
            // Fresh install, no rebalance needed
            return false;
        }
    }

    return _oldShardCount != newShardCount;
}

// ---------------------------------------------------------------------------
// Staging directory creation
// ---------------------------------------------------------------------------

void ShardRebalancer::createStagingDirs(unsigned newShardCount) {
    for (unsigned s = 0; s < newShardCount; ++s) {
        fs::create_directories(shardDirNew(s) + "/tsm");
        fs::create_directories(shardDirNew(s) + "/native_index");
    }
}

// ---------------------------------------------------------------------------
// Phase A: WAL file processing
// ---------------------------------------------------------------------------

seastar::future<> ShardRebalancer::processWALFiles(unsigned oldShardCount, unsigned newShardCount) {
    for (unsigned oldShard = 0; oldShard < oldShardCount; ++oldShard) {
        std::string shardPath = shardDir(oldShard);

        // Wrap blocking filesystem calls in seastar::async to avoid reactor stalls
        auto walFiles = co_await seastar::async([&shardPath] {
            std::vector<std::string> files;
            if (!fs::exists(shardPath))
                return files;
            for (const auto& entry : fs::directory_iterator(shardPath)) {
                if (entry.path().extension() == ".wal") {
                    files.push_back(entry.path().string());
                }
            }
            return files;
        });

        if (walFiles.empty())
            continue;

        engine_log.info("[REBALANCE] Processing {} WAL files from shard {}", walFiles.size(), oldShard);

        for (const auto& walPath : walFiles) {
            // Replay WAL into a temporary MemoryStore (no WAL backing)
            auto tempStore = seastar::make_shared<::MemoryStore>(0);
            co_await tempStore->initFromWAL(walPath);

            if (tempStore->isEmpty())
                continue;

            // Group series by new target shard
            std::unordered_map<unsigned, seastar::shared_ptr<::MemoryStore>> perShardStores;

            for (auto& [seriesId, variantSeries] : tempStore->series) {
                unsigned targetShard = timestar::routeToCore(seriesId);

                auto it = perShardStores.find(targetShard);
                if (it == perShardStores.end()) {
                    auto store = seastar::make_shared<::MemoryStore>(0);
                    it = perShardStores.emplace(targetShard, std::move(store)).first;
                }

                it->second->series[seriesId] = std::move(variantSeries);
            }

            // Write each per-shard store as a new TSM file in the staging dir
            for (auto& [targetShard, store] : perShardStores) {
                if (store->isEmpty())
                    continue;

                // Generate a unique filename based on source WAL
                std::string basename = fs::path(walPath).stem().string();
                std::string tsmPath =
                    shardDirNew(targetShard) + "/tsm/0_wal_" + std::to_string(oldShard) + "_" + basename + ".tsm";

                co_await ::TSMWriter::runAsync(store, tsmPath);
                engine_log.debug("[REBALANCE] Wrote WAL data to {}", tsmPath);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase B+C: TSM file analysis and move/split
// ---------------------------------------------------------------------------

seastar::future<> ShardRebalancer::processTSMFiles(unsigned oldShardCount, unsigned newShardCount) {
    // Track next sequence ID per new shard to avoid collisions
    std::unordered_map<unsigned, uint64_t> nextSeqPerShard;

    for (unsigned oldShard = 0; oldShard < oldShardCount; ++oldShard) {
        std::string tsmDir = shardDir(oldShard) + "/tsm";

        // Wrap blocking filesystem calls in seastar::async to avoid reactor stalls
        auto tsmFiles = co_await seastar::async([&tsmDir] {
            std::vector<std::string> files;
            if (!fs::exists(tsmDir))
                return files;
            for (const auto& entry : fs::directory_iterator(tsmDir)) {
                if (entry.path().extension() == ".tsm") {
                    files.push_back(fs::canonical(entry.path()).string());
                }
            }
            return files;
        });

        if (tsmFiles.empty())
            continue;

        engine_log.info("[REBALANCE] Analyzing {} TSM files from shard {}", tsmFiles.size(), oldShard);

        for (const auto& tsmPath : tsmFiles) {
            // Open the TSM file and read its sparse index
            auto tsm = seastar::make_shared<::TSM>(tsmPath);
            co_await tsm->open();
            co_await tsm->readSparseIndex();

            auto seriesIds = tsm->getSeriesIds();
            if (seriesIds.empty()) {
                co_await tsm->close();
                continue;
            }

            // Determine target shards for all series
            std::unordered_map<unsigned, std::vector<::SeriesId128>> shardGroups;
            for (const auto& id : seriesIds) {
                unsigned target = timestar::routeToCore(id);
                shardGroups[target].push_back(id);
            }

            if (shardGroups.size() == 1) {
                // All series go to the same shard — move the file
                unsigned targetShard = shardGroups.begin()->first;
                std::string destDir = shardDirNew(targetShard) + "/tsm/";
                std::string filename = fs::path(tsmPath).filename().string();
                std::string destPath = destDir + filename;

                // Avoid filename collisions
                if (fs::exists(destPath)) {
                    auto& seq = nextSeqPerShard[targetShard];
                    destPath = destDir + "0_rebal_" + std::to_string(seq++) + ".tsm";
                }

                co_await tsm->close();

                // Hard link (fast, avoids copy) — wrapped in seastar::async
                // since filesystem operations are blocking
                co_await seastar::async([&] {
                    std::error_code ec;
                    fs::create_hard_link(tsmPath, destPath, ec);
                    if (ec) {
                        // Fallback to copy if hard link fails (cross-device)
                        fs::copy_file(tsmPath, destPath, ec);
                        if (ec) {
                            throw std::runtime_error("Failed to copy TSM file " + tsmPath + " to " + destPath + ": " +
                                                     ec.message());
                        }
                    }

                    // Also move the tombstone file if it exists
                    std::string tombPath = tsmPath.substr(0, tsmPath.rfind('.')) + ".tombstone";
                    if (fs::exists(tombPath)) {
                        std::string destTomb = destPath.substr(0, destPath.rfind('.')) + ".tombstone";
                        fs::copy_file(tombPath, destTomb, ec);
                    }
                });

                engine_log.debug("[REBALANCE] Moved TSM {} -> {} (shard {})", tsmPath, destPath, targetShard);
            } else {
                // File has series spanning multiple new shards — must split
                engine_log.debug("[REBALANCE] Splitting TSM {} across {} shards", tsmPath, shardGroups.size());

                // Load tombstones for filtering during read
                co_await tsm->loadTombstones();

                for (auto& [targetShard, ids] : shardGroups) {
                    auto splitStore = seastar::make_shared<::MemoryStore>(0);

                    for (const auto& seriesId : ids) {
                        auto typeOpt = tsm->getSeriesType(seriesId);
                        if (!typeOpt)
                            continue;

                        // Helper: extract all data from a TSMResult into an InMemorySeries
                        auto extractData = [&]<typename T>(::TSMResult<T>& result) {
                            if (result.empty())
                                return;
                            auto [ts, vals] = result.getAllData();
                            if (ts.empty())
                                return;
                            ::InMemorySeries<T> series;
                            series.timestamps = std::move(ts);
                            series.values = std::move(vals);
                            splitStore->series[seriesId] = std::move(series);
                        };

                        switch (*typeOpt) {
                            case ::TSMValueType::Float: {
                                auto result = co_await tsm->queryWithTombstones<double>(
                                    seriesId, 0, std::numeric_limits<uint64_t>::max());
                                extractData(result);
                                break;
                            }
                            case ::TSMValueType::Boolean: {
                                auto result = co_await tsm->queryWithTombstones<bool>(
                                    seriesId, 0, std::numeric_limits<uint64_t>::max());
                                extractData(result);
                                break;
                            }
                            case ::TSMValueType::String: {
                                auto result = co_await tsm->queryWithTombstones<std::string>(
                                    seriesId, 0, std::numeric_limits<uint64_t>::max());
                                extractData(result);
                                break;
                            }
                            case ::TSMValueType::Integer: {
                                auto result = co_await tsm->queryWithTombstones<int64_t>(
                                    seriesId, 0, std::numeric_limits<uint64_t>::max());
                                extractData(result);
                                break;
                            }
                        }
                    }

                    if (!splitStore->isEmpty()) {
                        auto& seq = nextSeqPerShard[targetShard];
                        std::string destPath =
                            shardDirNew(targetShard) + "/tsm/0_split_" + std::to_string(seq++) + ".tsm";
                        co_await ::TSMWriter::runAsync(splitStore, destPath);
                        engine_log.debug("[REBALANCE] Wrote split TSM {} ({} series)", destPath, ids.size());
                    }
                }

                co_await tsm->close();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Phase D: Copy per-shard NativeIndex directories
// ---------------------------------------------------------------------------

void ShardRebalancer::moveNativeIndex() {
    // After rebalancing, series are rerouted across shards. Copying old shard N's
    // NativeIndex to new shard N would produce stale metadata: entries for series
    // that moved away, and missing entries for series that moved in. A stale index
    // is worse than an empty one (ghost results, missed discoveries).
    //
    // Instead, we leave the new shards with empty native_index/ directories
    // (already created by createStagingDirs). The NativeIndex will be rebuilt
    // incrementally as data is written/queried. Existing TSM data remains
    // queryable through direct series key lookups and scatter-gather discovery.
    unsigned oldCount = _oldShardCount > 0 ? _oldShardCount : detectShardCountFromDirs();
    engine_log.info("[REBALANCE] Skipping NativeIndex copy ({} old shards) — "
                    "index will rebuild incrementally from TSM data",
                    oldCount);
}

// ---------------------------------------------------------------------------
// Phase E: Atomic cutover
// ---------------------------------------------------------------------------

void ShardRebalancer::performCutover(unsigned oldShardCount, unsigned newShardCount) {
    // Mark renames started — this is the point of no return
    writeState({RebalancePhase::RenamesStarted, oldShardCount, newShardCount});

    // Step 1: Rename old shard dirs to _old
    for (unsigned s = 0; s < oldShardCount; ++s) {
        std::string src = shardDir(s);
        std::string dst = shardDirOld(s);
        if (fs::exists(src)) {
            fs::rename(src, dst);
        }
    }

    // Step 2: Rename new staging dirs to final names
    for (unsigned s = 0; s < newShardCount; ++s) {
        std::string src = shardDirNew(s);
        std::string dst = shardDir(s);
        if (fs::exists(src)) {
            fs::rename(src, dst);
        }
    }

    // fsync the data directory to persist all renames — without this,
    // power loss can silently lose the directory entry updates
    int dirfd = ::open(_dataDir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirfd >= 0) {
        ::fsync(dirfd);
        ::close(dirfd);
    }

    writeState({RebalancePhase::Complete, oldShardCount, newShardCount});
}

void ShardRebalancer::completeCutover(unsigned oldShardCount, unsigned newShardCount) {
    // Some renames may have completed before the crash. Finish them.

    // Move any remaining old shard dirs to _old
    for (unsigned s = 0; s < oldShardCount; ++s) {
        std::string src = shardDir(s);
        if (!fs::exists(src))
            continue;
        std::string oldDst = shardDirOld(s);
        if (s >= newShardCount) {
            // Scale-down: no _new dir exists for this shard. Archive unconditionally
            // to prevent orphaned directories that break future detectShardCountFromDirs().
            fs::rename(src, oldDst);
        } else {
            std::string newSrc = shardDirNew(s);
            if (fs::exists(newSrc)) {
                fs::rename(src, oldDst);
            }
        }
    }

    // Move any remaining staging dirs to final
    for (unsigned s = 0; s < newShardCount; ++s) {
        std::string src = shardDirNew(s);
        std::string dst = shardDir(s);
        if (fs::exists(src) && !fs::exists(dst)) {
            fs::rename(src, dst);
        }
    }
}

// ---------------------------------------------------------------------------
// Phase F: Cleanup
// ---------------------------------------------------------------------------

void ShardRebalancer::cleanup(unsigned oldShardCount) {
    for (unsigned s = 0; s < oldShardCount; ++s) {
        std::string oldDir = shardDirOld(s);
        if (fs::exists(oldDir)) {
            std::error_code ec;
            fs::remove_all(oldDir, ec);
            if (ec) {
                engine_log.warn("[REBALANCE] Failed to remove {}: {}", oldDir, ec.message());
            }
        }
    }

    // Also clean up any leftover staging dirs
    std::regex newPattern("shard_\\d+_new");
    if (fs::exists(_dataDir)) {
        for (const auto& entry : fs::directory_iterator(_dataDir)) {
            if (entry.is_directory() && std::regex_match(entry.path().filename().string(), newPattern)) {
                std::error_code ec;
                fs::remove_all(entry.path(), ec);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Crash recovery
// ---------------------------------------------------------------------------

seastar::future<> ShardRebalancer::recoverIfNeeded(unsigned newShardCount) {
    auto state = readState();

    switch (state.phase) {
        case RebalancePhase::None:
            co_return;

        case RebalancePhase::InProgress:
            // Crash during WAL/TSM processing. Old dirs are intact.
            // Delete any partial staging dirs and restart from scratch.
            engine_log.warn(
                "[REBALANCE] Recovering from interrupted rebalance "
                "(phase: InProgress, {}->{})",
                state.oldShardCount, state.newShardCount);
            for (unsigned s = 0; s < state.newShardCount; ++s) {
                std::error_code ec;
                fs::remove_all(shardDirNew(s), ec);
            }
            removeState();
            // Set _oldShardCount so execute() knows the old count
            _oldShardCount = state.oldShardCount;
            // Now execute the full rebalance
            co_await execute(newShardCount);
            co_return;

        case RebalancePhase::RenamesStarted:
            // Crash during directory renames. Complete them.
            engine_log.warn(
                "[REBALANCE] Recovering from interrupted rename "
                "(phase: RenamesStarted, {}->{})",
                state.oldShardCount, state.newShardCount);
            co_await seastar::async([this, &state] {
                completeCutover(state.oldShardCount, state.newShardCount);
                cleanup(state.oldShardCount);
                writeShardCountMeta(_dataDir, state.newShardCount);
                removeState();
            });
            engine_log.info("[REBALANCE] Recovery complete (rename phase)");
            co_return;

        case RebalancePhase::Complete:
            // Crash after renames but before cleanup. Just clean up.
            engine_log.warn(
                "[REBALANCE] Recovering from interrupted cleanup "
                "(phase: Complete, {}->{})",
                state.oldShardCount, state.newShardCount);
            co_await seastar::async([this, &state] {
                cleanup(state.oldShardCount);
                writeShardCountMeta(_dataDir, state.newShardCount);
                removeState();
            });
            engine_log.info("[REBALANCE] Recovery complete (cleanup phase)");
            co_return;
    }
}

// ---------------------------------------------------------------------------
// Main execute
// ---------------------------------------------------------------------------

seastar::future<> ShardRebalancer::execute(unsigned newShardCount) {
    unsigned oldShardCount = _oldShardCount;
    if (oldShardCount == 0) {
        oldShardCount = detectShardCountFromDirs();
    }
    if (oldShardCount == 0 || oldShardCount == newShardCount) {
        co_return;
    }

    engine_log.info("[REBALANCE] Starting shard rebalance: {} -> {} shards", oldShardCount, newShardCount);

    auto startTime = std::chrono::steady_clock::now();

    // Mark rebalance in progress
    co_await seastar::async([this, oldShardCount, newShardCount] {
        writeState({RebalancePhase::InProgress, oldShardCount, newShardCount});
        createStagingDirs(newShardCount);
    });

    // Phase A: Process WAL files
    engine_log.info("[REBALANCE] Phase A: Processing WAL files...");
    co_await processWALFiles(oldShardCount, newShardCount);

    // Phase B+C: Analyze and process TSM files
    engine_log.info("[REBALANCE] Phase B+C: Processing TSM files...");
    co_await processTSMFiles(oldShardCount, newShardCount);

    // Phase D: Copy per-shard NativeIndex directories
    engine_log.info("[REBALANCE] Phase D: Copying per-shard NativeIndex directories...");
    co_await seastar::async([this] { moveNativeIndex(); });

    // Phase E: Atomic cutover
    engine_log.info("[REBALANCE] Phase E: Performing directory cutover...");
    co_await seastar::async([this, oldShardCount, newShardCount] { performCutover(oldShardCount, newShardCount); });

    // Phase F: Cleanup
    engine_log.info("[REBALANCE] Phase F: Cleaning up old shard directories...");
    co_await seastar::async([this, oldShardCount, newShardCount] {
        cleanup(oldShardCount);
        writeShardCountMeta(_dataDir, newShardCount);
        removeState();
    });

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - startTime);
    engine_log.info("[REBALANCE] Shard rebalance complete ({} -> {} shards) in {}ms", oldShardCount, newShardCount,
                    elapsed.count());
}

}  // namespace timestar
