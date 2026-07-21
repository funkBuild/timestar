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
    return timestar::shardDataPath(shardId) + "/tsm/";
}

seastar::future<> TSMFileManager::init() {
    timestar::tsm_log.info("TSMFileManager init. shardId={}", shardId);

    // Initialize compactor
    compactor = std::make_unique<TSMCompactor>(this);

    // Seed the per-tier reclaim rates shallow-first, so the first scheduling
    // decisions already match the measured prior (tier 0 reclaims ~20 MB/s vs
    // tier 1's ~17.6 MB/s on a duplicate-bearing workload) instead of treating
    // all tiers as equal until enough merges have run to tell them apart. These
    // are starting points only -- recordReclaimRate() replaces them with this
    // shard's actual behaviour, at a 0.3 EWMA, within a few merges.
    for (uint64_t t = 0; t < MAX_TIERS; ++t) {
        tierReclaimRate_[t] = 20.0e6 / static_cast<double>(t + 1);
    }

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
        // NOTE: this is a SKIP, not a failure. The file stays on disk but is never
        // registered in sequencedTsmFiles/tiers, so its data silently disappears
        // from query results and it is never compacted or reclaimed. Make that
        // unmistakable in the log -- it is the difference between "one bad file"
        // and "this node is serving incomplete data".
        timestar::tsm_log.error(
            "DATA UNAVAILABLE: failed to open TSM file {}: {}. This file's data will NOT be served by queries "
            "and the file will not be compacted or deleted.",
            path, e.what());
        co_return;
    }
}

seastar::future<uint64_t> TSMFileManager::writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier) {
    // Honour a rollover-time reservation when present -- see
    // reserveSequenceId() for why assigning here instead would invert LWW
    // ranking under concurrent conversions. Fresh assignment remains for
    // callers that convert sequentially (startup recovery, tests).
    const uint64_t seqNum = memStore->reservedTsmSeq.has_value() ? *memStore->reservedTsmSeq : nextSequenceId++;

    std::string filename = basePath() + std::to_string(tier) + "_" + std::to_string(seqNum) + ".tsm";

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

    uint64_t bytesWritten = 0;
    try {
        bytesWritten = co_await seastar::file_size(filename);
    } catch (...) {
        // Size is for reporting only; never fail a conversion over it.
    }

    co_await openTsmFile(filename);

    // Deliberately does NOT trigger compaction inline. writeMemstore runs on the
    // WAL conversion fiber while _conversionSemaphore (capacity 1) is held, and
    // the caller only unlinks the WAL file once this returns. Awaiting a tier
    // merge here meant a 76s tier-3 merge held the shard's single conversion
    // slot for 76s: WAL disk was not reclaimed, the unconverted-store backlog
    // hit its limit, and rollover blocked -- surfacing to clients as write
    // timeouts. Tier merges now run on the background loop in the low-priority
    // compaction group, where they are preemptible and cannot block ingest.
    // Conversion is bounded by store size (~64MB) rather than tier depth.
    co_return bytesWritten;
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

seastar::future<bool> TSMFileManager::compactOneTier(uint64_t tier) {
    if (!shouldCompactTier(tier)) {
        co_return false;
    }
    // Back off after a failure instead of retrying immediately. A failed
    // compaction re-reads and re-processes its entire input set before
    // throwing, so an un-throttled retry loop (measured at ~6 attempts
    // per second) starves foreground I/O for as long as the tier stays
    // stuck -- which, once it is stuck, is forever.
    auto& failureState = tierFailures_[tier];
    if (failureState.cooldownCycles > 0) {
        --failureState.cooldownCycles;
        co_return false;
    }

    timestar::compactor_log.info("Tier {} needs compaction ({} files)", tier, getFileCountInTier(tier));

    auto plan = compactor->planCompaction(tier);
    if (!plan.isValid()) {
        co_return false;
    }
    try {
        CompactionStats stats;
        if (_compactionGroupSet) {
            // Run compaction under the low-priority scheduling group so
            // query I/O gets preferential disk bandwidth.
            //
            // executeCompaction takes the plan BY VALUE. Passing it as an
            // argument here is NOT what makes this safe -- the previous version
            // did the same thing and still crashed, because the lambda is not a
            // coroutine: it returned the moment executeCompaction first
            // suspended, destroying `p` while the callee still referenced it.
            // Ownership has to transfer into the coroutine frame, which is why
            // the parameter is by value and this moves into it.
            stats = co_await seastar::with_scheduling_group(
                _compactionGroup, [this](CompactionPlan p) { return compactor->executeCompaction(std::move(p)); },
                std::move(plan));
        } else {
            stats = co_await compactor->executeCompaction(plan);
        }
        ++completedCompactions_;
        failureState.consecutive = 0;
        failureState.cooldownCycles = 0;
        recordReclaimRate(tier, stats.bytesRead, stats.bytesWritten, stats.duration.count());
        // Log bytes in/out, not just duration. Whether tier merges deserve
        // priority over WAL conversion is a question about SPACE RECLAIMED PER
        // SECOND, and duration alone cannot answer it.
        const double reclaimedPct =
            stats.bytesRead > 0 ? 100.0 * (1.0 - static_cast<double>(stats.bytesWritten) / stats.bytesRead) : 0.0;
        timestar::compactor_log.info(
            "Compacted {} files from tier {} to tier {} in {}ms | {} -> {} bytes ({:.1f}% reclaimed, {} dupes)",
            stats.filesCompacted, tier, tier + 1, stats.duration.count(), stats.bytesRead, stats.bytesWritten,
            reclaimedPct, stats.duplicatesRemoved);
    } catch (const std::exception& e) {
        ++failureState.consecutive;
        ++totalCompactionFailures_;
        // Exponential backoff capped at MAX_COMPACTION_BACKOFF_CYCLES.
        failureState.cooldownCycles =
            std::min<uint64_t>(1ull << std::min<uint64_t>(failureState.consecutive, 10), MAX_COMPACTION_BACKOFF_CYCLES);
        timestar::compactor_log.error(
            "Compaction failed for tier {}: {}. Consecutive failures: {}. Backing off {} "
            "cycles.",
            tier, e.what(), failureState.consecutive, failureState.cooldownCycles);
    }
    co_return true;
}

void TSMFileManager::recordReclaimRate(uint64_t tier, uint64_t bytesRead, uint64_t bytesWritten, int64_t durationMs) {
    if (tier >= MAX_TIERS || durationMs <= 0 || bytesRead == 0) {
        return;
    }
    // Bytes reclaimed per second. Can legitimately be ~0 on an append-only
    // workload (nothing to dedupe), which is the signal that this tier's merges
    // are buying read amplification only -- not space.
    //
    // Clamped at zero: output CAN exceed input (measured -0.9% on append-only
    // conversions -- re-encoding overhead with no duplicates to remove), and a
    // negative rate breaks higherValueTierPending's multiplicative margins.
    // With mine = -1 MB/s, `other > mine * 1.2` compares against -1.2 MB/s, so
    // a merely-less-negative deep tier reads as "higher value" and tier 0
    // yields to it -- inverting the priority exactly on the workloads where
    // no tier reclaims anything. Zero is also the truthful floor: a merge
    // that grew the data reclaimed nothing, not negative space.
    const double reclaimed = std::max(0.0, static_cast<double>(bytesRead) - static_cast<double>(bytesWritten));
    const double rate = reclaimed / (static_cast<double>(durationMs) / 1000.0);
    constexpr double kAlpha = 0.3;  // favour recent behaviour; workloads shift
    tierReclaimRate_[tier] = (kAlpha * rate) + ((1.0 - kAlpha) * tierReclaimRate_[tier]);
}

bool TSMFileManager::higherValueTierPending(uint64_t tier) const {
    const double mine = tierReclaimRate_[tier];
    for (uint64_t t = 0; t < MAX_TIERS - 1; ++t) {
        if (t == tier || !shouldCompactTier(t)) {
            continue;
        }
        // Require a clear margin before yielding, so two tiers with
        // near-identical rates do not ping-pong and stall each other.
        if (tierReclaimRate_[t] > mine * 1.2) {
            return true;
        }
        // Tie-break on depth while rates are close (including at startup, when
        // both are still at their seeded values): shallower merges are cheaper
        // per byte and tier 0 is what query read amplification depends on.
        if (t < tier && tierReclaimRate_[t] >= mine * 0.8) {
            return true;
        }
    }
    return false;
}

const char* TSMFileManager::compactionYieldReason(uint64_t tier) const {
    // Escape valve, checked first: never defer indefinitely. Tier 0 growing
    // without bound degrades every query on the shard, so past the ceiling we
    // merge regardless of what else is pending.
    if (getFileCountInTier(0) >= compactionStarvationCeiling()) {
        return nullptr;
    }

    // WAL-first priority, on two independent grounds:
    //
    //  1. SPACE. Measured on a duplicate-bearing workload, WAL->TSM reclaims
    //     14.8% (0.99 -> 0.84 GB). Overwritten points are resolved as the store
    //     is folded into columnar form, so conversion is where last-write-wins
    //     duplicates actually stop costing disk.
    //  2. MEMORY. A retained memory store holds its full UNCOMPRESSED dataset in
    //     RAM (~64MB), and conversion is the ONLY thing that frees it. Tier
    //     merges free none. The unconverted-store backlog is what forces writes
    //     to be shed, so this is a hard constraint, not a preference.
    //
    // Ground 2 is why this outranks the reclaim-rate comparison below rather
    // than competing in it: on an append-only workload conversion reclaims
    // almost nothing (measured -0.9%, because the WAL is already encoded with
    // the same ALP/Simple8b/zstd codecs TSM uses) yet must still run first, or
    // ingest stalls. Ranking it purely on bytes would starve it exactly when
    // there are no duplicates to reclaim.
    if (walConversionPending_ && walConversionPending_()) {
        return "WAL conversion pending";
    }

    // Among tiers, yield to whichever reclaims space fastest. Measured with
    // duplicates present, shallow tiers do reclaim more (tier 0: 27.7%,
    // 20.0 MB/s; tier 1: 9.3%, 17.6 MB/s), so depth seeds the ordering -- but
    // the live measurement governs, so this adapts if a workload's duplicates
    // cluster deeper. Without this the fibers would contend blindly for the
    // shared merge slot and a low-value deep merge could hold it while tier 0
    // piles up.
    if (higherValueTierPending(tier)) {
        return "higher-reclaim tier pending";
    }
    return nullptr;
}

seastar::future<> TSMFileManager::startCompactionLoop() {
    // Built on compactOneTier() rather than TSMCompactor::runCompactionLoop():
    // only this path carries the per-tier failure backoff, the completed/failed
    // counters behind the Prometheus gauges, and the compaction scheduling
    // group.
    if (!compactor || !tierTasks_.empty()) {
        co_return;
    }
    compactionLoopEnabled_ = true;
    compactionAbort_.emplace();
    tierTasks_.reserve(MAX_TIERS - 1);
    for (uint64_t tier = 0; tier < MAX_TIERS - 1; ++tier) {
        // Start the fiber INSIDE the compaction group, not just the merge it
        // eventually calls. Wrapping only executeCompaction left the loop, its
        // planning and all its logging in `main` -- which is what the field
        // report used as evidence ("256 occurrences of [shard 1:main]
        // timestar.compactor"). The heavy work was correctly placed but the
        // diagnostic still said otherwise, so the fix was unverifiable from
        // outside the process.
        if (_compactionGroupSet) {
            tierTasks_.push_back(
                seastar::with_scheduling_group(_compactionGroup, [this, tier] { return tierCompactionLoop(tier); }));
        } else {
            tierTasks_.push_back(tierCompactionLoop(tier));
        }
    }
    co_return;
}

seastar::future<> TSMFileManager::tierCompactionLoop(uint64_t tier) {
    while (compactionLoopEnabled_) {
        bool didWork = false;
        try {
            if (shouldCompactTier(tier)) {
                if (const char* reason = compactionYieldReason(tier)) {
                    // Log once per deferral streak, not per poll. At info, not
                    // debug: this is the prioritisation actually making a
                    // decision, and at debug the entire WAL-first policy was
                    // invisible in a default-configured run.
                    if (++deferredCycles_[tier] == 1) {
                        timestar::compactor_log.info("Tier {} deferring: {} ({} files in tier {}, {} in tier 0)", tier,
                                                     reason, getFileCountInTier(tier), tier, getFileCountInTier(0));
                    }
                } else {
                    if (deferredCycles_[tier] > 0) {
                        timestar::compactor_log.info("Tier {} resuming after {} deferred cycles", tier,
                                                     deferredCycles_[tier]);
                    }
                    deferredCycles_[tier] = 0;
                    didWork = co_await compactOneTier(tier);
                }
            }
        } catch (const std::exception& e) {
            timestar::compactor_log.error("Tier {} compaction cycle failed: {}", tier, e.what());
        }
        if (!compactionLoopEnabled_) {
            break;
        }
        // Poll briskly while this tier has work (including while deferring, so
        // it starts the moment the higher-priority work clears); back off when
        // idle so a quiet shard is not woken continuously. Deeper tiers poll
        // slower -- measured, they need it far less often (2 tier-2 merges vs
        // 77 tier-0 merges in the same run). Abortable so shutdown does not
        // wait out an idle sleep.
        const bool busy = didWork || deferredCycles_[tier] > 0;
        const auto pollInterval =
            busy ? std::chrono::milliseconds(50 * (tier + 1)) : std::chrono::milliseconds(2000 * (tier + 1));
        try {
            co_await seastar::sleep_abortable(pollInterval, *compactionAbort_);
        } catch (const seastar::sleep_aborted&) {
            break;
        }
    }
}

seastar::future<> TSMFileManager::stopCompactionLoop() {
    compactionLoopEnabled_ = false;
    if (compactionAbort_.has_value() && !compactionAbort_->abort_requested()) {
        compactionAbort_->request_abort();
    }
    if (compactor) {
        compactor->stopCompaction();
    }
    // Every tier fiber must be drained before this returns -- they capture
    // `this` and touch the file collections, so leaving one running past
    // shutdown is a use-after-free. Drained unconditionally: the fibers are
    // owned by this class, so they must be awaited even if `compactor` was
    // never constructed.
    auto tasks = std::exchange(tierTasks_, {});
    for (auto& t : tasks) {
        try {
            co_await std::move(t);
        } catch (const std::exception& e) {
            timestar::compactor_log.error("Tier compaction fiber failed during shutdown: {}", e.what());
        }
    }
    co_return;
}