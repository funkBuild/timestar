#pragma once

#include "aligned_buffer.hpp"
#include "memory_store.hpp"
#include "timestar_config.hpp"
#include "tsm.hpp"

#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <vector>

// Forward declaration
class TSMCompactor;

class TSMFileManager {
private:
    static constexpr size_t MAX_TIERS = 5;
    // Upper bound on the per-tier retry backoff, in compaction cycles.
    static constexpr uint64_t MAX_COMPACTION_BACKOFF_CYCLES = 64;
    static size_t filesPerCompaction() { return timestar::config().storage.compaction.tier0_min_files; }

    int shardId;
    // No atomic needed: TSMFileManager is a per-shard object in Seastar's shard-per-core model,
    // only accessed from a single thread.
    uint64_t nextSequenceId = 0;
    // Track files by tier for compaction
    std::vector<seastar::shared_ptr<TSM>> tiers[MAX_TIERS];

    // Compactor (unique ownership, destructor defined in .cpp where TSMCompactor is complete)
    std::unique_ptr<TSMCompactor> compactor;

    // Counter for completed compactions (read by Engine to update Prometheus metrics)
    uint64_t completedCompactions_ = 0;

    // Per-tier failure tracking, used both for operator visibility and to back
    // off retries. A failing compaction re-reads its whole input set before
    // throwing, so retrying it at full speed burns CPU and disk continuously.
    struct TierFailureState {
        uint64_t consecutive = 0;
        // Skip this many compaction cycles before trying this tier again.
        uint64_t cooldownCycles = 0;
    };
    std::array<TierFailureState, MAX_TIERS> tierFailures_{};
    uint64_t totalCompactionFailures_ = 0;

    // I/O scheduling group for compaction (lower priority than queries).
    // Set by Engine via setCompactionGroup() after scheduling groups are created.
    seastar::scheduling_group _compactionGroup;
    bool _compactionGroupSet = false;

    // I/O scheduling group for WAL->TSM conversion, at higher shares than
    // _compactionGroup. Conversion frees WAL disk and drains the ingest
    // backlog, so it must not queue behind a deep tier merge.
    seastar::scheduling_group _flushGroup;
    bool _flushGroupSet = false;

    seastar::future<> openTsmFile(std::string path);
    std::string basePath();

    // Compact one tier if it is eligible and not in failure backoff.
    // Returns true if a merge actually ran.
    seastar::future<bool> compactOneTier(uint64_t tier);

    // One INDEPENDENT FIBER PER TIER.
    //
    // Measured on a 1.3B-point ingest (2 shards): tier 0 accounted for 77 of 89
    // merges and carried the worst latency by a wide margin (7920ms max, vs
    // 839ms for tier 2) -- it is both the highest-volume and the burstiest tier,
    // because every WAL conversion lands a new file there. A single shared loop
    // makes every other tier wait behind whichever merge is running, so a
    // multi-second tier-0 merge stalls tier 2 and vice versa, and tier 0 (which
    // gates query read amplification) is exactly what you least want blocked.
    //
    // Independent fibers remove that coupling: each tier makes progress on its
    // own schedule. They are NOT unbounded parallelism -- every merge still
    // takes a slot from TSMCompactor's compactionSemaphore, so total concurrent
    // merge I/O is capped, and deeper tiers yield to shallower ones for that
    // slot (see shallowerTierNeedsCompaction).
    //
    // A member coroutine, not a lambda: a lambda coroutine's closure is not kept
    // alive by the frame, so its captures dangle at the first suspension.
    seastar::future<> tierCompactionLoop(uint64_t tier);

    // True if some other eligible tier reclaims space FASTER than `tier`, so
    // `tier` should yield the shared merge slot to it.
    //
    // Driven by measured MB-reclaimed-per-second (tierReclaimRate_), not by
    // depth alone. Measured on a duplicate-heavy workload, shallow tiers do
    // reclaim more -- tier 0 at 27.7% / 20.0 MB/s vs tier 1 at 9.3% /
    // 17.6 MB/s -- so depth is the right PRIOR, and it seeds the rates below.
    // But it is only a prior: on a workload whose duplicates cluster at a
    // different depth the ordering should follow the bytes, not the assumption.
    bool higherValueTierPending(uint64_t tier) const;

    // EWMA of bytes reclaimed per second, per tier. Seeded shallow-first so the
    // very first decisions match the measured prior before any local data.
    std::array<double, MAX_TIERS> tierReclaimRate_{};
    // Takes raw values rather than CompactionStats: that type lives in
    // tsm_compactor.hpp, which this header must not pull in (the compactor
    // includes this one).
    void recordReclaimRate(uint64_t tier, uint64_t bytesRead, uint64_t bytesWritten, int64_t durationMs);

    // Whether tier work should yield right now (WAL conversion pending, or a
    // shallower tier is waiting). Returns the reason for logging, or nullptr.
    const char* compactionYieldReason(uint64_t tier) const;

    std::vector<seastar::future<>> tierTasks_;
    bool compactionLoopEnabled_ = false;
    std::array<uint64_t, MAX_TIERS> deferredCycles_{};

    // Interrupts fiber sleeps at shutdown. Without it, stopCompactionLoop()
    // waits out the longest idle sleep (2s * (tier+1), i.e. up to 8s), adding
    // that to every shard's shutdown. Recreated on each start so a
    // stop/start cycle (tests) gets a fresh, un-aborted source.
    std::optional<seastar::abort_source> compactionAbort_;

    // Returns true while memory stores are awaiting WAL->TSM conversion.
    // Registered by WALFileManager; unset in tests that construct a
    // TSMFileManager standalone, in which case no deferral happens.
    std::function<bool()> walConversionPending_;

    // Tier-0 file count at which tier compaction stops yielding to WAL
    // conversion. Bounds query read amplification during a sustained burst.
    static size_t compactionStarvationCeiling() {
        return timestar::config().storage.compaction.tier0_starvation_ceiling;
    }

    std::map<uint64_t, seastar::shared_ptr<TSM>> sequencedTsmFiles;

public:
    // Read-only access to the sequenced TSM file map.
    const std::map<uint64_t, seastar::shared_ptr<TSM>>& getSequencedTsmFiles() const { return sequencedTsmFiles; }

    // Direct insertion into the sequenced file map (for testing).
    void setSequencedTsmFile(uint64_t seq, seastar::shared_ptr<TSM> tsm) { sequencedTsmFiles[seq] = std::move(tsm); }

    TSMFileManager();
    ~TSMFileManager();  // Defined in .cpp where TSMCompactor is complete

    seastar::future<> init();
    seastar::future<> stop();
    // Returns the size in bytes of the TSM file produced, so callers can report
    // how much space the WAL->TSM conversion reclaimed.
    seastar::future<uint64_t> writeMemstore(seastar::shared_ptr<MemoryStore> memStore, uint64_t tier = 0);
    std::optional<TSMValueType> getSeriesType(const std::string& seriesKey);
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

    // Compaction support
    std::vector<seastar::shared_ptr<TSM>> getFilesInTier(uint64_t tier) const;
    size_t getFileCountInTier(uint64_t tier) const;
    bool shouldCompactTier(uint64_t tier) const;
    seastar::future<> addTSMFile(seastar::shared_ptr<TSM> file);
    seastar::future<> removeTSMFiles(const std::vector<seastar::shared_ptr<TSM>>& files);

    // Allocate a globally unique sequence ID for new TSM files
    uint64_t allocateSequenceId() { return nextSequenceId++; }

    // Get the compactor (for tombstone rewrites)
    TSMCompactor* getCompactor() { return compactor.get(); }

    // Number of compactions completed since startup (for Prometheus metrics)
    uint64_t getCompletedCompactions() const { return completedCompactions_; }

    // --- Compaction health (for Prometheus / operator visibility) ---
    // A tier that can never merge is otherwise invisible until the server
    // starts refusing writes: /health kept reporting "healthy" while a tier
    // grew without bound and the compactor retried ~6x/second forever.
    uint64_t getConsecutiveFailures(uint64_t tier) const {
        return tier < MAX_TIERS ? tierFailures_[tier].consecutive : 0;
    }
    uint64_t getTotalCompactionFailures() const { return totalCompactionFailures_; }
    static constexpr uint64_t maxTiers() { return MAX_TIERS; }
    // Deepest tier currently at or over its compaction threshold, -1 if none.
    int getDeepestBackloggedTier() const {
        for (int tier = static_cast<int>(MAX_TIERS) - 2; tier >= 0; --tier) {
            if (shouldCompactTier(static_cast<uint64_t>(tier))) {
                return tier;
            }
        }
        return -1;
    }

    // Set the I/O scheduling group for background compaction.
    void setCompactionGroup(seastar::scheduling_group sg) {
        _compactionGroup = sg;
        _compactionGroupSet = true;
    }
    bool hasCompactionGroup() const { return _compactionGroupSet; }
    seastar::scheduling_group compactionGroup() const { return _compactionGroup; }

    // Set the I/O scheduling group for WAL->TSM conversion.
    void setFlushGroup(seastar::scheduling_group sg) {
        _flushGroup = sg;
        _flushGroupSet = true;
    }
    bool hasFlushGroup() const { return _flushGroupSet; }
    seastar::scheduling_group flushGroup() const { return _flushGroup; }

    // Register the WAL-backlog probe used to prioritise conversion over merges.
    void setWalConversionProbe(std::function<bool()> probe) { walConversionPending_ = std::move(probe); }

    // Reserve a TSM sequence number ahead of writeMemstore().
    //
    // LWW correctness rests on the invariant that a flush file's seqNum (which
    // is its dataSeq, hence its duplicate-resolution rank) follows STORE WRITE
    // ORDER. With conversions running concurrently, the store that finishes
    // close() first reaches writeMemstore() first -- so assigning the seq there
    // let a NEWER store take a LOWER seq, and queries then preferred the older
    // value on duplicate timestamps. The rollover critical section is the only
    // place write order is still serialized, so the seq must be taken there and
    // carried on the store (MemoryStore::reservedTsmSeq). The 30s conversion
    // retry keeps the same reservation for the same reason: a fresh seq at
    // retry time would outrank every store converted in the interim.
    uint64_t reserveSequenceId() { return nextSequenceId++; }

    // Start background compaction
    seastar::future<> startCompactionLoop();
    seastar::future<> stopCompactionLoop();

    // Per-shard query I/O semaphore.  Gates concurrent DMA reads during
    // query execution to prevent reactor stalls from unbounded parallel I/O.
    // Series-level concurrency stays high (64) for CPU-bound work; only
    // actual disk reads are throttled.  Default 16 matches typical NVMe
    // queue depth while keeping the pipeline full.
    seastar::semaphore queryIoSem{16};
};
