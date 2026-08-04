#pragma once

#include "query_parser.hpp"  // AggregationMethod enum
#include "retention_policy.hpp"
#include "series_compaction_data.hpp"  // Phase 3: Parallel series processing
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "tsm.hpp"
#include "tsm_result.hpp"
#include "tsm_writer.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>
#include <string>
#include <unordered_map>
#include <vector>

// Forward declarations
class CompactionStrategy;
class TSMFileManager;

// Compaction statistics for monitoring
struct CompactionStats {
    uint64_t filesCompacted = 0;
    uint64_t bytesRead = 0;
    uint64_t bytesWritten = 0;
    uint64_t pointsRead = 0;
    uint64_t pointsWritten = 0;
    uint64_t duplicatesRemoved = 0;
    std::chrono::milliseconds duration{0};
};

// Result from compact(): output path and stats, avoiding shared mutable state.
struct CompactionResult {
    std::string outputPath;
    CompactionStats stats;
};

// Retention context for ONE compaction, resolved at execute time.
//
// Deliberately not a "set this before you trigger" handshake. The compactor
// runs up to compactionSemaphore concurrent merges driven by one fiber per
// tier, so any state parked on the compactor for "the next" compaction can be
// consumed by a different plan than the one it was built for. This bundle is
// produced per plan, from that plan's own series ids, and flows straight into
// compact() as arguments.
struct RetentionCompactionContext {
    std::unordered_map<std::string, RetentionPolicy> policies;
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurement;
    // seriesId -> FIELD name, populated ONLY for series whose measurement's
    // policy names that field in `fieldMethods` (Phase 4). Deliberately sparse:
    // a policy with no per-field overrides leaves this empty and costs the
    // provider not one extra index read, so the default path is unchanged.
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesField;
};

// Injected by Engine at startup (see Engine::init). Takes the series ids a
// compaction plan touches and resolves the retention context for exactly
// those. Async because resolution may consult the shard's index.
//
// An exception from the provider FAILS the compaction: compacting without
// retention when a policy exists is silent data retention loss (expired points
// survive) and silent non-downsampling — precisely the defect this replaced.
using RetentionContextProvider =
    std::function<seastar::future<RetentionCompactionContext>(const std::vector<SeriesId128>&)>;

// Cheap synchronous "could retention apply on this shard at all?" probe,
// injected alongside the provider (same shape as setWalConversionProbe).
//
// The provider's own fast path already returns empty maps without an index
// read, but reaching it still costs getAllSeriesIds() over every source file's
// sparse index — O(series) per merge, ~500k hash inserts on a 128k-series
// shard, paid on every merge of a shard that has no policy at all. This probe
// lets executeCompaction skip that work entirely. Unset means "assume active".
using RetentionActiveProbe = std::function<bool()>;

// ONE STAGE of a downsample cascade, resolved for a compaction at execute time.
//
// Defined in lib/retention/retention_policy.hpp alongside the builder that
// produces it: Engine::sweepDownsampleRewrites() must decide "is a fold worth
// scheduling?" from exactly the stages the compactor will then apply, and two
// copies of that arithmetic would drift into either an endless rewrite loop or
// a quiet series that never folds.
using DownsampleStage = timestar::retention::DownsampleStage;

// Cascade depth cap; mirrors timestar::retention::kMaxDownsampleTiers. Kept as
// a fixed array bound so a per-series retention context costs no allocation.
inline constexpr size_t MAX_DOWNSAMPLE_STAGES = timestar::retention::kMaxDownsampleTiers;

// Represents a plan for compacting a set of TSM files
struct CompactionPlan {
    std::vector<seastar::shared_ptr<TSM>> sourceFiles;
    uint64_t targetTier;
    uint64_t targetSeqNum;
    std::string targetPath;
    uint64_t estimatedSize;

    bool isValid() const { return !sourceFiles.empty() && !targetPath.empty(); }
};

class TSMCompactor {
private:
    // Compaction limits and thresholds — read from TOML config at runtime.
    static size_t batchSize() { return timestar::config().storage.compaction.batch_size; }

    TSMFileManager* fileManager;
    std::unique_ptr<CompactionStrategy> strategy;
    // lastCompactStats removed — compact() now returns stats via CompactionResult
    seastar::semaphore compactionSemaphore{2};  // Re-initialized in constructor from config
    // Units are KiB of estimated decoded bytes; bounds total concurrent
    // coalescing work across all series in flight on this shard.
    seastar::semaphore coalesceBudget_{COALESCE_TOTAL_BUDGET_BYTES / 1024};
    bool compactionEnabled{true};

    // Per-series retention context built in compact(), passed to processSeriesForCompaction
    struct SeriesRetentionContext {
        uint64_t ttlCutoff = 0;  // Points with ts < ttlCutoff are expired

        // The downsample CASCADE, ordered FINEST FIRST (stages[0] is the tier a
        // point reaches first as it ages: the largest threshold, the smallest
        // interval). Thresholds are non-increasing and intervals strictly
        // increasing across k.
        //
        // A point's stage is the LARGEST k with `ts < stages[k].threshold`;
        // a point with `ts >= stages[0].threshold` is raw and passes through.
        //
        // Because validation forces interval[k] to divide interval[k+1] and
        // every threshold is aligned down to its own interval, the bucket START
        // timestamps produced by stage k+1 are all strictly below those produced
        // by stage k, which are all strictly below the first raw point. The fold
        // output is therefore ascending without sorting — the property the
        // streaming writer depends on.
        std::array<DownsampleStage, MAX_DOWNSAMPLE_STAGES> stages{};
        uint8_t stageCount = 0;
        // The fold method for the series this context is resolved FOR. With
        // per-field overrides (Phase 4) this is the tier method only when the
        // series' field names none.
        timestar::AggregationMethod downsampleMethod = timestar::AggregationMethod::AVG;

        // May a Boolean/String series fold at all? True ONLY when the series'
        // field carries an explicit `fieldMethods` entry of "latest".
        //
        // Deliberately NOT `downsampleMethod == LATEST`: a measurement-wide
        // `"method": "latest"` must leave non-numeric series passing through
        // exactly as it did before Phase 4. Opting a status word into
        // destruction has to be an explicit act.
        bool nonNumericFold = false;

        // Boundary between "folds at some stage" and "passes through raw".
        uint64_t finestThreshold() const { return stageCount > 0 ? stages[0].threshold : 0; }
    };
    // seriesId -> the context resolved for its (MEASUREMENT, FIELD). A pointer,
    // not a copy: every series under one measurement-and-field shares one
    // context, and a merge on a fully-policy-covered 128k-series shard would
    // otherwise carry 128k copies of an 88-byte struct (~16 MB per merge, and
    // merges can run every few seconds). The pointees live in compact()'s
    // context table, which outlives every processSeriesForCompaction it awaits;
    // unordered_map nodes are address-stable, so a rehash of that table cannot
    // dangle these.
    using SeriesRetentionMap = std::unordered_map<SeriesId128, const SeriesRetentionContext*, SeriesId128::Hash>;

    // Resolves retention context per compaction plan. Unset (tests, standalone
    // benchmarks) means "no retention", which is safe: nothing installed it, so
    // no policy exists to be skipped.
    RetentionContextProvider retentionContextProvider_;
    RetentionActiveProbe retentionActiveProbe_;

    // Track active compactions
    struct ActiveCompaction {
        CompactionPlan plan;
        std::chrono::steady_clock::time_point startTime;
        CompactionStats stats;
    };
    std::vector<ActiveCompaction> activeCompactions;

    // Generate output filename for compacted file
    std::string generateCompactedFilename(uint64_t tier, uint64_t seqNum, uint64_t dataSeq);
    // Newest write generation across the input files — the output's dataSeq.
    static uint64_t maxDataSeqOf(const std::vector<seastar::shared_ptr<TSM>>& files);

    // Phase 3: Process series for compaction without writing (for parallel processing)
    // Sink used to emit merged points incrementally. When supplied, the merge
    // path hands off each bounded chunk instead of accumulating the whole
    // series in SeriesCompactionData::timestamps/values.
    template <typename T>
    using PointChunkSink =
        std::function<seastar::future<>(std::vector<uint64_t>&& timestamps, std::vector<T>&& values)>;

    // Points buffered before a chunk is handed to the sink. 256K points is
    // ~4 MB for double (8B ts + 8B value), i.e. bounded regardless of series
    // size, and a comfortable multiple of MaxPointsPerBlock (3000).
    static constexpr size_t MERGE_CHUNK_POINTS = 256 * 1024;

    // Largest time-connected block group the in-memory bulk mergers may decode
    // at once. A group is unbounded in principle: N input files that all span
    // the same time range (backfill / rewrite workloads) chain into a single
    // group covering the whole series, and decoding it wholesale is the
    // original compaction bad_alloc. Groups above this bound take the lazy
    // cursor merge instead, which holds ONE decoded block per source file.
    // 256 blocks ≈ 768K points ≈ 12 MB of decoded doubles.
    static constexpr size_t MAX_BUFFERED_GROUP_BLOCKS = 256;

    // Shard-wide budget (KiB units) for bytes DECODED by the bulk group
    // mergers. The per-group block bound above caps one series, but up to
    // seriesBatchSize() x 4 type pipelines merge concurrently, so per-series
    // caps alone multiply into shard-killing totals on a small-memory host.
    // Reservations are estimates (compressed bytes x expansion); a group that
    // cannot reserve takes the lazy cursor merge instead of waiting, so this
    // can never deadlock — only degrade to the bounded path.
    static size_t mergeDecodeBudgetBytes() {
        const size_t total = seastar::memory::stats().total_memory();
        if (total == 0) {
            return 128ull << 20;  // no seastar arena (unit tests) — safe default
        }
        return std::clamp<size_t>(total / 8, 32ull << 20, 256ull << 20);
    }
    seastar::semaphore mergeDecodeBudget_{mergeDecodeBudgetBytes() / 1024};

    // Concurrent series per TYPE pipeline (4 pipelines run at once). 20 was
    // benchmark-tuned for throughput (10 → 651ms, 20 → 541ms, 50 → 2533ms
    // regression), but each in-flight series holds up to a ~4 MB chunk buffer
    // plus decode state, so 80 concurrent series is untenable on a
    // small-memory shard. Scale with the shard arena: full 20 at >= 4 GB
    // (benchmark behavior unchanged), down to 4 on a ~1 GB shard.
    static size_t seriesBatchSize() {
        const size_t total = seastar::memory::stats().total_memory();
        if (total == 0) {
            return 20;  // no seastar arena (unit tests)
        }
        return std::clamp<size_t>(total / (200ull << 20), 4, 20);
    }

    // Estimated decoded:compressed expansion, used to size budget
    // reservations from index-known compressed bytes. Observed numeric
    // expansion is ~12x (16x for headroom). Strings routinely exceed that —
    // zstd on repetitive text reaches far higher ratios — so they carry a
    // larger factor AND a smaller per-series coalesce cap below.
    static constexpr uint64_t COALESCE_EXPANSION_ESTIMATE = 16;
    static constexpr uint64_t STRING_EXPANSION_ESTIMATE = 64;
    template <typename T>
    static constexpr uint64_t decodedExpansionEstimate() {
        return std::is_same_v<T, std::string> ? STRING_EXPANSION_ESTIMATE : COALESCE_EXPANSION_ESTIMATE;
    }

    template <typename T>
    // blockAlign: target output block size in points, used for chunk-spill
    // alignment and for scaling the under-full coalescing threshold. Pass
    // blockCapForTier(targetTier); 0 falls back to the config block size.
    seastar::future<SeriesCompactionData<T>> processSeriesForCompaction(
        const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
        const SeriesRetentionMap& seriesRetention, PointChunkSink<T> sink = nullptr, size_t blockAlign = 0);

    // Output block-size cap for a merge writing into `tier`: doubles per tier
    // from the config base, bounded by deep_block_points_cap. Measured on the
    // 128k-series fleet shape, flush blocks hold a few dozen points; without
    // per-tier growth the under-full coalescer stops at the flush-size cap and
    // deep tiers never reach the compression ratio the format is capable of.
    static size_t blockCapForTier(uint64_t tier) {
        const auto& cfg = timestar::config().storage;
        const size_t base = cfg.max_points_per_block;
        const size_t cap = std::max<size_t>(cfg.compaction.deep_block_points_cap, base);
        const size_t shifted = tier < 8 ? (base << tier) : cap;
        return std::min(shifted, cap);
    }

    // Phase 3: Write pre-processed series data to TSMWriter
    template <typename T>
    // Takes `data` BY VALUE: this is a coroutine that suspends on flushIfNeeded(),
    // so an rvalue-reference parameter would dangle across the suspension.
    seastar::future<> writeSeriesCompactionData(TSMWriter& writer, SeriesCompactionData<T> data,
                                                CompactionStats& stats);

    // Get all unique series IDs from files
    std::vector<SeriesId128> getAllSeriesIds(const std::vector<seastar::shared_ptr<TSM>>& files);

public:
    explicit TSMCompactor(TSMFileManager* manager);
    ~TSMCompactor() = default;

    // Install the retention context provider. Called once by Engine::init(),
    // after TSMFileManager::init() has constructed this compactor.
    void setRetentionContextProvider(RetentionContextProvider provider, RetentionActiveProbe active = nullptr) {
        retentionContextProvider_ = std::move(provider);
        retentionActiveProbe_ = std::move(active);
    }

    // Main compaction method - merges files and returns result with path + stats.
    // retentionPolicies: per-measurement policies for TTL/downsampling (empty = no retention).
    // seriesMeasurementMap: SeriesId128 -> measurement name, pre-built by caller for efficiency.
    // seriesFieldMap: SeriesId128 -> field name. Only needs entries for series
    //   whose policy names their field in `fieldMethods`; anything absent takes
    //   the tier's default method, which is exactly pre-Phase-4 behaviour.
    // targetTier/targetSeq: pre-allocated from CompactionPlan.  When called
    // without a plan (e.g. from tests), pass 0 for both and compact() will
    // compute the tier from input files and allocate a fresh sequence ID.
    //
    // The three maps are taken by const reference and consumed BEFORE the first
    // co_await in the body (the context build sits above every suspension
    // point), which is what makes the `= {}` defaults safe in a coroutine: the
    // caller's temporaries outlive every read of them. Moving the context build
    // below a suspension would break that silently.
    seastar::future<CompactionResult> compact(
        const std::vector<seastar::shared_ptr<TSM>>& files, uint64_t targetTier, uint64_t targetSeq,
        const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies = {},
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap = {},
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesFieldMap = {});

    // Convenience overload for callers without a pre-allocated plan (auto-allocates tier/seq).
    seastar::future<CompactionResult> compact(
        const std::vector<seastar::shared_ptr<TSM>>& files,
        const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies,
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap,
        const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesFieldMap = {}) {
        return compact(files, 0, 0, retentionPolicies, seriesMeasurementMap, seriesFieldMap);
    }

    // Convenience overload: compact files with no retention and auto-allocated tier/seq.
    seastar::future<CompactionResult> compact(const std::vector<seastar::shared_ptr<TSM>>& files) {
        return compact(files, 0, 0);
    }

    // --- Under-full block coalescing ---
    // A sparse, high-cardinality series gets a tiny block per memstore rollover,
    // and the zero-copy carry preserves those forever: file count drops, block
    // count never does. Re-encoding through the merge path re-chunks them at
    // MaxPointsPerBlock.
    //
    // The gate must NOT be derived from TSMIndexBlock::blockCount: for Float that
    // is the NON-NaN count (an all-NaN block reports 0), so a sparse series of
    // FULL blocks would look empty and get decoded. Compressed block BYTES are
    // NaN-independent and are what we use instead.
    // 4, not 8: merges pull in files_per_merge == 4 inputs, and on
    // high-cardinality workloads each input holds ONE tiny block per series.
    // At 8, a first-level merge of four 30-point blocks could never coalesce
    // -- the fragmentation survived into every deeper tier untouched.
    static constexpr size_t MIN_BLOCKS_TO_CONSIDER_COALESCING = 4;
    // Blocks averaging under this many compressed bytes are considered under-full.
    // A full 3000-point Float block measures ~4 KB in practice.
    static constexpr uint64_t UNDERFULL_BLOCK_BYTES = 512;
    // Ceiling on a single series' compressed bytes before we will re-encode it.
    // Strings get a smaller cap: their expansion estimate carries much more
    // uncertainty (see STRING_EXPANSION_ESTIMATE), so the worst-case decoded
    // size of one string series is kept to ~128 MB of ESTIMATE rather than 8 MB
    // of compressed input that could decode to far more.
    static constexpr uint64_t MAX_COALESCE_COMPRESSED_BYTES = 8ull << 20;         // 8 MB
    static constexpr uint64_t MAX_COALESCE_COMPRESSED_BYTES_STRING = 2ull << 20;  // 2 MB
    // Shared byte budget so CONCURRENT series cannot each spend the per-series
    // ceiling (up to seriesBatchSize() x 4 type pipelines run at once).
    static constexpr uint64_t COALESCE_TOTAL_BUDGET_BYTES = 128ull << 20;  // 128 MB

    // Source blocks read in parallel per batch on the zero-copy carry path.
    // Bounds resident block bytes while keeping the read pipeline full.
    static constexpr size_t BLOCK_READ_BATCH = 16;

    // Create a compaction plan for given tier
    CompactionPlan planCompaction(uint64_t tier);

    // Execute a compaction plan
    // Takes the plan BY VALUE, deliberately. This is a coroutine, so a
    // reference parameter is stored in the frame as a reference: any caller
    // whose plan dies at the first suspension leaves it dangling. That is
    // exactly what happened at the with_scheduling_group call site, where the
    // plan lived in a by-value parameter of a NON-coroutine lambda that
    // returned as soon as this coroutine first suspended. By value, the frame
    // owns the plan and every caller is safe by construction.
    seastar::future<CompactionStats> executeCompaction(CompactionPlan plan);

    // Check if compaction is needed for a tier
    bool shouldCompact(uint64_t tier) const;

    // Run continuous background compaction
    seastar::future<> runCompactionLoop();

    // Stop all compactions
    void stopCompaction() { compactionEnabled = false; }

    // Get current compaction statistics
    std::vector<CompactionStats> getActiveCompactionStats() const;

    // Force a full compaction of all tiers
    seastar::future<> forceFullCompaction();

    // Check if a file is currently part of an active compaction.
    // Single-threaded per shard — no synchronisation needed.
    bool isFileInActiveCompaction(const seastar::shared_ptr<TSM>& file) const;

    // True when at least one compaction semaphore unit is available.
    // Single-threaded: safe to call and then co_await executeCompaction()
    // without a race, because no other task runs between the check and the
    // first suspension point inside executeCompaction (get_units).
    bool hasCompactionCapacity() const { return compactionSemaphore.available_units() > 0; }

    // Rewrite a single tombstoned file at the same tier to reclaim space.
    // Caller must verify hasCompactionCapacity() first (non-blocking design).
    seastar::future<CompactionStats> executeTombstoneRewrite(seastar::shared_ptr<TSM> file);

    // Rewrite a single file at the same tier so its aged points fold to their
    // downsample stage. Driven by Engine::sweepDownsampleRewrites() for series
    // that stopped receiving writes and would therefore never be re-compacted.
    //
    // Mechanically identical to executeTombstoneRewrite — one file, same tier,
    // through executeCompaction, which is what makes the retention provider
    // apply the cascade. Separate only so the two reasons a file gets rewritten
    // are distinguishable in the log.
    //
    // Caller must verify hasCompactionCapacity() first (non-blocking design).
    seastar::future<CompactionStats> executeDownsampleRewrite(seastar::shared_ptr<TSM> file);

private:
    // Shared body of the two single-file rewrites above. `reason` only labels
    // the log line.
    seastar::future<CompactionStats> executeSingleFileRewrite(seastar::shared_ptr<TSM> file, const char* reason);
};

// Abstract base class for compaction strategies
class CompactionStrategy {
public:
    virtual ~CompactionStrategy() = default;

    // Determine if compaction should run for a tier
    virtual bool shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const = 0;

    // Select which files to compact
    virtual std::vector<seastar::shared_ptr<TSM>> selectFiles(
        const std::vector<seastar::shared_ptr<TSM>>& availableFiles, uint64_t tier) const = 0;

    // Get target tier for compacted file
    virtual uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const = 0;
};

// Leveled compaction strategy - similar to Cassandra/RocksDB.
//
// FIXED-COUNT merges: every compaction takes EXACTLY files_per_merge of the
// oldest files in a tier (with 6 files, the oldest 4 merge and the newest 2
// wait until 2 more accumulate). The old take-up-to-MAX behaviour (8, or 16 at
// tier 3) merged whatever backlog had piled up, so tier-file sizes wandered by
// 2-4x and compounded with depth. Consistent counts keep a tier-N file at
// ~files_per_merge^N flush files, which is what makes tier depth a meaningful
// signal for per-tier block sizing and reclaim-rate scheduling -- and each
// merge's cost predictable for the scheduler that interleaves them with WAL
// conversion.
class LeveledCompactionStrategy : public CompactionStrategy {
private:
    static constexpr size_t NUM_TIERS = 4;

    static size_t filesPerMerge() { return timestar::config().storage.compaction.files_per_merge; }

public:
    bool shouldCompact(uint64_t tier, size_t fileCount, size_t /*totalSize*/) const override {
        if (tier >= NUM_TIERS)
            return false;
        // Count-based only. The old byte trigger (merge early past a per-tier
        // byte ceiling) merged FEWER than files_per_merge files, producing
        // exactly the inconsistent file sizes fixed-count merges exist to
        // prevent.
        return fileCount >= filesPerMerge();
    }

    std::vector<seastar::shared_ptr<TSM>> selectFiles(const std::vector<seastar::shared_ptr<TSM>>& availableFiles,
                                                      uint64_t tier) const override {
        if (tier >= NUM_TIERS)
            return {};

        std::vector<seastar::shared_ptr<TSM>> selected;

        // Filter files by tier
        for (const auto& file : availableFiles) {
            if (file->tierNum == tier) {
                selected.push_back(file);
            }
        }

        const size_t mergeCount = filesPerMerge();
        // Fewer than a full merge available (also reachable when eligible
        // files are excluded because an in-flight compaction already claimed
        // them): merge NOTHING rather than a runt set. A short merge produces
        // an undersized tier file, defeating the size consistency, and in the
        // 1-file case would be a pointless same-data rewrite.
        if (selected.size() < mergeCount) {
            return {};
        }

        // Sort by sequence number (oldest first)
        std::sort(selected.begin(), selected.end(), [](const auto& a, const auto& b) { return a->seqNum < b->seqNum; });

        // Take EXACTLY one merge's worth -- the oldest files.
        selected.resize(mergeCount);

        // Deliberately NOT capped by input bytes. Compaction streams (bounded
        // output buffer, one source block resident at a time), so peak memory no
        // longer scales with the size of the input set -- and a byte cap would
        // stop deep tiers from ever forming, which is the whole point of the
        // tiering.
        return selected;
    }

    uint64_t getTargetTier(uint64_t sourceTier, size_t fileCount) const override {
        if (sourceTier >= NUM_TIERS)
            return sourceTier;
        // Promote only FULL merges. selectFiles() always hands over exactly
        // files_per_merge, so scheduled compactions promote -- but direct
        // compact() callers with fewer files (tombstone rewrites, retention
        // rewrites of a single file) must stay in their own tier: a promoted
        // rewrite would masquerade as a consolidation it never performed, and
        // pick up the deeper tier's larger block cap to boot. Tier-3 merges
        // stay in tier 3 (files_per_merge -> 1) to bound the deepest tier's
        // file count.
        if (fileCount >= filesPerMerge() && sourceTier < 3) {
            return sourceTier + 1;
        }
        return sourceTier;
    }
};
