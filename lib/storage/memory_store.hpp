#pragma once

#include "../core/vshard.hpp"  // VIRTUAL_SHARD_COUNT (the bitmap below is sized from it)
#include "logger.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "timestar_value.hpp"
#include "tsm.hpp"
#include "wal.hpp"

#include <tsl/robin_map.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <variant>
#include <vector>

class WAL;

namespace timestar {
class StorageLayout;
}

// Running statistics for a float series in memory — maintained during insert.
// Enables block-stats-like pushdown aggregation without per-point scanning
// when the query range covers the entire series.
//
// Split into two phases:
//   1. sum/min/max via SIMD (no data dependency, vectorizable)
//   2. Welford mean/m2 via scalar (sequential dependency on mean)
struct InMemorySeriesStats {
    double sum = 0.0;
    double sumCompensation = 0.0;  // Kahan compensated sum
    // ±Inf identities so legitimate ±Inf values order correctly (see
    // AggregationState). Emptiness is signalled by count == 0.
    double min = std::numeric_limits<double>::infinity();
    double max = -std::numeric_limits<double>::infinity();
    // Number of non-NaN values (NaN = missing data and is never counted;
    // docs/nan_policy.md). Matches AggregationState::count semantics so the
    // stats pushdown is placement-independent.
    uint64_t count = 0;
    // Welford's online variance accumulators
    double mean = 0.0;
    double m2 = 0.0;
    // First/Latest tracking
    double firstValue = 0.0;
    uint64_t firstTimestamp = std::numeric_limits<uint64_t>::max();
    double latestValue = 0.0;
    uint64_t latestTimestamp = 0;

    bool valid = false;  // false until first update

    // Compensated sum accessor matching AggregationState convention
    double compensatedSum() const { return sum + sumCompensation; }

    void update(const double* values, const uint64_t* timestamps, size_t n);
};

template <class T>
class InMemorySeries {
public:
    std::vector<uint64_t> timestamps;
    std::vector<T> values;
    // Parallel per-point replicated revision (ADR 0003), carried through LWW so a
    // flushed tier-0 block can emit its revision column and [minRev,maxRev]. It is
    // EITHER empty (untracked -- non-cluster mode, zero memory/CPU overhead) OR
    // exactly as long as `values`. The paired sort/merge/dedup ops maintain that
    // invariant and are branch-guarded on emptiness so the untracked path is free.
    std::vector<uint64_t> revisions;

    // Running stats — only valid for double series, updated on each insert.
    // Used by pushdown aggregation to skip per-point scanning when query
    // covers the entire series time range.
    InMemorySeriesStats stats;

    void insert(TimeStarInsert<T>&& insertRequest);

    // Sort all timestamps and values together. Convenience wrapper for sortPaired()
    // over the entire range, used by TSM writer before flushing to disk.
    // The insert path maintains sortedness (is_sorted fast paths + suffix
    // sort + merge), so at flush time the data is almost always already
    // sorted — an O(N) sequential check avoids the O(N log N) index sort
    // plus full gather/copy-back permutation.
    void sort() {
        if (!std::ranges::is_sorted(timestamps)) [[unlikely]] {
            sortPaired(0, timestamps.size());
        }
    }

    // Sort a subrange [from, to) of timestamps and values together using index-based
    // permutation. Only allocates temporary storage proportional to the subrange size,
    // not the full vector. Used for sorting unsorted suffixes before merging.
    // Stable: equal timestamps keep their input order, so "last write in the
    // batch" stays last and dedupSuffixKeepLast() implements last-write-wins.
    void sortPaired(size_t from, size_t to);

    // Merge two individually-sorted, internally duplicate-free runs
    // [0, midpoint) and [midpoint, n) into a single sorted sequence.
    // Last-write-wins: on equal timestamps the suffix run (newer write)
    // replaces the prefix point. Returns true if any point was overwritten,
    // in which case running stats must be recomputed.
    bool mergePaired(size_t midpoint);

    // Collapse equal-timestamp runs in the sorted suffix [from, end), keeping
    // the last value of each run (last write wins). Returns true if anything
    // was removed.
    bool dedupSuffixKeepLast(size_t from);

    // Rebuild running stats from the full (sorted, deduplicated) series.
    // Needed after an overwrite: incremental stats can't cheaply un-count a
    // replaced value (Kahan sum, Welford M2, min/max).
    void recomputeStats();
};

// Order of variants must match TSMValueType order
using VariantInMemorySeries =
    std::variant<InMemorySeries<double>, InMemorySeries<bool>, InMemorySeries<std::string>, InMemorySeries<int64_t>>;

class MemoryStore {
private:
    std::unique_ptr<WAL> wal;
    bool closed = false;
    // Tracks cumulative estimated (worst-case/pre-compression) insert sizes.
    // The actual compressed WAL size grows much slower than estimates due to
    // XOR float compression and FFOR timestamp compression.  Using only
    // the compressed size for rollover decisions can cause the WAL to never
    // trigger rollover when compression is highly effective.  This counter
    // provides a conservative upper bound for rollover decisions.
    size_t estimatedAccumulatedSize = 0;

    // Resident-memory estimate, tracked separately from the WAL-byte estimate
    // above. estimatedAccumulatedSize models COMPRESSED ON-DISK bytes, which is
    // the right bound for the WAL but a wild under-count of RAM: it under-counts
    // doubles ~3.5x, and highly-repetitive strings ~1000x (the per-string floor
    // is 1 byte while the resident cost is the std::string plus its payload).
    //
    // It also ignores per-SERIES cost entirely, so a high-cardinality workload
    // never reaches the threshold: 6,000 series x 20 points measured ~400 MB
    // resident while the estimate stayed under 1 MB and the store never rolled
    // over at all. Rollover now triggers on whichever estimate crosses first.
    size_t residentBytesEstimate = 0;

    // Rough per-series bookkeeping cost: robin_map slot (key + variant payload,
    // at ~0.5 load factor) plus the two heap vector allocations.
    static constexpr size_t PER_SERIES_OVERHEAD_BYTES = 448;

    // Presence bitmap over the virtual shards (debt D-35); see noteVShard().
    // 64 words == 512 bytes, zero-initialised, never grows, never allocates.
    //
    // The word count is NOT independent of the VShard space: noteVShard indexes
    // `vshard >> 6`, so raising VIRTUAL_SHARD_COUNT without widening this array is an
    // out-of-bounds WRITE on every insert of a high-numbered VShard -- silent heap
    // corruption on the hottest path in the system. Fail the build instead.
    static constexpr size_t kVShardBitmapWords = timestar::VIRTUAL_SHARD_COUNT / 64;
    static_assert(timestar::VIRTUAL_SHARD_COUNT == 64 * 64,
                  "MemoryStore's VShard presence bitmap (debt D-35) is sized for 4096 VShards; widen "
                  "vshardBits_ and re-check noteVShard/touchesVShard's shift and mask if that changes");
    std::array<uint64_t, kVShardBitmapWords> vshardBits_{};

public:
    size_t getResidentBytesEstimate() const { return residentBytesEstimate; }

    // Threshold for WAL rollover decisions (based on estimated sizes).
    // Read from timestar::config().storage.wal_size_threshold at runtime.
    static size_t walSizeThreshold() { return timestar::config().storage.wal_size_threshold; }

    // Resident-memory ceiling for one memory store. Deliberately a multiple of
    // the WAL threshold rather than equal to it: the WAL threshold governs
    // compressed on-disk bytes, and typical compression means a store that hits
    // the WAL bound holds several times that in RAM. This is the backstop for
    // the cases where compression is so effective (or cardinality so high) that
    // the WAL bound is never reached at all.
    static size_t residentBytesThreshold() { return timestar::config().storage.wal_size_threshold * 4; }

    // True when durability for this shard's writes is already provided OUTSIDE the
    // per-shard WAL, so writing the WAL too would be a second fsync of the same data
    // (the "double WAL"). That is exactly replicated cluster mode: every write reaches
    // the Engine only by being committed through its VShard's Raft group, which has
    // already appended it to the Raft journal and fsync'd it.
    //
    // Safe because the Raft log is never compacted past what is durable in TSM:
    // ReplicatedVShardHost::snapshotVShard compacts only up to the snapshot's
    // FLUSHED-TSM revision, and RaftNode's constructor resets lastApplied_ to
    // log_.snapshotIndex() -- so on restart every entry above that boundary is
    // RE-APPLIED. Anything the memory store lost is therefore replayed from the Raft
    // log. Re-application is idempotent (revisions are the log index, LWW).
    //
    // Flush cadence is unaffected: estimatedAccumulatedSize still advances per batch
    // and rollover triggers on max(wal size, estimated) as well as resident bytes.
    static bool externalDurability() {
        const auto& c = timestar::config();
        return c.cluster.enabled && c.cluster.partitioned && c.cluster.replication_factor > 1;
    }
    const unsigned int sequenceNumber;

    // TSM sequence number reserved for this store's flush file, taken inside
    // the rollover critical section so that TSM seq order == store write order
    // even when conversions complete out of order (see
    // TSMFileManager::reserveSequenceId). Unset for stores that never roll
    // over through that path (startup recovery, tests); writeMemstore then
    // falls back to assigning at write time, which is safe there because those
    // paths convert sequentially.
    std::optional<uint64_t> reservedTsmSeq;
    // Use robin_map for O(1) lookups with better cache locality than std::unordered_map
    tsl::robin_map<SeriesId128, VariantInMemorySeries, SeriesId128::Hash> series;

    // ---- which VShards this store holds data for (debt D-35) ----
    //
    // A presence BITMAP over the 4096 virtual shards -- 512 bytes per store, one bit
    // set per insert (`hash & 4095`, a pure function of the series id). It exists to
    // make the snapshot producer's "is this VShard's flushed data a contiguous prefix?"
    // question answerable PER VSHARD instead of per shard.
    //
    // The old answer was `WALFileManager::hasPendingConversions()` -- "does ANY rolled
    // store on this shard still await TSM conversion?" -- which is correct (see
    // EngineLocalStore::hasUnconvertedStores) and blunt: under sustained load a shard
    // may never have zero pending conversions, and then EVERY group on it keeps its
    // whole Raft log forever. The precise condition is the one this bitmap answers:
    // does any UNCONVERTED rolled store hold data for THIS VShard? If none does, then
    // all of that VShard's rolled data is already in TSM and the only unflushed
    // remainder is in the ACTIVE store, which by construction holds a contiguous
    // SUFFIX of the VShard's revisions -- which is exactly what makes the manifest's
    // max flushed revision a safe truncation boundary.
    //
    // Set on the ONE lowest-level insertion path (insertMemory), so WAL replay
    // (initFromWAL -> WALReader::readAll -> insertMemory) populates it identically to
    // a live write. A bitmap rather than a set because it must never allocate on the
    // insert path and must be O(1) to probe from the snapshot sweep.
    void noteVShard(uint16_t vshard) noexcept { vshardBits_[vshard >> 6] |= (uint64_t{1} << (vshard & 63)); }
    [[nodiscard]] bool touchesVShard(uint16_t vshard) const noexcept {
        return (vshardBits_[vshard >> 6] & (uint64_t{1} << (vshard & 63))) != 0;
    }
    // How many VShards this store has seen (diagnostics; O(64)).
    [[nodiscard]] size_t vshardsTouched() const noexcept {
        size_t n = 0;
        for (uint64_t w : vshardBits_)
            n += static_cast<size_t>(__builtin_popcountll(w));
        return n;
    }

    MemoryStore(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {
        timestar::memory_log.debug("Memory store {} created", sequenceNumber);
    }
    ~MemoryStore() { timestar::memory_log.debug("Memory store {} removed", sequenceNumber); }

    // The owning WALFileManager injects its layout + shard so this store's WAL
    // segment lands under the configured data root.
    seastar::future<> initWAL(const timestar::StorageLayout& layout, unsigned shardId);
    seastar::future<> removeWAL();

    // On-disk size of this store's WAL segment. Read before removeWAL() to
    // measure how much space the conversion to TSM reclaims. Defined in the
    // .cpp because WAL is only forward-declared here.
    size_t walSizeOnDisk() const;
    seastar::future<> initFromWAL(std::string filename);
    seastar::future<> close();
    template <class T>
    void insertMemory(TimeStarInsert<T>&& insertRequest);
    // Insert a single series write. Returns true if WAL needs rollover.
    // On success (returns false), insertRequest is moved-from and must not be reused.
    // On rollover (returns true), insertRequest is NOT consumed and can be retried.
    template <class T>
    seastar::future<bool> insert(TimeStarInsert<T>& insertRequest);

    template <class T>
    seastar::future<bool> insertBatch(
        std::vector<TimeStarInsert<T>>& insertRequests,
        size_t preComputedBatchSize = 0);  // Batch insert - returns true if WAL needs rollover
    bool isFull() const;
    // Takes non-const ref because WAL::estimateInsertSize() caches the computed
    // size in insertRequest._cachedEstimatedSize (mutable field).
    template <class T>
    bool wouldExceedThreshold(TimeStarInsert<T>& insertRequest);
    // Variant that also returns the computed estimated size to avoid
    // double-estimation in the single-insert path.
    template <class T>
    bool wouldExceedThreshold(TimeStarInsert<T>& insertRequest, size_t& outEstimatedSize);
    bool isClosed() const { return closed; }
    bool isEmpty() const { return series.empty(); }
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId) const;
    WAL* getWAL() { return wal.get(); }

    // Query method to get data for a series.
    // Returns a const pointer to avoid copying the entire series data.
    // Returns nullptr if the series is not found or has a different type.
    template <class T>
    const InMemorySeries<T>* querySeries(const SeriesId128& seriesId) const {
        auto it = series.find(seriesId);
        if (it != series.end()) {
            return std::visit(
                [](const auto& arg) -> const InMemorySeries<T>* {
                    using SeriesType = std::decay_t<decltype(arg)>;
                    if constexpr (std::is_same_v<SeriesType, InMemorySeries<T>>) {
                        return &arg;
                    }
                    return nullptr;
                },
                it->second);
        }
        return nullptr;
    }

    // Earliest timestamp in [startTime, endTime) for a series of ANY
    // aggregatable type — one hash lookup, visiting the type variant, instead
    // of one querySeries<T> probe per candidate type.  String series return
    // nullopt (they are not numerically aggregatable).
    std::optional<uint64_t> earliestTimestampInRange(const SeriesId128& seriesId, uint64_t startTime,
                                                     uint64_t endTime) const {
        auto it = series.find(seriesId);
        if (it == series.end()) {
            return std::nullopt;
        }
        return std::visit(
            [&](const auto& s) -> std::optional<uint64_t> {
                using SeriesType = std::decay_t<decltype(s)>;
                if constexpr (std::is_same_v<SeriesType, InMemorySeries<std::string>>) {
                    return std::nullopt;
                } else {
                    if (s.timestamps.empty()) {
                        return std::nullopt;
                    }
                    auto lo = std::lower_bound(s.timestamps.begin(), s.timestamps.end(), startTime);
                    if (lo != s.timestamps.end() && *lo < endTime) {
                        return *lo;
                    }
                    return std::nullopt;
                }
            },
            it->second);
    }

    // Clamped [first, last] timestamp bounds of this series' data within
    // [startTime, endTime] (inclusive), or nullopt if no data in range.
    // Used by the pushdown LWW gate to detect cross-store duplicates.
    std::optional<std::pair<uint64_t, uint64_t>> seriesTimeBoundsInRange(const SeriesId128& seriesId,
                                                                         uint64_t startTime, uint64_t endTime) const {
        auto it = series.find(seriesId);
        if (it == series.end()) {
            return std::nullopt;
        }
        return std::visit(
            [&](const auto& s) -> std::optional<std::pair<uint64_t, uint64_t>> {
                if (s.timestamps.empty()) {
                    return std::nullopt;
                }
                auto lo = std::lower_bound(s.timestamps.begin(), s.timestamps.end(), startTime);
                auto hi = std::upper_bound(lo, s.timestamps.end(), endTime);
                if (lo == hi) {
                    return std::nullopt;
                }
                return std::make_pair(*lo, *(hi - 1));
            },
            it->second);
    }

    // Delete data in a time range for a series
    void deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime);
};
