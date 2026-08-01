#pragma once

#include "memory_store.hpp"
#include "tsm_file_manager.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_ptr.hh>
#include <stdexcept>
#include <utility>
#include <vector>

class Engine;

namespace timestar {
// Thrown when a single insert/batch cannot fit in a WAL segment even after a
// rollover.  The HTTP layer maps this to 413 Payload Too Large (a client
// error), not a generic 500.
class InsertTooLargeException : public std::length_error {
public:
    explicit InsertTooLargeException(const std::string& what) : std::length_error(what) {}
};

// Thrown when the shard's unconverted memory-store backlog has reached its
// ceiling. The HTTP layer maps this to 503 + Retry-After, so a client sees an
// explicit, retryable signal.
//
// This is deliberately a REJECTION and never a block. The ingest path must not
// stall: bursts are absorbed by letting stores accumulate, and only a sustained
// overrun that would otherwise exhaust memory reaches this. Rejecting one write
// keeps the shard alive; blocking it stalls every writer behind the capacity-1
// rollover semaphore, and doing nothing gets the process OOM-killed.
class IngestBacklogException : public std::runtime_error {
public:
    explicit IngestBacklogException(const std::string& what) : std::runtime_error(what) {}
};
}  // namespace timestar

// Pairs a raw pointer to in-memory series data with the shared_ptr that
// keeps the owning MemoryStore alive.  Callers hold MemoryStoreMatch
// values to prevent the MemoryStore (and its InMemorySeries) from being
// destroyed during background TSM conversion.
template <class T>
struct MemoryStoreMatch {
    seastar::shared_ptr<MemoryStore> store;
    const InMemorySeries<T>* series;
};

class WALFileManager {
private:
    const timestar::StorageLayout layout_;
    int shardId;
    uint64_t currentWalSequenceNumber = 0;
    bool walSequenceInitialized_ = false;
    std::vector<seastar::shared_ptr<MemoryStore>> memoryStores;
    // Stores whose TSM is already query-visible but whose source WAL has not
    // crossed its unlink directory barrier. They must not remain hidden from
    // retained-memory admission while a background retry owns their contents.
    std::vector<seastar::shared_ptr<MemoryStore>> walRetirementStores_;
    TSMFileManager* tsmFileManager;
    std::function<seastar::future<>(const std::string&)> directorySync_;
    seastar::gate _backgroundGate;               // Tracks in-flight background TSM conversions
    seastar::semaphore compactionSemaphore{1};   // Only allow 1 rollover at a time
    seastar::semaphore _conversionSemaphore{1};  // Serialize background TSM conversions
    size_t conversionConcurrency_ = 1;
    std::optional<seastar::abort_source> conversionRetryAbort_;
    std::chrono::milliseconds conversionRetryDelay_{std::chrono::seconds(30)};
    // Backlog at which the burst is considered unabsorbed and logged. Rollover
    // does NOT block here -- see rolloverMemoryStore(). Admission control lives
    // at the request edge via isIngestBacklogged().
    static constexpr size_t kMaxUnconvertedMemoryStores = 4;

    // Hard ceiling on retained stores. Each holds its full UNCOMPRESSED dataset
    // in RAM (residentBytesThreshold() = wal_size_threshold * 4, ~64MB), so this
    // is a memory commitment, not a disk one: absorbing a burst costs RAM until
    // retained stores can be spilled to their WAL files. Sized so a shard tops
    // out around 1GB of retained data. Past this, writes are rejected with 503
    // rather than blocked -- a labelled rejection is recoverable for a client,
    // an OOM kill of the whole shard is not.
    static constexpr size_t kIngestRejectMemoryStores = 16;

    // Recovery stores intentionally do not own a live WAL object, so their
    // source segment is retired here rather than through WAL::remove(). The
    // operation is idempotent across an unlink-success/directory-sync-failure
    // retry and does not return until the removal is directory-durable.
    seastar::future<> removeRecoveredWal(const std::string& path);
    bool isRetainedStore(const seastar::shared_ptr<MemoryStore>& store) const;
    seastar::future<> runBackgroundConversion(seastar::shared_ptr<MemoryStore> store);
    seastar::future<> retryConversionUntilSuccess(seastar::shared_ptr<MemoryStore> store);
    seastar::future<bool> rolloverMemoryStoreImpl(bool force, std::optional<uint16_t> requiredVShard = std::nullopt);

public:
    struct VShardFlushState {
        bool pendingConversion = false;
        // The first surviving revision in the active store. Everything below
        // this is already materialised in TSM or has been durably deleted.
        std::optional<uint64_t> oldestUnflushedRevision;
    };

    struct SnapshotQuiesce {
        seastar::semaphore_units<> rolloverUnits;
        seastar::semaphore_units<> conversionUnits;
    };

    WALFileManager(timestar::StorageLayout layout, unsigned shard);
    void setDirectorySyncForTesting(std::function<seastar::future<>(const std::string&)> sync) {
        directorySync_ = std::move(sync);
    }
    void setConversionRetryDelayForTesting(std::chrono::milliseconds delay) { conversionRetryDelay_ = delay; }

    // True while any rolled-over store is still awaiting TSM conversion.
    // Drives compaction's WAL-first priority.
    bool hasPendingConversions() const { return pendingConversion(memoryStores); }

    // As a pure function of the store list, so a test asserts against THIS predicate
    // rather than a hand-respelled copy of it (`stores.size() > 1` written out again in a
    // test is a second definition that silently stops tracking this one).
    static bool pendingConversion(const std::vector<seastar::shared_ptr<MemoryStore>>& stores) {
        return stores.size() > 1;
    }

    // The same question, asked about ONE VShard (debt D-35): is there a rolled-over
    // store still awaiting TSM conversion that holds data for `vshard`?
    //
    // `memoryStores[0]` is the ACTIVE store (rolloverMemoryStore inserts the fresh
    // store at the front); every later element is a rolled store whose TSM file has
    // not been registered yet -- conversion erases the store from this vector only after
    // `writeMemstore` has returned, i.e. after `TSMFileManager::openTsmFile` registered the
    // new file (see convertWalToTsm), which is the same visibility ordering queries rely
    // on. So the rolled set is exactly `[1, size)`, and the answer is a
    // bit probe per element of a vector that is at most kIngestRejectMemoryStores (16)
    // long.
    //
    // WHY THIS IS THE RIGHT REFINEMENT, not merely a cheaper one. The blunt predicate
    // above is a per-SHARD stand-in for a per-VSHARD condition: it refuses to let ANY
    // group compact while ANY store is converting, so a shard under sustained ingest
    // (which is never at zero pending conversions) keeps every one of its ~1365 groups'
    // Raft logs in full. Restricting the question to the stores that actually hold the
    // VShard's data preserves the safety argument exactly -- if no unconverted rolled
    // store holds VShard V, then every rolled point of V is in TSM and the only
    // unflushed remainder of V is in the active store, i.e. a contiguous SUFFIX of V's
    // revisions -- while letting the other 1364 groups compact.
    //
    // A failed conversion remains in this vector and is retried under the
    // background gate until success or shutdown. This keeps its data queryable
    // and keeps both pending predicates conservative; request-edge admission at
    // kIngestRejectMemoryStores bounds the retained-memory cost.
    bool hasPendingConversionsForVShard(uint16_t vshard) const {
        return pendingConversionForVShard(memoryStores, vshard);
    }

    // One non-suspending observation of both halves of the snapshot boundary:
    // rolled stores can create holes through out-of-order conversion, while the
    // active store identifies the first surviving unflushed revision.
    VShardFlushState vshardFlushState(uint16_t vshard) const {
        if (vshard >= timestar::VIRTUAL_SHARD_COUNT)
            throw std::invalid_argument("WALFileManager::vshardFlushState: invalid VShard");
        VShardFlushState state;
        state.pendingConversion = pendingConversionForVShard(memoryStores, vshard);
        if (!memoryStores.empty() && memoryStores.front())
            state.oldestUnflushedRevision = memoryStores.front()->oldestRevisionForVShard(vshard);
        return state;
    }

    // The rule above as a pure function of the store list, so it can be tested against a
    // hand-built (active, rolled...) vector rather than only through a live shard whose
    // background conversions finish when they please.
    static bool pendingConversionForVShard(const std::vector<seastar::shared_ptr<MemoryStore>>& stores,
                                           uint16_t vshard) {
        for (size_t i = 1; i < stores.size(); ++i) {  // [0] is the ACTIVE store
            if (stores[i] && stores[i]->touchesVShard(vshard))
                return true;
        }
        return false;
    }

    // True once the retained-store backlog reaches the rejection ceiling.
    // Consulted at the request edge to shed load without blocking.
    bool isIngestBacklogged() const { return retainedMemoryStoreCount() >= kIngestRejectMemoryStores; }

    // Stores currently retained (including the active store and published
    // stores awaiting WAL retirement), for metrics and admission control.
    size_t retainedMemoryStoreCount() const { return memoryStores.size() + walRetirementStores_.size(); }

    seastar::future<> init(Engine& engine, TSMFileManager& _tsmFileManager);
    template <class T>
    seastar::future<> insert(TimeStarInsert<T>& insertRequest);
    template <class T>
    seastar::future<> insertBatch(std::vector<TimeStarInsert<T>>& insertRequests);
    seastar::future<> rolloverMemoryStore();
    // Rotate even when the current store became logically empty after a
    // generation tombstone. This retires its old WAL bytes so the next suffix
    // write cannot get stuck behind the normal empty-store rollover shortcut.
    seastar::future<> forceRolloverMemoryStore();
    // Rotate only if the active store still has surviving points for `vshard`
    // after acquiring the rollover lock. Snapshot production uses this to make
    // progress when receipt retirement is fenced behind an unflushed write.
    seastar::future<bool> forceRolloverMemoryStoreForVShard(uint16_t vshard);

    // Stop rollovers/conversions and synchronously publish+retire every rolled
    // store that still contains `vshard`. Holding the returned units prevents
    // a retiring store from publishing stale target data during snapshot swap.
    seastar::future<SnapshotQuiesce> quiesceForVShardSnapshot(uint16_t vshard);
    seastar::future<> convertWalToTsm(seastar::shared_ptr<MemoryStore> store);
    seastar::future<> close();
    std::optional<TSMValueType> getSeriesType(const std::string& seriesKey);
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

    // Query memory stores for data (deletion filtering removed - WAL replay handles current state)
    // Returns the first matching in-memory series together with a shared_ptr
    // that keeps the owning MemoryStore alive, or std::nullopt if not found.
    // NOTE: With background TSM conversion, multiple memory stores may contain
    // data for the same series. This returns only the first match. Use
    // queryAllMemoryStores() to get data from all stores.
    template <class T>
    std::optional<MemoryStoreMatch<T>> queryMemoryStores(const std::string& seriesKey) {
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        return queryMemoryStores<T>(seriesId);
    }

    template <class T>
    std::optional<MemoryStoreMatch<T>> queryMemoryStores(const SeriesId128& seriesId) {
        for (auto& memStore : memoryStores) {
            auto result = memStore->querySeries<T>(seriesId);
            if (result != nullptr) {
                return MemoryStoreMatch<T>{memStore, result};
            }
        }
        return std::nullopt;
    }

    // Query ALL memory stores and return every matching series together with
    // a shared_ptr that keeps the owning MemoryStore alive.  The caller holds
    // the returned MemoryStoreMatch values to prevent use-after-free if a
    // background TSM conversion removes a store from the memoryStores vector.
    template <class T>
    std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(const std::string& seriesKey) {
        SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
        return queryAllMemoryStores<T>(seriesId);
    }

    template <class T>
    std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(const SeriesId128& seriesId) {
        return queryAllMemoryStores<T>(memoryStores, seriesId);
    }

    // Static overload operating on a caller-held (pinned) snapshot of the
    // memory-store list.  See pinMemoryStores() for the visibility contract.
    template <class T>
    static std::vector<MemoryStoreMatch<T>> queryAllMemoryStores(
        const std::vector<seastar::shared_ptr<MemoryStore>>& stores, const SeriesId128& seriesId) {
        std::vector<MemoryStoreMatch<T>> results;
        for (auto& memStore : stores) {
            auto result = memStore->querySeries<T>(seriesId);
            if (result != nullptr) {
                results.push_back(MemoryStoreMatch<T>{memStore, result});
            }
        }
        return results;
    }

    // Access the underlying memory stores (for batch operations that iterate
    // all stores once instead of per-series lookups).
    const std::vector<seastar::shared_ptr<MemoryStore>>& getMemoryStores() const { return memoryStores; }

    // Pin the current memory-store set for the duration of a query.
    //
    // VISIBILITY INVARIANT (WAL->TSM conversion): a background conversion
    // registers the new TSM file FIRST and only then erases the retiring
    // memory store from `memoryStores`.  A query therefore sees every point
    // at every instant iff it (1) pins the memory stores BEFORE snapshotting
    // the TSM file list and (2) reads only from the pinned copy afterwards.
    // The shared_ptr copies keep retiring stores (and their series data)
    // alive and queryable even if a conversion completes — and erases the
    // store from the live vector — while the query is suspended on TSM I/O.
    // Reading the LIVE vector after a co_await instead would open a window
    // where a just-converted series is visible in NEITHER source (the TSM
    // snapshot predates registration, the store is already gone).
    std::vector<seastar::shared_ptr<MemoryStore>> pinMemoryStores() const { return memoryStores; }

    // Return the earliest timestamp across all memory stores for a given series
    // within [startTime, endTime].  Returns nullopt if no memory data in range.
    // Used by pushdown aggregation to determine the split point between the
    // TSM-only portion (full pushdown) and the overlap portion (fallback).
    // Type-agnostic: one hash lookup per store (visits the series variant)
    // instead of one probe per candidate value type.  String series are
    // excluded to match the semantics of probing only aggregatable types.
    std::optional<uint64_t> getEarliestMemoryTimestampAnyType(const SeriesId128& seriesId, uint64_t startTime,
                                                              uint64_t endTime) {
        return getEarliestMemoryTimestampAnyType(memoryStores, seriesId, startTime, endTime);
    }

    // Static overload operating on a pinned snapshot (see pinMemoryStores()).
    static std::optional<uint64_t> getEarliestMemoryTimestampAnyType(
        const std::vector<seastar::shared_ptr<MemoryStore>>& stores, const SeriesId128& seriesId, uint64_t startTime,
        uint64_t endTime) {
        std::optional<uint64_t> earliest;
        for (auto& memStore : stores) {
            auto ts = memStore->earliestTimestampInRange(seriesId, startTime, endTime);
            if (ts && (!earliest.has_value() || *ts < *earliest)) {
                earliest = ts;
            }
        }
        return earliest;
    }

    // Delete data from memory stores and write to WAL
    seastar::future<> deleteFromMemoryStores(const std::string& seriesKey, uint64_t startTime, uint64_t endTime);

    // Persist and apply a whole-VShard generation tombstone. Snapshot install
    // must quiesce conversions before calling this so no retiring store can
    // publish an un-tombstoned TSM concurrently.
    seastar::future<size_t> deleteVShardFromMemoryStores(uint16_t vshard);
};
