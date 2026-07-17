#include "query_runner.hpp"

#include "logger.hpp"
#include "logging_config.hpp"
#include "memory_store.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "tsm_result.hpp"
#include "value_type_dispatch.hpp"

#include <boost/iterator/counting_iterator.hpp>

#include <algorithm>
#include <optional>
#include <queue>
#include <ranges>
#include <seastar/core/distributed.hh>
#include <unordered_set>

// ---------------------------------------------------------------------------
// Multi-way merge of K+1 sorted sequences (TSM + K memory stores)
// ---------------------------------------------------------------------------
// Each sorted sequence is represented as a SortedSpan: a read-only view over
// contiguous timestamp/value arrays with a current cursor position.  The TSM
// sequence (index 0) has dedup priority over memory sequences.
//
// Uses index-based access into std::vector<T> rather than raw T* pointers,
// because std::vector<bool> is bit-packed and does not support .data().
//
// Dispatch by total sequence count:
//   1    : no merge needed (TSM only or single memory store)
//   2    : two-pointer merge with dedup
//   3-4  : linear-scan min-finding merge
//   5+   : heap-based k-way merge

template <class T>
struct SortedSpan {
    const uint64_t* tsPtr;         // pointer into timestamps array
    const std::vector<T>* valVec;  // pointer to values vector (index-based access)
    size_t baseIdx;                // offset into valVec for this span's first element
    size_t pos;                    // current read position within span
    size_t len;                    // total number of elements in this span

    bool exhausted() const { return pos >= len; }
    uint64_t ts() const { return tsPtr[pos]; }
    auto val() const { return (*valVec)[baseIdx + pos]; }
    void advance() { ++pos; }

    // Skip all elements with timestamp <= target (dedup helper).
    void advancePast(uint64_t target) {
        while (pos < len && tsPtr[pos] <= target)
            ++pos;
    }
};

// Two-pointer merge of exactly 2 sorted spans into output vectors.
// SPAN ORDER CONTRACT (all span merges): spans are ordered oldest source
// first — TSM at index 0, then memory stores oldest→newest.  On equal
// timestamps the HIGHER-index span (newer source) wins, implementing
// last-write-wins: a rewritten point in a memory store shadows the stale
// copy in TSM / older stores.  (During rollover a just-converted TSM file
// and its still-pinned source store hold identical copies, so either
// winner is correct there.)
template <class T>
static void mergeTwoSpans(SortedSpan<T>& a, SortedSpan<T>& b, std::vector<uint64_t>& outTs, std::vector<T>& outVal) {
    while (!a.exhausted() && !b.exhausted()) {
        const uint64_t tsA = a.ts();
        const uint64_t tsB = b.ts();

        if (tsA < tsB) {
            outTs.push_back(tsA);
            outVal.push_back(a.val());
            a.advance();
        } else if (tsB < tsA) {
            outTs.push_back(tsB);
            outVal.push_back(b.val());
            b.advance();
        } else {
            // Equal timestamps: span b (higher index = newer source) wins.
            // Use advance() on `b` (not advancePast) so that if `b` has
            // multiple points at the same timestamp they are each emitted,
            // matching the behaviour of the < / > branches.
            // `a` uses advancePast to skip all its duplicates at this ts.
            outTs.push_back(tsB);
            outVal.push_back(b.val());
            b.advance();
            a.advancePast(tsA);
        }
    }

    // Drain whichever span still has data in bulk.
    for (auto* s : {&a, &b}) {
        const size_t remaining = s->len - s->pos;
        if (remaining == 0)
            continue;
        outTs.insert(outTs.end(), s->tsPtr + s->pos, s->tsPtr + s->len);
        const auto& vals = *s->valVec;
        const size_t base = s->baseIdx + s->pos;
        if constexpr (std::is_same_v<T, bool>) {
            for (size_t i = 0; i < remaining; ++i) {
                outVal.push_back(vals[base + i]);
            }
        } else {
            outVal.insert(outVal.end(), vals.begin() + base, vals.begin() + base + remaining);
        }
        s->pos = s->len;
    }
}

// Linear-scan merge for 3-4 sorted spans.
// On each iteration, find the span with the minimum current timestamp
// (higher index = newer source wins ties; see span order contract above),
// emit it, and advance all spans that share that timestamp (dedup).
template <class T>
static void mergeSmallNSpans(std::vector<SortedSpan<T>>& spans, std::vector<uint64_t>& outTs, std::vector<T>& outVal) {
    const size_t K = spans.size();
    size_t activeCount = K;

    // Track which spans are exhausted to avoid repeated checks.
    for (size_t i = 0; i < K; ++i) {
        if (spans[i].exhausted())
            --activeCount;
    }

    while (activeCount > 0) {
        // When only one span remains, drain it in bulk and exit.
        if (activeCount == 1) {
            for (size_t i = 0; i < K; ++i) {
                if (spans[i].exhausted())
                    continue;
                auto& s = spans[i];
                const size_t remaining = s.len - s.pos;
                outTs.insert(outTs.end(), s.tsPtr + s.pos, s.tsPtr + s.len);
                const auto& vals = *s.valVec;
                const size_t base = s.baseIdx + s.pos;
                if constexpr (std::is_same_v<T, bool>) {
                    for (size_t j = 0; j < remaining; ++j) {
                        outVal.push_back(vals[base + j]);
                    }
                } else {
                    outVal.insert(outVal.end(), vals.begin() + base, vals.begin() + base + remaining);
                }
                s.pos = s.len;
                break;
            }
            break;
        }

        // Find minimum timestamp across active spans.
        // Higher index (newer source) wins on equal timestamps: `<=` lets a
        // later span overwrite the candidate at the same timestamp.
        size_t bestIdx = SIZE_MAX;
        uint64_t bestTs = UINT64_MAX;

        for (size_t i = 0; i < K; ++i) {
            if (spans[i].exhausted())
                continue;
            const uint64_t ts = spans[i].ts();
            if (ts <= bestTs) {
                bestTs = ts;
                bestIdx = i;
            }
        }

        // Safety guard: if every span was exhausted but activeCount was stale,
        // bestIdx remains SIZE_MAX.  Break rather than access spans[SIZE_MAX].
        if (bestIdx == SIZE_MAX)
            break;

        // Emit the winning point.
        outTs.push_back(bestTs);
        outVal.push_back(spans[bestIdx].val());

        // Advance the winner by one (matching mergeTwoSpans behaviour:
        // if the winner has multiple points at the same timestamp, each
        // is emitted on a subsequent iteration).
        spans[bestIdx].advance();
        if (spans[bestIdx].exhausted())
            --activeCount;

        // Advance all OTHER spans that share this timestamp (dedup).
        for (size_t i = 0; i < K; ++i) {
            if (i == bestIdx || spans[i].exhausted())
                continue;
            if (spans[i].ts() == bestTs) {
                spans[i].advancePast(bestTs);
                if (spans[i].exhausted())
                    --activeCount;
            }
        }
    }
}

// Heap-based k-way merge for 5+ sorted spans.
template <class T>
static void mergeHeapSpans(std::vector<SortedSpan<T>>& spans, std::vector<uint64_t>& outTs, std::vector<T>& outVal) {
    struct HeapEntry {
        uint64_t timestamp;
        size_t spanIdx;
        // Min-heap by timestamp; higher spanIdx (newer source) wins ties —
        // last-write-wins, see span order contract above.
        bool operator>(const HeapEntry& other) const {
            if (timestamp != other.timestamp)
                return timestamp > other.timestamp;
            return spanIdx < other.spanIdx;
        }
    };

    std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;
    for (size_t i = 0; i < spans.size(); ++i) {
        if (!spans[i].exhausted()) {
            heap.push({spans[i].ts(), i});
        }
    }

    std::vector<HeapEntry> sameTs;
    sameTs.reserve(spans.size());

    while (!heap.empty()) {
        // When only one span remains in the heap, drain it in bulk.
        if (heap.size() == 1) {
            auto& s = spans[heap.top().spanIdx];
            const size_t remaining = s.len - s.pos;
            outTs.insert(outTs.end(), s.tsPtr + s.pos, s.tsPtr + s.len);
            const auto& vals = *s.valVec;
            const size_t base = s.baseIdx + s.pos;
            if constexpr (std::is_same_v<T, bool>) {
                for (size_t i = 0; i < remaining; ++i) {
                    outVal.push_back(vals[base + i]);
                }
            } else {
                outVal.insert(outVal.end(), vals.begin() + base, vals.begin() + base + remaining);
            }
            break;
        }

        HeapEntry best = heap.top();
        heap.pop();
        const uint64_t currentTs = best.timestamp;

        // Collect all entries with the same timestamp.
        sameTs.clear();
        while (!heap.empty() && heap.top().timestamp == currentTs) {
            sameTs.push_back(heap.top());
            heap.pop();
        }

        // Emit from the winner (highest spanIdx = newest source).
        outTs.push_back(currentTs);
        outVal.push_back(spans[best.spanIdx].val());

        // Advance the winner by one (matching mergeTwoSpans behaviour:
        // if the winner has multiple points at the same timestamp, each
        // is emitted on a subsequent iteration).
        spans[best.spanIdx].advance();
        if (!spans[best.spanIdx].exhausted()) {
            heap.push({spans[best.spanIdx].ts(), best.spanIdx});
        }

        // Advance all other spans that shared the same timestamp (dedup).
        for (auto& entry : sameTs) {
            spans[entry.spanIdx].advancePast(currentTs);
            if (!spans[entry.spanIdx].exhausted()) {
                heap.push({spans[entry.spanIdx].ts(), entry.spanIdx});
            }
        }
    }
}

template <class T>
seastar::future<QueryResult<T>> QueryRunner::queryTsm([[maybe_unused]] const std::string& series, SeriesId128 seriesId,
                                                      uint64_t startTime, uint64_t endTime) {
    LOG_QUERY_PATH(timestar::query_log, debug,
                   "QueryRunner: Querying TSM files for series={}, startTime={}, endTime={}", series, startTime,
                   endTime);

    // Pin the memory STORES (shared_ptr copies) BEFORE snapshotting the TSM
    // file list.  Background WAL->TSM conversion registers the new TSM file
    // first, then erases the retiring store from the live vector; pinning the
    // stores first guarantees every point is visible in at least one of the
    // two sources at every instant (a conversion that completed before this
    // line already registered its TSM file, so the snapshot below includes
    // it; one that completes later cannot destroy the pinned stores — the
    // shared_ptrs keep their series maps alive and queryable).  Reading the
    // LIVE store list after the TSM co_awaits used to open a window where a
    // just-converted series was visible in neither source.
    //
    // NOTE: only the store pointers are pinned here.  The per-series lookup
    // (raw pointers into each store's robin_map) happens AFTER the TSM I/O
    // below — an ACTIVE store keeps accepting inserts while this coroutine is
    // suspended, and a map rehash would invalidate any series pointer taken
    // now.  The lookup result is then used strictly synchronously.
    const auto pinnedStores = walFileManager->pinMemoryStores();

    // Snapshot the TSM file map so that compaction cannot mutate it
    // mid-iteration across co_await suspension points.
    // Pre-filter using sparse index time bounds (in-memory, no I/O) to skip
    // files whose series data is entirely outside [startTime, endTime].
    // This avoids DMA reads for full index entries in irrelevant files.
    std::vector<seastar::shared_ptr<TSM>> candidateFiles;
    for (const auto& [seq, tsmFile] : fileManager->getSequencedTsmFiles()) {
        if (tsmFile->seriesMayOverlapTime(seriesId, startTime, endTime)) {
            candidateFiles.push_back(tsmFile);
        }
    }

    // Pre-allocate indexed slots to avoid concurrent push_back on a shared vector.
    std::vector<std::optional<TSMResult<T>>> tsmSlots(candidateFiles.size());

    // Query only candidate TSM files — use boost::counting_iterator to provide
    // an explicit index rather than relying on a shared mutable counter.
    co_await seastar::parallel_for_each(
        boost::counting_iterator<size_t>(0), boost::counting_iterator<size_t>(candidateFiles.size()),
        [&tsmSlots, &candidateFiles, &seriesId, startTime, endTime](size_t myIdx) -> seastar::future<> {
            const auto& tsmFile = candidateFiles[myIdx];

            // Use queryWithTombstones to automatically filter out deleted data
            // SeriesId128 pre-computed by caller — no redundant hash here
            TSMResult<T> results = co_await tsmFile.get()->queryWithTombstones<T>(seriesId, startTime, endTime);

            if (results.empty())
                co_return;

            // readSeriesBatched() already sorts blocks by start time after
            // parallel_for_each (tsm.cpp:707), so no redundant sort needed here.
            tsmSlots[myIdx] = std::move(results);
        });

    // Collect non-empty results from the slots, paired with sparse time bounds.
    struct TimeBoundedResult {
        TSMResult<T> result;
        uint64_t minTime;  // Earliest block start time in this result
        uint64_t maxTime;  // Latest block end time in this result
    };
    std::vector<TimeBoundedResult> tsmResults;
    tsmResults.reserve(tsmSlots.size());
    for (size_t i = 0; i < tsmSlots.size(); ++i) {
        if (!tsmSlots[i].has_value())
            continue;
        auto& res = *tsmSlots[i];
        // Compute time bounds from actual decoded blocks (accurate after time filtering)
        uint64_t rmin = UINT64_MAX, rmax = 0;
        for (const auto& block : res.blocks) {
            if (!block->timestamps.empty()) {
                rmin = std::min(rmin, block->timestamps.front());
                rmax = std::max(rmax, block->timestamps.back());
            }
        }
        tsmResults.push_back({std::move(res), rmin, rmax});
    }

    QueryResult<T> result;

    if (tsmResults.empty()) {
        // No TSM data — result stays empty
    } else if (tsmResults.size() == 1) {
        // Single source: direct concatenation, no merge needed.
        result = QueryResult<T>::fromTsmResults(tsmResults[0].result);
    } else {
        // Sort by minTime ascending for the non-overlap check.
        std::sort(tsmResults.begin(), tsmResults.end(),
                  [](const auto& a, const auto& b) { return a.minTime < b.minTime; });

        // Check for non-overlapping time ranges across files.
        // If each file's minTime > previous file's maxTime, there's no overlap
        // and we can concatenate in order instead of doing an N-way merge.
        bool nonOverlapping = true;
        for (size_t i = 1; i < tsmResults.size(); ++i) {
            if (tsmResults[i].minTime <= tsmResults[i - 1].maxTime) {
                nonOverlapping = false;
                break;
            }
        }

        if (nonOverlapping) {
            // Fast path: concatenate blocks from each file in time order.
            // No per-point comparison needed — just bulk append.
            size_t totalPoints = 0;
            for (const auto& tbr : tsmResults) {
                for (const auto& block : tbr.result.blocks) {
                    totalPoints += block->size();
                }
            }
            result.timestamps.reserve(totalPoints);
            result.values.reserve(totalPoints);

            for (auto& tbr : tsmResults) {
                for (auto& block : tbr.result.blocks) {
                    result.timestamps.insert(result.timestamps.end(), block->timestamps.begin(),
                                             block->timestamps.end());
                    if constexpr (std::is_same_v<T, bool>) {
                        for (size_t j = 0; j < block->values.size(); ++j) {
                            result.values.push_back(block->values[j]);
                        }
                    } else {
                        result.values.insert(result.values.end(), block->values.begin(), block->values.end());
                    }
                }
            }
        } else {
            // Overlapping: fall back to the full N-way merge with dedup.
            // Extract TSMResults and sort by rank descending for dedup priority.
            std::vector<TSMResult<T>> mergeInputs;
            mergeInputs.reserve(tsmResults.size());
            for (auto& tbr : tsmResults) {
                mergeInputs.push_back(std::move(tbr.result));
            }
            std::sort(mergeInputs.begin(), mergeInputs.end(),
                      [](const auto& a, const auto& b) { return a.rank > b.rank; });
            result = QueryResult<T>::fromTsmResults(mergeInputs);
        }
    }

    // Look up the series in the PINNED memory stores (pinned before the TSM
    // snapshot above — see the visibility-invariant comment there).  The
    // lookup runs after the last co_await, so the raw series pointers below
    // are valid for the rest of this (fully synchronous) merge.  With
    // background TSM conversion, multiple stores may hold data for the same
    // series, so all of them are queried.  Duplicates between a
    // just-converted TSM file and its still-pinned source store are removed
    // by the merge below (the newer source wins on equal timestamps —
    // last-write-wins; for rollover duplicates both copies are identical).
    auto memoryMatches = WALFileManager::queryAllMemoryStores<T>(pinnedStores, seriesId);

    // No memory data: TSM result is already sorted and deduped, return directly.
    if (memoryMatches.empty()) {
        co_return std::move(result);
    }

    // -----------------------------------------------------------------------
    // Multi-way merge: K+1 sorted sequences (1 TSM + K memory stores).
    //
    // Instead of appending memory data then sorting with an index permutation,
    // we merge all sorted sequences directly into a new output buffer.  Each
    // memory store's data is pre-filtered to [startTime, endTime] using binary
    // search, then represented as a lightweight SortedSpan.  Spans are ordered
    // oldest source first (TSM at index 0, then memory stores oldest→newest);
    // on equal timestamps the merge keeps the HIGHEST-index span's value, so
    // the newest write wins (last-write-wins).
    // -----------------------------------------------------------------------

    // Build sorted spans: TSM first (index 0), then each memory store,
    // oldest→newest.
    // SAFETY: SortedSpan holds raw pointers into memory store data.  The
    // MemoryStoreMatch values in memoryMatches keep each MemoryStore alive
    // via shared_ptr, so the underlying InMemorySeries data remains valid
    // for the lifetime of memoryMatches — even if a background TSM
    // conversion removes the store from the WALFileManager's vector.
    std::vector<SortedSpan<T>> spans;
    spans.reserve(1 + memoryMatches.size());

    // Span 0: TSM data (already sorted + deduped).
    // INVARIANT: result.timestamps must not be modified (no push_back/resize)
    // after this point — tsmSpan.tsPtr is a raw pointer into its storage.
    if (!result.timestamps.empty()) {
        SortedSpan<T> tsmSpan;
        tsmSpan.tsPtr = result.timestamps.data();
        tsmSpan.valVec = &result.values;
        tsmSpan.baseIdx = 0;
        tsmSpan.pos = 0;
        tsmSpan.len = result.timestamps.size();
        spans.push_back(tsmSpan);
    }

    // Pre-filter each memory store to [startTime, endTime] and create spans.
    // Binary search is O(log n) per store; the filtered range is a read-only
    // view with no copying.
    // memoryMatches is NEWEST-first (memoryStores[0] is the active store);
    // iterate in reverse so span order is oldest→newest per the contract.
    for (const auto& match : memoryMatches | std::views::reverse) {
        const auto& storeData = *match.series;
        if (storeData.timestamps.empty())
            continue;

        auto beginIt = std::lower_bound(storeData.timestamps.begin(), storeData.timestamps.end(), startTime);
        auto endIt = std::upper_bound(beginIt, storeData.timestamps.end(), endTime);
        size_t startIdx = static_cast<size_t>(beginIt - storeData.timestamps.begin());
        size_t endIdx = static_cast<size_t>(endIt - storeData.timestamps.begin());
        size_t count = endIdx - startIdx;

        if (count > 0) {
            SortedSpan<T> memSpan;
            memSpan.tsPtr = storeData.timestamps.data() + startIdx;
            memSpan.valVec = &storeData.values;
            memSpan.baseIdx = startIdx;
            memSpan.pos = 0;
            memSpan.len = count;
            spans.push_back(memSpan);
        }
    }

    // Edge case: no spans at all (no data anywhere) or only TSM data.
    if (spans.empty()) {
        co_return std::move(result);
    }

    // Single span: either TSM-only (already in result) or memory-only.
    if (spans.size() == 1) {
        if (spans[0].valVec == &result.values) {
            // TSM span is the only one, result already contains the data.
            co_return std::move(result);
        }
        // Memory-only: copy the single memory span into result.
        const auto& s = spans[0];
        result.timestamps.assign(s.tsPtr, s.tsPtr + s.len);
        result.values.clear();
        if constexpr (std::is_same_v<T, bool>) {
            // vector<bool> bit proxies: element-wise copy.
            result.values.reserve(s.len);
            for (size_t i = 0; i < s.len; ++i) {
                result.values.push_back((*s.valVec)[s.baseIdx + i]);
            }
        } else {
            // Contiguous types: ranged insert compiles to memcpy.
            result.values.insert(result.values.end(), s.valVec->begin() + s.baseIdx,
                                 s.valVec->begin() + s.baseIdx + s.len);
        }
        co_return std::move(result);
    }

    // Pre-calculate total output size for a single reserve() call.
    size_t totalPoints = 0;
    for (const auto& span : spans) {
        totalPoints += span.len;
    }

    // Fast path: single memory store with all timestamps > TSM max.
    // This is the common case (recent writes only in memory).  Check if the
    // TSM span and memory span are already non-overlapping and ordered.
    // Guard: only take fast path when spans[0] is the TSM span backed by result
    // (when no TSM files exist, spans[0] is a memory store and result is empty).
    if (spans.size() == 2 && spans[0].valVec == &result.values && !spans[0].exhausted() && !spans[1].exhausted()) {
        uint64_t tsmMaxTs = spans[0].tsPtr[spans[0].len - 1];
        uint64_t memMinTs = spans[1].tsPtr[0];
        if (memMinTs > tsmMaxTs) {
            // Non-overlapping: just append memory data directly.
            // IMPORTANT: Save what we need from spans[1] before clearing spans.
            // spans[0].tsPtr points into result.timestamps.data(); clearing spans
            // before calling reserve() prevents a dangling raw pointer surviving a
            // potential reallocation of result.timestamps below.
            // SAFETY: memTsPtr points into storeData owned by memoryMatches; the shared_ptr keeps it alive.
            const uint64_t* memTsPtr = spans[1].tsPtr;
            size_t memLen = spans[1].len;
            const std::vector<T>* memValVec = spans[1].valVec;
            size_t memBaseIdx = spans[1].baseIdx;
            spans.clear();  // release raw pointers into result before any reallocation

            result.timestamps.reserve(totalPoints);
            result.values.reserve(totalPoints);
            result.timestamps.insert(result.timestamps.end(), memTsPtr, memTsPtr + memLen);
            const auto& memVals = *memValVec;
            if constexpr (std::is_same_v<T, bool>) {
                // vector<bool> bit proxies: element-wise copy.
                for (size_t i = 0; i < memLen; ++i) {
                    result.values.push_back(memVals[memBaseIdx + i]);
                }
            } else {
                // Contiguous types: ranged insert compiles to memcpy.
                result.values.insert(result.values.end(), memVals.begin() + memBaseIdx,
                                     memVals.begin() + memBaseIdx + memLen);
            }
            co_return std::move(result);
        }
    }

    // General merge: allocate output buffers and merge all spans.
    std::vector<uint64_t> mergedTs;
    std::vector<T> mergedVal;
    mergedTs.reserve(totalPoints);
    mergedVal.reserve(totalPoints);

    const size_t numSpans = spans.size();

    if (numSpans == 2) {
        mergeTwoSpans(spans[0], spans[1], mergedTs, mergedVal);
    } else if (numSpans <= 4) {
        mergeSmallNSpans(spans, mergedTs, mergedVal);
    } else {
        mergeHeapSpans(spans, mergedTs, mergedVal);
    }

    result.timestamps = std::move(mergedTs);
    result.values = std::move(mergedVal);

    co_return std::move(result);
}

// Convenience overload: computes SeriesId128 internally from the series key string.
seastar::future<VariantQueryResult> QueryRunner::runQuery(const std::string& seriesKey, uint64_t startTime,
                                                          uint64_t endTime) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    co_return co_await runQuery(seriesKey, seriesId, startTime, endTime);
}

seastar::future<VariantQueryResult> QueryRunner::runQuery(const std::string& seriesKey, SeriesId128 seriesId,
                                                          uint64_t startTime, uint64_t endTime) {
    LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Running query for series='{}', startTime={}, endTime={}",
                   seriesKey, startTime, endTime);

    // Check TSM files first: established series (the common query case) are already
    // flushed to TSM. Each TSM file uses a bloom filter for O(1) negative rejection,
    // so absent series are ruled out quickly. WAL memory stores are the fallback for
    // recently written data that hasn't been flushed yet.
    LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Getting series type from TSM for series: '{}'",
                   seriesKey);
    std::optional<TSMValueType> seriesType = fileManager->getSeriesType(seriesId);

    if (!seriesType.has_value()) {
        LOG_QUERY_PATH(timestar::query_log, debug,
                       "[QUERYRUNNER] Series type not found in TSM, trying WAL for series: '{}'", seriesKey);
        seriesType = walFileManager->getSeriesType(seriesId);
    }

    // If there's no type, the series doesn't exist
    if (!seriesType.has_value()) {
        LOG_QUERY_PATH(timestar::query_log, debug,
                       "[QUERYRUNNER] Series type not found anywhere for series: '{}' - series doesn't exist",
                       seriesKey);
        throw SeriesNotFoundException(seriesKey);
    }

    LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Found series type: {} for series: '{}'",
                   static_cast<int>(seriesType.value()), seriesKey);

    VariantQueryResult results =
        co_await timestar::dispatchValueType(seriesType.value(), [&]<class T>() -> seastar::future<VariantQueryResult> {
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Querying series '{}' (type tag {})", seriesKey,
                           static_cast<int>(seriesType.value()));
            VariantQueryResult r = co_await queryTsm<T>(seriesKey, seriesId, startTime, endTime);
            co_return r;
        });

    co_return std::move(results);
};

// ---------------------------------------------------------------------------
// Pushdown aggregation
// ---------------------------------------------------------------------------

// Use the shared isStreamableMethod from aggregator.hpp (in namespace timestar).
// LATEST/FIRST are streamable there (fold via addValueForMethod), but at this
// call site they have already been handled by the LATEST/FIRST fast path above,
// so the wider definition is safe.
// Aliased here to avoid ADL ambiguity between a file-local static and the
// namespace-level inline function.
static constexpr auto isStreamableAggMethod = timestar::isStreamableMethod;

// ---------------------------------------------------------------------------
// aggregateMemoryStores — fold MemoryStore data directly into a
// BlockAggregator without materialising intermediate QueryResult vectors.
// Returns the number of points aggregated.
// ---------------------------------------------------------------------------
// Helper: fold typed memory store matches into aggregator, converting to double.
// Operates on a PINNED memory-store snapshot (see WALFileManager::pinMemoryStores):
// the caller pins the stores before any TSM I/O so a background WAL->TSM
// conversion completing mid-query cannot make the data invisible.
template <typename T>
static size_t aggregateMemoryStoresTyped(const std::vector<seastar::shared_ptr<MemoryStore>>& pinnedStores,
                                         const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                         timestar::BlockAggregator& aggregator) {
    auto memoryMatches = WALFileManager::queryAllMemoryStores<T>(pinnedStores, seriesId);
    size_t totalPoints = 0;
    for (const auto& match : memoryMatches) {
        const auto& storeData = *match.series;
        if (storeData.timestamps.empty())
            continue;

        // Fast path for double series: if running stats cover the query range
        // and the aggregator can use block stats, skip per-point scanning entirely.
        if constexpr (std::is_same_v<T, double>) {
            if (storeData.stats.valid && storeData.stats.count > 0) {
                // Check if query range fully covers this series' time range
                bool queryCoversAll =
                    (startTime <= storeData.stats.firstTimestamp && endTime > storeData.stats.latestTimestamp);
                if (queryCoversAll && storeData.stats.count <= UINT32_MAX &&
                    aggregator.canUseBlockStats(storeData.stats.firstTimestamp, storeData.stats.latestTimestamp,
                                                true)) {
                    // Use pre-computed stats — O(1) instead of O(n).
                    // Pass compensated sum for numerical stability.
                    aggregator.addBlockStats(storeData.stats.compensatedSum(), storeData.stats.min, storeData.stats.max,
                                             static_cast<uint32_t>(storeData.stats.count),
                                             storeData.stats.firstTimestamp, storeData.stats.latestTimestamp,
                                             storeData.stats.m2, storeData.stats.firstValue,
                                             storeData.stats.latestValue);
                    totalPoints += storeData.stats.count;
                    continue;
                }
            }
        }

        auto beginIt = std::lower_bound(storeData.timestamps.begin(), storeData.timestamps.end(), startTime);
        auto endIt = std::upper_bound(beginIt, storeData.timestamps.end(), endTime);
        size_t startIdx = static_cast<size_t>(beginIt - storeData.timestamps.begin());
        size_t endIdx = static_cast<size_t>(endIt - storeData.timestamps.begin());
        size_t count = endIdx - startIdx;
        if (count == 0)
            continue;

        if constexpr (std::is_same_v<T, double>) {
            // Float data: use zero-copy range path (enables SIMD fold + single-bucket fast path)
            aggregator.addPointsRange(storeData.timestamps, storeData.values, startIdx, endIdx);
        } else {
            // Non-double types: convert to double per-point
            for (size_t i = startIdx; i < endIdx; ++i) {
                aggregator.addPoint(storeData.timestamps[i], static_cast<double>(storeData.values[i]));
            }
        }
        totalPoints += count;
    }
    return totalPoints;
}

// Non-numeric gate for memory-resident series.  A series is exactly one type
// across every source, so the first pinned store that knows the series decides.
// Boolean and String never aggregate arithmetically (canonical non-numeric
// rule), so they must bypass pushdown entirely and let the caller materialise
// them in their written type.  The TSM-resident case is gated separately, by
// the seriesType checks on the candidate-file loops below; this covers series
// whose data lives only in memory while other series have TSM files (where
// those loops find no candidate for this series and would otherwise fold the
// memory data as doubles).
static bool seriesIsNonNumericInMemory(const std::vector<seastar::shared_ptr<MemoryStore>>& pinnedStores,
                                       const SeriesId128& seriesId) {
    for (const auto& store : pinnedStores) {
        if (auto type = store->getSeriesType(seriesId)) {
            return isNonNumericValueType(*type);
        }
    }
    return false;
}

// Last-write-wins gate for memory folding: true when the series' in-range
// data spans more than one pinned store with overlapping time bounds.  The
// same timestamp may then exist in two stores (a rewrite across rollover
// generations, or the rollover conversion window itself), and the per-store
// folds in aggregateMemoryStores would observe both copies.  Callers must
// fall back to the dedup merge path in that case.
static bool pinnedStoresOverlapForSeries(const std::vector<seastar::shared_ptr<MemoryStore>>& pinnedStores,
                                         const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    for (const auto& store : pinnedStores) {
        if (auto bounds = store->seriesTimeBoundsInRange(seriesId, startTime, endTime)) {
            ranges.push_back(*bounds);
        }
    }
    if (ranges.size() < 2) {
        return false;
    }
    std::sort(ranges.begin(), ranges.end());
    uint64_t prevMax = ranges[0].second;
    for (size_t i = 1; i < ranges.size(); ++i) {
        if (ranges[i].first <= prevMax) {
            return true;
        }
        prevMax = std::max(prevMax, ranges[i].second);
    }
    return false;
}

static size_t aggregateMemoryStores(const std::vector<seastar::shared_ptr<MemoryStore>>& pinnedStores,
                                    const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                    timestar::BlockAggregator& aggregator) {
    // Check float first (most common); a series is always one type, so skip
    // int64/bool if float already found data.
    size_t pts = aggregateMemoryStoresTyped<double>(pinnedStores, seriesId, startTime, endTime, aggregator);
    if (pts == 0) {
        pts = aggregateMemoryStoresTyped<int64_t>(pinnedStores, seriesId, startTime, endTime, aggregator);
        if (pts == 0) {
            pts = aggregateMemoryStoresTyped<bool>(pinnedStores, seriesId, startTime, endTime, aggregator);
        }
    }
    return pts;
}

seastar::future<std::optional<timestar::PushdownResult>> QueryRunner::queryTsmAggregated(
    SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
    timestar::AggregationMethod method, bool foldNoInterval) {
    // Gate 0.5: MEDIAN and EXACT_MEDIAN need all raw values — cannot use
    // pushdown aggregation.  T-digest is used at merge time for cross-shard
    // aggregation, not during per-value accumulation (rawValues is faster).
    if (method == timestar::AggregationMethod::MEDIAN || method == timestar::AggregationMethod::EXACT_MEDIAN) {
        co_return std::nullopt;
    }

    // Pin the memory stores BEFORE any read of the TSM file list.  A background
    // WAL->TSM conversion registers its TSM file first and erases the retiring
    // store second, so pin-memory-then-snapshot-TSM ordering guarantees the
    // series is visible in at least one source for the whole query.  All
    // memory probes/folds below MUST use this pinned snapshot — re-reading the
    // live store list after a co_await would drop the data of any store whose
    // conversion completed while this coroutine was suspended (its TSM file is
    // excluded from the range split below, and the store itself is gone).
    const auto pinnedStores = walFileManager->pinMemoryStores();

    // Last-write-wins gate: if the series' in-range data lives in two pinned
    // stores with overlapping time bounds, the same timestamp may exist in
    // both (rewrite across rollover generations); every fold below feeds each
    // store independently and would observe both copies.  Fall back to the
    // merge path, which dedups with newest-source priority.
    if (pinnedStoresOverlapForSeries(pinnedStores, seriesId, startTime, endTime)) {
        co_return std::nullopt;
    }

    // Non-numeric series (Boolean, String) never aggregate arithmetically.
    if (seriesIsNonNumericInMemory(pinnedStores, seriesId)) {
        co_return std::nullopt;
    }

    const bool noTsmFiles = fileManager->getSequencedTsmFiles().empty();
    const bool isLatest = (method == timestar::AggregationMethod::LATEST);
    const bool isFirst = (method == timestar::AggregationMethod::FIRST);

    // Gate 0: No TSM files — all data is in memory stores.
    // Instead of falling back to full materialisation, aggregate MemoryStore
    // data directly into a BlockAggregator (avoids copying all raw points
    // into intermediate QueryResult vectors).
    //
    // For bucketed queries (aggregationInterval > 0) this folds N points into
    // M buckets — a huge win when N >> M.  For non-bucketed queries
    // (aggregationInterval == 0) we still store raw (timestamp, value) pairs
    // in the aggregator rather than folding to a single state, because callers
    // (e.g. DerivedQueryExecutor) may need per-point data.
    if (noTsmFiles) {
        // Fold-to-single-state for interval == 0 ONLY when the caller
        // explicitly asked for a collapsed result (foldNoInterval, which
        // defaults to false — no production caller sets it, since collapsing a
        // range nobody asked to collapse breaks the canonical shape rules).
        // LATEST/FIRST are NOT special here: without an interval they keep
        // every timestamp like every other method.
        const bool foldSingle = (aggregationInterval == 0) && foldNoInterval && isStreamableAggMethod(method);
        timestar::BlockAggregator aggregator(aggregationInterval, startTime, endTime, method, true);
        if (foldSingle) {
            aggregator.enableFoldToSingleState();
        }

        size_t pts = aggregateMemoryStores(pinnedStores, seriesId, startTime, endTime, aggregator);
        if (pts == 0) {
            co_return std::nullopt;
        }

        timestar::PushdownResult result;
        result.totalPoints = aggregator.pointCount();
        if (aggregationInterval > 0) {
            result.bucketStates = aggregator.takeBucketStates();
        } else if (foldSingle) {
            result.aggregatedState = aggregator.takeSingleState();
        } else {
            aggregator.sortTimestamps();
            result.sortedTimestamps = aggregator.takeTimestamps();
            result.sortedValues = aggregator.takeValues();
        }
        co_return result;
    }

    // Gate 1 (split logic): Instead of rejecting entirely when memory data
    // exists, determine the split point so the TSM-only portion can still
    // benefit from pushdown aggregation.  The type-agnostic probe visits the
    // series variant with a single hash lookup per store — for historical
    // queries (no memory data, the common case) this replaces three per-type
    // probe misses per store.
    auto memMinTimeOpt = WALFileManager::getEarliestMemoryTimestampAnyType(pinnedStores, seriesId, startTime, endTime);

    // tsmEndTime: the upper bound for the TSM-only pushdown range.
    // fallbackStartTime: the lower bound for the fallback (TSM+memory) range.
    // When there is no memory data, the entire range is pushdown-eligible.
    uint64_t tsmEndTime = endTime;
    uint64_t fallbackStartTime = 0;
    bool needsFallback = false;

    if (memMinTimeOpt.has_value()) {
        uint64_t memMinTime = *memMinTimeOpt;
        if (memMinTime <= startTime || memMinTime == 0) {
            // Entire requested range overlaps with memory data.  Under
            // last-write-wins a memory point may be a REWRITE of a point
            // already flushed to TSM, so folding both sources would let the
            // stale TSM copy participate — wrong even for "idempotent"
            // methods (an overwritten MAX would resurrect the dead value;
            // LATEST/FIRST could return the stale copy on timestamp ties).
            // Fall back to the merge-based path, which deduplicates with
            // newest-source-wins priority.
            co_return std::nullopt;
        } else {
            // Split: pushdown [startTime, memMinTime-1], fallback [memMinTime, endTime]
            tsmEndTime = (memMinTime > 0) ? memMinTime - 1 : 0;
            if (tsmEndTime < startTime) {
                tsmEndTime = startTime;
            }
            fallbackStartTime = memMinTime;
            needsFallback = true;
        }
    }

    // Fast path for BUCKETED LATEST/FIRST: skip the expensive Gate 2 overlap
    // check.  LATEST/FIRST don't aggregate across blocks (no double-counting
    // risk), so we can filter files using getSeriesType() (bloom filter +
    // sparse index, pure in-memory, no I/O) instead of getFullIndexEntry()
    // (DMA read), and stop as soon as each bucket has its one point.
    //
    // interval == 0 deliberately does NOT come here: without an interval
    // LATEST/FIRST no longer collapse the range — every distinct timestamp
    // survives, exactly as for every other method (CLAUDE.md "Aggregation
    // Result Shape").  There is no single point to seek to, so those queries
    // fall through to the normal pushdown and return raw sorted vectors.
    if ((isLatest || isFirst) && aggregationInterval > 0) {
        // Snapshot the TSM file map so that compaction cannot mutate it
        // mid-iteration across co_await suspension points.
        std::vector<std::pair<uint64_t, seastar::shared_ptr<TSM>>> seqFilesSnap(
            fileManager->getSequencedTsmFiles().begin(), fileManager->getSequencedTsmFiles().end());

        // Filter files using in-memory checks only (no I/O).
        // seriesMayOverlapTime checks bloom filter + sparse time bounds.
        std::vector<seastar::shared_ptr<TSM>> candidateFiles;
        for (const auto& [rank, tsmFile] : seqFilesSnap) {
            if (!tsmFile->seriesMayOverlapTime(seriesId, startTime, tsmEndTime))
                continue;
            auto type = tsmFile->getSeriesType(seriesId);
            if (!type.has_value())
                continue;
            // Float and Integer support pushdown aggregation.  String and
            // Boolean are non-numeric: they never aggregate arithmetically, so
            // they bypass pushdown and are returned in their written type by the
            // caller (raw, or LATEST-per-bucket with an interval).
            if (isNonNumericValueType(*type)) {
                co_return std::nullopt;
            }
            candidateFiles.push_back(tsmFile);
        }

        if (candidateFiles.empty() && !needsFallback) {
            co_return std::nullopt;
        }

        // Last-write-wins gates — fall back to the dedup merge path when the
        // early-terminating folds below could observe a stale copy:
        //
        // 1. TSM data inside the memory fallback range [fallbackStartTime,
        //    endTime]: the memory-only fold would miss TSM-only points there
        //    (out-of-order flush), and a rewritten point could fold its stale
        //    TSM copy alongside the memory value.
        if (needsFallback) {
            for (const auto& [rank, tsmFile] : seqFilesSnap) {
                if (tsmFile->seriesMayOverlapTime(seriesId, fallbackStartTime, endTime)) {
                    co_return std::nullopt;
                }
            }
        }
        // 2. Candidate files whose series time ranges overlap each other in
        //    [startTime, tsmEndTime]: the same timestamp may then exist in
        //    more than one file (a rewrite of an already-flushed point), and
        //    the first-file-wins / filledBuckets early termination below
        //    would keep whichever copy it visits first, not the newest write.
        if (candidateFiles.size() > 1) {
            std::vector<std::pair<uint64_t, uint64_t>> seriesRanges;
            seriesRanges.reserve(candidateFiles.size());
            for (const auto& f : candidateFiles) {
                const uint64_t lo = std::max(f->getSeriesMinTime(seriesId), startTime);
                const uint64_t hi = std::min(f->getSeriesMaxTime(seriesId), tsmEndTime);
                if (lo <= hi) {
                    seriesRanges.emplace_back(lo, hi);
                }
            }
            std::sort(seriesRanges.begin(), seriesRanges.end());
            uint64_t prevMax = 0;
            for (size_t i = 0; i < seriesRanges.size(); ++i) {
                if (i > 0 && seriesRanges[i].first <= prevMax) {
                    co_return std::nullopt;
                }
                prevMax = std::max(prevMax, seriesRanges[i].second);
            }
        }

        // Sort by sparse index time bounds: use the series-level maxTime/minTime
        // from the sparse index to order files by actual data recency.
        // LATEST: files with newest data first (descending maxTime).
        // FIRST: files with oldest data first (ascending minTime).
        const bool reverse = isLatest;
        if (reverse) {
            std::sort(candidateFiles.begin(), candidateFiles.end(),
                      [&seriesId](const seastar::shared_ptr<TSM>& a, const seastar::shared_ptr<TSM>& b) {
                          return a->getSeriesMaxTime(seriesId) > b->getSeriesMaxTime(seriesId);
                      });
        } else {
            std::sort(candidateFiles.begin(), candidateFiles.end(),
                      [&seriesId](const seastar::shared_ptr<TSM>& a, const seastar::shared_ptr<TSM>& b) {
                          return a->getSeriesMinTime(seriesId) < b->getSeriesMinTime(seriesId);
                      });
        }

        // Bucketed LATEST/FIRST: fold TSM + MemoryStore data into one
        // AggregationState per epoch-aligned bucket.
        //
        // The aggregator is constructed over the FULL query range [startTime,
        // endTime], not the TSM-only sub-range: when needsFallback is set, the
        // memory-store fold below feeds points up to endTime into this same
        // aggregator, and the constructor's range determines the epoch-bucket
        // layout (single-bucket optimisation).  Constructing over the narrower
        // TSM sub-range used to collapse memory points from later epoch
        // buckets into the first bucket.
        timestar::BlockAggregator aggregator(aggregationInterval, startTime, endTime, method, true);

        // Zero-I/O fast path: for LATEST/FIRST needing only 1 point, use the
        // v3 block stats cached in the sparse index. Files are already sorted
        // by time bounds, so the first candidate has the best point. No DMA
        // reads required — pure in-memory lookup.
        // One point suffices when the whole TSM range lands in one bucket.
        // (aggregationInterval > 0 is guaranteed by the branch gate above.)
        const uint64_t firstBucket = (startTime / aggregationInterval) * aggregationInterval;
        const uint64_t lastBucket = (tsmEndTime / aggregationInterval) * aggregationInterval;
        const bool needsSinglePoint = (firstBucket == lastBucket);

        if (needsSinglePoint && !candidateFiles.empty()) {
            // Try sparse index stats first (zero I/O).
            // Skip files with tombstones — sparse stats don't reflect deletions.
            bool resolved = false;
            for (auto& file : candidateFiles) {
                if (file->hasTombstones())
                    break;  // Tombstones invalidate sparse stats
                auto pt = reverse ? file->getLatestFromSparse(seriesId) : file->getFirstFromSparse(seriesId);
                if (pt.has_value() && pt->timestamp >= startTime && pt->timestamp <= tsmEndTime) {
                    aggregator.addPoint(pt->timestamp, pt->value);
                    resolved = true;
                    break;
                }
                // Sparse stats timestamp outside query range — need block-level scan
                if (pt.has_value())
                    break;
            }

            if (!resolved) {
                // Fallback: DMA-based selective read for the single point
                for (auto& file : candidateFiles) {
                    size_t pts = co_await file->aggregateSeriesSelective(seriesId, startTime, tsmEndTime, aggregator,
                                                                         reverse, 1);
                    if (pts > 0)
                        break;
                }
            }
        } else if (aggregationInterval == 0) {
            // Non-bucketed: need only 1 point total from TSM.
            for (auto& file : candidateFiles) {
                size_t pts =
                    co_await file->aggregateSeriesSelective(seriesId, startTime, tsmEndTime, aggregator, reverse, 1);
                if (pts > 0) {
                    break;  // Got our point
                }
            }
        } else {
            // Bucketed: iterate files in preferred order, share filledBuckets
            // across files for cross-file early termination.  Reuses the
            // first/last bucket computed above rather than recomputing them.
            const size_t totalBuckets = (lastBucket >= firstBucket)
                                            ? static_cast<size_t>((lastBucket - firstBucket) / aggregationInterval + 1)
                                            : 1;

            if (totalBuckets == 1) {
                // Handled above in needsSinglePoint path
            } else {
                std::unordered_set<uint64_t> filledBuckets;
                filledBuckets.reserve(totalBuckets);

                for (auto& file : candidateFiles) {
                    if (filledBuckets.size() >= totalBuckets) {
                        break;
                    }
                    co_await file->aggregateSeriesBucketed(seriesId, startTime, tsmEndTime, aggregator, reverse,
                                                           aggregationInterval, filledBuckets, totalBuckets);
                }
            }
        }

        // Fallback: fold MemoryStore data directly into the aggregator
        // for the overlap range [fallbackStartTime, endTime].  This avoids
        // materialising intermediate QueryResult vectors.
        if (needsFallback) {
            aggregateMemoryStores(pinnedStores, seriesId, fallbackStartTime, endTime, aggregator);
        }

        timestar::PushdownResult result;
        result.totalPoints = aggregator.pointCount();
        result.bucketStates = aggregator.takeBucketStates();

        co_return result;
    }

    // Gate 2: Collect index-block time ranges from every TSM file for this
    // series and verify that no blocks overlap across files within the
    // TSM-only range [startTime, tsmEndTime].  Overlapping blocks would
    // cause double-counting in SUM/AVG/COUNT.
    struct FileRef {
        seastar::shared_ptr<TSM> file;
        uint64_t maxBlockTime = 0;  // Latest block maxTime for this series in this file
        uint64_t minBlockTime = std::numeric_limits<uint64_t>::max();  // Earliest block minTime
    };
    std::vector<FileRef> filesWithData;
    std::vector<std::pair<uint64_t, uint64_t>> allBlockRanges;

    // Snapshot the TSM file map so that compaction cannot mutate it
    // mid-iteration across co_await suspension points.
    std::vector<std::pair<uint64_t, seastar::shared_ptr<TSM>>> seqFilesSnap(fileManager->getSequencedTsmFiles().begin(),
                                                                            fileManager->getSequencedTsmFiles().end());

    // Last-write-wins gate: if any TSM file has series data inside the memory
    // fallback range [fallbackStartTime, endTime], the memory-only fold below
    // would miss TSM-only points there (out-of-order flush), and a rewritten
    // point could fold its stale TSM copy alongside the memory value.  Fall
    // back to the dedup merge path.
    if (needsFallback) {
        for (const auto& [rank, tsmFile] : seqFilesSnap) {
            if (tsmFile->seriesMayOverlapTime(seriesId, fallbackStartTime, endTime)) {
                co_return std::nullopt;
            }
        }
    }

    // Pre-filter using sparse index (in-memory), then prefetch full index entries
    // in parallel for all candidate files. This overlaps DMA reads instead of
    // serializing them.
    std::vector<seastar::shared_ptr<TSM>> gate2Candidates;
    for (const auto& [rank, tsmFile] : seqFilesSnap) {
        if (tsmFile->seriesMayOverlapTime(seriesId, startTime, tsmEndTime))
            gate2Candidates.push_back(tsmFile);
    }
    if (gate2Candidates.size() > 1) {
        co_await seastar::parallel_for_each(gate2Candidates, [&seriesId](seastar::shared_ptr<TSM>& f) {
            return f->getFullIndexEntry(seriesId).discard_result();
        });
    }

    bool hasNonFloat = false;
    for (auto& tsmFile : gate2Candidates) {
        auto* indexEntry = co_await tsmFile->getFullIndexEntry(seriesId);
        if (!indexEntry)
            continue;

        // Float and Integer support pushdown; String and Boolean are
        // non-numeric and never aggregate arithmetically.
        if (isNonNumericValueType(indexEntry->seriesType)) {
            hasNonFloat = true;
            break;
        }

        FileRef ref{tsmFile};
        bool hasBlocks = false;
        for (const auto& block : indexEntry->indexBlocks) {
            if (block.minTime <= tsmEndTime && startTime <= block.maxTime) {
                allBlockRanges.push_back({block.minTime, block.maxTime});
                ref.maxBlockTime = std::max(ref.maxBlockTime, block.maxTime);
                ref.minBlockTime = std::min(ref.minBlockTime, block.minTime);
                hasBlocks = true;
            }
        }
        if (hasBlocks) {
            filesWithData.push_back(std::move(ref));
        }
    }

    if (hasNonFloat) {
        co_return std::nullopt;
    }

    if (filesWithData.empty() && !needsFallback) {
        co_return std::nullopt;  // No TSM data and no fallback needed
    }

    // Check for overlap: sort ranges by minTime and look for any
    // block whose minTime falls inside the previous block's span.
    // Overlapping blocks mean the same timestamp may exist in more than one
    // file — either a transient identical copy (compaction lag) or a genuine
    // rewrite of an already-flushed point.  Under last-write-wins BOTH are
    // disqualifying: additive methods would double-count, and even
    // "idempotent" MIN/MAX could resurrect an overwritten value.  Fall back
    // to the dedup merge path, which resolves duplicates newest-write-first.
    if (allBlockRanges.size() > 1) {
        std::sort(allBlockRanges.begin(), allBlockRanges.end());
        uint64_t prevMax = 0;
        for (size_t i = 0; i < allBlockRanges.size(); ++i) {
            if (i > 0 && allBlockRanges[i].first <= prevMax) {
                co_return std::nullopt;
            }
            prevMax = std::max(prevMax, allBlockRanges[i].second);
        }
    }

    // Gate checks passed — perform pushdown aggregation.  The aggregator is
    // constructed over the FULL query range [startTime, endTime] (not the
    // TSM-only sub-range) because the memory fallback below feeds points up to
    // endTime into it and the constructor's range determines the epoch-bucket
    // layout; TSM reads themselves stay bounded by tsmEndTime.
    //
    // Fold-to-single for interval == 0 is caller-controlled (foldNoInterval):
    // group-by callers collapse per group anyway; per-timestamp callers need
    // the raw vectors.  The decision must never depend on data placement.
    const bool canFold = (aggregationInterval == 0) && foldNoInterval && isStreamableAggMethod(method);
    timestar::BlockAggregator aggregator(aggregationInterval, startTime, endTime, method, true);
    if (canFold) {
        aggregator.enableFoldToSingleState();  // no collectRaw needed for streaming methods
    }

    // Default path: all aggregation methods other than LATEST/FIRST — read all blocks.
    // Pass per-shard I/O semaphore to bound concurrent DMA reads across all
    // series being queried on this shard (prevents reactor stalls at scale).
    seastar::semaphore* ioSem = &fileManager->queryIoSem;
    for (auto& ref : filesWithData) {
        co_await ref.file->aggregateSeries(seriesId, startTime, tsmEndTime, aggregator, ioSem);
    }

    // Fallback: fold MemoryStore data directly into the aggregator
    // for the overlap range [fallbackStartTime, endTime].  This avoids
    // materialising intermediate QueryResult vectors.
    if (needsFallback) {
        aggregateMemoryStores(pinnedStores, seriesId, fallbackStartTime, endTime, aggregator);
    }

    timestar::PushdownResult result;
    result.totalPoints = aggregator.pointCount();
    if (aggregationInterval > 0) {
        result.bucketStates = aggregator.takeBucketStates();
    } else if (canFold) {
        result.aggregatedState = aggregator.takeSingleState();
    } else {
        // Non-bucketed: ensure per-timestamp data is sorted (batches may
        // complete in non-deterministic order via parallel_for_each).
        aggregator.sortTimestamps();
        result.sortedTimestamps = aggregator.takeTimestamps();
        result.sortedValues = aggregator.takeValues();
    }

    co_return result;
}

// Template instantiations
template seastar::future<QueryResult<bool>> QueryRunner::queryTsm<bool>(const std::string& series, SeriesId128 seriesId,
                                                                        uint64_t startTime, uint64_t endTime);
template seastar::future<QueryResult<double>> QueryRunner::queryTsm<double>(const std::string& series,
                                                                            SeriesId128 seriesId, uint64_t startTime,
                                                                            uint64_t endTime);
template seastar::future<QueryResult<std::string>> QueryRunner::queryTsm<std::string>(const std::string& series,
                                                                                      SeriesId128 seriesId,
                                                                                      uint64_t startTime,
                                                                                      uint64_t endTime);
template seastar::future<QueryResult<int64_t>> QueryRunner::queryTsm<int64_t>(const std::string& series,
                                                                              SeriesId128 seriesId, uint64_t startTime,
                                                                              uint64_t endTime);
