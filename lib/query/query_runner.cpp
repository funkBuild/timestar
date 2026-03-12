#include "query_runner.hpp"

#include "logger.hpp"
#include "logging_config.hpp"
#include "memory_store.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "tsm_result.hpp"

#include <algorithm>
#include <chrono>
#include <map>
#include <optional>
#include <queue>
#include <seastar/core/distributed.hh>

typedef std::chrono::high_resolution_clock Clock;

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
// span[0] (TSM) has dedup priority: on equal timestamps its value wins.
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
            // Equal timestamps: TSM (span a, index 0) wins; skip duplicates in both.
            outTs.push_back(tsA);
            outVal.push_back(a.val());
            a.advancePast(tsA);
            b.advancePast(tsB);
        }
    }

    // Drain whichever span still has data in bulk.
    for (auto* s : {&a, &b}) {
        const size_t remaining = s->len - s->pos;
        if (remaining == 0)
            continue;
        // Bulk insert timestamps (contiguous memory).
        outTs.insert(outTs.end(), s->tsPtr + s->pos, s->tsPtr + s->len);
        // Index-based copy for values (required for vector<bool> compatibility).
        const auto& vals = *s->valVec;
        const size_t base = s->baseIdx + s->pos;
        for (size_t i = 0; i < remaining; ++i) {
            outVal.push_back(vals[base + i]);
        }
        s->pos = s->len;
    }
}

// Linear-scan merge for 3-4 sorted spans.
// On each iteration, find the span with the minimum current timestamp
// (TSM / lower index wins ties), emit it, and advance all spans that
// share that timestamp (dedup).
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
                for (size_t j = 0; j < remaining; ++j) {
                    outVal.push_back(vals[base + j]);
                }
                s.pos = s.len;
                break;
            }
            break;
        }

        // Find minimum timestamp across active spans.
        // Lower index (TSM = 0) wins on equal timestamps.
        size_t bestIdx = SIZE_MAX;
        uint64_t bestTs = UINT64_MAX;

        for (size_t i = 0; i < K; ++i) {
            if (spans[i].exhausted())
                continue;
            const uint64_t ts = spans[i].ts();
            if (ts < bestTs) {
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

        // Advance ALL spans that share this timestamp (dedup).
        for (size_t i = 0; i < K; ++i) {
            if (spans[i].exhausted())
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
        // Min-heap by timestamp; lower spanIdx wins ties (TSM priority).
        bool operator>(const HeapEntry& other) const {
            if (timestamp != other.timestamp)
                return timestamp > other.timestamp;
            return spanIdx > other.spanIdx;
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
            for (size_t i = 0; i < remaining; ++i) {
                outVal.push_back(vals[base + i]);
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

        // Emit from the winner (lowest spanIdx = highest priority).
        outTs.push_back(currentTs);
        outVal.push_back(spans[best.spanIdx].val());

        // Advance the winner and re-push if not exhausted.
        spans[best.spanIdx].advancePast(currentTs);
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
seastar::future<QueryResult<T>> QueryRunner::queryTsm(std::string series, SeriesId128 seriesId, uint64_t startTime,
                                                      uint64_t endTime) {
    LOG_QUERY_PATH(timestar::query_log, debug,
                   "QueryRunner: Querying TSM files for series={}, startTime={}, endTime={}", series, startTime,
                   endTime);

    // Pre-allocate indexed slots to avoid concurrent push_back on a shared vector.
    // Each coroutine writes to its own slot, eliminating the race condition where
    // parallel_for_each coroutines could push_back concurrently after co_await points.
    const auto& seqFiles = fileManager->getSequencedTsmFiles();
    std::vector<std::optional<TSMResult<T>>> tsmSlots(seqFiles.size());

    // First query TSM files
    size_t slotIdx = 0;
    co_await seastar::parallel_for_each(
        seqFiles.begin(), seqFiles.end(),
        [&tsmSlots, &seriesId, startTime, endTime,
         &slotIdx](const std::pair<unsigned int, seastar::shared_ptr<TSM>>& tsmTuple) -> seastar::future<> {
            // Assign index before any suspension point - safe in cooperative scheduling
            // because parallel_for_each invokes the lambda sequentially before yielding
            size_t myIdx = slotIdx++;
            const auto& [tsmRank, tsmFile] = tsmTuple;

            // Use queryWithTombstones to automatically filter out deleted data
            // SeriesId128 pre-computed by caller — no redundant hash here
            TSMResult<T> results = co_await tsmFile.get()->queryWithTombstones<T>(seriesId, startTime, endTime);

            if (results.empty())
                co_return;

            // readSeriesBatched() already sorts blocks by start time after
            // parallel_for_each (tsm.cpp:707), so no redundant sort needed here.
            tsmSlots[myIdx] = std::move(results);
        });

    // Collect non-empty results from the slots
    std::vector<TSMResult<T>> tsmResults;
    tsmResults.reserve(tsmSlots.size());
    for (auto& slot : tsmSlots) {
        if (slot.has_value()) {
            tsmResults.push_back(std::move(*slot));
        }
    }

    // Sort by rank in descending order
    std::sort(tsmResults.begin(), tsmResults.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.rank > rhs.rank; });

    // Produce sorted, deduped TSM result via the existing heap-based merge.
    QueryResult<T> result = QueryResult<T>::fromTsmResults(tsmResults);

    // Query memory stores from WAL.
    // With background TSM conversion, multiple memory stores may hold data
    // for the same series, so we query all of them.
    auto memoryMatches = walFileManager->queryAllMemoryStores<T>(seriesId);

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
    // search, then represented as a lightweight SortedSpan.  The TSM sequence
    // (index 0) has dedup priority: on equal timestamps, TSM values are kept
    // because they represent committed/established data.
    // -----------------------------------------------------------------------

    // Build sorted spans: TSM first (index 0), then each memory store.
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
    for (const auto& match : memoryMatches) {
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
        result.values.reserve(s.len);
        for (size_t i = 0; i < s.len; ++i) {
            result.values.push_back((*s.valVec)[s.baseIdx + i]);
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
    if (spans.size() == 2 && !spans[0].exhausted() && !spans[1].exhausted()) {
        uint64_t tsmMaxTs = spans[0].tsPtr[spans[0].len - 1];
        uint64_t memMinTs = spans[1].tsPtr[0];
        if (memMinTs > tsmMaxTs) {
            // Non-overlapping: just append memory data directly.
            // IMPORTANT: Save what we need from spans[1] before clearing spans.
            // spans[0].tsPtr points into result.timestamps.data(); clearing spans
            // before calling reserve() prevents a dangling raw pointer surviving a
            // potential reallocation of result.timestamps below.
            const uint64_t* memTsPtr = spans[1].tsPtr;
            size_t memLen = spans[1].len;
            const std::vector<T>* memValVec = spans[1].valVec;
            size_t memBaseIdx = spans[1].baseIdx;
            spans.clear();  // release raw pointers into result before any reallocation

            result.timestamps.reserve(totalPoints);
            result.values.reserve(totalPoints);
            result.timestamps.insert(result.timestamps.end(), memTsPtr, memTsPtr + memLen);
            // Use index-based copy for values (vector<bool> compatibility).
            const auto& memVals = *memValVec;
            for (size_t i = 0; i < memLen; ++i) {
                result.values.push_back(memVals[memBaseIdx + i]);
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
seastar::future<VariantQueryResult> QueryRunner::runQuery(std::string seriesKey, uint64_t startTime, uint64_t endTime) {
    SeriesId128 seriesId = SeriesId128::fromSeriesKey(seriesKey);
    co_return co_await runQuery(seriesKey, seriesId, startTime, endTime);
}

seastar::future<VariantQueryResult> QueryRunner::runQuery(std::string seriesKey, SeriesId128 seriesId,
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
        throw std::runtime_error("Series not found");
    }

    LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Found series type: {} for series: '{}'",
                   static_cast<int>(seriesType.value()), seriesKey);

    VariantQueryResult results;

    switch (seriesType.value()) {
        case TSMValueType::Boolean:
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Querying boolean series: '{}'", seriesKey);
            results = co_await queryTsm<bool>(seriesKey, seriesId, startTime, endTime);
            break;
        case TSMValueType::Float:
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Querying float series: '{}'", seriesKey);
            results = co_await queryTsm<double>(seriesKey, seriesId, startTime, endTime);
            break;
        case TSMValueType::String:
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Querying string series: '{}'", seriesKey);
            results = co_await queryTsm<std::string>(seriesKey, seriesId, startTime, endTime);
            break;
        case TSMValueType::Integer:
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Querying integer series: '{}'", seriesKey);
            results = co_await queryTsm<int64_t>(seriesKey, seriesId, startTime, endTime);
            break;
        default:
            LOG_QUERY_PATH(timestar::query_log, debug, "[QUERYRUNNER] Unknown series type {} for series: '{}'",
                           static_cast<int>(seriesType.value()), seriesKey);
            throw std::runtime_error("Unknown series type");
    }

    co_return std::move(results);
};

// ---------------------------------------------------------------------------
// Pushdown aggregation
// ---------------------------------------------------------------------------

seastar::future<std::optional<timestar::PushdownResult>> QueryRunner::queryTsmAggregated(
    std::string seriesKey, SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval) {
    // Gate 0: Skip pushdown entirely if there are no TSM files — all data
    // is in memory stores and the full materialisation path is appropriate.
    if (fileManager->getSequencedTsmFiles().empty()) {
        co_return std::nullopt;
    }

    // Gate 1 (split logic): Instead of rejecting entirely when memory data
    // exists, determine the split point so the TSM-only portion can still
    // benefit from pushdown aggregation.
    auto memMinTimeOpt = walFileManager->getEarliestMemoryTimestamp<double>(seriesId, startTime, endTime);

    // tsmEndTime: the upper bound for the TSM-only pushdown range.
    // fallbackStartTime: the lower bound for the fallback (TSM+memory) range.
    // When there is no memory data, the entire range is pushdown-eligible.
    uint64_t tsmEndTime = endTime;
    uint64_t fallbackStartTime = 0;
    bool needsFallback = false;

    if (memMinTimeOpt.has_value()) {
        uint64_t memMinTime = *memMinTimeOpt;
        if (memMinTime <= startTime || memMinTime == 0) {
            // Entire requested range overlaps with memory (or edge case
            // where memMinTime==0 would underflow) — cannot split, fall back.
            co_return std::nullopt;
        }
        // Split: pushdown [startTime, memMinTime-1], fallback [memMinTime, endTime]
        tsmEndTime = memMinTime - 1;
        fallbackStartTime = memMinTime;
        needsFallback = true;
    }

    // Gate 2: Collect index-block time ranges from every TSM file for this
    // series and verify that no blocks overlap across files within the
    // TSM-only range [startTime, tsmEndTime].  Overlapping blocks would
    // cause double-counting in SUM/AVG/COUNT.
    struct FileRef {
        seastar::shared_ptr<TSM> file;
    };
    std::vector<FileRef> filesWithData;
    std::vector<std::pair<uint64_t, uint64_t>> allBlockRanges;

    for (const auto& [rank, tsmFile] : fileManager->getSequencedTsmFiles()) {
        auto* indexEntry = co_await tsmFile->getFullIndexEntry(seriesId);
        if (!indexEntry)
            continue;

        // If any file reports a non-Float type, pushdown is inapplicable.
        if (indexEntry->seriesType != TSMValueType::Float) {
            co_return std::nullopt;
        }

        bool hasBlocks = false;
        for (const auto& block : indexEntry->indexBlocks) {
            if (block.minTime <= tsmEndTime && startTime <= block.maxTime) {
                allBlockRanges.push_back({block.minTime, block.maxTime});
                hasBlocks = true;
            }
        }
        if (hasBlocks) {
            filesWithData.push_back({tsmFile});
        }
    }

    if (filesWithData.empty() && !needsFallback) {
        co_return std::nullopt;  // No TSM data and no fallback needed
    }

    // Check for overlap: sort ranges by minTime and look for any
    // block whose minTime falls inside the previous block's span.
    if (allBlockRanges.size() > 1) {
        std::sort(allBlockRanges.begin(), allBlockRanges.end());
        for (size_t i = 1; i < allBlockRanges.size(); ++i) {
            if (allBlockRanges[i].first <= allBlockRanges[i - 1].second) {
                co_return std::nullopt;  // Overlap detected — fall back
            }
        }
    }

    // Gate checks passed — perform pushdown aggregation on TSM-only range.
    // Pass startTime/tsmEndTime so the constructor can pre-reserve the bucket map.
    timestar::BlockAggregator aggregator(aggregationInterval, startTime, tsmEndTime);

    for (auto& ref : filesWithData) {
        co_await ref.file->aggregateSeries(seriesId, startTime, tsmEndTime, aggregator);
    }

    // Fallback: query the overlap range [memMinTime, endTime] via the full
    // materialisation path (queryTsm handles TSM+memory dedup) and fold
    // the resulting points into the same BlockAggregator.
    if (needsFallback) {
        QueryResult<double> fallbackResult = co_await queryTsm<double>(seriesKey, seriesId, fallbackStartTime, endTime);
        if (!fallbackResult.timestamps.empty()) {
            aggregator.addPoints(fallbackResult.timestamps, fallbackResult.values);
        }
    }

    timestar::PushdownResult result;
    result.totalPoints = aggregator.pointCount();
    if (aggregationInterval > 0) {
        result.bucketStates = aggregator.takeBucketStates();
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
template seastar::future<QueryResult<bool>> QueryRunner::queryTsm<bool>(std::string series, SeriesId128 seriesId,
                                                                        uint64_t startTime, uint64_t endTime);
template seastar::future<QueryResult<double>> QueryRunner::queryTsm<double>(std::string series, SeriesId128 seriesId,
                                                                            uint64_t startTime, uint64_t endTime);
template seastar::future<QueryResult<std::string>> QueryRunner::queryTsm<std::string>(std::string series,
                                                                                      SeriesId128 seriesId,
                                                                                      uint64_t startTime,
                                                                                      uint64_t endTime);
template seastar::future<QueryResult<int64_t>> QueryRunner::queryTsm<int64_t>(std::string series, SeriesId128 seriesId,
                                                                              uint64_t startTime, uint64_t endTime);
