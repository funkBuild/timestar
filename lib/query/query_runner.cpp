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
#include <unordered_set>

using Clock = std::chrono::high_resolution_clock;

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

    // Query only candidate TSM files
    size_t slotIdx = 0;
    co_await seastar::parallel_for_each(candidateFiles.begin(), candidateFiles.end(),
                                        [&tsmSlots, &seriesId, startTime, endTime,
                                         &slotIdx](const seastar::shared_ptr<TSM>& tsmFile) -> seastar::future<> {
                                            // Assign index before any suspension point - safe in cooperative scheduling
                                            // because parallel_for_each invokes the lambda sequentially before yielding
                                            size_t myIdx = slotIdx++;

                                            // Use queryWithTombstones to automatically filter out deleted data
                                            // SeriesId128 pre-computed by caller — no redundant hash here
                                            TSMResult<T> results = co_await tsmFile.get()->queryWithTombstones<T>(
                                                seriesId, startTime, endTime);

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

// Methods that can be computed via streaming fold (addValue) without
// materialising all raw values. MEDIAN requires nth_element on the full
// dataset, LATEST/FIRST have their own fast path.
static bool isStreamableMethod(timestar::AggregationMethod method) {
    switch (method) {
        case timestar::AggregationMethod::AVG:
        case timestar::AggregationMethod::MIN:
        case timestar::AggregationMethod::MAX:
        case timestar::AggregationMethod::SUM:
        case timestar::AggregationMethod::COUNT:
        case timestar::AggregationMethod::SPREAD:
        case timestar::AggregationMethod::STDDEV:
        case timestar::AggregationMethod::STDVAR:
            return true;
        default:
            return false;
    }
}

// ---------------------------------------------------------------------------
// aggregateMemoryStores — fold MemoryStore data directly into a
// BlockAggregator without materialising intermediate QueryResult vectors.
// Returns the number of points aggregated.
// ---------------------------------------------------------------------------
static size_t aggregateMemoryStores(WALFileManager* walFileManager, const SeriesId128& seriesId, uint64_t startTime,
                                    uint64_t endTime, timestar::BlockAggregator& aggregator) {
    auto memoryMatches = walFileManager->queryAllMemoryStores<double>(seriesId);
    size_t totalPoints = 0;
    for (const auto& match : memoryMatches) {
        const auto& storeData = *match.series;
        if (storeData.timestamps.empty())
            continue;
        auto beginIt = std::lower_bound(storeData.timestamps.begin(), storeData.timestamps.end(), startTime);
        auto endIt = std::upper_bound(beginIt, storeData.timestamps.end(), endTime);
        size_t startIdx = static_cast<size_t>(beginIt - storeData.timestamps.begin());
        size_t endIdx = static_cast<size_t>(endIt - storeData.timestamps.begin());
        size_t count = endIdx - startIdx;
        if (count == 0)
            continue;

        // Feed points directly into the aggregator — no vector copy.
        for (size_t i = startIdx; i < endIdx; ++i) {
            aggregator.addPoint(storeData.timestamps[i], storeData.values[i]);
        }
        totalPoints += count;
    }
    return totalPoints;
}

seastar::future<std::optional<timestar::PushdownResult>> QueryRunner::queryTsmAggregated(
    std::string seriesKey, SeriesId128 seriesId, uint64_t startTime, uint64_t endTime, uint64_t aggregationInterval,
    timestar::AggregationMethod method) {
    // Gate 0.5: MEDIAN needs all raw values for nth_element — cannot stream.
    if (aggregationInterval == 0 && method == timestar::AggregationMethod::MEDIAN) {
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
        // LATEST/FIRST without interval: fold all MemoryStore points into a
        // single AggregationState (returns 1 point, not N raw points).
        const bool foldLatestFirst = (aggregationInterval == 0) && (isLatest || isFirst);
        timestar::BlockAggregator aggregator(aggregationInterval, startTime, endTime, method, true);
        if (foldLatestFirst) {
            aggregator.setFoldToSingleState(false);
        }

        size_t pts = aggregateMemoryStores(walFileManager, seriesId, startTime, endTime, aggregator);
        if (pts == 0) {
            co_return std::nullopt;
        }

        timestar::PushdownResult result;
        result.totalPoints = aggregator.pointCount();
        if (aggregationInterval > 0) {
            result.bucketStates = aggregator.takeBucketStates();
        } else if (foldLatestFirst) {
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
            // Entire requested range overlaps with memory — aggregate both
            // TSM and memory data directly instead of falling back to full
            // materialisation with intermediate QueryResult vectors.
            const bool foldLatestFirst = (aggregationInterval == 0) && (isLatest || isFirst);
            timestar::BlockAggregator aggregator(aggregationInterval, startTime, endTime, method, true);
            if (foldLatestFirst) {
                aggregator.setFoldToSingleState(false);
            }

            // Aggregate TSM data first, then memory stores.
            std::vector<std::pair<uint64_t, seastar::shared_ptr<TSM>>> seqSnap(
                fileManager->getSequencedTsmFiles().begin(), fileManager->getSequencedTsmFiles().end());

            if (isLatest || isFirst) {
                // LATEST/FIRST: only need 1 point from TSM (the newest/oldest).
                // Sort files by series time bounds and use sparse index for zero-I/O.
                std::sort(seqSnap.begin(), seqSnap.end(), [&](const auto& a, const auto& b) {
                    return isLatest ? a.second->getSeriesMaxTime(seriesId) > b.second->getSeriesMaxTime(seriesId)
                                    : a.second->getSeriesMinTime(seriesId) < b.second->getSeriesMinTime(seriesId);
                });

                bool tsmResolved = false;
                for (const auto& [rank, tsmFile] : seqSnap) {
                    if (!tsmFile->seriesMayOverlapTime(seriesId, startTime, endTime))
                        continue;
                    if (tsmFile->hasTombstones()) {
                        tsmResolved = false;
                        break;
                    }
                    auto pt = isLatest ? tsmFile->getLatestFromSparse(seriesId) : tsmFile->getFirstFromSparse(seriesId);
                    if (pt.has_value() && pt->timestamp >= startTime && pt->timestamp <= endTime) {
                        aggregator.addPoint(pt->timestamp, pt->value);
                        tsmResolved = true;
                        break;
                    }
                    if (pt.has_value())
                        break;
                }

                if (!tsmResolved) {
                    for (const auto& [rank, tsmFile] : seqSnap) {
                        if (!tsmFile->seriesMayOverlapTime(seriesId, startTime, endTime))
                            continue;
                        size_t pts = co_await tsmFile->aggregateSeriesSelective(seriesId, startTime, endTime,
                                                                                aggregator, isLatest, 1);
                        if (pts > 0)
                            break;
                    }
                }
            } else {
                // Prefetch index entries for all files in parallel, then aggregate.
                // This overlaps DMA reads instead of serializing them per file.
                {
                    std::vector<seastar::shared_ptr<TSM>> toFetch;
                    for (const auto& [rank, tsmFile] : seqSnap) {
                        if (tsmFile->seriesMayOverlapTime(seriesId, startTime, endTime))
                            toFetch.push_back(tsmFile);
                    }
                    if (toFetch.size() > 1) {
                        co_await seastar::parallel_for_each(toFetch, [&seriesId](seastar::shared_ptr<TSM>& f) {
                            return f->getFullIndexEntry(seriesId).discard_result();
                        });
                    }
                }
                for (const auto& [rank, tsmFile] : seqSnap) {
                    co_await tsmFile->aggregateSeries(seriesId, startTime, endTime, aggregator);
                }
            }

            // Then fold in MemoryStore data.
            aggregateMemoryStores(walFileManager, seriesId, startTime, endTime, aggregator);

            if (aggregator.pointCount() == 0) {
                co_return std::nullopt;
            }

            timestar::PushdownResult result;
            result.totalPoints = aggregator.pointCount();
            if (aggregationInterval > 0) {
                result.bucketStates = aggregator.takeBucketStates();
            } else if (foldLatestFirst) {
                result.aggregatedState = aggregator.takeSingleState();
            } else {
                aggregator.sortTimestamps();
                result.sortedTimestamps = aggregator.takeTimestamps();
                result.sortedValues = aggregator.takeValues();
            }
            co_return result;
        }
        // Split: pushdown [startTime, memMinTime-1], fallback [memMinTime, endTime]
        tsmEndTime = memMinTime - 1;
        fallbackStartTime = memMinTime;
        needsFallback = true;
    }

    // Fast path for LATEST/FIRST: skip the expensive Gate 2 overlap check.
    // LATEST/FIRST don't aggregate across blocks (no double-counting risk),
    // so we can filter files using getSeriesType() (bloom filter + sparse
    // index, pure in-memory, no I/O) instead of getFullIndexEntry() (DMA read).
    if (isLatest || isFirst) {
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
            // Float, Integer, and Boolean support pushdown aggregation; String does not.
            if (*type == TSMValueType::String) {
                co_return std::nullopt;
            }
            candidateFiles.push_back(tsmFile);
        }

        if (candidateFiles.empty() && !needsFallback) {
            co_return std::nullopt;
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

        // Non-bucketed LATEST/FIRST: fold TSM + MemoryStore data into a single
        // AggregationState so we return 1 point total, not N raw points.
        const bool foldNonBucketed = (aggregationInterval == 0);
        timestar::BlockAggregator aggregator(aggregationInterval, startTime, tsmEndTime, method, true);
        if (foldNonBucketed) {
            aggregator.setFoldToSingleState(false);
        }

        // Zero-I/O fast path: for LATEST/FIRST needing only 1 point, use the
        // v3 block stats cached in the sparse index. Files are already sorted
        // by time bounds, so the first candidate has the best point. No DMA
        // reads required — pure in-memory lookup.
        bool needsSinglePoint = (aggregationInterval == 0);
        if (!needsSinglePoint && aggregationInterval > 0) {
            uint64_t firstBucket = (startTime / aggregationInterval) * aggregationInterval;
            uint64_t lastBucket = (tsmEndTime / aggregationInterval) * aggregationInterval;
            needsSinglePoint = (firstBucket == lastBucket);
        }

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
            // across files for cross-file early termination.
            uint64_t firstBucket = (startTime / aggregationInterval) * aggregationInterval;
            uint64_t lastBucket = (tsmEndTime / aggregationInterval) * aggregationInterval;
            size_t totalBuckets = static_cast<size_t>((lastBucket - firstBucket) / aggregationInterval + 1);

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
            aggregateMemoryStores(walFileManager, seriesId, fallbackStartTime, endTime, aggregator);
        }

        timestar::PushdownResult result;
        result.totalPoints = aggregator.pointCount();
        if (aggregationInterval > 0) {
            result.bucketStates = aggregator.takeBucketStates();
        } else if (foldNonBucketed) {
            result.aggregatedState = aggregator.takeSingleState();
        } else {
            aggregator.sortTimestamps();
            result.sortedTimestamps = aggregator.takeTimestamps();
            result.sortedValues = aggregator.takeValues();
        }

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

        // Float, Integer, and Boolean support pushdown; String does not.
        if (indexEntry->seriesType == TSMValueType::String) {
            hasNonFloat = true;
            break;
        }

        FileRef ref{tsmFile};
        for (const auto& block : indexEntry->indexBlocks) {
            if (block.minTime <= tsmEndTime && startTime <= block.maxTime) {
                allBlockRanges.push_back({block.minTime, block.maxTime});
                ref.maxBlockTime = std::max(ref.maxBlockTime, block.maxTime);
                ref.minBlockTime = std::min(ref.minBlockTime, block.minTime);
            }
        }
        if (ref.maxBlockTime > 0) {
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
    const bool canFold = (aggregationInterval == 0) && isStreamableMethod(method);
    timestar::BlockAggregator aggregator(aggregationInterval, startTime, tsmEndTime, method, true);
    if (canFold) {
        aggregator.setFoldToSingleState(false);  // no collectRaw needed for streaming methods
    }

    // Default path: all aggregation methods other than LATEST/FIRST — read all blocks.
    for (auto& ref : filesWithData) {
        co_await ref.file->aggregateSeries(seriesId, startTime, tsmEndTime, aggregator);
    }

    // Fallback: fold MemoryStore data directly into the aggregator
    // for the overlap range [fallbackStartTime, endTime].  This avoids
    // materialising intermediate QueryResult vectors.
    if (needsFallback) {
        aggregateMemoryStores(walFileManager, seriesId, fallbackStartTime, endTime, aggregator);
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
