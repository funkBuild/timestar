#include "tsm_compactor.hpp"

#include "aggregator.hpp"         // AggregationState for downsampling
#include "bulk_block_loader.hpp"  // Phase A: Bulk loading optimization
#include "logger.hpp"
#include "tsm_file_manager.hpp"
#include "tsm_writer.hpp"

#include <chrono>
#include <filesystem>
#include <limits>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/util/defer.hh>
#include <set>
#include <unordered_map>
#include <unordered_set>

namespace fs = std::filesystem;

TSMCompactor::TSMCompactor(TSMFileManager* manager)
    : fileManager(manager),
      strategy(std::make_unique<LeveledCompactionStrategy>()),
      compactionSemaphore(timestar::config().storage.compaction.max_concurrent) {}

std::string TSMCompactor::generateCompactedFilename(uint64_t tier, uint64_t seqNum, uint64_t dataSeq) {
    // The `_d<dataSeq>` suffix records the newest write generation contained
    // in the output (max of the inputs' dataSeq) so last-write-wins dedup
    // ranks this file by its data recency, not by its (fresh) seqNum.
    return timestar::shardDataPath(seastar::this_shard_id()) + "/tsm/" + std::to_string(tier) + "_" +
           std::to_string(seqNum) + "_d" + std::to_string(dataSeq) + ".tsm";
}

uint64_t TSMCompactor::maxDataSeqOf(const std::vector<seastar::shared_ptr<TSM>>& files) {
    uint64_t maxSeq = 0;
    for (const auto& file : files) {
        maxSeq = std::max(maxSeq, file->dataSeq);
    }
    return maxSeq;
}

std::vector<SeriesId128> TSMCompactor::getAllSeriesIds(const std::vector<seastar::shared_ptr<TSM>>& files) {
    // Use unordered_set for O(1) average insert instead of O(log n) with std::set.
    // At 100K series this is ~6x faster (see B6 benchmark).
    std::unordered_set<SeriesId128, SeriesId128::Hash> uniqueIds;

    // Pre-size for expected cardinality to avoid rehashing. Count and visit
    // without materializing per-file id vectors — getSeriesIds() is a
    // 16 B x series copy per call, and this used to pay it TWICE per file
    // (once discarded immediately for .size()).
    size_t totalEstimate = 0;
    for (const auto& file : files) {
        totalEstimate += file->getSeriesCount();
    }
    uniqueIds.reserve(totalEstimate);

    for (const auto& file : files) {
        file->forEachSeriesId([&](const SeriesId128& id) { uniqueIds.insert(id); });
    }

    return std::vector<SeriesId128>(uniqueIds.begin(), uniqueIds.end());
}

// Phase 3: Process series for compaction without writing (enables parallel processing)
template <typename T>
seastar::future<SeriesCompactionData<T>> TSMCompactor::processSeriesForCompaction(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention, PointChunkSink<T> sink) {
    SeriesCompactionData<T> result(seriesId, TSM::getValueType<T>());

    // Index-only pass: figure out WHAT this series looks like before loading
    // anything.  Decoding every block of the series from every input file up
    // front made compaction memory scale with the DECOMPRESSED size of the
    // series (~12x the on-disk size on real data), which is what made deep-tier
    // merges throw std::bad_alloc while the files on disk were still small.
    //
    // The zero-copy path below never touches decoded data -- it re-reads the
    // blocks compressed and passes them through -- so the overlap decision has
    // to happen before any load, not after.  Only the merge path needs points.
    // Block metadata is COPIED OUT here, in the same iteration that loaded the
    // index entry, with no suspension in between.
    //
    // It must not be re-read later: getFullIndexEntry() populates a byte-budgeted
    // LRU (TSM::fullIndexCache) that other concurrently-compacting series share,
    // and getSeriesBlocks() returns a static EMPTY vector on a cache miss rather
    // than signalling one. Loading every file first and reading the entries back
    // afterwards therefore risks a mid-loop eviction turning into "this file has
    // no blocks for this series" -- which would drop that file's data from the
    // output while executeCompaction() still deletes the file. Silent data loss,
    // and invisible in the stats, since points-read and points-written are both
    // derived from the surviving blocks.
    std::vector<seastar::shared_ptr<TSM>> filesWithSeries;
    std::vector<BlockMetadata<T>> blockMeta;
    filesWithSeries.reserve(sources.size());
    for (const auto& file : sources) {
        // Populates the full-index cache; no block data is read.
        auto* indexEntry = co_await file->getFullIndexEntry(seriesId);
        if (!indexEntry) {
            continue;  // Series not in this file
        }
        // Read straight from the entry we just obtained, NOT via a second
        // getSeriesBlocks() lookup that a later suspension could invalidate.
        const auto& indexBlocks = indexEntry->indexBlocks;
        if (indexBlocks.empty()) {
            continue;
        }

        const size_t fileIdx = filesWithSeries.size();
        filesWithSeries.push_back(file);
        const uint64_t fileRank = file->dataRank();

        blockMeta.reserve(blockMeta.size() + indexBlocks.size());
        for (size_t blockIdx = 0; blockIdx < indexBlocks.size(); blockIdx++) {
            const auto& indexBlock = indexBlocks[blockIdx];

            BlockMetadata<T> meta;
            meta.minTime = indexBlock.minTime;
            meta.maxTime = indexBlock.maxTime;
            meta.fileIndex = fileIdx;
            meta.blockIndex = blockIdx;
            meta.blockPtr = nullptr;  // only the merge path needs decoded points
            meta.fileRank = fileRank;
            meta.sourceFile = file;
            meta.indexBlock = indexBlock;

            blockMeta.push_back(meta);
        }
    }

    if (filesWithSeries.empty()) {
        co_return result;  // Empty result
    }

    // Collect tombstones
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    for (const auto& file : sources) {
        if (file->hasTombstones()) {
            auto tombstones = file->getTombstones();
            if (tombstones) {
                auto ranges = tombstones->getTombstoneRanges(seriesId);
                tombstoneRanges.insert(tombstoneRanges.end(), ranges.begin(), ranges.end());
            }
        }
    }

    // Sort and merge overlapping tombstone ranges
    if (!tombstoneRanges.empty()) {
        std::sort(tombstoneRanges.begin(), tombstoneRanges.end());

        std::vector<std::pair<uint64_t, uint64_t>> mergedRanges;
        mergedRanges.push_back(tombstoneRanges[0]);

        for (size_t i = 1; i < tombstoneRanges.size(); ++i) {
            auto& last = mergedRanges.back();
            const auto& current = tombstoneRanges[i];

            if (current.first <= last.second + 1) {
                last.second = std::max(last.second, current.second);
            } else {
                mergedRanges.push_back(current);
            }
        }

        tombstoneRanges = std::move(mergedRanges);
    }

    // Sort blocks by timestamp
    std::sort(blockMeta.begin(), blockMeta.end());

    // Analyze overlap patterns
    std::vector<MergeSegment> segments;
    if (!blockMeta.empty()) {
        size_t segmentStart = 0;
        bool currentNeedsMerge = false;

        for (size_t i = 1; i < blockMeta.size(); i++) {
            bool overlaps = blockMeta[i - 1].overlapsWith(blockMeta[i]);

            if (i == 1) {
                currentNeedsMerge = overlaps;
            } else if (overlaps != currentNeedsMerge) {
                segments.push_back({segmentStart, i - 1, currentNeedsMerge});
                segmentStart = i;
                currentNeedsMerge = overlaps;
            }
        }

        segments.push_back({segmentStart, blockMeta.size() - 1, currentNeedsMerge});
    }

    // Look up retention context for this series (populated in compact())
    auto retIt = seriesRetention.find(seriesId);
    bool hasRetention = (retIt != seriesRetention.end());
    uint64_t ttlCutoff = hasRetention ? retIt->second.ttlCutoff : 0;
    uint64_t downsampleThreshold = hasRetention ? retIt->second.downsampleThreshold : 0;
    uint64_t downsampleInterval = hasRetention ? retIt->second.downsampleInterval : 0;
    auto downsampleMethod = hasRetention ? retIt->second.downsampleMethod : timestar::AggregationMethod::AVG;

    // Check if we can use zero-copy fast path
    // Retention (TTL or downsampling) disables zero-copy since we need to filter/transform points
    bool allBlocksNonOverlapping =
        segments.empty() ||
        std::all_of(segments.begin(), segments.end(), [](const MergeSegment& seg) { return !seg.needsMerge; });

    // Whole-block TTL elimination: drop blocks where all data is expired
    if (ttlCutoff > 0 && !blockMeta.empty()) {
        size_t beforeCount = blockMeta.size();
        std::erase_if(blockMeta, [ttlCutoff](const BlockMetadata<T>& meta) { return meta.maxTime < ttlCutoff; });
        size_t eliminated = beforeCount - blockMeta.size();
        if (eliminated > 0) {
            timestar::compactor_log.info("TTL: Eliminated {} whole blocks for series {}", eliminated, seriesId.toHex());
        }

        if (blockMeta.empty()) {
            // All data expired
            co_return result;
        }
    }

    bool hasPerPointRetention = (ttlCutoff > 0 || downsampleThreshold > 0);

    // String dictionaries (STR2 blocks store per-file dictionary IDs): the
    // zero-copy carry is only sound when the output file's index entry can
    // hold ONE dictionary that resolves every carried block's IDs — i.e. all
    // blocks come from a single source file. Dictionaries from different
    // source files have incompatible ID→string mappings, so such series must
    // take the slow path (decode each file with its own dictionary, re-encode
    // with a fresh one). Previously the zero-copy path NEVER propagated the
    // dictionary: the compacted file contained STR2 blocks with dictSize=0 in
    // its index, making the series' values permanently undecodable ("Invalid
    // magic number in string encoding") after their first compaction.
    bool stringDictForcesSlowPath = false;
    std::shared_ptr<const std::vector<std::string>> carriedStringDict;
    if constexpr (std::is_same_v<T, std::string>) {
        size_t dictSources = 0;
        for (const auto& file : filesWithSeries) {
            // Cheap: the index-only pass above already populated the cache.
            auto* entry = co_await file->getFullIndexEntry(seriesId);
            if (entry && entry->stringDictionary && !entry->stringDictionary->empty()) {
                ++dictSources;
                carriedStringDict = entry->stringDictionary;
            }
        }
        if (dictSources > 0 && filesWithSeries.size() > 1) {
            stringDictForcesSlowPath = true;
            carriedStringDict = nullptr;
        }
    }

    // Should this series be re-encoded to consolidate under-full blocks?
    //
    // Sized from COMPRESSED bytes, never from TSMIndexBlock::blockCount: for
    // Float that field is the non-NaN count (an all-NaN block reports 0), so a
    // sparse series of FULL blocks would look empty and get decoded -- which is
    // how an earlier version of this gate reintroduced the very bad_alloc this
    // work removes. Compressed bytes are NaN-independent and also correctly
    // reflect string payloads, which sizeof(T) does not.
    bool coalesceUnderfullBlocks = false;
    uint64_t coalesceBudgetKiB = 0;
    if (blockMeta.size() >= MIN_BLOCKS_TO_CONSIDER_COALESCING && tombstoneRanges.empty() && !hasPerPointRetention) {
        uint64_t totalCompressedBytes = 0;
        for (const auto& meta : blockMeta) {
            totalCompressedBytes += meta.indexBlock.size;
        }
        const uint64_t avgBlockBytes = totalCompressedBytes / blockMeta.size();

        // Strings: bigger expansion estimate, smaller compressed cap — zstd on
        // repetitive text can exceed the numeric ~12x by a wide margin.
        const uint64_t coalesceCap =
            std::is_same_v<T, std::string> ? MAX_COALESCE_COMPRESSED_BYTES_STRING : MAX_COALESCE_COMPRESSED_BYTES;
        if (avgBlockBytes < UNDERFULL_BLOCK_BYTES && totalCompressedBytes <= coalesceCap) {
            // Reserve the estimated decoded cost from the shard-wide budget, so
            // concurrent series cannot each spend the per-series ceiling. If the
            // budget is exhausted, skip coalescing -- it is an optimisation, and
            // the zero-copy carry remains correct.
            coalesceBudgetKiB = std::max<uint64_t>(1, totalCompressedBytes * decodedExpansionEstimate<T>() / 1024);
            if (coalesceBudgetKiB <= coalesceBudget_.current() && coalesceBudget_.try_wait(coalesceBudgetKiB)) {
                coalesceUnderfullBlocks = true;
            }
        }
    }
    // Release the reservation however this coroutine exits.
    auto releaseCoalesceBudget = seastar::defer([this, &coalesceUnderfullBlocks, coalesceBudgetKiB] {
        if (coalesceUnderfullBlocks) {
            coalesceBudget_.signal(coalesceBudgetKiB);
        }
    });

    if (allBlocksNonOverlapping && !stringDictForcesSlowPath && !blockMeta.empty() && tombstoneRanges.empty() &&
        !hasPerPointRetention && !coalesceUnderfullBlocks) {
        // ZERO-COPY PATH: record where each block lives; do not read any of them.
        // The writer streams them through one at a time, so this whole path now
        // performs no block I/O and holds no block bytes.
        result.isZeroCopy = true;
        result.blockRefs.reserve(blockMeta.size());

        for (const auto& meta : blockMeta) {
            typename SeriesCompactionData<T>::BlockRef ref;
            ref.sourceFile = meta.sourceFile;
            ref.indexBlock = meta.indexBlock;
            ref.minTime = meta.minTime;
            ref.maxTime = meta.maxTime;
            result.blockRefs.push_back(std::move(ref));

            // Stats come from the index, so they are known without reading.
            result.pointsRead += meta.indexBlock.blockCount;
            result.pointsWritten += meta.indexBlock.blockCount;
        }

        // Carry the single-source string dictionary so the output file's
        // index entry can decode the carried STR2 blocks.
        result.stringDictionary = std::move(carriedStringDict);

        co_return result;
    }

    // SLOW PATH: Decompress and merge (overlap, tombstones, or retention filtering).
    // This is the ONLY path that needs decoded points, so the load happens here
    // rather than up front -- a series that can be carried through zero-copy now
    // costs nothing but its index entries.
    // NOTE: the bulk load is deliberately NOT done here. When the blocks do not
    // overlap -- which is the case whenever this path is entered only because of
    // tombstones or retention, not because of genuinely interleaved data -- the
    // points are consumed strictly in time order, so blocks can be decoded one at
    // a time and released. Loading all of them first would hold the entire
    // decoded series (~12x its on-disk size) resident for no reason.
    // Only a real merge needs every block simultaneously; see the else-branch.
    result.isZeroCopy = false;
    result.timestamps.reserve(batchSize() * 10);  // Pre-allocate
    result.values.reserve(batchSize() * 10);

    uint64_t lastTimestamp = std::numeric_limits<uint64_t>::max();
    size_t ttlFiltered = 0;

    // Incremental emission state. Chunking is available whenever a sink is
    // supplied — including under a downsample policy: the merged stream is
    // ascending and the threshold is fixed, so every old-segment
    // (< threshold) point precedes every recent one. Old points are folded
    // into a persistent bucket map at spill time (never handed to the sink),
    // and the completed buckets are emitted the moment the first recent point
    // reaches a spill — before any recent point is written, preserving
    // ascending block order. Downsampling therefore no longer needs the whole
    // series resident; it was the last remaining whole-series path.
    const bool chunkedEmit = static_cast<bool>(sink);
    size_t bufferedSinceSpill = 0;

    constexpr bool kTypeSupportsDownsample = std::is_same_v<T, double> || std::is_same_v<T, int64_t>;
    const bool streamingDownsample =
        chunkedEmit && kTypeSupportsDownsample && downsampleThreshold > 0 && downsampleInterval > 0;
    std::map<uint64_t, timestar::AggregationState> dsBuckets;  // old-segment fold (streaming downsample)
    bool dsFlushed = false;        // buckets have been emitted; stream is pure recent from here
    size_t dsOldPointsFolded = 0;  // raw points folded, for pointsWritten accounting

    // Fold result[0..count) into dsBuckets and drop them from the buffers.
    // Only ever called with count <= the deduplicated prefix (the retained
    // last point is excluded until the series is complete).
    auto foldOldPrefixIntoBuckets = [&](size_t count) {
        if constexpr (kTypeSupportsDownsample) {
            for (size_t i = 0; i < count; ++i) {
                uint64_t bucket = (result.timestamps[i] / downsampleInterval) * downsampleInterval;
                dsBuckets[bucket].addValue(static_cast<double>(result.values[i]), result.timestamps[i]);
            }
            dsOldPointsFolded += count;
            result.timestamps.erase(result.timestamps.begin(), result.timestamps.begin() + count);
            result.values.erase(result.values.begin(), result.values.begin() + count);
        } else {
            (void)count;
        }
    };

    // Emit the completed bucket map through the sink in bounded chunks
    // (ascending by construction — std::map), then release it.
    auto flushDsBuckets = [&]() -> seastar::future<> {
        if constexpr (kTypeSupportsDownsample) {
            if (!dsBuckets.empty()) {
                const size_t bucketCount = dsBuckets.size();
                std::vector<uint64_t> ts;
                std::vector<T> vals;
                ts.reserve(std::min(bucketCount, MERGE_CHUNK_POINTS));
                vals.reserve(std::min(bucketCount, MERGE_CHUNK_POINTS));
                for (auto& [bucket, state] : dsBuckets) {
                    ts.push_back(bucket);
                    double aggVal = state.getValue(downsampleMethod);
                    if constexpr (std::is_same_v<T, double>) {
                        vals.push_back(aggVal);
                    } else {
                        vals.push_back(static_cast<int64_t>(aggVal));
                    }
                    if (ts.size() >= MERGE_CHUNK_POINTS) {
                        result.emittedViaSink = true;
                        co_await sink(std::move(ts), std::move(vals));
                        ts = {};
                        vals = {};
                    }
                }
                if (!ts.empty()) {
                    result.emittedViaSink = true;
                    co_await sink(std::move(ts), std::move(vals));
                }
                // processPoint counted every folded raw point as written;
                // replace that with the bucket count actually emitted.
                result.pointsWritten = result.pointsWritten - dsOldPointsFolded + bucketCount;
                timestar::compactor_log.info("Downsample (streaming): {} old points -> {} buckets for series {}",
                                             dsOldPointsFolded, bucketCount, seriesId.toHex());
                dsBuckets.clear();
            }
        }
        dsFlushed = true;
    };

    // Hand buffered points to the sink, always RETAINING at least the final
    // point: processPoint resolves a duplicate timestamp by overwriting
    // result.values.back(), so spilling the whole buffer would leave the next
    // duplicate with nothing to overwrite and emit the point twice, breaking
    // last-write-wins across a chunk boundary.
    //
    // Two deliberate refinements over the obvious copy-prefix-then-erase:
    //  - The sent count is BLOCK-ALIGNED: appendSeriesChunk re-chunks each
    //    call at MaxPointsPerBlock, so an unaligned spill left a short
    //    (~MERGE_CHUNK_POINTS % block) tail block on EVERY spill — systematic
    //    fragmentation the under-full coalescer never reclaims. Only the
    //    series' final chunk may now end short.
    //  - The buffers are MOVED into the chunk (the small unaligned tail is
    //    re-seeded into fresh vectors), instead of copying the prefix and
    //    erasing it — which transiently held ~2x the chunk per series.
    const size_t blockPoints = MaxPointsPerBlock();
    auto spill = [&]() -> seastar::future<> {
        if (!chunkedEmit || result.timestamps.size() < 2) {
            co_return;
        }

        if (streamingDownsample && !dsFlushed) {
            // Fold the old-segment part of the deduplicated prefix (all but
            // the retained last point) into the bucket map instead of sending
            // it to the sink.
            const size_t prefix = result.timestamps.size() - 1;
            const size_t partIdx = static_cast<size_t>(
                std::lower_bound(result.timestamps.begin(), result.timestamps.begin() + prefix, downsampleThreshold) -
                result.timestamps.begin());
            if (partIdx > 0) {
                foldOldPrefixIntoBuckets(partIdx);
            }
            if (!result.timestamps.empty() && result.timestamps.front() >= downsampleThreshold) {
                // A recent point reached a spill: the ascending stream proves
                // the old segment is complete. Emit the buckets NOW, before
                // any recent point is written, so block order stays ascending.
                co_await flushDsBuckets();
                // Fall through to the normal send below for the recent points.
            } else {
                // Everything spillable was old and is now folded; only the
                // retained point remains.
                bufferedSinceSpill = result.timestamps.size();
                co_return;
            }
        }

        // Largest block-aligned send that still retains >= 1 point.
        const size_t send = ((result.timestamps.size() - 1) / blockPoints) * blockPoints;
        if (send == 0) {
            // Less than one full block buffered — nothing to hand off yet.
            // Reset the trigger so every subsequent group boundary does not
            // re-enter here (the downsample fold above can shrink a full
            // buffer to a handful of points).
            bufferedSinceSpill = result.timestamps.size();
            co_return;
        }

        std::vector<uint64_t> tailTs(result.timestamps.begin() + send, result.timestamps.end());
        std::vector<T> tailVals(std::make_move_iterator(result.values.begin() + send),
                                std::make_move_iterator(result.values.end()));
        result.timestamps.resize(send);
        result.values.resize(send);

        auto chunkTs = std::move(result.timestamps);
        auto chunkVals = std::move(result.values);
        result.timestamps = std::move(tailTs);
        result.values = std::move(tailVals);
        bufferedSinceSpill = result.timestamps.size();
        result.emittedViaSink = true;

        co_await sink(std::move(chunkTs), std::move(chunkVals));
    };

    // Helper lambda: check tombstone + TTL for a single point, append if valid
    auto processPoint = [&](uint64_t ts, const T& val) {
        result.pointsRead++;

        // TTL filtering
        if (ttlCutoff > 0 && ts < ttlCutoff) {
            ttlFiltered++;
            return;
        }

        // Check tombstones
        bool isTombstoned = false;
        if (!tombstoneRanges.empty()) {
            auto it = std::upper_bound(
                tombstoneRanges.begin(), tombstoneRanges.end(), ts,
                [](uint64_t timestamp, const std::pair<uint64_t, uint64_t>& range) { return timestamp < range.first; });

            if (it != tombstoneRanges.begin()) {
                --it;
                const auto& [startTime, endTime] = *it;
                if (ts >= startTime && ts < endTime) {
                    isTombstoned = true;
                }
            }
        }

        if (isTombstoned) {
            return;
        }

        // Deduplicate (last-write-wins). The bulk mergers already emit one
        // point per distinct timestamp (newest source, last copy per source),
        // so equal consecutive timestamps only occur on the sequential
        // non-overlapping path, where the stream is a single file in append
        // order — the LATER copy is the newer write and replaces the kept one.
        if (ts != lastTimestamp) {
            result.timestamps.push_back(ts);
            result.values.push_back(val);
            lastTimestamp = ts;
            result.pointsWritten++;
            ++bufferedSinceSpill;
        } else {
            result.values.back() = val;
            result.duplicatesRemoved++;
        }
    };

    // Helper to process blocks
    auto processBlock = [&](const TSMBlock<T>* block) {
        if (!block) {
            timestar::compactor_log.warn("Skipping null block pointer for series {}", seriesId.toHex());
            return;
        }
        for (size_t i = 0; i < block->timestamps.size(); i++) {
            processPoint(block->timestamps.at(i), block->values.at(i));
        }
    };

    // Decode a single block with its own file's string dictionary.
    // Dictionaries are per-series-per-file, and the index entry can be evicted
    // from the LRU across the read, so take a refcount rather than a raw pointer.
    auto decodeBlockOf = [&](const BlockMetadata<T>& meta) -> seastar::future<std::unique_ptr<TSMBlock<T>>> {
        [[maybe_unused]] std::shared_ptr<const std::vector<std::string>> dictRef;
        const std::vector<std::string>* dict = nullptr;
        if constexpr (std::is_same_v<T, std::string>) {
            auto* entry = co_await meta.sourceFile->getFullIndexEntry(seriesId);
            if (entry && entry->stringDictionary && !entry->stringDictionary->empty()) {
                dictRef = entry->stringDictionary;
                dict = dictRef.get();
            }
        }
        co_return co_await meta.sourceFile->template readSingleBlock<T>(meta.indexBlock, 0,
                                                                        std::numeric_limits<uint64_t>::max(), dict);
    };

    // Walk the (time-sorted) blocks in maximal TIME-CONNECTED GROUPS: a block
    // joins the current group if it starts at or before the group's running max
    // end time, otherwise it begins a new one. Groups are therefore disjoint in
    // time and can be processed strictly in sequence.
    //
    // This is what bounds memory. Only a group's blocks need to be resident at
    // once -- never the whole series -- and a series with no overlap at all
    // degenerates to groups of one, i.e. exactly one decoded block at a time.
    //
    // Note this is deliberately NOT the `segments` split computed earlier: that
    // marks runs by whether CONSECUTIVE blocks overlap, so a pair straddling a
    // boundary (last block of one run overlapping the first of the next) would
    // land in different runs and never be merged against each other -- silently
    // dropping last-write-wins between them.
    size_t groupStart = 0;
    while (groupStart < blockMeta.size()) {
        uint64_t groupMaxEnd = blockMeta[groupStart].maxTime;
        size_t groupEnd = groupStart + 1;
        while (groupEnd < blockMeta.size() && blockMeta[groupEnd].minTime <= groupMaxEnd) {
            groupMaxEnd = std::max(groupMaxEnd, blockMeta[groupEnd].maxTime);
            ++groupEnd;
        }

        // Bulk (in-memory) merge admission: the group must be small in BLOCKS
        // (per-series bound) AND fit a reservation in the SHARD-WIDE decode
        // budget (concurrency bound — up to seriesBatchSize() x 4 pipelines
        // merge at once, so per-series caps alone multiply into shard-killing
        // totals on a small host). try_wait, never wait: a group that cannot
        // reserve takes the lazy cursor merge, so this cannot deadlock.
        uint64_t bulkReserveKiB = 0;
        bool bulkReserved = false;
        if (groupEnd - groupStart > 1 && groupEnd - groupStart <= MAX_BUFFERED_GROUP_BLOCKS) {
            uint64_t groupCompressedBytes = 0;
            for (size_t i = groupStart; i < groupEnd; ++i) {
                groupCompressedBytes += blockMeta[i].indexBlock.size;
            }
            bulkReserveKiB = std::max<uint64_t>(1, groupCompressedBytes * decodedExpansionEstimate<T>() / 1024);
            if (bulkReserveKiB <= mergeDecodeBudget_.current() && mergeDecodeBudget_.try_wait(bulkReserveKiB)) {
                bulkReserved = true;
            }
        }
        auto releaseBulkReserve = seastar::defer([this, &bulkReserved, &bulkReserveKiB] {
            if (bulkReserved) {
                mergeDecodeBudget_.signal(bulkReserveKiB);
            }
        });

        if (groupEnd - groupStart == 1) {
            // Lone block: no merge needed, decode and stream it.
            auto block = co_await decodeBlockOf(blockMeta[groupStart]);
            if (block && !block->timestamps.empty()) {
                processBlock(block.get());
            }
        } else if (!bulkReserved) {
            // LAZY MERGE for groups that may not be decoded wholesale — either
            // oversized (N files spanning the same time range chain into ONE
            // group that can be the entire series; decoding it is the original
            // compaction bad_alloc for backfill/rewrite inputs) or denied a
            // shard-wide decode-budget reservation (too many concurrent bulk
            // merges for this host's memory). Merge over per-file BLOCK
            // CURSORS: blockMeta is time-sorted and each file's blocks are
            // internally disjoint and ascending, so each file is a sorted
            // stream and only its CURRENT block needs to be decoded. This
            // reproduces the bulk mergers' semantics exactly — one point per
            // distinct timestamp, the newest file (highest rank) wins
            // cross-file duplicates, the last copy wins within a file — while
            // holding O(source files) decoded blocks instead of O(group).
            struct LazyCursor {
                std::vector<const BlockMetadata<T>*> metas;  // this file's blocks, ascending
                size_t metaIdx = 0;
                std::unique_ptr<TSMBlock<T>> block;  // current decoded block (null = exhausted)
                size_t pointIdx = 0;
                uint64_t fileRank = 0;
                uint64_t currentTs() const { return block->timestamps[pointIdx]; }
            };
            std::vector<LazyCursor> cursors;
            for (size_t i = groupStart; i < groupEnd; ++i) {
                const auto& meta = blockMeta[i];
                auto it = std::find_if(cursors.begin(), cursors.end(), [&](const LazyCursor& c) {
                    return c.metas.front()->sourceFile == meta.sourceFile;
                });
                if (it == cursors.end()) {
                    cursors.emplace_back();
                    it = cursors.end() - 1;
                    it->fileRank = meta.fileRank;
                }
                it->metas.push_back(&meta);
            }

            // Decode the next non-empty block when the current one is used up.
            auto refill = [&](LazyCursor& c) -> seastar::future<> {
                while (!c.block && c.metaIdx < c.metas.size()) {
                    auto b = co_await decodeBlockOf(*c.metas[c.metaIdx++]);
                    if (b && !b->timestamps.empty()) {
                        c.block = std::move(b);
                        c.pointIdx = 0;
                    }
                }
            };
            // Advance one point WITHOUT suspending; returns true when the
            // cursor crossed a block boundary and needs an (async) refill.
            // The merge loop advances per point, and a coroutine call per
            // point allocates a frame each time — measured ~15x slower than
            // the bulk mergers. A suspension is only genuinely needed once
            // per ~MaxPointsPerBlock points per file.
            auto advanceSync = [](LazyCursor& c) -> bool {
                if (c.block && ++c.pointIdx >= c.block->timestamps.size()) {
                    c.block.reset();
                    c.pointIdx = 0;
                    return true;  // block exhausted — refill required
                }
                return false;
            };

            for (auto& c : cursors) {
                co_await refill(c);
            }

            while (true) {
                // Winner: smallest current timestamp; newest file on a tie.
                uint64_t minTs = std::numeric_limits<uint64_t>::max();
                LazyCursor* winner = nullptr;
                for (auto& c : cursors) {
                    if (!c.block) {
                        continue;
                    }
                    const uint64_t ts = c.currentTs();
                    if (winner == nullptr || ts < minTs || (ts == minTs && c.fileRank > winner->fileRank)) {
                        minTs = ts;
                        winner = &c;
                    }
                }
                if (winner == nullptr) {
                    break;
                }

                // Last copy at minTs within the winning file (legacy intra-file
                // duplicates), mirroring BulkMergeContext::takeLastAtCurrentTs.
                T value = winner->block->values[winner->pointIdx];
                if (advanceSync(*winner)) {
                    co_await refill(*winner);
                }
                while (winner->block && winner->currentTs() == minTs) {
                    value = winner->block->values[winner->pointIdx];
                    if (advanceSync(*winner)) {
                        co_await refill(*winner);
                    }
                }
                // Losing sources' duplicates at minTs are consumed (LWW).
                for (auto& c : cursors) {
                    if (&c == winner) {
                        continue;
                    }
                    while (c.block && c.currentTs() == minTs) {
                        if (advanceSync(c)) {
                            co_await refill(c);
                        }
                    }
                }

                processPoint(minTs, value);

                // Spill INSIDE the merge: for this path the group itself is
                // huge, so waiting for the group boundary would buffer the
                // whole merged output. spill() retains the final point, so
                // last-write-wins survives the chunk seam.
                if (chunkedEmit && bufferedSinceSpill >= MERGE_CHUNK_POINTS) {
                    co_await spill();
                }
            }
        } else {
            // Overlapping run: decode just this group and merge it. Blocks are
            // grouped by source file because the mergers dedup by file rank
            // (newest file wins a duplicate timestamp).
            std::vector<SeriesBlocks<T>> groupSources;
            for (size_t i = groupStart; i < groupEnd; ++i) {
                const auto& meta = blockMeta[i];
                auto block = co_await decodeBlockOf(meta);
                if (!block || block->timestamps.empty()) {
                    continue;
                }

                auto it = std::find_if(groupSources.begin(), groupSources.end(),
                                       [&](const SeriesBlocks<T>& sb) { return sb.source == meta.sourceFile; });
                if (it == groupSources.end()) {
                    SeriesBlocks<T> sb(seriesId);
                    sb.source = meta.sourceFile;
                    sb.fileRank = meta.fileRank;
                    groupSources.push_back(std::move(sb));
                    it = groupSources.end() - 1;
                }
                // blockMeta is time-sorted, so each file's blocks are appended in
                // ascending order -- which is what the merge cursors assume.
                it->totalPoints += block->timestamps.size();
                it->blockIndex.push_back(meta.indexBlock);
                it->blocks.push_back(std::move(block));
            }

            if (groupSources.size() == 1) {
                for (const auto& block : groupSources[0].blocks) {
                    processBlock(block.get());
                }
            } else if (groupSources.size() == 2) {
                BulkMerger2Way<T> merger(groupSources);
                while (merger.hasNext()) {
                    auto [ts, val] = merger.next();
                    processPoint(ts, val);
                }
            } else if (groupSources.size() == 3) {
                BulkMerger3Way<T> merger(groupSources);
                while (merger.hasNext()) {
                    auto [ts, val] = merger.next();
                    processPoint(ts, val);
                }
            } else if (groupSources.size() > 3) {
                BulkMerger<T> merger(groupSources);
                while (merger.hasNext()) {
                    auto [ts, val] = merger.next();
                    processPoint(ts, val);
                }
            }
            // groupSources (and its decoded blocks) are released here.
        }

        // Group boundary: a safe suspension point to hand off a chunk.
        if (chunkedEmit && bufferedSinceSpill >= MERGE_CHUNK_POINTS) {
            co_await spill();
        }

        groupStart = groupEnd;
    }

    if (ttlFiltered > 0) {
        timestar::compactor_log.info("TTL: Filtered {} expired points for series {}", ttlFiltered, seriesId.toHex());
    }

    // Streaming downsample completion: the series is finished, so the
    // retained last point can be folded too (no more duplicates can arrive).
    // Buckets are emitted BEFORE the recent tail below, keeping order.
    if (streamingDownsample && !dsFlushed) {
        const size_t partIdx = static_cast<size_t>(
            std::lower_bound(result.timestamps.begin(), result.timestamps.end(), downsampleThreshold) -
            result.timestamps.begin());
        if (partIdx > 0) {
            foldOldPrefixIntoBuckets(partIdx);
        }
        co_await flushDsBuckets();
    }

    // Hand off whatever is left. Once anything has been emitted through the sink
    // the remainder must go the same way, otherwise the tail would be dropped
    // (writeSeriesCompactionData skips a series flagged emittedViaSink).
    if (chunkedEmit && result.emittedViaSink && !result.timestamps.empty()) {
        auto tailTs = std::move(result.timestamps);
        auto tailVals = std::move(result.values);
        result.timestamps.clear();
        result.values.clear();
        co_await sink(std::move(tailTs), std::move(tailVals));
    }

    // Phase 4: Downsampling for series processed WITHOUT a sink (direct
    // callers/tests) — sink-driven series were folded incrementally above.
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, int64_t>) {
        if (!streamingDownsample && downsampleThreshold > 0 && downsampleInterval > 0 && !result.timestamps.empty()) {
            // Find partition point: first timestamp >= threshold
            auto partIt = std::lower_bound(result.timestamps.begin(), result.timestamps.end(), downsampleThreshold);
            size_t partIdx = static_cast<size_t>(partIt - result.timestamps.begin());

            if (partIdx > 0) {
                // Old segment: timestamps[0..partIdx) — downsample these
                std::vector<uint64_t> dsTimestamps;
                std::vector<T> dsValues;

                // Bucket and aggregate
                std::map<uint64_t, timestar::AggregationState> buckets;
                for (size_t i = 0; i < partIdx; ++i) {
                    uint64_t bucket = (result.timestamps[i] / downsampleInterval) * downsampleInterval;
                    double val;
                    if constexpr (std::is_same_v<T, double>) {
                        val = result.values[i];
                    } else {
                        val = static_cast<double>(result.values[i]);
                    }
                    buckets[bucket].addValue(val, result.timestamps[i]);
                }

                dsTimestamps.reserve(buckets.size());
                dsValues.reserve(buckets.size());
                for (auto& [bucket, state] : buckets) {
                    dsTimestamps.push_back(bucket);
                    double aggVal = state.getValue(downsampleMethod);
                    if constexpr (std::is_same_v<T, double>) {
                        dsValues.push_back(aggVal);
                    } else {
                        dsValues.push_back(static_cast<int64_t>(aggVal));
                    }
                }

                // Recent segment: timestamps[partIdx..end) — keep at full resolution
                size_t recentCount = result.timestamps.size() - partIdx;
                dsTimestamps.reserve(dsTimestamps.size() + recentCount);
                dsValues.reserve(dsValues.size() + recentCount);
                for (size_t i = partIdx; i < result.timestamps.size(); ++i) {
                    dsTimestamps.push_back(result.timestamps[i]);
                    dsValues.push_back(result.values[i]);
                }

                size_t originalCount = result.timestamps.size();
                result.timestamps = std::move(dsTimestamps);
                result.values = std::move(dsValues);
                result.pointsWritten = result.timestamps.size();

                timestar::compactor_log.info(
                    "Downsample: {} -> {} points for series {} ({} old -> {} buckets, {} recent kept)", originalCount,
                    result.timestamps.size(), seriesId.toHex(), partIdx, buckets.size(), recentCount);
            }
        }
    }

    co_return result;
}

// Phase 3: Write pre-processed series data to TSMWriter.
//
// Drains the writer's buffer to disk as it goes: a single high-volume series can
// contribute hundreds of MB, and without an intra-series flush point the whole
// output file accumulates in one contiguous allocation.
template <typename T>
seastar::future<> TSMCompactor::writeSeriesCompactionData(TSMWriter& writer, SeriesCompactionData<T> data,
                                                          CompactionStats& stats) {
    if (data.isZeroCopy) {
        // Zero-copy path: read each source block, hand it to the writer, drop it.
        // Only one compressed block is resident at a time, so peak memory here is
        // O(block size) rather than O(series size).
        //
        // The dictionary must be attached BEFORE any block is written, so a flush
        // mid-series cannot land STR2 blocks in the file with no dictionary
        // recorded for them yet.
        if (data.stringDictionary && !data.blockRefs.empty()) {
            writer.setSeriesStringDictionary(data.seriesId, std::move(data.stringDictionary));
        }
        // Read in bounded batches rather than one block at a time. This loop runs
        // holding the single write semaphore, so a strictly sequential
        // read-then-write would serialise every source read in the whole
        // compaction behind the writer -- the reads were previously issued in
        // parallel. A batch keeps the I/O pipeline full while capping resident
        // block bytes at BLOCK_READ_BATCH blocks (~4 KB each in practice).
        for (size_t i = 0; i < data.blockRefs.size(); i += BLOCK_READ_BATCH) {
            const size_t batchEnd = std::min(i + BLOCK_READ_BATCH, data.blockRefs.size());

            std::vector<seastar::future<seastar::temporary_buffer<uint8_t>>> reads;
            reads.reserve(batchEnd - i);
            for (size_t j = i; j < batchEnd; ++j) {
                reads.push_back(data.blockRefs[j].sourceFile->readCompressedBlock(data.blockRefs[j].indexBlock));
            }
            auto blocks = co_await seastar::when_all_succeed(reads.begin(), reads.end());

            for (size_t j = i; j < batchEnd; ++j) {
                auto& ref = data.blockRefs[j];
                TSMIndexBlock srcBlock = ref.indexBlock;
                srcBlock.minTime = ref.minTime;
                srcBlock.maxTime = ref.maxTime;
                writer.writeCompressedBlockWithStats(data.seriesType, data.seriesId, std::move(blocks[j - i]),
                                                     srcBlock);

                // Block boundary: safe to drain (no back-patch is outstanding).
                co_await writer.flushIfNeeded();
            }
        }
    } else {
        // Slow path: write decompressed data.
        // writeSeriesStreaming, NOT writeSeries: the latter emits every block of
        // the series before returning, so the buffer could only be drained after
        // the whole series was in memory -- which for one large series is the
        // very allocation this path exists to avoid.
        if (data.emittedViaSink) {
            // Already written incrementally through the chunk sink; the vectors
            // hold nothing that has not been emitted.
        } else if (!data.timestamps.empty()) {
            co_await writer.writeSeriesStreaming(data.seriesType, data.seriesId, data.timestamps, data.values);
        }
    }

    // Update stats
    stats.pointsRead += data.pointsRead;
    stats.pointsWritten += data.pointsWritten;
    stats.duplicatesRemoved += data.duplicatesRemoved;
}

seastar::future<CompactionResult> TSMCompactor::compact(
    const std::vector<seastar::shared_ptr<TSM>>& files, uint64_t targetTier, uint64_t targetSeq,
    const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies,
    const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap) {
    if (files.empty()) {
        co_return CompactionResult{};
    }

    // When called without a pre-allocated plan (targetSeq == 0), compute the
    // target tier from the input files and allocate a fresh sequence ID.
    // This preserves backward compatibility for direct callers (e.g. tests).
    if (targetSeq == 0) {
        uint64_t maxTier = 0;
        for (const auto& file : files) {
            maxTier = std::max(maxTier, file->tierNum);
        }
        targetTier = strategy->getTargetTier(maxTier, files.size());
        targetSeq = fileManager->allocateSequenceId();
    }

    std::string outputPath = generateCompactedFilename(targetTier, targetSeq, maxDataSeqOf(files));

    // Create temporary file for writing
    std::string tempPath = outputPath + ".tmp";
    // The writer is declared OUTSIDE the try so the failure path can still close
    // its streaming file handle; it now owns an open fd for the duration of the
    // compaction rather than opening one only at close time.
    TSMWriter writer(tempPath);
    std::exception_ptr compactionError;
    CompactionResult compactionResult;
    try {
        // Use higher zstd compression for deeper tiers (better ratio, acceptable speed)
        if (targetTier >= 1) {
            writer.setCompressionLevel(3);
        }
        // Do NOT pre-reserve the output buffer to the summed input size. The
        // writer streams to disk (writeSeriesCompactionData drains it at block
        // boundaries), so it only ever holds a bounded window; reserving the
        // whole output up front would reinstate exactly the single large
        // contiguous allocation this path exists to avoid.

        CompactionStats stats;
        stats.filesCompacted = files.size();
        auto startTime = std::chrono::steady_clock::now();

        // Get all unique series across files
        auto allSeries = getAllSeriesIds(files);

        // Build per-series retention context from policies + metadata map (local to this compaction)
        SeriesRetentionMap seriesRetention;
        if (!retentionPolicies.empty() && !seriesMeasurementMap.empty()) {
            uint64_t now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();

            for (const auto& sid : allSeries) {
                auto measIt = seriesMeasurementMap.find(sid);
                if (measIt == seriesMeasurementMap.end())
                    continue;

                auto policyIt = retentionPolicies.find(measIt->second);
                if (policyIt == retentionPolicies.end())
                    continue;

                const auto& policy = policyIt->second;
                SeriesRetentionContext ctx;

                if (policy.ttlNanos > 0 && now > policy.ttlNanos) {
                    ctx.ttlCutoff = now - policy.ttlNanos;
                }

                if (policy.downsample.has_value() && policy.downsample->afterNanos > 0 &&
                    now > policy.downsample->afterNanos) {
                    ctx.downsampleThreshold = now - policy.downsample->afterNanos;
                    ctx.downsampleInterval = policy.downsample->intervalNanos;

                    const auto& method = policy.downsample->method;
                    if (method == "min")
                        ctx.downsampleMethod = timestar::AggregationMethod::MIN;
                    else if (method == "max")
                        ctx.downsampleMethod = timestar::AggregationMethod::MAX;
                    else if (method == "sum")
                        ctx.downsampleMethod = timestar::AggregationMethod::SUM;
                    else if (method == "latest")
                        ctx.downsampleMethod = timestar::AggregationMethod::LATEST;
                    else
                        ctx.downsampleMethod = timestar::AggregationMethod::AVG;
                }

                if (ctx.ttlCutoff > 0 || ctx.downsampleThreshold > 0) {
                    seriesRetention[sid] = ctx;
                }
            }

            if (!seriesRetention.empty()) {
                timestar::compactor_log.info("Compaction: {} series have retention policies applied",
                                             seriesRetention.size());
            }
        }

        // Phase 2.2: Group series by type for pipelined processing
        std::vector<SeriesId128> floatSeries;
        std::vector<SeriesId128> boolSeries;
        std::vector<SeriesId128> stringSeries;
        std::vector<SeriesId128> integerSeries;

        for (const auto& seriesId : allSeries) {
            // Check ALL files for series type, not just the first file.
            // A series may only exist in files[1..N], so we must iterate
            // until we find a file that contains this series.
            //
            // SeriesId128 is derived from measurement+tags+field only -- the value
            // type is not part of the key -- and the memstore's type-conflict
            // check is scoped to a single LIVE store. So once a store flushes,
            // the same field can be re-written with a different type, leaving
            // file A holding Boolean blocks and file B holding Float blocks for
            // one series id. Taking the type from the first file that has the
            // series and decoding every other file as that type is what produced
            // "TSM block type mismatch: block contains type 1 but reader expects
            // type 0", failing the whole compaction repeatedly and wedging the
            // tier. Detect the divergence here and skip just that series, so one
            // bad series cannot block compaction of everything else.
            std::optional<TSMValueType> seriesType;
            bool typeDiverges = false;
            for (const auto& file : files) {
                auto fileType = file->getSeriesType(seriesId);
                if (!fileType.has_value()) {
                    continue;
                }
                if (!seriesType.has_value()) {
                    seriesType = fileType;
                } else if (*fileType != *seriesType) {
                    typeDiverges = true;
                    break;
                }
            }
            if (typeDiverges) {
                // Fail the whole compaction rather than skipping the series.
                // Skipping would be silent DATA LOSS: a successful compaction
                // deletes its source files (executeCompaction -> removeTSMFiles),
                // so any series omitted from the output is gone. The output
                // format stores exactly one value type per series per file, so
                // there is no way to carry both types through either.
                //
                // Failing keeps every input file intact and readable. The cost is
                // that the tier cannot compact until the conflict is resolved --
                // but that is now an explicit, named condition with a backoff,
                // rather than an opaque decoder error retried forever.
                throw std::runtime_error(
                    "Series " + seriesId.toHex() +
                    " has conflicting value types across input TSM files; refusing to compact (compacting would "
                    "have to drop one type). Source files are left untouched.");
            }
            if (!seriesType.has_value()) {
                continue;
            }

            if (seriesType.value() == TSMValueType::Float) {
                floatSeries.push_back(seriesId);
            } else if (seriesType.value() == TSMValueType::Boolean) {
                boolSeries.push_back(seriesId);
            } else if (seriesType.value() == TSMValueType::String) {
                stringSeries.push_back(seriesId);
            } else if (seriesType.value() == TSMValueType::Integer) {
                integerSeries.push_back(seriesId);
            }
        }

        // The id list is fully partitioned into the four type vectors; free
        // the combined copy before processing starts. At high cardinality the
        // duplicate list is 16 B x series held for the whole compaction.
        allSeries.clear();
        allSeries.shrink_to_fit();

        // Phase 3: Parallel series processing with batching.
        // Memory-adaptive: see seriesBatchSize() — benchmark-tuned 20 on big
        // hosts, scaled down on small-memory shards where 80 concurrent
        // series' chunk buffers alone would dominate the arena.
        const size_t SERIES_BATCH_SIZE = seriesBatchSize();

        // Semaphore to serialize writes (TSMWriter not thread-safe)
        seastar::semaphore writeSemaphore{1};

        // Helper lambda for parallel batch processing
        auto processBatch = [&](const auto& seriesVec, auto* typePtr) -> seastar::future<> {
            using ValueType = std::remove_pointer_t<decltype(typePtr)>;

            for (size_t i = 0; i < seriesVec.size(); i += SERIES_BATCH_SIZE) {
                size_t batchEnd = std::min(i + SERIES_BATCH_SIZE, seriesVec.size());

                // Start all series in batch in parallel
                std::vector<seastar::future<>> processFutures;
                processFutures.reserve(batchEnd - i);

                for (size_t j = i; j < batchEnd; j++) {
                    const auto& seriesId = seriesVec[j];

                    // Process series and write with serialization
                    // Sink: emit merged points in bounded chunks straight into the
                    // writer instead of letting one series accumulate whole. Takes
                    // the write semaphore per chunk, so chunks from different
                    // series interleave in the file -- which is fine, since the
                    // index records each block's absolute offset.
                    PointChunkSink<ValueType> chunkSink = [this, &writer, &writeSemaphore, seriesId](
                                                              std::vector<uint64_t>&& ts,
                                                              std::vector<ValueType>&& vals) -> seastar::future<> {
                        return seastar::with_semaphore(
                            writeSemaphore, 1,
                            [this, &writer, seriesId, ts = std::move(ts), vals = std::move(vals)]() mutable {
                                return writer.appendSeriesChunk(TSM::getValueType<ValueType>(), seriesId, std::move(ts),
                                                                std::move(vals));
                            });
                    };

                    auto future =
                        processSeriesForCompaction<ValueType>(seriesId, files, seriesRetention, std::move(chunkSink))
                            .then([this, &writer, &writeSemaphore, &stats](SeriesCompactionData<ValueType> data) {
                                // Serialize writes with semaphore. The write path is
                                // now a coroutine (it drains the output buffer to
                                // disk), so the semaphore is held across those
                                // suspensions -- which is exactly what keeps the
                                // single-writer invariant intact.
                                return seastar::with_semaphore(
                                    writeSemaphore, 1, [this, &writer, &stats, data = std::move(data)]() mutable {
                                        return writeSeriesCompactionData(writer, std::move(data), stats);
                                    });
                            });

                    processFutures.push_back(std::move(future));
                }

                // Wait for batch to complete
                co_await seastar::when_all_succeed(processFutures.begin(), processFutures.end());
            }
        };

        // Process all series types concurrently (writes serialized by writeSemaphore)
        std::vector<seastar::future<>> typeFutures;
        typeFutures.reserve(4);
        if (!floatSeries.empty())
            typeFutures.push_back(processBatch(floatSeries, (double*)nullptr));
        if (!boolSeries.empty())
            typeFutures.push_back(processBatch(boolSeries, (bool*)nullptr));
        if (!stringSeries.empty())
            typeFutures.push_back(processBatch(stringSeries, (std::string*)nullptr));
        if (!integerSeries.empty())
            typeFutures.push_back(processBatch(integerSeries, (int64_t*)nullptr));
        co_await seastar::when_all_succeed(typeFutures.begin(), typeFutures.end());

        // Finalize the file
        co_await writer.writeIndexStreaming();
        co_await writer.closeDMA();

        // Calculate statistics
        auto endTime = std::chrono::steady_clock::now();
        stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

        // Atomic rename from temp to final (async to avoid blocking the reactor)
        co_await seastar::rename_file(tempPath, outputPath);

        // Fsync the parent directory to ensure the rename (directory entry update)
        // is durable.  Without this, a crash could lose the rename even though
        // the file data is already on disk.
        auto slash = outputPath.rfind('/');
        std::string dir = (slash != std::string::npos) ? outputPath.substr(0, slash) : ".";
        try {
            auto dirFile = co_await seastar::open_directory(dir);
            co_await dirFile.flush();
            co_await dirFile.close();
        } catch (...) {
            // Best-effort: log but don't fail the compaction
            timestar::compactor_log.warn("Failed to fsync parent directory after compaction rename: {}", dir);
        }

        compactionResult = CompactionResult{outputPath, stats};
    } catch (...) {
        compactionError = std::current_exception();
    }

    if (compactionError) {
        // Clean up outside the handler so the work can be async: GCC does not
        // allow co_await inside a catch block, which is why this used to be a
        // blocking std::filesystem::remove on the reactor thread.
        // Close the fd BEFORE unlinking so the partially-written temp file is
        // not left open.
        co_await writer.abortStream();
        try {
            co_await seastar::remove_file(tempPath);
        } catch (...) {
            // Best effort. A leftover .tmp is harmless: it fails the ".tsm"
            // suffix check on startup and is deleted by TSMFileManager::init().
        }
        std::rethrow_exception(compactionError);
    }

    co_return compactionResult;
}

CompactionPlan TSMCompactor::planCompaction(uint64_t tier) {
    CompactionPlan plan;
    plan.targetTier = tier;

    // Get files from file manager using new tier tracking, excluding any
    // that are already part of an in-flight compaction to prevent double-
    // compaction races (which can crash or corrupt output).
    std::vector<seastar::shared_ptr<TSM>> tierFiles = fileManager->getFilesInTier(tier);
    std::erase_if(tierFiles, [this](const seastar::shared_ptr<TSM>& f) { return isFileInActiveCompaction(f); });

    // Use strategy to select files
    plan.sourceFiles = strategy->selectFiles(tierFiles, tier);

    if (!plan.sourceFiles.empty()) {
        plan.targetTier = strategy->getTargetTier(tier, plan.sourceFiles.size());
        plan.targetSeqNum = fileManager->allocateSequenceId();
        plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum, maxDataSeqOf(plan.sourceFiles));

        // Estimate output size (rough estimate - 70% of input due to compression)
        plan.estimatedSize = 0;
        for (const auto& file : plan.sourceFiles) {
            plan.estimatedSize += file->getFileSize();
        }
        plan.estimatedSize = plan.estimatedSize * 0.7;
    }

    return plan;
}

seastar::future<CompactionStats> TSMCompactor::executeCompaction(const CompactionPlan& plan) {
    if (!plan.isValid()) {
        co_return CompactionStats{};
    }

    // Acquire semaphore to limit concurrent compactions
    auto units = co_await seastar::get_units(compactionSemaphore, 1);

    // Track this compaction
    ActiveCompaction active;
    active.plan = plan;
    active.startTime = std::chrono::steady_clock::now();
    activeCompactions.push_back(active);

    // Scope guard: always remove from active list, even on exception.
    // Without this, a failed compaction permanently poisons the active set,
    // preventing those files from ever being compacted again.
    auto removeActive = seastar::defer([this, &plan] {
        std::erase_if(activeCompactions,
                      [&](const ActiveCompaction& a) { return a.plan.targetPath == plan.targetPath; });
    });

    // Perform compaction (passing pending retention context if any).
    // Use the plan's pre-allocated targetTier and targetSeqNum so the output
    // file path matches plan.targetPath (avoids wasting sequence IDs).
    // Move policies into locals so they survive across multiple executeCompaction calls
    // (e.g., forceFullCompaction calls executeCompaction per tier).
    auto localRetention = std::exchange(_pendingRetentionPolicies, {});
    auto localSeriesMap = std::exchange(_pendingSeriesMeasurementMap, {});
    auto compactionResult =
        co_await compact(plan.sourceFiles, plan.targetTier, plan.targetSeqNum, localRetention, localSeriesMap);

    if (!compactionResult.outputPath.empty()) {
        // Open the new file
        auto newTSM = seastar::make_shared<TSM>(compactionResult.outputPath);
        co_await newTSM->open();

        // Add to file manager
        co_await fileManager->addTSMFile(newTSM);

        // Remove old files from manager and mark for deletion
        // (removeTSMFiles also deletes associated tombstone files)
        co_await fileManager->removeTSMFiles(plan.sourceFiles);
    }

    // Propagate stats from the compact() call and add timing.
    // Update the vector element (not the local copy) so getActiveCompactionStats() sees real stats.
    CompactionStats finalStats = compactionResult.stats;
    auto endTime = std::chrono::steady_clock::now();
    finalStats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - active.startTime);

    for (auto& ac : activeCompactions) {
        if (ac.plan.targetPath == plan.targetPath) {
            ac.stats = finalStats;
            break;
        }
    }

    // removeActive fires here via scope guard
    co_return finalStats;
}

bool TSMCompactor::shouldCompact(uint64_t tier) const {
    // Use the file manager's method to check
    return fileManager->shouldCompactTier(tier);
}

seastar::future<> TSMCompactor::runCompactionLoop() {
    while (compactionEnabled) {
        bool didCompaction = false;

        // Check each tier for compaction
        for (uint64_t tier = 0; tier < 4; tier++) {
            if (shouldCompact(tier)) {
                auto plan = planCompaction(tier);
                if (plan.isValid()) {
                    try {
                        auto stats = co_await executeCompaction(plan);
                        didCompaction = true;

                        timestar::compactor_log.info("Compacted {} files in tier {}, removed {} duplicates in {}ms",
                                                     stats.filesCompacted, tier, stats.duplicatesRemoved,
                                                     stats.duration.count());
                    } catch (const std::exception& e) {
                        timestar::compactor_log.error("Compaction failed for tier {}: {}. Will retry on next cycle.",
                                                      tier, e.what());
                    }
                }
            }
        }

        // Sleep before next check
        if (!didCompaction) {
            co_await seastar::sleep(std::chrono::seconds(30));
        } else {
            // Short sleep after compaction to allow system to stabilize
            co_await seastar::sleep(std::chrono::seconds(5));
        }
    }
}

seastar::future<> TSMCompactor::forceFullCompaction() {
    timestar::compactor_log.info("Starting forced full compaction...");

    // Compact each tier from bottom up
    for (uint64_t tier = 0; tier < 4; tier++) {
        // Get all files in this tier, excluding any in active compaction
        std::vector<seastar::shared_ptr<TSM>> tierFiles;
        for (const auto& [seq, file] : fileManager->getSequencedTsmFiles()) {
            if (file->tierNum == tier && !isFileInActiveCompaction(file)) {
                tierFiles.push_back(file);
            }
        }

        if (tierFiles.size() > 1) {
            timestar::compactor_log.info("Compacting {} files in tier {}", tierFiles.size(), tier);

            CompactionPlan plan;
            plan.sourceFiles = tierFiles;
            plan.targetTier = std::min(tier + 1, uint64_t{3});
            plan.targetSeqNum = fileManager->allocateSequenceId();
            plan.targetPath =
                generateCompactedFilename(plan.targetTier, plan.targetSeqNum, maxDataSeqOf(plan.sourceFiles));

            try {
                auto stats = co_await executeCompaction(plan);
                timestar::tsm_log.info("Compacted tier {}: {} files, {} points written, {} duplicates removed", tier,
                                       stats.filesCompacted, stats.pointsWritten, stats.duplicatesRemoved);
            } catch (const std::exception& e) {
                timestar::compactor_log.error("forceFullCompaction: tier {} failed: {}", tier, e.what());
            }
        }
    }

    timestar::tsm_log.info("Full compaction complete");
}

bool TSMCompactor::isFileInActiveCompaction(const seastar::shared_ptr<TSM>& file) const {
    for (const auto& active : activeCompactions) {
        for (const auto& src : active.plan.sourceFiles) {
            if (src.get() == file.get()) {
                return true;
            }
        }
    }
    return false;
}

seastar::future<CompactionStats> TSMCompactor::executeTombstoneRewrite(seastar::shared_ptr<TSM> file) {
    // Build a single-file compaction plan at the same tier
    CompactionPlan plan;
    plan.sourceFiles = {file};
    plan.targetTier = file->tierNum;
    plan.targetSeqNum = fileManager->allocateSequenceId();
    plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum, file->dataSeq);
    plan.estimatedSize = file->getFileSize();

    timestar::compactor_log.info("[TOMBSTONE-REWRITE] Rewriting {} at tier {} seq {} -> seq {}", file->getFileSize(),
                                 plan.targetTier, file->seqNum, plan.targetSeqNum);

    co_return co_await executeCompaction(plan);
}

std::vector<CompactionStats> TSMCompactor::getActiveCompactionStats() const {
    std::vector<CompactionStats> stats;
    for (const auto& active : activeCompactions) {
        stats.push_back(active.stats);
    }
    return stats;
}

// Phase 3: Parallel processing template instantiations
template seastar::future<SeriesCompactionData<double>> TSMCompactor::processSeriesForCompaction<double>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention, PointChunkSink<double> sink);

template seastar::future<SeriesCompactionData<bool>> TSMCompactor::processSeriesForCompaction<bool>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention, PointChunkSink<bool> sink);

template seastar::future<SeriesCompactionData<std::string>> TSMCompactor::processSeriesForCompaction<std::string>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention, PointChunkSink<std::string> sink);

template seastar::future<> TSMCompactor::writeSeriesCompactionData<double>(TSMWriter& writer,
                                                                           SeriesCompactionData<double> data,
                                                                           CompactionStats& stats);

template seastar::future<> TSMCompactor::writeSeriesCompactionData<bool>(TSMWriter& writer,
                                                                         SeriesCompactionData<bool> data,
                                                                         CompactionStats& stats);

template seastar::future<> TSMCompactor::writeSeriesCompactionData<std::string>(TSMWriter& writer,
                                                                                SeriesCompactionData<std::string> data,
                                                                                CompactionStats& stats);

template seastar::future<SeriesCompactionData<int64_t>> TSMCompactor::processSeriesForCompaction<int64_t>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention, PointChunkSink<int64_t> sink);

template seastar::future<> TSMCompactor::writeSeriesCompactionData<int64_t>(TSMWriter& writer,
                                                                            SeriesCompactionData<int64_t> data,
                                                                            CompactionStats& stats);