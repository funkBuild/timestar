#include "tsm_compactor.hpp"

#include "aggregator.hpp"           // AggregationState for downsampling
#include "bulk_block_loader.hpp"    // Phase A: Bulk loading optimization
#include "compaction_pipeline.hpp"  // Phase 2.1: Include prefetch manager
#include "logger.hpp"
#include "tsm_file_manager.hpp"
#include "tsm_merge_specialized.hpp"  // Phase 5.1: Specialized N-way merges
#include "tsm_writer.hpp"

#include <chrono>
#include <cinttypes>
#include <filesystem>
#include <limits>
#include <seastar/core/reactor.hh>
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

std::string TSMCompactor::generateCompactedFilename(uint64_t tier, uint64_t seqNum) {
    int shardId = seastar::this_shard_id();
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "shard_%d/tsm/%" PRIu64 "_%" PRIu64 ".tsm", shardId, tier, seqNum);
    return std::string(buffer);
}

std::vector<SeriesId128> TSMCompactor::getAllSeriesIds(const std::vector<seastar::shared_ptr<TSM>>& files) {
    // Use unordered_set for O(1) average insert instead of O(log n) with std::set.
    // At 100K series this is ~6x faster (see B6 benchmark).
    std::unordered_set<SeriesId128, SeriesId128::Hash> uniqueIds;

    // Pre-size for expected cardinality to avoid rehashing
    size_t totalEstimate = 0;
    for (const auto& file : files) {
        totalEstimate += file->getSeriesIds().size();
    }
    uniqueIds.reserve(totalEstimate);

    for (const auto& file : files) {
        auto ids = file->getSeriesIds();
        for (const auto& id : ids) {
            uniqueIds.insert(id);
        }
    }

    return std::vector<SeriesId128>(uniqueIds.begin(), uniqueIds.end());
}

// Phase 3: Process series for compaction without writing (enables parallel processing)
template <typename T>
seastar::future<SeriesCompactionData<T>> TSMCompactor::processSeriesForCompaction(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention) {
    SeriesCompactionData<T> result(seriesId, TSM::getValueType<T>());

    // Load ALL blocks from ALL files
    auto allBlocks = co_await BulkBlockLoader<T>::loadFromFiles(sources, seriesId);

    if (allBlocks.empty()) {
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

    // Build metadata index from the actually-loaded blocks.
    // allBlocks may have fewer entries than sources (files where the series
    // has no data are skipped by BulkBlockLoader::loadFromFiles), so we must
    // use each entry's source pointer -- NOT index into sources[].
    std::vector<BlockMetadata<T>> blockMeta;

    for (size_t fileIdx = 0; fileIdx < allBlocks.size(); fileIdx++) {
        const auto& fileBlocks = allBlocks[fileIdx];
        auto indexBlocks = fileBlocks.source->getSeriesBlocks(seriesId);

        for (size_t blockIdx = 0; blockIdx < indexBlocks.size(); blockIdx++) {
            const auto& indexBlock = indexBlocks[blockIdx];

            BlockMetadata<T> meta;
            meta.minTime = indexBlock.minTime;
            meta.maxTime = indexBlock.maxTime;
            meta.fileIndex = fileIdx;
            meta.blockIndex = blockIdx;
            meta.blockPtr = (blockIdx < fileBlocks.blocks.size()) ? fileBlocks.blocks[blockIdx].get() : nullptr;
            meta.fileRank = fileBlocks.fileRank;
            meta.sourceFile = fileBlocks.source;
            meta.indexBlock = indexBlock;

            blockMeta.push_back(meta);
        }
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

    if (allBlocksNonOverlapping && !blockMeta.empty() && tombstoneRanges.empty() && !hasPerPointRetention) {
        // ZERO-COPY PATH: Load compressed blocks in parallel
        result.isZeroCopy = true;
        result.compressedBlocks.reserve(blockMeta.size());

        std::vector<seastar::future<seastar::temporary_buffer<uint8_t>>> readFutures;
        readFutures.reserve(blockMeta.size());

        for (const auto& meta : blockMeta) {
            readFutures.push_back(meta.sourceFile->readCompressedBlock(meta.indexBlock));
        }

        auto compressedData = co_await seastar::when_all_succeed(readFutures.begin(), readFutures.end());

        for (size_t i = 0; i < blockMeta.size(); i++) {
            typename SeriesCompactionData<T>::CompressedBlock block;
            block.data = std::move(compressedData[i]);
            block.minTime = blockMeta[i].minTime;
            block.maxTime = blockMeta[i].maxTime;
            block.blockSum = blockMeta[i].indexBlock.blockSum;
            block.blockMin = blockMeta[i].indexBlock.blockMin;
            block.blockMax = blockMeta[i].indexBlock.blockMax;
            block.blockCount = blockMeta[i].indexBlock.blockCount;
            block.blockM2 = blockMeta[i].indexBlock.blockM2;
            block.blockFirstValue = blockMeta[i].indexBlock.blockFirstValue;
            block.blockLatestValue = blockMeta[i].indexBlock.blockLatestValue;
            block.hasExtendedStats = blockMeta[i].indexBlock.hasExtendedStats;
            result.compressedBlocks.push_back(std::move(block));
        }

        co_return result;
    }

    // SLOW PATH: Decompress and merge (overlap, tombstones, or retention filtering)
    result.isZeroCopy = false;
    result.timestamps.reserve(batchSize() * 10);  // Pre-allocate
    result.values.reserve(batchSize() * 10);

    uint64_t lastTimestamp = std::numeric_limits<uint64_t>::max();
    size_t tombstonesFiltered = 0;
    size_t ttlFiltered = 0;

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
                if (ts >= startTime && ts <= endTime) {
                    isTombstoned = true;
                    tombstonesFiltered++;
                }
            }
        }

        if (isTombstoned) {
            return;
        }

        // Deduplicate
        if (ts != lastTimestamp) {
            result.timestamps.push_back(ts);
            result.values.push_back(val);
            lastTimestamp = ts;
            result.pointsWritten++;
        } else {
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

    // Process based on overlap
    if (allBlocksNonOverlapping) {
        for (const auto& meta : blockMeta) {
            processBlock(meta.blockPtr);
        }
    } else {
        // Slow path: full merge
        size_t fileCount = allBlocks.size();
        if (fileCount == 2) {
            BulkMerger2Way<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                processPoint(ts, val);
            }
        } else if (fileCount == 3) {
            BulkMerger3Way<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                processPoint(ts, val);
            }
        } else {
            BulkMerger<T> merger(allBlocks);
            while (merger.hasNext()) {
                auto [ts, val] = merger.next();
                processPoint(ts, val);
            }
        }
    }

    if (ttlFiltered > 0) {
        timestar::compactor_log.info("TTL: Filtered {} expired points for series {}", ttlFiltered, seriesId.toHex());
    }

    // Phase 4: Downsampling — only for numeric types (double and int64_t)
    if constexpr (std::is_same_v<T, double> || std::is_same_v<T, int64_t>) {
        if (downsampleThreshold > 0 && downsampleInterval > 0 && !result.timestamps.empty()) {
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

// Phase 3: Write pre-processed series data to TSMWriter
template <typename T>
void TSMCompactor::writeSeriesCompactionData(TSMWriter& writer, SeriesCompactionData<T>&& data,
                                             CompactionStats& stats) {
    if (data.isZeroCopy) {
        // Zero-copy path: write compressed blocks directly, carrying forward stats
        for (auto& block : data.compressedBlocks) {
            TSMIndexBlock srcBlock;
            srcBlock.minTime = block.minTime;
            srcBlock.maxTime = block.maxTime;
            srcBlock.blockSum = block.blockSum;
            srcBlock.blockMin = block.blockMin;
            srcBlock.blockMax = block.blockMax;
            srcBlock.blockCount = block.blockCount;
            srcBlock.blockM2 = block.blockM2;
            srcBlock.blockFirstValue = block.blockFirstValue;
            srcBlock.blockLatestValue = block.blockLatestValue;
            srcBlock.hasExtendedStats = block.hasExtendedStats;
            writer.writeCompressedBlockWithStats(data.seriesType, data.seriesId, std::move(block.data), srcBlock);
        }
    } else {
        // Slow path: write decompressed data
        if (!data.timestamps.empty()) {
            writer.writeSeries(data.seriesType, data.seriesId, data.timestamps, data.values);
        }
    }

    // Update stats
    stats.pointsRead += data.pointsRead;
    stats.pointsWritten += data.pointsWritten;
    stats.duplicatesRemoved += data.duplicatesRemoved;
}

seastar::future<CompactionResult> TSMCompactor::compact(
    const std::vector<seastar::shared_ptr<TSM>>& files,
    const std::unordered_map<std::string, RetentionPolicy>& retentionPolicies,
    const std::unordered_map<SeriesId128, std::string, SeriesId128::Hash>& seriesMeasurementMap) {
    if (files.empty()) {
        co_return CompactionResult{};
    }

    // Determine output tier and filename
    uint64_t maxTier = 0;
    uint64_t maxSeq = 0;
    for (const auto& file : files) {
        maxTier = std::max(maxTier, file->tierNum);
        maxSeq = std::max(maxSeq, file->seqNum);
    }

    uint64_t targetTier = strategy->getTargetTier(maxTier, files.size());
    uint64_t targetSeq = fileManager->allocateSequenceId();
    std::string outputPath = generateCompactedFilename(targetTier, targetSeq);

    // Create temporary file for writing
    std::string tempPath = outputPath + ".tmp";
    // Track whether we successfully renamed — if not, clean up the temp file.
    bool tempRenamed = false;
    // Scope guard: clean up the temp file on any exception before the rename succeeds.
    struct TempCleanup {
        const std::string& path;
        bool& renamed;
        ~TempCleanup() { if (!renamed) { try { fs::remove(path); } catch (...) {} } }
    } tempCleanup{tempPath, tempRenamed};
    TSMWriter writer(tempPath);
    // Use higher zstd compression for deeper tiers (better ratio, acceptable speed)
    if (targetTier >= 1) {
        writer.setCompressionLevel(3);
    }

    CompactionStats stats;
    stats.filesCompacted = files.size();
    auto startTime = std::chrono::steady_clock::now();

    // Get all unique series across files
    auto allSeries = getAllSeriesIds(files);

    // Build per-series retention context from policies + metadata map (local to this compaction)
    SeriesRetentionMap seriesRetention;
    if (!retentionPolicies.empty() && !seriesMeasurementMap.empty()) {
        uint64_t now =
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
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
        std::optional<TSMValueType> seriesType;
        for (const auto& file : files) {
            seriesType = file->getSeriesType(seriesId);
            if (seriesType.has_value()) {
                break;
            }
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

    // Phase 3: Parallel series processing with batching
    // Batch size tuned for performance vs memory tradeoff
    // Testing shows: batch_size=10 → 651ms, batch_size=20 → 541ms, batch_size=50 → 2533ms (regression)
    const size_t SERIES_BATCH_SIZE = 20;

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
                auto future = processSeriesForCompaction<ValueType>(seriesId, files, seriesRetention)
                                  .then([this, &writer, &writeSemaphore, &stats](SeriesCompactionData<ValueType> data) {
                                      // Serialize writes with semaphore
                                      return seastar::with_semaphore(
                                          writeSemaphore, 1, [this, &writer, &stats, data = std::move(data)]() mutable {
                                              writeSeriesCompactionData(writer, std::move(data), stats);
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
    writer.writeIndex();
    co_await writer.closeDMA();

    // Calculate statistics
    auto endTime = std::chrono::steady_clock::now();
    stats.filesCompacted = files.size();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Atomic rename from temp to final (async to avoid blocking the reactor)
    co_await seastar::rename_file(tempPath, outputPath);
    tempRenamed = true;  // Rename succeeded — don't delete the output file

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

    co_return CompactionResult{outputPath, stats};
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
        plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);

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

    // Perform compaction (passing pending retention context if any)
    auto compactionResult = co_await compact(plan.sourceFiles, _pendingRetentionPolicies, _pendingSeriesMeasurementMap);

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

    // Propagate stats from the compact() call and add timing
    active.stats = compactionResult.stats;
    auto endTime = std::chrono::steady_clock::now();
    active.stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - active.startTime);

    // removeActive fires here via scope guard
    co_return active.stats;
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
    for (uint64_t tier = 0; tier < 3; tier++) {
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
            plan.targetTier = tier + 1;
            plan.targetSeqNum = fileManager->allocateSequenceId();
            plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);

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
    plan.targetPath = generateCompactedFilename(plan.targetTier, plan.targetSeqNum);
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
    const SeriesRetentionMap& seriesRetention);

template seastar::future<SeriesCompactionData<bool>> TSMCompactor::processSeriesForCompaction<bool>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention);

template seastar::future<SeriesCompactionData<std::string>> TSMCompactor::processSeriesForCompaction<std::string>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention);

template void TSMCompactor::writeSeriesCompactionData<double>(TSMWriter& writer, SeriesCompactionData<double>&& data,
                                                              CompactionStats& stats);

template void TSMCompactor::writeSeriesCompactionData<bool>(TSMWriter& writer, SeriesCompactionData<bool>&& data,
                                                            CompactionStats& stats);

template void TSMCompactor::writeSeriesCompactionData<std::string>(TSMWriter& writer,
                                                                   SeriesCompactionData<std::string>&& data,
                                                                   CompactionStats& stats);

template seastar::future<SeriesCompactionData<int64_t>> TSMCompactor::processSeriesForCompaction<int64_t>(
    const SeriesId128& seriesId, const std::vector<seastar::shared_ptr<TSM>>& sources,
    const SeriesRetentionMap& seriesRetention);

template void TSMCompactor::writeSeriesCompactionData<int64_t>(TSMWriter& writer, SeriesCompactionData<int64_t>&& data,
                                                               CompactionStats& stats);

// TimeBasedCompactionStrategy implementation
bool TimeBasedCompactionStrategy::shouldCompact(uint64_t tier, size_t fileCount, size_t totalSize) const {
    // For time-based, compact if we have old files
    // This would need file creation time tracking
    return fileCount >= 2;  // Simple check for now
}

std::vector<seastar::shared_ptr<TSM>> TimeBasedCompactionStrategy::selectFiles(
    const std::vector<seastar::shared_ptr<TSM>>& availableFiles, uint64_t tier) const {
    // Select oldest files
    std::vector<seastar::shared_ptr<TSM>> selected = availableFiles;

    // Sort by sequence number (assuming older = lower seq)
    std::sort(selected.begin(), selected.end(), [](const auto& a, const auto& b) { return a->seqNum < b->seqNum; });

    // Take first half of files
    if (selected.size() > 2) {
        selected.resize(selected.size() / 2);
    }

    return selected;
}