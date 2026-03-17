#pragma once

#include "block_aggregator.hpp"
#include "bloom_filter.hpp"
#include "query_result.hpp"
#include "series_id.hpp"
#include "timestar_config.hpp"
#include "tsm_result.hpp"
#include "tsm_tombstone.hpp"

#include <tsl/robin_map.h>

#include <fstream>
#include <limits>
#include <list>
#include <memory>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// Forward declarations
class Slice;

enum class TSMValueType { Float = 0, Boolean, String, Integer };

struct TSMIndexBlock {
    uint64_t minTime;
    uint64_t maxTime;
    uint64_t offset;
    uint32_t size;
    // Block-level statistics (all types in V2; Float-only in V1)
    // For Float: native double values.
    // For Integer: int64 values stored as double (lossless up to 2^53).
    // For Boolean: blockSum = trueCount, blockMin/blockMax = 0.0/1.0 or unused.
    // For String: only blockCount is meaningful.
    double blockSum = 0.0;
    double blockMin = std::numeric_limits<double>::max();
    double blockMax = std::numeric_limits<double>::lowest();
    uint32_t blockCount = 0;  // 0 means stats not available
    // Extended statistics (Float/Integer in V2)
    double blockM2 = 0.0;           // Welford's M2 accumulator for STDDEV/STDVAR (Float only)
    double blockFirstValue = 0.0;   // Value at earliest timestamp (for FIRST)
    double blockLatestValue = 0.0;  // Value at latest timestamp (for LATEST)
    bool hasExtendedStats = false;  // true when first/latest are populated
    // Boolean-specific stats (V2)
    uint32_t boolTrueCount = 0;    // Number of true values in block
    bool boolFirstValue = false;   // Value at earliest timestamp
    bool boolLatestValue = false;  // Value at latest timestamp
};

// Batch of contiguous blocks for optimized I/O
struct BlockBatch {
    uint64_t startOffset;               // Offset of first block in batch
    uint64_t totalSize;                 // Sum of all block sizes (uint64_t to avoid overflow)
    std::vector<TSMIndexBlock> blocks;  // Blocks in this batch

    BlockBatch() : startOffset(0), totalSize(0) {}
};

// Sparse index entry for lazy loading
struct SparseIndexEntry {
    SeriesId128 seriesId;     // 16 bytes - for hash map key
    uint64_t fileOffset;      // 8 bytes - where to read in file
    uint32_t entrySize;       // 4 bytes - how much to read
    TSMValueType seriesType;  // series value type (captured during sparse index parse)
    // Per-series time bounds (parsed from first/last block during sparse index load).
    // Enables skipping entire files for time-filtered queries without loading the
    // full index entry — critical for narrow-range queries with many TSM files.
    uint64_t minTime = 0;
    uint64_t maxTime = 0;
    // Block-level stats cached from first/last block for zero-I/O LATEST/FIRST.
    // Populated during readSparseIndex() for Float and Integer series (V2).
    double firstValue = 0.0;   // blockFirstValue from the first block
    double latestValue = 0.0;  // blockLatestValue from the last block
    bool hasExtendedStats = false;
    // Boolean sparse stats for zero-I/O LATEST/FIRST
    bool boolFirstValue = false;
    bool boolLatestValue = false;
};

struct TSMIndexEntry {
    SeriesId128 seriesId;
    TSMValueType seriesType;
    std::vector<TSMIndexBlock> indexBlocks;
    // String dictionary (Phase 3): populated for dictionary-encoded String series.
    // When non-empty, string blocks use varint IDs referencing this dictionary.
    std::vector<std::string> stringDictionary;
};

// TSM file format version.
// V1: Float blocks have stats (80 bytes), non-Float blocks are base-only (28 bytes).
// V2: All types have block stats (Float=80, Integer=72, Boolean=40, String=32).
static constexpr uint8_t TSM_VERSION = 2;
static constexpr uint8_t TSM_VERSION_MIN = 1;  // oldest version we can read

// Per-type index block byte size for V2 files.
// V1 files: Float=80, all others=28 (no stats).
// V2 files: Float=80, Integer=72, Boolean=40, String=32.
inline size_t indexBlockBytesV2(TSMValueType type) {
    switch (type) {
        case TSMValueType::Float:
            return 80;  // 28 base + 52 stats
        case TSMValueType::Integer:
            return 72;  // 28 base + 4 count + 40 (sum/min/max/first/latest as int64)
        case TSMValueType::Boolean:
            return 40;  // 28 base + 4 count + 4 trueCount + 1 first + 1 latest + 2 pad
        case TSMValueType::String:
            return 32;  // 28 base + 4 count
        default:
            return 28;
    }
}

inline size_t indexBlockBytes(TSMValueType type, uint8_t version) {
    if (version < 2) {
        // V1: only Float has stats
        return (type == TSMValueType::Float) ? 80 : 28;
    }
    return indexBlockBytesV2(type);
}

class TSM {
private:
    std::string filePath;
    seastar::file tsmFile;
    uint64_t length = 0;
    uint8_t fileVersion = 1;

    // Lazy loading: sparse index + bloom filter for memory efficiency
    tsl::robin_map<SeriesId128, SparseIndexEntry, SeriesId128::Hash> sparseIndex;
    bloom_filter seriesBloomFilter;

    // Full index cache with byte-budgeted LRU eviction for hot series.
    // Budget limits total memory instead of entry count, preventing large series
    // (many blocks) from consuming disproportionate memory.
    using LRUList = std::list<std::pair<SeriesId128, TSMIndexEntry>>;
    mutable LRUList lruList;
    mutable std::unordered_map<SeriesId128, LRUList::iterator, SeriesId128::Hash> fullIndexCache;
    mutable size_t fullIndexCacheBytes = 0;

    static size_t estimateEntryBytes(const TSMIndexEntry& entry) {
        return sizeof(TSMIndexEntry) + entry.indexBlocks.size() * sizeof(TSMIndexBlock) +
               sizeof(std::pair<SeriesId128, TSMIndexEntry>);  // list node overhead
    }

    // Configuration for bloom filter and cache (read from TOML config)
    static double bloomFpr() { return timestar::config().storage.tsm_bloom_fpr; }
    // Byte budget per TSM file (default: 4096 entries * ~200 bytes ≈ 800KB)
    static size_t maxCacheBytes() { return timestar::config().storage.tsm_cache_entries * 200; }

    // Tombstone support
    std::unique_ptr<timestar::TSMTombstone> tombstones;

    // Helper to get tombstone file path
    std::string getTombstonePath() const;

public:
    uint64_t tierNum;
    uint64_t seqNum;

    TSM(std::string _absoluteFilePath);
    seastar::future<> open();
    seastar::future<> close();
    uint64_t rankAsInteger();

    // Schedule async deletion — closes file and removes from disk
    seastar::future<> scheduleDelete();

    // Lazy loading index methods
    seastar::future<> readSparseIndex();
    seastar::future<TSMIndexEntry*> getFullIndexEntry(const SeriesId128& seriesId);
    // Bulk prefetch: warm the full index cache for multiple series in parallel
    seastar::future<> prefetchFullIndexEntries(const std::vector<SeriesId128>& seriesIds);

    template <class T>
    seastar::future<> readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                 TSMResult<T>& results);
    template <class T>
    seastar::future<> readSeriesBatched(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                        TSMResult<T>& results);
    template <class T>
    seastar::future<> readBlockBatch(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                     TSMResult<T>& results);
    std::optional<TSMValueType> getSeriesType(const SeriesId128& seriesId);

    // Check if a series in this file could overlap a time range using sparse
    // index bounds (no I/O).  Returns false if the series is absent or its
    // time range is entirely outside [startTime, endTime].
    bool seriesMayOverlapTime(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) const {
        if (!seriesBloomFilter.contains(seriesId.getRawData()))
            return false;
        auto it = sparseIndex.find(seriesId);
        if (it == sparseIndex.end())
            return false;
        return it->second.minTime <= endTime && startTime <= it->second.maxTime;
    }

    // Sparse index time bound accessors (no I/O — used for LATEST/FIRST file ordering)
    uint64_t getSeriesMaxTime(const SeriesId128& seriesId) const {
        auto it = sparseIndex.find(seriesId);
        return (it != sparseIndex.end()) ? it->second.maxTime : 0;
    }
    uint64_t getSeriesMinTime(const SeriesId128& seriesId) const {
        auto it = sparseIndex.find(seriesId);
        return (it != sparseIndex.end()) ? it->second.minTime : std::numeric_limits<uint64_t>::max();
    }

    // Zero-I/O LATEST/FIRST from sparse index stats.
    // Returns the latest or first (timestamp, value) for a series without any disk reads.
    // Returns nullopt if series not found or stats unavailable.
    struct PointResult {
        uint64_t timestamp;
        double value;
    };
    std::optional<PointResult> getLatestFromSparse(const SeriesId128& seriesId) const {
        auto it = sparseIndex.find(seriesId);
        if (it == sparseIndex.end() || !it->second.hasExtendedStats)
            return std::nullopt;
        return PointResult{it->second.maxTime, it->second.latestValue};
    }
    std::optional<PointResult> getFirstFromSparse(const SeriesId128& seriesId) const {
        auto it = sparseIndex.find(seriesId);
        if (it == sparseIndex.end() || !it->second.hasExtendedStats)
            return std::nullopt;
        return PointResult{it->second.minTime, it->second.firstValue};
    }

    // Block batching utilities
    std::vector<BlockBatch> groupContiguousBlocks(const std::vector<TSMIndexBlock>& blocks) const;
    template <class T>
    std::unique_ptr<TSMBlock<T>> decodeBlock(Slice& blockSlice, uint32_t blockSize, uint64_t startTime,
                                             uint64_t endTime);

    // Phase 1.1: New methods for streaming block access
    // Get index blocks for a series without reading data (for lazy loading)
    std::vector<TSMIndexBlock> getSeriesBlocks(const SeriesId128& seriesId) const;

    // Read a single block and return it (for on-demand loading)
    template <class T>
    seastar::future<std::unique_ptr<TSMBlock<T>>> readSingleBlock(const TSMIndexBlock& indexBlock, uint64_t startTime,
                                                                  uint64_t endTime);

    // Phase 2: Read compressed block bytes directly (zero-copy transfer)
    seastar::future<seastar::temporary_buffer<uint8_t>> readCompressedBlock(const TSMIndexBlock& indexBlock);

    // Get all series IDs in this file (for compaction)
    std::vector<SeriesId128> getSeriesIds() const {
        std::vector<SeriesId128> ids;
        ids.reserve(sparseIndex.size());
        for (const auto& [id, entry] : sparseIndex) {
            ids.push_back(id);
        }
        return ids;
    }

    // Get file size for compaction planning
    uint64_t getFileSize() const { return length; }

    template <class T>
    static constexpr TSMValueType getValueType() {
        if constexpr (std::is_same_v<T, double>) {
            return TSMValueType::Float;
        } else if constexpr (std::is_same_v<T, bool>) {
            return TSMValueType::Boolean;
        } else if constexpr (std::is_same_v<T, std::string>) {
            return TSMValueType::String;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return TSMValueType::Integer;
        } else {
            static_assert(sizeof(T) == 0, "Unsupported TSM value type");
        }
    }

    // Tombstone support methods
    seastar::future<> loadTombstones();

    // Delete range with verification
    seastar::future<bool> deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime);

    // Check if series exists in time range (for verification)
    bool hasSeriesInTimeRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) const;

    // Check if TSM file could contain data in time range (more permissive, for deletes)
    bool couldContainTimeRange(uint64_t startTime, uint64_t endTime) const;

    // Query with tombstone filtering
    template <class T>
    seastar::future<TSMResult<T>> queryWithTombstones(const SeriesId128& seriesId, uint64_t startTime,
                                                      uint64_t endTime);

    // Get tombstone manager (for compaction)
    timestar::TSMTombstone* getTombstones() { return tombstones.get(); }
    bool hasTombstones() const { return tombstones && tombstones->getEntryCount() > 0; }

    // Pushdown aggregation: decode blocks and fold directly into BlockAggregator
    // instead of materialising TSMResult. Returns the number of points aggregated.
    // Works for Float, Integer, and Boolean series; returns 0 for String.
    seastar::future<size_t> aggregateSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                            timestar::BlockAggregator& aggregator);

    // Selective block reading for LATEST/FIRST without bucketing (interval=0).
    // Reads blocks in forward (reverse=false) or reverse (reverse=true) order,
    // stopping after collecting maxPoints non-tombstoned points.
    // Returns the number of points aggregated.
    seastar::future<size_t> aggregateSeriesSelective(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                     timestar::BlockAggregator& aggregator, bool reverse,
                                                     size_t maxPoints);

    // Bucketed block reading for LATEST/FIRST with time buckets (interval>0).
    // Reads blocks in forward/reverse order, skipping blocks whose buckets are
    // already filled. Stops when all buckets in [startTime, endTime] are filled.
    // filledBuckets is shared across files for cross-file early termination.
    // Returns the number of points aggregated.
    seastar::future<size_t> aggregateSeriesBucketed(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                    timestar::BlockAggregator& aggregator, bool reverse,
                                                    uint64_t interval, std::unordered_set<uint64_t>& filledBuckets,
                                                    size_t totalBuckets);

    // Estimate fraction of file data covered by tombstones (metadata-only, no data reads)
    // Returns value in [0.0, 1.0] representing estimated dead bytes / file size
    seastar::future<double> estimateTombstoneCoverage();

    // Delete tombstone file after compaction
    seastar::future<> deleteTombstoneFile();
};
