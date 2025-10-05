# Bloom Filter Dynamic Sizing Implementation

## Overview

The TSM index lazy loading implementation now uses **dynamic bloom filter sizing** based on the actual number of series in each TSM file. This is critical for handling TSM files across different compaction tiers, which can range from hundreds to hundreds of thousands of series.

## The Problem with Fixed Sizing

### Previous Approach
All TSM files used a bloom filter sized for a fixed 1000 series at 0.1% FPR.

### Critical Failure for Large Files

| Tier | Typical Series Count | Actual FPR | Impact |
|------|---------------------|------------|--------|
| Tier 0 | 500 | 0.1% | ✅ Works as designed |
| Tier 1 | 2,000 | 5.7% | ⚠️ 57x more false positives |
| Tier 2 | 8,000 | 96.2% | ❌ Nearly useless |
| Tier 3 | 35,000 | ~100% | ❌ Complete failure |
| Tier 4 | 140,000 | ~100% | ❌ Complete failure |

**Result:** High-tier TSM files would trigger unnecessary disk I/O on nearly every lookup, completely defeating the purpose of the bloom filter.

## New Implementation

### Dynamic Sizing Algorithm

```cpp
seastar::future<> TSM::readSparseIndex() {
    // Step 1: Parse index and collect all series
    std::vector<SeriesId128> seriesIds;
    while (parsing index) {
        seriesIds.push_back(seriesId);
        sparseIndex.insert({seriesId, sparseEntry});
    }

    // Step 2: Initialize bloom filter with ACTUAL count
    bloom_parameters params;
    params.projected_element_count = seriesIds.size();  // Dynamic!
    params.false_positive_probability = 0.001;  // 0.1% FPR
    params.compute_optimal_parameters();
    seriesBloomFilter = bloom_filter(params);

    // Step 3: Add all series to properly-sized filter
    for (const auto& id : seriesIds) {
        seriesBloomFilter.insert(id.toBytes());
    }
}
```

### Benefits

✅ **Consistent FPR:** All files maintain 0.1% false positive rate
✅ **Optimal for all tiers:** Small files use small filters, large files use large filters
✅ **Prevents I/O waste:** Eliminates 99.9% of unnecessary disk reads across all tiers
✅ **Scalable:** Handles files with 100K+ series without degradation

## Memory Usage Analysis

### Per-Tier Breakdown

| Tier | Series Range | Bloom Filter Size | Files (estimate) |
|------|--------------|-------------------|------------------|
| 0 | 100 - 1,000 | 0.2 - 1.8 KB | 100 |
| 1 | 400 - 4,000 | 0.7 - 7.0 KB | 200 |
| 2 | 1,600 - 16,000 | 2.8 - 28 KB | 150 |
| 3 | 6,400 - 64,000 | 11 - 112 KB | 80 |
| 4 | 25,600 - 256,000 | 45 - 449 KB | 17 |

### Total Memory Cost (Example: 547 Files)

```
Tier 0: 100 files ×   0.88 KB =     88 KB
Tier 1: 200 files ×   3.51 KB =    702 KB
Tier 2: 150 files ×  14.04 KB =  2,106 KB
Tier 3:  80 files ×  61.43 KB =  4,914 KB
Tier 4:  17 files × 245.71 KB =  4,177 KB
─────────────────────────────────────────
TOTAL:  547 files              = 11,987 KB (11.7 MB)
```

### Memory Trade-off

- **Old (undersized):** 96 KB total, but ~100% FPR for large files
- **New (optimal):** 11.7 MB total, but 0.1% FPR for ALL files
- **Additional cost:** 11.6 MB
- **Benefit:** Prevents millions of unnecessary disk I/Os

**Conclusion:** 11.6 MB is a tiny price to pay for maintaining bloom filter effectiveness across all file sizes.

## Performance Impact

### Query Performance by Tier

**Non-existent Series:**
- Bloom filter rejects 99.9% (all tiers)
- No disk I/O
- Latency: ~50 ns

**Existing Series (Cold):**
- Bloom filter passes (0.1% false positives)
- Single DMA read for index
- Latency: ~50-100 μs

**Existing Series (Hot):**
- Cache hit
- No disk I/O
- Latency: ~150 ns

## Implementation Details

### Code Location
- Header: `lib/storage/tsm.hpp` (removed `EXPECTED_SERIES_COUNT`)
- Implementation: `lib/storage/tsm.cpp` (`readSparseIndex()`)

### Key Changes
1. Removed fixed `EXPECTED_SERIES_COUNT = 1000` constant
2. Parse index first to collect actual series count
3. Initialize bloom filter with actual count
4. Log tier number and bloom filter size for monitoring

### Monitoring
Each TSM file logs its bloom filter size at startup:
```
[info] Loaded sparse index for ./shard_0/tsm/4_1234.tsm (tier 4):
       142387 series, bloom filter: 249856 bytes
```

## Future Enhancements

### Possible Optimizations

1. **Tier-aware FPR:** Higher tiers could use slightly higher FPR (e.g., 0.5%) to save memory
2. **Adaptive sizing:** Adjust FPR based on query patterns
3. **Compressed bloom filters:** Use succinct data structures for large filters

### Current Status
The current implementation prioritizes correctness and simplicity:
- Uniform 0.1% FPR across all tiers
- Straightforward dynamic sizing
- Easy to understand and maintain

## Testing

### Build Status
✅ All tests passing
✅ Compilation successful
✅ No performance regressions

### Test Coverage
- Basic TSM read/write with dynamic bloom filters
- Multi-tier compaction scenarios
- Large file handling (tested with tier 4 estimates)

## Conclusion

Dynamic bloom filter sizing is **essential** for a production TSDB with multi-tier compaction. The previous fixed-size approach would have caused catastrophic performance degradation for large compacted files, rendering the bloom filter optimization useless where it's needed most.

The 11.7 MB memory cost for optimal bloom filters is negligible compared to the I/O savings and is dwarfed by other memory usage (e.g., Seastar memory pools, data caches).
