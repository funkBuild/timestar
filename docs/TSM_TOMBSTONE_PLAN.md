# TSM Tombstone Implementation Plan

## Overview

Implement a tombstone system for TSM files to handle deletions without immediately rewriting files. Tombstones act as an overlay that marks data as deleted within specific time ranges for specific series, deferring physical deletion until compaction.

## Design Principles

1. **Non-invasive**: Tombstones stored in separate files, not modifying TSM files directly
2. **Verification**: Ensure data exists in TSM before creating tombstone
3. **Efficient Overlay**: Fast query-time filtering of tombstoned data
4. **Compaction Integration**: Clean tombstoned data during compaction
5. **Persistence**: Tombstones survive restarts and crashes

## Architecture

### 1. Tombstone File Structure

Each TSM file (`file.tsm`) will have an associated tombstone file (`file.tombstone`) containing deletion records.

```
TSM File: shard_0/tsm/000000001.tsm
Tombstone: shard_0/tsm/000000001.tombstone
```

### 2. Tombstone Record Format

```cpp
struct TombstoneEntry {
    uint64_t seriesId;      // Series identifier (8 bytes)
    uint64_t startTime;     // Start of deletion range in nanoseconds (8 bytes)
    uint64_t endTime;       // End of deletion range in nanoseconds (8 bytes)
    uint32_t checksum;      // CRC32 checksum of entry (4 bytes)
};
// Total: 28 bytes per entry
```

### 3. Tombstone File Format

```
Header (16 bytes):
  - Magic number: 0x54534D54 ('TSMT') (4 bytes)
  - Version: 1 (4 bytes)
  - Entry count (4 bytes)
  - Header checksum (4 bytes)

Entries (N * 28 bytes):
  - Array of TombstoneEntry records

Footer (8 bytes):
  - File checksum (8 bytes)
```

## Implementation Components

### 1. Core Tombstone Manager Class

```cpp
// lib/tsm_tombstone.hpp
class TSMTombstone {
private:
    std::string tombstonePath;
    std::vector<TombstoneEntry> entries;
    std::map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>> seriesRanges;
    seastar::file tombstoneFile;
    bool isDirty = false;

public:
    // Load tombstones from disk
    seastar::future<> load();
    
    // Add a new tombstone (with verification)
    seastar::future<bool> addTombstone(
        uint64_t seriesId,
        uint64_t startTime,
        uint64_t endTime,
        TSM* tsmFile  // For verification
    );
    
    // Check if a point is tombstoned
    bool isDeleted(uint64_t seriesId, uint64_t timestamp) const;
    
    // Get all tombstone ranges for a series
    std::vector<std::pair<uint64_t, uint64_t>> getTombstoneRanges(uint64_t seriesId) const;
    
    // Persist tombstones to disk
    seastar::future<> flush();
    
    // Merge with another tombstone file (for compaction)
    void merge(const TSMTombstone& other);
    
    // Remove tombstones for compacted data
    void compact(uint64_t minTime, uint64_t maxTime);
};
```

### 2. TSM File Integration

Modify `TSM` class to include tombstone support:

```cpp
class TSM {
private:
    std::unique_ptr<TSMTombstone> tombstones;
    
public:
    // Initialize tombstones when opening TSM
    seastar::future<> open() {
        // ... existing code ...
        tombstones = std::make_unique<TSMTombstone>(getTombstonePath());
        co_await tombstones->load();
    }
    
    // Query with tombstone filtering
    template<typename T>
    seastar::future<TSMResult<T>> query(
        const std::string& seriesKey,
        uint64_t startTime,
        uint64_t endTime
    ) {
        auto result = co_await queryInternal<T>(seriesKey, startTime, endTime);
        
        // Apply tombstone filtering
        if (tombstones && !result.timestamps.empty()) {
            auto ranges = tombstones->getTombstoneRanges(getSeriesId(seriesKey));
            result = filterTombstoned(result, ranges);
        }
        
        co_return result;
    }
    
    // Add deletion with verification
    seastar::future<bool> deleteRange(
        const std::string& seriesKey,
        uint64_t startTime,
        uint64_t endTime
    ) {
        // Verify series exists in this TSM
        auto seriesId = getSeriesId(seriesKey);
        if (!hasSeriesInTimeRange(seriesId, startTime, endTime)) {
            co_return false;  // Series/time range not in this file
        }
        
        // Add tombstone
        co_return co_await tombstones->addTombstone(seriesId, startTime, endTime, this);
    }
};
```

### 3. Query Integration

Modify query execution to respect tombstones:

```cpp
// lib/query_runner.cpp modifications
seastar::future<QueryResult<T>> QueryRunner::query(...) {
    // ... existing query code ...
    
    // For each TSM file queried
    for (auto& tsm : tsmFiles) {
        auto tsmResult = co_await tsm->query<T>(seriesKey, startTime, endTime);
        // Tombstone filtering happens inside TSM::query now
        mergeResults(result, tsmResult);
    }
    
    // ... rest of query code ...
}
```

### 4. Compaction Integration

During compaction, apply tombstones to remove deleted data:

```cpp
// lib/tsm_compactor.cpp modifications
seastar::future<> TSMCompactor::compact(
    const std::vector<seastar::shared_ptr<TSM>>& inputFiles,
    const std::string& outputPath
) {
    TSMWriter writer(outputPath);
    TSMTombstone mergedTombstones;
    
    // Merge all tombstones from input files
    for (const auto& tsm : inputFiles) {
        if (tsm->hasTombstones()) {
            mergedTombstones.merge(tsm->getTombstones());
        }
    }
    
    // Write non-tombstoned data to new file
    for (const auto& tsm : inputFiles) {
        auto allSeries = co_await tsm->getAllSeries();
        
        for (const auto& series : allSeries) {
            auto data = co_await tsm->readAll(series);
            
            // Filter out tombstoned points
            auto ranges = mergedTombstones.getTombstoneRanges(series.id);
            data = filterTombstoned(data, ranges);
            
            if (!data.empty()) {
                co_await writer.write(series, data);
            }
        }
    }
    
    co_await writer.close();
    
    // Clean up old tombstone files after successful compaction
    for (const auto& tsm : inputFiles) {
        co_await tsm->deleteTombstoneFile();
    }
}
```

### 5. Engine Integration

Add delete API to Engine class:

```cpp
// lib/engine.hpp
class Engine {
public:
    // Delete data for a series in a time range
    seastar::future<bool> deleteRange(
        const std::string& seriesKey,
        uint64_t startTime,
        uint64_t endTime
    ) {
        // First, mark deletions in memory store
        memoryStore->deleteRange(seriesKey, startTime, endTime);
        
        // Then, add tombstones to relevant TSM files
        bool anyDeleted = false;
        for (auto& tsm : tsmFileManager->getAllFiles()) {
            bool deleted = co_await tsm->deleteRange(seriesKey, startTime, endTime);
            anyDeleted = anyDeleted || deleted;
        }
        
        // Log deletion to WAL for recovery
        co_await wal->logDeletion(seriesKey, startTime, endTime);
        
        co_return anyDeleted;
    }
};
```

### 6. WAL Integration

Extend WAL to support deletion entries:

```cpp
// lib/wal.hpp
enum class WALEntryType : uint8_t {
    Write = 1,
    Delete = 2,
    DeleteRange = 3
};

struct WALDeleteRangeEntry {
    WALEntryType type = WALEntryType::DeleteRange;
    std::string seriesKey;
    uint64_t startTime;
    uint64_t endTime;
};
```

## Implementation Phases

### Phase 1: Core Tombstone Infrastructure (Week 1)
- [ ] Implement `TSMTombstone` class with file I/O
- [ ] Add tombstone file format serialization/deserialization
- [ ] Implement in-memory tombstone index for fast lookups
- [ ] Add unit tests for tombstone operations

### Phase 2: TSM Integration (Week 1-2)
- [ ] Modify TSM class to load/manage tombstones
- [ ] Implement verification before tombstone creation
- [ ] Add tombstone filtering to TSM queries
- [ ] Update TSM tests for tombstone scenarios

### Phase 3: Query Path Integration (Week 2)
- [ ] Modify QueryRunner to respect tombstones
- [ ] Update aggregation to exclude tombstoned data
- [ ] Ensure memory store respects tombstones
- [ ] Add query tests with tombstones

### Phase 4: Compaction Integration (Week 2-3)
- [ ] Modify compactor to apply tombstones
- [ ] Implement tombstone merging during compaction
- [ ] Clean up tombstone files after compaction
- [ ] Test compaction with various tombstone scenarios

### Phase 5: Engine & API Integration (Week 3)
- [ ] Add deleteRange API to Engine
- [ ] Implement HTTP DELETE endpoint
- [ ] Add WAL support for deletions
- [ ] Implement recovery of tombstones from WAL

### Phase 6: Testing & Optimization (Week 4)
- [ ] Comprehensive integration tests
- [ ] Performance benchmarking
- [ ] Memory usage optimization
- [ ] Documentation updates

## Performance Considerations

1. **Memory Usage**: Keep tombstone index compact using interval trees or segment trees
2. **Query Performance**: Use binary search for tombstone range lookups
3. **Compaction Frequency**: Monitor tombstone accumulation and trigger compaction when needed
4. **File I/O**: Batch tombstone writes to reduce I/O operations
5. **Cache**: Consider caching frequently accessed tombstone files

## Testing Strategy

### Unit Tests
- Tombstone file creation/loading
- Tombstone range operations
- Verification logic
- Merge operations

### Integration Tests
- Query with tombstones across multiple TSM files
- Compaction with tombstones
- WAL recovery with deletions
- Concurrent deletion and queries

### Performance Tests
- Query performance with varying tombstone counts
- Compaction performance with heavy tombstones
- Memory usage under tombstone load

## API Design

### HTTP DELETE Endpoint

```http
DELETE /series?measurement=<name>&startTime=<ns>&endTime=<ns>&tags=<filters>

Response:
{
  "status": "success",
  "deletedPoints": 12345,
  "affectedSeries": 10,
  "affectedFiles": 3
}
```

### Client API

```cpp
// Delete all temperature data for a specific sensor in a time range
engine.deleteRange(
    "temperature,location=us-west,sensor=temp01",
    1704067200000000000,  // startTime
    1704153600000000000   // endTime
);
```

## Migration Strategy

1. **Backward Compatibility**: TSM files without tombstones continue to work
2. **Version Detection**: Check for tombstone file existence before loading
3. **Upgrade Path**: Existing systems can start using tombstones immediately
4. **Rollback**: Tombstone files can be deleted to rollback feature

## Future Enhancements

1. **Bulk Deletion**: Optimize for deleting entire measurements
2. **Retention Policies**: Automatic deletion based on data age
3. **Tombstone Compression**: Compress tombstone files for large deletions
4. **Statistics**: Track deletion statistics and tombstone effectiveness
5. **Partial Compaction**: Compact only heavily tombstoned files

## Success Metrics

1. **Correctness**: Deleted data never appears in queries
2. **Performance**: <5% query overhead with moderate tombstones
3. **Storage**: Tombstone files <1% of TSM file size
4. **Compaction**: 90% reduction in tombstones after compaction
5. **Recovery**: Full tombstone recovery from WAL on restart