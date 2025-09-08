# TSDB Metadata Model and LevelDB Integration

## Overview

The TSDB uses LevelDB as a persistent metadata index to efficiently map time series identifiers to numeric series IDs and store measurement metadata. This document describes the metadata model, key encoding schemes, and how LevelDB supports timeseries operations.

## Metadata Model

### Core Concepts

1. **Measurement**: A logical grouping of related time series data (e.g., "temperature", "cpu_usage")
2. **Tags**: Key-value pairs that identify and categorize a series (e.g., {"location": "us-west", "host": "server01"})
3. **Fields**: The actual data values being measured (e.g., "value", "humidity")
4. **Series**: A unique combination of measurement + tags + field that forms a distinct time series
5. **Series ID**: A numeric identifier (uint64_t) assigned to each unique series for efficient storage

### Series Identity

A series is uniquely identified by the tuple:
```
(measurement, tags, field) → series_id
```

Example:
```
("temperature", {"location": "us-west", "host": "server01"}, "value") → 12345
```

## LevelDB Index Architecture

### Storage Layout

Each shard maintains its own LevelDB instance stored at:
```
shard_N/index/
├── 000001.log     # Write-ahead log
├── 000002.ldb     # Sorted string table files
├── CURRENT        # Current manifest
├── LOCK          # Process lock file
├── LOG           # Debug log
└── MANIFEST-*    # Version manifest
```

### Key Encoding Scheme

The index uses prefix-based key encoding to separate different types of metadata:

```cpp
enum class IndexKeyType : uint8_t {
    SERIES_MAPPING    = 0x01,  // measurement+tags+field → series_id
    MEASUREMENT_FIELDS = 0x02,  // measurement → [fields]
    MEASUREMENT_TAGS  = 0x03,  // measurement → [tag_keys]
    TAG_VALUES       = 0x04,  // measurement+tag_key → [values]
};
```

### Key Formats

#### 1. Series Mapping Keys (0x01)
```
Key:   [0x01][measurement]\0[sorted_tags]\0[field]
Value: [series_id] (8 bytes, little-endian)
```

Example:
```
Key:   0x01temperature\0host=server01,location=us-west\0value
Value: 0x3930000000000000  (12345 in little-endian)
```

#### 2. Measurement Fields Keys (0x02)
```
Key:   [0x02][measurement]
Value: [field1]\0[field2]\0[field3]...
```

Example:
```
Key:   0x02temperature
Value: value\0humidity\0pressure
```

#### 3. Measurement Tags Keys (0x03)
```
Key:   [0x03][measurement]
Value: [tag_key1]\0[tag_key2]\0[tag_key3]...
```

Example:
```
Key:   0x03temperature
Value: location\0host\0sensor_type
```

#### 4. Tag Values Keys (0x04)
```
Key:   [0x04][measurement]\0[tag_key]
Value: [value1]\0[value2]\0[value3]...
```

Example:
```
Key:   0x04temperature\0location
Value: us-west\0us-east\0eu-central
```

## Operations

### Series ID Assignment

When a new series is encountered:

1. **Check Existing**: Query LevelDB for existing series mapping
2. **Generate ID**: If not found, generate next sequential ID using atomic counter
3. **Store Mapping**: Write series → ID mapping to LevelDB
4. **Update Metadata**: Update measurement fields, tags, and tag values as needed

```cpp
// Pseudocode for series ID assignment
uint64_t getOrCreateSeriesId(measurement, tags, field) {
    key = encodeSeriesKey(measurement, tags, field);
    
    if (value = leveldb.get(key)) {
        return decodeSeriesId(value);
    }
    
    newId = atomicIncrement(nextSeriesId);
    leveldb.put(key, encodeSeriesId(newId));
    
    updateMeasurementMetadata(measurement, field, tags);
    return newId;
}
```

### Metadata Discovery

The index supports efficient metadata queries:

```cpp
// Get all fields for a measurement
vector<string> getMeasurementFields(measurement) {
    key = [0x02] + measurement;
    value = leveldb.get(key);
    return split(value, '\0');
}

// Get all tag keys for a measurement
vector<string> getMeasurementTags(measurement) {
    key = [0x03] + measurement;
    value = leveldb.get(key);
    return split(value, '\0');
}

// Get all values for a specific tag
vector<string> getTagValues(measurement, tagKey) {
    key = [0x04] + measurement + '\0' + tagKey;
    value = leveldb.get(key);
    return split(value, '\0');
}
```

### Query Planning

The index enables efficient query planning by:

1. **Series Discovery**: Find all series IDs matching query filters
2. **Shard Routing**: Determine which shards contain relevant data
3. **Time Range Optimization**: Use TSM file metadata to skip irrelevant files

Example query flow:
```
Query: "avg:temperature(value){location:us-west}"
↓
1. Lookup all series with measurement="temperature", field="value"
2. Filter series where tags contain location="us-west"
3. Get series IDs: [12345, 12346, 12347]
4. Route query to shards containing these series
5. Execute query on TSM files and memory stores
```

## Performance Optimizations

### LevelDB Configuration

The implementation uses optimized LevelDB settings:

```cpp
leveldb::Options options;
options.create_if_missing = true;
options.compression = leveldb::kSnappyCompression;  // Snappy compression
options.write_buffer_size = 4 * 1024 * 1024;       // 4MB write buffer
options.max_open_files = 1000;                      // File handle limit
options.block_cache = leveldb::NewLRUCache(8*1024*1024); // 8MB cache
options.filter_policy = leveldb::NewBloomFilterPolicy(10); // Bloom filter
```

### Batch Operations

Bulk inserts use batch writes for efficiency:

```cpp
leveldb::WriteBatch batch;
for (const auto& series : newSeries) {
    batch.Put(encodeSeriesKey(series), encodeSeriesId(nextId++));
}
leveldb.Write(leveldb::WriteOptions(), &batch);
```

### Caching Strategy

1. **In-Memory Cache**: Recent series mappings cached in memory
2. **LRU Block Cache**: LevelDB's built-in block cache for hot data
3. **Bloom Filters**: Reduce disk I/O for non-existent keys

## Data Consistency

### Write Path

1. **Atomic Updates**: Series ID assignment uses atomic operations
2. **Write Batching**: Multiple metadata updates grouped in transactions
3. **WAL Integration**: Metadata updates coordinated with data writes

### Recovery

On startup:
1. Open LevelDB index (automatic recovery from WAL)
2. Scan for highest series ID to restore counter
3. Verify consistency between index and TSM files

## Storage Overhead

### Space Requirements

- **Series Mapping**: ~100 bytes per unique series
- **Measurement Metadata**: ~50 bytes per measurement
- **Tag Values**: ~20 bytes per unique tag value
- **Compression**: Snappy reduces storage by 30-50%

### Example Calculation

For a system with:
- 1000 measurements
- 100 unique series per measurement
- 5 tags per series with 10 values each

Storage requirement:
```
Series mappings:  100,000 × 100 bytes = 10 MB
Measurement meta: 1,000 × 50 bytes = 50 KB
Tag values:       1,000 × 5 × 10 × 20 bytes = 1 MB
Total (uncompressed): ~11 MB
Total (compressed): ~6-8 MB
```

## Query Examples

### Finding Series by Pattern

```cpp
// Find all temperature series from us-west
auto seriesIds = co_await index.findSeries(
    "temperature",                    // measurement
    {{"location", "us-west"}},        // tag filter
    {}                                // field filter (empty = all)
);
```

### Metadata Exploration

```cpp
// Discover available data
auto measurements = co_await index.listMeasurements();
for (const auto& m : measurements) {
    auto fields = co_await index.getMeasurementFields(m);
    auto tags = co_await index.getMeasurementTags(m);
    
    fmt::print("Measurement: {}\n", m);
    fmt::print("  Fields: {}\n", fmt::join(fields, ", "));
    fmt::print("  Tags: {}\n", fmt::join(tags, ", "));
}
```

### Cardinality Analysis

```cpp
// Count unique series per measurement
for (const auto& measurement : measurements) {
    auto count = co_await index.getSeriesCount(measurement);
    fmt::print("{}: {} series\n", measurement, count);
}
```

## Future Enhancements

### Planned Features

1. **Inverted Indexes**: Tag value → series ID mappings for faster queries
2. **Cardinality Limits**: Enforce limits on tag value cardinality
3. **Retention Policies**: Automatic metadata cleanup for expired series
4. **Statistics**: Track query patterns and access frequencies

### Potential Optimizations

1. **Partitioned Indexes**: Split index by time range for faster cleanup
2. **Compression**: Dictionary encoding for repeated tag values
3. **Memory Mapping**: mmap() support for read-heavy workloads
4. **Distributed Index**: Cross-shard index coordination

## Implementation Files

Key source files for the metadata system:

- `lib/index/leveldb_index.hpp/cpp` - Core LevelDB index implementation
- `lib/engine/engine.hpp/cpp` - Integration with storage engine
- `lib/http/http_metadata_handler.hpp/cpp` - HTTP metadata endpoints
- `test/test_leveldb_index.cpp` - Unit tests for index operations

## Best Practices

1. **Tag Design**: Use low-cardinality tags (< 1000 unique values)
2. **Field Grouping**: Group related fields in same measurement
3. **Naming Conventions**: Use consistent naming for measurements and tags
4. **Query Patterns**: Design tags to match common query filters
5. **Monitoring**: Track index size and query performance metrics