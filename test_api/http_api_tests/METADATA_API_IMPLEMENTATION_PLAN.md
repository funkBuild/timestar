# Metadata API Implementation Plan

## Overview
This document outlines the implementation plan for three new metadata endpoints:
- `/measurements` - List all measurements in the database
- `/tags` - List tag keys and values for a measurement
- `/fields` - List fields and their types for a measurement

## Architecture Integration

### 1. Leverage Existing LevelDB Index System
The TSDB already has a LevelDB-based indexing system (`lib/leveldb_index.hpp/cpp`) that stores:
- Series mappings: `measurement+tags+field → series_id`
- Measurement fields: `measurement → [field1, field2, ...]`
- Measurement tags: `measurement → [tag_key1, tag_key2, ...]`
- Tag values: `measurement+tag_key → [value1, value2, ...]`

**Key Insight**: We can query this existing index directly to implement the metadata APIs without scanning actual data files.

### 2. Cross-Shard Coordination
Since data is sharded across 32 shards, we need to:
- Query metadata from all shards
- Merge results (deduplicate measurements, combine tag values, merge field lists)
- Return consolidated view to client

## Implementation Details

### HttpMetadataHandler Class
Create a new handler class similar to existing handlers:

```cpp
// lib/http/http_metadata_handler.hpp
class HttpMetadataHandler {
private:
    Engine* engine;
    
public:
    HttpMetadataHandler(Engine* _engine);
    
    // Register routes
    void registerRoutes(seastar::httpd::routes& r);
    
    // Handler methods
    seastar::future<std::unique_ptr<seastar::httpd::reply>> 
        handleMeasurements(std::unique_ptr<seastar::httpd::request> req);
    
    seastar::future<std::unique_ptr<seastar::httpd::reply>> 
        handleTags(std::unique_ptr<seastar::httpd::request> req);
    
    seastar::future<std::unique_ptr<seastar::httpd::reply>> 
        handleFields(std::unique_ptr<seastar::httpd::request> req);
};
```

### Engine Integration
Add metadata query methods to the Engine class:

```cpp
// In lib/core/engine.hpp
seastar::future<std::vector<std::string>> getAllMeasurements();
seastar::future<std::unordered_map<std::string, std::vector<std::string>>> 
    getTagsForMeasurement(const std::string& measurement);
seastar::future<std::unordered_map<std::string, FieldInfo>> 
    getFieldsForMeasurement(const std::string& measurement);
```

### Implementation Steps

#### Phase 1: /measurements Endpoint
1. **Engine Method**: `getAllMeasurements()`
   - Query each shard's LevelDB index
   - Extract unique measurement names from series keys
   - Merge and deduplicate across shards
   - Sort alphabetically

2. **HTTP Handler**: 
   - Parse query parameters (prefix, limit, offset, include_stats)
   - Call engine method
   - Format JSON response
   - Handle errors

3. **Response Format**:
```json
{
  "measurements": ["cpu", "memory", "disk"],
  "total": 3
}
```

#### Phase 2: /tags Endpoint
1. **Engine Method**: `getTagsForMeasurement()`
   - Use existing index methods:
     - `getMeasurementTags()` - get tag keys
     - `getTagValues()` - get values for each key
   - Aggregate across all shards
   - Deduplicate values

2. **HTTP Handler**:
   - Require `measurement` parameter
   - Optional `tag` parameter for specific tag values
   - Format response with tag keys and their unique values

3. **Response Format**:
```json
{
  "measurement": "cpu",
  "tags": {
    "host": ["server01", "server02"],
    "datacenter": ["us-east", "us-west"]
  }
}
```

#### Phase 3: /fields Endpoint
1. **Engine Method**: `getFieldsForMeasurement()`
   - Use existing `getMeasurementFields()` from index
   - Query actual data to determine field types
   - Merge field lists from all shards
   - Handle type conflicts (same field, different types)

2. **HTTP Handler**:
   - Require `measurement` parameter
   - Optional tag filter
   - Optional statistics flag

3. **Response Format**:
```json
{
  "measurement": "cpu",
  "fields": {
    "usage": {
      "type": "float",
      "stats": {
        "min": 0.0,
        "max": 100.0,
        "mean": 45.2,
        "count": 1523
      }
    },
    "cores": {
      "type": "integer"
    }
  }
}
```

## Integration Points

### 1. Modify tsdb_http_server.cpp
```cpp
// Add metadata handler
std::unique_ptr<HttpMetadataHandler> g_metadataHandler;

// In initialization
g_metadataHandler = std::make_unique<HttpMetadataHandler>(g_engine.get());

// In set_routes
if (g_metadataHandler) {
    g_metadataHandler->registerRoutes(r);
}
```

### 2. LevelDB Index Enhancements
May need to add methods to efficiently:
- List all measurements (scan key prefixes)
- Get field types (may need to store type info in index)

### 3. Caching Strategy
Consider caching metadata since it changes infrequently:
- Cache measurements list for 60 seconds
- Cache tag values for 30 seconds
- Cache field definitions for 30 seconds
- Invalidate on writes to affected measurements

## Performance Considerations

1. **Index Scanning**: Use LevelDB iterators with prefix bounds to efficiently scan measurements
2. **Parallel Shard Queries**: Use `seastar::parallel_for_each` to query all shards concurrently
3. **Result Aggregation**: Use efficient data structures for merging (unordered_set for deduplication)
4. **Response Streaming**: For large result sets, consider streaming JSON responses

## Error Handling

1. **Missing Parameters**: Return 400 Bad Request with clear error message
2. **Non-existent Measurement**: Return 200 OK with empty results (not 404)
3. **Index Corruption**: Log error and return 500 Internal Server Error
4. **Timeout**: Set reasonable timeout for metadata queries (5 seconds)

## Testing Strategy

1. **Unit Tests**: Test engine methods with mock index
2. **Integration Tests**: Test full flow with real data
3. **Performance Tests**: Ensure metadata queries don't impact write/query performance
4. **API Tests**: The test file already created covers all scenarios

## Security Considerations

1. **DoS Prevention**: Limit result size (max 10,000 measurements/tags/fields)
2. **Input Validation**: Validate measurement names and tag filters
3. **Rate Limiting**: Consider rate limiting metadata endpoints

## Future Enhancements

1. **Metadata Write Endpoint**: Allow adding measurement/field descriptions
2. **Schema Enforcement**: Optional schema validation based on metadata
3. **Retention Policies**: Per-measurement retention configuration
4. **Access Control**: Measurement-level access control using metadata

## Implementation Order

1. First implement `/measurements` (simplest)
2. Then `/tags` (builds on measurements)
3. Finally `/fields` (most complex, may need type detection)

## Estimated Timeline

- `/measurements` endpoint: 2-3 hours
- `/tags` endpoint: 3-4 hours  
- `/fields` endpoint: 4-5 hours
- Testing and debugging: 2-3 hours
- **Total**: ~12-15 hours

## Success Criteria

1. All three endpoints return correct metadata
2. Response time < 100ms for typical queries
3. No memory leaks or crashes under load
4. All tests pass (including the new metadata_api.test.js)
5. Documentation updated with new endpoints