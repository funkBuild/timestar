# Query Endpoint Implementation Plan

## Overview
Implement a comprehensive query endpoint for the TSDB that supports flexible time series queries with filtering, aggregation, and multiple response formats.

## Query Format Design

### Basic Query Structure
```json
{
  "query": {
    "measurement": "temperature",
    "fields": ["value", "humidity"],  // Optional: specific fields to return
    "tags": {                          // Optional: tag filters
      "location": "us-west",
      "sensor": "temp-*"               // Support wildcards
    },
    "time_range": {
      "start": 1638202821000000000,   // Start time in nanoseconds
      "end": 1638289221000000000,     // End time in nanoseconds
      // Alternative format:
      "start": "2024-01-01T00:00:00Z",
      "end": "now"                    // Special keywords: "now", "today", "yesterday"
    },
    "aggregation": {                  // Optional: aggregation settings
      "function": "mean",              // mean, sum, min, max, count, first, last
      "interval": "5m",                // 5m, 1h, 1d, etc.
      "fill": "none"                   // none, null, previous, linear, 0
    },
    "limit": 1000,                     // Optional: max points to return
    "offset": 0,                       // Optional: pagination offset
    "order": "asc"                     // asc or desc
  }
}
```

### Advanced Query Features

#### Multiple Series Query
```json
{
  "queries": [
    {
      "id": "cpu_usage",
      "measurement": "cpu",
      "fields": ["usage_percent"],
      "tags": {"host": "server-01"},
      "time_range": {"start": "1h", "end": "now"}
    },
    {
      "id": "memory_usage",
      "measurement": "memory",
      "fields": ["used_gb"],
      "tags": {"host": "server-01"},
      "time_range": {"start": "1h", "end": "now"}
    }
  ],
  "join": "time"  // Join results by timestamp
}
```

#### Regex and Wildcard Support
```json
{
  "query": {
    "measurement": "cpu",
    "tags": {
      "host": "/^server-[0-9]+$/",    // Regex pattern
      "datacenter": "dc*"              // Wildcard pattern
    }
  }
}
```

#### Group By Query
```json
{
  "query": {
    "measurement": "temperature",
    "fields": ["value"],
    "group_by": ["location", "sensor"],
    "aggregation": {
      "function": "mean",
      "interval": "1h"
    }
  }
}
```

## Response Format

### Successful Response
```json
{
  "status": "success",
  "series": [
    {
      "measurement": "temperature",
      "tags": {
        "location": "us-west",
        "sensor": "temp-01"
      },
      "fields": {
        "value": {
          "timestamps": [1638202821000000000, 1638202822000000000],
          "values": [23.5, 23.6]
        },
        "humidity": {
          "timestamps": [1638202821000000000, 1638202822000000000],
          "values": [45.2, 45.3]
        }
      }
    }
  ],
  "statistics": {
    "series_count": 1,
    "point_count": 4,
    "execution_time_ms": 12.5,
    "shards_queried": [0, 1, 2, 3]
  }
}
```

### Error Response
```json
{
  "status": "error",
  "error": {
    "code": "INVALID_TIME_RANGE",
    "message": "Start time must be before end time",
    "details": {
      "start": 1638289221000000000,
      "end": 1638202821000000000
    }
  }
}
```

## Implementation Architecture

### 1. Query Handler (`lib/http_query_handler.hpp/cpp`)
```cpp
class HttpQueryHandler {
private:
    seastar::sharded<Engine>* engineSharded;
    
public:
    struct QueryRequest {
        std::string measurement;
        std::vector<std::string> fields;
        std::map<std::string, std::string> tags;
        uint64_t startTime;
        uint64_t endTime;
        std::optional<AggregationConfig> aggregation;
        size_t limit = 10000;
        size_t offset = 0;
    };
    
    struct QueryResponse {
        std::vector<SeriesResult> series;
        QueryStatistics stats;
    };
    
    seastar::future<std::unique_ptr<seastar::httpd::reply>> 
    handleQuery(std::unique_ptr<seastar::httpd::request> req);
    
    seastar::future<QueryResponse> 
    executeQuery(const QueryRequest& request);
    
private:
    QueryRequest parseQueryRequest(const rapidjson::Document& doc);
    std::string formatQueryResponse(const QueryResponse& response);
    seastar::future<std::vector<SeriesResult>> 
    queryAllShards(const QueryRequest& request);
};
```

### 2. Query Routing Strategy

#### Phase 1: Determine Target Shards
- For specific series (with all tags specified): Calculate exact shard
- For wildcard/regex queries: Query all shards
- Use bloom filters for quick negative lookups

#### Phase 2: Parallel Shard Execution
```cpp
seastar::future<std::vector<SeriesResult>> 
HttpQueryHandler::queryAllShards(const QueryRequest& request) {
    std::vector<seastar::future<std::vector<SeriesResult>>> shard_futures;
    
    for (unsigned shard = 0; shard < seastar::smp::count; ++shard) {
        auto f = engineSharded->invoke_on(shard, 
            [request](Engine& engine) -> seastar::future<std::vector<SeriesResult>> {
                return engine.executeLocalQuery(request);
            });
        shard_futures.push_back(std::move(f));
    }
    
    // Wait for all shards and merge results
    co_return co_await mergeResults(std::move(shard_futures));
}
```

#### Phase 3: Result Aggregation
- Merge sorted time series from multiple shards
- Apply aggregation functions if requested
- Handle pagination and limits

### 3. Engine Query Extensions

Add to `lib/engine.hpp/cpp`:
```cpp
class Engine {
public:
    // New query methods
    seastar::future<std::vector<SeriesResult>> 
    executeLocalQuery(const QueryRequest& request);
    
    seastar::future<std::set<std::string>> 
    findMatchingSeries(const std::string& measurement, 
                       const std::map<std::string, std::string>& tagFilters);
    
    seastar::future<QueryResult<T>> 
    queryWithAggregation(const std::string& seriesKey,
                         uint64_t startTime, 
                         uint64_t endTime,
                         const AggregationConfig& config);
};
```

### 4. Query Optimization Features

#### Caching Layer
- Cache recent query results with TTL
- Use series key + time range as cache key
- Invalidate on new writes to series

#### Index Optimizations
- Pre-compute common aggregations during compaction
- Maintain min/max time indexes per series
- Use skip lists for time-based lookups

#### Parallel Processing
- Split large time ranges into chunks
- Process multiple series in parallel
- Stream results to client for large datasets

## Implementation Phases

### Phase 1: Basic Query Support (Week 1)
- [ ] Implement basic query request parsing
- [ ] Single series exact match queries
- [ ] Time range filtering
- [ ] JSON response formatting
- [ ] Integration with existing Engine::query()

### Phase 2: Advanced Filtering (Week 2)
- [ ] Wildcard support for tags
- [ ] Regex support for tags
- [ ] Multiple field selection
- [ ] Tag-based series discovery

### Phase 3: Aggregation Support (Week 3)
- [ ] Time-based aggregation (mean, sum, min, max)
- [ ] Group by tags
- [ ] Fill policies for missing data
- [ ] Downsampling for large datasets

### Phase 4: Performance & Features (Week 4)
- [ ] Query result caching
- [ ] Streaming response for large results
- [ ] Query cancellation support
- [ ] Query performance metrics

## Testing Strategy

### Unit Tests
1. Query parsing validation
2. Time range conversion
3. Aggregation function correctness
4. Response formatting

### Integration Tests
1. Multi-shard query coordination
2. Large dataset queries
3. Concurrent query handling
4. Memory pressure scenarios

### Performance Tests
1. Query latency benchmarks
2. Throughput testing
3. Cache hit rate analysis
4. Resource usage monitoring

## API Examples

### Simple Query
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "measurement": "temperature",
      "fields": ["value"],
      "tags": {"location": "us-west"},
      "time_range": {
        "start": "2024-01-01T00:00:00Z",
        "end": "2024-01-02T00:00:00Z"
      }
    }
  }'
```

### Aggregated Query
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "measurement": "cpu",
      "fields": ["usage_percent"],
      "tags": {"host": "server-*"},
      "time_range": {"start": "1h", "end": "now"},
      "aggregation": {
        "function": "mean",
        "interval": "5m"
      }
    }
  }'
```

### Multi-Series Query
```bash
curl -X POST http://localhost:8086/query \
  -H "Content-Type: application/json" \
  -d '{
    "queries": [
      {
        "id": "cpu",
        "measurement": "cpu",
        "fields": ["usage_percent"],
        "tags": {"host": "server-01"}
      },
      {
        "id": "memory",
        "measurement": "memory", 
        "fields": ["used_gb"],
        "tags": {"host": "server-01"}
      }
    ],
    "time_range": {"start": "1h", "end": "now"}
  }'
```

## Success Metrics

1. **Performance**
   - P99 query latency < 100ms for simple queries
   - P99 query latency < 1s for aggregated queries
   - Support 1000+ concurrent queries

2. **Functionality**
   - Support all common time series query patterns
   - Compatible with Grafana and other visualization tools
   - Full feature parity with InfluxDB query capabilities

3. **Reliability**
   - Zero data loss during queries
   - Graceful degradation under load
   - Comprehensive error handling and reporting

## Future Enhancements

1. **Query Language Support**
   - InfluxQL compatibility layer
   - SQL-like query syntax
   - Flux query language support

2. **Advanced Analytics**
   - Moving averages
   - Rate calculations
   - Anomaly detection
   - Forecasting functions

3. **Federation Support**
   - Cross-cluster queries
   - Remote storage integration
   - Query result caching and sharing