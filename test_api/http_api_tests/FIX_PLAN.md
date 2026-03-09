# Fix Plan for Remaining TimeStar Test Issues

## Current Status
- **14/25 tests passing (56%)**
- Core aggregation functionality fixed
- 11 tests still failing due to formatting and edge cases

## Issue Analysis & Fixes

### 1. Error Response Format (6 test failures) - HIGHEST PRIORITY

**Problem**: Tests expect error message directly in `error.response.data.error` but server returns nested structure:
```javascript
// Current server response:
{
  "status": "error",
  "error": {
    "code": "INVALID_QUERY",
    "message": "Query needs to specify an aggregation method"
  }
}

// Tests expect error string directly:
error.response.data.error // Should contain the message string
```

**Fix**: Modify `HttpQueryHandler::createErrorResponse()` in `http_query_handler.cpp` (line 298):
```cpp
std::string HttpQueryHandler::createErrorResponse(const std::string& code, const std::string& message) {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();
    
    doc.AddMember("status", "error", allocator);
    
    // Change: Put message directly in "error" field for backward compatibility
    rapidjson::Value msgVal;
    msgVal.SetString(message.c_str(), allocator);
    doc.AddMember("error", msgVal, allocator);
    
    // Optional: Keep detailed error in separate field
    rapidjson::Value errorDetails(rapidjson::kObjectType);
    rapidjson::Value codeVal;
    codeVal.SetString(code.c_str(), allocator);
    msgVal.SetString(message.c_str(), allocator);
    errorDetails.AddMember("code", codeVal, allocator);
    errorDetails.AddMember("message", msgVal, allocator);
    doc.AddMember("details", errorDetails, allocator);
    
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
    
    return buffer.GetString();
}
```

### 2. Group-By Processing Issue (1 test failure)

**Problem**: When group-by is specified, the aggregation is reducing each group to a single value instead of preserving the time series.

**Current Behavior**: 
- `applyGroupBy()` calls `Aggregator::aggregateMultiple()` which reduces to single value when no interval specified
- This happens in lines 719-723 of `http_query_handler.cpp`

**Fix**: In `applyGroupBy()` function, don't aggregate if we want to preserve time series:
```cpp
// Line 717-734 in http_query_handler.cpp
for (const auto& [fieldName, fieldSeries] : fieldGroups) {
    // If no aggregation interval, just merge the series without aggregating
    if (request.aggregationInterval == 0) {
        // Merge series at same timestamps for the group
        std::map<uint64_t, std::vector<double>> timeMap;
        
        for (const auto& [timestamps, values] : fieldSeries) {
            for (size_t i = 0; i < timestamps.size(); ++i) {
                timeMap[timestamps[i]].push_back(values[i]);
            }
        }
        
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        
        // Apply aggregation method at each timestamp
        for (const auto& [timestamp, vals] : timeMap) {
            timestamps.push_back(timestamp);
            double aggregatedValue;
            switch (request.aggregation) {
                case AggregationMethod::MIN:
                    aggregatedValue = *std::min_element(vals.begin(), vals.end());
                    break;
                case AggregationMethod::MAX:
                    aggregatedValue = *std::max_element(vals.begin(), vals.end());
                    break;
                case AggregationMethod::AVG:
                    aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0) / vals.size();
                    break;
                case AggregationMethod::SUM:
                    aggregatedValue = std::accumulate(vals.begin(), vals.end(), 0.0);
                    break;
                case AggregationMethod::LATEST:
                    // Take the last value for this timestamp
                    aggregatedValue = vals.back();
                    break;
                default:
                    aggregatedValue = vals.front();
            }
            values.push_back(aggregatedValue);
        }
        
        grouped.fields[fieldName] = std::make_pair(timestamps, values);
    } else {
        // With interval, use existing aggregation
        auto aggregated = Aggregator::aggregateMultiple(
            fieldSeries,
            request.aggregation,
            request.aggregationInterval
        );
        
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        
        for (const auto& point : aggregated) {
            timestamps.push_back(point.timestamp);
            values.push_back(point.value);
        }
        
        grouped.fields[fieldName] = std::make_pair(timestamps, values);
    }
}
```

### 3. Add Scope Information to Response (1 test failure)

**Problem**: Tests expect `scopes` field in response showing applied filters

**Fix**: In `formatQueryResponse()` function, add scopes from the original request:
```cpp
std::string HttpQueryHandler::formatQueryResponse(const QueryResponse& response) {
    // ... existing code ...
    
    // Add scopes if they exist
    if (!response.request.scopes.empty()) {
        rapidjson::Value scopesArray(rapidjson::kArrayType);
        
        for (const auto& [name, value] : response.request.scopes) {
            rapidjson::Value scopeObj(rapidjson::kObjectType);
            scopeObj.AddMember("name", rapidjson::Value(name.c_str(), allocator), allocator);
            scopeObj.AddMember("value", rapidjson::Value(value.c_str(), allocator), allocator);
            scopesArray.PushBack(scopeObj, allocator);
        }
        
        doc.AddMember("scopes", scopesArray, allocator);
    }
    
    // ... rest of code ...
}
```

**Note**: Need to pass the original QueryRequest to formatQueryResponse or store scopes in QueryResponse.

### 4. Boolean Data Issue (1 test failure)

**Problem**: Boolean query returning only 1 value instead of 2

**Investigation Needed**:
- Check if boolean values are being stored correctly
- Verify LATEST aggregation preserves all values
- May need special handling for boolean types

### 5. String/Image Data Issue (1 test failure)

**Problem**: String fields might be in wrong location in response

**Investigation Needed**:
- Check if strings are in `fields` or `string_fields`
- Verify string data encoding/decoding

### 6. Field Prefix Issue (1 test failure)

**Problem**: Query for field "pnf" when "pnf_status" also exists returns wrong number of values

**Likely Cause**: Field name matching might be using prefix matching instead of exact match

## Implementation Order

1. **Fix Error Response Format** (5 minutes)
   - Simple change to `createErrorResponse()`
   - Will fix 6 tests immediately

2. **Fix Group-By Processing** (20 minutes)
   - Modify `applyGroupBy()` to preserve time series
   - Will fix group-by test

3. **Add Scope Information** (15 minutes)
   - Need to pass request info through to response
   - Add to `formatQueryResponse()`

4. **Debug Boolean/String Issues** (30 minutes)
   - Investigate actual data flow
   - May need special type handling

## Expected Results

After implementing fixes 1-3:
- **Expected passing**: 21/25 tests (84%)
- **Remaining issues**: Data type edge cases

After all fixes:
- **Target**: 25/25 tests (100%)

## Code Files to Modify

1. `lib/http/http_query_handler.cpp`
   - `createErrorResponse()` - line 298
   - `applyGroupBy()` - lines 666-740
   - `formatQueryResponse()` - line 227
   - May need to modify `QueryResponse` struct to include request info

2. `lib/http/http_query_handler.hpp`
   - May need to update `QueryResponse` struct

## Testing Command

After each fix:
```bash
cd /home/matt/Desktop/source/timestar/build
make timestar_http_server
pkill -f timestar_http_server; sleep 1
./bin/timestar_http_server > /tmp/server.log 2>&1 &
cd ../test/http_api_tests
npm test
```