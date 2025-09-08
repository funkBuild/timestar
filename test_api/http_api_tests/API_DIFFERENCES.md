# TSDB HTTP API Differences and Required Changes

## Test Results Summary

- **Total Tests**: 25
- **Passed**: 4 (16%)
- **Failed**: 21 (84%)

## Key API Differences Found

### 1. Aggregation Behavior (CRITICAL)

**Issue**: Aggregation functions (MIN, MAX, AVG, SUM) are not combining data across series when no group-by is specified.

**Current Behavior**:
- Query: `min:measurement()` returns multiple series (one per unique tag combination)
- Each series shows its own min value

**Expected Behavior**:
- Should return a single aggregated series combining all data points
- MIN should return minimum values across ALL series for each timestamp

**Example**:
```javascript
// Current: Returns 3 series
{
  "series": [
    { "tags": {"device": "dev0"}, "values": [0] },
    { "tags": {"device": "dev1"}, "values": [10] },
    { "tags": {"device": "dev2"}, "values": [20] }
  ]
}

// Expected: Returns 1 series with minimum across all
{
  "series": [
    { "tags": {}, "values": [0] }  // Minimum across all devices
  ]
}
```

### 2. Field Selection

**Issue**: Empty parentheses `()` should return all fields, but behavior needs verification.

**Test Status**: Failing due to aggregation issue above

### 3. Scope Filtering

**Issue**: The `scopes` field in response has different format.

**Current Format**:
```javascript
// Query: avg:measurement(){tag:value}
// Response includes scopes but format may differ
```

**Expected Format**:
```javascript
{
  "scopes": [
    { "name": "paddock", "value": "back-paddock" }
  ]
}
```

### 4. Group By Functionality

**Status**: Partially working
- Works when group-by is specified explicitly
- Fails when expecting automatic aggregation without group-by

### 5. Data Type Support

#### Boolean Values
**Issue**: Boolean values stored as 1.0/0.0 (working) but aggregation issues affect tests

#### String/Image Data  
**Issue**: String fields may be in different response location
- Might be in `string_fields` instead of `fields`
- Need to verify binary/string data handling

### 6. Error Response Format

**Current Format**:
```javascript
{
  "error": {
    "code": "INVALID_QUERY",
    "message": "Query parse error: ..."
  }
}
```

**Test Expects**:
- Error message to contain specific keywords like "aggregation method"
- Current messages are more technical/specific

### 7. Time Interval Aggregation

**Status**: Unknown - blocked by basic aggregation issues

### 8. Latest Aggregation

**Issue**: LATEST aggregation behavior needs verification
- Should return the most recent values
- Currently blocked by aggregation issues

## Required Server Changes

### Priority 1 (Critical)

1. **Fix Aggregation Logic**
   - When no `group by` clause is present, aggregate across ALL series
   - Combine data points at same timestamps across different tag combinations
   - Return single series with empty tags when not grouping

2. **Implement Proper MIN/MAX/AVG/SUM**
   - MIN: Return minimum value at each timestamp across all series
   - MAX: Return maximum value at each timestamp across all series  
   - AVG: Return average value at each timestamp across all series
   - SUM: Return sum of values at each timestamp across all series

### Priority 2 (Important)

3. **Fix LATEST Aggregation**
   - Should return the most recent data points
   - When aggregating, return latest from each series then combine

4. **Standardize Error Messages**
   - Make error messages match expected patterns
   - Include keywords tests are looking for

5. **Verify String/Binary Field Handling**
   - Ensure string fields are accessible in consistent location
   - Support binary data (images) properly

### Priority 3 (Nice to Have)

6. **Add Scopes to Response**
   - Include applied scope filters in response
   - Format: `"scopes": [{"name": "tag", "value": "value"}]`

7. **Improve Statistics**
   - Ensure point_count reflects actual data points processed
   - Add more detailed timing information

## Implementation Notes

The core issue is in the query execution logic. Currently, the server appears to:
1. Find all matching series
2. Apply aggregation to each series independently
3. Return all series

It should instead:
1. Find all matching series
2. If no group-by: merge all series data
3. Apply aggregation to merged data
4. Return single aggregated series

## Test Command

To reproduce issues:
```bash
cd test/http_api_tests
npm test

# Or run specific test suites:
npm test -- --testNamePattern="Aggregation Functions"
```

## Files to Modify

Based on the codebase structure:
- `lib/http/http_query_handler.cpp` - Main query handling logic
- `lib/query/aggregator.cpp` - Aggregation implementation
- `lib/query/query_planner.cpp` - Query planning logic
- `lib/query/query_parser.cpp` - Error message improvements