# Datadog-Compatible Metric Functions

This document describes all Datadog metric functions and their implementation status in TSDB.

## Function Categories Overview

| Category | Description | Status |
|----------|-------------|--------|
| [Algorithms](#1-algorithms) | Anomaly detection, forecasting, outliers | ✅ Complete |
| [Arithmetic](#2-arithmetic) | Mathematical operations | ✅ Complete |
| [Count](#3-count) | Count non-zero/non-null values | ✅ Complete |
| [Exclusion](#4-exclusion) | Filter/clamp values | ✅ Complete |
| [Interpolation](#5-interpolation) | Fill missing values | ✅ Complete |
| [Rank](#6-rank) | Top/bottom N series selection | ✅ Complete |
| [Rate](#7-rate) | Derivatives and rate calculations | ✅ Complete |
| [Regression](#8-regression) | Trend lines and regression | ✅ Complete |
| [Rollup](#9-rollup) | Time aggregation | ✅ Complete |
| [Smoothing](#10-smoothing) | EWMA, median filters | ✅ Complete |
| [Timeshift](#11-timeshift) | Historical comparison | ✅ Complete |

---

## 1. ALGORITHMS

Functions for detecting patterns, anomalies, and predicting future values.

### anomalies()

Detects when a metric behaves differently than expected based on historical patterns.

```
anomalies(query, algorithm, bounds, direction)
```

**Parameters:**
- `query` - The metric query
- `algorithm` - Detection algorithm: `basic`, `agile`, `robust`
- `bounds` - Number of standard deviations (typically 2-3)
- `direction` - `above`, `below`, or `both`

**Algorithms:**
| Algorithm | Description | Best For |
|-----------|-------------|----------|
| `basic` | Rolling quantile, no seasonality | Metrics without seasonal patterns |
| `agile` | SARIMA-based, adapts quickly | Metrics with level shifts |
| `robust` | Seasonal-trend decomposition | Stable seasonal metrics |

**Status:** ⚠️ Partial (have anomaly detection, need full Datadog syntax)

### forecast()

Predicts future metric values based on historical patterns.

```
forecast(query, algorithm, deviations, seasonality, model, history)
```

**Parameters:**
- `query` - The metric query
- `algorithm` - `linear` or `seasonal`
- `deviations` - Confidence interval width (1-4)
- `seasonality` - `hourly`, `daily`, `weekly`, `auto`, `multi`
- `model` - For linear: `default`, `simple`, `reactive`
- `history` - Duration string like `1w`, `3d`

**Status:** ✅ Implemented

### outliers()

Identifies metric series that behave differently from their peers (spatial anomaly).

```
outliers(query, algorithm, tolerance, percentage)
```

**Parameters:**
- `query` - The metric query returning multiple series
- `algorithm` - `dbscan` or `mad` (Median Absolute Deviation)
- `tolerance` - Sensitivity threshold (higher = fewer outliers)
- `percentage` - Minimum percentage of points to be outlier (0-100)

**Status:** ✅ Implemented

---

## 2. ARITHMETIC

Mathematical operations on metric values.

### abs()

Returns absolute value of metric.

```
abs(query)
```

**Status:** ✅ Implemented

### log2()

Returns base-2 logarithm of metric values.

```
log2(query)
```

**Status:** ✅ Implemented

### log10()

Returns base-10 logarithm of metric values.

```
log10(query)
```

**Status:** ✅ Implemented

### cumsum()

Cumulative sum over time window.

```
cumsum(query)
```

**Status:** ✅ Implemented

### integral()

Computes the integral (area under the curve) of the metric.

```
integral(query)
```

**Status:** ✅ Implemented

---

## 3. COUNT

Functions for counting values.

### count_nonzero()

Counts non-zero values in each time interval.

```
count_nonzero(query)
```

**Status:** ✅ Implemented

### count_not_null()

Counts non-null values in each time interval.

```
count_not_null(query)
```

**Status:** ✅ Implemented

---

## 4. EXCLUSION

Functions for filtering and clamping metric values.

### exclude_null()

Removes series with null tag values from results.

```
exclude_null(query)
```

**Status:** ❌ Not Implemented

### clamp_min()

Sets a floor value - values below min become min.

```
clamp_min(query, min_value)
```

**Status:** ✅ Implemented

### clamp_max()

Sets a ceiling value - values above max become max.

```
clamp_max(query, max_value)
```

**Status:** ✅ Implemented

### cutoff_min()

Removes (nullifies) values below threshold.

```
cutoff_min(query, threshold)
```

**Status:** ✅ Implemented

### cutoff_max()

Removes (nullifies) values above threshold.

```
cutoff_max(query, threshold)
```

**Status:** ✅ Implemented

---

## 5. INTERPOLATION

Functions for handling missing data.

### fill()

Fills missing values using specified method.

```
fill(query, method)
```

**Methods:**
- `linear` - Linear interpolation between known points
- `last` - Use last known value (forward fill)
- `zero` - Fill with zeros
- `null` - No interpolation (gaps remain)

**Status:** ✅ Implemented

### default_zero()

Fills empty time intervals with zero.

```
default_zero(query)
```

**Status:** ✅ Implemented

---

## 6. RANK

Functions for selecting top/bottom N series.

### top()

Selects top N series by aggregation method.

```
top(query, n, method, order)
```

**Parameters:**
- `query` - Multi-series metric query
- `n` - Number of series to return (5, 10, 15, 20)
- `method` - Ranking method: `mean`, `min`, `max`, `last`, `area`, `l2norm`
- `order` - `asc` or `desc`

**Status:** ✅ Implemented

### top_offset()

Selects top N series after skipping offset series.

```
top_offset(query, n, method, order, offset)
```

**Status:** ✅ Implemented

### bottom()

Selects bottom N series by aggregation method.

```
bottom(query, n, method, order)
```

**Status:** ✅ Implemented

---

## 7. RATE

Functions for calculating rates and derivatives.

### diff()

Calculates the difference between consecutive points.

```
diff(query)
```

**Formula:** `diff[i] = value[i] - value[i-1]`

**Status:** ✅ Implemented

### derivative()

Calculates first derivative (rate of change).

```
derivative(query)
```

**Formula:** `derivative[i] = (value[i] - value[i-1]) / (time[i] - time[i-1])`

**Status:** ✅ Implemented

### rate()

Like derivative but skips non-monotonically increasing values (for counters).

```
rate(query)
```

**Status:** ✅ Implemented

### per_second()

Alias for `rate()`.

```
per_second(query)
```

**Status:** ✅ Implemented

### per_minute()

Rate multiplied by 60.

```
per_minute(query)
```

**Status:** ✅ Implemented

### per_hour()

Rate multiplied by 3600.

```
per_hour(query)
```

**Status:** ✅ Implemented

### dt()

Time delta between consecutive points.

```
dt(query)
```

**Status:** ✅ Implemented

### monotonic_diff()

Difference that handles counter resets (resets to 0 instead of negative).

```
monotonic_diff(query)
```

**Status:** ✅ Implemented

---

## 8. REGRESSION

Functions for trend analysis and fitting.

### trend_line()

Fits an ordinary least squares (OLS) regression line.

```
trend_line(query)
```

**Status:** ✅ Implemented

### robust_trend()

Fits a robust regression line using Huber loss (ignores outliers).

```
robust_trend(query)
```

**Status:** ✅ Implemented

### piecewise_constant()

Fits a step function with automatic breakpoint detection.

```
piecewise_constant(query)
```

**Status:** ✅ Implemented

---

## 9. ROLLUP

Functions for time aggregation.

### rollup()

Aggregates data points into time buckets.

```
rollup(query, method, interval)
```

**Parameters:**
- `method` - Aggregation: `avg`, `sum`, `min`, `max`, `count`
- `interval` - Bucket size in seconds

**Status:** ✅ Implemented (as aggregationInterval)

### moving_rollup()

Rolling window aggregation.

```
moving_rollup(query, window, method)
```

**Parameters:**
- `window` - Window size in seconds
- `method` - Aggregation method

**Status:** ✅ Implemented

---

## 10. SMOOTHING

Functions for reducing noise in metrics.

### autosmooth()

Automatically selects optimal smoothing span.

```
autosmooth(query)
```

**Status:** ✅ Implemented

### ewma()

Exponentially Weighted Moving Average.

```
ewma(query, span)
```

**Parameters:**
- `span` - Number of points for weighting (3, 5, 10, 20, or custom)

**Note:** Datadog uses `ewma_3()`, `ewma_5()`, etc. We use generic `ewma(query, span)`.

**Status:** ✅ Implemented

### median()

Median filter for noise reduction.

```
median(query, window)
```

**Parameters:**
- `window` - Window size (3, 5, 7, 9, or custom)

**Note:** Datadog uses `median_3()`, `median_5()`, etc. We use generic `median(query, window)`.

**Status:** ✅ Implemented

---

## 11. TIMESHIFT

Functions for comparing current data with historical periods.

### timeshift()

Shifts metric data by specified offset.

```
timeshift(query, offset)
```

**Parameters:**
- `offset` - Time offset as duration string (e.g., `-1h`, `-1d`, `-1w`)

**Status:** ✅ Implemented

### hour_before()

Convenience function for 1-hour shift.

```
hour_before(query)
```

Equivalent to `timeshift(query, -1h)`

**Status:** ✅ Alias via timeshift()

### day_before()

Convenience function for 1-day shift.

```
day_before(query)
```

Equivalent to `timeshift(query, -1d)`

**Status:** ✅ Alias via timeshift()

### week_before()

Convenience function for 1-week shift.

```
week_before(query)
```

Equivalent to `timeshift(query, -1w)`

**Status:** ✅ Alias via timeshift()

### month_before()

Convenience function for 1-month shift.

```
month_before(query)
```

Equivalent to `timeshift(query, -1mo)`

**Status:** ✅ Alias via timeshift()

---

## Implementation Status Summary

### ✅ Phase 1: Core Functions (Complete)
1. **Rate functions** - diff, derivative, rate, per_second/minute/hour, dt, monotonic_diff
2. **Arithmetic** - abs, log2, log10, cumsum, integral
3. **Smoothing** - ewma, median, autosmooth

### ✅ Phase 2: Analysis Functions (Complete)
4. **Timeshift** - timeshift (use for hour/day/week/month_before)
5. **Rank** - top, top_offset, bottom (with mean/min/max/last/sum/area/l2norm methods)
6. **Regression** - trend_line, robust_trend, piecewise_constant

### ✅ Phase 3: Advanced Functions (Complete)
7. **Exclusion** - clamp_min, clamp_max, cutoff_min, cutoff_max
8. **Interpolation** - fill (linear/last/zero/null), default_zero
9. **Count** - count_nonzero, count_not_null
10. **Rollup** - moving_rollup (avg/sum/min/max/count)

### ⚠️ Remaining Work
- **exclude_null()** - Series filtering by null tag values
- Full Datadog query syntax parsing integration

---

## Design Notes

### Generic vs Convenience Functions

Where Datadog uses fixed-parameter convenience functions, we prefer generic parameterized versions:

| Datadog | TSDB (Generic) |
|---------|----------------|
| `ewma_3(q)`, `ewma_5(q)`, `ewma_10(q)`, `ewma_20(q)` | `ewma(q, span)` |
| `median_3(q)`, `median_5(q)`, `median_7(q)`, `median_9(q)` | `median(q, window)` |
| `top5_mean(q)`, `top10_min(q)`, etc. | `top(q, n, method)` |
| `bottom5_mean(q)`, `bottom10_min(q)`, etc. | `bottom(q, n, method)` |
| `hour_before(q)`, `day_before(q)`, etc. | `timeshift(q, offset)` |

Convenience aliases can be added for compatibility.

### Function Composition

Functions should be composable:
```
ewma(derivative(query), 10)
top(anomalies(query, agile, 2), 5, mean)
```

### Query Syntax

Functions are applied in the query string:
```json
{
  "query": "ewma(avg:cpu.usage{host:*}, 10)",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000
}
```
