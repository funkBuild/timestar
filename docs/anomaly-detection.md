# Anomaly Detection

Detect anomalies in time series data using statistical algorithms. Used via the `anomalies()` function in derived query formulas.

## Syntax

```
anomalies(query_ref, 'algorithm', bounds[, 'seasonality'])
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query_ref` | identifier | yes | Reference to a sub-query (e.g., `a`) |
| `algorithm` | string | yes | `'basic'`, `'agile'`, or `'robust'` |
| `bounds` | number | yes | Standard deviations for threshold (typically 1-4, but any positive value is accepted) |
| `seasonality` | string | no | `'hourly'`, `'daily'`, or `'weekly'` |

## Example

```json
{
  "queries": [{"query": "avg:cpu(usage){host:server-01}", "name": "a"}],
  "formula": "anomalies(a, 'basic', 2)",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": "5m"
}
```

## Algorithms

### Basic

Rolling window anomaly detection. Computes bounds as `mean +/- (bounds * stddev)` over a sliding window.

- O(N) complexity with incremental statistics
- No seasonality support
- Best for: stationary metrics without periodic patterns

Parameters: `windowSize` (default 60 points).

### Agile

Holt-Winters triple exponential smoothing with seasonal prediction.

- Adapts quickly to level shifts while respecting seasonal patterns
- Combines recent values with historical same-period values
- Supports seasonality

Smoothing coefficients: alpha=0.3 (level), beta=0.1 (trend), gamma=0.3 (seasonality).

**Caveat:** If the data length is less than the seasonal period, seasonality is silently disabled and the detector falls back to non-seasonal Holt-Winters (effectively basic-like behavior). The warm-up period is `max(minDataPoints, seasonalPeriod)` -- points before this threshold have infinite bounds and zero scores.

Best for: metrics with seasonal patterns that may shift in level.

### Robust

STL (Seasonal-Trend decomposition using Loess) based detection.

- Decomposes series into trend + seasonal + residual
- Detects anomalies in the residual component
- Resistant to outliers with bisquare weighting
- Supports seasonality

Parameters: `stlSeasonalWindow` (default 7, must be odd; even values are rounded up), `stlRobust` (default true; enables bisquare robustness weighting in STL iterations).

**Caveat:** If the seasonal period exceeds `n/2` (half the data length), the STL decomposition silently falls back to non-seasonal mode (trend-only via moving average, with zero seasonal component). Unlike basic and agile, robust has **no `minDataPoints` warm-up** -- all points get finite bounds from the first data point onward.

Best for: stable metrics with consistent seasonal patterns.

## Seasonality

| Value | Period |
|-------|--------|
| `'hourly'` | 60-minute cycle |
| `'daily'` | 24-hour cycle |
| `'weekly'` | 7-day cycle |

Seasonality is ignored by the basic algorithm. For agile and robust algorithms, it determines the seasonal period used in decomposition.

## Output

The response contains multiple series "pieces":

| Piece | Description |
|-------|-------------|
| `raw` | Original input values |
| `upper` | Upper anomaly bound |
| `lower` | Lower anomaly bound |
| `score` | Anomaly score (0 = within bounds, higher = more anomalous) |

Score calculation:
```
score = max(0, value - upper) + max(0, lower - value)
```
- Within bounds: `0.0` (both terms are zero)
- Below lower: `lower - value` (raw deviation below the lower bound)
- Above upper: `value - upper` (raw deviation above the upper bound)

Each piece includes an `alert_value` field with the maximum anomaly score.

## Response Statistics

```json
{
  "algorithm": "basic",
  "bounds": 2.0,
  "seasonality": "none",
  "anomaly_count": 5,
  "total_points": 1000,
  "execution_time_ms": 32.1
}
```

## Minimum Data

At least `minDataPoints` (default 10) values are needed before bounds are produced. Earlier points will have infinite bounds and zero scores. This warm-up applies to **basic** and **agile** only. For agile, the effective warm-up is `max(minDataPoints, seasonalPeriod)`. The **robust** algorithm has no warm-up -- it runs STL decomposition over the entire series and produces finite bounds for all points.
