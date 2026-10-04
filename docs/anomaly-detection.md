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

## Per-series fan-out

`anomalies()` runs **once per series** its sub-query resolves to and returns one
group per series, labelled in `group_tags`. A group's identity is its
**(tag set, field)** key — so `by {deviceId}` fans out by device, and a
multi-field leg fans out by field with a synthetic `_field=<name>` label. No
request flag is needed.

The group key, the ordering rule, the `_field=` label convention and the size
bounds are specified once in
[docs/api-derived.md](api-derived.md#per-series-fan-out).

Detection runs **independently per group**, over that group's own observations
rather than over the shared time axis it is projected onto. Groups carrying too
few observations are [declined](api-derived.md#declined-groups) and counted in
`declined_series_count` (see "Minimum Data" below for the exact rule, which
differs from `forecast()`'s).

> **Known limitation.** `AnomalyStatistics` carries `declined_series_count` but
> no counter for groups actually **emitted** — there is no anomaly analogue of
> forecast's `series_count`, so the invariant *emitted + declined = resolved*
> cannot be checked from the response alone. Count the distinct `group_tags`
> under `series` if you need the emitted figure.

`algorithm`, `bounds` and `seasonality` are **global** — all three are taken
once from the request's config and describe the whole query, not any one group.
`anomaly_count` and `total_points` are **sums** across the emitted groups.

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

State is initialized from the first finite observation. Every point is scored against the prior state and prior residual spread, then admitted into the model. Outliers are clipped to that prior envelope for learning so a spike cannot set its own threshold or drag the next prediction towards itself. Seasonal state is learned progressively; short windows no longer change initialization when extended.

Best for: metrics with seasonal patterns that may shift in level.

### Robust

Causal trailing median and median absolute deviation (MAD) detection.

- Without seasonality, the baseline and scale use the previous `max(windowSize, minDataPoints)` finite readings.
- With seasonality, the baseline uses the same phase in preceding cycles; the scale uses earlier prediction residuals.
- Each point is scored before it enters either window. Appending observations cannot revise past classifications on a fixed sampling grid.
- Isolated spikes do not anticipate neighbouring alarms. A persistent shift eventually becomes the new normal; use an explicit operating threshold when a sustained fault must stay in alarm.

`stlSeasonalWindow` is retained for compatibility and controls the number of prior cycles (default 7, minimum 3). `stlRobust` is no longer used by this detector. The standalone STL decomposition functions are unchanged.

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
  "declined_series_count": 2,
  "execution_time_ms": 32.1
}
```

`declined_series_count` is **additive**: omitted from the JSON when zero, and
(as a proto3 scalar) likewise absent from the protobuf wire format. Absent means
none. `total_points` counts the **real** observations across the answered
groups, not the width of the shared axis — under fan-out a group carries `null`
wherever it has no sample, and those slots are not its data.

## Minimum Data

All three detectors wait for `minDataPoints` (default 10) finite previous observations. Warm-up bounds are infinite (serialized as null), with zero scores. Missing/non-finite readings do not advance warm-up or contaminate the state. Robust seasonal detection additionally needs two prior observations of the current phase; this avoids treating a newly seen seasonal phase as an anomaly.

Under grouped queries, a sparse series with fewer than `minDataPoints` real observations on a longer shared axis is declined and counted in `declined_series_count`. Short series remain returnable with their raw readings and warm-up bounds. See [declined groups](api-derived.md#declined-groups).
