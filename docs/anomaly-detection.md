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

At least `minDataPoints` (default 10) values are needed before bounds are produced. Earlier points will have infinite bounds and zero scores. This warm-up applies to **basic** and **agile** only. For agile, the effective warm-up is `max(minDataPoints, seasonalPeriod)`. The **robust** algorithm has no warm-up -- it runs STL decomposition over the entire series and produces finite bounds for all points.

A **sparse** series carrying fewer than `minDataPoints` **observations** is not warmed up at all -- it is **declined**, and reported in `declined_series_count` rather than returned. The warm-up above is indexed by slot, which is only equivalent to counting observations when every slot holds one; under the per-group fan-out of `anomalies()` a group carries `null` wherever it has no sample, so a device with a single observation would otherwise leave warm-up and be given a full confidence envelope built from that one sample. See "Declined groups" in [docs/api-derived.md](api-derived.md).

The decline applies only to a series **longer than** `minDataPoints`. A series at or below that length is always answered, however many of its slots are `null` -- a series that short cannot leave a slot-indexed warm-up at all, so for **basic** and **agile** there is nothing there to fabricate, and declining it would only throw away the stored values under `raw` and `predictions`. Both halves of the rule matter: judging a series on the observations it really holds is the point, and not applying that judgement below the warm-up length is what keeps a genuinely short series returnable.

For **robust** the decline effectively never fires on such a series: robust has no warm-up (see above), so it produces a finite envelope at every slot however short the input is -- a dense 3-point robust series comes back with a `±0.36` band, not an all-`null` one. What the length arm of the rule preserves for robust is simply the answer it gave before per-group fan-out existed.

`forecast()` and `anomalies()` **disagree** about a short dense series, and that is deliberate and pre-existing: a 5-point series is answered by `anomalies()` (its stored values are returned; only the envelope is empty) and declined by `forecast()` (a fit under `minDataPoints` has no values to lose in the first place -- see [docs/forecasting.md](forecasting.md), "Minimum Data"). Both halves are preserving the behaviour they had before the fan-out work; neither was unified with the other.
