# Forecasting

Predict future time series values using linear regression or seasonal decomposition. Used via the `forecast()` function in derived query formulas.

## Syntax

```
forecast(query_ref, 'algorithm', deviations[, seasonality='...'][, model='...'][, history='...'][, horizon='...'])
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query_ref` | identifier | yes | Reference to a sub-query (e.g., `a`) |
| `algorithm` | string | yes | `'linear'` or `'seasonal'` |
| `deviations` | number | yes | Confidence interval width in std deviations (1-4) |
| `seasonality` | string | no | `'none'`, `'hourly'`, `'daily'`, `'weekly'`, `'auto'`, `'multi'` |
| `model` | string | no | Linear model type: `'default'`, `'simple'`, `'reactive'` |
| `history` | string | no | Training history ending at the query end: `'1w'`, `'3d'`, `'12h'`; fetched even outside the visible window |
| `horizon` | string | no | Future duration: `'1d'`, `'6h'`; defaults to the requested query duration |

## Examples

**Linear forecast:**
```json
{
  "queries": [{"query": "avg:temperature(value){location:us-west}", "name": "a"}],
  "formula": "forecast(a, 'linear', 2)",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": "5m"
}
```

**Seasonal forecast with auto-detection:**
```json
{
  "queries": [{"query": "avg:cpu(usage){host:server-01}", "name": "a"}],
  "formula": "forecast(a, 'seasonal', 2, seasonality='auto')",
  "startTime": 1704067200000000000,
  "endTime": 1704153600000000000,
  "aggregationInterval": "5m"
}
```

## Per-series fan-out

`forecast()` runs **once per series** its sub-query resolves to and returns one
group per series, labelled in `group_tags`. A group's identity is its
**(tag set, field)** key, so both `by {deviceId}` (two devices → two groups) and
a multi-field leg (`(f1,f2,f3)` → three groups, labelled `_field=f1` and so on)
fan out. No request flag is needed.

The group key, the ordering rule, the `_field=` label convention and the three
size bounds are specified once in
[docs/api-derived.md](api-derived.md#per-series-fan-out). This page covers only
what is specific to fitting.

Each group is fitted **independently** — its own slope, intercept and residual
band — over its **own** observations, not over the shared time axis it is
projected onto. A device that stopped reporting is therefore not credited with
the other devices' timestamps, and a group with too little data of its own is
[declined](api-derived.md#declined-groups) rather than answered with a
fabricated fit.

## Algorithms

### Linear

Least-squares linear regression extrapolation with three model variants:

| Model | Description |
|-------|-------------|
| `default` | Standard least-squares regression |
| `simple` | Uniform weighting on last half; less sensitive to recent changes |
| `reactive` | Exponential decay weighting; more sensitive to recent changes |

Confidence intervals are computed from residual standard deviation.

Output statistics include: `slope`, `intercept`, `r_squared`, `residual_std_dev`.

> **Note on `r_squared`:** Seasonal R² compares one-step residual SSE with original-scale SST over the same fitted observations. It is an in-sample statistic, can be negative, and does not measure multi-step forecast accuracy. Evaluate held-out periods and compare with a seasonal-naive baseline.

### Seasonal

The fixed-period model uses multiplicative seasonal autoregression, with default ordinary AR order `p=2` and seasonal AR order `P=1`. It fits these coefficients with Yule-Walker equations. This is an autoregressive approximation, not a full maximum-likelihood ARIMA implementation; MA terms are not fitted.

When sufficient seasonal history is available, one seasonal difference (`D=1`, `d=0`) removes the repeating cycle and the model estimates the mean cycle-to-cycle change. Without seasonal differencing, one regular difference (`d=1`, `D=0`) is used. An additional regular difference is not applied unconditionally to seasonal data: doing so turns recent cycle noise into accumulating forecast drift. Series requiring higher-order differencing or structural-change modelling need a different model or preprocessing.

The separate `forecastMSTL` path decomposes multiple seasonal components and extrapolates the trend. Its seasonal components are damped by 1% per forecast cycle, with a 50% floor. This damping does not apply to the fixed-period autoregressive model.

Best for: metrics with strong periodic patterns.

## Seasonality Options

| Value | Description |
|-------|-------------|
| `'none'` | Disable seasonal decomposition; use trend-only forecasting |
| `'hourly'` | Fixed 1-hour cycle |
| `'daily'` | Fixed 24-hour cycle |
| `'weekly'` | Fixed 7-day cycle |
| `'auto'` | Auto-detect single best period via FFT + ACF |
| `'multi'` | Auto-detect and combine multiple periods (MSTL) |

### Auto-Detection

Periodicity detection uses a hybrid FFT + ACF approach:

1. Detrend data (remove linear trend)
2. Apply Hann window to reduce spectral leakage
3. Compute power spectrum via DFT
4. Find peaks above noise threshold (using MAD)
5. Validate peaks using autocorrelation
6. Return top periods sorted by confidence

Parameters: `minPeriod` (default 4), `maxPeriod` (default n/2), `maxSeasonalComponents` (default 3), `seasonalThreshold` (default 0.2).

## Seasonal reliability

The ordinary and seasonal AR polynomials are multiplied, including their cross terms. Inverse seasonal differencing feeds each forecast cycle into the next. Prediction variance is the innovation variance times the cumulative squared impulse response of the combined AR and differencing operators. This propagates uncertainty on the original scale; it does not include parameter-estimation uncertainty. Numerically invalid fits are declined instead of emitting non-finite predictions or a misleading narrow band.

For the mathematical convention, see [seasonal ARIMA](https://otexts.com/fpp3/seasonal-arima.html) and [ARMA impulse responses](https://www.statsmodels.org/stable/generated/statsmodels.tsa.arima_process.arma_impulse_response.html).

## Forecast Horizon

Use `forecast(a, 'seasonal', 2, seasonality='weekly', history='8w', horizon='2d')` to train on eight weeks and predict two days, independently of the dashboard window. Both durations must be positive and representable in nanoseconds. The horizon must span at least one sample interval and uses whole forecast steps. It starts after the final observed timestamp, not the wall clock. Omitting `horizon` preserves the existing query-duration behavior below. Input and output limits still apply.

When `forecastHorizon` is 0 (default), the system auto-computes it as:

```
horizon = min(max(N / 5, 50), 2000)
```

where N is the number of historical points. The minimum of 50 ensures short series still produce meaningful forecasts; the cap of 2000 prevents excessive computation on large datasets.

`POST /derived`'s `forecast()` normally **overrides** that auto horizon, projecting the forecast to the end of the requested window instead:

```
horizon = duration / interval
```

where `interval` is the leg's own sampling interval. The horizon is **never truncated**. Whenever the data spans the window (the ordinary case: 120 daily buckets over 120 days, 1440 one-minute buckets over a day) `duration / interval` is just the axis length; when the window is wider than the data's span the projection is correspondingly longer, and that is answered in full.

The override is **conditional**. `duration / interval` is computed only when the requested window is non-empty (`endTime > startTime`) **and** the leg's shared time axis carries at least **two** timestamps yielding a non-zero interval. Miss either condition and `forecastHorizon` stays 0, so `ForecastExecutor::resolveHorizon` falls back to the auto formula above.

In practice that fallback does not surface in a successful `/derived` forecast: a leg with fewer than two distinct timestamps on its axis is refused first, with

```json
{"status": "error",
 "error": {"message": "Insufficient data: at least 2 historical points are required to compute a forecast interval"}}
```

Verified against a live server for all three ways of reaching it — a one-point leg, a leg whose points all share one timestamp (last-write-wins collapses them), and a leg whose points all fall in a single bucket under the `aggregationInterval`. An empty window (`endTime <= startTime`) likewise matches no data. The fallback is nonetheless real, so a future change to either guard would expose the auto horizon on this path.

What is bounded is the **size of the result**, not the horizon. A `forecast()` leg is refused with HTTP 400 when

```
series x (historical points + horizon)  >  500,000
```

This is what stops a leg whose data covers only a **sliver** of its window from extrapolating without limit: 60 points at one-second spacing inside a 30-day window asks for 2,592,000 forecast points -- a measured **~200 MB** synchronous response built from 60 observations, with reactor stalls up to 442 ms -- and is refused, naming the numbers and suggesting a coarser resolution or a narrower window. Unlike the per-leg cell bound (see "Fan-out limits" in [docs/api-derived.md](api-derived.md)) this one applies to a single-series leg too, because what it counts is fabricated forecast slots rather than stored data.

> The body size above is **approximate and fixture-specific**. An independent reproduction of the same 60-points-at-1s-in-30-days shape measured **122.19 MB**. Each of the 2.59M forecast slots is rendered as text, so the total tracks the decimal width of the stored values. The **point count** -- which is what the bound actually counts -- does not vary.

The budget is **twice** the cell bound's, not equal to it. For a leg that spans its window `horizon ≈ N`, so `series × (N + horizon)` is about twice `series × N`; at equal constants the output bound would be strictly tighter than the cell bound on every forecast and would start refusing shapes the cell bound was calibrated to admit.

The arithmetic that decides the bound **saturates rather than wrapping**: the
horizon is `duration / interval` with nothing upstream bounding it, so 15 points
one nanosecond apart inside the widest window `uint64` nanoseconds can express
gives a horizon of 2^64 − 1, and a wrapping `historical + horizon` would come out
as 14 — comfortably inside the budget. That input is reachable over plain JSON,
not just protobuf: `POST /write` parses with `glz::generic_u64` precisely so
nanosecond timestamps keep full `uint64` precision.

> **Known limitation (pre-existing).** `ForecastExecutor::generateForecastTimestamps`
> can still wrap `uint64` for a window ending near 2^64. The output bound
> narrows the reachable range but does not close it.

Refusing rather than clamping is deliberate: a fixed horizon ceiling cannot tell an ordinary long projection from an absurd one, so it silently shortens legitimate forecasts. 1440 one-minute points projected over a 30-day window is 43,200 forecast points (44,640 output points -- comfortably inside the bound) and is answered in full; a horizon ceiling of 2,000 would have returned 33 hours of a 30-day request with nothing in the response to say so. **Answer, or refuse with a reason.**

## Auto-Windowing

Before expensive computation, the system automatically trims old data:

- Keeps `maxHistoryCycles` (default 4) worth of the largest detected period
- Only trims if saving >33% of data
- Respects `minDataPoints` (default 10)

This optimization prevents reactor blocking on large historical datasets.

> **Known limitation under fan-out.** `ForecastExecutor::executeMulti` sizes the
> trim from the **first** series only (`seriesValues[0]`) and applies it to
> every group. This is unreachable from `POST /derived`, which never sets
> `forecastSeasonality` and so never takes the auto-windowing path at all, but
> it is a latent asymmetry for any future caller that does.

## Output

The response contains multiple series "pieces":

| Piece | Description |
|-------|-------------|
| `past` | Historical values (null for forecast period) |
| `forecast` | Predicted values (null for historical period) |
| `upper` | Upper confidence bound (forecast period only) |
| `lower` | Lower confidence bound (forecast period only) |

The `forecast_start_index` field indicates where the forecast begins in the `times` array.

## Response Statistics

```json
{
  "algorithm": "linear",
  "deviations": 2.0,
  "seasonality": "auto",
  "detected_periods": [288],
  "slope": 0.15,
  "intercept": 23.0,
  "r_squared": 0.89,
  "residual_std_dev": 2.5,
  "historical_points": 500,
  "forecast_points": 100,
  "original_points": 2000,
  "windowed_points": 500,
  "series_count": 1,
  "execution_time_ms": 28.5
}
```

`series_count` is the number of groups actually **emitted** under `series`, not
the number the sub-query resolved to. Groups that were declined for want of
data appear in `declined_series_count` instead (omitted when zero); the two sum
to the resolved group count. See "Declined groups" in
[docs/api-derived.md](api-derived.md) for when a group is declined.

> **Known limitation — the fit statistics describe only the FIRST group.**
> Under fan-out, `slope`, `intercept`, `r_squared` and `residual_std_dev` are
> those of the **first emitted group** — first in the
> [fan-out order](api-derived.md#ordering), i.e. lowest tag set — and nothing in
> the response says which group that is. Verified live on a leg whose two
> groups have slopes of **+1** and **−500**: `series_count` is 2 and the
> response reports a single `"slope": -500`, the value belonging to whichever
> group sorts first. Treat these four as meaningful only when `series_count` is
> 1; the per-group values are not currently exposed.

## Minimum Data

The linear model refuses to publish a fit it cannot put an honest error bar
around:

- fewer than `minDataPoints` (default 10) **finite** values — counted over the
  group's own samples, not over the shared axis a fan-out projects it onto — is
  declined;
- fewer than **three** points once the model has selected its data is declined
  even if `minDataPoints` is lowered. A 2-parameter model has `k - 2` residual
  degrees of freedom, so at `k == 2` the line passes exactly through both points
  and the prediction interval collapses to zero width. A zero-width band is
  indistinguishable, at the wire, from a band the model is confident about.
