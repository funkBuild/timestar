# Expression Functions

Functions available in derived query formulas. All functions operate element-wise on aligned time series unless noted otherwise.

## Arithmetic Operators

| Operator | Description |
|----------|-------------|
| `a + b` | Addition (series + series or series + scalar) |
| `a - b` | Subtraction |
| `a * b` | Multiplication |
| `a / b` | Division (series/series: NaN for 0/0, Inf for nonzero/0; scalar divisor of 0 throws error) |
| `-a` | Unary negation |

## Math Functions

| Function | Description |
|----------|-------------|
| `abs(a)` | Absolute value |
| `log(a)` | Natural logarithm |
| `log10(a)` | Base-10 logarithm |
| `sqrt(a)` | Square root |
| `ceil(a)` | Round up to integer |
| `floor(a)` | Round down to integer |
| `pow(a, b)` | Exponentiation |
| `min(a, b)` | Element-wise minimum |
| `max(a, b)` | Element-wise maximum |
| `clamp(a, min, max)` | Clamp values to [min, max] |
| `clamp_min(a, min)` | Clamp values to minimum |
| `clamp_max(a, max)` | Clamp values to maximum |

## Transform Functions

| Function | Description |
|----------|-------------|
| `diff(a)` | Difference between consecutive points |
| `monotonic_diff(a)` | Diff with counter reset handling |
| `default_zero(a)` | Replace NaN with 0 |
| `normalize(a)` | Rescale to [0, 1]; constant series becomes 0 |
| `cutoff_min(a, threshold)` | Set values below threshold to NaN |
| `cutoff_max(a, threshold)` | Set values above threshold to NaN |

## Counter & Rate Functions

| Function | Description |
|----------|-------------|
| `rate(a)` | Per-second rate between adjacent counter readings; a decrease assumes a reset to zero and counts the new reading |
| `irate(a)` | Instantaneous rate (last two points, constant series) |
| `increase(a)` | Sum of reset-adjusted increments between adjacent readings (scalar); does not include the first reading or extrapolate beyond the samples |
| `per_minute(a, spp)` | Signed per-second difference scaled to per-minute; does not adjust counter resets |
| `per_hour(a, spp)` | Signed per-second difference scaled to per-hour; does not adjust counter resets |

`irate(a)` uses the same reset correction as `rate(a)`, but repeats the last
interval's rate across the result. It is a summary, not a rolling historical rate.
For counter flow in other units use `rate(a) * 60` or `rate(a) * 3600`.
To demonstrate the signed conversion functions on a resetting counter, first
unwrap it: `per_minute(cumsum(monotonic_diff(a)), spp)` (or `per_hour`).
The conversions use actual timestamp intervals; `spp` is retained for API compatibility.

## Counting Functions

| Function | Description |
|----------|-------------|
| `count_nonzero(a)` | Count of non-zero values (scalar) |
| `count_not_null(a)` | Count of non-NaN values (scalar) |

## Gap-Fill / Interpolation

| Function | Description |
|----------|-------------|
| `fill_forward(a)` | Last observation carried forward; leading NaNs stay NaN |
| `fill_backward(a)` | Next observation carried backward; trailing NaNs stay NaN |
| `fill_linear(a)` | Linear interpolation between known values using timestamps |
| `fill_value(a, v)` | Replace every NaN with constant `v` |

## Accumulation Functions

| Function | Description |
|----------|-------------|
| `cumsum(a)` | Running cumulative sum; NaN treated as 0 |
| `integral(a)` | Trapezoidal integration over time; NaN trapezoids contribute 0 |

## Rolling Window Functions

| Function | Description |
|----------|-------------|
| `rolling_avg(a, N)` | N-point simple moving average |
| `rolling_min(a, N)` | N-point rolling minimum |
| `rolling_max(a, N)` | N-point rolling maximum |
| `rolling_stddev(a, N)` | N-point rolling standard deviation (population) |
| `zscore(a, N)` | Rolling z-score: (value - rolling_mean) / rolling_stddev |

## Exponential Smoothing

| Function | Description |
|----------|-------------|
| `ema(a, alpha_or_span)` | Exponential moving average |
| `holt_winters(a, alpha, beta)` | Double exponential smoothing (Holt's linear method) |

`ema`: if param <= 1.0, treated as alpha directly; if > 1.0, treated as span N where alpha = 2/(N+1).

`holt_winters`: alpha controls level smoothing, beta controls trend smoothing. Both in (0, 1].

## Cross-Series Functions

| Function | Description |
|----------|-------------|
| `as_percent(a, total)` | Element-wise: 100 * a / total |
| `avg_of_series(a, b, ...)` | Element-wise mean across series |
| `sum_of_series(a, b, ...)` | Element-wise sum across series |
| `min_of_series(a, b, ...)` | Element-wise minimum across series |
| `max_of_series(a, b, ...)` | Element-wise maximum across series |
| `percentile_of_series(p, a, b, ...)` | p-th percentile across series (first arg is percentile) |
| `topk(N, a)` | Keep top-N groups by mean value |
| `bottomk(N, a)` | Keep bottom-N groups by mean value |

## Time Functions

| Function | Description |
|----------|-------------|
| `time_shift(query, 'offset')` | Shift timestamps by duration (e.g., `'7d'`, `'-1h'`) |

## Special Functions

| Function | Description |
|----------|-------------|
| `anomalies(query, 'algo', bounds[, 'seasonality'])` | Anomaly detection (see [Anomaly Detection](anomaly-detection.md)) |
| `forecast(query, 'algo', deviations[, ...])` | Forecasting (see [Forecasting](forecasting.md)) |

## NaN Handling

Most functions propagate NaN values. Notable exceptions:

- `default_zero(a)`: Replaces NaN with 0
- `fill_forward/backward/linear/value`: Replace NaN via interpolation
- `cumsum(a)`: Treats NaN as 0
- `integral(a)`: NaN trapezoids contribute 0
- `rolling_*`: NaN values within window propagate to output
- `ema/holt_winters`: NaN inputs carry forward previous state
