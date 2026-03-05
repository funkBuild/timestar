# Forecasting

Predict future time series values using linear regression or seasonal decomposition. Used via the `forecast()` function in derived query formulas.

## Syntax

```
forecast(query_ref, 'algorithm', deviations[, seasonality='...'][, model='...'][, history='...'])
```

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `query_ref` | identifier | yes | Reference to a sub-query (e.g., `a`) |
| `algorithm` | string | yes | `'linear'` or `'seasonal'` |
| `deviations` | number | yes | Confidence interval width in std deviations (1-4) |
| `seasonality` | string | no | `'none'`, `'hourly'`, `'daily'`, `'weekly'`, `'auto'`, `'multi'` |
| `model` | string | no | Linear model type: `'default'`, `'simple'`, `'reactive'` |
| `history` | string | no | History window: `'1w'`, `'3d'`, `'12h'` |

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

> **Note on `r_squared`:** For the seasonal algorithm, R-squared is computed as `1 - (AR_residual_variance / total_variance)` and clamped to a minimum of 0. Because the AR residual variance is measured on differenced data, this is an approximation of goodness-of-fit rather than a true in-sample R-squared.

### Seasonal

SARIMA-based seasonal forecasting with STL (Seasonal-Trend Loess) decomposition.

- Decomposes into trend + seasonal + residual components
- Extrapolates trend and repeats seasonal pattern
- SARIMA parameters (defaults): p=2, d=1, q=2, P=1, D=1, Q=1. The AR order (`arOrder`), MA order (`maOrder`), seasonal AR order (`seasonalArOrder`), and seasonal MA order (`seasonalMaOrder`) are configurable in the forecast config.
- Seasonal damping: extrapolated seasonal components are damped by 1% per cycle into the future, floored at 50% amplitude. This prevents overly confident repetition of seasonal patterns at long horizons.

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

## Forecast Horizon

When `forecastHorizon` is 0 (default), the system auto-computes it as:

```
horizon = min(max(N / 5, 50), 2000)
```

where N is the number of historical points. The minimum of 50 ensures short series still produce meaningful forecasts; the cap of 2000 prevents excessive computation on large datasets.

## Auto-Windowing

Before expensive computation, the system automatically trims old data:

- Keeps `maxHistoryCycles` (default 4) worth of the largest detected period
- Only trims if saving >33% of data
- Respects `minDataPoints` (default 10)

This optimization prevents reactor blocking on large historical datasets.

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
