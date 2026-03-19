# TimeStar NaN Handling Policy

## Principle

**NaN = missing data.** A NaN floating-point value represents an absent or
invalid measurement. It is *not* a sentinel, *not* a valid numeric result, and
must never silently corrupt aggregation outputs.

---

## Rules by subsystem

### 1. Aggregation (AggregationState, Aggregator)

NaN values are **skipped** for every aggregation method:

| Method   | NaN behaviour                                      |
|----------|-----------------------------------------------------|
| SUM      | NaN values excluded from running sum                |
| AVG      | NaN values excluded from sum and count              |
| COUNT    | Only non-NaN values are counted                     |
| MIN      | NaN values excluded; min starts at +infinity        |
| MAX      | NaN values excluded; max starts at -infinity        |
| STDDEV   | NaN values excluded from variance accumulation      |
| STDVAR   | Same as STDDEV (returns variance instead of sqrt)   |
| SPREAD   | NaN values excluded from min/max tracking           |
| MEDIAN   | NaN values excluded from raw value collection       |
| LATEST   | NaN values skipped; latest non-NaN value returned   |
| FIRST    | NaN values skipped; first non-NaN value returned    |

If every input value is NaN, the aggregation result is NaN.

### 2. TSM block statistics (TSM writer)

The TSM writer **excludes NaN** when computing per-block min, max, sum, and
count statistics. A block whose values are all NaN will have count = 0 and
min/max/sum = NaN.

### 3. SIMD block aggregation (BlockAggregator)

The SIMD fold paths (Highway-accelerated SUM, MIN, MAX, COUNT, AVG) **assume
NaN-free input**. This is safe because:

- TSM blocks should not contain NaN values; the write path filters them.
- If NaN does appear (e.g., data sourced directly from a memory store before
  flush), the scalar fallback in `calculateMin` / `calculateMax` /
  `calculateSum` detects NaN and skips it.

Callers must not pass blocks known to contain NaN into the SIMD fast path
without a preceding NaN check.

### 4. Expression evaluator

Binary operations (`+`, `-`, `*`, `/`) **propagate NaN** per IEEE 754
semantics. If either operand is NaN the result is NaN.

Unary functions (`abs`, `log`, `ceil`, `floor`, `round`, `sqrt`, `exp`)
likewise propagate NaN: `f(NaN) = NaN`.

Clamping functions (`clamp_min`, `clamp_max`) use `std::fmax` / `std::fmin`
semantics after the codebase fix: NaN in the *value* operand propagates NaN;
NaN in the *bound* operand is ignored.

### 5. Streaming aggregator

- **Double values:** NaN is treated as missing and **skipped** (not added to
  sum, not counted, not compared for min/max).
- **String values:** Always counted. The concept of NaN does not apply to
  strings.

### 6. JSON output serialization

NaN values in query responses are serialized as **JSON `null`**, because JSON
has no NaN literal. Downstream clients should interpret `null` in a numeric
values array as "no data for this timestamp."

---

## Summary invariant

> At every layer of the stack, NaN means "this data point does not exist."
> Aggregations ignore it, expressions propagate it, and the API surfaces it
> as `null`.

---

## References

- `lib/query/aggregator.hpp` / `aggregator.cpp` -- scalar aggregation
- `lib/query/block_aggregator.hpp` -- SIMD block-level aggregation
- `lib/query/simd_aggregator.cpp` -- Highway SIMD fold implementations
- `lib/query/streaming_aggregator.cpp` -- streaming (SSE) aggregation
- `lib/query/expression_evaluator.cpp` -- expression evaluation
- `lib/storage/tsm_writer.cpp` -- block stat computation
- `lib/utils/json_escape.hpp` -- JSON serialization helpers
