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

The TSM writer **excludes NaN** when computing per-block min, max, sum,
count, and M2 statistics. `blockCount` is the number of **non-NaN** values —
this is what makes COUNT/AVG stats pushdown identical to the scalar
NaN-skipping fold (placement-independent results). Blocks containing NaN are
written with NaN sentinels in M2/firstValue/latestValue; the reader derives
`hasExtendedStats = false` from NaN endpoints, so STDDEV/LATEST/FIRST
shortcuts decode and skip per value. A block whose values are all NaN has
count = 0 (stats pushdown disabled) and min/max = NaN.

The block header carries the true total point count for decoding; the
COUNT-only read path (`decodeBlockCountOnly`) compares it against
`blockCount` and falls back to a full value decode when they differ (i.e.
the block carries NaN). Legacy files written before this rule stored the raw
total in `blockCount` and keep NaN-counting pushdown behaviour until
compaction rewrites them.

The in-memory-store running stats (`InMemorySeriesStats`) follow the same
rules: NaN is excluded from sum/min/max/count/mean/M2 and from first/latest
value tracking.

### 3. SIMD block aggregation (BlockAggregator)

Decoded block data CAN contain NaN (NaN round-trips storage verbatim; the
write path does **not** filter it). The SIMD fold paths (Highway-accelerated
SUM, MIN, MAX, COUNT, AVG, STDDEV) detect NaN cheaply and fall back to the
scalar NaN-skipping fold:

- Entry prefilter: first element NaN → scalar fold for the whole batch.
- SUM/AVG/STDDEV/COUNT: a NaN SIMD sum (interior NaN, or an Inf/-Inf mix)
  → scalar fold.
- MIN/MAX/SPREAD: the NaN-skipping kernels return NaN only when no valid
  value exists → scalar fold.
- LATEST: NaN last element → scalar fold (FIRST is covered by the prefilter).

For MIN/MAX/SPREAD/LATEST/FIRST fast paths, interior NaN may still be
included in `state.count`; count is not part of those methods' results (it
only gates emptiness), so this is unobservable.

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

## Other IEEE-754 special values

**±Infinity is valid data**, not missing data:

- Raw reads return Inf exactly (the ALP encoder stores NaN/±Inf/-0.0 as
  raw-bit exceptions — bit-exact round-trip).
- Aggregations let Inf participate arithmetically: SUM/AVG propagate it
  (`+Inf + -Inf = NaN` per IEEE 754 is the correct aggregate), MIN/MAX order
  it, COUNT counts it, STDDEV/STDVAR of data containing Inf is NaN.
- Min/max identities are ±infinity (never DBL_MAX/DBL_LOWEST) so all-(+Inf)
  data yields min = +Inf; emptiness is signalled by count == 0.
- Kahan-compensated sums guard the compensation term: once the running sum
  is non-finite the term is reset to 0.0, otherwise `(Inf - Inf) - y = NaN`
  would silently corrupt every later Inf sum into NaN.

**-0.0 round-trips raw reads bit-exactly** (ALP exception path). Aggregated
results may normalize -0.0 to +0.0 (IEEE addition: `-0.0 + (+0.0) = +0.0`;
min/max comparisons do not distinguish the zeros). This is documented
encoding/aggregation semantics, not a bug.

## Summary invariant

> At every layer of the stack, NaN means "this data point does not exist."
> Aggregations ignore it, expressions propagate it, and the API surfaces it
> as `null`. ±Inf and -0.0 are real data: they round-trip storage exactly,
> and ±Inf participates arithmetically in aggregation.

---

## References

- `lib/query/aggregator.hpp` / `aggregator.cpp` -- scalar aggregation
- `lib/query/block_aggregator.hpp` -- SIMD block-level aggregation
- `lib/query/simd_aggregator.cpp` -- Highway SIMD fold implementations
- `lib/query/streaming_aggregator.cpp` -- streaming (SSE) aggregation
- `lib/query/expression_evaluator.cpp` -- expression evaluation
- `lib/storage/tsm_writer.cpp` -- block stat computation
- `lib/utils/json_escape.hpp` -- JSON serialization helpers
