# Stack-level property fuzzer — design

## Why

`test/unit/encoding/decoder_mutation_fuzz_test.cpp` already fuzzes one axis:
**malformed bytes into a decoder**, asserting it neither crashes nor invents
data. That is the "unvalidated count sizes an allocation" bug class.

Every bug found in the July 20 session was a *different* class, and none of them
required malformed input. They were all **well-formed data returning a wrong
answer**, where the answer depended on something the caller never asked about:

| Bug | What varied | Symptom |
|---|---|---|
| `StringEncoder::decode` cleared a shared vector | number of **blocks** in the series | 3600 timestamps / 600 values → OOB read → empty strings on a 200, then a segfault |
| FFOR filtered decode did not clamp | requested count vs **FFOR group size** (1024) | phantom points extrapolating the series; inflated count desyncs the value decoder |
| bucketed non-numeric assembly unbounded | **memory pressure** | `bad_alloc` on a query whose output was three values |

The shared shape: **the answer changed with the plan, the placement, or the
block layout — none of which are part of the query.** A hand-written test picks
one layout and passes forever. The existing mutation fuzzer never sees these,
because it never runs a query.

So this fuzzer targets: *well-formed data, full stack, answer must not depend on
anything the caller did not ask about.*

## Shape

Two assertion strategies, both driven by one generator.

### 1. Metamorphic properties (no oracle)

Cheap, and they carry most of the value — each of the three bugs above violates
at least one. No reference implementation to get wrong.

| ID | Property | Catches |
|---|---|---|
| **P1** | **Placement invariance.** Same query before flush, after flush, after compaction → same answer. | string-decoder bug (multi-block only appears after flush) |
| **P2** | **Length coherence.** For every returned field, `\|timestamps\| == \|values\|`. | the desync itself, at the API boundary |
| **P3** | **Range decomposition.** Raw read `[a,c]` == raw `[a,b]` ++ raw `(b,c]`. | plan divergence between whole-range and split reads |
| **P4** | **Bucket well-formedness.** Bucket starts epoch-aligned, strictly ascending, no duplicates, all within `[start,end]`, count ≤ `ceil(range/interval)+1`. | bucketed-reduction merge errors |
| **P5** | **Type preservation.** Strings/bools return as strings/bools, values byte-identical to what was written. | non-numeric folded through a numeric path |
| **P6** | **Idempotence.** Same query twice → identical bytes. | cache/state contamination |
| **P7** | **Range monotonicity.** Widening the range never drops a point the narrower range returned. | early-termination bugs |
| **P8** | **No phantom timestamps.** Every returned timestamp was actually written. | any decode that emits points past the encoded run |
| **P9** | **LWW.** After rewriting a timestamp, only the newest value is ever visible; aggregates count it once. | dedup/merge regressions |

P1+P2 catch the string bug — **verified** by reverting that fix and watching the
fuzzer fail at `type=string points=3001`, the first multi-block string series.

**P3+P8 do NOT catch the FFOR over-read, and the original version of this
document was wrong to claim they would.** That claim was asserted from the shape
of the bug without checking whether the stack can *reach* it. It cannot: in the
TSM read path `timestampSize` comes from the block header and therefore always
equals the encoded count, so a query never requests fewer values than were
encoded. Reverting the clamp and running the stack fuzzer confirms it passes.

The bug is reachable by any caller that supplies its own count — the WAL path,
the protobuf ingest path (which derives a *bound* from payload length, not a
count), and future code. So that invariant belongs at the **decoder API**, and
is covered by `DecoderNeverEmitsMoreThanRequested` in the same file, which
rediscovers it in two iterations (`encoded=1256 requested=1017 filtered=yes ->
emitted 1024`).

The lesson generalises: **a property only catches a bug the generator can
actually reach.** Reachability has to be demonstrated by reverting the fix, not
argued from the bug's shape.

**Float caveat (important):** P1 must NOT demand bit-equality for `sum`/`avg`.
Different placements legitimately fold in different orders (SIMD vs scalar,
block-stats pushdown vs raw), and IEEE addition is not associative. Use a
relative-ULP tolerance for `sum`/`avg`/`stddev`/`stdvar`/`spread`, and **exact**
equality for `min`/`max`/`count`/`latest`/`first` — those must be bit-identical,
including `-0.0` and NaN handling per `docs/nan_policy.md`. Getting this
backwards produces either false alarms or a blind spot.

### 2. Oracle differential

A deliberately dumb reference model: `map<seriesKey, map<timestamp, value>>`,
last write wins. Correct by inspection because it has no encoding, no blocks, no
placement. Query results are recomputed from it using the documented semantics
(epoch-aligned buckets, NaN-skipped aggregation, non-numeric LATEST-per-bucket).

Risk: the oracle re-states semantics and can itself be wrong — a disagreement
means "one of these two is wrong", not "the DB is wrong". Mitigation: keep it
under ~150 lines, no optimisation, and treat CLAUDE.md as the spec. The
metamorphic properties stay the primary signal precisely because they need no
oracle.

## Generator

Seeded (`TIMESTAR_FUZZ_SEED`, else fixed) so every failure is replayable.
Dimensions chosen because they are where the bugs actually lived:

1. **Schema** — series count, tag cardinality, field types (float / int64 / bool
   / string).
2. **String cardinality** — low (≤50 uniques → STR2 dictionary path) and high
   (→ raw STRG path). *The dictionary/raw split is a real branch in
   `decodeBlockFlat`; both must be exercised.*
3. **Point counts straddling block edges** — derived at runtime from
   `MaxPointsPerBlock()` (config, default **3000**) and
   `IntegerEncoderFFOR::kBlockSize` (**1024**), never hard-coded. *An earlier
   revision guessed 1024 as the block size, so the largest generated series was
   exactly one block and the multi-block string bug was literally unreachable.*
4. **Timestamps** — regular, irregular, clustered, sparse, out-of-order,
   duplicates (LWW), and runs that cross bucket boundaries.
5. **Values** — NaN, ±Inf, `-0.0`, denormals, empty strings, UTF-8, long
   strings, values that compress well vs not at all.
6. **Ingest path** — JSON, JSON with explicit `field_types`, protobuf, single
   vs batch.
7. **Placement** — random flush/rollover/compaction points *between* writes, so
   a series ends up split across memstore + several TSM generations.
8. **Queries** — method × interval (0, aligned, misaligned, larger than range) ×
   group-by (present/absent/unknown key) × range (inside, spanning, disjoint,
   boundary-exact, single-point).

## Shrinking (phase 2 — implemented)

Non-negotiable. A failure over 3000 random points is not actionable.

Everything the generator decides lives in a `Workload` struct, and
`materialize()` is a pure function of it — that is what makes minimisation
possible at all. On failure the shrinker simplifies one field at a time (binary
search on `count`, then irregular→regular spacing, step→1s, string
cardinality→1, values→simplest) and re-runs, keeping any change that still
fails. It then emits a paste-ready reproducer plus the seed.

Two things this got wrong first, both worth keeping:

**Shrink must preserve the SAME failure.** The first version accepted *any*
failure, walked from a real multi-block bug into an unrelated one, and reported a
"minimal" case that never demonstrated the original problem. Candidates are now
compared by failure *signature* (the message with digit runs collapsed, since
counts legitimately change as the case shrinks).

**Runs must not reuse a measurement name within a process.** Cleaning the shard
directories is NOT sufficient isolation: recreating an engine over wiped
directories leaves process-level state that makes a previously-seen measurement
resolve to nothing. Measured directly — one name reused across 12 engine
instances failed 11 times; unique names failed 0 times. Phase 1 accidentally
avoided this because each iteration used a different measurement; the shrinker
re-runs the same index, so every probe after the first came back empty and the
shrinker read that as "still failing". Each run now appends a process-unique id.

A worked result, with the string-decoder fix reverted:

```
original: type=string count=3001 stepSec=3 irregular=no strCard=35 valueSeed=20260755
shrunk:   type=string count=3001 stepSec=1 irregular=no strCard=1  valueSeed=0
```

`count` is irreducible at 3001 — binary search proves 3000 and below pass — and
3001 is `MaxPointsPerBlock() + 1`, the first two-block series. **The minimum is
the diagnosis.**

## Execution

- **In-process** (`ScopedShardedEngine`, no HTTP) for the core loop — fast, so
  iteration counts can be high.
- **HTTP-level** thin variant to cover JSON/protobuf parse + response
  serialisation, which the in-process path skips.
- **ASAN.** The string bug was an out-of-bounds *read*; without ASAN it silently
  returned empty strings and only sometimes crashed. The fuzzer must run under
  `build-asan/` in CI or it will miss exactly the class it exists for.
- **Budget:** iteration count from env. CI: fixed seeds, ~30s. Nightly: random
  seeds, long run, failing seed reported.

## Functions / derived

Same generator, separate assertions, since `/derived` is numeric-only:

- Non-numeric operand → rejected, never coerced (booleans must not become
  `1.0`/`0.0` — a fixed bug worth pinning).
- NaN = missing, per `docs/nan_policy.md`.
- Placement invariance (P1) again — derived runs sub-queries, so it inherits
  every plan-dependence bug.
- Transform functions: no crash, output length == input length where the
  function is elementwise, NaN propagation matches the documented rule.

## Phasing

1. Generator + P1/P2/P3/P8 in-process. *Smallest thing that reproduces all three
   July 20 bugs — validate by reverting each fix and confirming the fuzzer
   fails.*
2. Shrinker + reproducer emission.
3. Remaining properties (P4–P7, P9), oracle differential.
4. HTTP/protobuf variant, derived/functions.
5. CI wiring: fixed-seed short run per PR, nightly long random run.

**Acceptance test for the fuzzer itself:** revert each fix in turn; the fuzzer
must fail, naming a replayable seed. A fuzzer that cannot rediscover known bugs
is decoration.

Results of running that acceptance test (phase 1):

| Fix reverted | Caught by | Notes |
|---|---|---|
| string decoder `clear()` | `WellFormedData...` (P1) | fails at `type=string points=3001` — the first multi-block string series |
| FFOR filtered clamp | `DecoderNeverEmitsMoreThanRequested` | NOT reachable via the query stack; see above |
| unbounded assembly (`bad_alloc`) | **neither** | needs a memory-constrained shard, which an in-process unit test cannot arrange. Covered instead by `nonnumeric_chunked_recovery_test.cpp`, which calls the recovery path directly. Closing this properly needs an allocation-failure injection hook — phase 4. |

Two things the acceptance test exposed that are worth keeping in mind:

1. **Derive boundaries from the code, never guess them.** The first version
   hard-coded 1024 as the block size; the real `MaxPointsPerBlock()` is 3000, so
   the largest generated series was exactly one block and the multi-block bug was
   unreachable. The generator now reads both constants at runtime.
2. **Assert the precondition, or the property is vacuous.** Iterations shared
   shard directories, so `flushToTsm` returned immediately on a previous
   iteration's file and P1 compared memstore against memstore — passing no matter
   what the TSM path did. Iterations are now isolated and the flush is asserted
   to have landed.
