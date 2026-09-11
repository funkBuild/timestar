# Downsampling cascade plan

> Historical design/performance record. The September 2026 correctness fixes
> replace the value-only fold with persisted V4 rollup state and add reads for
> partially aged sweep candidates. The unweighted-average decision and the
> run-batched-fold performance figures below do not describe the current path.
> See [Retention API](api-retention.md) for current semantics and compatibility.


**Status:** IMPLEMENTED — all four phases built and adversarially reviewed.
The body below is the original proposal, kept as written for the record; where
the shipped code diverges from it, "Deviations from the plan as built" at the
end of this document is authoritative. User-facing behaviour is specified in
`docs/api-retention.md`.

**Requirement:** SCADA ingest at ~1 Hz. Per measurement: roll up to 1-minute
resolution after age A1, then to 15-minute resolution after age A2 > A1,
optionally expire entirely after a TTL. This is an ordered cascade of
downsample stages, not the single stage the current schema models.

## Objective

Make the existing per-measurement downsampling feature actually run in the
server, then extend it from one stage to an ordered tier list, with:

- a fold that is idempotent under repeated compaction of the same stage;
- defined (and honest) semantics for stage-over-stage aggregation;
- an age-driven trigger so quiet series still get folded;
- no violation of the canonical invariants: NaN-as-missing
  (docs/nan_policy.md), last-write-wins duplicates, the block
  decode-count contract, and query-shape independence from data placement.

## Current state

### What exists and works

- **API**: `PUT/GET/DELETE /retention`
  (`lib/http/http_retention_handler.cpp`; JSON and protobuf,
  `proto/timestar.proto:275-308`; docs/api-retention.md). Policies persist in
  NativeIndex under key prefix `0x0B` and are broadcast to every shard's
  `Engine::_retentionPolicies` cache on PUT
  (`http_retention_handler.cpp:190-198`) and at startup
  (`Engine::loadAndBroadcastRetentionPolicies()`, `lib/core/engine.cpp:1067`).
- **Schema**: `RetentionPolicy{measurement, ttl, ttlNanos,
  optional<DownsamplePolicy>}` with `DownsamplePolicy{after, interval, method}`
  (`lib/retention/retention_policy.hpp:8-28`). One tier, one method, all fields.
- **Fold logic**: `TSMCompactor::processSeriesForCompaction()` partitions each
  series at `downsampleThreshold`; older points fold into `intervalNanos`
  buckets through `timestar::AggregationState`, newer points pass through.
  Two implementations: a streaming fold that never materialises the whole
  series (`lib/storage/tsm_compactor.cpp:361-423`, buckets flushed at
  `:820-824`) and a buffered fallback for sink-less callers (`:837-894`).
  Per-series retention context is built from the policy map + a
  `SeriesId128 -> measurement` map inside `compact()` (`:1018-1068`).
  Per-point TTL trimming shares the same context (`:507-510`) and applies to
  all value types; whole-block TTL elimination at `:212-224`.
- **Unit tests**: `test/unit/storage/tsm_compaction_retention_test.cpp` covers
  the fold thoroughly — at the compactor level.

### The live production defect: the fold is never invoked

`TSMCompactor::setRetentionContext()` (`lib/storage/tsm_compactor.hpp:199`),
whose own comment says "Called by Engine before triggering compaction", has
**zero callers outside the unit test**. The production compaction path is:

```text
TSMFileManager::tierCompactionLoop (tsm_file_manager.cpp:458)
  -> compactOneTier (:277)
  -> TSMCompactor::executeCompaction (tsm_compactor.cpp:1310)
  -> std::exchange(_pendingRetentionPolicies, {})   (:1352)  <- ALWAYS EMPTY
  -> compact(files, targetTier, targetSeq, {}, {})
```

`Engine::_retentionPolicies` is consumed on the data path by exactly one
thing: `Engine::sweepExpiredFiles()` (`lib/core/engine.cpp:1125`), which only
deletes **whole files** whose every series is fully expired. Consequences,
today, in every running server:

- **Per-point TTL trimming is silently not applied.** A file containing one
  in-retention point retains every expired point until the file itself fully
  expires — which for a file whose series keep receiving writes is never.
- **Downsampling never happens at all.**

The test suite is green because every retention test calls
`compactor->compact(files, policies, seriesMap)` directly, bypassing the
Engine and the pending-map handshake. Even the test named
`SetRetentionContextAppliedOnCompact` (`tsm_compaction_retention_test.cpp:604`)
passes the policies *explicitly* to `compact()` as well (`:632-634`), so the
`std::exchange` consumption path at `executeCompaction` has literally zero
coverage. Supporting evidence that this is a wrong-layer wiring problem:
`TSMCompactor::runCompactionLoop()` (`tsm_compactor.cpp:1401`) — the loop the
handshake was presumably designed around — is itself dead in production; the
real loop is `TSMFileManager::startCompactionLoop()`
(`tsm_file_manager.cpp:429`), which was deliberately built beside it.

### Corrections to the prior characterisation

Verified against source; two points in the earlier analysis are wrong or
incomplete:

1. **Single-tier folding is NOT idempotent at the threshold-straddling
   bucket.** `downsampleThreshold = now - afterNanos` (`tsm_compactor.cpp:1043`)
   is not aligned to the bucket grid. A bucket `[b, b+interval)` that contains
   the threshold gets its `[b, T)` prefix folded (emitted at timestamp `b`)
   while its `[T, b+interval)` points pass through raw. A later compaction,
   with the threshold now past `b+interval`, folds the partial-bucket
   aggregate *together with* the raw remainder — `avg(avg(prefix), rest...)`,
   an unweighted fold-of-fold with wrong weights. Every bucket is a straddler
   for as long as the moving threshold is inside it, so with frequent
   compaction this can compound more than once per bucket. The claim
   "harmless at one tier because refolding 1-min points into 1-min buckets is
   idempotent" is true only for buckets that were *complete* when first
   folded. Fix (trivial, Phase 1): align each threshold down to its own bucket
   grid — `threshold = ((now - after) / interval) * interval` — so only
   complete buckets ever fold. Refolding a completed bucket's single
   bucket-start point is then genuinely idempotent for every offered method.
2. **An all-NaN bucket emits a fabricated point.**
   `foldOldPrefixIntoBuckets` creates the bucket state via `operator[]`
   before `addValue()` NaN-skips (`tsm_compactor.cpp:374-375`,
   `lib/query/aggregator.hpp:100-102`), and `getValue()` returns NaN for
   `count == 0` (`aggregator.hpp:362-365`), so a bucket whose raw points are
   all NaN emits a NaN point at a bucket-start timestamp that never existed
   in the data. Under NaN-as-missing this is invisible to aggregations, but
   raw reads surface it as a `null` at a synthetic timestamp. Fix (Phase 1):
   skip emission when `state.count == 0`, in both fold paths (`:395-413` and
   `:865-872`). Do NOT filter NaN *results* generally — `+Inf + -Inf = NaN`
   from real data is the correct IEEE aggregate and must still be emitted.

One additional point in the plan's favour: `executeTombstoneRewrite()`
(`tsm_compactor.cpp`, single-file rewrite driven by
`Engine::sweepTombstoneRewrites()` at `engine.cpp:1217`) also routes through
`executeCompaction()`, so fixing the wiring at that level makes tombstone
rewrites apply retention for free.

## Correctness contract

Rules every phase must preserve; the test plan pins each one.

- Turning a policy on must never lose in-retention, in-resolution data. An
  aged point is *replaced by its bucket aggregate*, never silently dropped
  (except by TTL).
- Only **complete** buckets fold. Folding the same stage twice is
  byte-identical (idempotent) for every offered method.
- NaN is missing: NaN raw points never poison an aggregate, and an all-NaN
  bucket emits nothing. Data-derived non-finite aggregates (±Inf, Inf−Inf)
  are emitted as computed.
- The fold runs before encoding; every written block has matching timestamp
  and value counts, so the `decodeBlockFlat` produced-vs-expected contract is
  untouched. No consumer-side clamps are (re)introduced.
- Query result shape stays a pure function of the query. Downsampling changes
  *which points are stored*, not how any path aggregates or labels them.
- A compaction that cannot obtain retention context **fails the compaction**
  (retried with backoff by `compactOneTier`), it does not silently compact
  without retention. Silent no-retention is exactly today's defect.
- Non-numeric (Boolean/String) series are never coerced numeric. In v1 they
  pass through undownsampled (TTL still applies); see "Non-numeric fields".

## Phase 1 — wire the existing single-tier feature (independently shippable)

Small, reviewable, and valuable on its own: it turns on per-point TTL and
single-tier downsampling, both currently dead, plus the two correctness fixes
above. No schema change.

### Design: provider injection, not call-before-trigger

The obvious fix — have Engine call `setRetentionContext()` before each
compaction — is rejected. `compactionSemaphore{2}` (`tsm_compactor.hpp:65`)
plus one fiber per tier (`tsm_file_manager.cpp:440-453`) means two
`executeCompaction()` calls can be in flight; a pending-map handshake set for
one plan can be `std::exchange`d by the other, and "always empty" simply
becomes "sometimes empty". The handshake is the defect's shape; keep none of it.

Instead, mirror the existing `setWalConversionProbe` pattern
(`tsm_file_manager.hpp:210`): Engine injects an async provider into the
compactor at startup.

```cpp
// tsm_compactor.hpp
struct RetentionCompactionContext {
    std::unordered_map<std::string, RetentionPolicy> policies;
    std::unordered_map<SeriesId128, std::string, SeriesId128::Hash> seriesMeasurement;
};
using RetentionContextProvider =
    std::function<seastar::future<RetentionCompactionContext>(const std::vector<SeriesId128>&)>;
void setRetentionContextProvider(RetentionContextProvider p);
```

`executeCompaction()` (`tsm_compactor.cpp:1310`), after acquiring the
semaphore, computes `getAllSeriesIds(plan.sourceFiles)` (sparse-index
iteration, no I/O), `co_await`s the provider, and passes the result to
`compact()` through its existing parameters. `setRetentionContext()` /
`_pendingRetentionPolicies` / `_pendingSeriesMeasurementMap`
(`tsm_compactor.hpp:81-82, 199-204`) are deleted; the direct `compact(files,
policies, seriesMap)` overload stays for tests and for future direct callers.

Provider failure policy: an exception propagates out of `executeCompaction()`
and is handled by `compactOneTier`'s existing failure backoff
(`tsm_file_manager.cpp:330-340`). Fast-path: when the shard's policy cache has
no policy with a TTL or downsample clause, the provider returns empty maps
without touching the index — zero cost for the common no-policy case.

### Building the seriesId -> measurement map affordably

`sweepExpiredFiles()` builds its map via
`index.getAllSeriesForMeasurement(measurement)` (`engine.cpp:1149-1158`) —
O(series in measurement) per call. On the compaction path that is the wrong
shape: a merge touches only the series present in its input files, and at
high cardinality a full-measurement prefix scan per merge (merges can run
every few seconds under load) is unaffordable and mostly wasted.

Use the reverse direction instead: the provider resolves the plan's series
IDs through `NativeIndex::getSeriesMetadataBatch()`
(`lib/index/native/native_index.hpp:110`; `SeriesMetadata` carries
`measurement`, `lib/index/index_backend.hpp:89-93`). Cost is O(series in the
input files) point lookups (bloom-filtered), on the shard that owns them.
Add a per-shard `SeriesId128 -> measurement` cache in Engine, populated from
batch results, size-capped (~128k entries ≈ 8 MB worst case; cap and evict).
Only series matching a policy-bearing measurement need retention context, but
we cannot know that before resolving — the cache is what amortises this to
near-zero for stable series sets. **Unknown to verify during implementation:**
measured latency of a 100k-series `getSeriesMetadataBatch` cold call; if it is
material, fall back to resolving lazily per-measurement using the `0x0A`
measurement-series prefix instead. Flagged, not assumed.

### Correctness fixes bundled into Phase 1

Shipping the wiring without these would enable a known-wrong fold:

1. **Bucket-aligned thresholds** — in the context build
   (`tsm_compactor.cpp:1041-1044`):
   `ctx.downsampleThreshold = ((now - after) / interval) * interval`.
2. **All-NaN bucket suppression** — skip `count == 0` states at both emission
   sites (`:395-413`, `:865-872`).

### Phase 1 touch points

| File | Change |
|---|---|
| `lib/storage/tsm_compactor.hpp` | delete `:81-82`, `:199-204`; add provider member + setter |
| `lib/storage/tsm_compactor.cpp` | `executeCompaction` (`:1310-1355`): call provider; context build `:1041-1044` threshold alignment; `:395`, `:865` NaN-bucket skip |
| `lib/core/engine.cpp` | install provider during init (`:19-61`, after tsmFileManager/compactor exist); provider impl + measurement cache |
| `lib/core/engine.hpp` | cache member, provider plumbing |
| `test/unit/storage/tsm_compaction_retention_test.cpp` | rewrite test 9 to drive `executeCompaction()`, not `compact()` |
| new Engine-level test | see test plan |

Lifetime note: `Engine::stop()` closes the retention gate (`engine.cpp:110`)
and stops the compaction loop (`engine.cpp:117`) before member teardown, so a
provider capturing the Engine is safe; verify that ordering holds for any new
call site.

### Phase 1 gates

- [ ] End-to-end: aged points written through Engine, policy set via the
      handler path, compaction triggered through
      `TSMFileManager::compactOneTier()`, query shows folded/TTL-trimmed data.
- [ ] Threshold straddle: fold, advance clock, fold again — the boundary
      bucket's final value equals the direct fold of all raw points.
- [ ] Idempotency: second compaction of already-folded data is point-identical.
- [ ] All-NaN bucket emits no point; mixed NaN bucket aggregates non-NaN only.
- [ ] No-policy shards: provider fast-path, zero index reads, no perf
      regression on `timestar_insert_bench` / compaction throughput.

## Phase 2 — the cascade

### Schema

`RetentionPolicy.downsample` becomes an ordered tier list. C++:

```cpp
struct RetentionPolicy {
    std::string measurement;
    std::string ttl;
    uint64_t ttlNanos = 0;
    std::optional<DownsamplePolicy> downsample;   // legacy mirror, see migration
    std::vector<DownsamplePolicy> downsampleTiers; // canonical; empty = none
};
```

The user's exact case:

```json
PUT /retention
{
  "measurement": "scada",
  "ttl": "730d",
  "downsample": [
    { "after": "7d",  "interval": "1m",  "method": "avg" },
    { "after": "90d", "interval": "15m", "method": "avg" }
  ]
}
```

`PUT` accepts `downsample` as an object (legacy, = one tier) or an array.

Validation (in `http_retention_handler.cpp`, factored into a shared
`validateRetentionPolicy()` so future callers cannot skip it — today the
method-string mapping in the compactor silently defaults unknown methods to
AVG, `tsm_compactor.cpp:1047-1056`, which is drift waiting to happen):

- 1..4 tiers; `afterNanos` strictly increasing; `intervalNanos` strictly
  increasing; **`interval[k+1] % interval[k] == 0`** (with epoch-aligned
  buckets, divisibility guarantees no stage-k bucket straddles a stage-k+1
  boundary; 15m = 15 × 1m qualifies);
- `after[k] >= interval[k]` (a threshold shorter than its own bucket is
  degenerate);
- `ttl`, if set, `> after[last]` (extends the existing check at
  `http_retention_handler.cpp:177-181`);
- one method for all tiers in v1 (open question 2);
- method ∈ {avg, min, max, sum, latest} (`:23-24`, unchanged — note COUNT is
  deliberately absent: count-of-counts does not compose).

### Fold generalisation

`SeriesRetentionContext` (`tsm_compactor.hpp:72-77`) grows from one
`(threshold, interval)` to an ordered array of up to 4 `(threshold[k],
interval[k])` with `threshold[k]` strictly *decreasing* with k (older data,
coarser bucket), each aligned down to `interval[k]`. The merged stream is
ascending, so both fold paths generalise naturally: a point's stage is the
largest k with `ts < threshold[k]`; the partition points are found once per
chunk with `lower_bound`, exactly as today (`:842`); bucket maps flush in
stage order before any finer-stage point is emitted, preserving ascending
block order (the property the streaming path already relies on, `:349-357`).

Memory note (pre-existing, worse under cascade): `dsBuckets` holds one
`AggregationState` (~112 B + optional vector) per bucket for the whole old
segment of a series before flushing. First fold of 83 days of 1 Hz data at 1m
is ~120k buckets ≈ 14 MB per in-flight series, times up to
`seriesBatchSize() × 4` pipelines. Since the stream is ascending, a bucket is
final the moment a timestamp ≥ its end arrives: emit incrementally and keep
O(1) live states per stage. Do this in Phase 2 while the flush logic is open.

### Persistence migration / back-compat

Policies are JSON records under index prefix `0x0B`. Old records carry
`downsample` as an object; the glaze meta must keep parsing them.

- **Read**: if `downsampleTiers` empty and legacy `downsample` present,
  promote it to a one-element tier list at load
  (`NativeIndex::getRetentionPolicy` / `getAllRetentionPolicies` callers, and
  `loadAndBroadcastRetentionPolicies`).
- **Write**: persist `downsampleTiers` AND mirror tier 1 (the finest) into
  legacy `downsample`. A rolled-back binary then still parses the record and
  applies tier 1 only — graceful degradation (finer than intended, never
  coarser, never data-destroying).
- **GET response**: returns both fields, same mirroring. Existing readers of
  `policy.downsample.{after,interval,method}` keep working for single-tier
  policies unchanged.
- **Proto** (`proto/timestar.proto`): add
  `repeated DownsamplePolicy downsample_tiers = 5` to `RetentionPolicy` and
  `= 4` to `RetentionPutRequest`; keep field 4/3 (`downsample`) with the same
  mirroring rules. Proto3 repeated-absent = empty, so old clients are
  unaffected.

### Phase 2 gates

- [ ] Two-tier fold: 1 Hz → 1m → 15m over synthetic data with gaps; min/max/
      sum/latest results equal the direct raw→15m fold exactly (composition
      proof, below); avg within the documented bound and exact when per-minute
      counts are uniform.
- [ ] Divisibility/ordering/ttl validation rejections, JSON + proto.
- [ ] Old persisted single-tier record loads and behaves identically pre/post
      upgrade; new two-tier record read by "old" reader shape (legacy field
      only) yields tier-1-only behaviour.
- [ ] Bounded memory: fold of 90 days × 1 Hz single series stays under a
      fixed budget (incremental bucket emission).

## Design decision — multi-stage aggregation exactness

The heart of the cascade. Stage 2 folds stage-1 outputs; is the result equal
to folding raw?

**Composition facts** (same method re-applied, complete buckets only):

| Method | stage-over-stage | exact? |
|---|---|---|
| min | min of minima | yes |
| max | max of maxima | yes |
| sum | sum of sums | yes |
| latest | latest bucket's latest (bucket-start timestamps preserve order) | yes |
| avg | **unweighted** mean of bucket means | **no** — exact only when every stage-1 bucket in the stage-2 window holds the same sample count |
| count | not offered (count-of-counts ≠ count; would need sum) | n/a |

For 1 Hz SCADA the per-minute count is 60 except during comm gaps, outages,
and deployment edges — which is precisely when SCADA data has gaps, so the
error case is real, not hypothetical. Magnitude: the composed mean is a convex
combination of bucket means with weights `1/n` instead of `count_i / Σcount`;
the error is bounded by the spread of the bucket means and scales with count
imbalance (a 1-sample minute weighs as much as a 60-sample minute).

**Options considered:**

- **(A) Persist per-point sample counts in TSM.** An optional per-block
  weights column (varint, ~1 B/pt) would make avg-of-avgs exact. Cost: TSM
  format v4 (docs/tsm_format.md — the index entry and block body both change),
  touching writer, reader, sparse index, zero-copy carry, and the
  decode-count contract's blast radius; plus rules for what happens when
  weighted and unweighted points merge. Weeks of work and permanent format
  surface for one method's exactness.
- **(B) Infer stage from timestamp spacing** (points spaced exactly
  `interval[k]` are stage-k outputs of assumed weight). Wrong exactly in the
  gap case it exists to fix — a gap makes raw spacing look like stage spacing
  and vice versa. Rejected.
- **(C) Shadow count series** (derived SeriesId carrying per-bucket counts).
  No format change, but creates series invisible to the index — breaking the
  compactor's own measurement lookup, cardinality estimates, discovery, and
  backup assumptions. Rejected as a class: data the index cannot see.
- **(D) Keep raw until the deepest threshold** and always fold raw→final.
  Defeats the storage purpose (60× more data retained between A1 and A2).
  Rejected.
- **(E) Accept documented approximation for `avg`; make exact methods easy.**
  Unweighted avg-of-avgs is what Graphite/whisper cascaded retentions and
  InfluxDB CQ-over-CQ pipelines ship. Combined with per-field method
  overrides (Phase 4) so totalizers/counters use `sum`/`max`/`latest` — which
  compose exactly — the residual inexactness is confined to analog averages
  under irregular sampling.

**Recommendation: (E)**, with (A) specified above as the committed escape
hatch if exact averages become a hard requirement — in which case it should be
built as "persist (sum, count) per downsampled point" rather than a weight
sidecar, and budgeted as a format-version project, not a retention patch. The
plan documents the approximation in docs/api-retention.md in those words. A
per-series "downsampled-through" watermark in the index was considered and is
*not* recommended in v1: with bucket-aligned thresholds the fold is idempotent
without it, it cannot fix avg weighting (it attributes stages but carries no
counts), and it adds a persisted structure that must survive compaction,
delete, and (later) replication. Revisit only if the Phase 3 density heuristic
proves insufficient (open question 4).

## Phase 3 — age-driven trigger

Today the fold runs only when tier merges happen, and merges need
`files_per_merge` files to accumulate (`tsm_compactor.hpp:340-348`). A
decommissioned RTU's data stops generating files, so it is never re-compacted
and stays at 1 Hz forever. Tier-3 files do keep merging (`getTargetTier`,
`tsm_compactor.hpp:399-402`) but only under the same accumulation condition.

Hang the trigger on the existing retention sweep: the shard-0 timer
(`Engine::startRetentionSweepTimer()`, `engine.cpp:1093`, config
`engine.retention_sweep_interval_minutes` = 15,
`lib/config/timestar_config.hpp:189`) already `invoke_on_all`s
`sweepExpiredFiles().then(sweepTombstoneRewrites)` (`engine.cpp:1103-1106`).
Add a third stage, `sweepDownsampleRewrites()`, modeled line-for-line on
`sweepTombstoneRewrites()` (`engine.cpp:1217` — snapshot files, estimate,
cap, rewrite):

- **Candidate test (no I/O)**: for each TSM file and policy-bearing series,
  from the sparse index: does the file hold blocks entirely older than some
  stage threshold whose point density exceeds that stage's bucket rate?
  Estimated density = Σ`blockCount` / time span of the aged blocks; a file
  already folded to stage k sits at ≈ `1/interval[k]` and is skipped. This
  makes the sweep self-limiting without any persisted watermark. Caveat
  (documented in-code at `tsm_compactor.cpp:256-263` for the coalescer):
  Float `blockCount` is the non-NaN count, so NaN-heavy series under-estimate
  density and may never trigger — acceptable, since folding NaN points yields
  nothing but block-count reduction.
- **Hysteresis**: require the estimated fold to reduce points by a configured
  factor (default ≥ 2×) so a tier-3 file is not rewritten every sweep as the
  threshold creeps.
- **Rewrite**: single-file, same-tier, through `executeCompaction()` exactly
  as `executeTombstoneRewrite()` does — which, after Phase 1, applies
  retention automatically. `LeveledCompactionStrategy::getTargetTier`
  already keeps sub-`files_per_merge` rewrites in their own tier
  (`tsm_compactor.hpp:388-403`).
- **Bounds**: reuse the tombstone sweep's shape — `hasCompactionCapacity()`
  check, cap per sweep (new config `engine.max_downsample_rewrites_per_sweep`,
  default 2), skip files in active compaction.

Gates:

- [ ] A quiet series (no new writes) reaches 1m and then 15m resolution
      within two sweep periods of crossing each threshold.
- [ ] An already-folded file is not rewritten on subsequent sweeps
      (density heuristic holds); sweep does zero work with no policies.
- [ ] Sweep respects the rewrite cap and never starves WAL conversion
      (compaction scheduling group unchanged).

## Phase 4 — per-field method overrides (and the non-numeric decision)

SCADA needs `avg` for analogs but `max`/`latest`/`sum` for counters and
status words; one method per measurement makes `avg` silently destroy
totalizers. Schema addition:

```json
"downsample": [ { "after": "7d", "interval": "1m", "method": "avg",
                  "fieldMethods": { "flow_total": "max", "status": "latest" } } ]
```

The per-series context build already resolves each series individually and
`SeriesMetadata.field` is available from the same batch lookup Phase 1 adds,
so the compactor change is small: pick the method per series from
`fieldMethods` with the tier's `method` as default. Validation: same allowed
set; same method per field across tiers (v1 rule).

**Non-numeric fields**: the fold is guarded numeric-only
(`tsm_compactor.cpp:361, 839`); Boolean and String pass through unfolded
(TTL still applies — the per-point TTL filter at `:507` is type-generic).
Decision: **keep passthrough as the default and call it correct.** The
canonical LATEST-per-bucket rule (CLAUDE.md) governs *query-time* reduction
when a caller asks for an interval; storage-side folding is destructive, and
for SCADA status words the transition history is usually the point of
retaining them. Offer opt-in destruction via Phase 4:
`"fieldMethods": {"status": "latest"}` on a Boolean/String field enables a
LATEST-per-bucket fold for that field (matching query semantics, values kept
in their written type — never 1.0/0.0, per the dynamo-equivalence rules).
Any other method on a non-numeric field is a validation error, mirroring how
`DerivedQueryExecutor` refuses non-numeric operands rather than coercing.

## Interactions with existing invariants

- **Decode-count contract**: the fold consumes decoded points and re-encodes;
  every output block's header count equals its real pair count by
  construction. No change to `decodeBlockFlat`/`readSingleBlock`; no clamps
  added. (Verified: the fold sits entirely before `writeSeriesCompactionData`.)
- **Sparse index / stats pushdown**: rewritten files get freshly computed
  block stats from the folded points, so bucketed `latest`/`first` stat
  resolution and COUNT pushdown remain placement-correct. Nothing reads block
  stats across the fold boundary.
- **LWW / backfill**: a late write into an already-folded range lands at raw
  resolution in a newer file and, on the next fold, aggregates *together
  with* the existing bucket point — for `avg`, another unweighted fold. If
  the late point's timestamp equals the bucket start exactly, `dataRank`
  resolves the duplicate (newer file wins) before folding. Backfill into
  folded history is therefore approximate, not lost. Document as a known
  limit; exact treatment needs option (A).
- **Query shape**: unchanged. A series whose stored resolution steps
  mid-range returns exactly its stored points on raw reads and epoch-aligned
  buckets under an interval; empty-bucket omission already means callers
  cannot assume uniform spacing. The aggregation-shape rules are about the
  query, not storage density.
- **Mixed-resolution aggregates**: `avg:m(v)` over a range spanning the fold
  boundary weights the aged region by its (fewer) stored points. Inherent to
  storage downsampling in every TSDB that does it; document in
  api-retention.md.
- **/derived forecast & anomalies**: models fit on stored spacing.
  `lib/query/forecast/periodicity_detector.cpp:80-108` already decimates
  large inputs internally, so *coarse* data is handled; a resolution *step*
  inside one fit window changes the sample-density assumptions and can bias
  period estimates. Severity unknown — flagged, not asserted. Mitigation is
  documentation ("fit windows should sit within one resolution regime") and,
  if field reports demand it, splitting fit windows at fold boundaries.
  Not blocking.
- **`QUERY_INCOMPLETE`**: unchanged; a folded block that fails decode fails
  the query like any other block.

## Cluster dimension (flag only)

Main is single-node. In the `tsdb-cluster-design` worktree (RF=3), compaction
is per-replica and each replica would evaluate `now - after` independently:
replicas of the same VShard hold byte-divergent files near the fold boundary,
and (with follower/replica reads wired) two reads of the same aged range can
disagree transiently — bounded by one sweep/compaction period, and only for
`avg` materially (exact methods converge to identical values even if folded
at different times, once thresholds pass). Snapshot/repair streaming copies
whole engine state, so divergence is re-created rather than repaired-away.
The eventual fix is to make the fold deterministic — thresholds derived from
a replicated clock value (e.g. carried in a periodic Raft command) instead of
local `now`. Out of scope here; must be on the cluster debt register before
retention ships in cluster mode.

## Test plan — closing the Engine-level hole

The bug survived a green suite because every retention test entered below the
wiring. The principle for new tests: **enter at or above the layer that was
broken.**

1. **Production-path integration test (the tripwire).** Seastar test: real
   Engine + TSMFileManager; write aged + recent points through
   `Engine::insert()`; install a policy via the same cache/broadcast calls the
   handler uses; accumulate `files_per_merge` tier-0 files; drive
   `TSMFileManager::compactOneTier(0)`; assert via `Engine` query that aged
   points are folded and TTL-expired points are gone. This test fails against
   today's tree — that is its acceptance criterion.
2. **Rewrite test 9** (`tsm_compaction_retention_test.cpp:604`) to call
   `executeCompaction()` with the provider installed and *without* passing
   policies to `compact()` — covering the exact consumption path that was
   never covered.
3. **Straddle/idempotency/NaN-bucket unit tests** at the compactor level
   (Phase 1 gates above).
4. **Cascade equivalence suite** (Phase 2): for each method, compare
   `raw --fold--> 1m --fold--> 15m` against `raw --fold--> 15m` on gap-bearing
   synthetic data; assert exact equality for min/max/sum/latest and the
   documented bound for avg. This is the executable form of the composition
   table.
5. **Sweep tests** (Phase 3): quiet-series folding, density-heuristic
   no-rechurn, rewrite cap.
6. **Migration tests** (Phase 2): legacy persisted record; mixed-version
   record shapes; proto round-trip.
7. **API tests** (`test_api/`): PUT/GET with tier arrays, validation errors,
   legacy-object acceptance.

Run discipline per MEMORY.md: all five suites, not unit-only; force-clean
stale test objects after header changes.

## Risks and unknowns

- **Cold-cache cost of the provider's metadata batch lookup** at 100k+ series
  per merge is unmeasured (Phase 1 unknown; fallback identified).
- **Fold memory under cascade** is bounded only after the incremental bucket
  emission change; until then first-fold of deep history is the risk case.
- **`hasPerPointRetention` disables the zero-copy carry**
  (`tsm_compactor.cpp:226, 304`): once a measurement has any TTL/downsample
  policy, *every* compaction of its series decodes and re-encodes, forever —
  including data already at final resolution. This is a real write-amp cost
  Phase 1 turns on. Mitigation candidates (not committed): skip retention
  context for blocks entirely newer than the finest threshold (cheap, partial)
  or a density check to re-enable carry for already-folded segments. Measure
  first with the Jul-16 canonical baselines.
- **Approximate `avg` composition** is a semantic commitment; if the user
  rejects it, option (A) reopens as a format project.
- **Clock dependence**: thresholds use wall-clock `now`; a host clock step
  moves fold boundaries. Same exposure as TTL today; noted, not new.

## Open questions for the reviewer

1. Is the unweighted `avg` cascade (option E) acceptable for the SCADA use
   case, given per-field overrides for counters — or is exact `avg`
   (format-change option A) a requirement worth its cost?
2. Should per-tier method *variation* (e.g. `avg` then `max`) be allowed?
   v1 says no (composition semantics unclear); relaxing later is
   backward-compatible.
3. Phase 4 scope: are per-field overrides needed before first ship (they are
   arguably required for SCADA counter correctness), or acceptable as
   fast-follow after Phase 2?
4. Is the density-heuristic sweep (no persisted watermark) acceptable, or is
   a persisted per-series fold watermark wanted from day one despite its
   compaction/delete/replication surface?
5. Non-numeric default: is pass-through-unless-opted-in the right call for
   status words, or should Boolean fields default to `latest` folding?
6. TTL wiring alone (a strict subset of Phase 1) could ship even earlier as a
   pure defect fix. Split further, or keep Phase 1 as specified?

---

## Deviations from the plan as built

Recorded after the fact. The plan body above is unchanged; this section is
authoritative wherever the two disagree.

### Answers to the open questions

1. **Unweighted `avg` (option E) was accepted**, with per-field overrides as
   the mitigation. Option (A) — persisting `(sum, count)` per downsampled
   point — remains the specified escape hatch and is still a TSM format
   project, not a retention patch.
2. **Per-tier method variation stays rejected** in v1, and the rule grew a
   per-field analogue: a field named in one tier's `fieldMethods` must be
   named in *every* tier, with the same method.
3. **Phase 4 shipped**, not deferred — SCADA counter correctness needs it.
4. **The density heuristic was kept**; no persisted fold watermark was added.
5. **Non-numeric default is pass-through**, with `latest` as the only opt-in.
6. **Phase 1 was not split**; TTL wiring and single-tier downsampling shipped
   together.

### Structural deviations

- **One fold implementation, not two.** The plan described fixing the NaN-bucket
  bug at two emission sites. Both paths were instead collapsed into a single
  `CascadeFolder<T>` (`tsm_compactor.cpp`), so the streaming fold and the
  no-sink fallback cannot diverge. The `count == 0` suppression exists once.
- **Incremental emission landed as specified**, and the `dsBuckets` map is gone
  entirely: live state is one `AggregationState` per series, not one per bucket.
  The no-sink fallback still materialises its output vectors, but production
  compaction always supplies a sink.
- **Threshold resolution is shared code.** `buildDownsampleStages()`
  (`lib/retention/retention_policy.cpp`) is the single definition consumed by
  both the compactor and the sweep, so the sweep cannot nominate a fold the
  compactor would decline — which would have been an endless rewrite loop.
- **The hysteresis default is 1.25, not the planned 2.0.** The estimate counts
  *buckets*, so a legal 2x tier step yields a ratio strictly below 2 (minimum
  1.5 at even bucket counts). A 2.0 default admitted no 2x step at all, and
  since the series is quiet by definition no later sweep could ever change the
  answer. See the derivation in `EngineConfig`.
- **A validation failure disables folding rather than defaulting to AVG.** The
  original code mapped an unrecognised method string to AVG, which would have
  let a typo destructively average a totalizer. TTL still applies.

### Defects the adversarial reviews caught

- **Partial-bucket refold.** An unaligned threshold split the straddling bucket,
  folding its prefix and re-folding that aggregate against the raw remainder on
  the next compaction — `avg(avg(prefix), rest…)`, compounding once per
  compaction. Fixed by aligning every threshold down to its own interval.
- **Fabricated all-NaN points.** A bucket whose raw points were all NaN emitted
  a NaN at a bucket-start timestamp that never existed in the data. Fixed by
  suppressing `count == 0` buckets — deliberately a count check, not a NaN
  check, so a data-derived `+Inf + -Inf` still emits.
- **Endless rewrite loop via `fieldMethods` presence.** Validation originally
  compared only the *resolved* method, so a field named in the second tier only
  (with a tier method equal to the override) validated. The compactor derives
  the non-numeric opt-in from tier 0 while the sweep derived it from the union
  over all tiers, so the sweep nominated a file the compactor then declined to
  fold — forever. Fixed by requiring presence in every tier.
- **Glaze dropping whole policies on downgrade.** An unknown key failed the
  entire record, discarding the TTL along with the tiers. Fixed with a lenient
  read (`kRetentionReadOpts`); the caveat for binaries predating that fix is
  documented in `docs/api-retention.md`.
- **Zero-copy carry correctness.** Retention forces decode/re-encode, so the
  carry is disabled whenever a series resolves a TTL cutoff or any stage.

### Known limitations carried forward

Not defects — accepted costs, listed so they are not rediscovered as surprises.

- **Zero-copy carry is disabled wholesale** for any series with a TTL cutoff or
  a reachable stage, including blocks entirely newer than the finest threshold.
  The planned narrowing (skip retention context for such blocks) is **not
  implemented**, so a policy-bearing measurement pays decode/re-encode on every
  compaction forever. Write-amplification cost is unmeasured on this tree.
- **The provider's cold field/measurement resolution is uncapped** — one
  sequential index lookup per unresolved series, in a single unyielding batch on
  the merge path, and a failure there fails the merge. The sweep's equivalent
  path *is* capped (4096/sweep); the provider's is not. Cost is cold-start only
  (the caches hold permanent facts), and unmeasured at 100k+ series.
- **`_seriesMeasurementCache` clears wholesale at 128k entries** rather than
  evicting, so a shard above that cardinality can clear mid-population and
  re-resolve on the next merge. (`_seriesFieldCache` uses the opposite policy —
  stop admitting — because its misses are far more expensive.)
- **Failed downsample rewrites have no backoff** and are re-nominated every
  sweep. Pre-existing shape, shared with `sweepTombstoneRewrites`.
- **The 10M series enumeration cap wedges the sweep** for an over-cardinality
  measurement: age-driven folding stops until cardinality drops (merge-driven
  folding is unaffected).
- **Cluster determinism is unaddressed**, as flagged: replicas evaluate
  `now - after` independently. Must reach the cluster debt register before
  retention ships in cluster mode.
