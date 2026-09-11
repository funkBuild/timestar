# Retention compaction performance review

> Historical design/performance record. The September 2026 correctness fixes
> replace the value-only fold with persisted V4 rollup state and add reads for
> partially aged sweep candidates. The unweighted-average decision and the
> run-batched-fold performance figures below do not describe the current path.
> See [Retention API](api-retention.md) for current semantics and compatibility.


**Status:** REVIEW — measurements taken on branch `downsampling-cascade` at
`8080797`, single machine (Ryzen 9 7950X, AVX-512, NVMe), otherwise idle.
Nothing in this document is implemented; the one prototype built to bound the
fold opportunity was reverted after measurement. Companion to
`docs/downsampling-cascade-plan.md`, whose "Known limitations carried forward"
list flagged the costs quantified here.

**The question:** any TTL or downsample policy sets `hasPerPointRetention`,
which disables the zero-copy carry (`tsm_compactor.cpp`), so every compaction
of a policy-bearing series decodes, filters/folds, and re-encodes every block
— forever, including data already at final resolution. How much of that
penalty is real, where exactly does it go, and which parts can be recovered?

## Method

All end-to-end numbers come from `test/benchmark/tsm_compaction_benchmark`
(`--smp 1 -m 8G`, O_DIRECT data dir on the NVMe root filesystem, policy cases
run round-robin, medians over 7 timed runs after 2 warmups; ablations used 5
runs after 1 warmup). Profiling is unavailable on this host
(`perf_event_paranoid=4`), so the decomposition is by **ablation**: throwaway
builds that cut the pipeline after decode ("decode-only") and after
filter/fold ("no-encode"), plus a standalone micro-benchmark of the fold loop
itself. Every throwaway edit was reverted; the tree at the end of the review
is clean apart from this document.

Two measurement windows were discarded after per-run inspection caught the
documented multi-second background-I/O episodes this box suffers: one
inflated a `recent` decode-only median to 1907 ms (min 336 — the min row gave
it away), the other pushed a `folded` no-policy carry to a *consistent*
705 ms across five runs; a clean re-run immediately after read 263 ms. Both
case sets were re-measured in full. Medians-with-min/max plus interleaving is
the right discipline here; a single sequential campaign would have produced a
confident, wrong ratio.

## Baseline

Reproduced independently before touching anything; agrees with the prior
campaign within ~±3 %.

| Dataset | no-policy | ttl-only | cascade | ttl× | cascade× |
|---|---|---|---|---|---|
| recent (1 Hz, 40 M pts, ends now) | 345.8 ms | 534.8 ms | 530.2 ms | 1.55 | 1.53 |
| scada7 (straddles 7 d/1 m) | 336.9 ms | 519.5 ms | 590.0 ms | 1.54 | 1.75 |
| scada90 (straddles 90 d/15 m) | 343.1 ms | 526.9 ms | 619.9 ms | 1.54 | 1.81 |
| folded (already 15 m, 30 M pts) | 262.6 ms | 377.5 ms | 582.7 ms | 1.44 | 2.22 |

(`tier0real`, the 20 k-series × 200-pt under-full shape, re-verified on the
pristine build at the end of this review: no-policy 3582.6 ms, ttl-only
3579.8 ms (1.00×), cascade 3623.8 ms (1.01×). Retention costs nothing there
because `coalesceUnderfullBlocks` already takes those series off the carry
with no policy — the penalty this review chases only exists where blocks are
full enough to carry.)

## Where the retention penalty actually goes

Ablation stages, `recent` dataset (40 M points; constant per-run overhead of
file open/index reads is inside every row):

| Pipeline prefix | Median | Increment |
|---|---|---|
| read + decode (drop every point) | 332 ms | — |
| + TTL filter/tombstone/dedup/buffer | 347 ms | **~15 ms (0.4 ns/pt)** |
| + encode + write (= full ttl-only) | 535 ms | **~188 ms (4.7 ns/pt)** |
| no-policy carry (read + write compressed), for scale | 346 ms | — |

Fold arithmetic, isolated two independent ways — (a) no-encode ablation,
cascade minus ttl-only in the same interleaved run; (b) full-run cascade
minus ttl-only where output sizes match:

| Dataset | Folded points | Fold cost | ns/pt |
|---|---|---|---|
| scada7 (60 pts/bucket, 1 stage live) | ~22.7 M | 126 ms (a) | 5.5 |
| scada90 (both stages live) | ~39.7 M | 202–206 ms (a) | 5.2 |
| folded (1 pt/bucket) | 30 M | 205 ms (b) / 209 ms (a) | 6.8–7.0 |

Conclusions the rest of this review builds on:

1. **The TTL-only penalty (~1.5×) is the re-encode side, full stop.** Decode
   of 40 M points costs almost exactly what the carry's compressed-block
   read+write costs (332 vs 346 ms), and the filter itself is noise
   (0.4 ns/pt). What a policy actually buys is +4.7 ns/pt of encode
   (ALP + FFOR + zstd + block bookkeeping). Any recovery of the TTL-only gap
   must avoid *encoding*, not avoid *filtering*.
2. **The cascade's additional penalty is the fold line item**: 5.2–7.0 ns/pt
   over every point below the finest threshold — partially offset in mixed
   datasets by the encode it saves when output shrinks (scada7's net cascade
   delta is +70 ms = 126 ms fold − ~56 ms saved encode).
3. **The prior decomposition of the `folded` case is confirmed**: of its
   320 ms cascade penalty, ~205 ms is fold arithmetic on data the fold cannot
   change, ~115 ms is the carry loss (decode+encode replacing the compressed
   pass-through).
4. `getAllSeriesIds` measured 25–35 µs per call at 500 series (hash-set
   build over resident sparse indexes, no I/O). Linear scaling puts the
   duplicated call at ~1–1.5 ms per 20 k-series merge whose compaction costs
   ~2.7 s. The deferral as "constant-factor" was correct — see the
   not-worth-it register.

## The fold line item, dissected

A standalone micro-benchmark (same `AggregationState` arithmetic verbatim,
same data shapes) attributes the 5.2–7.0 ns/pt:

| Variant (cache-warm, 60 k-pt per-series windows) | folded shape (1 pt/bkt) | scada shape (60 pts/bkt) |
|---|---|---|
| v0 — current: per-point `addValue` + div/pt | 1.82 ns/pt | 1.97 ns/pt |
| v1 — method-aware add (skip Welford/min/max) | 1.83 | 1.96 |
| v2 — v1 + division strength-reduction | 1.93 | 1.98 |
| v3 — v2 + single-point-bucket passthrough | 1.20 | 1.97 |
| v4 — run-batched fold, 4 independent NaN-skip accumulators | 1.73 | **0.63** |

> **CORRECTION (adversarial review of the run-batched fold implementation).**
> The v0 and v1 rows above are **invalid**, and the "Welford-skip buys nothing"
> conclusion drawn from them is **wrong**. The micro-benchmark hard-coded
> `getValue(AVG)`, which lets the compiler prove `mean`/`m2` dead and delete the
> Welford update — so v0 never executed the code v1 removes, and the two
> *had* to agree. In the real fold `CascadeFolder::method_` is a runtime member,
> `getValue(method_)` may read `mean`/`m2` (STDDEV/STDVAR), and the update is
> live.
>
> Re-measured with the method made opaque to the optimiser (same host, same
> shapes, min of 7):
>
> | | folded shape (1 pt/bkt) | scada shape (60 pts/bkt) |
> |---|---|---|
> | v0 `addValue`, Welford live | 3.75 ns/pt | 4.95 ns/pt |
> | v1 `addValueForMethod(AVG)`, Welford skipped | 3.14 | 3.83 |
>
> Welford-skip is worth **~1.1 ns/pt (23 %)**, not zero. The arithmetic said so
> before the measurement did: `mean += delta / count` puts a double division on
> the *loop-carried* chain, and that chain measured 3.59 ns/pt in isolation
> (~18 cycles) against a plain `sum +=` at 0.53 — so no variant that really
> runs it can also run at 1.8–2.0 ns/pt.
>
> Two knock-on corrections:
> - The "tight loop is not where most of the real cost is / ~60–70 % is
>   surrounding plumbing" finding below is **overstated**. With Welford live the
>   tight loop alone is 3.75–4.95 ns/pt against a real fold line of 5.2–7.0, so
>   most of the fold cost *was* the arithmetic — much of it the division this
>   benchmark had optimised away.
> - The division strength-reduction result (v2) is **not** re-validated here; it
>   rode on the same DCE'd baseline and should be treated as unmeasured rather
>   than as a negative result.
>
> None of this changes the recommendation: the run-batched fold subsumes
> Welford-skip entirely, because its kernels bypass `AggregationState`.

Three findings, two of them negative — **but see the correction above: the
first is wrong and the second is overstated**:

- ~~**Skipping Welford and the per-point `ts / interval` division buys
  nothing.**~~ **WRONG for the Welford half** (see correction). The
  *division* half — `ts / interval` per point — is not re-tested here and
  remains plausible, but was measured against the same DCE'd baseline.
  Recorded so nobody re-litigates a `libdivide`/reciprocal scheme without
  first re-measuring it honestly.
- **The tight loop is not where most of the real cost is.** Cache-warm v0
  runs at ~1.9 ns/pt; the compactor's measured fold line is 5.2–7.0 ns/pt.
  So ~60–70 % of the real fold cost is the surrounding plumbing: the
  per-point `CascadeFolder::add` state machine (stage scan, live-bucket
  compare), the per-bucket `AggregationState{}` reset (~136 B including a
  `std::vector` member), and the emit/drain path.
  **Overstated — see the correction: v0 with Welford live is 3.75–4.95 ns/pt,
  so the split is far closer to even.**
- **Run-batching is the only variant that moves the needle**, and it moves it
  3.1× on multi-point buckets (0.63 vs 1.97 ns/pt) because it converts the
  loop-carried scalar fold into independent accumulator chains over a flat
  run — and eliminates the per-point state machine as a side effect.

### Prototype: run-batched fold in the compactor (measured, then reverted)

`CascadeFolder` gained an `addRange(ts*, vals*, n)` used by
`foldOldPrefixIntoBuckets` (which already owns flat arrays): find each
bucket's run boundary by scanning the sorted timestamps, fold the run with a
4-independent-accumulator NaN-skip sum+count kernel, emit `sum/count`
directly; the trailing (possibly chunk-straddling) run and any live-bucket
continuation stay on the existing per-point path, so cross-chunk semantics
are untouched. Prototype scope: `double`/AVG (the benchmark's method); other
methods fall back to the per-point path.

Measured (7 timed runs after 2 warmups, same methodology; the "fold line" is
cascade − ttl-only from the *same interleaved run*, so it is negative where
the fold now costs less than the encode of the points it removes):

| Dataset | cascade before | cascade after | fold line before → after | cascade× before → after |
|---|---|---|---|---|
| scada7 | 590.0 ms | 495.8 ms | +70.5 → **−36.1 ms** | 1.75 → 1.46 |
| scada90 | 619.9 ms | 422.1 ms | +93.0 → **−96.8 ms** | 1.81 → 1.25 |
| folded | 582.7 ms | 467.6 ms | +205.2 → **+89.1 ms** | 2.22 → 1.78 |

In the isolable case (`folded`, where input and output sizes are identical)
the fold line drops 205 → 89 ms (2.3×); in the mixed cases the cascade now
runs *cheaper than TTL-only* because folding 60 points into one costs less
than encoding them. The scada90 merge — both stages live — improved 32 %
end-to-end. The remaining `folded` gap over TTL-only (~89 ms) is the
emit-bound 1-pt/bucket floor discussed under SIMD below.

**Correctness.** The first prototype build failed two existing tests —
`DownsampleCascadeAdversarialTest.CascadeSpanningMultipleSpillsStaysOrderedAndExact`
and `TSMChunkedMergeTest.DownsampleStreamsThroughCompactionWithMultipleSpills`
— on a real seam bug: a bucket carried live across a spill boundary was not
closed before the batch loop emitted later buckets, breaking ascending
output order. One added `closeLiveBucket()` at the seam fixed it; all 95
retention/cascade/chunked-merge tests then pass against the prototype
(re-verified on the fixed build: `folded` cascade 471.8 ms median vs 467.6
pre-fix — unchanged within noise, as the benchmark's series fit one chunk and
never hit the seam).
Two lessons worth keeping: the adversarial multi-spill suite catches exactly
this class of bug, and any production implementation must land with a
batch-vs-per-point equivalence test over chunk-straddling data.

## What SIMD can and cannot do here

The project mandate is SIMD-wherever-possible via Highway. The honest
assessment, with numbers:

**Where it already is.** The heavy per-point stages of this path are the
codecs, and they are already Highway/SIMD: ALP float encode/decode
(`lib/encoding/alp/alp_simd.cpp`), the FFOR/Simple8b timestamp family
(`integer_encoder_ffor.cpp`), zigzag (`zigzag_simd.cpp`). The 4.7 ns/pt
encode line and the decode line are post-SIMD numbers; there is no untapped
vector win sitting in them for this review to claim.

**The TTL filter: nothing to vectorise.** The filter measured 0.4 ns/pt
(15 ms of a 535 ms run). Structurally it is not a scattered predicate: the
cutoff is a constant and timestamps are sorted, so "TTL filtering" is a
prefix cut — the compress-store gather this review was asked to consider has
no scattered survivors to gather. A `lower_bound`-and-skip would be the
idiomatic form, but at 0.4 ns/pt there is nothing worth collecting. **Not
applicable, measured.**

**The fold: batching is the win; lane width is not.** The run-batched kernel
is exactly the shape of the existing `simd_aggregator.cpp` kernels
(independent accumulators, NaN masked to the identity — reuse, not
invention). But note what the micro-benchmark says about lanes: the
4-accumulator form compiled at plain `-O3` (SSE2 baseline) hit 0.63–0.78
ns/pt, and recompiling with `-march=native` (AVX-512 available) changed
nothing (0.80 ns/pt cold; the folded shape even regressed slightly). At 60
points per run the kernel is bound by run-boundary discovery and emit, not by
FP throughput — wider vectors have nothing left to eat. A dedicated Highway
kernel with dynamic dispatch is therefore *optional* for this workload: the
portable unrolled-scalar form already captures the win, which came from
reassociation and from deleting the per-point state machine, not from vector
width. This matches the project's own hot-path review lesson ("algorithmic
wins survive, SIMD/copy micro-opts usually don't").

**The 1-pt/bucket shape is emit-bound, and no kernel fixes it.** In the
already-folded case every "run" is length 1; the batch kernel degenerates to
a copy with a bucket-boundary check, and the cost floor is reading 16 B and
writing 16 B per point through the drain path. v3/v4 shave ~10–35 % of the
tight loop there, not 3×. The remaining `folded` gap after the fold fix is
the decode+re-encode floor — the carry problem, not an arithmetic problem.

**Kahan and the semantics.** The per-point Kahan compensation is measurably
free (v0 ≈ v1); vectorising Kahan is unnecessary and was not attempted. The
batch kernel preserves the pinned semantics by construction: NaN lanes are
masked to the additive identity and excluded from the count (NaN-as-missing,
`docs/nan_policy.md`); an all-NaN run has `count == 0` and emits nothing
(the fabricated-point rule); ±Inf participates arithmetically and
`+Inf + −Inf` inside one bucket yields NaN *with* `count > 0`, which is
emitted — the data-derived-NaN case the count-check (not NaN-check)
emission rule exists to protect. What the kernel does change is summation
*order*: a reassociated 4-chain sum can differ from the sequential
Kahan-compensated sum in the last ulp for a pathological bucket. For AVG the
cascade is already documented as approximate (`docs/api-retention.md`); for
SUM the production version should either keep per-point Kahan on the
run path (cost: measured zero) or accept the reassociation — decide at
implementation time, but the idempotency contract (single-point buckets
refold byte-identically) is unaffected either way, because a fold-of-one is
exact in every ordering.

## Recovering the TTL-only floor: partial carry (bounded, not implemented)

The encode line (4.7 ns/pt) is the whole TTL-only penalty, and the fold fix
does not touch it. The known-rejected per-block
`minTime >= max(ttlCutoff, threshold)` skip is *not* re-proposed in its
naive form — it recovered nothing for aged data and regressed output size
46 % because `coalesceUnderfullBlocks` is decided under
`!hasPerPointRetention` and never revisited. The refined form that survives
those objections:

- Classify each block of a series once, from index metadata already in hand:
  **carryable** iff the series' blocks are non-overlapping, tombstone-free,
  and the block lies entirely at or above every per-point boundary
  (`minTime >= ttlCutoff` and `minTime >= finestThreshold`).
- Compute the under-full coalescing decision **on the carryable subset**
  (same compressed-bytes metric) — this is the specific interaction the
  naive prototype ignored; blocks that coalesce simply leave the carryable
  set.
- Emit per series in ascending order: folded/filtered output first (all of
  it lies below the finest threshold by construction), then the carried
  compressed blocks. The file format is indifferent — the index records each
  block's absolute offset, and chunks from different series already
  interleave — but the writer currently treats a series as either wholly
  carried or wholly encoded; allowing both within one series, with the index
  entry merging both block lists in time order, is the main new surface.

Bounded upside from this review's measurements: `recent` (the steady-state
shape of every policy-bearing measurement whose data has not yet aged)
recovers essentially the whole 189 ms penalty → ~1.0×; scada7 carries the
~43 % of its blocks that lie above the threshold → roughly 70–90 ms of its
253 ms penalty; `folded` recovers nothing (every block is below the threshold —
that case is the fold fix's job, and after it, the decode+encode floor).
The earlier rejected prototype's own result — 100 % recovery on TTL-only —
is the existence proof for the upside; what was missing was the coalescer
interaction and the mixed-emission design. Risk: moderate — writer surface
(mixed carried/encoded series), LWW at the carry/fold seam (safe: blocks are
non-overlapping and the fold region is strictly below the carry region), and
the string-dictionary single-source rule must join the carryable test
unchanged. This is a design change, not a patch; budget accordingly.

**What cannot recover the `folded` case's remaining ~1.4×:** re-enabling the
carry for already-folded blocks would need proof that every point in a block
is bucket-aligned with one point per bucket. The sparse index cannot supply
it: `(minTime, maxTime, blockCount)` admits counterexamples (three points
`{0, 30, 120}` at a 60 s interval satisfy any span/count test yet fold to
two), and Float `blockCount` is the non-NaN count besides. Only a per-block
"folded-through stage k" marker persisted at write time could prove it —
a TSM format change carried on every block forever, to save ~115 ms per
compaction of fully-aged series. **Not recommended** at this cost/benefit;
re-open only if aged-data rewrite volume becomes a measured operational
problem (the Phase-3 density heuristic already keeps quiet folded files from
being re-nominated every sweep).

## Measured and not worth it

Recorded so they are not re-proposed:

- ~~**Welford-skip / method-aware `addValueForMethod` in the fold** — zero
  measurable effect~~ — **RETRACTED.** The benchmark that produced that result
  had dead-code-eliminated the Welford update it claimed to be measuring; with
  the method opaque it is worth ~1.1 ns/pt (23 %). See the correction under
  "The fold line item, dissected". Moot in practice only because the shipped
  run-batched kernels bypass `AggregationState` altogether — not because the
  saving was not there.
- **Division strength-reduction (reciprocal or boundary-advance)** — zero
  measurable effect (v2 ≈ v0). **Treat as UNMEASURED**: v2 was compared against
  the same DCE'd v0 baseline, so the comparison establishes nothing either way.
- **Single-point-bucket passthrough as a standalone fix** — real but small
  (1.82 → 1.20 ns/pt warm ≈ ~35 ms end-to-end on `folded`); subsumed by the
  run-batched fold, which short-circuits length-1 runs anyway.
- **`-march=native` / wider lanes for the fold kernel** — no gain over the
  portable 4-accumulator form (0.78 → 0.80 ns/pt); the folded shape
  regressed ~10 %.
- **SIMD compress-store for the TTL filter** — structurally inapplicable
  (sorted input, constant cutoff ⇒ prefix cut) and the whole filter is
  0.4 ns/pt.
- **De-duplicating `getAllSeriesIds` per merge** — ~25–35 µs at 500 series,
  extrapolating to ~1–1.5 ms at 20 k series against a 2.7 s merge (<0.1 %).
  Fine to fold into an unrelated refactor of `executeCompaction` (pass the
  list into `compact()`), not worth a change on its own.
- **Naive per-block `minTime` retention skip** — prior result, restated for
  the record: 100 % recovery on TTL-only, ~0 % on aged data, +46 % output
  size via the unrevisited coalescer decision. Superseded by the partial
  carry design above.

## Recommendations, prioritised

1. **Run-batched fold in `CascadeFolder` (implement).** Measured on a working
   prototype: cascade merges improve 16–32 % end-to-end (fold line 205 → 89 ms
   on already-folded data; a both-stages-live merge drops from 1.81× to
   1.25×, cheaper than TTL-only). ~90 lines in one file, no format or
   semantic change.
   Production scope beyond the prototype: kernels for SUM/MIN/MAX/LATEST
   (same shape as `simd_aggregator.cpp`'s NaN-skip kernels — LATEST is a
   backward scan for the last non-NaN), `int64_t` via the existing
   cast-to-double semantics, per-point fallback retained for Boolean/String
   (opt-in only) and for chunk-straddling runs. Test surface already exists:
   `CompactionRetentionTest` pins straddle, idempotency, and NaN-bucket
   rules; add a batch-vs-per-point equivalence test over gap-bearing data.
   Low risk; the fold stays one implementation used by both paths.
2. **Partial zero-copy carry under retention (design + implement).**
   Recovers the ~1.5× TTL-only floor for un-aged data — the steady-state
   cost every policy-bearing measurement pays on every compaction of every
   tier. Bounded above at ~189 ms/40 M pts (to ~1.0×) for un-aged series;
   moderate effort and risk (writer surface); the coalescer interaction is
   the acceptance test the naive form failed.
3. **Leave alone:** the TTL filter, `getAllSeriesIds`, division/Welford
   micro-opts, format-level folded-block markers, and any lane-width work on
   the fold kernel — all measured or bounded above as not worth their cost.

With (1) and (2) both landed, the projected steady state is: un-aged
policy-bearing data ≈ 1.0× (carry restored), actively-folding merges
dominated by the encode of their (much smaller) output, and fully-aged
already-folded data ≈ 1.4× on the rare occasions it is rewritten at all —
a gap this review recommends accepting.
