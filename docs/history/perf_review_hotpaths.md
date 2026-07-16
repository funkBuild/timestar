> **HISTORICAL DOCUMENT** (review, June 2026) — describes the design as planned; symbol names and details have since changed. See [docs/architecture.md](../architecture.md) for current state.

# TimeStar Performance Review — Insert & Query Hot Paths

> Multi-agent review (June 2026). 16 hot-path components reviewed by focused agents; every
> finding adversarially verified by 3 diverse lenses (is-it-real / correctness-safe /
> actually-hot), majority-keep with a correctness-risk veto. **73 findings → 62 confirmed**
> in the main pass; a backfill of 3 rate-limited components (WAL, TSM-read, align/tombstone)
> added **15 → 11 confirmed**. Rejected findings were false, cold, or already-handled.

## Implementation status (branch `perf/hotpath-quick-wins`)

Applied in the first quick-wins batch:

| # | Win | File | Status |
|---|-----|------|--------|
| 1 | Timestamp block encode → `encodeInto` (no aligned temp + double-copy) | `lib/storage/tsm_writer.cpp` writeBlock + bool inline path | ✅ done |
| 2 | int64 value block encode → `encodeInto` | `lib/storage/tsm_writer.cpp` | ✅ done |
| 3 | `writeIndex` series-id via `write_bytes` (no per-series `toBytes()` string) | `lib/storage/tsm_writer.cpp` | ✅ done |
| 4 | Per-point series-key bound by const-ref (no happy-path copy) | `lib/storage/memory_store.cpp` | ✅ done |
| 5 | `findOrCreateCandidate` single `try_emplace` (no double hash/probe) | `lib/http/http_write_handler.cpp` | ✅ done |
| 6 | Validation context strings only built on invalid path | `lib/http/http_write_handler.cpp` | ✅ done |
| 7 | `compressStrings` reuses thread-local staging (no per-call 4KB page alloc) | `lib/encoding/string_encoder.cpp` | ✅ done |
| 8 | WAL int64 zigzag scratch → thread-local (no per-entry alloc+zero) | `lib/storage/wal.cpp` | ✅ done |

Deferred (next PRs): batch fan-out consolidation, `SeriesId128` plumbing through `MetadataOp`,
ALP float-encode SIMD, concurrent metadata resolve, SIMD aggregation gaps, tombstone two-pointer
sweep, raw-read direct-decode. See detail sections below.

---

## 1. Executive Summary

**Insert** — dominant throughput losses:
1. **Batch fan-out fragmentation** — the JSON batch path fans out cross-shard once *per
   MultiWritePoint* instead of once per request (`http_write_handler.cpp:1664`), multiplying
   WAL/subscriber/metrics overhead and allocating `4×shardCount` nested vectors per group. The
   protobuf path already proves the consolidated model (`:1422-1530`).
2. **Redundant series-key build + XXH3-128 hash, up to 3× per new series**
   (`http_write_handler.cpp:1131` → `engine.cpp:279` → `native_index.cpp:665`).
3. **TSM-flush encode** allocating a fresh 4KB-page-aligned `AlignedBuffer` and double-copying
   per block for timestamps/int64/bool/string (`tsm_writer.cpp:114/146/158`) plus a fully-scalar
   ALP float encode (`alp_encoder.cpp:265`).

Fixing the fan-out consolidation and converting all encode paths to the existing `encodeInto`
pattern are the two biggest wins.

**Query** — dominant losses:
1. **Per-series metadata resolve fully serialized** — N sequential SSTable walks per discovery
   with no overlap (`native_index.cpp:2197`).
2. **Discovery loop deep-copies the full tag `std::map` per series** + builds a never-read
   `seriesKey` through the pushdown path (`http_query_handler.cpp:820-825`; the `seriesKey` param
   is confirmed unused in `query_runner.cpp`).
3. **SIMD gaps** — STDDEV/STDVAR fall to scalar Welford while a tested `CalculateVariance` kernel
   sits dead (`block_aggregator.hpp:504`); vector division has no SIMD path
   (`expression_evaluator.cpp:70`).

---

## 2. Top Quick Wins (high-impact + small/medium effort)

| # | Title | Path | File:line | Impact | Effort | One-line change |
|---|-------|------|-----------|--------|--------|-----------------|
| 1 | Timestamp/int64/bool block encode allocates aligned buffer + double-copies | insert | `tsm_writer.cpp:114` | High | Small | Use `IntegerEncoder::encodeInto(..., buffer)` with a back-patched length placeholder |
| 2 | JSON batch fans out cross-shard per-MWP instead of per-request | insert | `http_write_handler.cpp:1664` | High | Medium | Hoist per-shard accumulation into the batch driver; dispatch each shard once |
| 3 | Eager `seriesKey` built in discovery, copied through pushdown, never read | query | `http_query_handler.cpp:821` | Medium | Medium | Drop `seriesKey` param from `queryAggregated`/`queryTsmAggregated`; build lazily |
| 4 | Discovery deep-copies tags-map/field per series (movable source) | query | `http_query_handler.cpp:824` | Medium | Small | Iterate owned vector by non-const ref and `std::move` `tags`/`field`/`measurement` |
| 5 | SeriesId128 re-derived (build+hash) up to 3× per new series | insert | `native_index.cpp:665` | Medium | Small | Add `SeriesId128` to `MetadataOp`; pass precomputed id to a `getOrCreateSeriesId` overload |
| 6 | Per-point series-key copied on happy path only for an error log | insert | `memory_store.cpp:429` | Medium | Small | `const std::string& key = ...seriesKey();` |
| 7 | `writeIndexBlock` runs `minmax_element` on already-sorted block | insert | `tsm_writer.cpp:204` | Medium | Small | Use `front()`/`back()` and `0`/`n-1` (caller already sorted) |
| 8 | STDDEV/STDVAR fall to scalar; SIMD `CalculateVariance` is dead code | query | `block_aggregator.hpp:504` | Medium | Medium | Add STDDEV/STDVAR case calling `simd::calculateVariance` + `mergeWelford` |
| 9 | Vector/vector division has no SIMD path (add/sub/mul do) | query | `expression_evaluator.cpp:70` | Medium | Small | Add `anomaly::simd::vectorDivide` Highway kernel and dispatch above `kSimdMinSize` |
| 10 | `compressStrings` allocates a fresh 4KB-aligned buffer per call | insert | `string_encoder.cpp:130` | Medium | Small | Stage into a `static thread_local std::vector<uint8_t>` (mirror `tlCompBuf`) |
| 11 | `coalesceWrites` builds heap context strings per tag/field on valid path | insert | `http_write_handler.cpp:745` | Medium | Small | Guard with `isValidName`/`isValidTagValue`; build context only on failure |
| 12 | `findOrCreateCandidate` hashes/probes robin_map twice | insert | `http_write_handler.cpp:624` | Low | Small | `try_emplace`; init+move on inserted |
| 13 | `writeIndex` allocates a 16-byte string per series via `toBytes()` | insert | `tsm_writer.cpp:405` | Low | Small | `buffer.write_bytes(seriesId.getRawData().data(), 16)` |
| 14 | `queryTsm` rescans all blocks for min/max despite sorted input | query | `query_runner.cpp:315` | Low | Small | `blocks.front()->ts.front()` / `blocks.back()->ts.back()` |
| 15 | `parseAggregation` takes a heap `substr` of the query string | query | `query_parser.cpp:41` | Low | Small | Change param to `std::string_view`; pass zero-copy substr |

---

## 3. Larger Investments (high-impact / large-effort)

### ALP float encode is 100% scalar — no Highway SIMD on the encode hot loop
**`lib/encoding/alp/alp_encoder.cpp:265`** (impact: high, effort: large). `scaleValue()` runs per
value (`value*FACT_ARR[exp]`, `std::round`, two overflow compares, int64 cast, integer divide,
verify multiply + `==`) and is re-run during delta-benefit sampling (`:189`) and across ~190
`(exp,fac)` pairs in `findBestExpFac`. The only Highway kernel in `alp/` is the decode-side
`AlpReconstructKernel`; encode exports nothing. **Fix:** add a Highway kernel that loads doubles,
multiplies by frac/fact vectors, `hn::Round`, converts to int64, divides/multiplies-back and
compares against the original to mask exceptions — emitting the exact-roundtrip mask + scaled
int64s with a scalar tail. Dominant per-block float-encode cost; reuses the int64↔double conversion
pattern already proven on decode.

The two highest-impact non-large items — per-MWP fan-out consolidation
(`http_write_handler.cpp:1664`) and concurrent metadata resolve (`native_index.cpp:2197`) — are
medium-effort but warrant the same design attention.

---

## 4. Per-Path Detail

### INSERT

#### HTTP write + JSON/line parsing (`http_write_handler.cpp`)
- **Validation context strings built on the valid path — `:745` (also `:749`, `:787`)** *(med/small)*.
  `coalesceWrites` passes eagerly-concatenated context strings to `validateName`/`validateTagValue`
  for every tag and field, allocating a fresh `std::string` even though the validator's common path
  returns `{}`. The DOM sibling `parseMultiWritePoint` (`:257-289`) already guards with allocation-free
  `isValidName()`/`isValidTagValue()` first. **[APPLIED]**
- **`findOrCreateCandidate` double hash/probe + 3rd key copy — `:624`** *(low/small)*. `find` then
  `[]` re-hashes/re-probes; `try_emplace` collapses to one. **[APPLIED]**
- **Per-MWP `std::unordered_set` field dedup is node-based and usually empty — `:1070`** *(low/small)*.
  Distinct fields → distinct series IDs, so the set rarely catches a duplicate. Switch to
  `tsl::robin_set<SeriesId128>` + `reserve`, or drop and rely on `indexMetadataSync` dedup.
- **String field values copied (not moved) out of the JSON DOM — `:877` (scalar lambda `:592`)**
  *(low/medium)*. Where the doc is exclusively owned, iterate by non-const ref and
  `std::move(elem.get_ref<std::string>())`.
- **MetaOp construction deep-copies the entire tag map per new series — `:1151` (`:1176/1205/1230`)**
  *(med/medium)*. Have `MetadataOp` hold a `shared_ptr<const std::map<...>>` (or per-batch tag-map
  pool), or build once per point and share/move.

#### Engine insert orchestration (`http_write_handler.cpp` / `engine.cpp`)
- **JSON batch fans out cross-shard per-MWP instead of per-request — `http_write_handler.cpp:1664`**
  *(HIGH/medium)*. K MWPs → K independent fan-outs, each allocating `4×shardCount` vectors and doing
  its own `dispatchShardInserts` + `when_all_succeed`. **Fix:** hoist per-shard accumulation into the
  batch driver — one `vector<vector<TimeStarInsert<T>>>(shardCount)` set across all MWPs, dispatch each
  shard once. Collapses K fan-outs to 1/shard, gives large consolidated WAL batches.
- **`processMultiWritePoint` allocates `4×shardCount` nested vectors per MWP — `:1092`** *(med/medium)*.
  Subsumed by the consolidation above; else special-case single-field/single-shard MWPs or use a sparse
  `tsl::robin_map<unsigned, ShardBatch>`.
- **`indexMetadataSync` re-derives `SeriesId128` from scratch per MetaOp — `engine.cpp:279`** *(med/small)*.
  Add `SeriesId128 seriesId` to `MetadataOp`, populate at construction, reduce grouping to
  `routeToCore(op.seriesId)`.

#### NativeIndex write — series-id, bitmaps, HLL (`native_index.cpp`)
- **Merged — series-key hashed up to 3× per new series (`:665`).** Routing at
  `http_write_handler.cpp:1131`, re-routing at `engine.cpp:279-280`, then `getOrCreateSeriesId`
  rebuilding `buildSeriesKey`+`fromSeriesKey` again before `seriesCacheContains`. **Fix:** plumb the
  computed `SeriesId128` through `MetadataOp` + a precomputed-id overload (skips `:665-666`).
- **`getOrCreateSeriesId` takes measurement/tags/field by value — `:659`** *(med/medium)*. Deep-copies
  on every op including warm-cache early-returns (`:670/677`). Take by const-ref / span; copy only at
  the cold `SeriesMetadata{...}` construction (`:687`).
- **New-series path walks the tag map three times — `:699`** *(med/medium)*. Bitmap, `updateTagHLL`
  (rebuilds the byte-identical cache key at `:2261-2267`), and tag-metadata loops. Fuse bitmap-add +
  tag-HLL-add reusing one scratch cache-key string.
- **`indexMetadataBatch` serially awaits per op — `:1035`** *(med/medium)*. 2N strictly-serialized
  suspensions; dedup `(measurement,field)→type` so `setFieldType` fires once per distinct field; batch
  per-op checks into one `IndexWriteBatch`.
- **Index KV entries copied into `std::string` twice (batch → memtable) — `write_batch.cpp:12`**
  *(low/medium)*. Add a consuming `applyTo(&&)` that `std::move`s key/value into the memtable.

#### MemoryStore insert (`memory_store.cpp` / `.hpp`)
- **Per-point series-key copied only to service an error log — `:429`** *(med/small)*. Bind
  `const std::string&`. **[APPLIED]**
- **`InMemorySeriesStats::update` makes 3 separate SIMD passes over the same array — `:28`** *(med/medium)*.
  Fuse sum/min/max into one Highway pass.
- **Two scalar loops over the batch (Welford, then first/latest) — `:44`** *(low/small)*. Fuse into one pass.
- **Series robin_map never reserved → repeated rehash of 168-byte slots — `memory_store.hpp:101`**
  *(med/small)*. `reserve()` from the previous store's final size.
- **`mergePaired`/`sortPaired` allocate fresh temporaries per out-of-order insert — `:194`** *(low/medium)*.
  Hold reusable per-shard scratch buffers; `resize()` not reallocate.

#### TSM writer / rollover (`tsm_writer.cpp`, `wal_file_manager.cpp`)
- **Merged — block ts/int64/bool encode allocates aligned buffer + double-copies (`:114/158/138`)**
  *(HIGH/small)*. Every block calls `IntegerEncoder::encode(...)` → fresh page-rounded `AlignedBuffer`,
  then `buffer.write(...)` copies again. Use `encodeInto` (write a length placeholder, encode in place,
  back-patch with `writeAt<uint32_t>`). On-disk layout byte-identical. **[APPLIED for ts + int64; bool
  vector reconstruction deferred]**
- **TSM bool path reconstructs a `std::vector<bool>` per block — `:138`** *(low/medium)*. Add a
  `BoolEncoderRLE::encodeInto(const vector<bool>&, offset, count)` overload using the libstdc++
  word-level fast path.
- **`writeIndexBlock` recomputes min/max via `minmax_element` despite sorted input — `:204`** *(med/small)*.
  `writeAllSeries` calls `series.sort()` before `writeSeries`, so each block is ascending. Use
  `front()`/`back()`. *(Deferred — needs an audit of all `writeSeries` callers / a sortedness assert.)*
- **TSMWriter output buffer never reserved → ~12-16 reallocs per flush — `:707`** *(med/medium)*. Plumb a
  size hint sized to ~0.3-0.5× the `totalMemoryEstimate` already computed in `convertWalToTsm`.
- **`convertWalToTsm` iterates every series for a log-only memory estimate — `wal_file_manager.cpp:429`**
  *(low/small)*. Gate behind log level, or repurpose the estimate as the `reserve()` hint above.
- **`writeIndex` allocates a 16-byte string per series via `toBytes()` — `:405`** *(low/small)*. Use
  `write_bytes(seriesId.getRawData().data(), 16)`. **[APPLIED]**
- **`writeCompressedBlockWithStats` does a `std::map` lookup per block (compaction) — `:364`** *(low/medium,
  background)*. Resolve the entry once via `writeSeriesBegin`.

#### Numeric encoders (`alp/alp_encoder.cpp`)
- **ALP float encode fully scalar — `:265`** *(HIGH/large)*. See §3.
- **ALP_DELTA rescans the whole block for min/max after zigzag — `:323`** *(low/small)*. Track running
  min/max inside the delta+zigzag loop.
- **`findBestExpFac` heap-allocates a `sample_indices` vector per encode — `:66`** *(low/small)*. Compute
  the index inline.

#### String encoder + dictionary (`string_encoder.cpp`, `tsm_writer.cpp`, `wal.cpp`)
- **`compressStrings` allocates a fresh 4KB-aligned buffer per call — `string_encoder.cpp:130`** *(med/small)*.
  `static thread_local std::vector<uint8_t>` (zstd reads a raw pointer — no DMA alignment). **[APPLIED]**
- **Dictionary idMap rebuilt + dictionary deep-copied per block — `string_encoder.cpp:513`** *(med/medium)*.
  Build the `string_view→ID` map once per series; pass by const-ref.
- **TSM string block path uses return-by-value `encode()` then copies — `tsm_writer.cpp:146`** *(med/medium)*.
  Add `encodeInto`/`encodeDictionaryInto` overloads.
- **`encodeDictionary` allocates per-block ID staging + result buffers — `string_encoder.cpp:522`** *(low/small)*.
  Stage IDs into thread-local; combine with `encodeDictionaryInto`.
- **`buildDictionary` copies every unique string twice — `string_encoder.cpp:437`** *(low/small)*. Key the
  dedup map with `string_view` into the reserved `dict.entries`.
- **WAL string insert does a redundant size pass — `wal.cpp:452`** *(low/small)*. Derive raw size from
  the `uncompressedSize` already returned by `compressStrings`.

### QUERY

#### HTTP query handler + scatter-gather (`http_query_handler.cpp`)
- **Merged — discovery loop copies tags-map/field/measurement per series + builds a never-read `seriesKey`
  (`:820-825`).** For the time-scoped path, `swmPtr` points at an owned, soon-discarded vector — the copies
  are gratuitous. **Fix:** iterate the owned vector by non-const ref and `std::move` `tags`/`field`/
  `measurement` (after `buildSeriesKey` reads them); keep copies only for the shared-cache branch.
- **Merged — eager `seriesKey` is dead weight on the pushdown path (`:821`).** Flows to `queryAggregated`
  → another copy in `queryTsmAggregated`, whose `std::string seriesKey` param is **never referenced**.
  ~3 allocs/moves per series, needed only by the rare fallback. **Fix:** stop building it eagerly; accept
  only `SeriesId128`; reconstruct lazily on `nullopt`.
- **Pattern-scope post-filter uses uncompiled `SeriesMatcher::matches` — `:817`** *(low/small)*. Compile each
  pattern scope to `std::regex` once before fan-out; use the precompiled-scopes overload.

#### Query + expression parser (`series_matcher.cpp`, `query_parser.cpp`, `expression_parser.cpp`)
- **Pattern-scope matching re-classifies/re-hashes every scope per series — `series_matcher.cpp:66`** *(med/medium)*.
  Classify each scope once before the per-series loop; use the precompiled overload (`:35`).
- **`matchesTag` allocates a `std::string` per series for `~regex`/`/regex/` — `series_matcher.cpp:69`** *(med/small)*.
  Strip delimiters once per query; use `string_view` + heterogeneous regex-cache lookup.
- **`parseAggregation` receives a heap `substr` — `query_parser.cpp:41`** *(low/small)*. Change the param to
  `std::string_view`; pass a zero-copy `string_view` substr.
- **`parseExpression` calls `getPrecedence()` twice per binary operator — `expression_parser.cpp:504`** *(low/small)*.
  Cache once, refresh after `advance()`.

#### Query runner core + series matching (`query_runner.cpp`, `tsm.cpp`)
- **Gate 1 unconditionally probes double, then int64, then bool memory stores — `query_runner.cpp:730`** *(med/medium)*.
  Thread the resolved `TSMValueType` into `queryTsmAggregated` and probe only the matching store. Same triple
  pass in `aggregateMemoryStores` (`:663-668`).
- **O(F²×N) file-has-data linear scan in batch aggregation — `query_runner.cpp:1393`** *(med/small)*. Replace
  `filesWithData` with a `flat_hash_set<const TSM*>` or invert the loop so Gate 2 emits per-file lists.
- **`mergeSmallNSpans` re-scans all spans to find the minimum per emitted point — `query_runner.cpp:147`** *(low/medium)*.
  Cache each active span's head timestamp, updated only on advance.
- **`queryTsm` recomputes per-file min/max by scanning all blocks despite sorted input — `query_runner.cpp:315`** *(low/small)*.
  Use `blocks.front()`/`back()`.

#### Aggregators — scalar/SIMD/block/streaming (`block_aggregator.hpp`, `aggregator.hpp`, `tsm.cpp`)
- **STDDEV/STDVAR fully scalar; SIMD `CalculateVariance` is dead code — `block_aggregator.hpp:504`** *(med/medium, SIMD)*.
  `simd::CalculateVariance` (`simd_aggregator.cpp:105`) has zero callers. Add STDDEV/STDVAR case: SIMD block
  sum-of-squared-diffs + fold via `mergeWelford`.
- **Scalar `addValueForMethod` recomputes mean via a division on every AVG/SUM point — `aggregator.hpp:139`** *(low/small)*.
  Drop the per-point mean update; derive lazily in `getValue(AVG)`.
- **Integer/Bool block decode does scalar value→double before the SIMD aggregator — `tsm.cpp:138`** *(low/medium, SIMD)*.
  Index-assign after `resize`; SIMD-vectorize int64→double (Highway `ConvertTo<double>`); bool→double via `select`.
- **Tombstoned block aggregation fully scalar with per-point binary search — `tsm.cpp:1350`** *(low/medium, SIMD)*.
  Two-pointer cursor over tombstone ranges → maximal surviving runs → `addPointsRange`.

#### NativeIndex discovery / sstable / bloom (`native_index.cpp`, `sstable.cpp`)
- **Per-series metadata resolve fully serialized — N sequential SSTable walks per discovery — `native_index.cpp:2197`**
  *(HIGH/medium, async-io)*. `findSeriesWithMetadataTimeScoped` does serialized `co_await kvGet(key)` per
  cache-missed series; same in `findSeriesWithMetadata` (`:1120/1195`). **Fix:** collect all missed keys (no
  awaits), resolve concurrently via `seastar::max_concurrent_for_each(missKeys, 32, ...)`, or a single
  prefix-range `MergeIterator` sweep over the sorted SERIES_METADATA keys.
- **Discovered-series context build deep-copies measurement/tags/field — `http_query_handler.cpp:820`** *(med/small)*.
  Move on the owned (non-cached) branch.
- **`SSTableReader::get` copies the value into a `std::string` even for re-parsing callers — `sstable.cpp:636`** *(low/medium)*.
  Add a zero-copy accessor returning a `string_view` backed by a retained block handle for bitmap `readSafe`.

#### Expression / derived query evaluator (`expression_evaluator.cpp`, `streaming_derived_evaluator.cpp`)
- **Vector/vector division has no SIMD path while add/sub/mul do — `expression_evaluator.cpp:70`** *(med/small, SIMD)*.
  Add `anomaly::simd::vectorDivide` (Highway `Div`); ratio formulas are extremely common.
- **`evaluateNode` returns `AlignedSeries` by value → QueryRef leaf copies the whole series — `:1061`** *(med/medium)*.
  Read the `QueryResultMap` entry directly for `QUERY_REF` operands instead of round-tripping by-value.
- **Cross-series aggregation uses cache-unfriendly column-wise inner loop with no SIMD — `:1514`** *(med/medium)*.
  Transpose (outer over series, inner over points) so each buffer streams sequentially; Highway masked add/Min/Max.
- **Streaming evaluator rebuilds `std::set`/`std::map` per `closeBuckets` flush — `streaming_derived_evaluator.cpp:59`**
  *(med/medium)*. Replace the set with reserved sorted vector + unique; merge parallel sorted vectors via two-pointer.

---

## 5. Themes (cross-cutting patterns)

- **`encodeInto` exists but isn't used everywhere.** The single biggest recurring insert hotspot is "allocate
  a fresh 4KB-page-aligned `AlignedBuffer`, encode into it, then `buffer.write()` it a second time." Float VALUE
  already adopted `encodeInto`; timestamp (100% of blocks), int64, bool, and string paths did not. *(ts + int64 +
  string-staging now converted; bool-vector and TSM string `encodeInto` remain.)*
- **Tag-map (`std::map<string,string>`) deep-copies are the recurring per-series cost on both paths.** Insert
  clones per MetaOp + by-value into `getOrCreateSeriesId`; query clones per discovered series + downstream. Fix
  everywhere: const-ref, move from owned, or `shared_ptr`/interning.
- **Series-key build + XXH3-128 hash computed redundantly along the metadata path.** Up to 3× per new series, plus
  an eager-but-dead `seriesKey` on the query side. Plumb the computed `SeriesId128` through structs.
- **Recompute-over-sorted-input.** `minmax_element` per block (`tsm_writer.cpp:204`), per-file min/max scan
  (`query_runner.cpp:315`), ALP_DELTA min/max rescan (`alp_encoder.cpp:323`). All reduce to `front()`/`back()` or
  inline tracking.
- **SIMD gaps where the kernel exists or is trivial.** Dead `CalculateVariance`; missing vector divide; scalar
  cross-series aggregation, int/bool decode, tombstoned aggregation; scalar ALP encode; 3 separate SIMD stat
  passes that should fuse (`memory_store.cpp:28`).
- **`await` serialization on cold metadata paths.** Query resolve (`native_index.cpp:2197`, N sequential `kvGet`)
  and insert `indexMetadataBatch` (`native_index.cpp:1035`, 2N serial awaits) stall where
  `max_concurrent_for_each` or a single prefix-range sweep would overlap.
- **Per-call container allocation that should be reused/sparse.** `4×shardCount` nested vectors per MWP, node-based
  dedup sets, unreserved maps, per-out-of-order merge scratch, `findBestExpFac` sample vector, per-flush set/map
  rebuilds — all churn the heap on hot paths.

---

# Addendum — Re-Reviewed Components (WAL, TSM-read, Align/Tombstone)

Three components were independently re-reviewed and triple-checked after rate-limit deaths in the main pass.

## Quick-Wins Table

| Title | Path | File | Impact | Effort | One-line change |
|---|---|---|---|---|---|
| int64 zigzag scratch heap-alloc + zeroed per WAL entry | insert | `wal.cpp:464` | medium | small | thread-local `vector` + `resize(count)` (`encodeInsertEntry` has no co_await) **[APPLIED]** |
| Integer pushdown decode: 2 passes + scalar zigzag in fold loop | query | `tsm.cpp:139` | medium | medium | Decode only `[nSkipped..)`; bulk `zigzag_simd::zigzagDecodeSIMD` |
| Tombstone decode does `upper_bound` per point (5 copies) | query | `tsm.cpp:1352/1373/1395/1492/1622` | medium | medium | One monotonic two-pointer sweep (O(N+T)) |
| Non-pushdown raw read copies every point twice | query | `query_runner.cpp:362` | medium | large | Decode single-source raw queries directly into `QueryResult` |
| AlignedBuffer min alloc is a full 4096B page | insert | `aligned_buffer.hpp:44` | low | medium | Buffer pool, or non-DMA buffer for WAL (alignment unused) |
| Boolean pushdown decode expands `vector<bool>`→double per point | query | `tsm.cpp:165` | low | small | COUNT folds timestamps-only; use trueCount/blockCount stats |
| COUNT-only bucketing: division + hash probe per timestamp | query | `block_aggregator.hpp:174` | low | small | Monotonic-boundary run batching in `addTimestampsOnly` |
| `getTombstoneRanges` allocates + re-merges per call | query | `tsm_tombstone.cpp:513` | low | small | Cache merged ranges per seriesId; return const ref/span |
| SeriesAligner intersect/union reallocs per series | query | `series_aligner.cpp:90` | low | small | Ping-pong scratch buffers; in-place two-pointer intersection |
| Per-entry CRC32 scalar slicing-by-8 | insert | `crc32.hpp` (call `wal.cpp:484`) | low | large | Leave as-is: HW CRC32C is polynomial-incompatible with on-disk format |

### WAL Append
- **int64 zigzag scratch heap-allocated AND zeroed per entry — `wal.cpp:464`.** `std::vector<uint64_t>
  zigzagScratch(count)` value-inits to zero, then `zigzagEncodeInto` overwrites all. `encodeInsertEntry`
  (379–486) has no `co_await` — synchronous, single-threaded per shard. **Fix:** `static thread_local` +
  `resize(count)`. **[APPLIED]**
- **AlignedBuffer minimum allocation is a full 4096B page — `aligned_buffer.hpp:44`.** Allocator rounds up to a
  4096 multiple; smallest WAL buffer backs a full page. **Fix:** WAL buffer pool, or non-DMA buffer (alignment
  buys nothing — `output_stream` copies anyway).
- **Per-entry CRC32 scalar slicing-by-8 — `crc32.hpp` (call `wal.cpp:484`).** On-disk WAL uses reflected
  ISO-HDLC `0xEDB88320`; HW `crc32` instructions are CRC32C only. Leave as-is unless a deliberate WAL
  format-version bump switches to CRC32C.

### TSM Read / Decode
- **Integer pushdown decode: two materializing passes + scalar zigzag — `tsm.cpp:139`.** Decodes all raw values
  incl. the `nSkipped` prefix, then scalar `zigzagDecode` per element into a second vector. **Fix:** decode only
  `[nSkipped, nSkipped+nTimestamps)`, bulk `zigzag_simd::zigzagDecodeSIMD`. Same pattern in `readSingleBlock`
  (~851), `decodeBlock` (~1017), `decodeBlockFlat` (~1074).
- **Tombstone-filtered decode does `std::upper_bound` per point — `tsm.cpp:1352/1373/1395`.** Block timestamps
  sorted, ranges sorted+merged → one linear sweep (O(N+T)). Factor the 3 duplicated loops into one templated helper.
- **Boolean pushdown decode expands `vector<bool>`→double per point — `tsm.cpp:165`.** COUNT → timestamps-only;
  SUM/AVG → `trueCount`/`blockCount` stats for full blocks; partial blocks fold directly.
- **COUNT-only bucketed aggregation: division + hash lookup per timestamp — `block_aggregator.hpp:174`.** Apply
  `addPoints()`'s monotonic-boundary run batching to `addTimestampsOnly`.
- **Non-pushdown raw read copies every decoded point twice — `query_runner.cpp:362`.** Decode directly into the
  destination `QueryResult`; bulk-append bool blocks instead of per-element `push_back`.

### Series Alignment + Tombstone Application
- **Per-point `std::upper_bound` tombstone check across 5 decode loops — `tsm.cpp:1352/1373/1395/1492/1622`.**
  Replace with a monotonically-advancing range cursor like `queryWithTombstones` already does. O(N·log T) → O(N+T).
- **`getTombstoneRanges` allocates + re-merges a fresh vector per call — `tsm_tombstone.cpp:513`.** Cache merged
  ranges per `seriesId` (invalidate on addTombstone/compact/load); return a const ref/span.
- **SeriesAligner N-way intersection/union reallocates per iteration — `series_aligner.cpp:90`.** Ping-pong two
  reusable scratch buffers; for intersection, in-place two-pointer compaction avoids the second buffer.
