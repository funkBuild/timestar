# TimeStar Codebase Review - March 19, 2026

57 review agents examined every source file in the codebase. This report consolidates all findings, organized by severity.

---

## CRITICAL (5 issues)

### 1. WAL: Thread-local buffer race condition causes silent data loss
**File:** `lib/storage/wal.cpp:496-556`

The `static thread_local AlignedBuffer buffer` is populated **before** the `_io_sem` semaphore is acquired. If Insert A encodes into the buffer then suspends at `co_await get_units(_io_sem, 1)`, Insert B overwrites the same buffer. When Insert A resumes, it writes Insert B's data. **Insert A's original data is silently lost.**

**Fix:** Move encoding inside the semaphore-protected section, or use a per-coroutine stack-local buffer.

### 2. TSM Compactor: Index misalignment corrupts compaction output
**File:** `lib/storage/tsm_compactor.cpp:679-704`

`processSeriesForCompaction` builds `sourceIndexBlocks` from ALL source files, but `allBlocks` from `BulkBlockLoader::loadFromFiles` SKIPS files where the series has no data. When a series exists in only some files, blocks get paired with wrong index metadata -- wrong time ranges, wrong stats, wrong compressed data.

**Fix:** Build `sourceIndexBlocks` only for files that appear in `allBlocks`, or store source file pointer in `SeriesBlocks`.

### 3. Shard Rebalancer: `moveNativeIndex()` copies stale/incomplete index data
**File:** `lib/storage/shard_rebalancer.cpp:388-415`

After rebalancing, series are rerouted across shards. The copied index contains metadata for series that no longer live on that shard and is missing metadata for series moved in from other shards. Causes query discovery misses and ghost results.

**Fix:** Skip index copy and add an index rebuild step that scans each new shard's TSM files and re-indexes.

### 4. TSM: Stale `tlStringDict` in `readBlockBatch` path
**File:** `lib/storage/tsm.cpp:1206,1177`

`readSeriesBatched` sets thread-local `tlStringDict` then does `co_await dma_read_exactly`. During suspension, another coroutine overwrites `tlStringDict`. The original coroutine resumes and decodes string blocks with wrong dictionary. Same bug class was fixed in `readSingleBlock` but not applied here.

**Fix:** Capture `tlStringDict` into a local variable before the `co_await`.

### 5. TSM Compactor: `activeCompactions` never removed on exception
**File:** `lib/storage/tsm_compactor.cpp:1223-1265`

If `compact()` or related calls throw, the entry added to `activeCompactions` is never removed, permanently preventing those files from ever being compacted again.

**Fix:** Use a scope guard (`seastar::defer`) to remove the entry on both success and failure.

---

## HIGH (12 issues)

### 6. TSM Merge Iterators: `TwoWayMergeIterator::next()` doesn't dedup equal timestamps
**File:** `lib/storage/tsm_merge_specialized.hpp:78-137`

When both sources have the same timestamp, `next()` picks source 1 and advances only source 1. Source 0 still sits at the same timestamp, producing duplicates. The `nextBatch()` dedup logic is also broken.

### 7. TSM Merge Iterators: `FourWayMergeIterator` infinite loop when files.size() < 4
**File:** `lib/storage/tsm_merge_specialized.hpp:185-225`

Default-constructed `Source` entries have `exhausted = false`, so `hasNext()` always returns true for unused sources.

### 8. TSM Block Iterator: Destroying with outstanding prefetch crashes Seastar
**File:** `lib/storage/tsm_block_iterator.hpp:27`

If abandoned mid-iteration, the outstanding `prefetchFuture` triggers Seastar's "future destroyed without being awaited" assertion.

### 9. Query Runner: Bucketed MEDIAN silently produces NaN via pushdown
**File:** `lib/query/query_runner.cpp:656`

Gate 0.5 only rejects non-bucketed MEDIAN. Bucketed MEDIAN passes through to pushdown where `collectRaw=false`, so `rawValues` is never populated and `getValue(MEDIAN)` returns NaN for every bucket.

### 10. Engine Metrics: `.get()` on coroutine future in Prometheus gauge
**File:** `lib/core/engine_metrics.cpp:36`

`getSeriesCount().get()` is called synchronously in a Prometheus gauge lambda. If `getSeriesCount` ever adds a `co_await`, this will crash.

### 11. Function Framework: `interpolateLinear` crashes on empty input
**File:** `lib/functions/function_types.hpp:469`

When input is empty, `input.valueAt(input.size() - 1)` becomes `input.valueAt(SIZE_MAX)` -- out-of-bounds access.

### 12. Function HTTP Handler: `handleMultiSeriesOperation` returns fake data
**File:** `lib/functions/function_http_handler.cpp:757-915`

Returns hardcoded sine wave data instead of querying the engine. Silently misleads users.

### 13. Interpolation: SplineInterpolation division by zero on equal timestamps
**File:** `lib/functions/interpolation_functions.cpp:321`

No guard for `t2 == t1` in the ratio computation.

### 14. Config: `auth_enabled`/`auth_token` missing from Glaze meta
**File:** `lib/config/timestar_config.hpp:139-142`

Authentication can never be configured via config file or environment variables.

### 15. Shard Rebalancer: State files lack `fsync`
**File:** `lib/storage/shard_rebalancer.cpp:56-64,80-89`

`flush()` doesn't guarantee data reaches disk. Power failure could lose state, breaking crash recovery.

### 16. Shard Rebalancer: `completeCutover()` leaves orphaned directories on scale-down
**File:** `lib/storage/shard_rebalancer.cpp:449-457`

Old shards beyond `newShardCount` are never renamed to `_old` and never cleaned up.

### 17. Manifest: V1 backward-compat parsing corrupts multi-file snapshots
**File:** `lib/index/native/manifest.cpp:335-339`

For V1 snapshot records with multiple files, the parser consumes the second file's `fileNumber` bytes as `writeTimestamp`, corrupting all subsequent entries.

---

## MEDIUM (30+ issues - top highlights)

| File | Issue |
|------|-------|
| `http_write_handler.cpp:769` | Array field coalesce Integer->Float promotion writes to wrong vector |
| `index_wal.cpp:188` | `writePos_` double-counts tail buffer bytes across flushes |
| `index_wal.cpp:196-213` | Blocking `std::filesystem` calls in Seastar coroutines |
| `memory_store.cpp:300` | `insert()` silently moves from lvalue-ref parameter |
| `alp_decoder.cpp:112-280` | ~28-46KB stack per block iteration, risky on 128KB Seastar fiber stacks |
| `string_encoder.cpp:217` | Decompressed size not validated against header claim |
| `string_encoder.cpp:432` | `deserializeDictionary` unbounded `reserve()` from untrusted data |
| `tsm_file_manager.cpp:74-86` | Duplicate `rankAsInteger` leaves orphaned file in tiers[] |
| `tsm_file_manager.cpp:236-243` | Double `stopCompactionLoop()` is UB (moved-from future) |
| `tsm_file_manager.cpp:229-234` | `startCompactionLoop` overwrites without awaiting (future leak) |
| `compaction.cpp:106-113` | Orphaned SSTable files when all inputs empty |
| `compaction.cpp:120-183` | No exception safety for SSTableWriter (partial file leak) |
| `compaction.cpp:58-64` | No L1+ compaction (unbounded L1 file accumulation) |
| `sstable.cpp:444` | Unbounded `reserve()` from corrupted index entry count (OOM DoS) |
| `sstable.cpp:504` | Synchronous `pread()` blocks Seastar reactor |
| `manifest.cpp:273-274` | No `ifstream` error checking in `recover()` |
| `manifest.cpp:127-129` | `::write()` doesn't handle `EINTR` or short writes |
| `query_parser.cpp:174-199` | Measurement names can contain embedded whitespace |
| `float_encoder - alp_decoder.cpp` | No bounds validation on exp/fac/bw/block_count from headers |
| `bool_encoder_rle.cpp:42-49` | `readVarint` silently returns garbage on truncated input |
| `timestar_config.cpp:269-276` | `envU16`/`envU32` silently truncate on overflow |
| `aligned_buffer.hpp:40` | Allocator has no overflow check on `n*sizeof(T)` |
| `compressed_buffer.cpp:32` | `write()` doesn't mask value to bits width |
| `function_security.cpp:337-339` | `sanitizeInput` truncates JSON to 2000 chars even for 5000 limit |
| `function_query_parser.cpp:47-66` | Pipeline parser doesn't preserve execution order |
| `streaming_aggregator.hpp:26-42` | NaN permanently poisons BucketState sum |
| `series_aligner.cpp:40-43` | `align()` copies timestamp vectors N times (shared_ptr support exists) |
| `subscription_manager.cpp:224-230` | Out-of-bounds access when timestamps/values sizes differ |
| `key_encoding.cpp:348-392` | Missing null-byte validation in Phase 2-4 encoding functions |
| `timestar_value.hpp:70-73` | `addValue` doesn't invalidate `_cachedEstimatedSize` |
| `function_http_handler.cpp:468` | `multiply` validation checks wrong parameter name |
| `http_query_handler.cpp:1193` | `createErrorResponse` silently discards error code parameter |

---

## Performance Highlights

| File | Issue |
|------|-------|
| `alp_encoder.cpp:449-759` | `encodeInto` is 300-line duplicate of `encode` (maintenance hazard) |
| `subscription_manager.cpp:228` | Per-point tag map deep-copy on hot path |
| `streaming_aggregator.cpp:8-11` | Per-point string/map copies in `addPoint` |
| `http_metadata_handler.cpp:348-352` | Sequential field type lookups (should use `when_all_succeed`) |
| `smoothing_functions.cpp:114-155` | SMA pad modes are O(n*w) instead of O(n) |
| `bloom_filter.hpp:32-43` | `static` globals duplicated across 36+ TUs |
| `bulk_block_loader.hpp:122-137` | `loadFromFiles` loads files sequentially (should parallelize) |

---

## Dead Code Highlights

| File | Code |
|------|------|
| `tsm_compactor.cpp:58-630` | `mergeSeries`, `mergeSeriesBulk`, `mergeSeriesWithIterator` (~570 lines, never called) |
| `simple8b_extended.hpp` | Entire file (declared but never implemented) |
| `query_planner.cpp:15-52` | `createPlan` async path (dead in production) |
| `simd_aggregator.hpp:51-53,61-62` | `addArrays`/`minArrays`/`maxArrays`, `alignedAlloc`/`alignedFree` |
| `aggregator.cpp:134-202` | `mergeSortedRawValuesInto` |
| `bloom_filter.hpp:528-586` | `compressible_bloom_filter` class |

---

## Test Coverage Gaps (Critical Paths)

| Component | Gap |
|-----------|-----|
| **WAL** | No concurrent insert test (would catch the CRITICAL race) |
| **TSM Compactor** | No test where series exists in subset of source files (CRITICAL bug) |
| **Shard Rebalancer** | No end-to-end `execute()` test, no crash recovery test |
| **Memory Store** | Zero int64_t type coverage |
| **Integer Encoder** | Time-range filtered decode path untested |
| **Bool Encoder** | Skip-decode, word-boundary paths untested |
| **Function Framework** | `FunctionPipelineExecutor` has no unit tests |
| **STL Decomposition** | Zero standalone unit tests for this complex algorithm |
| **Periodicity Detector** | Zero standalone unit tests |
| **Query Runner** | `queryTsmAggregated` has no dedicated tests |
| **TSM Merge Iterators** | No dedup correctness tests |
| **NativeIndex** | `kvExists` optimization not tested |

---

## Summary Statistics

| Severity | Count |
|----------|-------|
| Critical | 5 |
| High | 12 |
| Medium | 30+ |
| Low | 100+ |
| Dead Code | 15+ items |
| Test Coverage Gaps | 40+ |

**Total files reviewed:** 166 source files across lib/, bin/, test/
**Review agents:** 57 parallel agents
