# TimeStar Codebase Review — 2026-03-19

## Executive Summary

Full codebase review across **59 review agents** covering every source file in `lib/`, `bin/`, and their corresponding tests. **250+ findings** identified across all severity levels.

**By Severity:**
- CRITICAL: 10 findings (data loss, UB, reactor blocking, ReDoS, crash recovery)
- HIGH: 45+ findings (incorrect results, resource exhaustion, missing safety, use-after-free)
- MEDIUM: 90+ findings (correctness edge cases, efficiency, fragile patterns)
- LOW: 140+ findings (style, dead code, minor efficiency, test gaps)

**Agents completed:** 57 of 59 (2 still running)

---

## CRITICAL Findings

### C1. Query Runner: Non-overlapping fast path drops entire memory store data
- **File:** `query_runner.cpp:477-501`
- **Issue:** When TSM result is empty and two memory spans exist (e.g., after rollover before TSM conversion), the fast path only appends spans[1]'s data, silently dropping spans[0] entirely.
- **Fix:** Guard fast path by checking `spans[0].valVec == &result.values` (confirming span 0 is TSM-backed).

### C2. Aggregator: SPREAD/STDDEV/STDVAR produce incorrect results in raw-value merge paths
- **File:** `aggregator.cpp:206-230, 270-286`
- **Issue:** `foldAlignedRawValues` and `nWayMergeRawValues` have no cases for SPREAD, STDDEV, STDVAR — they fall through to default which SUMs values.
- **Fix:** Route these methods through the AggregationState merge path, or add explicit fold logic.

### C3. SIMD Aggregator: `computeHistogram` scalar tail has UB from double→int overflow
- **File:** `simd_aggregator.cpp:196-199`
- **Issue:** Values outside `[min_val, max_val]` cause undefined behavior when casting negative double to int. SIMD path clamps correctly; scalar tail does not.
- **Fix:** Clamp the double before `static_cast<int>`.

### C4. Anomaly Detection: Incorrect MAD computation in robust detector
- **File:** `robust_detector.cpp:17-50`
- **Issue:** Computes `median(|residual|)` instead of `median(|residual - median(residual)|)`. For skewed distributions, produces incorrect scale estimates.
- **Fix:** Compute residual median first, then compute MAD from deviations around that median.

### C5. Smoothing Functions: Blocking `.get()` on future inside Seastar reactor
- **File:** `smoothing_functions.cpp:342`
- **Issue:** `validateParameters(context).get()` blocks the reactor thread, risking deadlock.
- **Fix:** Replace with inline parameter checks or use `co_await`.

### C6. Function Security: 30 `std::regex` patterns on every HTTP request hot path
- **File:** `function_security.cpp:345-351`
- **Issue:** `containsDangerousPatterns()` runs ~30 sequential regex evaluations per request. `std::regex` is notoriously slow in libstdc++.
- **Fix:** Replace simple patterns with `std::string::find()`. Reserve regex for complex patterns only.

### C7. Shard Rebalancer: PlacementTable not initialized — all series routed to shard 0
- **File:** `timestar_http_server.cpp:245-262`, `shard_rebalancer.cpp:227,299`
- **Issue:** Rebalancer runs at line 245 but `setGlobalPlacement()` is called at line 262. Default PlacementTable has `coreCount_=0`, so `routeToCore()` always returns 0. Every series is routed to shard 0 during rebalancing, completely defeating data distribution.
- **Fix:** Move `PlacementTable::buildLocal()` and `setGlobalPlacement()` to before the rebalancer block.

### C8. Index WAL: Only replays latest WAL generation — older files permanently lost
- **File:** `index_wal.cpp:200-226`
- **Issue:** `open()` scans for all `.wal` files but `replay()` only reads the maximum generation. If crash occurs between `rotate()` (creates gen N+1) and `deleteFile()` (deletes gen N), data in gen N is permanently lost.
- **Fix:** Replay all `.wal` files in ascending generation order.

### C9. Series Matcher: ReDoS bypass through subscription pre-compiled scopes
- **File:** `series_matcher.cpp:256` (`toRegexPattern`)
- **Issue:** `matchesRegex()` has thorough ReDoS protection (length limit, nested quantifier detection). But `toRegexPattern()` (used by subscription manager to pre-compile regexes) applies NO validation. A subscription with scope `~(a+)+` bypasses all guards and freezes the shard on crafted write input.
- **Fix:** Extract ReDoS validation into standalone `validateRegexPattern()` and call from both paths.

### C10. TSM: Use-after-free on LRU-evicted string dictionary pointer
- **File:** `tsm.cpp:830,912,1214,1257`
- **Issue:** `readSeriesBatched` sets `tlStringDict` to `&indexEntry->stringDictionary` from LRU cache. During DMA I/O suspension, concurrent coroutine can evict the entry, making both `localDict` and `tlStringDict` dangling pointers. Decode then reads freed memory.
- **Fix:** Copy the dictionary vector before any co_await, or use `shared_ptr` for dictionary storage.

---

## HIGH Findings

### Data Correctness

| # | File | Issue |
|---|------|-------|
| H1 | `aggregator.hpp:101` | Welford STDDEV tracks no dedicated mean — numerical instability for extreme value ranges |
| H2 | `aggregator.cpp:701-767` | Legacy `aggregate()` returns NaN for MEDIAN (never sets `collectRaw = true`) |
| H3 | `query_runner.cpp:724-812` | Pushdown aggregation double-counts when TSM and memory data overlap during rollover |
| H4 | `streaming_aggregator.hpp:21` | COUNT on string-only fields silently returns zero (skipped by `isStringOnly` check) |
| H5 | `tsm_writer.cpp:541-545` | Integer block stats: double→int64_t cast is UB when sum overflows INT64_MAX |
| H6 | `tsm_writer.cpp:308-328` | NaN in float blocks corrupts ALL block statistics (sum, min, max, mean, m2) |
| H7 | `derived_query_executor.cpp:330-339` | `convertQueryResponse` throws on `vector<bool>` variant (boolean fields break derived queries) |
| H8 | `derived_query_executor.cpp:319-343` | Silently returns empty result when requested field doesn't exist (hides user errors) |
| H9 | `block_aggregator.hpp:319` | SIMD fold paths skip NaN filtering, diverging from scalar path behavior |
| H10 | `robust_detector.cpp:17` (SIMD) | `computeAnomalyScores` propagates NaN instead of producing 0 for NaN inputs |
| H11 | `retention_handler.cpp:76` | Signed char comparison rejects valid UTF-8 measurement names |
| H12 | `linear_forecaster.cpp:152-255` | NaN/Inf not filtered in linear forecaster (Inf corrupts regression) |
| H13 | `seasonal_forecaster.cpp:387` | `vectorMean` on partially-NaN data corrupts seasonal forecaster pipeline |
| H14 | `expression_evaluator.cpp:1102-1118` | `time_shift` in binary ops produces misaligned timestamp arithmetic without detection |

### Safety / Resource Exhaustion

| # | File | Issue |
|---|------|-------|
| H15 | `alp_decoder.cpp:267-291` | ALP_RD decoder: ~44KB stack arrays risk overflow on Seastar 128KB fiber stacks |
| H16 | `tsm_compactor.cpp:435-439` | Blocking `fs::remove()` in destructor on Seastar reactor thread |
| H17 | `http_stream_handler.cpp:642` | Subscription limit check racy — no reservation mechanism |
| H18 | `http_stream_handler.cpp:499-515` | No limit on queries per multi-query subscription (DoS) |
| H19 | `http_stream_handler.cpp:720-756` | Backfill has no result-size limit (unbounded memory) |
| H20 | `http_write_handler.cpp:1843` | No limit on number of writes in batch array (memory amplification DoS) |
| H21 | `expression_parser.hpp:78-79` | No total expression size limit — can allocate 1GB+ heap |
| H22 | `tsm_file_manager.hpp` | No TSM file handle closure during shutdown (leaked Seastar file handles) |
| H23 | `tsm_file_manager.cpp:109-110` | Missing directory fsync after rename in writeMemstore |

### WAL / Recovery

| # | File | Issue |
|---|------|-------|
| H24 | `wal.cpp:934-945` | `DeleteRange` recovery lacks try-catch — aborts entire recovery on malformed entry |
| H25 | `wal.cpp:852-875` | Old-format WAL heuristic can misclassify valid entries as corrupt |
| H26 | `local_id_map.hpp:55-59` | Zero-initialized holes after partial restore cause garbage series IDs |

### Index / Storage

| # | File | Issue |
|---|------|-------|
| H27 | `tsm_compactor.cpp:658-693` | `getActiveCompactionStats()` returns zeroed stats (updates local copy, not vector element) |
| H28 | `tsm_compactor.cpp:426-428,620-647` | Double sequence ID allocation between plan and compact |
| H29 | `tsm_file_manager.cpp:78-86` | File descriptor leak on duplicate rank in openTsmFile |
| H30 | `aligned_buffer.hpp:65-68` | `dma_default_init_allocator::operator==` reports equality across different alignments |
| H31 | `aligned_buffer.hpp:105` | `std::_Bit_reference` is GCC-specific, non-portable |

### HTTP / Security

| # | File | Issue |
|---|------|-------|
| H32 | `function_http_handler.cpp:490` | Missing `jsonEscape` in error response (latent JSON injection) |
| H33 | `function_http_handler.cpp:226` | Error detection via fragile string search for `"success": false` |
| H34 | `function_monitoring.cpp:530-541` | `getCurrentMemoryUsage` throws on malformed `/proc/self/status` |
| H35 | `function_monitoring.cpp:81-84` | Exceptions silently swallowed when `detailed_logging_` is false |
| H36 | `http_query_handler.cpp:505` | Discovery phase has no timeout — can hang indefinitely |
| H37 | `http_query_handler.cpp:1182` | Internal exception messages leaked to clients |
| H38 | `index_wal.cpp:105-129` | Destructor O_APPEND writes after zero-filled gap — corrupts crash recovery |
| H39 | `index_wal.cpp:193-196` | `flushBuffer()` truncates file past DMA-written data — creates zero gap on disk |
| H40 | `shard_rebalancer.cpp:436-459` | No directory fsync after rename in performCutover — crash can lose renames |
| H41 | `shard_rebalancer.cpp:58-72` | Non-atomic file writes for state/metadata (truncate-then-write pattern) |
| H42 | `sstable.cpp:511` | Synchronous `pread()` blocks Seastar reactor thread on cache miss |
| H43 | `lru_cache.hpp:42-49` | `put()` update path does not enforce byte budget (can exceed indefinitely) |
| H44 | `json_escape.hpp:30` | DEL character (0x7F) not JSON-escaped per RFC 8259 |
| H45 | `aligned_buffer.hpp:105` | `std::_Bit_reference` is a GCC/libstdc++ internal — non-portable |
| H46 | `aligned_buffer.hpp:65-68` | `dma_default_init_allocator::operator==` cross-alignment equality (potential memory corruption) |

---

## MEDIUM Findings (Selected — 80+ total)

### Most Impactful

| File | Issue |
|------|-------|
| `engine.cpp:307` | Fire-and-forget `broadcastSchemaUpdate` not gate-protected (shutdown race) |
| `engine.cpp:928` | `sweepExpiredFiles` iterates TSM files without snapshot |
| `engine.cpp:848` | `loadAndBroadcastRetentionPolicies` dereferences `shardedRef` without null check |
| `query_parser.cpp:283` | Scope values containing commas silently split incorrectly |
| `query_parser.cpp:300` | Duplicate scope keys silently overwrite |
| `wal_file_manager.cpp:21-29` | `parseWalSeqNum` returns 0 for malformed filenames |
| `wal_file_manager.hpp:29` | `currentWalSequenceNumber` is `int` but stores unsigned (UB after 2^31 rollovers) |
| `memory_store.cpp:348-364` | Batch insert WAL-memory inconsistency on `insertMemory` exception |
| `tsm_tombstone.hpp:166` | `filterTombstoned` missing UINT64_MAX overflow guard |
| `hyperloglog.hpp:76-82` | No validation of register values after deserialization (values ≥64 → UB) |
| `write_batch.cpp:89` | Invalid OpType during deserialization silently treated as Delete |
| `manifest.cpp:234-242` | `atomicReplaceFiles` can accidentally remove newly added file |
| `bloom_filter.cpp:29` | `fastRange` overflow with large filters (>430M keys) — false negatives |
| `http_metadata_handler.cpp:311-331` | Tag filter parsing silently drops keys without colons |
| `http_delete_handler.cpp:93-98` | No validation for empty strings in fields array |
| `interpolation_functions.cpp:97` | Infinite loop when timestamp + interval overflows uint64_t |
| `function_pipeline_executor.cpp:12-86` | Hardcoded implementations disconnected from FunctionRegistry |
| `function_query_parser.cpp:52-56` | `findAndCollect` uses wrong position for whole-word matches |
| `streaming_aggregator.cpp:61` | Potential overflow in bucket completion check |
| `string_encoder.cpp:183-185` | `encodeInto({})` produces un-decodable output |
| `tsm_compactor.cpp:672` | `_pendingRetentionPolicies` never cleared after use (stale policies) |
| `tsm_compactor.cpp:739` | `forceFullCompaction()` skips tier 3 entirely |

---

## Key Test Coverage Gaps

Many agents identified significant test gaps. The most impactful:

1. **Query Runner**: No test for two memory stores without TSM data (would catch C1)
2. **Aggregator**: No test for SPREAD/STDDEV/STDVAR through raw-value fold path (would catch C2)
3. **Block Aggregator**: No dedicated test file (SIMD fold paths untested)
4. **HTTP Query Handler**: Core scatter-gather logic (~800 lines) has no unit tests
5. **Streaming Aggregator**: `closeBuckets(nowNs)` with partial close completely untested (only production path)
6. **TSM File Manager**: No test for duplicate rank handling, .tmp cleanup, or shutdown
7. **Merge Iterator**: Sync interface (production hot path) has zero test coverage
8. **HTTP Stream Handler**: Async handler logic (where most bugs hide) has no tests
9. **Function Pipeline Executor**: Zero direct tests
10. **Cardinality endpoint**: No integration tests at all

---

## Missing SIMD Usage (per CLAUDE.md requirement)

The project requires "SIMD is used whenever possible using Google Highway." Several subsystems lack SIMD:

- `zigzag.hpp` — batch encode/decode are scalar loops (trivially vectorizable)
- `float_encoder` — no Highway SIMD despite being a hot path
- `hyperloglog.hpp` — merge loop over 16K bytes ideal for SIMD max
- `bloom_filter (native)` — no SIMD; storage bloom filter has extensive SIMD
- `functions/` — arithmetic, smoothing, interpolation all scalar despite "Vectorized" naming
- `transform_functions.hpp` — `moving_rollup` is O(n*w) scalar

---

## Architectural Observations

1. **QueryPlanner is dead code** — The entire `query_planner.cpp/hpp` is never called from production. The HTTP query handler implements its own inline scatter-gather. Consider removing or integrating.

2. **Function framework disconnect** — `FunctionPipelineExecutor` hardcodes SMA/EMA/add/multiply instead of using `FunctionRegistry`. The HTTP handler returns hardcoded function lists instead of querying the registry.

3. **Inconsistent NaN handling** — SIMD paths, scalar paths, block aggregator, streaming aggregator, and expression evaluator all handle NaN differently. A unified NaN policy should be established.

4. **GlazeDeleteRequest ODR violation** — Defined identically in 3 translation units. Should be in a shared header.

5. **`isAvx2Available()`/`isAvx512Available()` always return true** — Found in simd_aggregator.hpp, simd_anomaly.hpp, and transform_functions_simd.hpp. Misleading API.

---

## Recommendations (Priority Order)

### Immediate (data correctness / data loss)
1. Fix C1 (query runner data loss on two memory spans)
2. Fix C2 (aggregator SPREAD/STDDEV/STDVAR fall through to SUM)
3. Fix C8 (index WAL only replays latest generation — data loss on crash)
4. Fix C10 (TSM use-after-free on LRU-evicted string dictionary)
5. Fix C7 (shard rebalancer routes everything to shard 0)
6. Fix H3 (pushdown double-counting during rollover)

### Short-term (safety/stability)
7. Fix C9 (ReDoS bypass in subscription scopes)
8. Fix H15 (ALP_RD stack overflow risk on Seastar)
9. Fix H38/H39 (index WAL destructor + flushBuffer zero-gap corruption)
10. Fix H22 (TSM file handles not closed on shutdown)
11. Fix H24 (WAL DeleteRange recovery crash)
12. Fix C5/C6 (reactor blocking in functions)
13. Add resource limits: H18, H19, H20, H21
14. Fix H40/H41 (shard rebalancer fsync + atomic writes)

### Medium-term (correctness)
15. Fix H4 (streaming COUNT on string fields)
16. Fix H6 (NaN corrupts float block stats)
17. Fix H7 (boolean fields break derived queries)
18. Establish unified NaN handling policy
19. Fix security issues (H32, H37, C9)

### Longer-term (robustness)
20. Add missing SIMD implementations per project requirements
21. Close top 10 test coverage gaps
22. Remove dead code (QueryPlanner, unused WAL methods, etc.)
23. Fix H42 (SSTable synchronous pread blocking reactor)
24. Implement L1+ compaction strategy for NativeIndex
