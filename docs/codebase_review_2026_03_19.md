# TimeStar Codebase Review - March 19, 2026

## Summary

Full codebase review across 48 parallel review agents covering all source files in `lib/`, `bin/`, and their test coverage. This report consolidates findings organized by severity.

**Total issues found: ~400+**
- Critical/High: 25
- Medium: 65
- Low: ~140
- Test coverage gaps: ~180+

---

## CRITICAL / HIGH SEVERITY ISSUES

### 1. Reactor-blocking `.get()` in metrics gauge callback
**File:** `lib/core/engine_metrics.cpp:36`
`getSeriesCountSync()` calls `.get()` on an async future from the Seastar reactor thread, performing a full KV prefix scan with disk I/O. This can stall or deadlock the reactor on every Prometheus scrape (~15-30s).
**Fix:** Maintain a running `seriesCount_` counter incremented in `getOrCreateSeriesId()`.

### 2. `addTag()` does not invalidate series key/ID caches
**File:** `lib/core/timestar_value.hpp:68`
After `seriesKey()` is called (caching the result), calling `addTag()` leaves cached `_seriesKey` and `_seriesId128` stale. Data could be stored/queried under the wrong series ID.
**Fix:** Add `_seriesKeyCache = false; _seriesId128Cache = false; _cachedEstimatedSize.reset();` to `addTag()`.

### 3. Stack buffer overflow on deserialized bloom filter `k > 32`
**File:** `lib/index/native/bloom_filter.cpp:145`
`deserializeFrom()` reads `k` from untrusted SSTable data without validation. If `k > 32`, the SIMD `MayContainKernel` overflows stack arrays `probeBytes[32]` and `probeMasks[32]`.
**Fix:** Validate `if (k == 0 || k > 30) return createNull();` after deserialization.

### 4. CompressedBuffer `write()`/`read()` with `bits<=0` causes UB
**File:** `lib/storage/compressed_buffer.cpp:10-50, 139-168`
Negative `bits` parameter (representable as `int`) causes undefined behavior via negative shift counts. `rewind()` then `write()` on empty buffer causes out-of-bounds access.
**Fix:** Add `if (bits <= 0) return;` guard at top of `write()` and `read()`.

### 5. `when_all` + bare `.get()` leaves unconsumed exceptional futures
**File:** `lib/storage/bulk_block_loader.hpp:126-132`
If one future in a `when_all` result fails, subsequent futures are never consumed, triggering Seastar assertion failures ("exceptional future destroyed").
**Fix:** Use `when_all_succeed` or wrap in try/catch consuming all remaining futures.

### 6. Missing destructor to drain prefetch queue
**File:** `lib/storage/compaction_pipeline.hpp`
`SeriesPrefetchManager` has no destructor. Outstanding futures in `prefetchQueue` trigger Seastar assertions on destruction.
**Fix:** Add destructor draining queue with `then_wrapped` + `handle_exception`.

### 7. TSMBlockIterator default move semantics with `optional<future>`
**File:** `lib/storage/tsm_block_iterator.hpp:49-50`
Default move leaves the source's `optional<future>` in engaged-but-invalid state. Destructor then operates on moved-from Seastar future (UB).
**Fix:** Implement move explicitly to `reset()` the source's optional.

### 8. String dictionary not forwarded in TSMBlockIterator
**File:** `lib/storage/tsm_block_iterator.hpp:87,104`
`readSingleBlock<string>` called without string dictionary, relying on thread-local `tlStringDict` which can be corrupted by coroutine interleaving during string-type compaction.
**Fix:** Accept and forward `const vector<string>*` like `BulkBlockLoader`.

### 9. `BulkMerger::next()` does not deduplicate
**File:** `lib/storage/bulk_block_loader.hpp:399-414`
Unlike 2-way and 3-way variants, the N-way `BulkMerger::next()` emits duplicate timestamps. Inconsistent API that can produce incorrect compaction results.
**Fix:** Move dedup logic from `nextBatch()` into `next()`.

### 10. `std::clamp` with NaN is undefined behavior
**File:** `lib/query/expression_evaluator.cpp:221`
`std::clamp` requires well-ordered arguments. NaN violates this, producing UB.
**Fix:** Add explicit NaN checks before `std::clamp`, `std::max`, `std::min` calls.

### 11. `fill_linear` division by zero with duplicate timestamps
**File:** `lib/query/expression_evaluator.cpp:409-412`
When anchor timestamps are identical, `dt == 0.0` causes division by zero.
**Fix:** Guard with `if (dt == 0.0)` returning average of endpoints.

### 12. No unit tests for `TimeStarInsert` core data type
**File:** `lib/core/timestar_value.hpp`
Zero unit tests for `seriesKey()` cache invalidation, `setSharedTags()`, `setCachedSeriesKey()`, `fromSeriesKey()`, or `addTag()` correctness.

### 13. Empty block in TSMResult terminates iteration
**File:** `lib/query/query_result.hpp:91-94`
An empty block in the middle of a `TSMResult` causes `advanceIteratorPast` to treat the source as exhausted, losing all subsequent blocks.
**Fix:** Replace early return with loop skipping empty blocks.

### 14. Blocking filesystem calls on Seastar reactor thread
**File:** `lib/index/native/manifest.cpp:48-49, 55, 289`
`std::filesystem::create_directories()`, `exists()`, and `file_size()` are blocking POSIX syscalls on the reactor thread.
**Fix:** Wrap in `seastar::async()`.

### 15. Crash-atomicity claim incorrect for manifest writes
**File:** `lib/index/native/manifest.cpp:193-246`
POSIX `write()` is not atomic. A crash mid-write can leave a partial record. The comment claiming atomicity is misleading.
**Fix:** Use temp-file + `fsync` + `rename` pattern (like `writeSnapshot()` already does).

### 16. `acknowledgeAlert()` index invalidation
**File:** `lib/functions/function_monitoring.cpp:148-159`
Erasing from vector by index shifts subsequent elements, making sequential `acknowledgeAlert(i)` calls acknowledge wrong alerts.
**Fix:** Always acknowledge index 0 in a loop, or use unique ID.

### 17. Levinson-Durbin returns stale coefficients on early break
**File:** `lib/query/forecast/seasonal_forecaster.cpp:141-152`
When recursion breaks early, remaining `phi` coefficients contain garbage from `phiPrev` swap.
**Fix:** Zero out coefficients from break point to `order-1`.

### 18. Response formatter `appendChar` UB if buffer size 0
**File:** `lib/http/response_formatter.cpp:121-127`
`buf.resize(0 * 2)` stays at 0, followed by out-of-bounds `buf[pos]` access.
**Fix:** `buf.resize(std::max(buf.size() * 2, pos + 1))`.

### 19. NativeIndex iterator invalidation across `co_await` in `kvGet`/`kvExists`
**File:** `lib/index/native/native_index.cpp:287-295`
`sstableReaders_` map is iterated with reverse iterator, and loop body contains `co_await`. Background flush can call `refreshSSTables()` which modifies the map, invalidating the iterator. The `shared_ptr` copy protects the reader object but not the map iterator.
**Fix:** Snapshot readers into a local `vector<shared_ptr<SSTableReader>>` before the loop.

### 20. NativeIndex use-after-free risk in `kvPrefixScan`
**File:** `lib/index/native/native_index.cpp:416-418`
`SSTableBorrowedIteratorSource` takes a raw pointer via `reader.get()` without holding a `shared_ptr`. If `refreshSSTables()` erases the last reference during a `co_await`, the reader is destroyed and the iterator has a dangling pointer.
**Fix:** Store `shared_ptr<SSTableReader>` in iterator sources, or snapshot into local vector.

### 21. NativeIndex robin_map reference held across `co_await`
**File:** `lib/index/native/native_index.cpp:1698-1704`
Reference from `bitmapCache_[cacheKey]` is held across `co_await kvGet()`. Concurrent coroutine insertions can trigger rehash, invalidating the reference. Same issue at line 1801 for day bitmaps, and in `getOrCreateSeriesId` for schema caches.
**Fix:** Re-find entries after each `co_await`.

### 22. SSTable `blockReadSemaphore_` is `static` but should be `thread_local`
**File:** `lib/index/native/sstable.hpp:205`
All shards share a single pointer. Last shard to call `setBlockReadSemaphore` wins, creating a data race during startup and defeating per-shard concurrency limiting.
**Fix:** Change to `inline static thread_local seastar::semaphore* blockReadSemaphore_ = nullptr;`

### 23. TSM `readSparseIndex` missing bounds check on `dictBytes`
**File:** `lib/storage/tsm.cpp:434-437`
Corrupted `dictBytes` value can cause `indexSlice.offset` to overflow, producing wrong `SparseIndexEntry.entrySize` and causing incorrect DMA reads for that series.
**Fix:** Validate `indexSlice.offset + 4 + dictBytes <= indexSlice.length_` before advancing.

### 24. Integer encoder signed overflow UB in delta-of-delta
**File:** `lib/encoding/integer/integer_encoder_ffor.cpp:495-496`
Delta-of-delta computation on `int64_t` can overflow (UB). Works on x86-64 two's complement but a sufficiently aggressive optimizer could break it.
**Fix:** Perform delta computation in unsigned arithmetic, reinterpret as signed for zigzag.

### 25. Query runner pushdown double-counting during rollover
**File:** `lib/query/query_runner.cpp:872-874`
During memory store rollover, the pushdown aggregation split may double-count data points that exist in both a TSM file (from just-flushed store) and still-queryable memory stores. Non-pushdown path handles this via merge+dedup, but pushdown blindly sums both sources.
**Fix:** Add dedup awareness to the fallback aggregation path.

---

## MEDIUM SEVERITY ISSUES

### Core
- **engine.cpp:664-682**: `deleteRangeImpl()` computes SHA1 5 times for the same series key. Compute once and reuse.
- **engine.cpp:593-638**: `executeLocalQuery()` copies vectors element-by-element instead of using `std::move`.
- **engine.cpp:136-163**: Streaming subscriber notification duplicated between `insert()` and `insertBatch()`.
- **timestar_value.hpp:52**: Constructor copies strings instead of moving (`std::move`).
- **timestar_value.hpp:68**: `addTag` uses `insert` (first-writer-wins) silently ignoring duplicates.
- **placement_table.cpp:36**: `buildLocal(0)` silently produces broken table with `coreCount_=0`.
- **placement_table.cpp:92-108**: `fromJson()` no validation of `coreCount` or `coreId` ranges.

### Storage
- **tsm_compactor.cpp:188-219**: Zero-copy path skips `pointsRead`/`pointsWritten` stats (monitoring blind spot).
- **tsm_compactor.cpp:67**: All blocks decompressed even for zero-copy path (wasted I/O).
- **tsm_compactor.cpp:680-682**: `_pendingRetentionPolicies` cleared after first `executeCompaction`.
- **tsm_file_manager.cpp:117**: `writeMemstore` silently accepts `tier >= MAX_TIERS`.
- **tsm_file_manager.cpp:264-269**: Unhandled exception in compaction future can crash process.
- **tsm_tombstone.hpp:149-151**: `filterTombstoned` deep-copies vectors when no tombstones apply (hot path).
- **tsm_tombstone.cpp:429-437**: O(N) entries rebuild on every `addTombstone` call.
- **wal.cpp:201-204**: `size()` calls `walFile.size()` on potentially closed file descriptor.
- **wal_file_manager.cpp:420-460**: Expensive diagnostics loop runs on every WAL-to-TSM conversion.
- **compressed_buffer.hpp:18**: `ensure_capacity` name misleading -- only reserves, never resizes.
- **string_encoder.cpp:124-137**: `compressStrings` bypasses `AlignedBuffer::current_size` invariant.

### Index
- **native bloom_filter.cpp:95**: `mayContain()` missing `numBits <= 0xFFFFFFFF` guard for SIMD path.
- **native bloom_filter.cpp:14**: No validation of negative `bitsPerKey_` (crashes on build).
- **compaction.cpp:141**: Tombstone cutoff unsigned underflow possible.
- **compaction.cpp - pickCompaction()**: Only considers L0 files; L1+ files grow unbounded.
- **key_encoding.cpp**: `encodeSeriesKey` should validate null bytes like other key encoders.
- **key_encoding.cpp**: `encodeCardinalityHLLKey(measurement)` identical to prefix variant (ambiguous keys).

### Query
- **aggregator/simd_aggregator.cpp:330-338**: `scalar::calculateSum` does not skip NaN values (inconsistent with NaN policy).
- **query_result.hpp:29-30**: `startTime()`/`endTime()` undefined behavior on empty `timestamps` vector.
- **streaming_aggregator.cpp:21**: String count inflates denominator for AVG on mixed-type series.
- **derived_query_executor.cpp:209**: `executeFromJson` unconditionally overrides per-query time ranges.
- **derived_query_executor.cpp:598-599**: Unnecessary full vector copy when no history filtering needed.
- **streaming_derived_evaluator.cpp:80**: Multi-series per query label silently loses data (last wins).

### HTTP
- **http_query_handler.cpp:253-259**: `query_errors_total` not incremented on logical errors.
- **http_query_handler.cpp:312**: `stoull` silently accepts negative string timestamps.
- **http_delete_handler.cpp:248**: No timeout on scatter-gather `when_all_succeed` (can hang).
- **http_stream_handler.cpp:735-739**: Remote `addSubscription` failure leaves orphaned subscriptions.
- **http_stream_handler.cpp:853-854**: Multi-query + formula + backfill produces incorrect results.
- **http_retention_handler.cpp**: Missing `reply->done("json")` -- responses may lack Content-Type.
- **http_derived_query_handler.cpp:25**: `this` captured in lambda without lifetime guarantee.
- **response_formatter.cpp:164**: NaN/Inf handling in `appendDoubleArray` -- verify glz::to_chars produces valid JSON.

### Functions
- **function_monitoring.hpp:112-114**: `getCacheHitRate()` double-loads atomic (can exceed 1.0).
- **function_security.cpp:451-462**: `containsDangerousPatterns` false positive on `on`+word+`=`.
- **function_query_parser.cpp:53-57**: Parser only finds first occurrence of each function name.
- **function_http_handler.cpp:414-419**: Required parameter check uses naive string search instead of JSON parsing.
- **interpolation_functions.cpp:199-335**: `SplineInterpolationFunction` claims cubic but is linear.
- **interpolation_functions.cpp:311-332**: No boundary handling for out-of-range timestamps.

### Encoding
- **alp_encoder.cpp:114**: Signed integer overflow UB in `requiredBitWidth` when `max_val - min_val` overflows.
- **alp_simd.cpp**: `fforAddBase`/`fforAddBaseU64` declared and implemented but never called (dead code).

### Native Index
- **native_index.cpp:690-733**: Multiple `auto&` references into schema caches held across `co_await`. Concurrent `trimSchemaCaches()` invalidates them.
- **native_index.cpp:1858-1886**: `trimBitmapCache` byte-budget eviction uses entry-count stop condition, doesn't resolve byte budget violations.
- **native_index.cpp:892-907**: `setFieldType` silently drops type conflicts without logging.

### SSTable
- **sstable.cpp:608-610**: Double copy of block data in cache-miss path (copy into cache + return).
- **sstable.hpp:181**: `value_` is `string_view` that can dangle across `co_await` boundaries.

### TSM
- **tsm.cpp:434-437**: Missing `dictBytes` bounds check in `readSparseIndex` (buffer overread on corrupt input).
- **tsm.cpp:530-596 vs 741-801**: ~60 lines of duplicated index block parsing code.
- **tsm.cpp:1464-1522**: Tombstone per-point filtering triplicated for float/int/bool.

### TSM Writer
- **tsm_writer.cpp:164-200**: `writeSeriesDirect` missing timestamps/values size validation.
- **tsm_writer.cpp:130-135**: String dictionary copied per block (should be passed by reference).
- **tsm_writer.cpp:315-342**: Float/Integer stats loops not SIMD-accelerated (per CLAUDE.md mandate).

### Memory Store
- **memory_store.cpp:364-367**: `insertBatch` accesses potentially moved-from object in catch handler.
- **memory_store.hpp:87**: `isFull()` is an unnecessary coroutine (zero async work) and appears to be dead code.

### Query Runner
- **query_runner.cpp:872-874**: Pushdown aggregation split may double-count during memory store rollover.
- **query_runner.cpp:250**: `series` parameter passed by value (string copy) but only used for logging.

### HTTP Write Handler
- **http_write_handler.cpp:1334-1679**: ~350 lines dead code (`processWritePoint`, both `parseWritePoint` overloads).
- **http_write_handler.cpp:362-366**: Non-numeric timestamp array elements silently skipped.
- **http_write_handler.cpp:326-345 vs 817**: Inconsistent `null` field value handling between batch and single paths.

### Config
- **timestar_config.cpp:168-178**: TOML inline comment parsing fails with trailing quotes.
- **timestar_config.cpp:185**: Empty quoted string values silently dropped in `[seastar]` section.
- **timestar_config.cpp validate()**: No validation for `IOPriorityConfig` shares (0/negative crashes Seastar).

### Forecast/Anomaly
- **seasonal_forecaster.cpp:363-367**: MSTL path does not sanitize NaN values before STL decomposition.
- **seasonal_forecaster.cpp:518-524**: R-squared compares differenced vs original variance (wrong scale).
- **stl_decomposition.cpp (anomaly)**: `weightedLinearRegression` allocates two vectors per LOESS call (hot path).
- **transform_functions.hpp:630-654**: Deque min/max may report stale front element in `moving_rollup`.

---

## KEY EFFICIENCY ISSUES

- No SIMD in function implementations (arithmetic, smoothing, interpolation) despite CLAUDE.md mandate
- 330 lines of `#if 0` dead legacy code in `alp_encoder.cpp`
- ~250 lines dead `TwoWayMergeIterator`/`FourWayMergeIterator` in `tsm_merge_specialized.hpp`
- `std::regex` construction in shard rebalancer hot-ish paths
- Multiple `std::ostringstream` JSON builders instead of Glaze
- Subscription manager copies measurement/field strings per-point in hot notify path
- `piecewise_constant` change detection is O(n^2), fixable to O(n) with prefix sums

---

## DEAD CODE SUMMARY

| File | Dead Code |
|------|-----------|
| `alp_encoder.cpp:153-482` | 330-line `#if 0` legacy encode |
| `alp_simd.cpp` | `fforAddBase`, `fforAddBaseU64` never called |
| `alp_constants.hpp:46` | `FAC_COUNT` never referenced |
| `tsm_merge_specialized.hpp` | Entire file (classes never instantiated) |
| `tsm_reader.hpp` | `TSMReader` only used in 2 test files |
| `compaction_pipeline.hpp` | `maxMemoryBytes`, `currentMemoryUsage`, `PrefetchedSeries::initialized` |
| `wal.hpp:120` | `STREAM_BUFFER_SIZE` unused |
| `wal.hpp:163` | `flushBlock()` never called |
| `function_security.cpp:48` | `getDangerousPatterns()` compiles 30 regexes, never called |
| `function_utils.hpp/cpp` | `calculateMean()` never called |
| `function_http_handler.cpp:863-913` | 5 stub methods + `createJsonReply` never called |
| `function_http_handler.cpp:704-860` | 150-line `#if 0` block |
| `http_delete_handler.cpp:131` | `createSuccessResponse` never called |
| `forecast_result.hpp:363-382` | `SARIMAState` struct never instantiated |
| `basic_detector.cpp:113-165` | `computeRollingStats` legacy dead code |
| `series_aligner.cpp:281-293` | `findLowerBound` method never called |
| `query_parser.hpp:3` | Unused `#include <chrono>` |
| Multiple files | Unused `#include` directives (~20 instances) |

---

## TOP TEST COVERAGE GAPS

1. **Zero tests for `TimeStarInsert`** -- core data type with caching logic
2. **Zero tests for `EngineMetrics`** -- setup(), counter increments, gauge callbacks
3. **No `int64_t` WAL round-trip test** -- ZigZag encode/decode path untested at WAL level
4. **No MSTL/multi-seasonality forecast tests** -- entire `forecastMSTL()` path untested
5. **No STL decomposition unit tests** -- `decompose()`, `loessEvenlySpaced()`, robustness weights
6. **No cross-series aggregation tests** in expression evaluator -- `avg_of_series` etc.
7. **No `handleSubscribe()` tests** -- 580-line coroutine with zero test coverage
8. **No direct tests for SIMD kernels** -- HLL merge, HLL clamp, HLL estimate
9. **No compaction tombstone GC tests** -- whether tombstones are dropped/preserved correctly
10. **No tests for `QueryResult` merge paths** -- 2-way, small-N, heap merges
11. **`query_parser_detailed_test.cpp` has own `main()`** -- excluded from main test suite
12. **No NaN tests for clamp/clamp_min/clamp_max** -- would catch the UB bugs
13. **No negative string timestamp tests** for HTTP query handler
14. **No body size limit or Content-Type tests** for delete/retention handlers
15. **Function pipeline registry fallback path** entirely untested

---

## RECOMMENDATIONS (Priority Order)

1. **Fix the 18 critical/high items** -- reactor stalls, UB, cache invalidation bugs, stack overflow
2. **Add missing tests for `TimeStarInsert`, `EngineMetrics`, and core merge paths**
3. **Remove ~1000 lines of confirmed dead code** (TSMMergeIterator, ALP legacy, function stubs)
4. **Add SIMD to function implementations** per CLAUDE.md mandate
5. **Add timeouts to all scatter-gather operations** (delete handler, stream handler)
6. **Validate all deserialized data** from disk (bloom filter k, manifest records, WAL types)
7. **Fix NaN handling** in aggregator sum, expression clamp, and rolling functions
8. **Consolidate duplicated code** (streaming notifications, measurement validation, JSON builders)
