**Insert and query performance review — 11 September 2026**

**Benchmark update:** the [third pass](query_pass3_20260911.md) found and fixed quadratic response copying in the native query client. Its corrected comparisons clarify the large HTTP timings below; CPU benchmarks and Python probes are unaffected.

Reviewed release candidate `71b1ae9`, tracing HTTP parsing, shard dispatch, type enforcement, metadata indexing, WAL persistence, memory insertion, discovery, TSM reads, aggregation, merging, and response formatting. The strongest candidates are eliminating the numeric JSON fallback's second parse and replacing per-timestamp aggregation maps on large queries. Range-aware index prefetch is a smaller, well-contained query change.

This document records the initial review. Implementation and matched before/after results are now available in [the optimization results](performance_optimizations_20260911.md).

Only the insert benchmark timer was changed during the initial review. The database optimizations below are proposals; the measurements establish baselines and expose costs, not before/after speedups.

**Measured baseline.** Release build, GCC 14, `-O3 -g -DNDEBUG -march=native`, Ryzen 9 7950X. Server: four shards on CPUs 0–3, 8 GiB; native client: CPU 6, 2 GiB, eight connections. Fresh data directory per run on `/dev/nvme0n1p2`, ext4. Default `wal_sync_mode="always"`. Three runs per wire format, alternating format order, each with 100 × 10,000 timestamps × 10 fields = 10 million timed points and five warmup batches.

| Measurement | Result |
|---|---:|
| JSON array inserts, three runs | 19.99 / 18.55 / 19.76 M points/sec |
| Protobuf array inserts, three runs | 18.98 / 19.23 / 18.29 M points/sec |
| Timed insert requests | 600 successful; zero HTTP or connection failures |
| JSON first-value probe: `0.5`, median of 30 requests | 7.26 ms |
| Same probe with first value `0.0` | 11.42 ms |
| Full-range, single-field AVG, no interval | 389.35 ms average; 1,000,000 output points |
| Full-range AVG grouped by host, no interval | 542.49 ms average; 1,040,000 output points |
| Full-range AVG with one-day buckets | 0.44 ms average; 695 output points |
| Selected existing microbenchmarks | 14/14 passed on ext4; also 14/14 on tmpfs |

These are short burst insert tests (roughly half a second of timed writes), not sustained ingest or compaction throughput. The HTTP query run followed ingestion without forcing a fully compacted layout; cache warmups are included in the harness. The machine was not reserved exclusively, and disk microbenchmarks showed substantial latency variation. An initial tmpfs pass was useful for CPU comparison but is excluded from the disk throughput table. Kernel profiling was unavailable (`perf_event_paranoid=4`); query stall attribution uses Seastar's recorded stacks and `addr2line`.

**1. Avoid parsing integral-looking floating-point JSON twice.** High-confidence, directly measured insert opportunity.

In `lib/http/http_write_handler.cpp:1880`, `tryFastDoubleWrite()` parses the entire body into typed double vectors. If any field's first value satisfies `v == trunc(v)`, it discards that result. `handleWrite()` then parses the entire body again into the DOM at line 2153. Ordinary floating-point telemetry beginning with `0.0` or `100.0` therefore loses the fast path, even though its JSON token is explicitly floating-point.

The controlled probe used the same measurement, fields, shard routing, chronological append pattern, and equal-length payloads. Each request contained 10,000 timestamps and ten fields; the first value in each field was either `0.5` or `0.0`, with all other values `25.25`. Payloads were pre-generated, order alternated, the HTTP connection reused with TCP_NODELAY, five requests per case warmed up, and 30 per case measured. All writes returned success and acknowledged 100,000 points. The `0.0` case's median was **57% higher**. This is an end-to-end workload comparison, not an isolated parser profile or a promised optimization gain.

Preserve token type while parsing directly into typed arrays, or perform a lightweight token classification before allocating the double representation. Preserve the distinction between integer tokens and floating-point tokens, including exponent notation, and exact integer values beyond 2^53. Simply deleting the existing guard would reintroduce type inconsistencies.

Validate with the existing native array benchmark plus the archived first-value probe. Add integer-first, exponent, mixed-type, and large-integer cases to the relevant parser/type tests when implementing it. The native benchmark's random first values almost always have fractions, so its default workload misses this cost.

**2. Keep unbucketed query aggregation in compact sorted arrays.** High-impact query opportunity with observed stalls; larger implementation.

`streamingGroupByAggregation()` in `lib/http/http_query_handler.cpp:379` uses `unordered_map<uint64_t, AggregationState>` for each group. With no interval, `foldPoint()` at line 472 uses each distinct timestamp as the map key. Sorted pushdown output is consequently expanded into individually allocated, full aggregation states. At this point “streaming” still requires one state per output timestamp.

The full grouped query returned about 39.7 MB and averaged 542 ms. Server stacks captured repeated 32–33 ms stalls in destruction of `PartialAggregationResult::bucketStates`, reached from `finalizeMultiShardResponse()`. This directly establishes cleanup as a reactor-latency problem; it does not attribute the whole query time to cleanup.

Use a sorted merge/fold across series within each group, retaining compact timestamp/value arrays for single-contributor runs and method-specific state only where timestamps coincide. Reuse the existing sorted-vector and N-way merge machinery where possible. As an incremental latency improvement, destroy large maps in bounded chunks with yields, including error exits; yielding only between whole partials cannot bound destruction of one huge map.

Validate the full-range grouped HTTP case, `OptimizationBaseline.B1_AggregatorMergeScaling`, and aggregation stall benchmarks, with aligned and disjoint timestamps. Track peak memory and reactor stalls alongside latency. Preserve per-timestamp results, NaN policy, AVG counts, STDDEV merge state, and FIRST/LATEST tie rules. Collapsing the range to one value would change the API semantics.

**3. Compute timestamp properties once per shared input batch.** High-confidence repeated work; aggregate speedup unmeasured.

The HTTP layer already shares timestamps across fields (`lib/http/http_write_handler.cpp:1387`). Downstream, each field still scans the same array to record day memberships (`lib/index/native/native_index.cpp:3803`), detect sortedness (`lib/storage/memory_store.cpp:198`), check duplicates (`memory_store.cpp:236`), and check sortedness again for first/latest statistics (`memory_store.cpp:117`). Sharing the allocation has not eliminated repeated scans.

Attach validated metadata to the immutable shared timestamp input: monotonicity, duplicate presence, min/max, and exact distinct days. Reuse it across fields. After sorting and deduplication, the memory store can pass its known ordering to statistics rather than rescanning. Each series must still acquire its own day membership, and metadata must remain valid through coercion and timestamp ownership changes. Do not substitute a min/max day span for exact membership when inputs contain gaps.

Validate with `timestar_insert_bench` across field count and batch size, plus sorted, duplicate, out-of-order, and cross-day workloads. `OptimizationBaseline.B8_EngineInsertPath` is useful as a lower-level baseline, but it calls single-series `Engine::insert()` and copies the input; it does not model production HTTP `insertBatch()` or shared timestamps.

**4. Make index prefetch respect the requested time range.** Relatively contained query optimization.

`Engine::prefetchSeriesIndices()` (`lib/core/engine.cpp:789`) snapshots every TSM file and asks each to prefetch every requested series. `TSM::prefetchFullIndexEntries()` (`lib/storage/tsm.cpp:825`) checks cache, bloom filter, and series presence, but has no time-range argument. Both standard and streaming HTTP query paths call this before the later query reader prunes files/blocks by time.

For narrow queries over long-lived series, this can load full index entries from historical files that cannot contribute. Pass the query range into prefetch, prune with file and per-series sparse-index time bounds, and bound outstanding prefetch work. Retain existing coalescing among the entries that survive.

Use `TsmReadPathBenchmark.R1_NarrowRangeQuery_ManyFiles`, `R5_MultiSeriesNarrowRange`, and the HTTP narrow-range cases. Extend measurement to explicitly cold index caches and record index bytes/read counts: the current tests warm caches and cannot quantify the proposed I/O saving. Also confirm actual file count after compaction. Existing R6 shows stats-only full-range folding already takes about 5 µs; avoid spending effort reimplementing that optimization.

**5. Keep discovery contexts on the owning shard.** Likely useful at high series cardinality; unmeasured here.

`discoverSeriesAcrossShards()` (`lib/http/http_query_handler.cpp:1683`) retrieves cached immutable metadata but reconstructs a series-key string and copies a tag map for every matched series. It sends these contexts to the coordinator; `fanOutShardQueries()` at line 1842 then sends them back to their owning shards for execution. Thus even cache hits allocate contexts and require two shard RPC phases.

Fuse discovery and execution on each shard where possible. Alternatively retain an immutable shard-local discovery snapshot behind a request handle, returning only counts and grouping-key validation data before execution. The coordinator must still enforce existing series limits, unknown-grouping-key behavior, timeout/error handling, and cleanup of retained state. This is an architectural change rather than a trivial cache tweak.

Measure 1/4/8 shards with thousands of series, hot/cold discovery, tag patterns, and repeated narrow requests. `OptimizationBaseline.B7_NativeIndexHighCardinality` measures index costs, but an HTTP high-cardinality case is needed to capture context construction and the round trip. The 200-series native array workload does not establish its expected gain.

**6. Reduce work for small overwrites/backfills into large memory series.** Workload-dependent insert opportunity.

`InMemorySeries::insert()` (`lib/storage/memory_store.cpp:205`) updates incoming statistics before checking whether timestamps overlap existing data. Overlapping inserts call `mergePaired()` across the stored series; actual overwrites then call `recomputeStats()` over the full result. A tiny repeated correction can therefore cost a full-series merge and statistics scan, including an incoming statistics update that is discarded.

First avoid the redundant statistics update when a full recomputation will be required. For workloads dominated by corrections, evaluate chunked sorted storage with per-chunk statistics so a local overwrite updates only affected chunks. This needs careful last-write-wins and numerically stable variance handling; use explicit overwrite-size/series-size sweeps, not append-only throughput as evidence.

**Benchmark corrections and interpretation.**

The change in `bin/timestar_insert_bench.cpp:552` uses the worker's existing `wall_time`, which begins after array payload generation, for throughput. It separately reports total time including setup/teardown and states when scalar formats generate payloads lazily inside the timed interval. For the first ext4 JSON run, request execution took 0.500 s while total time was 1.204 s: the old formula would report about 8.31 M points/sec instead of 19.99 M. This is a measurement correction, not a database speedup.

The query suite completed 540 timed iterations: **515 succeeded, 25 failed**. Two all-field cases exceed the 10-million-point limit because warmup data raises the pre-merge count to 10.5 million. The harness currently includes zero-success cases in its “fastest” list and exits successfully despite failures. Several fixed-host narrow queries also return zero points. These cases must be excluded from latency comparisons and should become explicit expected-result/failure checks in a follow-up benchmark change.

Query names also require care: without `aggregationInterval`, current AVG and LATEST retain timestamps. The native “latest: single field” returned one million points, not one latest value. The historical March reports and current scalar-fold microbenchmarks cannot be used as equivalent HTTP baselines. Include explicit intervals to measure bounded aggregation or latest-per-bucket, and validate output shape/count before accepting a timing.

Build validation succeeded for the server, both native benchmarks, and the performance test executable. No production database code was changed during the initial review; see the linked implementation report for subsequent changes. Existing optimizations worth retaining include WAL group commit, cached series IDs, shared field inputs, SIMD batch statistics, stats-before-DMA filtering, and overlap-aware pushdown.

**Reproduction and evidence.** Raw results and the two small experiment drivers are in [performance_review_20260911](performance_review_20260911/). The drivers require a new output directory, start private servers on port 18086, and stop those servers afterward. Use an ext4/NVMe path for disk measurements, not `/tmp` on this machine.

```bash
cmake --build build --target timestar_perf_test timestar_insert_bench timestar_query_bench timestar_http_server -j 8
python3 benchmark/performance_review_20260911/run_http.py "$PWD/build/perf-review-repeat"
python3 benchmark/performance_review_20260911/json_fallback.py "$PWD/build/parse-review-repeat"

# Run from a new scratch directory: these existing fixtures delete shard_0.
repo="$PWD"
mkdir -p build/perf-review-micro-repeat
cd build/perf-review-micro-repeat
"$repo/build/test/timestar_perf_test" \
  --gtest_filter='OptimizationBaseline.B3_*:OptimizationBaseline.B8_*:OptimizationBaseline.B9_*:TsmReadPathBenchmark.*:QueryFoldBench.*' \
  -c 1 --cpuset 4 --memory 4G --overprovisioned
```
