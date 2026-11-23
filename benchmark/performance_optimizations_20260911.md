**Implemented insert and query optimizations — 11 September 2026**

**Benchmark update:** the [third pass](query_pass3_20260911.md) found and fixed quadratic response copying in the native query client. Its corrected comparisons clarify the large HTTP timings below; CPU benchmarks and Python probes are unaffected.

A [second query/aggregation pass](query_pass2_20260911.md) now follows this baseline, including a fix for the short grouped-query regression below.

Implemented changes across all six areas in the [original review](performance_review_20260911.md). The clearest measured gains are floating-point JSON requests that previously parsed twice, large unbucketed grouped responses, and small corrections/backfills into large memory series. General disk insert throughput remains too variable to claim a reliable gain.

**Changes**

- JSON array parsing classifies the first number's JSON token before allocating doubles. Explicit floats such as `0.0`, `100.0`, and exponent notation keep the fast path. Integer-first arrays fall back before conversion, preserving exact integers above 2^53. Full parsing still validates the array.
- Unbucketed grouped queries use the existing compact sorted partials and N-way merge instead of allocating a hash-map aggregation state per timestamp. Bucketed aggregation still folds on each shard; map nodes are consumed incrementally at existing yield points during final merging.
- Immutable shared timestamp batches carry ordering, strict monotonicity, min/max, and exact occupied days. JSON/protobuf fields, coercion, indexing, and memory insertion reuse those properties. Statistics reuse known ordering after sorting. Each series still records its own day membership, including gaps.
- Index prefetch receives inclusive query bounds, skips non-overlapping sparse-index entries, and limits concurrent file prefetches to eight.
- Discovery contexts borrow immutable cached metadata and stay behind a foreign pointer on their owning shard. The coordinator receives counts and grouping-key availability before returning the handle for execution. Global limits and unknown-key validation retain the two-phase protocol.
- Small overwrite batches update existing values in place. Other overlapping inserts merge only the affected suffix and avoid calculating statistics that an overwrite would discard. Floating-point overwrites still recompute full-series statistics for correctness. Chunked storage/per-chunk statistics remain a larger architectural option for future work.

**Matched results**

Baseline: preserved release binaries/library from `71b1ae9`. Optimized: release build of the current working tree. Both use GCC 14, `-O3 -g -DNDEBUG -march=native`, on a Ryzen 9 7950X. HTTP servers use four shards, CPUs 0–3, 8 GiB, default `wal_sync_mode="always"`. The native benchmark client uses CPU 6 and eight connections. Neither storage format nor durability settings changed.

Numbers below are medians. Speedup means baseline time divided by optimized time.

| Workload | Before | After | Speedup |
|---|---:|---:|---:|
| JSON arrays beginning with `0.0`, HTTP on tmpfs | 5.790 ms | 1.539 ms | 3.76× |
| Same JSON workload beginning with `0.5`, HTTP on tmpfs | 1.735 ms | 1.549 ms | 1.12× |
| Full-range AVG grouped by host, 1,040,000 output points | 512.19 ms | 401.86 ms | 1.27×; 21.5% lower latency |
| Bucketed AVG over 3,000 series | 0.729 ms | 0.601 ms | 1.21× |
| Wildcard-filtered bucketed AVG over that dataset | 0.458 ms | 0.397 ms | 1.15× |
| 50 small overwrite batches into a million-point memory series | 73.163 ms | 9.395 ms | 7.79× |
| 50 distinct backfill batches near that series' tail | 336.576 ms | 0.342 ms | 983× in this synthetic case |

The JSON experiment pre-generates equal-length payloads containing 10,000 timestamps × ten fields. Only each field's first value changes between `0.5` and `0.0`; the rest are `25.25`. Requests alternate cases, reuse a TCP_NODELAY connection, and append chronologically. Each version ran twice in before/after/after/before order, with five warmups and 30 measured requests per case per run. The table pools 60 samples per case/version. All requests acknowledged 100,000 points. **Tmpfs removes physical disk persistence cost**, so these are HTTP/CPU-path measurements, not durable ingest rates.

The full grouped query is an existing native query-benchmark case, five timed iterations per version after the same protobuf dataset load. Both returned ten series and 1,040,000 points. The response is about 39.7 MB; serialization and transfer remain substantial. Without an interval, AVG/FIRST/LATEST retain timestamps; no query was changed into a scalar reduction to obtain a speedup.

The high-cardinality HTTP probe loads 3,000 series with 16 aligned timestamps each and ten rack groups. It records 30 timed queries after five warmups. Results matched exactly between versions after normalizing series order. There is a **regression** on its unbucketed grouped query: **0.912 → 1.043 ms**, 14.3% higher latency (0.130 ms). Compact per-series partials send more intermediate data than shard-local hash folding when many short series share the same few timestamps. The change favors large distinct-timestamp results, not every grouping shape. The ordinary million-point, ungrouped AVG was also slightly slower in this run: median 362.76 → 371.23 ms (2.3%); ungrouped LATEST was essentially unchanged, 369.81 → 368.25 ms.

The memory-series driver excludes setup and I/O, compiles the same source against each version's headers/library, pins execution to CPU 6, and measures seven repetitions. Each repetition inserts 50 pre-generated batches of 128 points into one million existing points. Corrections are scattered; backfills add distinct odd timestamps within the final 10,000 original points. The exceptional backfill ratio comes from leaving almost the entire prefix untouched and extending statistics instead of copying the million-point arrays. It is not a general ingest speedup. Randomized correctness tests separately check last-write-wins values and statistics against an independent map oracle.

**Disk throughput: inconclusive**

Fresh ext4/NVMe directories, identical settings, fixed native client, alternating server versions. Three runs per version/format, each 200 timed batches × 100,000 points with ten warmups:

| Format/version | Run 1 | Run 2 | Run 3 | Median |
|---|---:|---:|---:|---:|
| JSON before | 4.000 | 3.612 | 22.669 | 4.000 |
| JSON after | 4.785 | 3.641 | 4.506 | 4.506 |
| Protobuf before | 3.701 | 3.811 | 21.073 | 3.811 |
| Protobuf after | 3.715 | 3.918 | 19.958 | 3.918 |

Units: million points/sec. All 2,400 timed insert requests succeeded. The medians suggest +12.6% JSON and +2.8% protobuf, but the within-version spread is much larger, so these are **not reliable improvement estimates**. `/proc/pressure/io` was recorded around every run. The machine was shared and the volume was 92% full. An earlier non-interleaved run misleadingly suggested a fivefold gain; it is archived for transparency and excluded from the speedup table. The original review had already measured roughly 19 M points/sec on the baseline.

The insert benchmark's corrected timer excludes payload setup for both versions. That timer correction is not credited as a database improvement. Its post-insert all-fields verification query exceeds the configured point limit with these dataset sizes; it is not used as persistence validation. Similarly, the native query suite has 25 expected point-limit failures from two all-fields cases and some empty-result cases; none contributes to the comparison above.

**Validation and reproduction**

New regression coverage checks explicit floats and malformed JSON, exact large integers through HTTP, shared timestamp ordering/duplicates/ownership, day-membership gaps after index reopen, overwrite/backfill values and statistics including NaNs, grouped AVG/COUNT with aligned and disjoint timestamps, discovery-cache invalidation and grouping validation, and prefetch boundaries. The prefetch test verifies zero DMA bytes for an excluded range and an actual read at the exact inclusive boundary.

Validation results and commands are recorded in [implemented/validation.txt](performance_review_20260911/implemented/validation.txt). Raw timings, configuration dumps, and complete native query logs are in [implemented](performance_review_20260911/implemented/). Experimental drivers are in [performance_review_20260911](performance_review_20260911/).

```bash
cmake --build build --target timestar_http_server timestar_insert_bench timestar_query_bench timestar_perf_test timestar_unit_test -j 8

# Point these drivers at either build's binaries. Use a NEW output directory.
TIMESTAR_BENCH_BIN_DIR="$PWD/build/bin" python3 benchmark/performance_review_20260911/run_http.py "$PWD/build/http-repeat"
TIMESTAR_BENCH_BIN_DIR="$PWD/build/bin" python3 benchmark/performance_review_20260911/json_fallback.py /tmp/parser-repeat
TIMESTAR_BENCH_BIN_DIR="$PWD/build/bin" python3 benchmark/performance_review_20260911/high_cardinality_probe.py "$PWD/build/cardinality-repeat"

# Interleave the preserved build/perf-before-20260911 server with build/bin.
python3 benchmark/performance_review_20260911/interleave_insert.py "$PWD/build/interleaved-repeat"

# CPU driver: 'before' needs the preserved liblibtimestar.a and baseline
# memory_store.hpp, timestar_value.hpp, wal.hpp in build/perf-before-20260911.
# compile_commands.json and the perf target's link.txt supply matching flags.
python3 benchmark/performance_review_20260911/compile_insert_cpu_bench.py before
python3 benchmark/performance_review_20260911/compile_insert_cpu_bench.py after
taskset -c 6 build/perf-before-20260911/insert_cpu_bench
taskset -c 6 build/perf-after-20260911/insert_cpu_bench
```

Run each benchmark in isolation from builds/tests and use the same filesystem for both versions. The HTTP drivers start and stop private servers on port 18086. These measurements establish workload-specific gains on this machine; they do not cover sustained compaction, peak resident memory, or a long-running production mix.
