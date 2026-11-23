**Three optimization passes over write, read and query paths — 22 November 2026**

Seven changes across three passes, each measured against a preserved build of the tree as it stood at the start (`release/1.5.0-rc.1` @ `0cc8ac1` plus the uncommitted September work). Nothing is committed; everything is in the working tree.

| Workload | Before | After | Change |
|---|---:|---:|---|
| Protobuf ingest, full-precision floats, 60M points | 29.1M pts/s | 70.5M pts/s | 2.43× throughput |
| — server CPU for that run | 5.50 s | 2.70 s | −51% |
| — TSM bytes on disk | 672 MB | 436 MB | −35% |
| — server peak RSS | 1,668 MB | 1,367 MB | −18% |
| JSON array ingest, 3-decimal floats, 150M points | 100.3M pts/s | 114.2M pts/s | +14% throughput |
| — server CPU for that run | 4.94 s | 4.56 s | −8% |
| Unbucketed query, 10 series active at different times, 1M points out (server time) | 10.5 ms | 5.5 ms | 1.9× |
| Same, `latest` | 16.0 ms | 5.5 ms | 2.9× |
| Same queries, series interleaved point by point (server time) | 10.6 ms | 10.9 ms | 3% slower |
| Large JSON response (1M points), client-observed latency | 52.3 ms | 49.6 ms | 5% lower; 15% where the merge also gains |
| Float encode, clean decimals, per value | 2.8–9.4 ns | 2.0–2.7 ns | 1.3–3.5× |
| Float encode, full-precision, per value | 51.6 ns | 17.7 ns | 2.9× |
| WAL/manifest checksum throughput | 3.3 GB/s | 21.4 GB/s | 6.5× |

Ingest rows are medians of three alternating base/new pairs; query rows are medians of five alternating pairs with 20 timed requests each, and every query returned byte-identical results from both builds.

**How this was measured**

- Release build, GCC 14, `-O3 -g -DNDEBUG`, **without** `-march=native` — the flags the Docker image ships with. The September review used `-march=native`, so absolute numbers are not comparable with it.
- Ryzen 9 7950X, server on four shards pinned to CPUs 8–11 with 8 GiB, client pinned elsewhere. Data on tmpfs, so these are CPU-path numbers: an fsync costs nothing here, and real-disk ingest will be bounded by the device instead.
- The machine was shared with other sessions throughout. Every comparison alternates the two builds and reports medians; where a pair range is wide it is noise, and one early scare (a 2.7× "regression") disappeared when the case was re-run in isolation.
- `perf` is unavailable here (`perf_event_paranoid=4`), so profiles come from a small `LD_PRELOAD` SIGPROF sampler written for this review (`perf_passes_20261122/tools/sprof.c`). It works with Seastar because it keeps the reactor threads from blocking SIGPROF.

**Pass 1 — float encoder search, series merge, response copy**

*Float encoder parameter search* (`lib/encoding/alp/alp_encoder.cpp`). The search brute-forced up to 190 (exponent, factor) pairs × 256 samples per TSM block and per WAL entry, stopping early only for a pair with no exceptions. Clean decimals stopped after a few pairs; anything else paid for the whole grid, which made the search 10–17× more expensive than the encode it configures. The replacement returns the same pair using three facts: special values fail everywhere so they are counted once; a pair over half-failed can never be used; and a factor above zero cannot rescue a value that fails at factor zero (proved in the source for scaled values below 2^50, evaluated directly above). In 600,000 randomized arrays factor > 0 was never chosen by the old search either.

The encoder also scaled the first 1,024 values of every stream twice — once, value by value, purely to decide whether delta encoding helps. It now decides from the block it is about to encode, falling back to the scalar scan only where the SIMD kernel's tie rounding could differ.

Output after this step was **byte-identical** to the original encoder across 180,000 random arrays (1.06 GB of encoded data) and all benchmark shapes.

| Shape, 3,000 values per call | Before, ns/value | After, ns/value | Speedup |
|---|---:|---:|---:|
| 1 decimal | 2.96 | 1.99 | 1.49× |
| 3 decimals | 4.71 | 2.26 | 2.09× |
| 6 decimals | 9.40 | 2.66 | 3.49× |
| 2 decimals with sparse NaN | 12.47 | 1.82 | 6.84× |
| 2 decimals with sparse outliers | 12.51 | 2.03 | 6.17× |
| 40% full-precision mixed in | 41.91 | 7.66 | 5.54× |
| Full-precision | 53.63 | 17.30 | 3.11× |

*Run-aware series merge* (`lib/query/aggregator.cpp`). The N-way merge pulled every point through a heap. When one series supplies a stretch on its own — series active at different times, a late or backfilled series, the tail after the others end — those points are now emitted straight from that series. Whether a series is in a run falls out of the heap repair that already happens.

| Isolated merge, 10 series × 100k points | Before, ms | After, ms | Ratio |
|---|---:|---:|---:|
| 10k-point stretches, avg | 12.07 | 7.39 | 1.63× |
| 10k-point stretches, latest | 17.72 | 7.53 | 2.35× |
| 10k-point stretches, stddev | 23.39 | 10.77 | 2.17× |
| 8-point stretches, avg | 7.21 | 3.88 | 1.86× |
| Runs of exactly 2, avg | 5.95 | 6.29 | 0.95× |
| Interleaved point by point, avg | 6.56 | 6.71 | 0.98× |
| Interleaved point by point, latest | 8.62 | 9.08 | 0.95× |
| Half the series share timestamps, avg | 5.40 | 5.75 | 0.94× |

**This one has a cost.** Data with no runs is 2–6% slower in the isolated merge, which is 1–3% of server time for those queries. Two cheaper variants were tried and measured; this is the best of the three. Identical timestamps across series take a separate path and are unaffected.

*Response body* (`lib/http/response_formatter.cpp`). The JSON body was built in a `std::string` and then copied into the `sstring` the reply carries — 27 MB for a million points, both alive at once. It is now built in the `sstring` directly.

**Pass 2 — high-entropy floats, response reservation**

*Scheme choice for high-entropy floats.* The search minimises exceptions and nothing else, so for full-precision data it picks a 15-digit exponent under which about half the values "round-trip" as 53-bit integers and the rest become 10-byte exceptions: **11 bytes for an 8-byte double**. When the first block comes out wider than 48 bits per value, the encoder now encodes the same block with the existing ALP_RD scheme and keeps the smaller. Clean decimal data never reaches that branch and encodes to the same bytes as before. No format change: ALP_RD streams were already valid and older binaries read them.

| Shape | Bytes/value before | After | Decode ns/value before | After |
|---|---:|---:|---:|---:|
| Full-precision | 10.96 | 7.10 (−35%) | 4.42 | 2.48 |
| float32 widened to double | 9.13 | 7.11 (−22%) | 3.40 | 2.50 |
| 40% full-precision mixed in | 8.86 | 6.74 (−24%) | 2.36 | 2.20 |
| Clean decimals (all shapes) | unchanged | unchanged | unchanged | unchanged |

*Response reservation.* The formatter reserved 66 bytes per point. The true worst case is 47 (a 20-character integer and a 25-character double, each with a comma), confirmed by formatting ~400M random and extreme values against a canary-filled buffer; a test now holds the number writer to those widths. A million-point response reserves 47 MB instead of 66 MB.

**Pass 3 — checksums**

The WAL, manifest, tombstone and rollup checksums used table-driven CRC32 (slicing-by-8). `lib/utils/crc32_simd.cpp` adds a carry-less-multiply fold through Highway with runtime dispatch, for inputs of 128 bytes and up. Same polynomial, same checksum — 240,000 differential cases against the tables with no mismatch — so nothing on disk changes. Throughput is 3.3 → 21.4 GB/s at 1 KB and above, 2.1× at 128 bytes. CRC was 2.5–5% of ingest CPU in the profiles; its end-to-end effect was not measured separately from the final cumulative runs.

**Found and measured, not changed**

1. **Bucketed aggregation runs on a node-based hash map.** About 35% of a fine-grained bucketed query (7-minute buckets over 1M points, ~20 ns/point) is `std::unordered_map<uint64_t, AggregationState>` overhead: bucket-index division and the cache miss behind it (21.6% self time), node allocation and erase, and a sort of the unordered output. The fix is a sorted or dense bucket container, but the type appears at 38 sites in `lib` and 74 in tests, and `BlockAggregator` documents a dependence on its reference stability. This is the largest remaining query-side item.
2. **`{"writes":[...]}` batches go through a JSON DOM.** 60% of server CPU on that path is the DOM parse and another 16% is destroying it; server cost is ~83 ns/point against ~31 for array-format JSON. The existing comment puts the DOM at 10–15× the body in memory. Parsing one write at a time would bound memory to roughly the body size.
3. **Seastar's HTTP server copies every response body once more** (`connection::write_body`), 11.8% of coordinator CPU on large responses. Neither `_content` nor a body writer avoids it in this Seastar version; it needs a patch to the submodule.
4. **Memory-store series regrow from zero after every rollover** — 9.4% of ingest CPU in vector relocation at steady state. Pre-sizing each series from the retiring store removes it, but raises transient memory right after a rollover by up to ~0.6× the store threshold per shard. Given the project's low-memory goals I left it out; it would suit a config switch.
5. **The parameter search is still exception-count-only.** Mixed-precision data (mostly 2 decimals, some full-precision) gets a wide exponent where a narrow one with more exceptions would be smaller. A size-aware search should reach roughly 5.75 bytes/value on the 40%-mixed shape (6.74 now, 8.86 originally) — an estimate, not a measurement.
6. **JSON number formatting is ~30% of large-response CPU.** glaze's double writer always produces 15–17 digits and trims. Protobuf responses avoid it entirely.
7. **Unbucketed `stddev`/`spread` builds a full aggregation state for every point with a single contributor** (19–25 ns/point) though the answer is known in closed form.
8. **`make all` fails without `-DTIMESTAR_NATIVE_ARCH=ON`:** `test/benchmark/bit_operations_benchmark.cpp` uses raw AVX2 intrinsics. Build the targets you need by name.
9. **`timestar_insert_bench --format json-batch` measures the client.** It reported 2.9M pts/s while the server sat about 6% busy; payloads are generated inside the timed interval.

**Validation**

- `timestar_unit_test`: **3,968 tests pass** on two shards, no failures. Ten are new: brute-force references for the parameter search and the scheme decision, a no-expansion check for high-entropy streams, a run-structured merge oracle across nine methods, the number-writer width contract, and the CRC differential tests.
- `timestar_perf_test` gates (`QueryFoldBench*`, `OptimizationBaseline.B1/B3`, `AggregationStallBench*`, `TsmReadPathBenchmark.*`): 16 pass.
- `clang-format` 21.1.6 `--Werror` is clean over all of `lib/` and `bin/` and the touched tests.
- Jest HTTP API suite against the new server: **198 of 198 pass** (14 suites).
- No compiler warnings in the changed files under the project's warning set.
- **Not run:** the Vitest suite (needs two configured servers), sanitizer builds, a `-march=native` build, and any real-disk benchmark (the ext4 volume is 98% full).

**Reproduction**

Baseline binaries are in the ignored `build/perf-baseline-20261122/`, with the starting worktree diff alongside. Drivers and raw results are in [perf_passes_20261122](perf_passes_20261122/).

```bash
cmake --build build --target timestar_http_server timestar_unit_test timestar_perf_test -j 24

T=benchmark/perf_passes_20261122/tools
# Standalone CPU drivers: build.py <src> <out> [--lib <baseline liblibtimestar.a>]
python3 $T/build.py $T/alp_bench2.cpp /tmp/alp_new
python3 $T/build.py $T/alp_bench2.cpp /tmp/alp_base --lib build/perf-baseline-20261122/liblibtimestar.a
python3 $T/build.py $T/merge_bench.cpp /tmp/merge_new
python3 $T/build.py $T/crc_bench.cpp /tmp/crc_bench

# HTTP drivers start private servers on port 18186 and alternate the two builds.
# They hardcode a scratch directory near the top; point it somewhere with room.
$T/ab_insert.sh json 1500 3 json        # format, batches, pairs, tag
$T/ab_insert.sh protobuf 600 3 proto
python3 $T/load_layouts.py 18186        # against a running server, then restart it
$T/ab_query.sh <datadir> 5 q && python3 $T/ab_report.py q
```
