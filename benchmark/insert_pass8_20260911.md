**Eighth pass: incremental overwrite statistics and timestamp packing at byte offsets — 11 September 2026**

Small corrections to a million-point float series now avoid repeated full-series statistics scans. The focused workload improved **13.3×**, and durable HTTP correction latency on tmpfs fell from **0.306 ms to 0.122 ms**. No persistent statistics cache or per-series field was added. A **17% regression on the 4,096-point correction case remains unresolved**; this is a measured improvement for large series, not proof that all insert/query paths are optimal.

The baseline is the completed seventh-pass worktree, including the still-provisional sixth-pass WAL optimization. [The production patch](insert_pass8_20260911/production.patch) isolates this pass's memory-store and encoder changes.

**Statistics update and correctness**

The existing small-correction path already locates every target timestamp before modifying values. It previously rebuilt all statistics after replacing them, reading the entire series regardless of batch size. The new path keeps that target validation and last-write-wins ordering, including duplicate targets within a request.

For eligible finite corrections, it maintains compensated sum, extrema, first/latest values and central moments using the old and new values. Mean and M2 remain in extended precision during each request. Timestamp lookup costs O(K log N), and updating the summary costs O(K), for K corrections to N stored points. The existing K-entry target vector remains the only variable-sized scratch allocation on this path.

Full recomputation is retained when an old extremum is removed, a replaced value or summary is non-finite, variance becomes non-finite/negative, or the mean is more than one million standard deviations from zero. Short series also retain the SIMD scan: incremental updates require at least 128 stored points per incoming correction. That crossover helps relative to the first prototype but has not eliminated the small-series regression.

A further guard rebuilds whenever M2 crosses into a lower binary exponent. Checking only whether a single request removed half the variance was insufficient: a new regression test gradually replaces almost all interior values with zero while preserving two extrema. The initial candidate ended with M2 = 1.9999999979475771 instead of 2. The revised guard catches cumulative shrinkage at power-of-two boundaries, preventing this amplification of old rounding error without another per-series counter. The test now meets its 1e-12 absolute tolerance. The arithmetic helper is out of line so its extended-precision temporaries do not participate in the common insert function's register allocation.

This pass also fixes a pre-existing deletion/append error. Deletion invalidates the summary, but a subsequent append previously extended the stale count and moments and marked them valid again. Insert now rebuilds an invalid summary from the surviving points before extending it. A regression test deletes half a series, appends a point, and checks count, extrema, endpoints, sum and M2 against the actual survivors.

**Timestamp encoder issue found during validation**

The first sanitizer HTTP run stopped before reaching memory insertion. `IntegerEncoderFFOR::encodeInto` appended packed timestamp words at a byte-aligned WAL offset, cast the destination to `uint64_t*`, and stored through it. UBSan reported a misaligned store in `ffor_pack_u64`.

The unsigned packer now accepts a byte-aligned destination and writes complete words using fixed-size `memcpy`. The two encoder call sites pass their byte pointers directly. There is no alignment scratch buffer, pre-zeroing pass, additional allocation or format change. The power-of-two and generic packing paths both support arbitrary byte offsets.

A standalone UBSan reproducer fails against the preserved baseline header and passes against the candidate. It exercises bit widths 0–64, empty and partial blocks, every byte alignment, guard bytes, and decoding back to the original integers. The unit suite adds equivalent nonempty coverage. [The baseline failure and passing candidate result](insert_pass8_20260911/results/alignment-summary.json) and [the original HTTP trace](insert_pass8_20260911/results/sanitizer-failure/server.log) are retained.

This is a reproduced encoder bug. It has **not** been established as the cause of the older allocator crash from pass six, which remains unresolved.

**CPU measurements and memory**

Both versions use GCC 14 release flags on the same Ryzen 9 7950X, with the same driver compiled against matching headers/libraries. CPU 4 is pinned. Three process pairs alternate after/before, before/after, after/before. Each process performs one warmup and six measured repetitions per shape and size; values below are medians of process medians. Builds, tests and other project benchmarks do not overlap these measurements.

Each repetition pre-generates 50 requests containing 128 corrections; duplicate-target requests contain 256 entries. Initial data, requests, independent checks and destruction of the completed series are outside timing. The extrema fixture alternates removing/reinstating an extremum. The NaN fixture forces special-value fallback; the large-offset fixture uses values around 1e12. Append controls add new timestamps instead of overwriting them.

| Workload, 50 requests | Stored points | Before, ms | After, ms | Speedup |
|---|---:|---:|---:|---:|
| Interior corrections | 4,096 | 0.182 | 0.213 | 0.85×; 17% slower |
| Interior corrections | 16,384 | 0.470 | 0.379 | 1.24× |
| Interior corrections | 65,536 | 1.007 | 0.459 | 2.20× |
| Interior corrections | 1,048,576 | 9.976 | 0.749 | **13.33×** |
| Duplicate targets | 1,048,576 | 10.494 | 1.251 | **8.39×** |
| Alternating extrema | 4,096 | 0.168 | 0.182 | 0.92×; 8.4% slower |
| Alternating extrema | 1,048,576 | 9.854 | 6.030 | 1.63× |
| NaN fallback | 1,048,576 | 97.534 | 96.998 | Essentially unchanged |
| Large-offset fallback | 1,048,576 | 9.878 | 10.016 | 1.4% slower |
| Append control | 1,048,576 | 3.275 | 3.327 | 1.6% slower |

All 24 cases, process ranges and the earlier candidate measurements are retained under [CPU results](insert_pass8_20260911/results/cpu/summary.json) and [initial CPU results](insert_pass8_20260911/results/initial-cpu/summary.json). Small/control regressions are included rather than excluded from the comparison. The smaller cases need further profiling; the out-of-line helper did not resolve their regression.

Both versions report a 144-byte `InMemorySeries<double>` object and a 96-byte statistics object. The million-point correction case retains exactly 16,777,216 bytes of timestamp/value capacity in both versions. All measured point capacities match between versions. These measurements cover object sizes and retained array capacity, **not peak RSS or allocation-call counts**. Source inspection shows no added heap allocation in the statistics updater. See [memory and numerical-error data](insert_pass8_20260911/results/cpu/memory-and-error-summary.json).

The independent long-double oracle exposed an existing large-offset summation limitation: at 65,536 points around 1e12, both versions differ from the reference sum by 680, and M2 by about 7.13, or 1.28e-9 relatively. That case is recorded explicitly and required to match a freshly computed summary exactly; it is not silently counted as meeting the original stricter sum tolerance. The fallback preserves the existing numerical result rather than fixing it. Across the other measured cases, the largest observed relative M2 error was 1.65e-12 before and 5.05e-13 after. Those are observations for these fixtures, not universal error bounds.

**HTTP measurements**

Four server shards use CPUs 0–3 and 8 GiB; the Python client pins itself to CPU 6 after starting the server. Default `always` WAL durability is retained. Fresh tmpfs directories remove physical-disk variation; these are durable-acknowledgement CPU/memory-path measurements, not SSD throughput estimates.

Each run loads 1,048,576 points into one float series in sixteen batches, then sends 220 sequential 128-point correction requests. The first twenty corrections are warmups. Payload generation is outside timing. The probe asserts that no TSM files exist before/after corrections or queries, so the measured series remains in memory. Three alternating pairs give:

| HTTP correction latency | Before | After | Change |
|---|---:|---:|---:|
| Median of process medians | 0.3056 ms | 0.1216 ms | **60.2% lower; 2.51× faster** |

After corrections, SUM, AVG, COUNT, MIN, MAX, STDDEV, FIRST and LATEST are checked against independently computed expected values. Each query method uses eight sequential connections, twenty warmups and fifty timed requests per connection. All responses pass. This distributes the sample across frontend shards; the earlier single-connection comparison is also retained.

Query medians remain mixed: SUM 0.0961 → 0.0904 ms, AVG 0.0919 → 0.0946 ms, COUNT 0.0894 → 0.0985 ms, MIN 0.0926 → 0.0961 ms, MAX 0.0964 → 0.0965 ms, STDDEV 0.0892 → 0.0908 ms, FIRST 0.0940 → 0.0901 ms and LATEST 0.0927 → 0.0893 ms. This pass establishes **no general query latency gain**. It maintains usable summaries rather than deferring the overwrite scan to the next aggregate query. [Final HTTP measurements and verification](insert_pass8_20260911/results/http/summary.json) include all runs.

**Validation and reproduction**

The final full release suite passed **3,926 tests**, with ten existing skips. The final four-shard numeric/encoding selection passed **45 tests**. Tests include 2,000 correction rounds with values near both zero and 1e8, duplicate targets, special-value transitions, extrema, high offsets, variance collapse and deletion followed by append. The final server also completes the million-point correction workload and aggregate checks under AddressSanitizer/UndefinedBehaviorSanitizer, with no reported invalid access or undefined behavior after the alignment fix. Leak detection is disabled. That build uses the default allocator, so Seastar's `--memory` option is ignored. [Validation logs](insert_pass8_20260911/results/validation.txt) include the earlier failures separately.

Preserved baseline artifacts are under ignored `build/perf-insert-pass8-before`. The snapshot includes the server, library and matching memory-store/encoder sources. [Artifact hashes](insert_pass8_20260911/results/artifacts.json) identify the final binaries.

```bash
cmake --build build --target timestar_http_server timestar_unit_test -j 8
python3 benchmark/insert_pass8_20260911/compile_cpu_bench.py before
python3 benchmark/insert_pass8_20260911/compile_cpu_bench.py after
python3 benchmark/insert_pass8_20260911/compare_cpu.py /dev/shm/tsdb-pass8-cpu-repeat
python3 benchmark/insert_pass8_20260911/summarize_memory.py /dev/shm/tsdb-pass8-cpu-repeat
python3 benchmark/insert_pass8_20260911/compare_http.py /dev/shm/tsdb-pass8-http-repeat
```

Use new output directories and run performance comparisons separately from builds/tests. HTTP drivers own private servers on port 18086 and stop them afterward. The standalone alignment-check compile commands are recorded in its summary JSON. The HTTP probe also accepts `TIMESTAR_BENCH_BIN_DIR` to select a rebuilt sanitizer server; use `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1` and `UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1` for that diagnostic run.

The goal remains active. Immediate remaining work includes the short-series regression, special-value/extrema overwrite scans, large-offset summary accuracy, and the older allocator-crash investigation. The timestamp decoder also still copies packed data into an aligned 8 KiB stack buffer before decoding, a potential query-path cost exposed by the alignment investigation. Sustained compaction workloads and peak memory still need broader measurement.

[Pass nine](insert_pass9_20260911.md) rechecks the short-series result and replaces independent timestamp searches with batched lookup. The measured short-series correction regression is resolved in that pass; the old allocator-crash and numerical-accuracy limitations remain open.
