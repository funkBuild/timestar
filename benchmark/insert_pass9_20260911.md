**Ninth pass: batched correction timestamp lookup — 11 September 2026**

Corrections now search eight timestamps together, reducing measured CPU time by **2.8–4.2×** across the four correction-series sizes compared with pass eight. The 4,096-point case improves from **0.189 ms to 0.068 ms** per 50 requests, resolving the previously observed short-series regression in this workload. Million-point HTTP correction latency improves by **22.3%**, from 0.1448 ms to 0.1125 ms. This remains a workload-specific improvement; the broader optimization goal is active.

The baseline is the completed eighth-pass worktree. [The production patch](insert_pass9_20260911/production.patch) contains only this pass's changes to `memory_store.cpp`; all earlier insert/query changes are present on both sides. Baseline sources, library and server are preserved under ignored `build/perf-insert-pass9-before`.

**Change and correctness**

The small-correction path previously called `std::lower_bound` independently for every incoming timestamp. Its replacement advances eight binary searches together, using conditional index selection so independent memory accesses can overlap. A scalar conditional-search version handles the final zero to seven keys of larger requests. Requests containing fewer than eight entries retain the original inline `std::lower_bound` loop. Both versions support non-power-of-two series lengths and recognize missing keys, including those beyond the final stored timestamp.

All target positions are validated before any stored value changes. A missing key returns to the existing merge path; request order and duplicate last-write-wins behavior are preserved. The same helper serves double, integer, Boolean and string series. Statistics eligibility, update arithmetic, fallback scans and append behavior are unchanged by this pass.

Complexity remains O(K log N), with the existing K-entry target vector. No persistent field, cache, variable-sized scratch allocation or timestamp copy is added. The helper uses a fixed eight-position stack array; this describes its source-level scratch space, not total compiler-generated stack usage.

New tests compare the actual insertion result against an independent ordered-map oracle across all four value types. Fixtures include series lengths on either side of powers of two, correction batches of 1–33 entries, unsorted and repeated targets, both timestamp endpoints, `UINT64_MAX`, and missing keys at the start, middle and end of the timestamp range. Late misses exercise fallback after complete groups of successful searches. Double cases also verify statistics against the stored points.

**Rechecking the previous regression**

Before editing production code, another three alternating process pairs compared pass seven with pass eight using the unchanged eighth-pass CPU driver. The 4,096-point correction slowdown was **2.9%**, rather than the previously recorded 17%. Disassembly also showed the same 152-byte stack-frame reservation in both double insert functions, weakening the earlier stack-frame hypothesis. The old result is retained as an observation; its magnitude and cause were not established as stable. [Revalidation data](insert_pass9_20260911/results/revalidation/summary.json) and both disassemblies are retained.

A separate exploratory lookup probe compared standard search, scalar conditional search, four-way search and eight-way search on random, sorted and duplicate keys. All results were checked against `std::lower_bound`. Eight-way search was selected from that experiment, then evaluated through the real insertion path. The standalone numbers are diagnostic, not the claimed insert speedup. See [probe source](insert_pass9_20260911/search_probe.cpp) and [output](insert_pass9_20260911/results/search-probe.log).

The supplemental integer benchmark then exposed a separate small-batch regression in the initial candidate: fifty single-point corrections to 65,537 stored points rose from 0.001889 ms to 0.003943 ms. Searching these keys serially cannot overlap independent requests, and the repeated fixture benefits from the original branch prediction. Retaining the original inline loop for entire batches smaller than eight avoids imposing that candidate on this workload. The initial results are preserved separately under `results/initial-search`.

**CPU measurements**

GCC 14 release builds run on the same Ryzen 9 7950X, pinned to CPU 4. Three pairs alternate after/before, before/after, after/before. Each process uses one warmup and six measured repetitions; reported values are medians of process medians. Compilation, tests and other project benchmarks do not overlap performance measurements.

The primary driver is unchanged from pass eight: 50 pre-generated requests of 128 points, or 256 entries for duplicate targets. Setup, independent correctness checks and destruction of the final series are outside timing. All 24 shapes pass their numerical checks, including exact agreement with a freshly computed summary for the pre-existing high-offset accuracy limitation.

| Workload, 50 requests | Stored points | Before, ms | After, ms | Speedup |
|---|---:|---:|---:|---:|
| Interior corrections | 4,096 | 0.1893 | 0.0681 | 2.78× |
| Interior corrections | 16,384 | 0.3839 | 0.0923 | 4.16× |
| Interior corrections | 65,536 | 0.4698 | 0.1423 | 3.30× |
| Interior corrections | 1,048,576 | 0.7528 | 0.1901 | 3.96× |
| Duplicate targets | 4,096 | 0.3643 | 0.0968 | 3.76× |
| Duplicate targets | 1,048,576 | 1.1387 | 0.3028 | 3.76× |
| Alternating extrema | 1,048,576 | 6.0045 | 5.4648 | 1.10× |
| NaN fallback | 1,048,576 | 102.5506 | 96.5294 | 1.06× |
| High-offset fallback | 1,048,576 | 10.0876 | 9.4937 | 1.06× |
| Append control | 4,096 | 0.0548 | 0.0536 | 1.02× |
| Append control | 1,048,576 | 3.3371 | 3.3843 | 0.99×; 1.4% slower |

The shorter correction case is also well below the pre-pass-eight 0.1880 ms revalidation result, though that older comparison ran separately. Full ranges and all controls are retained in [CPU data](insert_pass9_20260911/results/cpu/summary.json). Large special-value series remain dominated by full statistics scans. Small control changes are reported; no broad append improvement is claimed. The initial candidate measured a 3.6% slowdown in the million-point NaN fixture, whereas the final run measures a 6.2% gain; this scan-dominated control varies across runs. Initial CPU and HTTP results are retained separately under `results/initial-cpu` and `results/initial-http`.

Both versions retain the same measured timestamp/value capacities in all cases, including 16,777,216 bytes for the million-point correction series. The series and statistics objects remain 144 and 96 bytes respectively. This does not measure peak RSS or allocation-call counts. [Memory and numerical error data](insert_pass9_20260911/results/cpu/memory-and-error-summary.json) are retained.

**Small requests and missing timestamps**

A supplemental integer-series driver exercises 4,097 and 65,537 stored points with requests containing 1, 7, 8, 9 and 128 entries. Random and sorted corrections are compared with requests whose first or last timestamp is new. Each case performs fifty pre-generated requests, with the same warmup, repetition, CPU pinning and alternating-process protocol. Final ordering, size and last-write-wins values are checked independently after timing. All forty cases pass.

| Integer workload, 50 requests | Before, ms | After, ms | Speedup |
|---|---:|---:|---:|
| 4,097 points, 1 random correction | 0.001553 | 0.001498 | 1.04× |
| 65,537 points, 1 sorted correction | 0.001879 | 0.001723 | 1.09× |
| 65,537 points, 7 sorted corrections | 0.011176 | 0.013060 | 0.86×; 16.9% slower |
| 4,097 points, 8 random corrections | 0.005536 | 0.003592 | 1.54× |
| 4,097 points, 128 random corrections | 0.244845 | 0.035006 | 6.99× |
| 65,537 points, 128 random corrections | 0.421675 | 0.087070 | 4.84× |
| 4,097 points, 128 entries, first key missing | 0.383027 | 0.397469 | 0.96×; 3.8% slower |
| 4,097 points, 128 entries, last key missing | 0.628002 | 0.424325 | 1.48× |
| 65,537 points, 128 entries, last key missing | 4.331847 | 3.908825 | 1.11× |

The single-entry regression in the initial candidate is removed in these measurements. Seven-entry requests remain uncertain: the sorted 65,537-point case is 16.9% slower by the aggregate median, although two of three paired processes improve and individual repetition ranges overlap. A longer-duration focused experiment is needed before attributing this difference to a stable code regression. No universal tiny-batch improvement is claimed. Early misses can add lookup work for keys that will ultimately use the merge path; the eight-entry/4,097-point early-miss case is 6.9% slower by the median. Large batches with late misses benefit from faster validation. All controls and ranges are retained in [supplemental results](insert_pass9_20260911/results/search/summary.json).

**HTTP measurements**

The unchanged pass-eight probe uses four server shards on CPUs 0–3, client CPU 6, an 8 GiB server allocation, default `always` WAL durability and fresh tmpfs directories. It loads one million-point float series, applies 220 sequential 128-point corrections with twenty warmups, then verifies eight aggregates. It asserts the series remains in memory. Three alternating pairs reduce the median of process write medians from **0.144760 ms to 0.112514 ms**, or **22.3%**. Each candidate process is faster than its paired baseline. The initial candidate comparison showed a smaller 10.7% reduction; baseline process medians vary materially across the two comparisons. These measurements exercise CPU/memory paths and durable acknowledgements on tmpfs, not physical SSD throughput.

SUM, AVG, COUNT, MIN, MAX, STDDEV, FIRST and LATEST all match independent expectations. Each method uses eight connections with twenty warmups and fifty timed requests each. Query medians vary in both directions: for example SUM 0.1030 → 0.0903 ms and LATEST 0.0975 → 0.1003 ms. No general query latency improvement is established. [HTTP results](insert_pass9_20260911/results/http/summary.json) retain every run and response check.

**Validation and reproduction**

The full release suite passes **3,930 tests**, with ten existing skips. The four-shard correction selection passes all twelve tests in both release and ASan/UBSan builds. The sanitized tests emit reactor-stall diagnostics while executing synchronous test loops, but no sanitizer error or failed assertion. The updated server completes the million-point correction and eight-aggregate workload under AddressSanitizer/UndefinedBehaviorSanitizer with halt-on-error enabled. Leak detection is disabled; that build uses the default allocator and ignores Seastar's `--memory` setting. Sanitized timing is not used in performance comparisons. ASan emits its usual warning that `makecontext`/`swapcontext` are not fully supported; these checks also use a different allocator from the release build.

```bash
cmake --build build --target timestar_http_server timestar_unit_test -j 8
python3 benchmark/insert_pass9_20260911/compile_cpu_bench.py before
python3 benchmark/insert_pass9_20260911/compile_cpu_bench.py after
python3 benchmark/insert_pass9_20260911/compare_cpu.py /dev/shm/tsdb-pass9-cpu-repeat
python3 benchmark/insert_pass9_20260911/summarize_memory.py /dev/shm/tsdb-pass9-cpu-repeat
python3 benchmark/insert_pass9_20260911/compare_http.py /dev/shm/tsdb-pass9-http-repeat
python3 benchmark/insert_pass9_20260911/compile_cpu_bench.py before search_insert_bench
python3 benchmark/insert_pass9_20260911/compile_cpu_bench.py after search_insert_bench
python3 benchmark/insert_pass9_20260911/compare_search.py /dev/shm/tsdb-pass9-search-repeat
```

Use fresh output directories and run benchmarks separately from builds/tests. The HTTP driver owns its server on port 18086 and stops it afterward. [Artifact hashes](insert_pass9_20260911/results/artifacts.json) identify the measured builds; validation logs and sanitizer output are retained in the results directory.

Remaining work includes more stable tiny-batch and early-backfill measurements, special-value/extrema scans, the inherited high-offset statistics accuracy limitation, query timestamp-decoder copies, sustained compaction and peak-memory measurement. The older allocator crash remains unexplained; this search change does not establish or fix its cause.

[Pass ten](query_pass10_20260911.md) removes timestamp-decoder alignment staging, fixes signed delta overflow, and measures the resulting CPU, stack and TSM query tradeoffs.
