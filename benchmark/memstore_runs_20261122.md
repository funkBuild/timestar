# Memory store: adopted runs instead of one growing vector — 22 Nov 2026

Branch `release/1.5.0-rc.1`, uncommitted, on top of `benchmark/round3_20261122.md`, which left this as the
one open item on the write path. "Before" is the build at the end of that round
(`build/perf-baseline-round3/`). Raw results and drivers are in `benchmark/memstore_runs_20261122/`.

## Result

150M points per run, 4 shards, 8 GB, alternating builds, medians.

| Ingest | Throughput | Server CPU | Peak RSS |
|---|---|---|---|
| JSON arrays, 8 connections | 118.8M → **130.9M** pts/s (+10%) | 4.34 → **3.87** s (−11%) | 2,292 → **1,809** MB (−21%) |
| Protobuf, 8 connections | 61.3M → **67.3M** pts/s (+10%) | 6.75 → **6.03** s (−11%) | 1,555 → **1,087** MB (−30%) |
| JSON arrays, 1 connection | 49.0M → **57.8M** pts/s (+18%) | 5.22 → **4.47** s (−14%) | 2,172 → **1,373** MB (−37%) |
| Per-point JSON batches (15M points) | 5.27M → 5.30M pts/s | 4.95 → 4.96 s | 600 → 597 MB |

- Per-point batches are unchanged, as they should be: their inserts are small and take the path that was
  already there.
- With one connection the ingest order is deterministic, and the TSM directories are the same size to the
  byte in every run of both builds (332,474,041). Byte-for-byte equality of the files themselves is what
  the new tests assert.
- Under a 2 GB cap with a client that neither throttles nor retries, the server's existing backpressure
  accepted 502 of 1,500 batches instead of 260 (one connection), and 357–388 instead of 260 (eight) — the
  same rule, tripping later because less is held.

Queries over data still in the memory store return bit-identical results. Steady-state times are unchanged;
the first query that needs a series' points after it has been written to now does the merge the insert used
to do (`results/memstore_queries.txt`, 6M points in memory, server ms):

| Query | First run | Later runs |
|---|---|---|
| raw, one series (80k points) | 1.39 → 0.55 | 0.28 → 0.29 |
| avg across hosts, no interval (600k) | 2.71 → 3.29 | 2.10 → 2.18 |
| stddev across hosts, no interval (600k) | 3.17 → 3.93 | 3.09 → 3.07 |
| max, all 100 series, 1h buckets | 5.38 → 8.94 | 4.44 → 4.70 |
| avg by host over the whole range (answered from running stats) | 0.09 → 0.09 | 0.02 → 0.02 |
| latest by host, 1m buckets (600k) | 19.17 → 19.74 | 16.54 → 16.50 |
| count by rack, 5m buckets | 4.19 → 3.96 | 3.98 → 3.73 |

Tests on the final tree: unit 4,013 (10 new) · perf 46 · integration 44 · e2e 45 · http 38 · Jest 198 ·
Vitest 256, all passing. `lib` and `bin` are lint-clean, and all 89 of their translation units compile
(optimizer on) under the exact `-Werror` set CI uses.

## What changed

A series in the memory store was two vectors, and every batch was appended to them: one copy of the batch,
and — each time the vectors doubled — a second copy of everything already there, which by then was cold.
That was 11–14% of ingest CPU, left up to 2× capacity slack, and gave every field of a write its own copy
of the timestamps.

A series is now those two vectors plus a list of **pending runs**. A batch that is a plain in-order append
(at least 1,024 points, strictly increasing, starting after everything held) is not appended; it is kept as
a run. Nothing is relocated, ever, for a series that is only written to.

- **Readers see one series.** The vectors are private; `timestamps()` and `values()` merge the pending runs
  first. Making them private is what found every reader — 38 sites in the library, ~270 in tests.
  `size()`, `empty()`, first/last timestamp and the running stats do not merge, so a query answered from
  stats leaves the runs alone.
- **Everything that is not a plain append is unchanged.** Small batches, out-of-order or duplicate
  timestamps and corrections merge the runs first and then take the existing path, which still owns
  last-write-wins.
- **Flush writes the runs where they are.** `TSMWriter::writeSeriesRuns` uses the block boundaries
  `writeSeries` would use over the concatenation, encodes a block that lies inside one run straight from the
  run, and gathers only a block that straddles two. The file does not depend on how the series was split.

### Whose memory a run holds

The first version adopted every batch as it arrived, and measured best: CPU 4.39 → 3.79 s, RSS 2,257 →
1,628 MB. It was wrong. A write is parsed on the shard that owns the connection and *moved* to the shard
that owns the series, and Seastar gives each shard a fixed pool that is never rebalanced. Keeping those
buffers for the life of a memory store means the connection's shard pays for other shards' stores — with
one ingest connection, for all of them, which on a many-core machine is more than one shard has.

So a run only ever holds its own shard's memory (`timestar::isShardLocalMemory`, from the allocator's
per-shard address range):

- a batch parsed on this shard is adopted as it stands — no copy at all;
- a batch from another shard is copied once into exact-size local storage (no relocation, no slack);
- a write's shared timestamps, when foreign, are copied once per shard and shared by all of the write's
  fields there, rather than once per field.

| Variant (JSON arrays, 8 connections) | Server CPU | Peak RSS |
|---|---|---|
| Before | 4.34 s | 2,292 MB |
| Adopt everything (keeps other shards' memory) — rejected | 3.79 s | 1,628 MB |
| Local adopted, foreign copied per field | 3.96 s | 2,059 MB |
| **Final: plus one local timestamp copy per write per shard** | **3.87 s** | **1,809 MB** |

(Each variant was measured against the baseline in its own run, which read 4.29–4.39 s and 2,257–2,293 MB;
the raw files are in `results/`.)

The memory store now holds *less* foreign memory than it did before this change: the old code moved the
first batch of every series in as it was. (String payloads are still moved, as they always were.)

One other variant was dropped on measurement: merging the runs at flush instead of writing them in place
saved the CPU but raised peak RSS (2,293 → 2,453 MB) — a second copy of the store on its way out.

## Holding it to the old behaviour

`memory_store_runs_test.cpp` replays the same writes three ways — adoption on, adoption with every batch
treated as foreign, and adoption off, which is the old copying path — and requires:

- identical series after every step, across random sequences of in-order batches, tail overwrites,
  in-batch duplicates, overlapping and unsorted batches, small appends, corrections, reads and deletions,
  for all four value types;
- running stats identical bit for bit (the same `update` calls in the same order, so this is exact);
- TSM files identical byte for byte, both for `writeSeriesRuns` against `writeSeries` over run lengths
  chosen around the block size, and end to end for a flushed store;
- that an adopted local batch really is the request's storage, that a foreign one leaves nothing of the
  request alive, and — on memory genuinely allocated on a second shard — that a store ends up holding only
  local memory with one shared timestamp copy per write.

The test found one asymmetry in the old code: a series emptied by deletion and then written again extended
its stale stats. `MemoryStore` erases a series it empties, so it could not be reached through the store,
but the first-insert path now resets the stats anyway.

## Notes

- The cost a reader can see: a query for raw points of a series that has been written to since it was last
  read pays the merge of those batches — the same copy the insert used to make, moved to the first read. A
  series queried after every write behaves as it did before.
- `test/benchmark/bit_operations_benchmark.cpp` did not compile on the default (portable) configuration —
  raw AVX2 intrinsics without `-mavx2` — so a plain `make` failed. Fixed in the follow-up below.
- CI lints `lib` and `bin` only; many files under `test/` do not pass `clang-format --Werror` and did not
  before.

## Follow-up: every suite green

Asked afterwards to clear the remaining test problems. Nothing in the suites themselves was failing; what
was wrong was around them:

- **A plain `make` failed** on `bit_operations_benchmark`. Its one SIMD kernel now goes through Highway like
  the rest of the project, so it builds at any target width. `ctest -L benchmark` runs all six benchmarks.
- **Standalone JS tests only ran against port 8086.** The seven scripts, their runner and
  `http_api_tests/run_tests.sh` now honour `TIMESTAR_HOST` / `TIMESTAR_PORT`, as the Jest suites already did.
- **Two multi-shard tests skipped silently when `CI` is set** (the new cross-shard memory test, and
  `ShardFailureAccountingIsConsistent`). They now follow the existing convention: skip on a one-shard
  developer run, fail in CI.
- **One test never ran under `ctest`.** `IndexWalCrc32cTest.SoftwareFallbackUsesCrc32cPolynomial` read its
  source file by a path relative to the working directory and skipped anywhere but the build root. It now
  gets an absolute path from the build system like the other source-inspection tests, and fails rather
  than skips if the file cannot be read (as does the one other test that skipped on that condition).
- **The disabled CI benchmark job** built a target that was deleted with the legacy encoders, and its
  comparison script wrote its output over the hand-written, tracked `benchmark/benchmark_report.md` when run
  locally. The dead target and its two metrics are gone, and the script writes `ci_benchmark_report.md`. The
  job is still disabled (`if: false`) — re-enabling it needs a baseline from CI hardware.
- **Docs described tests that do not exist** (`test_api/test_http_write.py` and a Python suite in
  `test_api/http_api_tests/README.md`) and stale counts. `CLAUDE.md` and that README now describe what is there.

Final state: unit 4,013 (also as CI runs it, `CI=true … -c 2`) · perf 46 · integration 44 · e2e 45 ·
http 38 · combined binary 4,142 · string script 17 · `ctest` 8,155 per-test launches plus 6 benchmarks ·
Jest 198 · standalone 8 · Vitest 256 — no failures and, with two or more shards, no skips.

Left as they are, deliberately: error-level log lines in passing runs (negative tests corrupt blocks and
manifests on purpose), "Reactor stalled" notices from CPU-bound test bodies, and formatting under `test/`,
which the project's lint gate does not cover.

## Reproducing

`benchmark/memstore_runs_20261122/tools/` (set `SCRATCH`, `TIMESTAR_BUILD`, `BASE_BIN`): `ab_insert.sh
<json|protobuf|json-batch> <batches> <pairs> <tag>` (`CONNS`, `SRV_MEM` to vary connections and memory),
`ab_memq.sh` + `memq.py` + `memq_report.py` (queries over an unflushed store, with a numeric comparison of
the results), `werror_ci.py` (full compile of every first-party unit under CI's warning set).
