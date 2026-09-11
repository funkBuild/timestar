# Bitmap cache lifetime investigation — 2026-09-11

The old code has a confirmed heap use-after-free. The current visitor-based
implementation removes the reproduced invalid access. No further production
change was needed during the final investigation.

## Cause and fix

`addDayMembership` previously awaited a future containing a pointer into
`dayBitmapCache_`, a `tsl::robin_map`. Seastar's coroutine awaiter checks
`need_preempt()` even when the future is ready (`external/seastar/include/seastar/core/coroutine.hh`).
The calling coroutine can therefore suspend after the helper returns the pointer
but before it uses it. Another task can rehash or evict cache entries during
that gap, invalidating the pointer. Copying or using the bitmap immediately
after `co_await` does not close this gap.

The current `withPostingsBitmap`, `withBitmapForInsert`, `withDayBitmap`, and
`withDayBitmapForInsert` helpers invoke synchronous callbacks while their cache
references are valid. Cold paths look up entries again after loading. Callers
retain owned bitmaps or scalar results across suspensions. Insert, discovery,
day-membership, cardinality, and HLL-seeding callers use this interface. The old
pointer-returning helpers have no remaining declarations or calls in `lib`.
Callbacks must not suspend, relocate cache entries, or retain the borrowed pointer.

## Direct reproducer

`preemption_probe.cpp` seeds a cached day, forces Seastar preemption, starts
`addDayMembership`, restores the preemption monitor, and rehashes the cache before
resuming the pending operation. It checks that an acknowledged new ID remains
present. This needs no disk access, HTTP traffic, or low-memory condition.

- Old code: exit 1; ASan reports `heap-use-after-free` in
  `ra_get_index` → `roaring_bitmap_contains` → `addDayMembership`.
  See [old-asan-final.log](results/old-asan-final.log).
- Current code: exit 0; `pending=1`, `added=1 present=1`, with no sanitizer report.
  See [current-asan-final.log](results/current-asan-final.log).

The old probe compiles the saved `build/perf-query-pass7-before` native index
against the sanitizer build's dependencies. It is an isolated comparison of the
native index implementations, not a full rebuild of the historical server.
Both the C++ index and CRoaring's C implementation must be instrumented; the
invalid read occurs in CRoaring. `build-asan` uses `CMAKE_BUILD_TYPE=Sanitize`
and `CMAKE_C_FLAGS=-fsanitize=address,undefined -fno-omit-frame-pointer`.

To rebuild and rerun using the retained build directories, from the repository root:

```sh
python3 benchmark/allocator_crash_20260911/compile_probe.py old-asan
python3 benchmark/allocator_crash_20260911/compile_probe.py current-asan
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 build/allocator-crash-probes/old-asan/preemption_probe -c 1 --cpuset 18 --overprovisioned
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 build/allocator-crash-probes/current-asan/preemption_probe -c 1 --cpuset 19 --overprovisioned
```

Choose available CPUs when running elsewhere. The permanent regression is
`BitmapQueryOptimizationTest.DayMembershipSurvivesPreemptionAndCacheRehash`.
Companion tests cover consumption before resumption, cache eviction, cold reload,
exact cardinality, and HLL seeding at the threshold and after reopening.

## Validation and limits

Release server and unit-test builds succeeded. The final ASan/UBSan server and
unit-test builds also succeeded; see `results/asan-build-final.log`.
The focused release run passed all 190 tests; see `results/release-targeted.log`.

Two four-shard release HTTP stress runs passed with a 0.005 ms task quota,
8 GiB configured memory, and eight client connections. Each run checked all ten
fields against an expected 2,100,000 points per field, including warmup, and
shut down successfully. See `results/current-stress-final/results.json` and
the per-run insert logs. These are correctness checks, not performance measurements.

The original server log records a crash in the Seastar allocator. This confirmed
bitmap lifetime defect is a plausible source of earlier memory corruption, but
the deterministic probe does not establish that it caused that particular crash.
Eight earlier stress runs also passed on the old server, so stress success alone
cannot establish the fix. An earlier 512 MiB run failed insert requests and is
not counted as successful validation. The sanitizer uses Seastar's default
allocator; this comparison verifies the invalid access rather than reproducing
the custom allocator's eventual crash. Leak detection is disabled.

Source and probe hashes are recorded in `results/verified-sha256.txt`.
