**Sixth pass: reuse shared timestamp encoding in WAL batches — 11 September 2026**

Follow-up: the [seventh pass](query_pass7_20260911.md) removes a bitmap-pointer lifetime hazard and benchmarks index cache paths. It does not establish that hazard as the cause of the crash recorded here.

The optimization is implemented in the worktree but remains **provisional because of an unresolved allocator crash observed during validation**. It reduces an isolated WAL append workload by 24–72%, without allocating a separate encoding cache. Full HTTP throughput results are inconclusive. This pass does not establish that the insert or query paths are compute- or memory-optimal; the thread goal remains active.

The baseline includes the completed [fifth pass](query_pass5_20260911.md). A [production patch](insert_pass6_20260911/production.patch) isolates the WAL changes from earlier work.

**Change and invariants**

Multi-field writes already share immutable timestamp vectors. `WAL::insertBatch` previously compressed those timestamps again for each field. It now retains the last shared vector's identity and the offset/length of its encoded bytes in the current output buffer. A matching subsequent entry copies those bytes into its own payload.

The bookkeeping consists of one pointer and two `size_t` values, 24 bytes on this machine. There is no separate compressed-data allocation, persistent cache, or cross-shard mutable cache. The request batch keeps timestamp vectors alive throughout the synchronous encoding loop. Owned vectors are never assumed immutable or interchangeable. Interleaved distinct shared vectors replace the one-entry cache; owned entries bypass it.

Offsets survive output-buffer growth. The append grows the buffer before obtaining the source pointer, and the source bytes precede the destination. Entry ordering, timestamp counts, compression-ratio feedback, value encoding, CRC calculation, WAL format, rollover decisions and durability modes are preserved.

**WAL measurements**

The same driver links against the preserved fifth-pass and current libraries/headers. A batch contains 32 float fields sharing 65,536 timestamps, totaling 2,097,152 points. Values are constant within each field. The irregular fixture adds deterministic timestamp jitter. Each process performs one warmup and six timed repetitions per shape; the table reports the median of three process medians, alternating after/before, before/after, after/before.

These runs use CPU 4, one Seastar shard, 4 GiB and tmpfs. Setup and final close are outside timing. The driver explicitly uses `wal_sync_mode=rollover` to isolate encoding and buffered append; **these numbers exclude the durable-ack flush cost**. The separate HTTP comparisons retain the default `always` mode.

| WAL workload | Before | After | Speedup |
|---|---:|---:|---:|
| Shared regular timestamps | 1.584 ms | 1.208 ms | **1.31×** |
| Shared irregular timestamps | 5.104 ms | 1.408 ms | **3.62×** |
| Owned timestamp control | 1.946 ms | 1.914 ms | Essentially unchanged |
| Shared scalar batch, 32 points | 0.0080 ms | 0.0075 ms | Small absolute difference |

Regular, irregular and scalar batches remain exactly 68,502, 235,414 and 3,222 encoded bytes respectively. Byte counts match across all six processes. Separate unit tests compare complete WAL bytes and recovered data, rather than relying only on size equality. See [raw logs and summary](insert_pass6_20260911/results/wal-summary.json).

**Existing HTTP insert benchmark**

The final comparison uses the existing `timestar_insert_bench`: 200 timed batches × 10,000 timestamps × ten fields, plus ten warmup batches, eight connections, seed 42. Servers use four shards on CPUs 0–3 with 8 GiB; the unchanged client runs on CPU 6 with 2 GiB. Fresh tmpfs data directories isolate CPU/memory work from physical-disk variation. Durability remains `always`; tmpfs results are not SSD throughput measurements. Builds and tests do not overlap performance measurements.

| Format | Before, median throughput | After, median throughput | Change |
|---|---:|---:|---:|
| JSON | 109.76 million points/s | 107.54 million points/s | 2.0% lower |
| Protobuf | 75.60 million points/s | 76.19 million points/s | 0.8% higher |

Run ranges overlap. The earlier comparison had the opposite small changes—JSON +2.4%, protobuf −2.0%—so neither format supports a clear end-to-end gain. Both comparisons are archived. The isolated WAL gain applies to the work saved; it must not be presented as a whole-server speedup.

The native benchmark's default all-fields verification requests more than the ten-million-point query limit with this fixture. The comparison driver now disables that check and performs ten separate COUNT queries outside timing, one per field. All twelve final runs recorded 200 successful timed writes with no HTTP/connection failures, and every field returned exactly **2,100,000 points**, including warmups. These are live data checks; WAL recovery and acknowledgement durability are covered separately by tests. See [verified HTTP results](insert_pass6_20260911/results/verified-http/summary.json).

**Unresolved crash and validation status**

An earlier verification-enabled comparison crashed during its first optimized protobuf run after 36 successful timed requests, leaving 164 connection failures. It is excluded from performance summaries and retained under [failed-protobuf-run](insert_pass6_20260911/results/failed-protobuf-run/). The symbolized trace shows Seastar's small-object allocator dereferencing address `0x2` while constructing strings for `NativeIndex::flushDayBitmapsNow`, before the current insert batch reaches the data WAL. That location does not establish where corruption originated or whether this optimization caused it.

Follow-up evidence:

- Eight additional runs of each release version completed without failure.
- A further 64 baseline and 64 optimized protobuf runs completed successfully.
- Four optimized-server runs with AddressSanitizer and UndefinedBehaviorSanitizer completed without reported invalid accesses; leak detection was disabled. These diagnostic timings are not performance evidence.
- The final twelve JSON/protobuf comparisons and all per-field COUNT checks completed successfully.

These checks did **not** reproduce or explain the crash and must not be described as a fix. The candidate remains provisional. The next investigation should preserve both binaries and the failed trace, seek a repeatable allocator-corruption trigger, and distinguish effects of encoding-buffer capacity, allocation patterns and concurrent insert lifetimes before accepting the change.

The release unit suite passed **3,918 tests**, with ten existing skips. A four-shard WAL/group-commit/query/insert selection passed **32 tests**. The new test covers float, integer, boolean and string entries, distinct shared vectors interleaved with an owned vector, underestimated reservation forcing buffer growth, complete WAL byte equality including CRCs/padding, and exact timestamp/value recovery. [Validation excerpts](insert_pass6_20260911/results/validation.txt) and stress/sanitizer logs are archived.

**Reproduction and remaining work**

Preserved baseline artifacts live under ignored `build/perf-insert-pass6-before`. The matching `wal.hpp` and `memory_store.hpp` snapshots are needed for the focused driver because those headers include one another.

```bash
cmake --build build --target timestar_http_server timestar_perf_test timestar_unit_test -j 8
python3 benchmark/insert_pass6_20260911/compile_cpu_bench.py before
python3 benchmark/insert_pass6_20260911/compile_cpu_bench.py after
python3 benchmark/insert_pass6_20260911/compare_cpu.py /dev/shm/tsdb-wal-repeat
python3 benchmark/insert_pass6_20260911/compare_http.py /dev/shm/tsdb-http-repeat
```

Output directories must be new. HTTP drivers use port 18086 and stop their private servers afterward. Failed runs are not successes merely because the native client exits zero. Diagnostic stress scripts and their results are separate from performance comparisons.

The broader goal is incomplete. Besides resolving the crash, a verified remaining insert cost is `InMemorySeries<double>::insert` rebuilding statistics over the full series after small overwrites. Removing that cost requires preserving NaN/Inf handling, extrema, variance accuracy and query pushdown correctness without excessive cache memory. The previous query/aggregation improvements also still have workload-specific tradeoffs documented in their reports.
