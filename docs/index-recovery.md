# Index durability and recovery

An acknowledged Engine write must retain both its data and the metadata needed
to discover that data. The owning shard now stages series metadata and exact day
membership, then syncs its index WAL **before** writing the data WAL. The HTTP
metadata-batch barrier also syncs the index. These barriers do not depend on the
background day-bitmap timer or the HTTP series-announcement cache.

Low-level `NativeIndex::indexInsert` and `getOrCreateSeriesId` stage changes;
call `NativeIndex::sync()` when using them as a durable write boundary. Repeated
membership in the same day does not dirty or rewrite an unchanged bitmap.

## Recovery rules

- Opening an index durably removes the previous clean-shutdown marker before
  writes can be accepted. Recorder generation 3 invalidates older clean markers.
- Tag-postings checkpoints cannot pass an unfinished series creation or a
  skipped bitmap load. An upgrade performs a one-time full postings repair from
  retained series metadata, since an older checkpoint may already be incorrect.
- After an unclean startup, Engine repairs day membership from TSM series
  bounds, including data recovered from the data WAL. The configured repair
  window bounds startup work; it is not a completeness claim about older data.
- The verified day range is persisted. Outside it, discovery uses ordinary
  series/tag/field filtering without day pruning. This also applies to future
  points beyond the repair cap, and to an unclean startup with repair disabled.
  A subsequent clean restart preserves that restriction. Existing series-count
  limits still apply; broad queries can therefore cost more or reach a limit.
- Clamped-history markers are monotonic across cold-cache loads and restarts.
- WAL syncs wait for in-flight flushes. Memtable writes and WAL rotation share
  an ownership lock; retries retain buffered records and dirty bitmap state.
  Writing a new WAL tail never first truncates the previously synced tail.
- Local series creation and replicated schema updates share a schema transaction
  lock through their cold reads and commit, preventing stale field/tag blobs
  from overwriting newer unions. Failed transactions invalidate provisional
  schema caches, and retained write batches are reapplied before later writes
  or flushes. A failed write can therefore still take effect during recovery.
- A failed SST flush keeps its immutable memtable and WAL together until retry
  finishes. Rotation failures leave the active memtable readable and must be
  resolved before another append. WAL removal is directory-synced before a
  newer memtable can be flushed, preventing stale WAL resurrection.
- A retried creation can reuse an ID assigned before its original failure. Its
  metadata transaction conservatively lowers the postings repair checkpoint.
  Day repair reconstructs missing LocalId mappings from retained metadata;
  missing source metadata leaves pruning disabled rather than certifying a
  partial repair.

The first upgrade/startup may take longer because of the one-time postings and
day repair. Normal writes now pay an index durability barrier once per batch.
These changes repair derived indexes where source metadata/data remain; they
cannot reconstruct names or tags from TSM hashes if the original metadata was
already irretrievably lost. They are not a substitute for backups or storage
hardware that honors fsync.

## Regression coverage

`IndexRecoveryRegressionTest` covers durable readback without closing the writer,
concurrent sync/rotation, failed WAL writes, failed SST bloom reads, legacy
checkpoints, cached partial discovery, and monotonic history. `DayBitmapRecoveryTest`
covers the Engine startup wiring, historical/future data and disabled repair.
`IndexProcessCrashTest` launches a separate worker and uses SIGKILL between
acknowledgement and shutdown, then verifies both discovery and actual data across
restarts, including a batch with no external metadata announcement.
`IndexTransactionRecoveryTest` adds concurrent local/broadcast schema writes,
failed-append retries and schema deltas, synchronous/background SST flush retry,
rotation failure, orphaned IDs below checkpoints, and partial repair across restart.

For example, from a disposable test working directory (tests remove their own
shard directories), run the built `timestar_unit_test` with:

```text
--gtest_filter=IndexRecoveryRegressionTest.*:IndexProcessCrashTest.*:DayBitmapRecoveryTest.*
-c 2 --memory 2G --overprovisioned
```
