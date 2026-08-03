# Backup and Restore

## Scope and production status

The procedures in this document apply only to a standalone, non-partitioned
TimeStar server. They are not a supported backup or restore procedure for an
RF=3 partitioned cluster.

A clustered data root contains persistent node identity, Group-0/control state,
the committed serving-map cache, VShard Engine objects, and per-VShard Raft
journals. Copying only `shard_*` directories omits cluster authority and cannot
produce a restorable cluster backup. Independently copying three live replicas
also does not create one coordinated recovery point.

Cluster export/import into a fresh cluster UUID is not implemented yet. Until
the pinned per-VShard export, membership-scrubbing restore, and recovery gate
are shipped, clustered deployment remains blocked for production. Do not clone
`node.json`, `control_map.cache`, or `cluster_raft/` into a new node or cluster;
that can duplicate identity or resurrect obsolete membership.

## Data Directory Structure

In standalone mode, TimeStar stores data in per-shard directories under the
configured `server.data_dir`. Each shard directory follows this layout:

```
shard_0/
  wal/          # Write-ahead log files (.wal)
  tsm/          # Time-structured merge files (.tsm)
  native_index/ # NativeIndex SSTable + WAL files for metadata
shard_1/
  wal/
  tsm/
  native_index/
...
```

- **`wal/`** -- Write-ahead log entries. WAL files record every write and delete before it is applied to the in-memory store. They are used to recover data that has not yet been flushed to TSM files.
- **`tsm/`** -- Immutable TSM data files containing compressed time series blocks (float, bool, string, integer). Once written, TSM files are never modified -- only replaced during compaction.
- **`native_index/`** -- LSM-tree metadata index (SSTables and a WAL). Stores series-to-ID mappings, tag postings, day bitmaps, HyperLogLog sketches, and bloom filters. Each shard's index is independent.

Back up the complete configured data root, not a hand-selected set of files.
This preserves storage-layout metadata introduced alongside the directories
above.

## Standalone Backup Procedure

TimeStar does not currently expose a dedicated backup API. Use filesystem-level copies instead.

### Option A: Fuzzy Copy (Best Effort Only)

1. Copy the complete configured data root while the server is running.
2. TSM files are immutable, so they are always safe to copy mid-flight.
3. WAL, NativeIndex WAL, manifests, and directory entries may be changing while
   they are copied. An incomplete final WAL record is discarded on recovery,
   but independent file copies are not one atomic recovery point.
4. Treat this as an emergency best-effort copy, not a verified production
   backup. It may omit acknowledged work or combine files from incompatible
   instants.

### Option B: Filesystem Snapshot (Consistent, Minimal Downtime)

For a fully consistent backup, use a filesystem snapshot:

1. Pause writes (stop sending requests, or put a reverse proxy in maintenance mode).
2. Take an LVM, ZFS, or Btrfs snapshot of the volume containing the complete
   configured data root.
3. Resume writes.
4. Copy data from the snapshot at your leisure.

This guarantees all shards are captured at the same point in time.

### Option C: Cold Backup (Full Consistency)

1. Stop the TimeStar server and wait for it to exit successfully.
2. Copy the complete configured data root.
3. Restart the server.

This is the only standalone procedure that does not depend on cross-file
snapshot semantics.

## Standalone Restore Procedure

1. **Stop TimeStar** if it is running.
2. **Rename the existing data root** so it remains available for rollback.
3. **Restore the complete backed-up data root** at the configured path, with
   the original ownership and permissions.
4. **Start TimeStar**. On startup, each shard replays its WAL to recover any writes that were buffered in memory at backup time. Incomplete WAL entries (from a fuzzy snapshot) are automatically discarded.

No additional manual steps are required -- the NativeIndex rebuilds its in-memory state from its own SSTable and WAL files during startup.

## Important Notes

- **Back up the complete standalone root.** Restoring a partial set of shards or
  selected files can produce missing data and broken cross-shard references.
- **WAL recovery is fail-closed.** Recovery discards only an incomplete trailing
  record. An invalid v1 header, checksum, framing, or complete corrupt record
  causes startup to fail rather than silently skipping acknowledged history.
- **TSM files are immutable.** They are never modified after creation, making them safe to copy even while the server is running.
- **NativeIndex files follow LSM conventions.** SSTables are immutable; only the NativeIndex WAL is append-only. The same fuzzy-snapshot considerations that apply to the main WAL apply here.
- **Disk space.** Ensure the restore target has enough space for all shard directories plus headroom for WAL growth and compaction during normal operation.
- **Version compatibility.** During the greenfield phase, restore with the exact
  build that created and verified the backup. Every durable boundary remains
  explicitly v1, but the v1 layout is updated in place and has no development-
  build migration guarantee.

## Required Cluster Backup/Restore Work

The clustered implementation still needs all of the following before this
document can provide an RF=3 procedure:

1. Export a pinned, hash-verified snapshot for every VShard while recording the
   exact logical backup boundary.
2. Include replicated schema, retention, and delete-plan state without copying
   old Raft membership or node identities into the destination.
3. Bootstrap a fresh cluster UUID and import each snapshot as generation-one
   state with new membership.
4. Prove full restore, partial/corrupt backup rejection, interrupted import
   resume, and post-restore readback in a bounded live gate.
5. Publish retention, encryption, key-management, and disaster-recovery
   procedures for the resulting artifact set.
