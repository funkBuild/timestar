# Backup and Restore

## Data Directory Structure

TimeStar stores all data in per-shard directories under the build/data root. Each shard directory follows this layout:

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

## Online Backup Procedure

TimeStar does not currently expose a dedicated backup API. Use filesystem-level copies instead.

### Option A: Fuzzy Snapshot (No Downtime)

1. Copy all `shard_*/` directories while the server is running.
2. TSM files are immutable, so they are always safe to copy mid-flight.
3. WAL and NativeIndex WAL files may be actively written. The copied WAL may contain a partial trailing entry -- TimeStar discards incomplete WAL entries on recovery, so this is safe.
4. There is a small window where cross-shard data may be inconsistent (one shard's WAL captured a write that another shard's WAL did not). For most workloads this is acceptable.

### Option B: Filesystem Snapshot (Consistent, Minimal Downtime)

For a fully consistent backup, use a filesystem snapshot:

1. Pause writes (stop sending requests, or put a reverse proxy in maintenance mode).
2. Take an LVM, ZFS, or Btrfs snapshot of the volume containing all shard directories.
3. Resume writes.
4. Copy data from the snapshot at your leisure.

This guarantees all shards are captured at the same point in time.

### Option C: Cold Backup (Full Consistency)

1. Stop the TimeStar server.
2. Copy all `shard_*/` directories.
3. Restart the server.

## Restore Procedure

1. **Stop TimeStar** if it is running.
2. **Remove or rename** the existing `shard_*/` directories.
3. **Copy the backup** `shard_*/` directories into the data root.
4. **Start TimeStar**. On startup, each shard replays its WAL to recover any writes that were buffered in memory at backup time. Incomplete WAL entries (from a fuzzy snapshot) are automatically discarded.

No additional manual steps are required -- the NativeIndex rebuilds its in-memory state from its own SSTable and WAL files during startup.

## Important Notes

- **Back up all shards together.** Series data is distributed across shards via consistent hashing. Restoring a partial set of shards will result in missing data and broken cross-shard references.
- **WAL recovery is automatic.** TimeStar detects and skips truncated or corrupt WAL entries on startup. No manual intervention is needed.
- **TSM files are immutable.** They are never modified after creation, making them safe to copy even while the server is running.
- **NativeIndex files follow LSM conventions.** SSTables are immutable; only the NativeIndex WAL is append-only. The same fuzzy-snapshot considerations that apply to the main WAL apply here.
- **Disk space.** Ensure the restore target has enough space for all shard directories plus headroom for WAL growth and compaction during normal operation.
- **Version compatibility.** Restore backups onto the same TimeStar version that created them. File format changes between versions may cause startup failures.
