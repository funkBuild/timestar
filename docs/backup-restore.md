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

The internal exact-v1 cluster artifact, coordinated export, and node-local
restore layers are implemented, but they are not yet an approved production
workflow. Until the all-voter RF=3 recovery gate, authenticated artifact
handling, capacity policy, and disaster-recovery runbook are complete,
clustered deployment remains blocked for production. Do not clone
`node.json`, `control_map.cache`, or `cluster_raft/` into a new node or cluster;
that can duplicate identity or resurrect obsolete membership.

## Pre-release cluster workflow

The in-tree artifact primitives now define one layout, updated in place during
the greenfield phase:

```
cluster-backup/
  manifest.tsbk1
  vshards/
    0000.tsp1
    0001.tsp1
    ...
    4095.tsp1
```

`manifest.tsbk1` is checksummed `TSBK` version 1. It records the source cluster
UUID, portable schema/retention/delete-plan state, and metadata for exactly
4,096 canonical `TSP1` files. Node identities, join tokens, controller
ownership, serving placement, movement jobs, and old Raft membership are
deliberately absent.

Each unit is copied through a fixed 1-MiB buffer, revalidated from the copied
file, fsynced, and published without replacement. The manifest is fsynced and
published last, so its presence is the completeness marker. Validation streams
object bytes without extraction and rejects a bad checksum/catalog/fence,
metadata mismatch, missing or extra unit, noncanonical name, symlink, partial
temporary, or unsupported version. These hashes detect corruption; the
artifact is not an authenticated or encrypted container and must not yet be
treated as a complete off-site backup product.

The bounded in-process gate creates all 4,096 units, includes one real Engine
snapshot, exercises interrupted staging and manifest publication, rejects
corruption/extra/missing entries, restores that real unit into a fresh Engine,
and reads the exact value back. That artifact-only gate does not simulate live
leader capture, new Raft membership, or an RF=3 disaster recovery.

The VShard host and export coordinator now provide the live-capture and
peer-transfer path. A hosting leader obtains a
deadline-bounded quorum ReadIndex, waits for a TSP1 boundary at or beyond that
index, conditionally rolls the active store once, and pins the durable sidecar
name. Exact-v1 peer RPCs can then begin an idempotent operation/VShard session,
read the immutable file in at most 1-MiB binary chunks, and finish it
idempotently. The reply repeats the total size and whole-file hash, a maximum of
eight sessions are retained per reactor, inactive sessions expire after five
minutes, and shutdown drains reads before releasing their pins. A newer Raft
snapshot therefore cannot unlink a source during remote transfer.

The Group-0 leader creates one checksummed `TBEX` v1 checkpoint beside the
archive before staging any unit. It binds one nonzero operation ID to the source
cluster UUID, complete serving map, and authority-free portable control state.
The coordinator processes one VShard at a time, follows leader redirects,
retries all serving replicas, streams to one bounded partial file, verifies the
whole-file hash, and stages the exact `TSP1` without replacement. Completed
archive units are the durable progress journal. A process restart resumes them
only when the same operation and Group-0 fence still match; conflicting state,
an active retention sweep, or a topology/portable-control change refuses
publication instead of mixing recovery points. A second Group-0 quorum fence is
checked before `manifest.tsbk1` is published last.

Each `TSP1` is a quorum-confirmed prefix of its own VShard. The export does not
claim one cluster-wide write timestamp: v1 has no cross-VShard transactions, so
VShards captured later can include later concurrent writes. The RF=3 gate must
still prove that every acknowledged pre-export write is present after restore.

## Pre-release cluster export API

These routes exist for the remaining RF=3 qualification work; their presence
does not remove the production block above. They are unavailable when server
bearer authentication is disabled and must be called on the current Group-0
leader.

Start a new export (the server returns a generated 32-hex operation ID), or
resume by resubmitting the returned ID and exact archive path:

```http
POST /cluster/backup/export
Authorization: Bearer <server-token>
Content-Type: application/json

{"archive_directory":"/srv/timestar-backups/2026-08-03"}
```

Observe the in-process task with `GET /cluster/backup/export`. The response
reports `running`, `cancelled`, `failed`, or `complete`, the operation and
source identities, serving-map epoch, control leader, and completed count out
of 4,096. After a process restart, resume the durable archive with:

```http
POST /cluster/backup/export
Authorization: Bearer <server-token>
Content-Type: application/json

{"archive_directory":"/srv/timestar-backups/2026-08-03",
 "operation_id":"<32-lowercase-hex-id>"}
```

Cancellation is operation-scoped:

```http
POST /cluster/backup/export/cancel
Authorization: Bearer <server-token>
Content-Type: application/json

{"operation_id":"<32-lowercase-hex-id>"}
```

Cancellation removes only the current incomplete download. The immutable
checkpoint and completed units are deliberately retained for a safe resume.
Cancellation is cooperative and can wait for the bounded in-flight capture or
peer-RPC deadline before the status changes from `running` to `cancelled`;
there is no server-side destructive delete operation. A failed operation whose
Group-0 fence changed cannot be reused: select a new empty archive path and a
new operation ID. Operators must not modify the sibling `<archive>.export.v1`
directory or a partial archive.

The server also has an offline, node-local generation-one importer. On a fresh
data root, `--cluster-restore <archive>` validates the complete exact-v1 archive
before Engine or any network listener opens, creates only the local VShard Raft
journals selected by the new epoch-1 serving map, and rebases them to term 1
with the newly configured voter sets. Revision-one empty snapshots are
losslessly re-encoded at revision 2 because Raft index 0 is the sentinel for no
snapshot. The configured control seed additionally receives a term-1 Group-0
snapshot containing the new cluster UUID, one new active seed identity, the new
serving map, and only the portable policy/retention/frozen-delete state. Old
nodes, voters, tokens, jobs, placement authority, and controller ownership are
not imported.

The importer writes a checksummed marker before creating `cluster_raft/`,
durably advances batched progress, revalidates already-written journals after a
crash, publishes `control_map.cache` before marking the node complete, and
refuses ordinary startup while the marker is incomplete. Resume requires the
same archive, cluster UUID, identity, core count, and serving map.

Preparation now exits successfully without opening Engine or a network
listener. It does not authorize that node to start. Collect the immutable
`cluster_restore.v1` marker from every data voter named by the new serving map
and from the Group-0 seed, then finalize one release offline:

```
timestar_cluster_restore finalize --output restore.tsrr1 \
  node1.cluster_restore.v1 node2.cluster_restore.v1 node3.cluster_restore.v1
```

The finalizer refuses an incomplete/duplicate participant set or markers that
disagree on source archive, new cluster UUID, serving map, control seed, or
participant set. Distribute the exact release to every prepared root and start
each with `--cluster-restore-release <release>`. The control seed's first
activation also requires `--cluster-init`. Activation durably installs an
immutable local receipt before normal recovery can open Engine or networking;
later restarts need neither restore option. A prepared root without a matching
release remains fenced offline.

This closes the accidental empty-majority hole in the restore composition: no
prepared voter can start until the offline finalizer has observed every voter's
complete marker. The markers and release are checksummed integrity records, not
cryptographic signatures; the operator and distribution channel remain trusted.
Cluster backup/restore is still unsupported for production until coordinated
live export, the full RF=3 recovery gate, authenticated artifact handling, and
the runbook are complete.

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

The remaining ordered task list before this document can provide an RF=3
procedure is:

- [x] Define and validate a bounded exact-v1 artifact containing all canonical
  VShard units and portable schema, retention, and frozen delete-plan state,
  without old identity, membership, placement, tokens, or controller authority.
- [x] Make unit staging and manifest-last publication crash-safe, reject partial
  or corrupt artifacts, and prove one real Engine snapshot readback.
- [x] On one hosting leader, obtain a deadline-bounded quorum ReadIndex,
  force/wait a safe snapshot boundary at or beyond it, and pin a superseded
  sidecar while it is copied and revalidated.
- [x] Expose the pinned capture to mTLS cluster peers through exact-v1,
  fixed-shape begin/read/finish RPCs with canonical operation/cluster identity,
  bounded binary chunks, bounded expiring sessions, leader redirection, and
  cross-reactor VShard routing.
- [x] Dispatch that capture to every current VShard leader and durably
  coordinate all 4,096 results with one portable Group-0 capture. Retry must
  survive leader changes and process/node restarts without mixing operations.
- [x] Expose authenticated, authorized server/operator commands for starting,
  observing, resuming, cancelling, and safely retaining an export.
- [x] On each node, bootstrap a different cluster UUID and import its selected
  `TSP1` units as generation-one state under new Raft membership. The offline
  importer is idempotent and durably resumable, rejects conflicting progress,
  and does not open Engine or networking while the local marker is partial.
- [x] Fence prepared nodes behind one exact-v1 offline release. Finalization
  requires a complete, consistent marker set for every configured data voter
  plus the Group-0 seed; activation persists a matching receipt before startup.
- [ ] Run the RF=3 live gate: writes during export, full restore, exact readback,
  corrupt/missing/extra artifact rejection, interrupted export/import resume,
  restart during import/activation, all-voter release rejection, and proof that
  old identities cannot join or actuate.
- [ ] Publish artifact retention, capacity/headroom, authenticated integrity or
  encryption, key management, off-site replication, restore, and disaster-
  recovery procedures.
