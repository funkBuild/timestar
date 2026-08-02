# Cluster integration plan

**Status:** the RF=3 data path, persistent Group 0, production topology changes,
replicated TTL retention, and pattern-delete failover/restart are integrated.
Streaming large snapshots and final release gates remain incomplete.

The authoritative deploy blockers are in
[cluster-production-readiness.md](cluster-production-readiness.md).

## Integration principles

- `SeriesId128 -> VShardId` is canonical and stable.
- Cluster mode maps each VShard to one reactor core; a data Raft group and its
  Engine state live on that core.
- Any node may coordinate. Only the current group leader may propose, and only
  quorum commit plus apply may produce an acknowledgement.
- Committed apply is deterministic and cannot be rejected by transient
  front-door admission.
- Reads and metadata fail closed while a group has committed-but-unapplied work
  or a snapshot install is publishing partial state.
- All wire and persisted formats are explicitly v1 and accept v1 only during
  the greenfield phase.

## Production components

### Placement and core ownership

`VShardDirectory` is the immutable routing view for the current map epoch.
`PlacementTable::mapping` assigns a VShard to its reactor core. Cluster startup
rejects unsupported/non-cohesive core layouts and mixed storage roots.

The control map is pinned for each request. A placement change may redirect a
retry, but cannot silently add or omit a VShard contribution inside one query.

### Typed data commands

`WriteBatch` groups homogeneous typed columns by canonical series key. The one
production Raft command is:

```cpp
using ReplicatedCommand = std::variant<
    WriteBatch,
    DeleteRangeBatch,
    RetentionCutoffCmd>;
```

`EngineDataStateMachine` applies the command through `EngineLocalStore`.
Point revisions equal the Raft log index. Deletes carry bounded,
snapshot-durable receipts. The superseded double-only command and in-memory
reference state machine have been deleted.

### Write routing

`ReplicatedBatchWriteRouter` splits a request by VShard, groups slices by the
current leader, and dispatches borrowed views. `ReplicatedCommandRouter` does the
same for deletes and maintenance commands. Local and remote proposal sinks
return `ProposeOutcome`, whose committed set is the only acknowledgement
authority.

Attempts, overall retries, receiver proposal work, and apply waiters are
deadline-bounded. Admission occurs before proposal; committed apply bypasses
transient backlog admission. Proposal size and uncommitted-log memory have
per-group and per-shard bounds.

### Data-plane transport

`DataPlaneRpc` carries forwarded writes, hinted proposals, node-query work,
metadata, pattern discovery, read-index calls, group-0 requests, and observer
join. The connection handshake sends `{min=1,max=1}`, agrees on v1, and rejects
any peer without v1.

Codecs are bounded, checksummed where applicable, and reject unknown versions,
unknown tags, invalid counts, truncation, and trailing bytes. Request deadlines
include handshake time. TLS credentials must be installed before startup for a
production configuration.

### Raft hosting and persistence

`ShardRaftPlane` and `ReplicatedVShardHost` own one `RaftGroup` and
`EngineDataStateMachine` per hosted VShard. Journal headers bind cluster UUID,
core, boot identity, and segment number. Discovery treats the journal directory
as an exclusive namespace and fails closed on ambiguous artifacts.

Raft envelopes use `TSR1`. Snapshot journal records use `TSRSNAP1`. Snapshot
transfer uses paced 4 MiB chunks with bounded staged memory. Complete VShard
snapshot construction is still capped at 128 MiB and must become streaming
before production approval.

### Snapshots and Engine replacement

The v1 `SnapshotPayload` is self-contained: immutable data objects, series
catalog, VShard identity, data/log fence, and delete-receipt state. Creation
materializes VShard-pure data and detects concurrent tombstone mutation.

Installation validates the complete payload before mutation, quiesces target
WAL conversion, replaces the VShard generation under an Engine-wide install
serialization point, reconstructs catalog/index state, and advances the Raft
applied boundary only after publication completes. Restart checkpoints make the
replacement retryable.

### Reads

Leader reads take a quorum-confirmed ReadIndex. Replica reads expose explicit
linearizable, session, and bounded-staleness policies internally; public modes
that are not fully wired remain rejected. `NodeQueryCoordinator` retries an
alternate assigned replica and merges each VShard once. RPC attempts share the
request deadline.

Metadata and pattern discovery use the same apply fence as data queries so a
successful response cannot omit committed series while catch-up or snapshot
installation is incomplete.

### Group 0 and control state

The production server can host persistent group 0 and explicitly bootstrap its
seed. Group 0 owns cluster identity, membership/control map, jobs, join tokens,
and frozen pattern-delete plans. Control commands use `TCC1`; snapshots use
`TSG0SNP1`.

Movement planning now atomically binds a `TSMJ1` job to the exact source voter
set and next map epoch. Durable progress may advance only one step at a time,
and Group 0 publishes the next complete serving map only after that exact job
is `Done` and only when the submitted map changes its authorized VShard.

Pattern delete freezes a bounded canonical target expansion in group 0 before
any VShard delete proposal. Lookup-first retry recovers a plan that may have
committed after an ambiguous reply. A bounded three-process production-server
gate proves complete-plan visibility on a second voter before coordinator loss,
exact target reuse under a new leader, changed-body conflict, and restart
recovery. Status reports retained plan, target, and encoded-byte totals.

The production driver, runtime publication, and local Raft retirement seam are
wired: dynamic peer/group creation, live directory cutover, applied-membership
fencing, terminal reclaim-floor publication, and exact-v1 journal
quarantine/grace/deletion run in process. Safe reclamation of retired logical
data from shared Engine WAL/TSM/index files and the multi-process end-to-end
proof remain open. Static peer-list editing is not a substitute.

### Storage durability

WAL, TSM, tombstone, NativeIndex WAL/manifest/SSTable, VShard manifests,
snapshot pin sets, and control-map caches use exact canonical namespaces and
fail closed on corruption or identity ambiguity. Publication and retirement
include required file and parent-directory durability barriers.

Failed WAL conversion remains query-visible and admission-accounted until
publication succeeds. Startup recovery fails if recovered data cannot be made
live. TSM and NativeIndex registration abort rather than serving a partial
immutable set.

## V1 versioning policy

Version markers remain mandatory, but no v2-vN implementation, activation
state, compatibility reader, or rolling-upgrade path exists. The current v1
layout is updated in place and development data is recreated after incompatible
changes. See [protocol-versioning.md](protocol-versioning.md).

## Completed integration gates

- [x] Typed WriteBatch and command codec bounds/corruption tests.
- [x] Engine state-machine apply, revision, receipt, and snapshot tests.
- [x] RF=3 quorum commit/apply, failover, movement-brick, and recovery tests on
  the current production state machine.
- [x] Leader-hint write routing and bounded retry tests.
- [x] ReadIndex, alternate-replica routing, and apply-fence tests.
- [x] Snapshot build/install and restart-boundary fault tests.
- [x] Journal, WAL, TSM, tombstone, manifest, SSTable, and namespace negative
  recovery tests.
- [x] Group-0 persistence, frozen-plan bounds, observer admission, and current
  v1 framing tests.
- [x] Group-0 movement-plan ownership, stale-configuration fencing, sequential
  durable progress, and exact completed-job serving-map cutover tests.
- [x] Removal of format activation and retired protocol/layout tests.

## Remaining milestones

### 1. Complete topology ownership

- Wire production join, drain, replace, and remove to group-0 jobs.
- **Done:** wire the bounded production driver that locates the VShard group and drives
  add-learner, catch-up, promote, remove-old, and completion
  steps idempotently after restart. Each pass executes and Group-0-persists one
  step before a later pass can proceed.
- Apply each committed serving-map epoch to the live sharded routing directory.
  **Done:** publication is durable-before-applied, reactor-local, monotonic, and
  idempotent; restart selects the durable high-water map and instantiates groups
  from recovered placement. **Done:** token-authorized destination peers are
  registered on every transport before Group-0 learner membership and restored
  from recovered node records. **Done:** destination data groups are created from
  the receiver's committed Group-0 job before movement starts.
- **Done:** publish the terminal Raft-journal reclaim floor and quarantine the
  local replica only after durable ownership transfer and applied membership
  removal.
- Reclaim the retired VShard's logical Engine data without deleting shared-file
  extents belonging to live VShards.
- Prove no acknowledged loss or duplicate VShard contribution during movement.

### 2. Replicate retention

- [x] Store bounded exact-version TTL policy cells and tombstones in Group 0.
- [x] Serialize one leader-derived cutoff through all 4,096 VShards with a
  durable cursor and globally contiguous sweep ID.
- [x] Snapshot one constant-space retry fence per VShard and fail-stop on ID
  collision or a missing sequence.
- [x] Enable clustered HTTP mutation after unit/snapshot tests and a bounded
  production-server controller-failover/restart gate. Cluster downsampling is
  rejected in v1.

### 3. Stream large snapshots

- Replace the monolithic encoded snapshot string with bounded object streaming.
- Keep verification, catalog, receipt, and data/log fences end to end.
- Resume or safely restart interrupted transfer without retaining two full
  payloads in memory.
- Prove catch-up for a VShard larger than the current 128 MiB cap.

### 4. Close API failover gates

- [x] Pattern delete: ambiguous freeze, group-0 leader loss, retry, restart, and
  exact target reuse.
- Receipt retirement: sustained delete-heavy work, bounded memory, advancing
  floor, snapshot progress, and journal reclaim.
- Cluster-aware streaming: one backfill/live barrier, resumable cursor, no loss
  or silent duplicate through leader movement.

### 5. Release hardening

- Run all multi-process gates one at a time with explicit memory budgets.
- Verify mTLS identity and authorization for every mutating operator action.
- Exercise disk-full, corrupt-artifact, partition, restart, and empty-node
  catch-up on the final commit.
- Record multi-host throughput, latency, failover, catch-up, and movement SLOs.
- Reconcile deployment examples and runbooks with the shipped behavior.
