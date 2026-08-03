# Cluster architecture

**Status:** Implemented exact-v1 design; production approval remains blocked by
the evidence listed in [Cluster production readiness](cluster-production-readiness.md).

TimeStar partitions series across 4,096 stable VShards and replicates each
VShard with an independent Raft group. Group 0 owns membership, desired and
serving placement, movement jobs, retention policies, backup coordination, and
other cluster-wide control state. The first release supports RF=3, linearizable
leader reads, and one exact protocol/storage version.

## Boundaries

The cluster provides:

- host-level RF=3 replication and majority durability;
- deterministic VShard routing, apply, snapshot, and recovery;
- online join, VShard move, drain, removal, and replacement;
- replicated TTL retention and frozen pattern-delete plans;
- authenticated, resumable, whole-cluster backup and restore;
- fail-closed exact-v1 wire and persisted formats.

The first release does not provide multi-VShard transactions, follower or
bounded-staleness reads, clustered streaming subscriptions, hot-series lanes,
automatic VShard-count changes, surgical Merkle repair, or any v2 compatibility
path. See [Deferred cluster tasks](clustering-deferred-tasks.md).

## Identity, topology, and placement

A cluster has one immutable 128-bit cluster UUID. Each node has a stable numeric
ID, configured endpoint, and failure-domain label. `node.json` binds a data root
to its cluster and node identities; startup rejects mismatches before serving.

`SeriesId128` is derived from the canonical series key. Its low 12 bits select a
VShard in `0..4095`. Group 0 stores desired placement and publishes a serving
map only after the associated movement job is durably complete. Data routing
uses the serving map; stale epochs cannot regress a node's durable map cache.

RF=3 voters must occupy three distinct configured failure domains. Reactor
cores are execution resources, never replica failure domains.

### CPU shards: derived local execution assignment

Each reactor owns one Engine, WAL stream, TSM directory, and NativeIndex.
`assignCore(vshard, core_count)` derives the local executor at startup. Cluster
mode accepts only core counts for which the series-hash and VShard assignment
are cohesive, ensuring all local data for a VShard is addressable through one
reactor. A supported `--smp` restart recomputes execution ownership without
rewriting files or persisting a second ownership registry.

## Control plane

Group 0 is a Raft-replicated state machine. Its committed state includes:

- node records and forward-only `Joining -> Active -> Draining -> Removed`
  lifecycle;
- desired and serving VShard maps;
- one current, exact-source-voter movement job;
- replicated TTL policy cells and sweep cursor;
- join tokens, backup control state, and delete-plan authority.

Only the current Group-0 leader mutates this state. Commands carry the expected
controller term/map epoch where required and are idempotent for an exact retry;
changed payloads conflict rather than silently replacing ambiguous work.

Node removal is fenced until no serving-map, data-voter, learner, movement, or
Group-0 reference remains. A drain moves one referenced VShard at a time in
canonical order. Removed local replicas install a durable empty Engine
generation, publish a terminal journal reclaim floor, and quarantine their
private journal for a fixed grace period before deletion.

## Data plane and Raft

Each VShard has an independent data Raft group. Private 1-MiB segmented
journals are the production layout; the optional shared-reactor journal is not
allowed for release, restore, or topology mutation. Raft persistence records
HardState, entries, snapshot descriptors, and reclaim floors with exact-v1
framing and checksums.

A leader accepts a bounded write proposal, waits for majority persistence and
commit, and applies it through the VShard state machine. The committed Raft log
index stamps every point in that entry. Apply verifies that all series derive
to the target VShard and routes the Engine transaction to its owning reactor.
Acknowledgement follows the configured WAL durability barrier.

Leader reads execute ReadIndex and wait until the local state machine has
applied the returned commit index. There is no first-release follower-read mode
or client consistency token. Cross-VShard queries pin one serving-map epoch,
route work to the relevant leaders, and return explicit incomplete/retryable
errors rather than silently omitting failed groups.

Transport uses separate bounded DataPlane and Raft RPC channels. Both require
the exact-v1 handshake and mutual TLS in production. Unknown versions, wrong
peer identity, malformed sizes, admission overflow, and unauthenticated
operator requests fail before state mutation.

## Storage and snapshots

Live local storage is reactor-oriented while replication remains VShard-oriented:

```text
<data_dir>/
  node.json
  control_map.cache
  cluster_raft/{group0,vshard_<id>,retired}/...
  shard_<reactor>/
    *.wal
    tsm/{*.tsm,*.tombstone}
    native_index/{MANIFEST,wal/,*.sst}
```

WAL, freshly flushed TSM, and NativeIndex files may contain multiple VShards.
Ordinary partitioned compaction converges mixed TSM input into VShard-pure
output, but logical correctness does not depend on physical purity. Flushes
reserve their store sequence at rollover; compaction inherits the newest input
rank, preserving immutable-generation last-write-wins even when conversions
finish out of order.

One VShard snapshot pins the current Engine generation, resolves tombstones,
and materializes a VShard-pure TSM object plus deterministic catalog, retention,
delete-receipt, and revision-fence state. The exact-v1 `TSP1` stream is produced
into a checksummed, size-bounded sidecar and transferred in paced chunks.
Install validates framing, hashes, object names, catalog, snapshot boundary, and
the resolved logical view before atomically publishing receiver-owned files.

There is no live `vshards/NNNN` directory, per-VShard local manifest, persistent
snapshot pin-set file, old storage reader, or migration rewriter. The precise
layout is in [ADR 0002](adr/0002-vshard-physical-layout.md), revision handling in
[ADR 0003](adr/0003-point-revision-assignment.md), and TSM framing in
[TSM file format](tsm_format.md).

## Membership and movement

A move is bound to a VShard, target map epoch, destination, optional victim,
and exact source voters. The controller performs one durable transition per
pass:

1. materialize and register the destination group;
2. add the destination as a learner;
3. catch it up by snapshot/log replay;
4. enter and leave joint consensus to promote it;
5. transfer leadership away from a victim when necessary;
6. remove the victim and publish the completed serving map.

Recovery derives the next action from committed Group-0 job state plus the
data group's latest configuration. It does not infer success from a controller
process's memory. A stale controller term, changed source set, skipped step, or
conflicting on-disk configuration fails closed.

## Retention and deletes

Clustered retention never uses a replica's wall clock to decide a cutoff. Group
0 owns exact-version TTL policies, selects one cutoff, and advances a durable
all-VShard sweep cursor only after each routed cutoff commits and applies.
Clustered downsampling is rejected in v1.

Pattern deletion first freezes an authenticated, bounded target plan under a
stable operation identity. Every VShard applies the same plan revision, and
delete receipts retain retry results until the destructive history is safely
covered by snapshots and journal reclaim. Compaction applies tombstones without
changing unrelated logical data.

## Backup and restore

Backup captures a stable serving map and portable Group-0 state, then exports
exactly 4,096 hash-identified `TSP1` units. The authenticated `TSBK` manifest is
published last. Progress is durable and exact retries resume immutable staged
units after coordinator or leader loss.

Restore validates the complete artifact set and HMAC, creates a fresh cluster
UUID and node identities, scrubs old membership authority, imports every unit,
and requires matching release markers from all configured voters before any
prepared root can open networking or Engine state. Partial or corrupt archives
remain offline. See [Backup and restore](backup-restore.md).

## Operations, limits, and security

The release requires bearer-authenticated operator routes, mTLS inter-node
transport with endpoint identity checks, protected backup authentication keys,
and external TLS/network policy for client HTTP. Unsafe test transport is never
a production setting.

Startup and runtime enforce finite file-descriptor, HTTP body, RPC admission,
proposal, snapshot, journal, and background-work budgets. Exact values,
bootstrap/join/drain procedures, monitoring, fault qualification, and recovery
rules are in [Cluster operations](cluster-operations.md) and
[Security](security.md).

## Versioning

Every durable and network boundary carries an explicit v1 marker. Encoders and
decoders are updated together in place while the project is greenfield.
Unsupported markers and retired layouts are rejected; development clusters are
recreated. No rolling mixed-version upgrade, downgrade, historical decoder, or
v2 constant is shipped. The marker inventory is in
[Protocol and persisted-format versioning](protocol-versioning.md).

## Production release gate

Implementation tests and bounded local multi-process gates are necessary but
not sufficient evidence for distinct-host production. The exact release commit
must pass the candidate-provenance preflight, host/failure-domain topology
checks, security and durability fault matrix, snapshot/recovery exercises, and
deployment-approved SLO run without changing binaries or configuration between
arms. The authoritative checklist and recorded findings are in
[Cluster production readiness](cluster-production-readiness.md).
