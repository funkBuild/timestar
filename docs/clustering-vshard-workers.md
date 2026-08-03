# Stable VShards and reactor-local storage

**Status:** Implemented, exact v1

**Parent design:** [Cluster Architecture and Implementation Plan](clustering.md)

## Outcome

TimeStar has 4,096 stable logical VShards. A VShard is a Raft, placement,
snapshot, backup, and retirement boundary; it is not a separate local storage
engine. Each reactor owns one Engine, WAL stream, TSM directory, and
NativeIndex. `assignCore(vshard, core_count)` derives which reactor executes a
VShard when the process starts.

Cluster mode accepts only reactor counts for which the hash-to-reactor rule and
VShard-to-reactor rule are cohesive. A supported `--smp` restart recomputes
execution ownership without rewriting data. Cores are never replica failure
domains; placement uses distinct hosts.

## Shipped storage boundary

- Stable node/control state lives at the data-root level.
- Private Raft journals live under `cluster_raft/vshard_<id>` by default.
- Engine WAL, TSM, tombstone, and NativeIndex files live under
  `shard_<reactor>` and may contain multiple VShards.
- Background compaction ordinarily converges mixed generations into
  VShard-pure output, but correctness never assumes physical purity.
- NativeIndex uses its one exact-v1 key grammar; only durable per-series keys
  that require VShard catalog extraction carry the VShard field.
- A snapshot resolves one VShard into a deterministic `TSP1` sidecar, including
  catalog, delete/retention state, revision fences, and logical hashes.
- Snapshot pinning is held by owned Engine generation handles while the
  sidecar is produced. There is no persistent pin-set file.

The full live layout and snapshot rules are in
[ADR 0002](adr/0002-vshard-physical-layout.md). Revision stamping and immutable
generation ordering are in [ADR 0003](adr/0003-point-revision-assignment.md).

## Removed development designs

The greenfield exact-v1 reset removed the worker registry, persisted local
ownership generations, runtime core handoff, `vshards/NNNN` live data tree,
per-VShard local manifest, generic VShard key wrapper, serialized extent map,
persistent snapshot pin set, and pre-Raft revision handoff. They have no reader,
migration tool, or compatibility tests.

Retired development roots are rejected before mutation. Existing development
data must be recreated; a future post-production layout change requires an
explicit v2 migration and rollback design.

## Production gates

- The startup cohesion check rejects unsafe reactor counts.
- Writes are acknowledged only after their configured durability barrier.
- Snapshot/install and retirement affect only the selected VShard's logical
  state and leave unrelated series available.
- Private journals bound one lagging VShard's retention independently.
- Exact-v1 frames reject unknown versions, malformed lengths, failed checksums,
  and trailing data before state is published.
- Multi-host release qualification must prove host-level topology, lifecycle,
  fault recovery, security, and SLO evidence; see
  [Production Readiness](cluster-production-readiness.md).
