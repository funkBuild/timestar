# Stable VShards and Derived Local Execution

**Status:** Rewritten 2026-07-23 after the simplification decision. The
persisted worker-registry/ownership machinery originally specified here was
implemented through Task 3b1 (with Task 3b2 in progress, uncommitted) and is
now decommissioned — see "What was dissolved and why". The frozen
`vshards/0000`–`vshards/4095` namespace survives. Next work: Task D0
(decommission), then Task 1 (derived assignment), then Task 4.

**Parent design:** [Cluster Architecture and Implementation Plan](clustering.md)

**Prerequisite:** [Storage Layout Foundation](clustering-starting-point.md)

## Outcome

This epic replaces CPU-core storage identity with stable VShard identity.
Storage is addressed by VShard only; the reactor core that executes a VShard
is derived at startup from the VShard ID and the live core count. A `--smp`
change is a restart plus a recomputed mapping: no data movement, no drain, no
persisted local ownership, and no possibility of two cores serving one VShard,
because assignment happens exactly once per process, at startup.

Reactor cores are never replica failure domains. Replica placement is
host-level and single-stage; core execution inside a host is invisible to
placement, movement, and replication.

## What was dissolved and why

An earlier revision of this epic modelled cores as persisted OSD-like storage
workers: a durable worker registry (`workers.json`), integer-ticket rendezvous
for local placement, effective-ownership generations with a frozen
49,212-byte layout binary, a high-water ownership manifest, and fenced
runtime handoff. Tasks 1, 2a, 2b, 3a, and 3b1 of that revision were
implemented and tested; Task 3b2 was in progress.

That machinery defended exactly one capability the objectives do not require:
rebalancing VShards between cores at runtime without a restart. Since `--smp`
changes already require a restart, and Ceph likewise does not rebalance PGs
across CPU threads, boot-derived assignment provides the same guarantees with
no durable state. The two-owners hazard, the drain protocol, the ownership
rollback witnesses, and both crash-safe metadata stores dissolve with it.

**Kept:**

- The `vshards/0000`–`vshards/4095` directory namespace and its
  `StorageLayout` methods (Task 4 builds on them).
- The root-lock and reserved-name fail-closed startup discipline.
- The crash-safe atomic-install patterns and crash-injection test techniques,
  which transfer directly to Task 4's catalog and manifest work.
- The rendezvous implementation, reusable for node-level placement in the
  control plane (parent plan, Phase 3).

**Removed (Task D0):**

- `worker_registry.*`, `worker_registry_store.*`,
  `effective_vshard_layout.*`, `effective_vshard_layout_store.*`,
  `effective_vshard_manifest.*`, their tests, and the uncommitted Task 3b2
  work (abandoned, not merged).
- The `workers.json`, `vshard_ownership/`, `vshard_ownership.manifest`, and
  `vshard_ownership.initializing` reserved artifacts: a new-format root must
  not contain them, and startup fails closed if they appear.
- `local_storage_placement.*` leaves the local path (parked for Phase 3 node
  placement).

Whole-root rollback detection, which the ownership manifest partially
provided, is owned by the group-0 external epoch witness (parent plan,
Phase 3): a node whose local generations regress relative to what group 0
recorded for it is quarantined rather than trusted.

## Derived assignment rules

- Assignment is a deterministic pure function
  `core = assign(vshard_id, core_count)` with an even spread (for example
  `vshard_id % core_count`); the exact function is defined in one place and
  golden-tested, and changing it is a startup-behaviour change requiring its
  own review, not a storage migration.
- It is computed exactly once, at startup, before storage services open.
  Nothing recomputes or reassigns mid-process.
- Journals are per core; every record is VShard-tagged with a per-VShard
  monotonic sequence. Startup replay routes each record to the current owning
  core — the same routing crash recovery needs — so records written under a
  previous core count remain recoverable.
- Every VShard directory is opened by its assigned core; a VShard directory
  never encodes a core number.

## Implementation tasks

### Task D0: Decommission the worker machinery

Remove the dissolved components listed above, keep the survivors, and add a
startup check that fails closed on legacy worker/ownership artifacts in a
new-format root, with diagnostics pointing at this section.

Gate: build and full suite green with the code removed; startup fails closed
with actionable diagnostics on roots containing legacy worker artifacts; the
`vshards/` namespace tests still pass.

### Task 1: Derived assignment

- Implement the assignment function with golden vectors.
- Wire startup to assign VShards to cores before storage services open.
- Prove replay routing: journal records written under core count N replay
  correctly under core count M.

Gate: any core-count change followed by a restart yields identical query
results with no data rewrite; assignment is identical for a fixed
(vshard, core count) pair across processes and platforms.

### Task 4: Complete VShard storage boundary

Task 4 is the largest storage rework in the plan and is split like the earlier
tasks were. Three ADRs are prerequisites and have **landed**:
[ADR 0001 journal segmentation/retention](adr/0001-journal-segmentation-and-retention.md),
[ADR 0002 physical TSM/NativeIndex layout](adr/0002-vshard-physical-layout.md),
and [ADR 0003 pre-Raft point-revision assignment](adr/0003-point-revision-assignment.md).
All sub-tasks follow the parent plan's multiplexed
object model — per-core journals with `(vshard, sequence)` record headers,
tier-0 TSM multiplexed with per-VShard extents converging to VShard-pure
files at tier ≥ 1, and one per-core NativeIndex with VShard-prefixed keys.

- **Task 4.0 — durable acknowledgement boundary.** Merge main's
  `wal_sync_mode` group commit and extend it to the core journal: an
  acknowledged write is covered by a completed flush-plus-fsync barrier; any
  journal I/O error latches the core's journal into a fenced failed state (no
  catch-and-continue on the durability path); query storage failures become
  explicit rather than omitted series. Power-loss tests for acknowledged
  synchronous writes run here — this discharges the transferred Phase 0 gate.
- **Task 4a — series catalog.** Durable, rebuildable catalog mapping
  `SeriesId128` to measurement, ordered tags, field, and value type;
  catalog-creation records travel in the same journal as data so the two can
  never diverge durably; defined relationship to the existing 0x05 metadata
  keys; rebuild source is journal replay plus catalog snapshots.
- **Task 4b — journal partitioning.** Per-VShard record tagging, per-VShard
  checkpoint/truncation watermarks, segment GC at the minimum resident
  watermark with copy-forward for laggards, and per-VShard memory-store
  accounting with a rollover trigger that does not require 4,096 independent
  stores.
- **Task 4c — TSM, index, and compaction partitioning.** VShard extents in
  tier-0 manifests; VShard-pure files at tier ≥ 1; per-block
  `[minRev, maxRev]` revision ranges replacing `_d<N>` dataSeq ranking inside
  `vshards/`; VShard-prefixed index keys including day bitmaps and HLL
  sketches; compaction scheduling across VShards within a core's existing
  fiber budget; the read path merging per-VShard state without per-VShard
  service instances.
- **Task 4d — snapshot and restore.** One VShard snapshot includes catalog,
  data extents, index extract (prefix range-scan; install is load, not
  rebuild), tombstone objects, and nothing else; persistent pin files with
  lease semantics; restore verifies the whole-snapshot verification hash
  defined in the parent plan's anti-entropy section.

Gate: snapshot/restore preserves the verification hash and discovery metadata
while unrelated VShards remain logically untouched — their verification
hashes and manifests unchanged, with shared tier-0 segment and multiplexed
index bytes exempt from byte-identity.

### Retired Task 6: shard_N migration

The migration tool was removed by the greenfield v1 reset. A root containing
the retired `shard_N` layout fails before mutation; development data must be
recreated in the current v1 layout. There is no compatibility reader, offline
rewriter, migrated generation, or migration-only test suite to maintain.

## Boundary with multi-machine replication

This epic is node-local. Multi-machine placement chooses distinct hosts for
each VShard replica; which core executes a VShard inside a host is derived
locally and invisible to replication. Cores never satisfy replica slots and
never raise the advertised failure-domain count.
