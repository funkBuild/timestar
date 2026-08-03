# ADR 0003 — Raft-index revision stamping

**Status:** Implemented, exact v1

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)

## Context

Cluster writes must replay deterministically and duplicate timestamps within a
committed write must resolve identically on every replica. Storage also needs a
compact revision range to fence snapshot and recovery work. Earlier drafts
specified a pre-Raft journal handoff and a per-point TSM revision merge; neither
model is part of the shipped greenfield system.

## Decision

The leader proposes a write without a caller-selected revision. Once its Raft
entry commits, the VShard state machine stamps every point in that entry with
the entry's log index while applying it. All points in one entry therefore
share a revision, and deterministic replay produces the same state.

MemoryStore and WAL data retain point revisions while mutable. Before a store
is flushed, duplicate timestamps are resolved inside that store. TSM files do
not contain a per-point revision column: each block records only the minimum and
maximum source revisions, and the file trailer records the maximum revision.
Those bounds support recovery, snapshots, and retention fences.

Immutable generations use their reserved store-write order for cross-file
last-write-wins. The sequence is reserved when a store rolls over, so a slow old
flush cannot become newer merely because it finishes later. Compaction output
inherits the maximum input data sequence. Because each immutable input has
already resolved its internal duplicates, this ordering supplies one winner
without a second per-point merge format.

Standalone writes are untracked by default. The embedded/local allocator is
used only when revision tracking is explicitly enabled or recovered data
already contains revisions. Recovery restores it above the largest MemoryStore,
WAL, or TSM revision; once recovered, tracking remains enabled so new local
writes cannot fall below durable tracked data. Cluster-supplied revision
vectors are preserved and never restamped by that allocator.

All of this is the current v1 layout. There is no pre-Raft-to-Raft handoff,
historical reader, or migration branch.

## Consequences

- Raft log position is the sole cluster ordering authority.
- TSM storage pays 16 bytes per block and 8 bytes per file for revision bounds,
  not one revision per point.
- Logical LWW depends on reserved store sequence order across immutable files;
  compaction must preserve that rank.
- Development data from retired revision layouts must be recreated.
