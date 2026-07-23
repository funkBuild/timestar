# ADR 0003 — Pre-Raft point-revision assignment and Raft-index compatibility

**Status:** Accepted (Phase 1 / Task 4 prerequisite; resolves the
journal-record / point-revision framing of open decision 1)

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)
(sections "Log record", "TSM and catalog requirements")

**Prerequisite for:** VShard-workers Task 4a/4c (revision column, `[minRev,maxRev]`
block ranges, read-path merge) and forward-compatible with Phase 2 Multi-Raft.

## Context

Duplicate-timestamp resolution (last-write-wins) must be **replicated and
comparable across replicas**, not the local `_d<N>` tier/sequence ranking used
today (two replicas that compact independently cannot compare local sequence
numbers). The plan's log record carries a "deterministic point revision", and
Task 4 is implemented **before** Multi-Raft (Phase 2). So Phase 1 must assign
revisions with a scheme that (a) gives correct LWW now, single-node, and (b) is
compatible with the Raft log index when it arrives — the "Raft-index
compatibility rule".

## Decision

### 1. A point revision is a per-VShard monotonic `u64`

Revision is scoped to one VShard (the LWW unit is `(vshard, series, timestamp)`)
and strictly increases over the life of that VShard. LWW winner = **highest
revision** at a given `(series, timestamp)`; ties cannot occur because a revision
is assigned to at most one write of a given point.

Reserved values:

- **Revision 0** is reserved for **migrated pre-cluster data** (from `shard_N`).
  It is the global floor: any replicated write (revision ≥ 1) beats migrated
  data at the same point. Migration (Task 6) emits every point at revision 0.
- **Revisions ≥ 1** are assigned to writes accepted by the VShard.

### 2. Phase 1 (pre-Raft, single node): revision = per-VShard journal sequence

Journals are already per-VShard sequenced (`vshard_seq`, ADR 0001). In Phase 1
the point revision **is** the `vshard_seq` of the journal record that carried the
write (starting at 1; 0 stays reserved for migration). This is:

- **Deterministic** — assigned by the durable journal append, replay reproduces
  it exactly.
- **Monotonic per VShard** — `vshard_seq` is gap-free and increasing.
- **Correct for LWW** — a later write to the same point has a higher sequence,
  hence a higher revision, hence wins.

A single journal record that writes many points assigns them all the same
revision (its `vshard_seq`); they have distinct timestamps within the record, so
no intra-record point-level tie needs resolving.

### 3. Raft-index compatibility rule (Phase 2 handoff)

When Multi-Raft is introduced, the point revision becomes the per-VShard **Raft
log index** of the entry that carried the write — also a per-VShard monotonic
`u64`. Compatibility is preserved by one rule:

> When a VShard transitions from the pre-Raft journal to a Raft log, the Raft log
> index space is initialised to continue **strictly above** the highest revision
> already assigned by the pre-Raft journal (`first_raft_index >
> max_pre_raft_revision`). Revision comparisons therefore never invert across the
> handoff.

Because both the pre-Raft `vshard_seq` and the Raft index are "the position of
the write in that VShard's authoritative log", the revision is *always* "the log
position", and the handoff is a monotonic continuation, not a re-numbering. The
reserved 0 (migration) stays below both.

### 4. Storage of revisions

Per ADR 0002 section 5:

- **Steady-state blocks (tier ≥ 1, VShard-pure, deduplicated):** store only a
  per-block `[minRev, maxRev]` in the index entry. No per-point revision column —
  the block holds at most one point per timestamp (the LWW winner), so a point's
  revision is not needed for correctness once duplicates are resolved; the range
  is retained for anti-entropy and for the read-path skip below.
- **Tier-0 blocks that may hold intra-file duplicates:** carry a **per-point
  revision column** (one `u64` per point, delta+FFOR encoded like timestamps —
  revisions are dense and monotonic, so they compress well).
- Flush and compaction **always materialise the LWW winner**; a compacted output
  block never contains two points at one timestamp.

### 5. Read-path merge

- Two blocks whose `[minRev,maxRev]` ranges **do not overlap** for the same
  `(series, timestamp)` region: the higher-range block wins outright; no
  per-point comparison, no revision column read.
- Where ranges **overlap** (only possible when a tier-0 block with intra-file
  dups is involved): compare per-point revisions and take the max per timestamp.
- The result is placement-independent: memory store, tier-0, and tier ≥ 1 all
  agree on the winner because they compare the same replicated revision, not
  local sequence numbers (this is the cluster-mode analogue of the Phase-0 LWW
  guarantee).

## Rationale

- Making the revision the log position (journal sequence now, Raft index later)
  means there is exactly one source of truth for ordering, and the pre-Raft →
  Raft transition is a monotonic continuation rather than a migration of
  revisions.
- Reserving 0 for migrated data encodes the intended precedence (any real write
  beats imported legacy data) without a separate "is-migrated" flag.
- Keeping the per-point revision column only in tier-0 (where intra-file
  duplicates can exist) keeps steady-state blocks lean — the common case pays
  only 16 bytes of `[minRev,maxRev]` per block, not a column.

## Alternatives considered

- **Keep `_d<N>` dataSeq ranking.** Rejected: local, non-comparable across
  replicas; the whole point of revisions is a replicated ordering.
- **Wall-clock / HLC timestamps as the revision.** Rejected for v1: cluster mode
  bans local `now()` in durability/ordering decisions; a closed-timestamp/HLC
  mechanism is deferred (open decision 7).
- **Global (not per-VShard) revision counter.** Rejected: it would serialise all
  VShards through one counter and has no natural mapping to the per-VShard Raft
  index.
- **Per-point revision column in every tier.** Rejected: wasteful in the common
  deduplicated case; the `[minRev,maxRev]` range plus tier-0-only column covers
  correctness.

## Consequences

- Task 4a's journal records carry the revision (= `vshard_seq`) so replay
  reproduces LWW deterministically.
- Task 4c stores `[minRev,maxRev]` per block and a revision column in tier-0
  blocks, and implements the overlap-aware read-path merge.
- Task 6 (migration) emits revision 0 for all imported points.
- Phase 2 (Multi-Raft) must enforce `first_raft_index > max_pre_raft_revision`
  per VShard at the handoff; this ADR is the contract it implements against.
