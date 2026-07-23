# ADR 0001 — Journal segmentation and retention for multiplexed VShard groups

**Status:** Accepted (Phase 1 / Task 4 prerequisite; resolves open decision 2)

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)
(sections "Journal safety contract", "Multiplexed object model", "Durability")

**Prerequisite for:** VShard-workers Task 4.0 (durable acknowledgement boundary)
and Task 4b (journal partitioning).

## Context

Storage is addressed by VShard, but there are 4,096 VShards and the plan
forbids 4,096 independent services ("no 4,096 independent services"). Journals
are therefore **per reactor core and multiplexed**: one append stream per core
carries records for every VShard that core currently owns. The owning core is
derived at startup (`assignCore(vshard, coreCount)`, ADR-adjacent, see
`lib/core/vshard.hpp`); there is no persisted local ownership.

The plan fixes the hard invariants (record framing, fenced-failed on I/O error,
GC at the minimum released position with laggard copy-forward, a per-VShard byte
cap, group-commit barriers paying alignment padding). This ADR fixes the
concrete segmentation and retention design those invariants require.

## Decision

### 1. Record framing

Every journal record is length-prefixed and CRC-covered and carries, at minimum:

```
record_len (u32)              # payload length, excludes this header + CRC
crc32c     (u32)              # over the payload bytes only
vshard     (u16)             # 0..4095; the record's owning VShard
vshard_seq (u64)             # per-VShard monotonic sequence (gap-free per VShard)
raft_term  (u64)             # per-VShard Raft hard state
raft_index (u64)             # per-VShard Raft log index
record_kind(u8)              # data / catalog / truncation / hard-state / config / retention
payload    (record_len bytes)
```

`(vshard, vshard_seq)` totally orders one VShard's records independent of their
physical position in the shared stream. Replay routes each record to
`assignCore(vshard, live_core_count)` and applies it in `vshard_seq` order, so
records written under a previous core count remain recoverable under a new one.

### 2. Segments

- A core journal is a directory `journals/core_<n>/` of fixed-capacity segment
  files `seg_<20-digit-zero-padded-segment-number>.jnl`. Segment numbers are
  globally increasing per core and never reused.
- **Segment target size: 64 MiB** (`journal.segment_bytes`, validated, default
  fixed in v1). A record is never split across segments; a record that would
  cross the boundary rotates first. A single record larger than a segment is a
  configuration/logic error and latches the journal (records are bounded by the
  write-batch limit, which is below the segment size).
- Each segment begins with a header: magic, format version, cluster UUID, core
  number, segment number, and the creating process's boot id. Recovery validates
  the header before trusting the segment.

### 3. Group commit and the durability barrier

- Appends accumulate into an O_DIRECT-aligned buffer. A **barrier**
  (`flush` + `fdatasync` covering the buffer's byte range) is scheduled at
  **buffer-fill (the aligned buffer is full) or a maximum delay of 4 ms
  (`journal.max_commit_delay_ms`), whichever comes first.**
- An acknowledged write (and any Raft vote/term response that references a
  record) is released only after the barrier that covers its byte range
  completes — the plan's "no RPC response before its covering barrier" rule and
  the transferred Phase-0 power-loss gate (Task 4.0).
- O_DIRECT alignment padding is paid **per barrier** (the tail is padded to the
  DMA alignment; the next append rewinds to the true logical end). Padding bytes
  per barrier are exported as `journal_barrier_padding_bytes` so the overhead is
  a documented, watchable metric.
- Any append or barrier I/O error **latches the core's journal into a fenced
  failed state**: it rejects all further appends, reports unhealthy, and marks
  every VShard it owns as storage-failed so queries against them return an
  explicit error rather than an omitted-series false negative. Catch-and-continue
  on the durability path is prohibited; crash injection covers injected flush
  errors.

### 4. Watermarks and retention

Each VShard maintains two durable per-VShard watermarks, checkpointed into the
core's manifest (not into the journal stream itself):

- `applied_seq` — the highest `vshard_seq` whose query-visible effects are fully
  materialised into TSM/index/catalog.
- `released_seq` — the highest `vshard_seq` no longer needed for recovery or
  replica catch-up (`min(applied_seq, min over live learners of their matched
  seq)`).

A record at position `P` is reclaimable once every VShard whose records precede
`P` has `released_seq` past it. Because streams are multiplexed, a single laggard
VShard would otherwise pin the whole shared segment.

### 5. Segment GC with laggard copy-forward

- GC reclaims a segment only when **every** record in it is below its VShard's
  `released_seq`.
- When the oldest segment contains live records for a **laggard** VShard while
  the rest of the segment is reclaimable, those live records are **copied
  forward** into the current segment (as opaque, re-CRC-checked records) before
  the old segment is deleted. GC then advances. This bounds shared-log growth to
  the live (un-released) working set, not to the slowest group's absolute age.
- **Per-VShard retained-log byte cap: 256 MiB** (`journal.per_vshard_cap_bytes`).
  If a laggard's un-released log for one VShard exceeds the cap, log catch-up for
  that learner is abandoned: the learner is dropped and **restarted from a fresh
  snapshot** (install-not-replay; see ADR 0002). This is the plan's
  "exceeding the cap abandons log catch-up and restarts from a fresh snapshot".

### 6. Recovery

Startup, for each core it will own after boot-derived assignment:

1. Enumerate its segments in order; validate headers; find the durable tail
   (last record fully covered by a completed barrier — a torn tail past the last
   barrier is discarded, matching the Phase-0 WAL recovery discipline).
2. Reconstruct per-VShard `(term, vote, log suffix)` honouring per-group logical
   truncation records in append order.
3. Route each surviving record to its current owning core and apply in
   `vshard_seq` order.

A logical truncation (post-leadership-conflict suffix drop) is itself an appended
record ordered after the conflict discovery, never an in-place erase.

## Rationale

- **64 MiB segments / 4 ms / 256 MiB cap** are chosen as safe, roundable v1
  defaults: large enough that segment rotation and directory-entry fsync are
  amortised across thousands of records, small enough that GC granularity and
  copy-forward cost stay bounded; 4 ms bounds ack latency while letting a busy
  core coalesce a full aligned buffer; 256 MiB per-VShard cap bounds the pin a
  single stuck learner imposes while comfortably exceeding a normal snapshot
  interval. All three are validated config, frozen (not advertised as tunable) in
  v1, and revisited with measured data before GA.
- Watermarks live in the manifest, not the journal, so GC does not itself append
  to the stream it is trying to reclaim.
- Copy-forward keeps multiplexing (one stream, cheap appends) while giving each
  VShard an independent retention horizon, reconciling "no 4,096 services" with
  per-VShard snapshot/catch-up guarantees.

## Alternatives considered

- **One journal per VShard (4,096 streams).** Rejected by the plan — 4,096 open
  files, 4,096 fsync cadences, and 4,096 group-commit timers per node.
- **Time-based retention.** Rejected: retention must follow the recovery/replica
  frontier (released position), not wall-clock, and cluster-mode bans local
  `now()` in durability decisions.
- **In-place segment compaction (rewrite the oldest segment in place).**
  Rejected: not crash-atomic. Copy-forward-then-delete chooses one complete
  generation on every crash.

## Consequences

- Task 4.0 implements the barrier + fenced-failed latch on this record framing.
- Task 4b implements the watermarks, GC, and copy-forward.
- The per-VShard cap makes learner restart-from-snapshot (ADR 0002) a required,
  tested path, not an exceptional one.
- Metrics: `journal_barrier_padding_bytes`, `journal_segment_count`,
  `journal_copy_forward_bytes`, `journal_per_vshard_retained_bytes`,
  `journal_fenced_total`.
