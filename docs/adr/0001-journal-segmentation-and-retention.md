# ADR 0001 — Raft journal segmentation and retention

**Status:** Accepted and implemented for exact v1

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md)

## Context

TimeStar hosts 4,096 VShard Raft groups. Each group needs durable HardState,
entries, configuration, and snapshot boundaries, but the implementation must
also bound replay, reclaim compacted prefixes, survive torn tails, and prevent
one failed storage device from acknowledging state it did not make durable.

The original plan selected one multiplexed journal per reactor. Production
experience changed that choice before release: the exact-v1 default is one
private journal per locally hosted VShard. This costs descriptors and fsyncs,
but isolates a durability failure to one replica and makes deletion-only GC
independent per group. Replicated startup therefore requires an
`RLIMIT_NOFILE` soft limit of at least 8,192 and raises it to that value when the
hard limit permits.

An optional shared-per-reactor journal remains behind
`TIMESTAR_CLUSTER_SHARED_JOURNAL=1` for measured group-commit qualification.
It is not accepted by the cluster restore workflow and is not the release
default.

## Decision

### Exact-v1 framing

Every segment begins with a CRC-covered header containing the exact format
version, cluster UUID, reactor number, segment number, and boot ID. Segment
filenames are canonical zero-padded identities and are never reused.

Every record is encoded little-endian as:

```text
body_len:u32 | crc32:u32 | vshard:u16 | vshard_seq:u64 |
raft_term:u64 | raft_index:u64 | kind:u8 | payload
```

The CRC covers the complete record body. Unknown kinds, reserved VShard bits,
bad lengths, bad CRCs, foreign cluster/core headers, noncanonical directory
entries, segment-number conflicts, and corruption in a sealed segment fail
closed. A torn record tail is repairable only in the final segment; recovery
truncates it to the last complete record.

### Layout and rotation

- The default private layout stores one journal in
  `cluster_raft/.../vshard_<id>/`, with a 1 MiB rotation target.
- The optional shared layout stores one journal per reactor in
  `cluster_raft/.../shard_<reactor>/`, with a 64 MiB rotation target.
- A record never straddles segments. A fresh segment may hold one record larger
  than its target; the Raft proposal and snapshot limits remain the binding
  producer bounds.
- Creating and deleting a segment includes a parent-directory sync. A segment
  is sealed by a durability barrier, truncation to its logical length, another
  flush, and close.

### Durability barrier and failure fence

The Raft driver appends a complete `Ready` and then awaits `sync()`. The direct
sink issues a whole-buffer `fdatasync` for that group. The shared sink may serve
multiple already-enqueued group waiters with one whole-buffer barrier. There is
no time-based 4 ms journal promise: the caller's awaited barrier is the
acknowledgement boundary.

No Raft message is sent, state-machine command applied, or proposal waiter
acknowledged before that covering barrier succeeds. Append, rotation,
snapshot-promotion, directory-sync, or barrier failure permanently fences the
writer and quarantines the affected replica before its failed `Ready` can send,
apply, or acknowledge. With a shared writer the fence deliberately covers
every group on that reactor, because none can prove a durable barrier after the
shared device failure.

### Snapshot and replay bounds

A data group becomes snapshot-eligible after either 8,192 applied entries or
64 MiB of applied entry bytes since its previous snapshot. The sweep runs every
five seconds and does not snapshot the same group more than once per minute.
The private layout produces at most one snapshot per reactor per sweep. The
shared layout derives a sequential batch intended to scan hosted groups within
15 minutes.

Production VShard snapshots are file-backed, limited to 1 TiB, transferred in
4 MiB chunks, and limited to four concurrent outbound transfers per reactor.
A follower behind a compacted prefix installs the retained snapshot rather
than depending on a separate 256 MiB log cap; no
`journal.per_vshard_cap_bytes` setting exists in v1.

### Retention and GC

Each persistence object publishes a monotonic released sequence derived from
its durable snapshot and retained live suffix. Once a minute, the snapshot
maintenance path publishes advanced floors and considers only sealed segments
older than the active segment.

- In the default private layout, a segment is deleted only when all of its
  records are released. A pinned segment retains that VShard's physical suffix;
  GC never writes through the active private writer.
- In the shared layout, fully released segments are deleted independently.
  A partially released segment may copy at most 512 live records into the
  active segment under the shared writer's exclusive barrier, make those copies
  durable, then delete and directory-sync the old segment. Mostly-live segments
  remain pinned, but do not prevent later independently released segments from
  being inspected.

Copy-forward is crash-safe: before its barrier the old segment is authoritative;
after the barrier the new copy is durable; before deletion completes both may
exist. Recovery replays identical `(vshard, vshard_seq)` records idempotently and
rejects uncovered gaps or conflicting state.

Replica retirement publishes a terminal release floor, atomically quarantines
the private journal generation, retains it for 24 hours, and reclaims it only
after the grace marker expires. Re-adding that VShard clears the old generation's
watermark before the first new append.

## Operational consequences

- Use the private layout for release qualification and restore. Do not set
  `TIMESTAR_CLUSTER_SHARED_JOURNAL` in a production candidate.
- Replicated startup fails before Engine or Raft opens if the process cannot
  obtain 8,192 file descriptors.
- `/cluster/status` exposes journal fsync/sync counts, GC passes, deleted and
  pinned segments, copy-forward records, retired/reclaimed replicas, snapshots,
  transfer backlog, and durability failures.
- Alert on any `raft_durability_failures`, steadily pinned segment growth,
  snapshot production refusal, apply failure, or nonzero durability fence.

The crash, compacted restart, empty-node catch-up, retirement, directory-sync,
and ENOSPC tests are the executable acceptance contract for this ADR.
