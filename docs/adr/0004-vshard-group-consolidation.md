# ADR 0004 — VShard : Raft-group consolidation

**Status:** Proposed — design only, NOT implemented (write-scaleout Phase 5c).
**The prep step recommended at the bottom of this ADR — an explicit group id in the
VShard directory — IS implemented** (debt D-11, batch F). See "Prep step: DONE" below
for what it converted and what is still identity-assuming. Nothing about the
recommendation has changed: **do not consolidate now.**

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md),
[Cluster Write Scale-Out Plan](../write-scaleout-plan.md) §4 Phase 5

**Relates to:** ADR 0002 (VShard physical layout), ADR 0001 (journal segmentation)

## Context

Today the mapping is **1 VShard : 1 Raft group**, with `VIRTUAL_SHARD_COUNT = 4096`.
Every node hosts a replica of every VShard it is assigned, so at RF == N a node runs
4096 Raft groups spread over its reactor shards (1024 per shard at `--smp 4`). Each
group has its own `RaftNode`, its own `RaftGroup` lock, its own journal directory and
`JournalWriter`, its own election timer, and its own heartbeat schedule.

That is the root of several costs the write path has been paying down phase by phase,
and Phase 5 is where the question "should there be fewer groups?" gets asked properly.

### What was measured (write-scaleout 5-pre, 2026-07-26)

3 nodes, RF=3, `--smp 4`, canonical bench (100 x 10k, hosts=1000, conns=8):

| quantity | idle | under load |
|---|---|---|
| Raft envelopes out, per shard | 2724 /s | 2500-2800 /s |
| Raft envelopes out, per node | ~10.9k /s | ~10.9k /s |
| Raft bytes out, per shard | 0.18 MB/s | 7-40 MB/s |
| node CPU (4 cores) | 4-5 % | 17-21 % |

Three things follow, and they matter more than any of them does alone:

1. **The message RATE is the same idle and loaded.** Load makes messages bigger, not
   more numerous. ~10.9k envelopes/s per node is a *standing* cost of hosting 4096
   groups, paid whether or not the cluster is doing anything.
2. **It is already far below the naive figure.** 4096 groups x 2 peers heartbeating on
   a 20 ms tick would be ~410k msg/s per node. Follower hibernation has taken ~97 % of
   that out already: the observed rate is ~1.3 heartbeat rounds per group per second.
3. **The node is not CPU-bound.** ~80 % of every core is idle at peak. The limiter is
   the quorum round trip, which consolidation does not shorten.

### What consolidation would and would not buy

A 16:1 mapping (4096 VShards over 256 groups) divides by 16 everything that scales with
GROUP count and nothing that scales with DATA volume:

- heartbeat envelopes: ~10.9k/s -> ~680/s per node (and 5a's framing already cut the
  *frames* by ~4x, so the residual syscall cost is ~2700/s -> ~170/s);
- election timers, `RaftGroup` locks, `RaftNode` instances: /16;
- journal writers and directories: 1024 -> 64 per shard. **This is the largest single
  effect and it is not about messages at all** — see below;
- per-proposal fixed cost: a 10k-point batch that today fans out to ~20 VShard
  proposals (each its own quorum round, its own fsync, its own set of waiters) would
  fan out to ~5-8 group proposals carrying more data each.

It buys **nothing** on:

- commit latency, which is a network round trip plus a durable append. Fewer, larger
  proposals amortize the fixed part but the round trip is unchanged, and that round
  trip is what the client waits for (5-pre: commit latency 12-66 ms per proposal,
  against a ~85 ms client p50 at 8 connections).
- throughput on THIS box, where CPU is 80 % idle. Removing CPU work from a system that
  is not CPU-bound does not raise its throughput; Phase 2's 2a/2b/2c measured exactly
  this and came out flat.

### The journal argument, which is the strongest one

`ReplicatedVShardHost::addVShard` creates **one `JournalWriter` per VShard**
(`journalRoot/vshard_N/`). `JournalRaftPersistence::sync()` is `writer.barrier()`,
i.e. a DMA write of the trailing partial block plus an `fdatasync` **on that VShard's
own file**. So the "group commit" the JournalWriter header describes — *"the writer is
SHARED and its barrier makes ALL groups' pending appends on the core durable in one
fsync"* — is not what production does. Each group syncs alone.

5-pre measured the consequence: `persist` (which times only the `sync()` call) is the
largest stage in the RaftGroup profile, ~1.6 ms per drain **on tmpfs**, where an
fdatasync should be nearly free.

This is also why write-scaleout 5b (per-shard fsync coalescing) could not be
implemented as specified: there is no shared per-shard writer to coalesce onto. The two
ways out are (a) a shared per-shard journal, which the recovery path already supports
(`recoverRaftState(records, vshard)` filters a core-wide record set by VShard, and
every record already carries its VShard tag and per-VShard sequence), or (b) this ADR —
16x fewer groups means 16x fewer journals and 16x fewer independent fsync streams.

They are not exclusive, and (a) is the smaller change of the two.

## Decision (proposed)

**Do not implement consolidation now.** Recommended order:

1. **First, share the journal per shard** (ADR 0001 / 0002 follow-up). It captures most
   of the fsync amortization that motivates consolidation, without touching placement,
   movement, or the VShard as the unit of anything. It is the change the journal format
   was designed for.
2. **Re-measure.** If per-group overhead is still material after the journal is shared
   and heartbeat framing (5a) has landed, revisit this ADR with numbers from a
   **real-disk** box. Every number in this document was taken on tmpfs, where fsync is
   nearly free — the case for consolidation is *stronger* on real disks, and it should
   be argued there.
3. **Only then consolidate**, and if so at 8:1 or 16:1 with the migration path below.

Reasons for the ordering, in the order they matter:

- **Movement granularity is the product's, not an implementation detail.** A VShard is
  the unit of placement, movement, repair, balance and snapshot (ADR 0002, Phase 7).
  Consolidating makes the *group* the unit of leadership and replication while the
  VShard stays the unit of placement, so the two stop coinciding — and every piece of
  the movement machinery is written on the assumption that they do. Moving one VShard
  would become "split its group's state", which is a far harder operation than "add a
  learner, catch it up, promote, remove".
- **It is a one-way door for on-disk state.** Group identity is embedded in journal
  layout and in Raft group ids on the wire. Going back after consolidation is a data
  migration, not a config change.
- **The measured benefit is on a resource that is not scarce.** Node CPU is 80 % idle.

## Consequences and design sketch (if it is done)

### Mapping

`group = vshard / K` with K a power of two fixed at cluster creation and recorded in
group 0's committed cluster config. It must NOT be a per-node setting: the group id is
on the wire and in the journal, and two nodes disagreeing about K is a split-brain
generator. Changing K on a live cluster is a full re-shard, not a rolling upgrade.

K = 16 gives 256 groups; K = 8 gives 512. Prefer the smaller K that gets the fsync
count where it needs to be, because every increment of K coarsens movement.

### Placement

Placement is currently per VShard (`PlacementTable`, 4096 entries). Under
consolidation, *replica placement* must become per GROUP — all K VShards of a group
share a replica set, or the group cannot have a single Raft membership. The
`VShardDirectory` keeps its per-VShard view for routing, derived from the group's.

This is the real loss: the placement balancer's freedom drops by K. With 4096 units
and 5 nodes it can balance to within a fraction of a percent; with 256 units the
quantization is 16x coarser, and hot-VShard isolation (moving one hot VShard off a
busy node) stops being expressible at all.

### Movement

`MoveJob` moves a VShard. Under consolidation the only cheap move is a whole group
(K VShards at once), which is K times as much data per step and K times as coarse a
throttle. Splitting a group to move one VShard needs new machinery: snapshot the
group, filter the snapshot by VShard, create a new group, transfer only the moving
VShard's entries. That is essentially a range split, and it is where most of the
implementation cost of this ADR lives.

Recommendation if consolidation happens: **do not build group splitting.** Accept
group-granular movement, and size K so that a group is a tolerable movement unit
(e.g. total_data / 256 per move step).

### Snapshots

`buildVShardSnapshot` is per VShard, and `EngineDataStateMachine` is constructed with
one `VShardId`. A group's state machine would fan out over its K VShards; a snapshot
would be a manifest of K VShard snapshots. `snapshotRevision` (ADR 0003) is per
VShard, and the group's compaction boundary becomes the MINIMUM over its VShards —
one lagging VShard pins the whole group's log. That is a new failure mode worth
naming: today a slow VShard only pins itself.

### Revisions

ADR 0003 stamps revisions from the Raft log index, per VShard. Under a shared group
the log index is shared by K VShards, so revisions become sparse per VShard but stay
strictly monotonic per VShard — which is all LWW requires. **No format change**, and
this is the one place consolidation is free.

### Migration path

There is no in-place migration. A consolidating cluster must either:

- **rebuild**: back up (Phase 8 backup/restore, which already scrubs membership),
  create a new cluster with the new K, restore. Simple, requires downtime or a
  dual-write window; or
- **grow into it**: stand up new groups alongside, move VShards into them with the
  existing per-VShard movement machinery, retire the old groups. Zero downtime, but it
  requires the cluster to run BOTH mappings at once, i.e. the directory must carry a
  per-VShard group id rather than computing `vshard / K`. If consolidation is ever
  likely, make the directory carry an explicit group id NOW — that is a small change
  today and the difference between a rolling migration and a rebuild later.

### Testing

- every Phase 3-8 gate re-run at the new K, in particular movement (Phase 7) and
  snapshot streaming;
- a mixed-K refusal test: a node configured with a different K must fail to join,
  loudly, at handshake — not converge into a cluster that disagrees about group ids;
- the group-commit fan-out test (one batch spanning K VShards of one group commits as
  one proposal) — this is the throughput claim and it should be measured, not assumed.

## Recommendation

**Not now, and not for throughput.** Share the per-shard journal first (smaller change,
captures the dominant cost, and the journal format already supports it), re-measure on
real disks, and consolidate only if the per-group overhead is still material — knowing
that the price is paid in movement granularity, which is the property the whole
placement/movement subsystem was built to provide.

If the decision goes the other way later, put the explicit group id in the VShard
directory first. That single field is what makes the difference between a rolling
migration and a rebuild.

## Prep step: DONE (debt D-11, batch F)

`ControlMap` now carries `std::map<uint16_t,uint16_t> groups` — **sparse, and empty on
every cluster in existence**. `VShardDirectory::groupOf(v)` reads it and falls back to
the identity, so today `groupOf(v) == v` and the system is behaviourally and
byte-for-byte what it was: an identity map emits **no** trailer at all in
`ControlMapCache::serialize()`, so the persisted control map is unchanged on disk.

### What was converted (asks the directory instead of assuming)

`shardForVShard(v)` — the "which reactor owns this VShard's Raft group" helper — is
**gone**, split into two functions that cannot be confused:

- `shardForGroup(groupId)` — the genuine integer op. Its one caller that starts from a
  group id is the transport's raw-deliver hook, where the 2-byte peek off the wire *is*
  the group id, so it is correct by construction.
- `shardOwningVShard(vshard, dir)` — `shardForGroup(dir->groupOf(vshard))`. Every site
  that starts from a VShard goes through it: the write fan-out
  (`writeSlicesToOwningShards`), both propose fan-outs (`proposeSlicesToOwningShards` and
  its hinted twin), `leaderReadIndex`, `leaderCommitIndex`, `resolveLeaders`,
  `ShardRaftPlane::counts`, `ShardRaftPlane::rebalance`, `ClusterDataPlane`'s startup
  `addVShard` fan-out and its `gatherLeaders` sweep.

The three fan-outs additionally funnel through one `bucketByOwningShard(groups, dir)`
rather than open-coding the bucket insert, so a mapping is honoured by all of them or
by none. `ShardRaftPlane` retains the directory pointer (`dir_`) for this; it is the
same immutable-after-`start()` object the routers already read.

`dir` is nullable and null means the identity — only for the unit tests that exercise
the shard fan-out with no control map.

### On-disk / on-wire compatibility

The group section is a **trailer** after the placement entries: magic + count + `(u16
vshard, u16 group)` pairs, emitted only when the mapping is non-identity. `load()`
**fails closed** on any trailing bytes it does not recognise (short trailer, unknown
magic, bytes past a section it did understand) and leaves the previously cached map
untouched, rather than parsing the prefix and silently routing by the identity.

What that does **not** buy, stated plainly: a *pre-D-11* binary's `load()` stops after
the placement entries and ignores trailing bytes, and cannot be made to fail closed
retroactively. The exposure is exactly zero today because nothing produces a
non-identity map — and closing it is the **mixed-K handshake refusal** already listed
under "Testing" below, which is a prerequisite of consolidation, not of this prep step.

### What remains before consolidation is possible

The prep step converted **routing**. It did not, and could not, convert the places where
a group id is *structural*. In rough order of cost:

1. **`ReplicatedVShardHost::addVShard` is still 1 VShard = 1 group** — and deliberately
   so; the file says as much at the call site. Three things there are keyed by the
   VShard id *as* a group id: `registry_.addGroup(vshard, …)`, the journal directory
   `vshard_<id>/`, and `EngineDataStateMachine(store_, VShardId{vshard})`. Converting
   these is not indirection, it *is* consolidation: a group gains a member VShard list
   and its state machine has to fan out over them.
2. **Journal key layout.** Every `JournalRecord` is tagged with a VShard and a per-VShard
   `vshard_seq`, and `recoverRaftState(records, vshard)` filters a record set by VShard.
   That is exactly right for a *shared* journal (see D-10) but under consolidation the
   Raft log is per GROUP, so the log records of a group's K VShards interleave in one
   index space: recovery would filter by `groupOf(record.vshard)` and the per-VShard
   sequence stops being the log's ordering key. Decide this at the same time as D-10 —
   the two touch the same records.
3. **Snapshot boundaries.** `snapshotVShard` builds one VShard's manifest; a group's
   snapshot is a manifest of K of them, and the group's compaction boundary becomes the
   **MIN** over its VShards. Today a lagging VShard pins only itself.
4. **Transport peek.** `raft_rpc_transport` peeks 2 bytes of group id to route an
   envelope to a reactor. This needs no change *if* the group id stays a `uint16_t` and
   groups stay ≤ 4096 — but it does mean group ids and VShard ids share a number space,
   so a mis-set K produces a *valid-looking* id rather than a decode error. That is the
   concrete shape of the split-brain the mixed-K handshake refusal has to prevent.
5. **Movement.** `MoveJob` moves a VShard; under consolidation the cheap move is a whole
   group. See "Movement" above — the recommendation stands: do not build group splitting.
6. **The balancer** (`ShardRaftPlane::rebalance`) enumerates VShards and asks the host
   whether it hosts each one. Under consolidation it should enumerate GROUPS; as written
   it would consider a group K times and count its leadership K times over.
