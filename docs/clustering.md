# Cluster Architecture and Implementation Plan

**Status:** Proposed

**Last reviewed:** July 2026

**Scope:** Multi-machine placement, replication, queries, failure recovery, and online rebalancing

This document defines the target clustering model for TimeStar. It supersedes
the multi-server and replication sections of the historical
[distributed index plan](history/distributed_index_plan.md). The completed
single-process, shard-per-core index work in that document remains relevant;
its proposed multi-server protocol does not provide sufficient consensus,
read-safety, or movement guarantees and must not be implemented as written.

## Executive decision

TimeStar will use stable virtual shards as replicated placement groups:

- A series hashes to one of 4,096 virtual shards. The virtual-shard count is
  fixed when the cluster is created in the first implementation.
- Each virtual shard is a multiplexed Raft group with one leader and voting
  replicas. Non-voting read replicas are a deferred post-v1 extension.
- Production uses three voting replicas and a majority of two. A write is
  acknowledged only after it is committed by Raft and durably group-committed
  on a majority.
- A built-in meta Raft group ("group 0"), hosted by designated TimeStar nodes
  and reusing the same multi-Raft machinery as the data groups, stores desired
  placement, membership intent, policies, and rebalance jobs. There is no
  external coordination service. Group 0 does not elect data leaders and is
  not in the steady-state read/write path.
- Current/effective Raft membership is distinct from desired placement. A map
  change never makes an unsynchronised replica serve traffic.
- Nodes join as learners. Data moves one virtual shard replica at a time using
  a pinned snapshot, checksum verification, log catch-up, joint membership
  change, and a grace period before source deletion.
- Queries execute at VShard leaders in v1, behind a ReadIndex barrier. Read
  load distributes because leadership is distributed; follower and
  read-replica serving is a deferred post-v1 extension, and the result
  envelope already carries the barrier metadata it will need.
- Placement balances capacity; leader placement balances ingest and, in v1,
  read work. These are related but separate control loops.

The design deliberately provides CP behaviour for committed data. If a virtual
shard cannot reach a majority, writes to that shard stop rather than risk split
brain or acknowledged-data loss.

## Claims and boundaries

### Supported claims

- A healthy production cluster can grow by one storage machine at a time.
- At stable replication factor, adding a machine increases usable storage and
  aggregate throughput for workloads containing many independently hashed
  series.
- A three-or-more-node cluster with three voting replicas tolerates one
  fail-stop or partitioned storage node without losing acknowledged data.
- Detectable corruption of one replica can be served around and repaired from
  healthy replicas.
- Concurrent read traffic is spread across nodes because leadership is
  balanced across nodes. Follower and read-replica serving is a deferred
  extension, not a v1 claim.
- Historical data and replicas are rebalanced online while foreground work
  continues within configured resource and latency budgets.

### Claims not made

- A one- or two-node cluster is not write-available after losing one node.
- Replication does not provide Byzantine fault tolerance. A node returning
  plausible, malicious false data is outside scope.
- One hot series does not gain ingest throughput merely because nodes are
  added. Hot-series striping is a separate design described below.
- An ordinary query over multiple virtual shards is not a globally atomic
  database snapshot. Each shard is read at a safe committed position.
- Replication is not backup. Operator error and cluster-wide corruption require
  separate backups.

## Topology contract

The bootstrap contract must be visible in configuration, status output, and
documentation. It must never silently claim a replication factor that the
available failure domains cannot satisfy.

| Storage nodes | Voting replicas | Majority | One-node write availability | Capacity/throughput effect |
| ---: | ---: | ---: | --- | --- |
| 1 | 1 | 1 | No | Single-node development or migration mode |
| 2 | transitional | — | No | Never a supported target: only a passing state while growing 1 -> 3 |
| 3 | 3 | 2 | Yes | First HA topology; each node holds nearly all logical data |
| 4+ | 3 | 2 | Yes | Usable storage and aggregate throughput grow with each node |

Growing from one to three nodes consumes the new capacity by increasing the
replication factor. RF=2 exists only as the moment between joint-consensus
steps during that growth; it is never an advertised or configurable mode.
Meaningful stable-replication storage scaling begins at
three to four nodes. A cluster may instead be created directly with three
nodes, which is the recommended production bootstrap.

If fewer than the configured number of distinct eligible hosts or racks exist,
placement must fail visibly. It must not put two replicas on one failure domain
and report the shard as fully protected.

## Existing repository constraints

The following are prerequisites, not networking tasks:

1. `lib/core/placement_table.*` currently routes by `hash % coreCount`; the
   virtual-shard server mapping is derived locally and serialized assignments
   are not retained.
2. WAL, TSM, NativeIndex, and compaction paths remain rooted in `shard_N`
   CPU-core directories, now derived from the injected immutable
   `StorageLayout` (completed — see
   [storage layout foundation](clustering-starting-point.md)). The remaining
   constraint is core-count-based identity, not path plumbing.
3. A core-owned TSM file may contain series from many virtual shards. It cannot
   be transferred as one placement group without filtering and rewriting.
4. The current core-count rebalancer deliberately creates an empty NativeIndex,
   while TSM stores only `SeriesId128` rather than the measurement, tags, and
   field required to reconstruct discovery metadata.
5. On this branch WAL immediate flush defaults to disabled, so a successful
   local write does not imply the durable-majority contract required here.
   Main has since landed group commit (`wal_sync_mode`, commit `16dc1c2`,
   default `always`): merging main supplies the local durable-acknowledgement
   primitive, and Task 4.0's acknowledgement boundary must extend that mode
   rather than invent a parallel mechanism.
6. Metadata, day postings, data, and deletes are not one atomic replicated
   state machine. Replication must not preserve this ordering ambiguity.
7. Some query paths can convert unexpected per-series failures into omitted
   series. Cluster queries must distinguish success, explicit partial success,
   and failure.
8. `lib/cluster/scatter_gather.hpp` and storage-side partial aggregation are
   useful boundaries to retain and extend across machines.

The unsafe automatic `ShardRebalancer` is already disabled: startup now runs
the fail-closed `ShardStoreStartupSession` gate before any storage opens (see
[storage layout foundation](clustering-starting-point.md), Step 1). Once
storage identity is decoupled from Seastar cores, changing `smp::count` will
reassign local execution only and will not rewrite data.

## Logical architecture

```text
      +--------------+         +--------------+         +--------------+
      | TimeStar A   |         | TimeStar B   |         | TimeStar C   |
      | meta voter   |<------->| meta voter   |<------->| meta voter   |
      | coordinator  |  Raft   | coordinator  |  Raft   | coordinator  |
      | storage      |         | storage      |         | storage      |
      +------+-------+         +------+-------+         +------+-------+
             |                        |                        |
             +--- group 0: cluster UUID, nodes, desired map, --+
             |        schema, retention, jobs (built-in)       |
             +---------- VShard 27: leader + followers --------+

     Nodes subscribe to group-0 map/policy changes over inter-node RPC

     Client -> any coordinator -> current placement cache
            -> one leader per written VShard
            -> one safe replica per queried VShard
```

Every TimeStar node can accept client HTTP requests and act as coordinator. A
separate stateless coordinator process can be introduced later if coordinator
CPU or deployment isolation requires it; the protocol must not assume one.

## Terminology

- **Virtual shard (VShard):** Stable hash bucket and placement unit.
- **Storage worker:** The Seastar reactor core that executes a VShard for the
  current process lifetime. Assignment is derived at startup from the VShard
  ID and the live core count; it is execution state only — never a persisted
  identity, a placement unit, or a replica failure domain.
- **Replica group:** Raft state machine responsible for one VShard.
- **Leader:** Voting replica that orders writes and supplies read barriers.
- **Follower:** Voting replica that follows the committed log.
- **Learner:** Non-voting replica being bootstrapped; never counts toward the
  configured replication factor or quorum.
- **Read replica:** Deferred post-v1: an optional non-voting, fully caught-up
  replica used to expand per-VShard read capacity without increasing write
  quorum size.
- **Desired placement:** Controller's target set of nodes for a VShard.
- **Effective membership:** The latest configuration entry present in that
  VShard's Raft log, committed or not, exactly as Raft requires; a joint
  configuration needs majorities of both the old and new voter sets.
- **Map epoch:** Version of cluster-wide topology and policy.
- **Term/index:** Raft identity and ordered log position within one VShard.
- **Applied index:** Highest committed entry whose data and all query-visible
  indexes have been applied locally.

### CPU shards: derived local execution assignment

Seastar reactor shards are fixed for the lifetime of a process, so reactor
cores cannot be hot-added. Local execution assignment is therefore not state
at all — it is a pure function computed at startup:

- VShard files are addressed by VShard identity only; no directory or file
  encodes a core number. At boot, each locally hosted VShard is assigned to
  exactly one reactor core by a deterministic function of the VShard ID and
  the live core count.
- Changing `--smp` requires restarting the process. At the next boot the
  derived mapping simply changes: different reactors open the same VShard
  directories. No data is rewritten, nothing is drained, no local ownership
  is persisted, and no VShard can be served by two cores because assignment
  happens exactly once, at startup, in one process.
- Journals are per core and every record is VShard-tagged. Startup replay
  routes records to the current owning core — the same routing crash recovery
  requires anyway — so records written under a previous core count remain
  fully recoverable.
- In an RF=3 cluster, other replicas preserve service while a node restarts
  with a new core count. Single-node mode has the restart outage and nothing
  else: the next boot serves all VShards under the new mapping.
- CPU cores on one machine share a host, power, kernel, and process. They can
  never satisfy two replica slots or increase the advertised failure-domain
  count.

Placement is therefore single-stage: select distinct eligible hosts for a
VShard's replicas. Which core executes the VShard inside a host is invisible
to placement, movement, and replication.

An earlier revision of this design modelled cores as persisted OSD-like
storage workers with a durable registry, fenced ownership generations, and
runtime local handoff. That machinery was deliberately dissolved: it defended
runtime core-to-core rebalancing, a capability the objectives do not require
(Ceph does not rebalance PGs across CPU threads either), at the cost of two
crash-safe metadata stores and a fencing protocol. The
[VShard-workers epic](clustering-vshard-workers.md) records the
decommissioning.

## Cluster and placement maps

### Node identity

Each node has a persistent UUID stored under its data directory. Network
address, hostname, and a small integer are not identities. Reinstalling a node
without its identity is a replacement, not a restart.

A node record contains:

- Node UUID and cluster UUID.
- Client and inter-node endpoints.
- Software and wire-protocol versions.
- Capacity weight and storage class (deferred: v1 assumes homogeneous nodes —
  weight 1, no storage classes; the fields exist but only those values are
  accepted).
- Region, zone, rack, host, and other failure-domain labels.
- Lifecycle state: `JOINING`, `ACTIVE`, `DRAINING`, `REMOVED`.
- Liveness and health summary.

### Desired placement

Weighted rendezvous hashing produces a deterministic initial target set for
each VShard, subject to:

- Distinct-node and configured failure-domain constraints.
- Node lifecycle and health.
- High/low disk watermarks (capacity weights are deferred; v1 places
  equal-weight).
- Maximum simultaneous movements.

Rendezvous output is only a desired state. A controller compares desired state
to effective Raft membership and schedules safe transitions. Nodes route using
effective membership and leader information, never an uncommitted target set.

The controller runs on the group-0 leader and persists jobs in the group-0
state machine, so its fencing is native Raft rather than a bolted-on lease
protocol: the controller epoch is the group-0 term under which its leader was
elected; every job-step RPC to a data node carries that epoch; data nodes
persist the highest epoch seen per group and reject lower ones; and every
controller mutation of control state is a group-0 proposal that commits only
under that same term, so a stalled, deposed controller's late writes fail
term checks instead of interleaving. Job steps are idempotent so the next
group-0 leader resumes after a crash.

### Group 0: the built-in control plane

Control-plane state lives in one distinguished meta Raft group — group 0 —
replicated by TimeStar nodes themselves. There is no external coordination
service: group 0 reuses the Phase 2 multi-Raft engine (elections, PreVote,
CheckQuorum, snapshots, learners, joint consensus, the journal safety
contract) with a control state machine instead of a storage one. This also
avoids bridging an external gRPC client into the Seastar reactor model.
Recommended state-machine key families are:

```text
meta
nodes/<node-uuid>
desired-placement/<vshard>
jobs/<job-uuid>
schema/<measurement>/<field>
retention/<measurement>
controller
```

Group-0 rules:

- **Membership is self-managed.** Group-0 voter membership changes only
  through group-0 configuration entries, never through rendezvous placement —
  this breaks the circular dependency between the meta group and the map it
  stores. Production uses an odd voter count, normally three, auto-selected
  across distinct failure domains and re-selected through joint consensus as
  nodes join, drain, or fail. A one-node development deployment runs a group
  of one but must report that the control plane is not highly available.
- **Bootstrap is an explicit ceremony.** `timestar cluster init` creates the
  cluster UUID and a group of one on the first node; subsequent nodes reach
  the meta group through configured seed addresses and join with tokens
  minted by group 0. A node is never implicitly initialized into a new
  cluster.
- **Watches are streamed notifications.** Nodes subscribe to map-epoch and
  policy changes over the ordinary inter-node RPC. The stream is a hint, not
  a source of truth: a node that misses events reconciles by reading the
  group-0 state machine at a ReadIndex barrier.
- **Linearizable CAS is a group-0 proposal.** Schema and policy
  compare-and-set operations are ordinary state-machine commands committed by
  the group-0 quorum.

A group-0 quorum outage freezes new joins, desired-placement changes, schema
creation, and policy edits. Existing data replica groups continue serving
reads and writes from their committed configurations. No per-write committed
index is written to group 0. Because group 0 runs in the same process as data
groups, a cluster-wide software fault can affect both planes at once; this
shared blast radius is an accepted trade (as in Ceph monitors and CockroachDB
system ranges) because the data plane already tolerates control-plane loss by
design.

Control-plane state restored from a backup or disaster rebuild is a
regression, not an outage. Every node persists the highest group-0 applied
index and map epoch it has acted on; on observing regressed group-0 state it
freezes control-plane actuation (the data plane continues as during an
outage) and requires an explicit operator supersession ceremony before
trusting the restored state. Schema and retention records must be
re-derivable from the replicated series catalogs so restored control state is
reconciled against data-plane truth rather than believed. A resurrected
shorter retention policy or conflicting schema type must never be actuated
silently.

Group 0 is also the external epoch witness that node-local storage requires
to detect coordinated whole-root rollback (the VShard-workers epic explicitly
defers that detection to "a future replicated control plane"): a majority of
healthy group-0 voters is external to any single node's data root, and a node
whose local generations regress relative to the epochs group 0 has recorded
for it is quarantined rather than trusted.

## Replicated state machine

### Multi-Raft execution

TimeStar must multiplex thousands of logical groups across each Seastar core:

- Batch heartbeats and append RPCs by peer node.
- Share connection pools, scheduling groups, timers, and fsync batches.
- Maintain lightweight per-group term, membership, log, commit, and apply state.
- Use one or a small number of core-local journal streams with every record
  tagged by VShard and Raft index.
- Preserve strict single-core ownership of each local group at a time.
- Move local ownership during restart or a fenced handoff without moving files.
- Implement PreVote and CheckQuorum. A rejoining node with a stale log must
  not disrupt stable leaders, and an isolated leader must step down rather
  than serve indefinitely.
- Drive per-group failure detection from node-level liveness so idle groups
  carry no heartbeat or election cost (hibernated groups). A node restart or
  partition must not trigger thousands of simultaneous elections; planned
  restarts and drains transfer leadership in batches first.

Creating 4,096 independent WAL managers, NativeIndex instances, compaction
loops, caches, and timers is not acceptable.

### Journal safety contract

Multiplexed journals must uphold per-group Raft hard-state semantics:

- A vote response or term change is durable (covered by a completed journal
  fsync barrier) before it is sent, sharing the same barrier discipline as
  appends. No RPC response referencing a journal record may precede the
  group-commit barrier that covers it.
- Log-suffix truncation after a leadership conflict is a logical truncation
  record appended to the journal, ordered after the conflict discovery.
  Recovery replays tagged records honouring per-group truncation order and
  reconstructs each group's term, vote, and log suffix.
- Any journal write or fsync error latches the journal — and the core that
  owns it — into a fenced failed state that rejects all further appends and
  reports unhealthy. Catch-and-continue on the durability path is prohibited;
  crash injection must cover injected I/O errors at flush.
- Journal segment GC advances at the minimum released position across resident
  groups. Records of laggard groups are copied forward into new segments so
  one slow group cannot pin unbounded shared log. Per-VShard retained log is
  additionally capped by bytes; exceeding the cap abandons log catch-up for
  that learner and restarts it from a fresh snapshot.
- Group-commit barriers on O_DIRECT journals pay forward alignment padding per
  barrier. Barriers are scheduled at buffer-fill or maximum delay, whichever
  comes first, and padding overhead is a documented metric.

### Log record

The replicated log is authoritative. Each entry includes at least:

```text
format version
cluster UUID
vshard ID
Raft term and index
configuration generation
operation UUID (trace/log correlation only; no idempotency state)
operation kind
series ID and series-catalog definition when first seen
schema and retention versions where relevant
point values, timestamps, and deterministic point revision
delete range or retention cutoff
record checksum
```

Series catalog creation, data mutation, day postings, deletes, and retention
are applied in this order as one logical state transition.
`applied_index` advances only after all query-visible effects are complete.
NativeIndex is derived state and must be rebuildable from durable replicated
state.

### Durability

For three voters, a write commits after a majority of two stores the entry in
their durable journals. The implementation may group-commit multiple requests
within a bounded interval, but HTTP success waits for the group-commit barrier.
An explicit weaker `async` mode may exist, but its potential loss window must be
reported in configuration, responses, and metrics. "Stored in a durable
journal" means a completed flush and fdatasync covering the record's byte
range; error handling follows the journal safety contract above.

`floor(voters / 2) + 1` is the quorum formula:

| Voters | Majority |
| ---: | ---: |
| 1 | 1 |
| 2 | 2 |
| 3 | 2 |
| 5 | 3 |

### Membership changes

Replica membership follows Raft joint consensus:

1. Add the destination as a learner.
2. Snapshot and catch it up without counting it toward quorum.
3. Commit a joint old+new voter configuration.
4. Commit the final configuration with the new voter.
5. If replacing a voter, commit removal of the old voter only after promotion.

A voter that is currently leader is removed only after a completed leadership
transfer, or through an explicit step-down committed with the final
configuration.

A failed controller cannot infer completion from group-0 job state alone. It
queries the current leader for the latest configuration entry in the log (not merely the
committed one — Raft nodes operate on the latest appended configuration) and
resumes the next idempotent step.

## Movable storage model

### Stable identity and layout

Storage identity is VShard-based, not core-based. A possible physical layout is:

```text
<data_dir>/
  node.json
  cluster-map-cache/
  journals/core_<n>/...
  vshards/<id>/
    raft.meta
    MANIFEST
    catalog/
    tsm/
    tombstones/
    snapshots/
```

The `vshards/0000`–`vshards/4095` (zero-padded) namespace frozen by the
VShard-workers epic is authoritative over this sketch. The worker-registry
and ownership artifacts that epic previously froze (`workers.json`,
`vshard_ownership/…`, `vshard_ownership.manifest`,
`vshard_ownership.initializing`) are decommissioned with the dissolved
local-worker machinery; startup fails closed if they appear in a new-format
root. `node.json`, `cluster-map-cache/`, and per-core `journals/` remain
proposals.

### Multiplexed object model

The plan simultaneously mandates multiplexing (journals, index, compaction —
"no 4,096 independent services") and per-VShard guarantees (snapshot,
pinning, extraction). These reconcile through one object model that
Task 4 must cite, rather than each guarantee implicitly assuming pure
physical partitioning:

- **Journals are per core and multiplexed.** Every record carries
  `(vshard, per-VShard monotonic sequence, CRC)`. Replay routes records to
  the boot-derived owning core and orders them per VShard by sequence, so
  records written under a previous core count remain recoverable. There is
  no local ownership generation: assignment is derived once at startup.
- **Tier-0 TSM output may be multiplexed** into shared segments with
  per-VShard extents recorded in the manifest; compaction converges to
  VShard-pure files at tier ≥ 1. Snapshotting a VShard whose tier-0 extents
  live in shared segments extracts only those extents into the snapshot.
- **NativeIndex is per core with VShard-prefixed keys.** A VShard snapshot
  includes a serialized index extract from a prefix range-scan, so install is
  a load, not a rebuild; rebuild-from-journal is the repair path only, with a
  stated cost bound.
- **Per-VShard gates are logical, not byte-for-byte**, wherever shared tier-0
  segments or multiplexed index files exist: unrelated VShards' logical
  hashes and manifests are unchanged, and shared segment bytes are exempt.

### TSM and catalog requirements

- TSM output must be VShard-partitioned, or its manifest must permit efficient
  extraction of one VShard without scanning and rewriting unrelated data.
- Every immutable object has a stable object UUID, format version, byte length,
  and whole-file hash.
- TSM blocks gain CRCs so corruption is attributable and repairable.
- A durable series catalog maps `SeriesId128` to measurement, ordered tags,
  field, and value type.
- Duplicate timestamp resolution uses replicated point revisions, not local
  TSM tier or sequence numbering. Within `vshards/` generations the legacy
  `_d<N>` dataSeq file ranking is dead: flush and compaction always
  materialize the LWW winner, steady-state blocks store only a per-block
  `[minRev, maxRev]` range in the index entry, and a per-point revision
  column exists only in tier-0 blocks that may hold intra-file duplicates.
  Read-path merge compares revisions where ranges overlap. Migrated
  pre-cluster data is emitted at reserved revision 0; replicated writes start
  at revision 1.
- Compaction may run independently on replicas but must preserve logical
  revisions and expose logical time-block hashes for anti-entropy.
- A snapshot pins its referenced manifest objects until all consumers release
  them. Pins are persistent manifest state (`vshards/<id>/snapshots/<S>.pin`
  listing object UUIDs), not open file descriptors: GC deletes only objects
  referenced by neither the MANIFEST nor any pin file, each pin carries a job
  UUID and a controller-renewed lease, and an expired lease drops the pin and
  restarts the job from a fresh snapshot. Fd-based deferred delete remains a
  query-lifetime mechanism only. Pinned-generation bytes count against the
  move-admission disk check.
- Tombstones are first-class per-VShard manifest objects keyed by
  `(series, range, creating revision)`. Legacy per-TSM sidecar tombstone
  files are migration input only and are never written inside `vshards/`.

### Migration from `shard_N`

Migration is an offline, exclusive operation under the existing root lock:
the server is stopped, no catch-up pass exists, and a multi-terabyte node is
down for the rewrite duration. Free space of at least the old generation's
size plus configured slack is verified before any write; otherwise migration
fails closed. The on-disk migration tool must:

1. Read existing core-owned WAL, NativeIndex, TSM, and tombstones together.
2. Recover the full series catalog before discarding any old index.
3. Partition output by VShard and write a complete generation to staging.
4. Verify counts, time bounds, logical hashes, and representative queries.
5. Atomically select the new format generation.
6. Preserve the old generation until a successful restart and grace period.

Crash recovery must choose one complete generation; it must never combine a
partially migrated index with old TSM data.

Two source anomalies need explicit policy. Series present in data but absent
from every index source (possible today: index and data WAL have independent
durability clocks, and deletes remove index entries before compaction removes
data) are exported to a quarantine catalog with synthetic identity, counted,
and reconciled in verification — never silently dropped. A series found in
more than one `shard_N` directory has incomparable local dataSeq values;
migration resolves LWW within one source directory only and fails closed on
cross-directory duplicates unless the operator selects a documented
directory-precedence rule.

## Write path

```text
HTTP write on any node
  -> parse and validate (operation IDs are trace/log correlation only)
  -> group fields by VShard
  -> consult effective placement and leader cache
  -> batch one RPC per destination leader node
  -> leader validates schema/configuration generation
  -> Raft append and durable majority commit
  -> apply catalog + points + postings + indexes
  -> coordinator returns success or explicit per-group failure
```

Multi-VShard writes are not atomic transactions. The response reports which
groups committed so a retry can complete the rest. There is no operation-ID
deduplication machinery: every operation in the data model is idempotent by
construction, so the retry contract is simply "retry the whole batch". Point
writes are LWW-keyed by timestamp (a re-applied batch overwrites itself with
identical values), series creation is get-or-create, range deletes are
idempotent, and retention cutoffs are monotonic. A re-applied batch is an
ordinary new log entry applied identically on every replica, so replicas
cannot diverge. Operation IDs exist only for tracing and log correlation and
carry no durable state. This idempotency is a standing invariant: any future
operation type that is not naturally idempotent must ship its own dedup
design first.

Idempotency here means *replica convergence*, not *retry-invisibility across
intervening operations*. Because a retry re-enters the log at a new, later
position, it is ordered against whatever committed in the gap since the original
proposal — exactly as the same operation issued once at that position would be.
A retried delete therefore removes writes ordered before it and is superseded by
writes ordered after it; a retried write likewise reappears after an intervening
delete (resurrection). Both are valid linearizations: the client never received
the first ack, so its operation is *concurrent* with anything that committed in
the gap, and either order is legal. Consequently a `delete` acknowledgement does
not mean data is destroyed forever while a concurrent write is still in flight,
and the RF gate's "no acknowledged data loss" is failure-induced loss — never a
write superseded by a later-ordered delete. Implemented in the per-VShard data
state machine (`lib/cluster/data/data_state_machine.cpp`), where point revisions
are assigned in log order and delete/write visibility is decided by revision;
identical-range tombstones coalesce so repeated retries cannot grow state without
bound.

The schema path uses a group-0 compare-and-set only when a measurement/field
is first observed or changed. Normal writes validate against cached schema.
Field type conflicts are rejected; lexicographic conflict resolution is not
valid.

Schema creation is ordered before data commitment, or committed data could
exist under a losing type:

- A leader must not propose data for a measurement/field absent from or
  conflicting with its validated schema cache. First-observation writes are
  held until the group-0 CAS commit completes (create wins, or the writer
  adopts the winner). A group-0 quorum outage therefore blocks new-field
  writes, which is consistent with the control-plane outage contract above.
- The coordinator resolves a request's field types once, before fan-out, so
  two destination leaders cannot register conflicting first observations for
  the same request.
- Every data log entry embeds the schema version it was validated against.
  Apply rejects or parks entries whose embedded version has been superseded
  by a conflicting type, and surfaces the rejection; it never coerces.

## Query path and replica reads

### Routing

A coordinator pins a map/configuration generation for the query. Initially,
broad tag queries may target all VShards; each VShard is served by its leader
(the v1 leader-read model). Requests are batched by node so fan-out is per
node, not per series or per replica.

Each storage node performs local discovery, block pruning, reads, and partial
aggregation. Results carry VShard ID, task ID, configuration generation, and
read barrier. The coordinator merges streaming partial states with bounded
memory and backpressure.

A retry replaces the result for the same task and VShard. It never appends a
second contribution. Hedged requests are cut from v1: retries happen on
failure or timeout only, so no two attempts ever race.

### Read consistency: leader reads in v1

v1 has exactly one read path: every VShard read executes at that VShard's
leader behind a ReadIndex barrier. The leader commits a no-op at term start,
captures ReadIndex = commit index only after a quorum round confirms
current-term leadership, and serves only once `applied_index >= ReadIndex`.
All reads — time-series queries, administrative metadata, and deletes — are
therefore linearizable; no consistency mode is configurable and no default
needs choosing.

This is deliberate scope reduction, not a capability ceiling. Read load still
distributes across the cluster because leadership is balanced across nodes;
what leader-only reads give up is spreading one VShard's concurrent read load
across its replicas. Session tokens, bounded staleness, follower reads, and
non-voting read replicas are one deferred post-v1 extension (Phase 6). To
keep that door open cheaply, every query result envelope carries the VShard,
term, and applied index it was served at — the barrier metadata future modes
need — but no token machinery exists in v1.

`applied_index` advances only after NativeIndex/catalog changes are
query-visible, and the serving leader re-validates its barrier at execution
time against its current applied state — routing state is a hint, and a
just-elected or restarted leader waits rather than serving below the barrier.

Lease-based leader reads are excluded until the design adopts an explicit
clock-error bound (open decision 7); ReadIndex requires no clock assumption.

### Errors and partial results

The default is fail-closed: an unavailable or corrupt required VShard makes the
query fail. `allow_partial=true` may return data only when the response names
every missing VShard and reason. Silent omission is forbidden.

### Query scaling

In v1 there is no per-query replica selection: reads go to leaders, so read
distribution is a property of leadership placement, and the leadership
balancer consumes queue depth, CPU load, and per-group read/write rates.
(When replica reads land post-v1, per-query selection re-introduces
eligibility, latency, locality, and error-rate signals.)

Broad queries require admission control, streaming aggregation, result-size
limits, and hierarchical merge when a single coordinator becomes saturated.
Routing summaries such as measurement-to-VShard bitmaps may later prune broad
fan-out. Such summaries must be conservative: false positives are allowed;
false negatives are not.

Splitting one hot series' historical read across replicas at one barrier is a
replica-read technique and is deferred with them (Phase 6).

## Load balancing and scale-out

### Separate balancing objectives

Replica placement balances:

- Logical bytes and projected growth.
- Disk utilisation and free-space headroom.
- Failure domains and capacity weights.
- Repair and snapshot bandwidth.

Leadership balances:

- Write operations and bytes per second.
- WAL/group-commit load.
- CPU and reactor utilisation.
- Network egress to followers.

Read routing in v1 is leadership routing: balancing leaders balances reads.
A separate read-routing loop (replica lag, locality, per-replica queue depth)
arrives only with post-v1 replica reads.

Moving leadership between existing replicas should be preferred over copying
data when only ingest load is uneven.

The balancer uses hysteresis and minimum residency time so minor telemetry
changes do not cause placement churn. Hard constraints and disk watermarks take
priority over load optimisation.

### Performance claim and metric

TimeStar's initial cluster goal is aggregate scaling across many independent
series. For stable RF=3 clusters, define scaling efficiency from a three-node
baseline:

```text
efficiency(N) = (throughput(N) / throughput(3)) / (N / 3)
```

The release target should be chosen from hardware benchmarks; a proposed
initial gate is at least 70% efficiency for ingest and concurrent narrow-query
workloads through the supported node range, without violating p99 latency and
error-rate budgets.

In v1, adding nodes improves read capacity by spreading leaderships; adding
replicas improves durability and failover, not read throughput, until replica
reads land post-v1. Replication increases write work by the replication
factor regardless.

### Hot-series option

A single series maps to one VShard leader and therefore remains leader-limited.
If hot-series ingest scaling is required, add deterministic lanes:

```text
lane = laneFor(series_id, timestamp, configured_lane_count)
placement_key = (series_id, lane)
```

Queries fan out across lanes and merge by timestamp and replicated revision.
Lane-count changes need their own versioned migration protocol. Decision:
lanes are out of the first cluster release entirely (open decision 6,
resolved); benchmarks must not claim hot-series scaling before they exist.

## Adding one machine

For an `N -> N+1` addition at stable RF:

1. The operator starts the node with a join token and failure-domain labels.
2. The control plane validates cluster UUID, wire/storage compatibility,
   identity, capacity, and available disk. The node enters `JOINING`.
3. Weighted placement calculates the desired replica and leader distribution.
4. The controller chooses a bounded move that changes one member of one group.
5. The source group commits addition of the destination as a learner.
6. The source applies through snapshot index `S`, pins the catalog, manifest,
   TSM objects, tombstones, and configuration corresponding to `S`, then streams
   them with resumable chunk hashes.
7. The destination installs and verifies the snapshot, then replays entries
   `S+1...commit_index`.
8. When `applied_index` catches up and health remains stable, the group promotes
   the learner through joint consensus.
9. Query routing may begin using the new follower after it proves eligibility.
10. Leadership may transfer separately if doing so improves write balance.
11. If replacing a replica, the group removes the old voter through a committed
    membership change. The old data remains quarantined for a grace period.
12. The controller repeats until desired placement converges and marks the node
    `ACTIVE`.

At all times:

- A learner does not count toward RF or quorum.
- No two replicas occupy the same forbidden failure domain.
- Only one member of a replica group moves at once.
- A voter is not removed before its replacement is committed.
- Foreground admission and disk headroom outrank rebalance progress.

If the snapshot source fails, the persisted job selects another healthy current
replica and resumes or restarts without weakening effective membership.

## Rebalance resource protection

Rebalance and repair use dedicated Seastar scheduling and I/O classes below
foreground query and write work. Controls include:

- Maximum cluster, source-node, and destination-node concurrent transfers.
- Network and disk byte-rate limits.
- CPU share and compaction concurrency.
- Minimum free-space and high-watermark rejection.
- Snapshot chunk size and outstanding-window limit.
- Automatic pause/resume based on foreground p99, error rate, reactor stalls,
  queue depth, and disk latency.
- Manual pause, cancel-before-cutover, and drain controls.

A cancellation after membership change must complete a safe forward transition;
it cannot simply delete the learner or new voter based on stale job state.

## Failure handling

| Failure | Required behaviour |
| --- | --- |
| Leader crashes | Remaining majority elects an up-to-date leader; acknowledged entries remain committed exactly once |
| Leader isolated 1-versus-2 | Minority stops committing; majority elects and continues; stale tail is truncated on heal |
| Follower crashes | Leader and remaining follower continue; repair/replacement is scheduled without unsafe immediate movement |
| Two RF=3 voters unavailable | Affected VShard stops writes; reads follow configured consistency and explicit partial policy |
| Stale leader resumes | Higher term/configuration rejects all stale proposals and appends |
| Group-0 quorum lost | Existing data groups continue; placement, joins, new schema, and policy changes pause until a meta quorum is restored |
| Disk full or fsync error | Replica stops acknowledging durable appends and becomes unhealthy; quorum decides availability |
| TSM/index/catalog corruption | Quarantine affected objects/replica, retry another replica, compare logical hashes, and repair |
| Map cache stale | Server redirects or rejects using newer epoch; it never serves an invalid group silently |
| Coordinator crashes | Client retries the whole batch; every operation is idempotent so re-application is harmless, and query tasks have no durable side effects |
| Asymmetric partition (leader hears but cannot send, or the reverse) | CheckQuorum steps the leader down; PreVote stops the isolated side from disrupting the healthy majority |
| Slow-but-alive (gray) leader — e.g. multi-second fsyncs, never "failed" | Latency-based leadership eviction distinct from failure detection; a commit-latency SLO breach transfers leadership to a healthy replica |
| Correlated fsync lies (volatile write caches, firmware) | Stated deployment assumption: quorum durability requires independent honest fsync; disable volatile caches; post-power-loss scrub verifies journals against replicas |
| Node clock jump | Retention proposals (and any future lease or staleness decision) use leader-domain or replicated timestamps; a local clock jump alone must not trigger destructive or stale-read behaviour |
| Deposed group-0 leader (stale controller) resumes | Term fencing: data nodes reject job-step RPCs, and group 0 rejects proposals, carrying a superseded controller term |
| Group 0 restored from backup or disaster rebuild | Nodes detect epoch regression, freeze control-plane actuation, and require operator supersession; resurrected schema, retention, or membership state is reconciled against replicated truth before any actuation |

Failure detection is a routing and repair hint, not proof that a node is
permanently gone. Removal from effective membership requires a committed
configuration change. This prevents simultaneous movement and data loss during
temporary network partitions.

## Anti-entropy and corruption

Replicas are logically identical by construction: data, deletes, retention,
and compaction cutoffs all flow through one deterministic log. v1 exploits
that instead of building surgical repair:

- Per-object CRCs and whole-file hashes detect storage and transfer
  corruption during scrub and reads.
- On detection, the replica quarantines the affected VShard and is rebuilt by
  full re-replication through the same learner/snapshot path used for
  movement — Ceph-style backfill, no new machinery.
- The verification hash is a whole-snapshot hash: the ordered
  `(series, timestamp, value bits)` stream of the resolved logical view
  (tombstones and replicated retention applied) at a named snapshot index.
  It is computed at snapshot, restore, and migration boundaries, not
  incrementally online. Revisions are excluded so a value-identical
  re-applied batch cannot perturb it.
- Scrubbing is continuous, rate-limited, observable, and higher priority than
  optional balancing when redundancy is degraded.
- Deferred post-v1: per-time-window Merkle summaries and streaming only
  missing or corrupt logical blocks. They become worthwhile when VShards are
  large enough that full re-replication is operationally painful.

A single-replica read cannot prove that plausible data is truthful. Byzantine
validation through quorum reads is not part of the initial design.

## Deletes, retention, and compaction

- Deletes and retention cutoffs are ordered Raft operations.
- Only the leader proposes scheduled retention for its VShard. A retention
  entry embeds the policy version it used; apply validates it and rejects or
  parks the entry if a newer policy with longer retention is known. Cutoffs
  are monotonic per VShard (a new cutoff is never below the last committed
  one), and a leader proposes one only if it refreshed the policy within a
  bounded freshness window — retention is never urgent enough to act on a
  stale cache.
- Followers do not independently choose a wall-clock cutoff. This bans the
  current single-node behaviour inside cluster storage: compaction-embedded
  TTL drops and downsampling both compute `now()` locally today
  (`tsm_compactor.cpp`), which would make replicas diverge permanently. In
  cluster mode compaction may apply only cutoffs at or below the last
  replicated retention operation, and downsampling is disabled in cluster
  mode v1 (making it a leader-proposed replicated operation is the post-v1
  path).
- Tombstones include the replicated revision/index that created them. A
  tombstone object is GC'd only when subsumed by a replicated cutoff or
  compaction watermark at or below the snapshot index any live verification
  hash is anchored to.
- A snapshot includes all tombstones through its snapshot index.
- Compaction is local and deterministic with respect to logical revisions, but
  physical output files need not be byte-identical.
- Pinned snapshot objects and log retention prevent compaction from removing
  state needed by a learner, subject to the journal-GC copy-forward and
  per-VShard retained-log cap in the journal safety contract: a slow learner
  is bounded, then restarted from a fresh snapshot, rather than pinning
  shared segments indefinitely.

## Streaming subscriptions

Cluster-aware SSE is implemented after replicated writes and queries:

- The accepting node is the subscription coordinator.
- It registers tasks against the current leaders for relevant VShards (read
  replicas: post-v1).
- Events carry VShard, term/index, and operation ID.
- Placement changes re-register from the last delivered commit position
  (VShard term/index).
- Delivery is at least once; the coordinator deduplicates by commit position
  and exposes resumable event IDs.
- Backfill and live transition use one read barrier so events are neither lost
  nor silently duplicated.

Until this exists, cluster mode must reject or clearly mark streaming as
unsupported rather than silently providing node-local subscriptions.

## Networking, security, and compatibility

Inter-node RPC uses a versioned protobuf protocol over a dedicated mTLS port.
It requires:

- Mutual node identity bound to cluster UUID.
- Minimum/maximum wire-version negotiation.
- Deadlines, cancellation, bounded queues, and connection backpressure.
- Batched append, query, heartbeat, and snapshot messages per peer.
- Epoch/term/configuration checks on every state-changing RPC.
- Separate client and inter-node authentication policies.
- Certificate rotation without restarting the entire cluster.

Rolling upgrades allow adjacent compatible versions. A group does not activate
a storage or log format until every current voter can read it and the feature
gate is committed.

## Observability and administration

Required cluster APIs or CLI operations include:

```text
cluster status
node list / join / drain / remove / replace
vshard describe / move / transfer-leader
rebalance status / pause / resume
repair status / scrub
placement explain
query replica decision trace
```

Metrics include:

- Map epoch, group-0 term/commit/applied index, and per-node notification lag.
- Per-group leader, term, commit index, applied index, and configuration.
- Replica lag, unhealthy/degraded/under-replicated groups.
- Leadership, bytes, write rate, query rate, and disk distribution per node.
- Raft proposal latency and group-commit latency.
- Snapshot/repair bytes, progress, retries, and throttling time.
- Leader-read barrier wait and retries (replica-read selection and hedging
  metrics arrive with post-v1 replica reads).
- Partial queries and missing VShard reasons.
- Foreground p50/p95/p99 during rebalance.

Readiness distinguishes client coordination, read eligibility, write
eligibility, joining, draining, and control-plane availability.

## Configuration outline

Names are illustrative and should be finalised with the implementation:

```toml
[cluster]                      # identity/domains wired in Phase 3; RPC in Phase 4
enabled = true
cluster_id = ""
node_id_file = "node.json"
rpc_listen = "0.0.0.0:9087"
failure_domain = "host"
# Fixed at cluster creation; validated, not tunable, in v1 — frozen
# 4,096-entry formats already exist (see open decision 5).
virtual_shards = 4096

[cluster.control]              # group 0 — built in; no external services
# Addresses used to find the meta group when joining; not an identity.
seed_nodes = ["ts-a:9087", "ts-b:9087", "ts-c:9087"]
meta_voters = 3                # odd; auto-placed across failure domains

[cluster.replication]          # wired in Phases 2 and 5
voters = 3
write_durability = "quorum_sync"
# v1 reads are leader reads behind ReadIndex: always linearizable, no mode key.
group_commit_max_delay_ms = 2

[cluster.rebalance]            # wired in Phase 7; needs foreground SLO metrics
enabled = true
max_cluster_transfers = 4
max_transfers_per_node = 1
max_mbps_per_node = 100
min_free_space_percent = 20
pause_on_p99_ms = 500
```

Secrets, join tokens, and private keys must not be stored directly in this TOML
file unless an external secret mechanism protects it.

## Implementation phases

### Phase 0: Baseline correctness

Begin with the independently reviewable
[storage layout foundation](clustering-starting-point.md).

**Status:** the storage-layout epic is complete (layout injection and the
fail-closed startup gate). Four bullets of this phase were NOT delivered by
that epic and are explicitly reassigned so they cannot silently drop:

- Preserve the working `server.data_dir` behaviour while centralizing its path
  rules in an injected immutable layout. *(Done.)*
- Fix or disable the current core-count rebalancer. *(Done — fail-closed
  startup gate.)*
- Define and test durable local acknowledgement modes. *(Reassigned: merge
  main's `wal_sync_mode` group-commit work, then extend it in VShard-workers
  Task 4.0.)*
- Make series metadata reconstructible before discarding any index.
  *(Reassigned: the Task 4a series catalog owns this; until then the startup
  gate prevents discarding.)*
- Make query storage failures explicit. *(Reassigned to Task 4.0 alongside
  the acknowledgement boundary.)*
- Add crash-injection and multi-process test harness foundations.
  *(Reassigned to the test-infrastructure workstream below; the
  since-decommissioned worker/ownership stores pioneered the single-process
  crash-injection technique, which carries forward into Task 4.)*

**Gate:** Power-loss tests cannot lose an acknowledged synchronous write, and a
core-count change cannot make an existing series undiscoverable. The
core-count half of this gate is met; the power-loss half transfers with
Task 4.0 and has not yet been run.

### Phase 1: True VShard storage

Follow the task boundaries and safety gates in
[Stable VShards and Derived Local Execution](clustering-vshard-workers.md).

- Replace local modulo routing with a stored, versioned placement abstraction.
- Decouple on-disk identity from Seastar core assignment: VShard-addressed
  directories, with core execution derived at startup from the live core
  count. No local ownership is persisted.
- Decommission the built worker-registry/ownership-generation machinery (the
  VShard-workers epic records what is removed and what survives), keeping the
  frozen `vshards/` namespace.
- Add VShard-tagged journals, catalog, manifests, and VShard-partitioned TSM,
  following the multiplexed object model above.
- Add point revisions, block CRCs, file hashes, and rebuildable indexes.
- Build the old-format migration tool (epic-sized; tracked as Task 6 of the
  VShard-workers doc, dependent on the Task 4 format).

Three ADRs must land before Task 4 starts — they are on its critical path, not
Phase 2's: journal segmentation/retention (decision 2), physical
TSM/NativeIndex layout (decision 3), and pre-Raft point-revision assignment
with its Raft-index compatibility rule (part of decision 1). The verification
hash consumed by the Task 4 gate and migration is the whole-snapshot stream
hash defined in the anti-entropy section; the deferred Merkle machinery needs
no ADR now.

**Gate:** Reassigning a VShard changes no unrelated data; a core-count change
followed by a restart redistributes execution across all cores with no data
rewrite and identical query results; and journal records written under the
previous core count replay correctly under the new mapping.

### Phase 2: Multi-Raft prototype

- Implement or integrate multiplexed Raft transport and persistence, honouring
  the journal safety contract (hard-state barriers, logical truncation, error
  latching, segment GC copy-forward).
- Add elections, PreVote, CheckQuorum, node-liveness-driven group hibernation,
  log matching, durable commit, snapshots, learners, joint consensus, leader
  transfer, and fault injection.
- Run thousands of lightweight groups without per-group heavyweight services.
- Build the multi-process cluster harness with step-hook crash injection and
  transport partition injection; this gate cannot be evaluated without it.

**Gate:** Model and failure tests prove no acknowledged-entry loss or stale
leader commits across crashes and partitions; killing a node that leads
thousands of groups completes re-election within a bounded time and fsync
count.

### Phase 3: Control plane

Phase 3 now depends hard on the Phase 2 engine — group 0 is one more
multi-Raft group, and there is no external-store fallback to build against
while Raft matures. That is a deliberate trade for eliminating external
service dependence.

- Add node UUIDs, cluster UUID, the `cluster init`/seed-address bootstrap
  ceremony, and group-0-minted join tokens.
- Implement the group-0 state machine on the Phase 2 engine: node records,
  desired placement, policy/schema CAS, persisted jobs, and the term-fenced
  controller on the group-0 leader.
- Implement meta-voter auto-selection across failure domains and voter
  re-selection through joint consensus on join, drain, and failure.
- Implement map-epoch/policy notification streams over inter-node RPC, with
  ReadIndex-barrier reconciliation for nodes that miss events.
- Cache the last valid control map on every node.
- Define group-0 outage behaviour and epoch-regression freezing.

**Gate:** Nodes converge on desired state while existing data Raft groups
continue through control-plane loss; a cluster bootstrapped with
`cluster init` grows its meta voters across failure domains without operator
placement; and a deposed controller's late job-step RPCs are rejected by term
fencing.

### Phase 4: Multi-node data plane

- Add mTLS protobuf RPC, batching, connection pools, deadlines, cancellation,
  and backpressure, including certificate rotation mechanics.
- Route any-node writes to VShard leaders.
- Run remote storage-side query discovery and partial aggregation.
- Reject or clearly mark `/subscribe` as node-local-only in cluster mode; the
  silently-degraded behaviour the streaming section forbids must be guarded
  the moment multi-node traffic exists.
- Extend the insert/query benchmarks to multi-host distributed clients.
- Ship the minimum operator surface: `cluster status`, `vshard describe`,
  `placement explain`, and per-group metrics — these debug Phases 4-7 and
  cannot wait for Phase 8.

**Gate:** Static multi-node RF=1 behaviour matches single-node results and
respects failure/partial-query contracts, and cluster mode does not silently
serve node-local subscriptions.

### Phase 5: Replicated production path

- Enable RF=3 production groups (RF=2 exists only transitionally inside
  membership changes, never as a target).
- Apply catalog, data, indexes, deletes, and retention through Raft.
- Add safe leader reads behind ReadIndex.
- Document the client retry contract: batches are idempotent, so the rule is
  "retry the whole batch on failure or timeout" — no tokens and no
  operation-ID persistence.

**Gate:** RF=3 tolerates one fail-stop or partitioned node without acknowledged
data loss, duplicates, or split brain.

### Phase 6: Replica reads — deferred post-v1

ReadIndex leader reads, pinned query tasks, and explicit partial results ship
in Phases 4-5. This phase is the deferred read-scaling extension and is out
of the first release:

- Session tokens, bounded staleness, follower reads, non-voting read
  replicas, per-query replica selection, hedging, and the read-routing
  balancing loop.

**Gate (when implemented):** Every replica-read adversarial test passes under
lag, retries, placement changes, and mid-query failures.

### Phase 7: Online join, move, repair, and balance

Three sub-epics, each needing its own plan doc and gate; the combined gate
below closes the phase:

- **7a Movement:** pinned resumable snapshots, catch-up, joint membership
  transitions, drain/replace, SLO-aware throttling.
- **7b Repair:** continuous scrub, corruption quarantine, and full-VShard
  re-replication through the movement path, with the whole-snapshot
  verification hash at snapshot/restore boundaries. (Surgical Merkle repair:
  post-v1.)
- **7c Balancing:** capacity and leadership balancing loops, consuming the
  telemetry (queue depth, per-group read/write rates) that Phases 4-5 must
  already export. Leadership balancing is also v1's read balancing.

**Gate:** A loaded RF=3 cluster grows from N to N+1 without falling below RF,
losing acknowledged data, double-counting query results, or breaching configured
foreground SLOs without automatically pausing movement.

### Phase 8: Feature completion

Named deliverables, several epic-sized — not a checkbox list:

- Cluster-aware streaming per the subscriptions section, with its own gate:
  backfill/live transition at one barrier, no loss or silent duplication.
- Backup/restore design and tooling: export is a pinned per-VShard snapshot
  stream (reusing Task 4 snapshot and Phase 7 movement machinery); restore
  bootstraps a new cluster UUID, imports snapshots as generation-one state,
  and scrubs old membership. Task 4's snapshot export format is the backup
  unit.
- Rolling upgrades and feature gating (decision 8).
- Remaining operator APIs and CLI beyond the Phase 4 minimum surface.
- Routing summaries (conservative measurement-to-VShard pruning) and
  hierarchical query merge.
- Update the official client bindings for the idempotent-retry contract.
- Replica reads (the deferred Phase 6), if per-VShard read-scaling demand
  materialises.
- Hot-series lanes: resolved out of the first release (decision 6); revisit
  only with demonstrated demand.

## Test infrastructure workstream

The gates and acceptance tests below repeatedly assume tooling that must be
built deliberately, not discovered missing at gate time:

- **Deterministic environment abstraction** (swappable transport, clock, and
  disk with seed-controlled scheduling): decided as part of the decision-1
  ADR, because it constrains the shape of the Phase 2 Raft code itself.
- **Multi-process cluster harness** with step-hook crash injection and
  network partition injection: built in Phase 2, required by its gate.
- **Fault-injecting file layer** under Seastar's O_DIRECT API (injected
  fsync/EIO/disk-full/torn-write): built in Phase 2, consumed by the
  durability acceptance tests.
- **Distributed load clients:** multi-host extension of the existing insert
  and query benches, built in Phase 4, required by the performance gates.
  The 70% scaling-efficiency release target is finalized from hardware
  benchmarks before Phase 7 exit.
- **"Model tests" definition:** whether Phase 2's proof obligation is a TLA+
  specification, deterministic-simulation exploration, or both is part of the
  decision-1 ADR; the phrase must not remain undefined.

## Falsifying acceptance tests

The following tests are release gates. They are intended to disprove the
architecture's claims rather than demonstrate only happy paths.

### Incremental growth

- Run continuous writes, deletes, retention, and queries during 1->2, 2->3,
  and 3->4. Crash at every snapshot, catch-up, promotion, and removal step.
- During 3->4, kill the snapshot source. Resume from another healthy replica
  without dropping below three committed voters.
- Verify that learners never count toward RF and all voters occupy distinct
  required failure domains.
- Verify movement is proportional to the new node's target share rather than a
  full-cluster rewrite.

### Consensus, HA, and durability

- Kill the leader immediately before and after every quorum acknowledgement.
  All acknowledged operations must survive exactly once.
- Partition the old leader one-versus-two. Minority writes stop within the
  election/fencing bound; the majority continues and heals by truncating the
  stale branch.
- Pause a leader until replacement, then resume it with client connectivity.
  Every stale-term proposal must be rejected.
- Inject fsync failure, disk-full, torn WAL, corrupt manifest, corrupt TSM,
  corrupt catalog, and corrupt NativeIndex on one node. Reads use a healthy
  replica, repair, or fail explicitly; successful omission is a failure.
- Stop enough meta voters to lose the group-0 quorum while data quorums remain
  healthy; verify the documented steady-state availability contract, then
  restore the quorum and verify frozen control actions resume.
- Restore group 0 from an old backup under load; nodes must freeze
  control-plane actuation on epoch regression, and no resurrected schema,
  retention, or membership state may be actuated without the operator
  supersession ceremony.
- Kill the group-0 leader mid-rebalance; the next leader resumes the persisted
  job under a higher term, and the deposed controller's late job-step RPCs are
  rejected by data nodes and by group 0.
- Drain a meta-voter node; group 0 re-selects a replacement voter in a distinct
  failure domain through joint consensus without losing quorum.
- Kill a node leading thousands of groups; re-election completes within the
  budgeted time and fsync count.
- With one slow learner on VShard A, journal GC for co-located VShard B keeps
  advancing past the configured retained-log cap.

### Leader reads

- Commit a write, kill the leader, and force a read through the newly elected
  leader before it commits its term no-op. The read waits for the no-op and
  ReadIndex barrier and never returns the old value.
- Partition a leader from quorum and issue reads through it; ReadIndex cannot
  confirm leadership, so it must reject rather than serve possibly-stale
  data.
- Change placement between discovery and execution. Each VShard contributes
  exactly once from a pinned configuration.
- Fail the serving leader mid-query and retry at the same or newer barrier
  without combining partial output from two attempts.
- Delay NativeIndex/catalog apply on the leader and prove it does not serve a
  barrier the apply has not reached.
- Prove batch-retry idempotency: re-send committed batches (writes, deletes,
  series creation) after every failure mode and verify point counts, values,
  and aggregation results are unchanged.

### Rebalancing

- Snapshot while compaction, retention, range delete, same-timestamp overwrite,
  WAL rollover, and new series creation run concurrently. Destination logical
  hashes match a healthy source at cutover index.
- Crash the controller after every persisted job transition and resume safely.
- Saturate foreground traffic. Rebalance automatically pauses when configured
  p99, error-rate, disk, or reactor-stall budgets are exceeded.
- Repeatedly cancel and resume jobs before and after membership transitions.

### Performance

- Benchmark stable RF=3 clusters with 3, 4, 5, and 6 nodes using distributed
  clients and enough independent series. Measure throughput scaling efficiency,
  p50/p95/p99, coordinator CPU, network bytes, disk balance, and replica lag.
- Run one extremely hot series separately. Do not claim hot-series ingest
  scaling unless deterministic lanes have been implemented and pass.
- Run a broad high-cardinality query and many concurrent narrow queries. A
  saturated coordinator with idle storage nodes is a failed query-scale result.
- Verify read traffic distributes with leadership placement and is never
  served by a joining, quarantined, or barrier-lagging node.

### Feasibility and operations

- Start a node hosting thousands of group state machines and enforce budgets
  for startup time, file descriptors, timers, cache memory, heartbeat traffic,
  and Seastar reactor stalls.
- Change Seastar core count and verify no data rewrite and identical queries.
- Roll compatible versions through the cluster while writing, querying, and
  rebalancing.
- Restore a backup into a new cluster UUID and verify replicas are rebuilt
  rather than confusing old membership with the restored cluster.

## Open implementation decisions

Decisions 2, 3, 5, and the journal-record/point-revision framing of decision 1
are on the Phase 1 Task 4 critical path and required ADRs before Task 4 starts.
Those ADRs have landed (see [docs/adr/](adr/)); the remainder require prototypes
or ADRs before Phase 2:

1. Multi-Raft implementation/library and its integration with Seastar ownership,
   DMA I/O, scheduling, and group commit. (The point-revision framing of this
   decision is **resolved** by [ADR 0003](adr/0003-point-revision-assignment.md):
   revision = per-VShard log position, journal sequence pre-Raft continuing into
   the Raft index. The Multi-Raft library choice remains a Phase 2 prototype.)
2. **Resolved** — [ADR 0001](adr/0001-journal-segmentation-and-retention.md):
   per-core multiplexed journals, 64 MiB segments, watermark-driven GC with
   laggard copy-forward and a per-VShard byte cap.
3. **Resolved** — [ADR 0002](adr/0002-vshard-physical-layout.md): VShard-pure
   TSM at tier ≥ 1 over multiplexed tier-0 extents, one NativeIndex per core with
   VShard-prefixed keys, object UUIDs/hashes/block CRCs.
4. Resolved: v1 reads are leader reads behind ReadIndex — always
   linearizable, no modes, no commit tokens. Token representation returns as
   a design question only with post-v1 replica reads (Phase 6).
5. Maximum supported node count for 4,096 fixed VShards and a future-compatible
   identity for splitting placement groups. The `vshards/0000`–`4095` namespace
   is frozen (the ownership-binary format was decommissioned with the
   local-worker machinery). The Task-4-critical part — VShard identity before
   Task 4 bakes IDs into every data file — is **resolved** by
   [ADR 0002 §6](adr/0002-vshard-physical-layout.md): accept the risk that
   splitting requires a versioned migration, made safe by stamping
   `virtual_shard_count` + `format_version` on every immutable object (a split is
   then a detectable format change, never a silent reinterpretation), with the
   top 4 bits of the 16-bit on-disk VShard field reserved-zero as an escape
   hatch. Maximum node count for placement remains a Phase 3 control-plane
   question.
6. Resolved: hot-series lanes are not in the first cluster release.
7. Whether a later closed-timestamp/HLC mechanism should provide a cluster-wide
   query timestamp.
8. Wire protocol and storage feature-gating rules for rolling upgrades.
9. Client-facing authentication in cluster mode: the current HTTP API has no
   authentication, while the networking section promises "separate client and
   inter-node authentication policies". Decide whether client auth is in
   scope for the first cluster release (and which phase owns it) or
   explicitly out of scope.
10. Group-0 specifics: meta-voter count default (3 vs 5) and auto-selection
    policy, snapshot cadence and size budget for the control state machine,
    and whether group 0 shares a voter node's data-plane journal stream or
    uses a dedicated journal. Prototype alongside decision 1 — group 0 is the
    first consumer of the multi-Raft engine.

## Design references

- [Ceph CRUSH maps](https://docs.ceph.com/en/latest/rados/operations/crush-map/):
  topology-aware placement and failure domains.
- [Ceph placement groups](https://docs.ceph.com/en/latest/rados/operations/placement-groups/):
  logical placement units between objects and storage daemons.
- [Ceph monitor architecture](https://docs.ceph.com/en/latest/rados/configuration/mon-config-ref/):
  consensus-controlled maps and map epochs.
- [Apache Cassandra Dynamo architecture](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html):
  virtual nodes, incremental node additions, and rack-aware replication.
- [CockroachDB replication layer](https://www.cockroachlabs.com/docs/stable/architecture/replication-layer):
  Raft replicas, learners, snapshots, catch-up, and leaseholder reads.
- [VictoriaMetrics cluster architecture](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/):
  simple storage fan-out and the operational consequences of not automatically
  redistributing historical data.
