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
- Each virtual shard is a multiplexed Raft group with one leader, voting
  replicas, and optional non-voting read replicas.
- Production uses three voting replicas and a majority of two. A write is
  acknowledged only after it is committed by Raft and durably group-committed
  on a majority.
- An external etcd cluster stores desired placement, membership intent,
  policies, and rebalance jobs. It does not elect data leaders and is not in
  the steady-state read/write path.
- Current/effective Raft membership is distinct from desired placement. A map
  change never makes an unsynchronised replica serve traffic.
- Nodes join as learners. Data moves one virtual shard replica at a time using
  a pinned snapshot, checksum verification, log catch-up, joint membership
  change, and a grace period before source deletion.
- Query coordinators select exactly one sufficiently current replica per
  virtual shard. Replicas distribute concurrent read load without multiplying
  the amount of query work.
- Placement balances capacity; leader placement balances ingest; read routing
  balances query work. These are related but separate control loops.

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
- Concurrent read traffic can be spread across leaders, followers, and
  optional non-voting read replicas subject to an explicit read barrier.
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
| 2 | 2 | 2 | No | Redundancy and distributed reads; either loss stops writes |
| 3 | 3 | 2 | Yes | First HA topology; each node holds nearly all logical data |
| 4+ | 3 | 2 | Yes | Usable storage and aggregate throughput grow with each node |

Growing from one to three nodes consumes the new capacity by increasing the
replication factor. Meaningful stable-replication storage scaling begins at
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
2. `Engine::basePath()`, WAL, TSM, NativeIndex, and compaction paths remain
   rooted in `shard_N` CPU-core directories. `server.data_dir` is now honoured,
   but through global path helpers rather than an immutable injected layout.
3. A core-owned TSM file may contain series from many virtual shards. It cannot
   be transferred as one placement group without filtering and rewriting.
4. The current core-count rebalancer deliberately creates an empty NativeIndex,
   while TSM stores only `SeriesId128` rather than the measurement, tags, and
   field required to reconstruct discovery metadata.
5. WAL immediate flush defaults to disabled. A successful local write therefore
   does not currently imply the durable-majority contract required here.
6. Metadata, day postings, data, and deletes are not one atomic replicated
   state machine. Replication must not preserve this ordering ambiguity.
7. Some query paths can convert unexpected per-series failures into omitted
   series. Cluster queries must distinguish success, explicit partial success,
   and failure.
8. `lib/cluster/scatter_gather.hpp` and storage-side partial aggregation are
   useful boundaries to retain and extend across machines.

The current `ShardRebalancer` should be fixed or disabled before cluster work.
Once storage identity is decoupled from Seastar cores, changing `smp::count`
will reassign local execution only and will not rewrite data.

## Logical architecture

```text
                           desired topology and policy
                     +----------------------------------+
                     | etcd quorum                      |
                     | cluster UUID, nodes, desired map |
                     | schema, retention, jobs          |
                     +----------------+-----------------+
                                      | watch
             +------------------------+------------------------+
             |                        |                        |
      +------v-------+         +------v-------+         +------v-------+
      | TimeStar A   |         | TimeStar B   |         | TimeStar C   |
      | coordinator  |<------->| coordinator  |<------->| coordinator  |
      | storage      |  Raft   | storage      |  Raft   | storage      |
      +------+-------+         +------+-------+         +------+-------+
             |                        |                        |
             +---------- VShard 27: leader + followers -------+

     Client -> any coordinator -> current placement cache
            -> one leader per written VShard
            -> one safe replica per queried VShard
```

Every TimeStar node can accept client HTTP requests and act as coordinator. A
separate stateless coordinator process can be introduced later if coordinator
CPU or deployment isolation requires it; the protocol must not assume one.

## Terminology

- **Virtual shard (VShard):** Stable hash bucket and placement unit.
- **Storage worker:** Persisted node-local execution target hosted by one
  Seastar reactor core. It is analogous to a Ceph OSD for placement and load
  accounting, but not an independent replica failure domain.
- **Replica group:** Raft state machine responsible for one VShard.
- **Leader:** Voting replica that orders writes and supplies read barriers.
- **Follower:** Voting replica that follows the committed log.
- **Learner:** Non-voting replica being bootstrapped; never counts toward the
  configured replication factor or quorum.
- **Read replica:** Optional non-voting, fully caught-up replica used to expand
  read capacity without increasing write quorum size.
- **Desired placement:** Controller's target set of nodes for a VShard.
- **Effective membership:** Replica set committed by that VShard's Raft group.
- **Map epoch:** Version of cluster-wide topology and policy.
- **Term/index:** Raft identity and ordered log position within one VShard.
- **Applied index:** Highest committed entry whose data and all query-visible
  indexes have been applied locally.

### CPU shards as OSD-like storage workers

Seastar reactor shards are fixed for the lifetime of a process, so reactor
cores cannot be hot-added. They can still be modelled as persisted storage
workers across a node restart:

- Each worker has a stable node-local ID and owns many VShards. A worker ID is
  mapped to a reactor core for the current process; reactor-core number is not
  stored as the data identity.
- Adding CPU capacity creates worker IDs and uses weighted rendezvous placement
  to assign a proportional subset of the node's VShards to them. Removing a
  worker drains its VShards before the worker record is retired.
- The effective owner remains authoritative until a fenced local handoff has
  quiesced writes, committed the new local ownership generation, and opened
  the VShard on the destination worker. A request is never served by both
  workers for the same ownership generation.
- VShard files remain addressed by VShard identity. Reassigning execution on a
  shared node-local volume should normally move ownership and in-memory state,
  not rewrite TSM data. A deployment with worker-specific devices may copy the
  VShard using the same snapshot protocol as cross-node movement.
- CPU workers on one machine share a host, power, kernel, and process. They may
  balance CPU and I/O load, but two such workers can never satisfy two replica
  slots or increase the advertised failure-domain count.

Placement is explicitly two-stage: first select distinct eligible hosts for a
VShard's replicas, then select one storage worker within each chosen host.
Worker weights can affect only the second stage and cannot weaken the
host/rack-level replica constraint.

Changing `--smp` therefore requires restarting that TimeStar process. In an
RF=3 cluster, other replicas preserve service while the node restarts, rejoins,
and performs local handoffs. Single-node mode has a restart outage but retains
the same automatic ownership rebalance after startup. When CPU count shrinks,
persisted workers that no longer have a dedicated reactor are temporarily
co-scheduled on the surviving reactors, fenced and drained one at a time, and
retired only after their VShards have new effective owners. A missing reactor
must never make its old worker's data unreachable before the drain can run.

## Cluster and placement maps

### Node identity

Each node has a persistent UUID stored under its data directory. Network
address, hostname, and a small integer are not identities. Reinstalling a node
without its identity is a replacement, not a restart.

A node record contains:

- Node UUID and cluster UUID.
- Client and inter-node endpoints.
- Software and wire-protocol versions.
- Capacity weight and storage class.
- Region, zone, rack, host, and other failure-domain labels.
- Lifecycle state: `JOINING`, `ACTIVE`, `DRAINING`, `REMOVED`.
- Liveness and health summary.

### Desired placement

Weighted rendezvous hashing produces a deterministic initial target set for
each VShard, subject to:

- Distinct-node and configured failure-domain constraints.
- Node lifecycle and health.
- Capacity weights and high/low disk watermarks.
- Storage class requirements.
- Maximum simultaneous movements.

Rendezvous output is only a desired state. A controller compares desired state
to effective Raft membership and schedules safe transitions. Nodes route using
effective membership and leader information, never an uncommitted target set.

The controller persists jobs and uses a fenced singleton lease. Job steps are
idempotent so another controller can resume after a crash.

### etcd responsibility

Recommended key families are:

```text
/timestar/<cluster>/meta
/timestar/<cluster>/nodes/<node-uuid>
/timestar/<cluster>/desired-placement/<vshard>
/timestar/<cluster>/jobs/<job-uuid>
/timestar/<cluster>/schema/<measurement>/<field>
/timestar/<cluster>/retention/<measurement>
/timestar/<cluster>/controller
```

Production uses an odd etcd quorum, normally three members. A one-node TimeStar
development deployment may use a single local member but must report that the
control plane is not highly available.

An etcd outage freezes new joins, desired-placement changes, schema creation,
and policy edits. Existing replica groups continue serving reads and writes
from their committed configurations. No per-write committed index is written
to etcd.

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

Creating 4,096 independent WAL managers, NativeIndex instances, compaction
loops, caches, and timers is not acceptable.

### Log record

The replicated log is authoritative. Each entry includes at least:

```text
format version
cluster UUID
vshard ID
Raft term and index
configuration generation
operation UUID / idempotency key
operation kind
series ID and series-catalog definition when first seen
schema and retention versions where relevant
point values, timestamps, and deterministic point revision
delete range or retention cutoff
record checksum
```

Series catalog creation, data mutation, day postings, deletes, retention, and
idempotency state are applied in this order as one logical state transition.
`applied_index` advances only after all query-visible effects are complete.
NativeIndex is derived state and must be rebuildable from durable replicated
state.

### Durability

For three voters, a write commits after a majority of two stores the entry in
their durable journals. The implementation may group-commit multiple requests
within a bounded interval, but HTTP success waits for the group-commit barrier.
An explicit weaker `async` mode may exist, but its potential loss window must be
reported in configuration, responses, and metrics.

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

A failed controller cannot infer completion from etcd alone. It queries the
group's committed membership and resumes the next idempotent step.

## Movable storage model

### Stable identity and layout

Storage identity is VShard-based, not core-based. A possible physical layout is:

```text
<data_dir>/
  node.json
  workers.json
  cluster-map-cache/
  journals/worker_<id>/...
  vshards/<id>/
    raft.meta
    MANIFEST
    catalog/
    tsm/
    tombstones/
    snapshots/
```

The exact number of files may be reduced through multiplexed manifests and
directories. The logical boundaries and snapshot/export APIs must remain per
VShard.

### TSM and catalog requirements

- TSM output must be VShard-partitioned, or its manifest must permit efficient
  extraction of one VShard without scanning and rewriting unrelated data.
- Every immutable object has a stable object UUID, format version, byte length,
  and whole-file hash.
- TSM blocks gain CRCs so corruption is attributable and repairable.
- A durable series catalog maps `SeriesId128` to measurement, ordered tags,
  field, and value type.
- Duplicate timestamp resolution uses replicated point revisions, not local
  TSM tier or sequence numbering.
- Compaction may run independently on replicas but must preserve logical
  revisions and expose logical time-block hashes for anti-entropy.
- A snapshot pins its referenced manifest objects until all consumers release
  them.

### Migration from `shard_N`

The on-disk migration tool must:

1. Read existing core-owned WAL, NativeIndex, TSM, and tombstones together.
2. Recover the full series catalog before discarding any old index.
3. Partition output by VShard and write a complete generation to staging.
4. Verify counts, time bounds, logical hashes, and representative queries.
5. Atomically select the new format generation.
6. Preserve the old generation until a successful restart and grace period.

Crash recovery must choose one complete generation; it must never combine a
partially migrated index with old TSM data.

## Write path

```text
HTTP write on any node
  -> parse, validate, and create operation IDs
  -> group fields by VShard
  -> consult effective placement and leader cache
  -> batch one RPC per destination leader node
  -> leader validates schema/configuration generation
  -> Raft append and durable majority commit
  -> apply catalog + points + postings + indexes
  -> return per-VShard commit tokens
  -> coordinator returns success or explicit per-group failure
```

Multi-VShard writes are not atomic transactions. The response reports which
groups committed so an idempotent retry can complete the rest. Operation IDs
and deduplication state persist for a documented retry window and survive
snapshot, restart, and movement.

The schema path uses an etcd compare-and-set only when a measurement/field is
first observed or changed. Normal writes validate against cached schema. Field
type conflicts are rejected; lexicographic conflict resolution is not valid.

## Query path and replica reads

### Routing

A coordinator pins a map/configuration generation for the query. Initially,
broad tag queries may target all VShards, but each VShard is assigned to exactly
one selected in-sync replica. Requests are batched by node so fan-out is per
node, not per series or per replica.

Each storage node performs local discovery, block pruning, reads, and partial
aggregation. Results carry VShard ID, task ID, configuration generation, and
read barrier. The coordinator merges streaming partial states with bounded
memory and backpressure.

A retry replaces the result for the same task and VShard. It never appends a
second contribution. Hedged requests may race, but only the first valid response
is accepted and the other is cancelled or discarded.

### Read consistency modes

| Mode | Protocol | Guarantee |
| --- | --- | --- |
| `linearizable` | Obtain Raft ReadIndex/quorum barrier; chosen replica waits until it applied the exact configuration/term/index | Includes all writes committed before the barrier |
| `session` | Client supplies per-VShard commit tokens returned by writes | Read-your-writes for supplied groups |
| `bounded_stale` | Choose replica within configured time/index lag | Explicitly stale, higher availability/lower latency |

The default for administrative metadata and deletes is linearizable. The
default for time-series queries must be chosen before implementation and exposed
in query statistics.

A node advertises an `applied_index` only after NativeIndex/catalog changes are
query-visible. A lagging, joining, quarantined, or snapshot-installing replica
is ineligible unless the selected bounded-staleness contract permits it.

### Errors and partial results

The default is fail-closed: an unavailable or corrupt required VShard makes the
query fail. `allow_partial=true` may return data only when the response names
every missing VShard and reason. Silent omission is forbidden.

### Query scaling

Replica selection uses:

- Eligibility at the requested barrier.
- Queue depth and CPU load.
- EWMA service and network latency.
- Locality to the coordinator.
- Current read traffic.
- Health and recent error rate.

Broad queries require admission control, streaming aggregation, result-size
limits, and hierarchical merge when a single coordinator becomes saturated.
Routing summaries such as measurement-to-VShard bitmaps may later prune broad
fan-out. Such summaries must be conservative: false positives are allowed;
false negatives are not.

For a hot historical series, replicas may read disjoint time ranges at one
barrier and the coordinator may merge them. This improves read latency without
weakening consistency.

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

Read routing balances:

- Query operations and bytes scanned.
- Queue depth and tail latency.
- Replica lag and locality.

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

Adding replicas improves concurrent read capacity. It does not guarantee lower
latency for a single broad query, and it increases write work by the replication
factor.

### Hot-series option

A single series maps to one VShard leader and therefore remains leader-limited.
If hot-series ingest scaling is required, add deterministic lanes:

```text
lane = laneFor(series_id, timestamp, configured_lane_count)
placement_key = (series_id, lane)
```

Queries fan out across lanes and merge by timestamp and replicated revision.
Lane-count changes need their own versioned migration protocol. This feature is
deferred until aggregate multi-series clustering is correct; benchmarks must
not claim hot-series scaling before it exists.

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
| etcd unavailable | Existing groups continue; placement, joins, new schema, and policy changes pause |
| Disk full or fsync error | Replica stops acknowledging durable appends and becomes unhealthy; quorum decides availability |
| TSM/index/catalog corruption | Quarantine affected objects/replica, retry another replica, compare logical hashes, and repair |
| Map cache stale | Server redirects or rejects using newer epoch; it never serves an invalid group silently |
| Coordinator crashes | Client retries with operation IDs; committed writes deduplicate and query tasks have no durable side effects |

Failure detection is a routing and repair hint, not proof that a node is
permanently gone. Removal from effective membership requires a committed
configuration change. This prevents simultaneous movement and data loss during
temporary network partitions.

## Anti-entropy and corruption

Physical manifests may differ after independent compaction, so repair compares
logical content:

- Per-object hashes detect transfer and storage corruption.
- Per-series/time-block logical hashes include timestamps, values, revisions,
  and tombstones.
- Merkle summaries identify divergent ranges without full transfer.
- A majority or committed log establishes the authoritative revision history.
- Repair streams only missing/corrupt logical blocks where possible.
- Scrubbing is continuous, rate-limited, observable, and higher priority than
  optional balancing when redundancy is degraded.

A single-replica read cannot prove that plausible data is truthful. Byzantine
validation through quorum reads is not part of the initial design.

## Deletes, retention, and compaction

- Deletes and retention cutoffs are ordered Raft operations.
- Only the leader proposes scheduled retention for its VShard.
- Followers do not independently choose a wall-clock cutoff.
- Tombstones include the replicated revision/index that created them.
- A snapshot includes all tombstones through its snapshot index.
- Compaction is local and deterministic with respect to logical revisions, but
  physical output files need not be byte-identical.
- Pinned snapshot objects and log retention prevent compaction from removing
  state needed by a learner.

## Streaming subscriptions

Cluster-aware SSE is implemented after replicated writes and queries:

- The accepting node is the subscription coordinator.
- It registers tasks against the current leaders or eligible read replicas for
  relevant VShards.
- Events carry VShard, term/index, and operation ID.
- Placement changes re-register from the last delivered commit token.
- Delivery is at least once; the coordinator deduplicates by operation ID and
  exposes resumable event IDs.
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

- Map epoch and control-plane watch revision.
- Per-group leader, term, commit index, applied index, and configuration.
- Replica lag, unhealthy/degraded/under-replicated groups.
- Leadership, bytes, write rate, query rate, and disk distribution per node.
- Raft proposal latency and group-commit latency.
- Snapshot/repair bytes, progress, retries, and throttling time.
- Replica read selection, hedges, retries, and barrier wait.
- Partial queries and missing VShard reasons.
- Foreground p50/p95/p99 during rebalance.

Readiness distinguishes client coordination, read eligibility, write
eligibility, joining, draining, and control-plane availability.

## Configuration outline

Names are illustrative and should be finalised with the implementation:

```toml
[cluster]
enabled = true
cluster_id = ""
node_id_file = "node.json"
rpc_listen = "0.0.0.0:9087"
failure_domain = "host"
virtual_shards = 4096

[cluster.control]
etcd_endpoints = ["https://etcd-a:2379", "https://etcd-b:2379", "https://etcd-c:2379"]

[cluster.replication]
voters = 3
write_durability = "quorum_sync"
default_read_consistency = "linearizable"
group_commit_max_delay_ms = 2

[cluster.rebalance]
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

- Preserve the working `server.data_dir` behaviour while centralizing its path
  rules in an injected immutable layout.
- Define and test durable local acknowledgement modes.
- Fix or disable the current core-count rebalancer.
- Make series metadata reconstructible before discarding any index.
- Make query storage failures explicit.
- Add crash-injection and multi-process test harness foundations.

**Gate:** Power-loss tests cannot lose an acknowledged synchronous write, and a
core-count change cannot make an existing series undiscoverable.

### Phase 1: True VShard storage

Follow the task boundaries and safety gates in
[Stable VShards and Local Storage Workers](clustering-vshard-workers.md).

- Replace local modulo routing with a stored, versioned placement abstraction.
- Decouple on-disk identity from Seastar core assignment.
- Persist OSD-like storage-worker identities and map them to reactor cores at
  startup.
- Rebalance local VShard ownership proportionally when workers are added or
  drained, using a crash-safe fenced ownership generation.
- Add VShard-tagged journals, catalog, manifests, and VShard-partitioned TSM.
- Add point revisions, block CRCs, file hashes, and rebuildable indexes.
- Build the old-format migration tool.

**Gate:** Reassigning a VShard changes no unrelated data; increasing local core
count balances work onto the new workers; changing local core count on shared
storage requires no TSM rewrite; and no VShard is served by two local workers
in one ownership generation.

### Phase 2: Multi-Raft prototype

- Implement or integrate multiplexed Raft transport and persistence.
- Add elections, log matching, durable commit, snapshots, learners, joint
  consensus, leader transfer, and fault injection.
- Run thousands of lightweight groups without per-group heavyweight services.

**Gate:** Model and failure tests prove no acknowledged-entry loss or stale
leader commits across crashes and partitions.

### Phase 3: Control plane

- Add node UUIDs, cluster UUID, join tokens, etcd watches, desired placement,
  policy/schema CAS, fenced controller, and persisted jobs.
- Cache the last valid control map on every node.
- Define etcd-outage behaviour.

**Gate:** Nodes converge on desired state, while existing Raft groups continue
through control-plane loss.

### Phase 4: Multi-node data plane

- Add mTLS protobuf RPC, batching, connection pools, deadlines, cancellation,
  and backpressure.
- Route any-node writes to VShard leaders.
- Run remote storage-side query discovery and partial aggregation.

**Gate:** Static multi-node RF=1 behaviour matches single-node results and
respects failure/partial-query contracts.

### Phase 5: Replicated production path

- Enable RF=2 bootstrap and RF=3 production groups.
- Apply catalog, data, indexes, deletes, and idempotency through Raft.
- Add commit tokens and safe leader reads.

**Gate:** RF=3 tolerates one fail-stop or partitioned node without acknowledged
data loss, duplicates, or split brain.

### Phase 6: Safe follower reads

- Implement ReadIndex barriers, session tokens, bounded staleness, replica
  eligibility, safe hedging, pinned query tasks, and explicit partial results.

**Gate:** Every replica-read adversarial test passes under lag, retries,
placement changes, and mid-query failures.

### Phase 7: Online join, move, repair, and balance

- Add pinned resumable snapshots, catch-up, joint membership transitions,
  drain/replace, logical anti-entropy, scrubbing, and SLO-aware throttling.
- Add capacity, leadership, and replica-read balancing loops.

**Gate:** A loaded RF=3 cluster grows from N to N+1 without falling below RF,
losing acknowledged data, double-counting query results, or breaching configured
foreground SLOs without automatically pausing movement.

### Phase 8: Feature completion

- Cluster-aware streaming, backups, rolling upgrades, operator APIs, routing
  summaries, hierarchical query merge, and optional hot-series lanes.

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
- Stop etcd while data quorums remain healthy and verify the documented
  steady-state availability contract.

### Replica reads

- Commit a write and force a linearizable read through each lagging follower.
  It waits or retries and never returns the old value.
- Change placement between discovery and execution. Each VShard contributes
  exactly once from a pinned configuration.
- Deliver both hedged responses and prove only one result reaches aggregation.
- Fail the chosen replica mid-query and retry at the same or newer barrier
  without combining partial output from two attempts.
- Delay NativeIndex/catalog apply and prove the follower cannot advertise the
  corresponding applied index.

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
- Verify hot read traffic is spread across eligible replicas and never sent to
  a joining, corrupt, or barrier-lagging replica.

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

These require prototypes or ADRs before Phase 2:

1. Multi-Raft implementation/library and its integration with Seastar ownership,
   DMA I/O, scheduling, and group commit.
2. Journal segmentation and retention strategy for many multiplexed groups.
3. Exact VShard-partitioned TSM and NativeIndex physical layout.
4. Default query consistency mode and commit-token representation.
5. Maximum supported node count for 4,096 fixed VShards and a future-compatible
   identity for splitting placement groups.
6. Whether hot-series lanes are required for the first cluster release.
7. Whether a later closed-timestamp/HLC mechanism should provide a cluster-wide
   query timestamp.
8. Wire protocol and storage feature-gating rules for rolling upgrades.

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
