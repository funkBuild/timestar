# Clustering Integration Plan: Wiring the Bricks into the Production Server

**Status:** In progress — production deployment blocked

**Current release authority:**
[Cluster production-readiness review and fix-up plan](cluster-production-readiness.md).
That review was performed against `cluster-design` at `f78e05d` on 2026-08-01
and supersedes this document's historical completion/ground-truth statements.
This file remains the milestone roadmap and target wiring sequence; it is not a
claim that the milestones described below are complete in the running server.

**Scope:** The milestone sequence that turns the gate-proven `lib/cluster/`
bricks (Phases 0-8) into the running `timestar_http_server`, from the M1
full-replication gateway to a Raft-replicated, online-rebalancing,
feature-complete cluster. Parent design: [clustering.md](clustering.md).
Historical deferred backlog:
[clustering-deferred-tasks.md](clustering-deferred-tasks.md). Current correctness
and production-release work is tracked in the production-readiness review.

This is a plan, not a design revision: no decision in `docs/clustering.md` is
reopened here. Where a milestone must re-open a previously proven gate, that is
stated explicitly.

## Historical starting point

The statements in this section describe the starting point when this roadmap
was written. Several foundation, M2, and RF>1 data-path pieces have since landed,
while group-0 composition, complete snapshot state, topology movement, true
ReadIndex leader reads, and several cluster-aware HTTP features remain open.
Use the production-readiness review for current status.

- All clustering phases 0-8 exist as reviewed library bricks with injected
  abstractions and green gates: multi-Raft engine + journals
  (`lib/cluster/raft/`), group-0 control plane (`lib/cluster/control/`),
  RF=1 data plane and RF=3 replicated path over a reference store
  (`lib/cluster/data/`), movement/balance/repair (`lib/cluster/movement/`),
  replica reads (`replica_read.hpp`, `replica_coordinator.hpp`), feature bricks
  (`lib/cluster/features/`).
- Integration M1 is DONE (`ef38b1e`): 3-node Docker cluster via best-effort
  full-replication HTTP forwarding (`lib/cluster/integration/cluster_gateway`),
  183/184 jest tests green cluster-wide. RF=N, no consensus, no partitioning.
- **The central blocker:** `data::DataPoint{SeriesId128, ts, double}` cannot
  carry a real Engine write. `SeriesId128` is a one-way hash (identity
  unrecoverable → no `indexMetadataSync`, no `TimeStarInsert` reconstruction),
  string fields are unrepresentable, int64 loses precision above 2^53, and
  revisions have no channel. `LocalStore::applyWrites(vector<DataPoint>)`
  therefore has no production implementation. Everything downstream of M1
  gates on closing this gap.
- No production `LocalStore`/Engine adapter, no group-0 bootstrap in the
  server, no cross-node RPC carrying real payloads, no partitioned routing
  (`VShardMapping::serverId` unused), no leader-read wiring.

## Milestone sequence and dependency order

```text
F   Foundation: enriched data-plane types + EngineLocalStore + transport
     |            (closes the DataPoint gap; gates everything below)
M2  VShard-partitioned RF=1 (Phase 4 bricks on the real Engine)
     |
M3  Raft-replicated RF=3 + group-0 bootstrap (Phase 2/3/5 bricks live)
     |            leader reads behind ReadIndex ship HERE, not later
M4  Read-path extensions: replica reads opt-in, client retry contract
M5  Online operations: join/move/repair/balance (Phase 7 wiring)
M6  Feature completion: cluster SSE, backup/restore, feature gates,
     routing summaries, full operator surface (Phase 8 wiring)
X   Cross-cutting workstreams (mTLS, harness, benchmarks, index prefixing,
     tombstone GC, client bindings) — staged against M3/M5/GA
```

**The single most important sequencing decision:** F is a pure
type-and-adapter milestone with no new distributed behaviour, and it is done
FIRST, alone. Every later milestone reuses its `WriteRecord`/typed-partial
types unchanged; doing F "inside" M2 or M3 would force re-opening the Phase
4/5 codec, router, state-machine, and RF=3 gates twice. F re-opens them
exactly once, re-proves them with the enriched types, and everything after is
wiring. Second-order decision: M2 uses a **static derived placement** (no
group 0), so the partitioned data plane is demonstrable before any Raft group
exists in the server; group 0 boots in M3 where it is first needed.

Each milestone ends in a demonstrable Docker-testable increment gated by the
existing `docker-compose.cluster.yml` + 184-test jest harness, extended per
milestone.

---

## F — Foundation: close the DataPoint gap

**Goal.** A lossless inter-node write/query representation the real Engine can
produce and consume, the production `EngineLocalStore` adapter, and the
transport carrying both. No behaviour change in the server yet (M1 forwarding
stays live until M2 replaces it); F lands as library + tests.

### F.1 The enriched write command: `WriteRecord`

New header `lib/cluster/data/write_record.hpp` (+ codec in
`dataplane_codec.*`):

```cpp
// One series' column batch — deliberately isomorphic to TimeStarInsert<T>.
struct WriteSeries {
    std::string seriesKey;            // canonical bytes; SeriesId128 = hash(seriesKey)
    TSMValueType type;                // Float | Integer | Boolean | String
    std::vector<uint64_t> timestamps;
    std::variant<std::vector<double>, std::vector<int64_t>,
                 std::vector<bool>,   std::vector<std::string>> values;
    std::vector<uint64_t> revisions;  // empty = unassigned (assigned at apply, ADR 0003)
};
struct WriteBatch {
    std::vector<WriteSeries> series;
    uint64_t schemaVersion = 0;
};
```

Decisions:

- **Carry the canonical `seriesKey` string, not split measurement/tags/field.**
  The seriesKey is the exact byte string whose hash IS the series identity, so
  receiver-side `SeriesId128`, `virtualShard()`, and core routing are
  guaranteed identical to the sender's, with no escaping-ambiguity channel.
  The receiver rebuilds components via the existing
  `TimeStarInsert<T>::fromSeriesKey()` (the canonical routing rule: always
  `fromSeriesKey(buildSeriesKey(...))`, never `fromComponents`). Measurement/
  tags/field for `indexMetadataSync` come from that same parse.
- **Typed per-series columns, not per-point variants.** A series is
  single-typed (Engine's series-type binding enforces it), so the type tag
  lives once per `WriteSeries` and the values stay a flat homogeneous vector —
  codec-compact and directly convertible to `TimeStarInsert<T>`.
- **Replace, do not parallel.** `DataCommand` becomes
  `variant<WriteBatch, DeleteRange, RetentionCutoff>`; the flat `WritePoints`
  is deleted. `DeleteRange` gains `std::string seriesKey` (replacing the bare
  `SeriesId128` — `Engine::deleteRange` takes the key string). Two write
  commands would double every state-machine invariant; there is no deployed
  cluster log to stay compatible with, so the log format version simply bumps.
  This is the honest cost: it **re-opens the Phase 5 DataStateMachine and
  RF=3 gates** (below).
- `DataPoint`/`QuerySpec`/`QueryPartial` survive only as the deterministic
  reference-model shapes inside `ReplicatedVShardStore` tests; nothing on the
  production path uses them after M2.

### F.2 The typed query request/partial

Production cluster queries do not fan out `QuerySpec{start,end,method}` — they
fan out the real parsed query. New header
`lib/cluster/data/node_query.hpp` (+ codec):

```cpp
struct NodeQueryRequest {
    timestar::QueryRequest request;   // the FULL parsed query: measurement, scopes,
                                      // fields, groupBy, interval, bucketAlignment,
                                      // booleansAsNumeric — reuse the existing
                                      // protobuf QueryRequest as the wire form
    uint64_t mapEpoch = 0;            // pinned placement epoch
    std::vector<uint16_t> vshards;    // the VShards THIS node must answer for
    uint64_t taskId = 0;              // retry-replaces-contribution key
};
struct NodeQueryPartial {
    // The existing intra-node cross-core partials, serialized: per-series
    // results (raw typed points — double/int64/bool/string — or per-bucket
    // values) plus unfinalized AggregationState for mergeable methods.
    std::vector<SerializedSeriesPartial> series;
    std::vector<data::ReadEnvelope> envelopes;  // (vshard, term, appliedIndex) per VShard
    // Fail-closed accounting: QUERY_INCOMPLETE reasons, dropped series.
    std::vector<std::string> incompleteReasons;
};
```

Decision: **the cross-node partial IS the cross-core partial.** The
coordinator-side merge reuses the exact `mergePartialAggregations*` /
`AggregationState` / `methodCanFoldRaw()` code that already merges core
partials in `http_query_handler.cpp` — not the toy `AggState`. This is what
makes the canonical semantics (shape independence, non-numeric passthrough,
NaN policy, spread-fold rule) hold across nodes for free: the same fold code
runs, just fed from a socket instead of `invoke_on`. `AggState` remains for
the reference-model gates only. Non-numeric results ride
`SerializedSeriesPartial` natively (typed raw vectors / LATEST-per-bucket),
closing the "typed query partials that carry non-numeric results" requirement.

### F.3 The production `EngineLocalStore`

New `lib/cluster/integration/engine_local_store.{hpp,cpp}`:

```cpp
// Node-level adapter over sharded<Engine>. Lives outside lib/core: the Engine
// stays cluster-unaware.
class EngineLocalStore {
public:
    explicit EngineLocalStore(seastar::sharded<Engine>& engines);
    // Group WriteSeries by owning core, build TimeStarInsert<T> per (series,type),
    // dispatch via invoke_on -> enforceSeriesTypes/insertBatch<T>, then
    // indexMetadataSync with MetadataOps parsed from each seriesKey.
    seastar::future<> applyWrites(WriteBatch batch);
    seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end);
    seastar::future<> applyRetention(uint64_t cutoff);
    // Run the existing two-phase local pipeline (discoverSeriesAcrossShards ->
    // executeShardQuery per core -> partial merge) RESTRICTED to req.vshards,
    // and serialize the partial.
    seastar::future<NodeQueryPartial> queryLocal(NodeQueryRequest req);
};
```

Key rules:

- **Core routing switches to VShard-cohesive in cluster mode.** Today
  `routeToCore()` is `hash % coreCount` while VShard identity is
  `hash & 4095`; a VShard's series scatter across cores. Cluster mode routes
  by `assignCore(virtualShard(id), coreCount)` (the `PlacementTable::mapping`
  authority) so each VShard is single-core — the precondition for per-VShard
  Raft groups (M3), snapshots, and moves. This is a data-placement change:
  cluster mode is only enabled on a fresh data dir or after the Task 6
  migration tool; the fail-closed startup gate refuses a mixed root.
- Revisions: `applyWrites` **stamps nothing itself.** In M2 (RF=1, no log) the
  Engine's own `setRevisionAssignment(true)` counter stamps at insert (ADR
  0003 pre-Raft journal-seq mode). In M3 the state machine passes log-derived
  revisions in `WriteSeries::revisions` and the Engine must not re-stamp —
  `stampRevisions` already skips non-empty revision vectors; add a test
  pinning that.
- Engine apply failures inside `applyWrites` are surfaced, never swallowed
  (the write router/state machine decide policy; see M3 risk on backpressure).

### F.4 Transport: extend `DataPlaneRpc`

Extend `lib/cluster/data/dataplane_rpc.{hpp,cpp}` with the enriched verbs
(the M2/M3 carrier; mTLS/protobuf are workstream X1):

```cpp
seastar::future<> forwardWriteBatch(NodeId to, WriteBatch batch);        // owner-apply (M2)
seastar::future<bool> proposeWrite(NodeId to, uint16_t vshard, DataCommand cmd); // leader-propose (M3)
seastar::future<NodeQueryPartial> queryNode(NodeId to, NodeQueryRequest req);
seastar::future<raft::LogIndex> leaderReadIndex(NodeId to, uint16_t vshard);     // M4 replica reads
seastar::future<raft::LogIndex> leaderCommitIndex(NodeId to, uint16_t vshard);
```

Keep the compact bounds-checked codec discipline (every decode returns
nullopt on malformed input); keep the `set_fixed_cpu(this_shard_id())` listen
pinning (the resolved Phase-4 hang). Frame every verb with a
`{wireVersion, clusterUuid}` envelope now — cheap, and it is the hook X1
(mTLS/protobuf) and M6 (feature gating) later negotiate through.

### F.5 Generalize the routing bricks over the enriched types

`VShardLeader`, `ReplicatedWriteRouter`, `ReplicatedQueryCoordinator`
(`replicated_vshard.hpp`, `replicated_router.hpp`), `WriteRouter`,
`QueryCoordinator`, and `ReplicaVShard`/`ReplicaQueryCoordinator` are written
against `DataCommand`/`QuerySpec`/`QueryPartial`. Generalize their
spec/partial parameters (template or a small trait: `Cmd`, `Spec`, `Partial`,
`mergeInto(Partial&, Partial)`) so one body serves both the reference types
(existing gates, unchanged) and the production types (new instantiations).
The routing INVARIANTS — resolve-every-leader-before-dispatch, fail-closed on
unassigned VShards, all-dispatches-awaited, retry-replaces-contribution — are
what the gates prove; re-run them under both instantiations.

### Gate (F)

Unit gates only (no docker change):

- Codec round-trip + adversarial-truncation tests for `WriteBatch`,
  `DeleteRange(seriesKey)`, `NodeQueryRequest`, `NodeQueryPartial` (extends
  `dataplane_codec` tests — **re-opens the Phase 4 codec gate**).
- `DataStateMachine`/`ReplicatedVShardStore` gates re-proven with `WriteBatch`
  (typed values, revisions-at-apply, delete-by-seriesKey) — **re-opens the
  Phase 5 state-machine gate**, including `RetryLogOrderSemantics`.
- `replicated_rf3_test` re-run with the enriched command — **re-opens the
  RF=3 gate** (mechanical: same scenarios, new payload type).
- `DataPlaneRpcTest` socket test extended: a `WriteBatch` containing all four
  value types + a string series + revisions forwarded over loopback RPC into a
  real `sharded<Engine>` via `EngineLocalStore`, then `queryNode` returns
  partials that merge to the single-node answer, including a non-numeric
  (string/bool) series and a `spread` group-by (the `methodCanFoldRaw` trap).
- `EngineLocalStore` unit tests: type enforcement, metadata sync visibility,
  vshard-restricted discovery.

**Risks:** seriesKey parse cost on the hot forward path (mitigate: per-batch
key interning — each unique key once per batch); `QueryRequest` wire reuse
must include the migration-compat fields (`bucketAlignment`,
`booleansAsNumeric`) or cluster answers diverge from single-node — pin with a
parity test. **Defers:** nothing; F is the foundation.

---

## M2 — VShard-partitioned RF=1

**Goal.** A 3-node Docker cluster where data is PARTITIONED, not replicated:
every series lives on exactly one owner node; any node coordinates. This
retires M1's full replication for /write and /delete.

**Activates:** `VShardDirectory`, `WriteRouter`, `QueryCoordinator` (enriched
instantiations), `DataPlaneRpc`, `subscribe_policy.hpp` (`evaluateSubscribe`),
`ClusterInspector` (cluster status / vshard describe / placement explain),
`scatter_gather.hpp` boundaries. **Pure wiring** — no library gate re-opens.

### Placement (static, derived — no group 0 yet)

`lib/cluster/integration/cluster_runtime.{hpp,cpp}` (new): builds a
`VShardDirectory` from a synthesized `ControlMap{epoch=1, placement[vs] =
{peers[vs % N]}}` derived from the existing `[cluster]` config
(`node_id`, `peers`). Deterministic on every node; changing the peer list is a
new cluster (documented limitation until M3). `VShardMapping::serverId` is now
real: `PlacementTable` gains the stored per-vshard server mapping behind the
existing `mapping(vshard)` accessor (the hook the comment reserves for
"Phase 6").

### Write path

`http_write_handler.cpp`: after parse (both JSON and protobuf phases), in
cluster mode group the built `TimeStarInsert<T>`s by
`ownerOfSeries(seriesId)`:

- Local groups: existing `invoke_on` insert path unchanged.
- Remote groups: convert to `WriteSeries` and `forwardWriteBatch` per owner
  (one RPC per destination node, batched — the plan's fan-out contract).
  Awaited, not best-effort: a failed owner fails those groups in the response
  (per-group failure reporting; retry-the-whole-batch is safe under LWW).
- The M1 `replicateWrite` broadcast path and `kForwardedHeader` loop guard are
  removed for /write and /delete. `ClusterGateway` survives only as the
  interim awaited-broadcast carrier for retention/schema mutations (M1.x):
  `/retention` PUT/DELETE bodies broadcast to all nodes and awaited —
  explicitly interim until M3 moves them into group-0 CAS.
- Backpressure: `IngestBacklogException` from a remote owner maps to 503 +
  Retry-After at the coordinator, same as local.

### Query path — reconciling with the two-phase core fan-out

The hard part. Decision: **elevate the node boundary to mirror the core
boundary.** The coordinator does NOT run cross-node series discovery; it
forwards the whole `NodeQueryRequest` to each owner node (initially all owner
nodes; RoutingSummary pruning is M6), and each node runs its existing
two-phase pipeline (`discoverSeriesAcrossShards` → `executeShardQuery` per
core → intra-node partial merge) over its own cores, restricted to
`req.vshards`, returning one `NodeQueryPartial`. The coordinator then runs
the same partial-merge code once more over node partials.

- `http_query_handler.cpp`: new top-level branch `executeClusterQuery()` —
  fan out `queryNode` to `dir.ownerNodes()` (self served via
  `EngineLocalStore::queryLocal` directly, no socket), merge with the
  existing `mergePartialAggregations*` path, then the existing finalize/
  consolidate/serialize pipeline unchanged.
- VShard filter: each node's discovery drops series whose
  `virtualShard(seriesId)` it does not own per the pinned epoch — trivial at
  static RF=1, load-bearing during M5 moves (a moved-away vshard's leftover
  files can never double-count).
- Fail-closed: a node RPC failure → `QUERY_INCOMPLETE` naming the unreachable
  vshards and reason (`QueryIncomplete` maps to the existing HTTP 500
  contract). `allow_partial=true` returns data plus the named-missing list.
  Retry replaces the contribution for `(taskId, vshard)`, never appends.
- Metadata endpoints (`/measurements`, `/tags`, `/fields`, `/cardinality`):
  scatter to all owner nodes, union/HLL-merge — same merge shapes as the
  cross-core versions. The originally planned owner-broadcast pattern delete is
  superseded: expansion can change across an ambiguous retry and delete a newly
  created series. Production now rejects pattern and mixed-pattern requests
  before discovery or proposal; re-enablement requires a replicated immutable
  expansion plan or an equivalently bounded selector command.

### Subscribe guard + operator surface

- SSE `/subscribe`: wire `evaluateSubscribe(isClustered(dir), rejectInCluster
  config)` — cluster mode rejects (default) or marks node-local in the
  subscription handshake response. The forbidden silent degradation dies here,
  the moment partitioned traffic exists.
- HTTP operator surface (new `lib/http/http_cluster_handler.{hpp,cpp}`):
  `GET /cluster/status`, `GET /cluster/vshard?id=`,
  `GET /cluster/placement?series=` over `ClusterInspector` — the Phase-4
  minimum surface that debugs everything after.
- Enable `Engine::setRevisionAssignment(true)` and
  `tsm_compactor::setVShardPartitioning(true)` in cluster mode (note the
  known mid-registration-retry residual — acceptable, dedup-correct; now
  actually exercised, add a soak assertion).

### Gate (M2)

Docker 3-node cluster, jest suite extended (`test_api/cluster/`):

- **Partitioned, not replicated:** write a spread of series through node 1;
  assert per-node series counts are disjoint and sum to the total (via
  `/cluster/vshard` + per-node `/measurements` in a node-local debug mode).
- **RF=1 == single-node:** the full 184-test API suite against any node
  matches single-node results byte-for-byte, including non-numeric fields,
  group-by spread, bucketAlignment, booleansAsNumeric (the canonical-semantics
  parity set from `dynamo_equivalence_test`).
- **Failure contract:** stop one container → queries touching its vshards
  return `QUERY_INCOMPLETE` naming them (not empty success); writes to its
  series fail with per-group errors; `allow_partial` names the missing.
- Subscribe in cluster mode is rejected/marked, asserted.

**Risks:** coordinator serialization cost for large raw partials (mitigate:
stream node partials; result-size limits already exist); jest suite assumes
any-node-sees-all — it does (any node coordinates), but node-local debug
endpoints must be excluded from parity. **Defers:** placement changes (map is
static until M3+M5), replication (M3), retention/schema via consensus (M3).

---

## M3 — Raft-replicated RF=3 + group-0 bootstrap

**Goal.** Production RF=3: per-VShard Raft groups over the real Engine,
group-0 control plane in the server, writes acknowledged only after durable
quorum commit, reads linearizable behind ReadIndex. The v1 read model
(leader reads) ships HERE — it is not a separate milestone.

**Activates:** `RaftGroup`, `RaftGroupRegistry`, `raft_journal_persistence`,
`raft_rpc_transport`, `DataStateMachine` (enriched), `ReplicatedVShard`,
`ReplicatedWriteRouter`/`ReplicatedQueryCoordinator` (enriched
instantiations), `Group0StateMachine`, `Group0Controller`, `meta_voters`,
`control_map_cache`, `controller_epoch_fence`, `epoch_regression_guard`.

### Server-side cluster runtime

Extend `cluster_runtime.{hpp,cpp}` + `bin/timestar_http_server.cpp`:

1. **Node identity:** persistent `node.json` (node UUID, cluster UUID) under
   `data_dir`; reinstall-without-identity is a replacement, per plan.
2. **Bootstrap ceremony:** `timestar cluster init` (new CLI verb on the server
   binary) → `Group0Controller::initCluster` on a group of one; subsequent
   nodes reach group 0 via `[cluster.control] seed_nodes` and
   `admitNodeWithToken` (tokens minted by `mintJoinToken` via operator CLI).
   No implicit initialization, ever.
3. **Group-0 hosting:** one `RaftGroup(0)` + `Group0StateMachine` on core 0,
   journal-backed via `raft_journal_persistence`; `reconcileMetaVoters`
   drives odd voter counts across `NodeRecord::failureDomain`s;
   `stampControllerTermIfLeader` on leadership. `epoch_regression_guard`
   wired: regressed group-0 state freezes control actuation (data plane
   continues), operator supersession required.
4. **Placement:** the controller computes desired placement (rendezvous over
   Active nodes, distinct failure domains, RF from
   `[cluster.replication] voters`) → `SetDesiredPlacement` per vshard; nodes
   reconcile their `control_map_cache` by readBarrier reads of group 0 (the
   authoritative path; the watch-stream RPC hint stays deferred) and
   instantiate/teardown local groups accordingly.
5. **Data groups:** per core, one `RaftGroupRegistry` (shared tick timer,
   shared transport, quiescent-follower hibernation) hosting an
   `EngineDataStateMachine` per locally-replicated vshard, on the core
   `assignCore(vshard, coreCount)` — matching the F.3 core-cohesive routing.

### `EngineDataStateMachine`

New `lib/cluster/integration/engine_data_state_machine.{hpp,cpp}`:
`raft::RaftStateMachine` whose `apply()` decodes `DataCommand` and applies
through `EngineLocalStore` on the group's own core, assigning revisions from
the log position (ADR 0003: revision = per-VShard log index continuation) into
`WriteSeries::revisions` before insert. Contracts:

- A committed-but-undecodable entry is fail-stop (divergence guard, as in the
  brick).
- Apply must not lose an entry: Engine backpressure (`IngestBacklogException`)
  must never fire inside apply — admission happens at PROPOSE time (leader
  checks `rejectIfIngestBacklogged` before proposing; a follower behind on
  apply is lag, handled by Raft flow control), so apply retries on transient
  resource pressure rather than skipping. This asymmetry is deliberate and
  documented in the header.
- Applied-index checkpointing is a perf optimization, not correctness:
  re-applying a committed suffix after restart is idempotent because revisions
  are log-derived (identical re-stamp) and writes are LWW. v1 persists a
  per-vshard applied-index watermark opportunistically (with the WAL fsync it
  already rides) and replays from the last watermark on boot.
- `snapshot()`: NOT the in-memory `store_.encode()` — production snapshots are
  `Engine::createVShardSnapshot` manifests (Task 4d pieces: extents +
  resolved-view verification hash), installed via
  `Engine::restoreVShardSnapshot`. M3 wires the monolithic InstallSnapshot
  payload as a manifest + object stream; chunking on the wire is M5.

### Write and read paths

- `LeaderResolver` becomes real: local `ReplicatedVShard` when this node leads
  the vshard; otherwise a `RemoteVShardLeader : VShardLeader` that calls
  `DataPlaneRpc::proposeWrite`/`queryNode` on the leader node (leader hint
  from the group's `leader()` propagated through the map cache; a `false`
  return = stale routing = retry, per the router contract). This closes the
  deferred "leader-reach" wiring.
- Write: HTTP → `ReplicatedWriteRouter::write` (enriched) →
  `proposeAndAwaitApplied` on each destination leader. Ack = durable quorum
  commit + local apply. `wal_sync_mode=always` group commit is the durable
  journal primitive under the Raft journal (constraint 5 of the plan).
- Read: `NodeQueryRequest` is served only for vshards this node LEADS, each
  behind `RaftGroup::readBarrier()` before local execution;
  `NodeQueryPartial::envelopes` carries `(vshard, term, appliedIndex)` — the
  v1 leader-read model, linearizable, no modes. The M2 coordinator changes
  only its fan-out target: leaders instead of owners.
- Schema/first-observation writes: coordinator resolves field types once
  before fan-out; first observation of a measurement/field runs the group-0
  `CasPolicy` CAS and holds the write until commit (create wins or adopt);
  data entries embed `schemaVersion`. Retention policy mutations move from
  the M2 gateway broadcast to group-0 CAS + replicated `RetentionCutoff`
  proposals by vshard leaders (leader-domain time, monotonic); downsampling
  is disabled in cluster mode v1, per plan. The M1/M2 `ClusterGateway`
  is now fully retired.

### Controller job driver (minimal)

`Group0Controller` gains the periodic reconcile loop it needs for M3 only:
desired-vs-effective membership comparison and `proposeConfChange` on data
groups to realize RF=3 placement at bootstrap (join-time learner → catch-up →
promote uses the `Mover` step machine already — instantiate `MoveExecutor`
over `RaftGroup` + snapshot install; SLO throttling and balancers stay M5).
Job-step RPCs carry the controller epoch; data nodes fence via
`controller_epoch_fence`.

### Gate (M3)

Docker 3-node RF=3 cluster + the multi-process harness (X2, built now):

- **The Phase 5 gate, live:** kill -9 one container mid-load → zero
  acknowledged-write loss, zero duplication (point counts + aggregation
  parity vs a control run), writes continue on the majority; restart → 
  catch-up; `docker network disconnect` a leader 1-vs-2 → minority stops
  acking within the election bound, majority continues, heal truncates the
  stale tail, no split-brain (divergence check via per-vshard verification
  hashes).
- **Leader reads:** kill a leader, read through the new leader before its
  term no-op — the read waits for the barrier, never returns stale (the
  falsifying leader-read tests, scripted against docker).
- **Group-0 contract:** stop meta voters to lose quorum → data plane serves,
  joins/schema/placement freeze, restore → resume. Deposed-controller
  job-step RPCs rejected by term fencing.
- 184-test API suite green at RF=3; all F/M2 parity assertions still green.

**Risks:** (1) 4096-group × RF3 resource budget per node — startup time,
timers, journal fan-in; the registry hibernation covers steady-state, but the
M3 gate must include the "start a node hosting thousands of groups" budget
test; if it fails, the fallback is grouping vshards into fewer Raft groups
(placement-group coalescing), a design change to escalate, not absorb.
(2) Double WAL: Raft journal + Engine WAL both fsync on the write path —
accepted for M3 (correct, 2x write amplification), with a follow-up decision
(X-open) to teach the Engine to treat the Raft journal as its WAL.
(3) `proposeAndAwaitApplied` holds the HTTP request across consensus —
latency budget needs the group-commit `max_delay_ms` tuning from the config
outline. **Defers:** replica reads (M4), balancers/scrub/throttle (M5),
chunked snapshots (M5), node-liveness detector (stays deferred; registry
tick-skipping suffices at M3 scale).

---

## M4 — Read-path extensions: replica reads + client retry contract

**Goal.** Opt-in read scaling on the proven Phase 6 bricks, and the
documented client contract in the official bindings. Leader reads (the v1
default) already shipped in M3; M4 adds no default-path change.

**Activates:** `ReplicaVShard`, `ReplicaSelector`, `ReplicaQueryCoordinator`,
`RaftGroup::waitApplied`, `operator_surface.hpp` `ReplicaDecision` trace.

Tasks:

1. Wire `LeaderReadIndexFn`/`LeaderCommitFn` over the F.4 RPC verbs
   (`leaderReadIndex`/`leaderCommitIndex`) — the deferred leader-reach step.
2. HTTP surface: `"consistency": "leader" (default) | "session" |
   "bounded_staleness"` + `"maxLagIndex"` on `/query`; the response envelope
   already carries `(vshard, term, appliedIndex)` per partial — echo it as
   the session token; `Session` requests carry it back. Any non-leader mode
   is explicitly non-default and documented as such (v1 claims stay intact).
3. Selector telemetry: reachable/lag/queueDepth/local/errorRate fed from the
   Raft transport health and per-node read stats; the standing read-routing
   balancing LOOP consumes the same telemetry M5's balancers export and lands
   with M5 (as the backlog notes).
4. `GET /cluster/replica-decision?vshard=` — `traceReplicaDecision` surface.
5. **Client bindings (X6):** JS/Python bindings retry byte-identical write
   batches on failure/timeout. Exact RF&gt;1 deletes preserve the request body,
   `Idempotency-Key`, and original `Idempotency-Key-Timestamp`; a retired
   identity is terminal and requires reconciliation rather than a fresh token.
   Bindings also implement `QUERY_INCOMPLETE`-is-not-empty handling. External
   repo; contract text from `docs/clustering.md` §"Write path" and
   `docs/api-delete.md` §"Clustered Mode".

**Gate:** replica-read adversarial suite re-run over live RPC (lag, retries,
placement change, mid-query leader failure, partition-reject — the
`replica_read_test` scenarios against docker); session read-your-writes
demonstrated across nodes; bounded-staleness rejects on partition.
**Re-opens:** none (pure wiring; the bricks' gates were built for injection).
**Defers:** time-bounded (clock) staleness — stays deferred pending the
clock-error-bound decision 7, as the backlog records.

---

## M5 — Online operations: join, move, repair, balance (Phase 7 wiring)

**Goal.** A loaded cluster grows N→N+1 online; corruption is scrubbed and
re-replicated; capacity/leadership balance under SLO protection; operators
drive it over HTTP/CLI.

**Activates:** `Mover`, `MoveJob`, `PlacementBalancer`, `LeadershipBalancer`,
`MovementThrottle`, `Scrubber`, `SnapshotStreamSource/Sink`, `RebalanceOps`,
`UpsertJob` job persistence.

Tasks:

1. **Controller job driver, full:** on the group-0 leader, a fiber scans
   `Group0State::jobs`, decodes `MoveJob`s, and drives `Mover::run` with a
   production `MoveExecutor` (voters/learners from the data group's config;
   `commitConfig` = `proposeConfChange` through joint consensus; `catchUp` =
   snapshot install + replay to cutover; `persist` = `UpsertJob` proposal).
   Crash-resume proven by the existing growth gate, now over the server.
   Leadership-transfer-before-victim-removal via `transferLeadership`.
2. **Chunked InstallSnapshot on the wire:** wire `snapshot_stream`'s resumable
   chunk hashing into the InstallSnapshot RPC (offset + outstanding window) —
   re-opens `raft_messages.hpp` framing (a versioned wire change behind the
   F.4 envelope).
3. **Telemetry for balancers:** extend `NodeRecord` with capacity weight,
   disk-used/free, and load summaries — a group-0 state/codec change
   (**re-opens the Phase 3 control-command codec gate**; mechanical). Feed
   `NodeLoad` from real per-node metrics; `ForegroundSignals` from reactor
   stall/p99/error/queue/disk metrics (the throttle's auto-pause inputs).
4. **Balancing loops:** placement balancer (hard failure-domain/watermark
   constraints first) emitting bounded `MovePlan`s → jobs; leadership
   balancer emitting `transferLeadership` batches (also v1's read balancing).
   Hysteresis and minimum residency per plan.
5. **Scrub loop:** background `Scrubber` pass per node (rate-limited, own
   scheduling group) verifying object hashes/CRCs; quarantine →
   `RepairRequest` → full re-replication through the movement path
   (Ceph-style backfill, no new machinery).
6. **Operator surface:** `node join/drain/remove/replace`,
   `vshard move/transfer-leader`, `rebalance status/pause/resume` (over
   `RebalanceOps`), `repair status/scrub` — HTTP + CLI verbs on the M2
   cluster handler.
7. **Multi-host benchmarks (X4):** extend `timestar_insert_bench`/
   `timestar_query_bench` to multi-host targets; measure scaling efficiency
   (target ≥70% per plan) and finalize the release number.

**Gate:** the Phase 7 growth gate on docker: loaded RF=3 cluster grows 3→4
with continuous writes/deletes/queries; RF never below 3 at any committed
step; destination logically byte-identical (verification hash) at cutover;
kill the snapshot source mid-move → resume from another replica; saturate
foreground → throttle auto-pauses; controller kill after every job step →
next leader resumes; no double-counted query results during moves (the M2
vshard-filter earning its keep). **Defers:** surgical Merkle repair
(post-v1), tombstone-GC-in-compaction lands here as a task if capacity
allows, else stays X5.

---

## M6 — Feature completion (Phase 8 wiring)

**Goal.** Close the remaining plan features: cluster-aware streaming, backup/
restore tooling, rolling-upgrade gates, routing summaries, hierarchical merge.

**Activates:** `stream_subscription.hpp` (`SubscriptionCursor`,
`BackfillLiveStream`), `backup_restore.hpp`, `feature_gate.hpp`
(`VersionRange`, `FeatureGate`), `routing_summary.hpp` (`RoutingSummary`,
`HierarchicalMerge`).

Tasks:

1. **Cluster SSE `/subscribe`** (replaces the M2 guard): the accepting node is
   subscription coordinator; it registers tasks against current VShard
   leaders; each leader emits `StreamEvent{vshard, term, commitIndex}` from
   the `EngineDataStateMachine` apply hook; backfill→live at ONE
   `readBarrier` per vshard via `BackfillLiveStream::deliver`; dedup/resume
   via `SubscriptionCursor` (resume token = last (term,index)); placement
   changes re-register from the last delivered position. Gate: the streaming
   gate live — kill/move a leader mid-stream, no loss, no silent duplication,
   resumable IDs verified.
2. **Backup/restore CLI:** `timestar cluster backup` = pinned per-VShard
   snapshot export (`BackupRestore::exportCluster` over
   `createVShardSnapshot` streams + verification hashes); `timestar cluster
   restore` = fresh cluster UUID, generation-one import via
   `restoreVShardSnapshot`, membership scrubbed, fail-closed on any hash
   mismatch or truncation. Gate: restore into a new UUID on docker; replicas
   rebuilt; old membership never resurrects.
3. **Feature gating:** carry `VersionRange` in the RPC handshake envelope
   (F.4); a format activates only via a committed group-0 command once
   `FeatureGate::canActivate` over the current voter set holds. Gate: rolling
   upgrade of a docker cluster across adjacent versions under load.
4. **Routing summaries:** populate `RoutingSummary` from the live index —
   `observe(measurement, vshard)` on the write path before query visibility,
   epoch-bound to placement, sealed after a complete scan; coordinator prunes
   fan-out only on sealed+current summaries (false negatives impossible by
   construction). Hierarchical merge activated for saturated coordinators
   (canonical left-fold order — result independent of topology, already
   pinned by the brick's gate).
5. Remaining operator/metrics surface per plan §"Observability".

**Re-opens:** the SSE handler's node-local guard (deliberately — that guard
exists to be replaced by exactly this). **Defers:** hot-series lanes (out of
first release, decision 6), replica-read demand-driven extensions.

---

## X — Cross-cutting workstreams

- **X1 mTLS + protobuf inter-node wire** (backlog: Phase 4 hardening).
  Staged: (a) F.4's versioned envelope now; (b) TLS on both RPC servers
  (seastar `tls::credentials_builder`, cluster-UUID-bound node certs) before
  any multi-tenant/untrusted-network deployment — target alongside M3
  hardening, required before GA; (c) protobuf message migration inside the
  versioned envelope, gated through M6's feature-gate machinery; (d) cert
  rotation without cluster restart; (e) pools/deadlines/cancellation/
  backpressure budgets on every verb. Not gate-blocking for M2/M3
  correctness, gate-blocking for GA.
- **X2 Multi-process cluster harness** (backlog: Phase 2 [~]). Built for the
  M3 gate: a test driver that spawns N real server processes (or docker
  containers) with step-hook crash injection (SIGKILL at named barriers via a
  debug HTTP hook) and partition injection (iptables/`docker network
  disconnect`). This closes the literal Phase 2 gate text and is the vehicle
  for the M3/M5 falsifying tests.
- **X3 Fault-injecting file layer** already exists from Phase 2; reused by M3
  (journal error-latching under injected EIO in the live server).
- **X4 Multi-host distributed benchmarks** — lands with M5 (above).
- **X5 Tombstone GC during compaction** (backlog: Phase 5 note). Compaction
  physically drops tombstone-covered points and retires fully-superseded
  tombstones, bounded by the snapshot-pin watermark. Schedule: with M5's
  compaction-adjacent work; LOW risk, correctness unaffected until then.
- **X6 Client bindings idempotent-retry** — lands with M4 (above); required
  before GA.
- **X7 VShard-prefix the measurement-scoped index keys** (backlog: Phase 1).
  Apply the 0x05/0x09-style vshard-in-key layout to 0x0A/0x06/0x0D. Value is
  index-extract efficiency for M5's move traffic (today's extract rebuilds
  from `SERIES_METADATA`, which is correct but slower). Schedule: before M5
  if move benchmarks show extract cost dominating; else stays deferred.
- **X-open (new decision to record):** collapse the double WAL — teach the
  Engine to accept the Raft journal as its durability log in replicated mode
  (the ADR 0003 revision continuity already aligns them). Prototype after M3
  benchmarks quantify the 2x write amplification.

Explicitly staying deferred (with the backlog's own rationale): node-liveness
failure detector (hibernation suffices), live watch-stream RPC (readBarrier
reconcile is authoritative; the stream is a hint), time-bounded staleness
(clock-error decision 7), strict retry-invisibility (resolved NOT NEEDED),
hot-series lanes (decision 6).

## Gate re-opening summary (honesty table)

| Milestone | Re-opens a proven gate? | Which / why |
| --- | --- | --- |
| F | YES | Phase 4 codec; Phase 4/5 router+coordinator (typed instantiation); Phase 5 DataStateMachine + RF=3 (WriteBatch replaces WritePoints); Phase 4 socket RPC test |
| M2 | No | Pure wiring; new e2e gates only |
| M3 | Partially | `DataStateMachine::snapshot` contract (manifest-based, not `encode()`); everything else is wiring of proven bricks |
| M4 | No | Bricks were built for injected transports |
| M5 | YES | Phase 3 control codec (`NodeRecord` telemetry fields); `raft_messages` InstallSnapshot framing (chunking) |
| M6 | No | SSE guard replaced by design |

## Risks and unknowns

1. **Per-node group count at 4096 vshards × RF=3.** The registry's
   hibernation is proven in-process; the live budget (fds, timers, journal
   fan-in, startup) is not. Measured at M3 gate; fallback (vshard→group
   coalescing) is a design escalation, decided then, not silently absorbed.
2. **Engine-apply inside the Raft apply loop.** Backpressure inversion
   (admission at propose, never failure at apply) is a new contract on a hot
   path; a bug here is a stalled state machine. Pinned by dedicated tests and
   the X3 fault layer.
3. **Snapshot of a live VShard over `shard_N`-era layout.**
   `createVShardSnapshot` reads flushed TSM only; concurrent tier-0
   multiplexed extents rely on the Task 4c repartition path. The M5 gate's
   snapshot-under-concurrent-everything test is the proof; until then M3
   restricts snapshot points to post-flush barriers.
4. **Double-WAL write amplification** (X-open) — cost known and accepted for
   M3; must be measured before the 70% scaling-efficiency claim is finalized.
5. **Query-semantics parity across the wire.** Every canonical-semantics rule
   (shape, NaN, non-numeric, LWW, `QUERY_INCOMPLETE`) must survive
   serialization; the F gate's parity tests and the M2 jest parity run are
   the standing defense — any new wire field must ship with a parity case.
6. **Placement-epoch discipline during M5 moves.** Double-count/silent-miss
   bugs hide here; the M2 vshard-filter plus pinned `mapEpoch` on every
   `NodeQueryRequest` is the invariant, asserted in the M5 gate's
   moves-under-query test.
7. **Client auth in cluster mode** remains open decision 9 — X1 delivers
   inter-node identity only; client-facing auth needs an explicit
   in/out-of-scope decision before GA.

## Backlog coverage map (clustering-deferred-tasks.md → this plan)

| Backlog item | Where |
| --- | --- |
| M1.x retention/schema replication | M2 (interim broadcast) → M3 (group-0 CAS) |
| M2 VShard-partitioned RF=1 | M2 |
| M3 RF=3 + enriched command + group-0 bootstrap | F + M3 |
| Multi-process cluster harness [~] | X2 (built for M3 gate) |
| Node-liveness failure detector | Stays deferred (recorded, X list) |
| Production Engine↔DataStateMachine adapter + leader routing | F.3/F.5 + M3 |
| Tombstone GC during compaction | X5 (target M5) |
| mTLS/protobuf wire + pools/deadlines/rotation | X1 (staged; GA-blocking) |
| Multi-host distributed benchmarks | X4 / M5 |
| Phase 8 production wiring (SSE, backup, gates, summaries, operator) | M6 |
| Client-binding idempotent retry | X6 / M4 |
| Phase 7 production wiring (job driver, telemetry, scrub, throttle) | M5 |
| Chunked InstallSnapshot on the wire | M5.2 |
| Phase 6 leader-reach RPC + read-routing loop | M4 (loop lands with M5 telemetry) |
| Time-bounded staleness | Stays deferred (decision 7) |
| Live watch-stream RPC | Stays deferred (hint only) |
| VShard-prefix measurement-scoped index keys | X7 (before M5 if extract-bound) |
| VShard-partitioned compaction mid-registration retry | Noted at M2 enablement (soak assertion) |
| Retry-invisibility | Resolved NOT NEEDED (unchanged) |

## Smallest next step

Land F.1 + the codec: `lib/cluster/data/write_record.hpp` with `WriteSeries`/
`WriteBatch`, swap `WritePoints` out of `DataCommand`, extend `DeleteRange`
with `seriesKey`, and re-prove the `dataplane_codec` and `DataStateMachine`
unit gates with the enriched types. It is a bounded, purely-local change
(~2-3 files + tests), it is the first domino for every milestone above, and
its review settles the one representation decision (seriesKey string + typed
per-series columns) everything else builds on. Follow immediately with F.3
(`EngineLocalStore`) so the socket round-trip gate (F's last bullet) can run
against the real Engine.
