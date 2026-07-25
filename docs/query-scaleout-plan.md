# Cluster Query Scale-Out Plan

Status: PLANNED (no phase implemented). Branch `cluster-design`, 2026-07-25.
Companion to `docs/clustering.md` and `docs/clustering-integration-plan.md`.

## 1. Problem

Measured RF=3 (3 nodes, `--smp 4`, 10M points, server-side `execution_time_ms`,
harness `qslow.sh` / `qprofile.sh`):

| query                          | single node | RF=3 cluster | overhead |
|--------------------------------|-------------|--------------|----------|
| avg, full range, single field  | 36 ms       | 87 ms        | 2.4x     |
| latest, single field           | 37 ms       | 87 ms        | 2.4x     |
| avg `by {host}`, full range    | 97 ms       | 566 ms       | **5.8x** |

Profiled cause chain (commits `ab0b413`, `39dd3f7`):

- The coordinator routes **every VShard to its current Raft leader**
  (`ClusterDataPlane::queryReplicated`), so ~2/3 of the data volume crosses the
  wire as aggregation partials even though, at RF=3 on 3 nodes, the coordinator
  holds a **local replica of every VShard**.
- Grouped partials ship the full 120-byte `AggregationState` per
  (group, timestamp): **102 B/point** on the wire vs 16 B/point for the
  ungrouped `sortedValues` path. One leader's 310k-point contribution: 31.6 MB
  payload, 134-141 ms encode, 213-317 ms RPC round-trip.
- Coordinator-side decode runs **serially per remote node** (~190 ms for two
  nodes) while the remote encodes at least overlap each other.
- The cross-node merge itself is NOT the problem (53 ms of 566 ms) — measured,
  do not re-litigate.

The write path is not CPU-bound; the query path **is**. Query CPU and query
bytes are the critical path.

## 2. The refactored model: local-first replica reads

Principle: **move the freshness barrier over the wire, not the data.** A
correct replica read needs only (a) a read barrier appropriate to the requested
consistency mode and (b) local apply progress past that barrier. Both
primitives exist and are gate-proven:

- `RaftGroup::waitApplied(index)` — any-role apply-wait (raft_group.hpp).
- `DataPlaneRpc::leaderReadIndex(to, vshard)` — leader-reach ReadIndex verb
  (dataplane_rpc.hpp), socket-proven.
- `ReplicaVShard` (replica_read.hpp:112-123) — the exact ladder:
  linearizable = leader ReadIndex then `waitApplied(ri)`; session =
  `waitApplied(token.appliedIndex)`; bounded/eventual = read now.
- `ReplicaEngineReader` — replica reads over the real `Engine` (M4, d183f8d),
  with the in-process cross-node gate proving a FOLLOWER serves a
  linearizable read (a0008dc).

What is missing is purely routing: `queryReplicated` never consults local
replica coverage. The refactor makes the coordinator answer every VShard it
hosts from **local storage** behind the mode's barrier, and fall back to
leader-partial RPC only for VShards with no local replica. Consequences:

- RF=3 on 3 nodes: **zero data movement**. Wire cost per query becomes one
  batched ReadIndex RPC per remote leader (KBs, only for linearizable), and
  grouped queries should approach the single-node 97 ms plus barrier latency.
- Larger clusters (N > RF): coverage is partial; the remainder is served by
  the *best replica holder* per VShard (not necessarily the leader), which
  spreads read load across all RF holders instead of concentrating it on
  leaders — that is the actual scale-out property.
- Consistency semantics are unchanged per mode; linearizable stays
  linearizable (this is exactly the Phase-6 replica-read design, applied at
  the coordinator). Fail-closed behaviour (`QUERY_INCOMPLETE`, never a silent
  partial or silent staleness) is preserved on every new edge.

The wire-format work (Phase 1) is still required: it is the fallback path for
N > RF clusters, mixed placements during moves, and any VShard whose local
replica is too far behind a bounded-staleness limit.

## 3. Phases

### Phase 1 — Wire diet (no routing change)

Target: grouped partials 102 B/pt → ≤ 24 B/pt; grouped query 566 ms → ≤ 250 ms.

1a. **Method-keyed minimal partial state** in `agg_partial_codec.cpp`.
    Encode only the fields the query's aggregation method folds with, behind a
    **self-describing per-payload format tag** (decoder never depends on
    out-of-band request state):
    - `avg`/`sum`: sum + Kahan `sumCompensation` + count. The compensation
      term is NOT droppable — dropping it perturbs last-ULP results against
      the pinned Kahan/NaN/Inf contracts (docs/nan_policy.md).
    - `min`/`max`/`count`: single accumulator (+count where the method needs
      it).
    - `first`/`latest`: value + timestamp.
    - `spread`: min + max.
    - `stddev`/`stdvar`: count + mean + m2.
    - `median` (`collectRaw`): rawValues, unchanged — it is irreducible and
      already bounded to EXACT_MEDIAN queries.
    Decode reconstructs an `AggregationState` with **documented identity
    fills** for unsent fields; a per-method audit must prove no downstream
    reader (merge, finalize, statistics) consumes an unsent field. Add a
    per-method cluster-vs-single equivalence test including NaN / ±Inf / -0.0.

1b. **Version-gate the new format.** `kNegotiateVersion` exists ("throws if
    incompatible") — confirm it is enforced on the data-plane connection
    before `kQueryNode`, and gate emission of the tagged format on the
    negotiated version so a rolling upgrade fails closed (old node rejects the
    tag) instead of misparsing. Mixed-version test required.

1c. **Delta-varint the timestamp keys.** Grouped payload keys are sorted
    per-group timestamps at fixed cadence; delta+varint takes the 8-byte key
    to ~1-2 bytes typical.

1d. **Parallelize coordinator decode.** Per-node payload decode currently runs
    serially on one shard; distribute payloads across coordinator shards
    (`submit_to`), chunked with yields (reactor-stall gate applies).

Gates: full unit suite; `qprofile.sh cluster` shows B/pt ≤ 24 and decode
overlapped; `qslow.sh` both modes; mixed-version fail-closed test;
`AggregationStallBench*`.

### Phase 2 — Local-first replica reads (the architectural change)

Target: RF=3/3-node grouped ≤ 1.5x single node (~150 ms); flat ≤ 1.3x.

2a. **Replica coverage map.** Coordinator computes, per VShard, whether it
    hosts a replica (leader OR follower) with an initialized group. Split the
    query: `localSet` (host locally) vs `remoteSet` (fallback to Phase-1
    leader-partial path). During moves/joins a VShard not yet promoted locally
    goes in `remoteSet` — correctness never depends on placement stability.

2b. **Batched read barrier.** New verb `kLeaderReadIndexBatch`:
    `(vshard list) → vector<(vshard, readIndex)>`, one RPC per distinct
    remote leader (the existing per-VShard verb would mean up to ~1365 RPCs).
    Then `waitApplied(ri)` on each local group — O(1) when already caught up.
    Mode ladder identical to `ReplicaVShard::read`:
    - linearizable → batch ReadIndex + waitApplied;
    - session → `waitApplied(token.appliedIndex)`, no RPC;
    - bounded staleness → local read with lag check;
    - eventual → local read immediately.

2c. **Local execution.** Run the existing local partial path
    (`queryLocalPartials`) over `localSet` — same code that answers a
    leader's own VShards today, so shape/semantics rules (aggregate ACROSS
    SERIES at equal timestamps; `QUERY_INCOMPLETE` on any unreadable series)
    are inherited, not re-implemented.

2d. **Failure semantics.** Barrier RPC failure to a dead leader routes into
    the existing unreachable-leader handling (wake `wakeFollowersOf`, return
    `QUERY_INCOMPLETE`, retryable). A local group behind the barrier that
    cannot catch up within the query deadline is `QUERY_INCOMPLETE`, never a
    stale answer under linearizable/session.

Gates: re-run the M4 follower-linearizability harness through this path;
node-kill read availability (killgate2 successor — expect barrier failure to
convert to re-election + fast retry, not a 50 s outage); RF=3 3-node
`qslow.sh` hits targets; all-modes-correct through `ClusterDataPlane`.

### Phase 3 — Replica load-balancing for N > RF

Target: read throughput on a fixed dataset scales with added nodes.

3a. **Remote-side freshness gating in `queryNode`** (the known M4 remainder):
    a non-leader replica holder runs the Phase-2 barrier ladder before
    computing partials, so *any* holder can serve a `NodeQueryRequest`, not
    just the leader.

3b. **Holder selection.** For `remoteSet`, choose among the RF holders per
    VShard with `ReplicaSelector` (hedge/retry/exactly-once already proven),
    grouping VShards to minimize node fan-out and biasing by load/lag
    telemetry (M5 selector stats). Leaders stop being a read bottleneck.

3c. **Consistency plumbing.** Session tokens and bounded-staleness limits ride
    `NodeQueryRequest` so the remote barrier can enforce them; responses carry
    applied-index for token advancement.

Gates: partitioned RF=1 3-node still works (no local coverage, pure remote
balancing); RF=3 with N=4+ in-process nodes spreads reads across followers
(assert via per-node query counters); linearizability harness against a
follower-served remote read.

### Phase 4 — Coordinator merge/finalize scale-out

Once the wire shrinks, coordinator-side CPU is the next ceiling (merge 53 ms +
finalize/build + 29 MB JSON).

4a. Distribute the grouped merge across coordinator shards (partition by group
    key hash), then a cheap final combine; stall-gated.
4b. Stream response building with yields; avoid materializing the full JSON
    for large raw responses.
4c. Confirm/assert every node is an equal coordinator (client can query any
    node; jest round-robin test).

Gates: `AggregationStallBench*`; grouped merge time scales down with `--smp`;
jest suites green against a cluster with round-robin targeting.

### Phase 5 — Acceptance benchmarks + baselines

- `qbench.sh` 34-query set, cluster vs single, 5 reps: record the new table
  next to the Jul 25 baseline; acceptance = flat ≤ 1.3x, grouped ≤ 1.5x.
- Node-down read behaviour: first query ≤ 1 `QUERY_INCOMPLETE`, recovery ≤ 10 s.
- Full five-suite run (unit, perf, socket, jest x2).
- Update perf memory with the new canonical numbers.

## 4. Explicitly out of scope

- TLS / cert rotation (excluded by standing directive).
- SSE/streaming cluster wiring (M6) — Phase 3's remote gating is designed not
  to preclude it.
- Result caching layers — revisit only after locality + wire diet land, they
  attack the same bytes.

## 5. Risks

- **Identity-filled decode states** (1a): a downstream reader of an unsent
  field silently gets wrong values, not a failure. Mitigation: per-method
  audit + per-method equivalence tests before enabling.
- **Version skew** (1b): without negotiation gating, an old node misparses the
  tagged format. Fail-closed is mandatory.
- **Barrier vs hibernation** (2b): batched ReadIndex against hibernating
  followers must not force full-rate ticking cluster-wide; `waitApplied` on a
  caught-up group is O(1) and needs no wake.
- **Placement churn** (2a/3b): coverage computed per query; a VShard mid-move
  falls back to the leader path. Never assume stability across queries.
- **Bounded staleness needs a lag signal** (2b/3c): use applied-index age, not
  wall clocks across nodes.
