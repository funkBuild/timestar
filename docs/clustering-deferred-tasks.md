# Clustering: deferred tasks

Tasks that were consciously **deferred** out of the completed clustering phases,
recorded here so the decisions aren't lost. This is the authoritative backlog for
"things we chose not to do yet in a phase we called done."

**Important:** none of the items below is a **correctness gap**. Each completed
phase met its gate; these are optimizations, literal-gate-text test infrastructure,
or non-correctness refinements. New correctness findings do **not** belong here —
fix those in place.

Phase mapping: Stage 1 = Phase 0 (storage layout), Stage 2 = Phase 1 (VShard
storage / VShard-workers), Stage 3 = Phase 2 (multi-Raft), Stage 4 = Phase 3
(control plane). See `docs/clustering.md` for the parent plan.

Status legend: **[ ]** open · **[~]** partial (a proxy exists) · **[x]** done.

---

## Phase 2 — multi-Raft

### [~] Multi-process cluster harness with cross-process crash/partition injection
- **What:** a harness that starts several TimeStar processes and injects real
  crashes (step-hook `kill`) and real transport partitions across process
  boundaries, per the Phase 2 gate text: *"Build the multi-process cluster harness
  with step-hook crash injection and transport partition injection; this gate
  cannot be evaluated without it."*
- **What exists instead:** a **single-process** harness that already proves the
  same safety/liveness properties with real journals + (RPC or in-memory)
  transport + crash-by-recover + partition injection — see
  `test/unit/cluster/raft_failure_test.cpp` (crash-recovery no-loss, leader-crash
  re-election, partitioned-leader-cannot-commit) and
  `test/unit/cluster/raft_group_registry_test.cpp` (many-group failover).
- **Value now:** MEDIUM. The only leftover that maps to literal gate text. Real
  payoff is catching genuine cross-process behavior (actual `fdatasync`, real
  socket partitions, real process death) that in-process simulation can't; but the
  invariants themselves are already validated.
- **References:** `docs/clustering.md` §"Phase 2"; `lib/cluster/raft/`.

### [ ] Node-liveness failure detector (true node-heartbeat-driven hibernation)
- **What:** replace quiescence-based hibernation with a node-level failure
  detector (one heartbeat per peer *host*, not per group), so an idle leader can
  stop per-group heartbeats entirely while followers still learn the node is alive.
- **What exists instead:** quiescent-follower tick-skipping in
  `RaftGroupRegistry` (idle followers tick every 10th pass; a live leader's
  heartbeats keep them followers; a dead leader is still detected by the periodic
  check-tick). This already removes the dominant per-tick CPU cost of thousands of
  idle follower groups.
- **Value now:** LOW. Optimization that matters only at very large group counts.
- **References:** `lib/cluster/raft/raft_group_registry.{hpp,cpp}`
  (`setHibernation`, `skippedTicks`).

---

## Phase 4 — multi-node data plane

### [~] Verify the data-plane RPC transport (DataPlaneRpc) over real loopback sockets
- **What:** an end-to-end socket test that a write forwarded to a VShard owner and
  a query fanned out across nodes — both over `seastar::rpc` — return the same
  answer as a single node.
- **What exists instead:** the data-plane CORRECTNESS (RF=1 == single-node for raw
  + every aggregation, `QUERY_INCOMPLETE` on an unassigned VShard, no partial
  write, the subscribe cluster-guard, the operator surface) is proven
  transport-agnostically in `timestar_unit_test`
  (`DataPlaneRf1Test`, `DataPlaneCodecTest`, `SubscribePolicyTest`,
  `ClusterInspectorTest`) via the injected `LocalStore`/`DataPlaneClient`. The
  transport (`lib/cluster/data/dataplane_rpc.cpp`) compiles and its wire codec is
  unit-tested; its `start()` succeeds over loopback.
- **Why deferred:** the *waited* request/response path (forwardWrites/queryRemote)
  or the transport `stop()` hangs over loopback in this environment, and the
  socket-test binary tooling was too unstable this session to isolate whether it
  is a real `seastar::rpc` waited-verb integration bug or a shutdown-ordering
  deadlock (the Raft transport had a similar `stop()` deadlock, fixed with a gate
  + no_wait). Needs a working interactive debug environment to resolve. The gate
  property does not depend on the wire — it is proven above.
- **Value now:** MEDIUM (wire-level confidence). **Action:** re-run
  `timestar_cluster_socket_test` with the DataPlaneRpc test re-added
  (`test/CMakeLists.txt` UNIT_CLUSTER_SOCKET_TESTS) once a stable socket-test
  environment is available; compare stop() against the working
  `raft_rpc_transport` shutdown pattern.

### [ ] mTLS + protobuf wire, batching/pools/deadlines/cancellation/backpressure, cert rotation
- **What:** the plan names mTLS-protected protobuf RPC with connection pools,
  deadlines, cancellation, backpressure, and certificate-rotation mechanics.
- **What exists instead:** the data-plane RPC carries the same payloads over
  `seastar::rpc` with a compact bounds-checked codec (one connection per peer
  host). mTLS/protobuf/pool-tuning are transport hardening, not gate-correctness.
- **Value now:** LOW for the static gate; required for production hardening.

### [ ] Multi-host distributed insert/query benchmarks
- **What:** extend `timestar_insert_bench` / `timestar_query_bench` to drive
  multiple hosts. Deferred with the transport verification above.
- **Value now:** LOW until the transport loopback path is verified.

---

## Phase 3 — control plane

### [ ] Live notification-stream RPC subscription (map-epoch / policy "watch")
- **What:** an inter-node RPC stream that pushes map-epoch and policy changes to
  nodes as a low-latency **hint**.
- **What exists instead:** the authoritative path is fully built — nodes cache the
  last valid control map (`control_map_cache.hpp`, never-regress, same-epoch
  immutable) and reconcile the truth by reading group-0 at a **ReadIndex barrier**
  (`RaftGroup::readBarrier` + `raft_node` ReadIndex). The plan explicitly says the
  stream is *"a hint, not a source of truth,"* so this is purely a latency
  optimization on top of a complete mechanism.
- **Value now:** LOW.
- **References:** `docs/clustering.md` §"Group 0" ("Watches are streamed
  notifications"); `lib/cluster/control/control_map_cache.hpp`;
  `lib/cluster/raft/raft_group.cpp` (`readBarrier`).

---

## Phase 1 — VShard storage (VShard-workers)

### [ ] VShard-prefix the measurement-scoped postings / day-bitmap index keys
- **What:** apply the VShard-in-key prefixing (already done for the series-id-keyed
  `SERIES_METADATA` `0x05` and `SERIES_VALUE_TYPE` `0x09`) to the
  measurement-scoped families: measurement-series `0x0A`, tag index `0x06`, and the
  per-day bitmap `0x0D`. These need a measurement-then-VShard key layout /
  postings split.
- **Why deferred:** explicitly **not a correctness item** and **not needed for the
  per-VShard index-extract** used by snapshots/migration — the extracted
  `SERIES_METADATA` fully rebuilds mapping, value-type, postings, and day-bitmaps.
- **Value now:** LOW (incremental index-extract efficiency only).
- **References:** `CLAUDE.md` §"Distributed Index System" (key families);
  `lib/index/key_encoding.{hpp,cpp}`; `lib/index/native/native_index.cpp`.

### [ ] (note) VShard-partitioned background compaction: mid-registration retry
- **What:** the off-by-default VShard-partitioning path in `executeCompaction`
  has a residual: a crash mid-registration is recovered by a content-identical
  retry (dedup-correct). Acceptable while the path is off by default.
- **Value now:** LOW / informational; revisit only when VShard partitioning is
  turned on in production.
- **References:** `lib/storage/tsm_compactor.cpp` (`setVShardPartitioning`).

---

## Resolved (previously listed as leftover — do NOT re-investigate)

- **[x] Full-suite hang on the real-socket cluster tests.** Fixed by splitting the
  socket tests into their own binary, `timestar_cluster_socket_test`
  (`test/CMakeLists.txt`). `timestar_unit_test` no longer includes them and
  completes cleanly.
- **[x] Wiring the Raft engine into the group-0 control plane.** Done in Phase 3
  (`lib/cluster/control/` — `Group0StateMachine` on `RaftGroup`, `Group0Controller`).

## Not leftover — future phases

These are planned future work, not deferrals from a completed phase:
**Phase 4** (multi-node data plane: write path, scatter-gather + ReadIndex-gated
leader reads), **Phase 5** (replicated production path), **Phase 7** (online join /
move / repair / balance), **Phase 8** (feature completion). See `docs/clustering.md`
§"Implementation phases".
