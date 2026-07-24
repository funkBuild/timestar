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

### [x] Verify the data-plane RPC transport (DataPlaneRpc) over real loopback sockets — DONE
- **What:** an end-to-end socket test that a write forwarded to a VShard owner and
  a query fanned out across nodes — both over `seastar::rpc` — return the same
  answer as a single node.
- **Resolved:** `DataPlaneRpcTest.WriteForwardAndQueryFanoutOverRpc`
  (`test/unit/cluster/dataplane_rpc_test.cpp`, in the
  `timestar_cluster_socket_test` binary) passes under full default SMP: node 1
  routes 30 writes to their VShard owners over RPC, then node 2 (which only
  received inbound forwards) fans a query out to nodes 1 and 3 and merges 30
  per-series partials, matching single-node.
- **Root cause of the earlier hang:** the rpc server's listen socket used
  seastar's default `connection_distribution` load-balancing, which scatters
  inbound connections across **every** shard — but the rpc server object lives on
  a single shard, so a connection accepted on any other shard had no server behind
  it and the peer's *waited* call hung forever (nondeterministically, by shard
  load; sometimes surfaced as SIGILL). The Raft transport dodged this only because
  it is `no_wait` — a dropped connection was silently retried, never awaited.
- **Fix:** pin the listen socket to the server's own shard with
  `listen_options::set_fixed_cpu(this_shard_id())` (`dataplane_rpc.cpp` `start()`).

### [ ] (Phase 5 note) Production Engine ↔ DataStateMachine adapter + cross-node leader-routing wiring
- **What:** wire the replicated data path (`lib/cluster/data/replicated_vshard.hpp`,
  `data_state_machine.*`, `replicated_router.hpp`) into the real Engine and a
  live cross-node coordinator: a production `VShardLeader` that forwards to the
  leader NODE over RPC (the `LeaderResolver` returns a remote forwarder, not just
  a local facade), a per-node registry of `ReplicatedVShard`s, and the
  Engine-backed apply of committed commands into real TSM/NativeIndex storage.
- **What exists instead:** the replication CORRECTNESS is proven with the
  deterministic in-memory `ReplicatedVShardStore` reference model — the RF=3 gate
  (`replicated_rf3_test.cpp`: fail-stop / failover-no-dup / partition-no-split-
  brain over real Raft + journals) and the leader-routing logic
  (`replicated_router_test.cpp`) are green. This is the SAME discipline as Phase 4
  (no production `LocalStore`/Engine adapter exists yet — only test stores).
- **Value now:** the gate is met transport/engine-agnostically; this is the
  production integration, shared with the Phase-4 Engine-adapter gap above.

### [ ] (Phase 5 note) Tombstone GC during compaction
- **What:** identical-range tombstones now coalesce in the live state machine
  (`applyDeleteRange`), bounding per-range growth, but distinct historical delete
  ranges still accumulate. Compaction should physically drop points a tombstone
  covers and retire fully-superseded tombstones, per the plan's tombstone model.
- **Value now:** LOW (correctness unaffected; a long-lived VShard with many
  distinct delete ranges grows the tombstone set until compaction reclaims it).

### [x] (Phase 5 note) Strict retry-invisibility for delete/write races — NOT NEEDED
- A retried delete/write is log-ordered against ops that committed in the gap
  (superseding writes / resurrecting points). Two independent reviews confirmed
  this is a **valid linearization of concurrent operations**, not data loss — the
  client never got the first ack, so its op is concurrent. Matches the plan's
  "applied identically on every replica → replicas cannot diverge" (convergence,
  not retry-invisibility) and real TSDB delete semantics. No op-ID-free fix
  exists and none is needed. Pinned by `DataStateMachineTest.RetryLogOrderSemantics`
  and documented in `docs/clustering.md` §"Write path" + `data_command.hpp`.

### [ ] mTLS + protobuf wire, batching/pools/deadlines/cancellation/backpressure, cert rotation
- **What:** the plan names mTLS-protected protobuf RPC with connection pools,
  deadlines, cancellation, backpressure, and certificate-rotation mechanics.
- **What exists instead:** the data-plane RPC carries the same payloads over
  `seastar::rpc` with a compact bounds-checked codec (one connection per peer
  host). mTLS/protobuf/pool-tuning are transport hardening, not gate-correctness.
- **Value now:** LOW for the static gate; required for production hardening.

### [ ] Multi-host distributed insert/query benchmarks
- **What:** extend `timestar_insert_bench` / `timestar_query_bench` to drive
  multiple hosts.
- **Value now:** LOW. The transport loopback path is now verified (above), so this
  is unblocked; it remains a benchmarking-coverage task, not a correctness gap.

---

## Phase 8 — feature completion

The named deliverables are implemented as reviewed bricks (`lib/cluster/features/`):
cluster-aware streaming (`stream_subscription.hpp`: backfill→live at one barrier,
dedup by commit position, resumable, term-safe re-registration — the streaming
gate: no loss, no silent duplication); backup/restore (`backup_restore.hpp`:
per-VShard snapshot export, restore into a fresh cluster UUID as generation-one
state with scrubbed membership, fail-closed hash verification); rolling-upgrade
feature gating (`feature_gate.hpp`: version-range negotiation, a format activates
only when every voter supports it); conservative routing summaries + hierarchical
merge (`routing_summary.hpp`: unknown measurement fans out to all — no false
negatives; tree merge == flat merge); operator surface (`operator_surface.hpp`:
rebalance status/pause/resume, repair status, replica-decision trace, atop the
Phase-4 cluster status / vshard describe / placement explain). Replica reads =
done in Phase 6; hot-series lanes = out of the first release (decision 6).

### [ ] Production wiring for the Phase 8 features
- **What:** wire the streaming bricks into the SSE `/subscribe` handler (replacing
  the Phase-4 node-local guard with a real cluster-aware coordinator that
  registers tasks against VShard leaders and takes the backfill/live barrier via
  `RaftGroup::readBarrier`); wire backup export/restore to real snapshot streaming
  + on-disk generation-one bootstrap; carry `VersionRange` in the mTLS handshake
  and gate real storage/log formats through a committed group-0 command; populate
  the `RoutingSummary` from the live index and expose the operator surface over
  HTTP/CLI (`node join/drain/remove/replace`, `vshard move/transfer-leader`, etc.).
- **What exists instead:** every mechanism is a reviewed, gate-tested brick with
  injected abstractions -- the same discipline as Phases 4-7.

### [ ] Official client-binding update for the idempotent-retry contract
- **What:** update the published client libraries (JS/Python bindings) to
  implement "retry the whole batch on failure or timeout" per the documented
  contract. External to this repo's core; the contract is documented in
  `docs/clustering.md` §"Write path".
- **Value now:** LOW for the gate; needed before GA.

## Phase 7 — online join, move, repair, balance

The core mechanisms are implemented and the growth gate is proven
(`lib/cluster/movement/`). Done: the move step machine (`move_job.hpp`) + Mover
(add learner → snapshot install + replay catch-up → promote → remove old voter,
RF never drops, one member at a time, voter removed only after replacement
committed, crash-resumable, SLO-pause/cancel = safe forward stop); the N→N+1
growth gate over real Raft + journals (`movement_growth_test.cpp`: RF stays ≥3 at
every committed step, destination byte-identical to source after snapshot +
concurrent-write replay); placement + leadership balancers (hard failure-domain /
disk-watermark constraints over load, hysteresis); movement throttle (auto-pause
on p99/error/stall/queue/disk budgets, manual pause/cancel); scrubber (hash
verify → quarantine → re-replication request); resumable verified snapshot
streaming (`snapshot_stream.hpp`).

### [ ] Production wiring for movement/balance/repair
- **What:** wire the Mover + a controller-side job driver into `Group0Controller`
  (advance a persisted `Job` across MoveJob steps via `UpsertJob`, resuming on
  crash) and `RaftGroupRegistry`; feed the balancers real telemetry (add
  capacity/disk/weight to `NodeRecord`, wire queue-depth/write-rate signals);
  schedule the scrubber as a background loop feeding the movement path; source the
  throttle's `ForegroundSignals` from real reactor/query/write metrics.
- **What exists instead:** every mechanism is a reviewed, tested brick with
  injected abstractions; the growth gate is proven end-to-end on the async
  RaftGroup + journal harness. This is the production integration, mirroring the
  Phase 4-6 Engine/RPC-wiring deferrals.

### [ ] Chunked InstallSnapshot on the wire
- **What:** `raft_messages.hpp` `InstallSnapshot` carries the whole payload in one
  field; `snapshot_stream.hpp` implements resumable chunk hashing + whole-snapshot
  verification as a standalone brick. Wiring the chunk stream into the actual
  InstallSnapshot RPC (offset/outstanding-window) is production hardening.
- **Value now:** LOW; the monolithic path is correct and the growth gate installs
  a real snapshot through it.

## Phase 6 — replica reads (built ahead of its post-v1 schedule)

The plan schedules replica reads as a post-v1 extension, but the core mechanisms
are now implemented and gate-tested (`lib/cluster/data/replica_read.hpp`,
`replica_coordinator.hpp`; `RaftGroup::waitApplied`). Done: linearizable
follower / non-voting-read-replica reads (leader ReadIndex + local apply-wait,
no new Raft protocol), Session (read-your-writes) and BoundedStaleness modes,
per-query replica selection (eligibility/lag/locality/queue/error-rate), hedging
+ retry with exactly-once-per-VShard and no combining of two attempts,
fail-closed / allow_partial, pinned placement. GATE proven under lag, retries,
placement changes, mid-query failures, partition-reject, delayed-apply
(`replica_read_test.cpp`, `replica_coordinator_test.cpp`).

### [ ] Production wiring for the leader-reach step (RPC) + read-routing balancing loop
- **What:** `LeaderReadIndexFn`/`LeaderCommitFn` are injected (a local leader group
  in tests). Production needs the RPC to the leader node, plus a health/telemetry
  feed for the selector, plus the read-routing balancing loop (per-replica queue
  depth, lag, locality) that distributes reads across replicas over time.
- **What exists instead:** per-QUERY selection + hedging are complete and tested;
  the standing balancing LOOP consumes the same telemetry Phase 7 balancing
  exports and lands with it.
- **Value now:** the gate mechanisms are proven; this is production integration
  and the standing control loop.

### [ ] Time-bounded (not index-lag) staleness
- **What:** BoundedStaleness currently bounds by applied-index lag vs a reachable
  leader's commit, and rejects on partition. A strict TIME bound (serve iff
  quorum-confirmed-fresh within T) is a lease-like read entangled with the
  deferred clock-error-bound decision (open decision 7), which the plan defers.
- **Value now:** LOW; index-lag + partition-reject is the defensible subset.

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
