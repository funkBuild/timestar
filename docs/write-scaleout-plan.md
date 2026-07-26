# Cluster Write Scale-Out Plan

Status: Phases 1, 2, 3 and 4 IMPLEMENTED (Phase 4: 2026-07-26); Phases 5-6 planned.
Branch `cluster-design`.

Phase 4 result: **[D6] is closed on the measurement it was defined by.** The canonical
bench's rejected-run rate went 2-in-6 (Phase 2) -> 1-in-9 (Phase 3) -> **0 in 8**, with
median throughput 5.34 M pts/s and p50 82.7 ms (Phase 3: 4.98 M / 93.5 ms), and no run's
MAX latency exceeded 173 ms -- i.e. no request came close to burning the 1.5 s retry
deadline, which is what the one rejected Phase-3 run did at 1471 ms. More importantly the
window is no longer measured statistically at all: `test/cluster_gates/fault_injection_
gate.sh` injects the actual fault (147 rounds of TCP RSTs destroying ~400 peer connections
mid-bench) and the same tree with ONLY the three 4a files reverted fails it 9+1 errors to
0. See "Phase 4 gate outcomes" below.

Phase 3 result: the availability phase, not a throughput phase — and it did not
move throughput (median 4.98M vs Phase 2's 5.06M, flat within noise), which is
the expected shape for work that changes what happens when routing is WRONG.
What it changed is the answer a client gets: a deposed-but-alive primary went
from ~29% HTTP 500 to 0, a 2216-transfer leadership rebalance under sustained
writes now costs latency instead of errors, overload degrades to 503 +
Retry-After instead of unbounded queueing, and every retryable cluster failure
is a 503 naming its cause rather than an opaque 500. Ack-at-commit (3c) was
measured and CLOSED — the commit→apply gap is 0.21 ms p50 against a 31 ms
commit latency. The [D6] collapse window is narrowed but alive (1 rejected run
in 9, against 2 in 6): Phase 4a is still required. An adversarial review round after
implementation found and closed one CRITICAL ack-without-commit regression introduced by
3a's own reject-list contract, plus four majors -- see the Phase 3 gate outcomes.

Live gates now live in `test/cluster_gates/` and FAIL (exit 1) rather than print
warnings, including an anti-vacuity assertion on `transfers_initiated`.

Phase 1 result: RF=3 3.98M -> 4.55M pts/s (+14%), median batch latency 129ms -> 104ms,
shard 0's RAFT_PROFILE outlier ratios cut 2-6x (persist 3.6x -> 2.06x, in_lock 17x ->
2.79x, apply 14x -> 2.39x, commit 4.1x -> 1.62x of the other shards). The 5.5M target
was NOT reached and in_lock/apply are still slightly outside the "within ~2x" gate --
the remaining levers are Phase 2 (route/copy/encode once) and the Raft listener, which
has the same non-distributing-listener defect Phase 1b fixed for the data plane (see
below). The write-collapse HTTP 500 bursts [D6] reproduce identically on the
pre-Phase-1 binary and remain for Phase 4.

Phase 2 result: RF=3 4.55-4.61M -> **5.03-5.09M pts/s** (median of 4 accepted runs
5.06M, +10%), median batch latency 102ms -> **92ms**. **The shard-0 profile outlier is
GONE**: in_lock 2.79x -> 1.09x, apply 2.39x -> 1.00x, persist 2.06x -> 0.82x, commit
1.62x -> 1.03x of the other shards -- the "within ~2x" gate now passes on every stage,
and the single lever that did it was 2-pre (the Raft listener, the last funnel onto
shard 0). 2a/2b (hash once, split once) and 2c (bulk codec) each measured flat within
run-to-run noise, which is the expected shape of a system that is NOT CPU-bound (§1):
they remove real work from the critical path but the limiter is elsewhere.

Phase 2 did NOT reach 5.5M, and p50 is 92ms against the 60ms target. With shard 0 no
longer an outlier and CPU still ~80% idle, the remaining wait is the quorum round
itself and the ack path -- i.e. Phase 3 (leader hints, bounded retry, ack-at-commit
evaluation, backpressure) and Phase 5b (per-shard fsync coalescing, disk-only), not
more routing surgery. The [D6] collapse window is still live: 2 of 6 canonical runs
took 2-3 HTTP 500s (Phase 4).

Codec status after 2c: `decodeWriteBatch` reads both v1 and the new v2 (delta-varint
timestamps) forever; the DATA-PLANE wire emits the version negotiated with each peer,
but the RAFT command path -- and hence the journal -- still emits v1 unconditionally,
because a log entry is replicated to voters that never take part in the pairwise
data-plane handshake. Raising it needs the cluster-wide gate group-0's committed format
activation (`activeFormatVersion` / `features::FeatureGate`) already provides; wiring
that to the codec is the one piece of 2c left open, and until then no v2 byte can reach
a journal. Journal back-compat was gate-proven end to end (old binary writes, kill -9,
restart on the new binary, all data readable, zero undecodable entries).

Correction to a premise used in §4-1b and in commit b98c1d1: this seastar hardcodes
`posix_reuseport_available() { return false; }`, so "every shard listens on the port
with SO_REUSEPORT" does not hold. Shard 0 owns the only socket and hands each accepted
fd to the shard the listen options name, so a per-shard listener distributes only under
`connection_distribution` -- `set_fixed_cpu` keeps everything on shard 0. Phase 1b
fixed this for the data plane; the Raft transport still pins.
Companion to `docs/query-scaleout-plan.md`, `docs/clustering.md`,
`docs/clustering-integration-plan.md`.

## 1. Problem

Measured (canonical bench: fresh dirs, leadership converged, 100x10k,
hosts=1000, conns=8, `--smp 4`):

| topology                    | throughput   | note                        |
|-----------------------------|--------------|-----------------------------|
| single node, non-clustered  | 11.6M pts/s  |                             |
| RF=1, 3 nodes, partitioned  | 7.75M pts/s  | @44ms, node CPU ~17-35%/400 |
| RF=3, 3 nodes               | 3.83-4.03M   | @155ms, after Jul 25 fixes  |

The controlling fact (measured, do not re-litigate): **RF=3 is NOT CPU-bound**.
~80% of every node's CPU is idle at peak, and client latency grows linearly
with added connections (151→312→618 ms) while throughput stays flat. The
limiter is WAITING on serialization points, not compute. The
`TIMESTAR_RAFT_PROFILE=1` profiler found and killed two of them already
(awaited cross-shard Raft send `5097499`; shard-0 Raft transport `b98c1d1`);
after those, shard 0 remains the outlier (persist 132 ms / apply 53 ms /
in_lock 42 ms vs 3-8 ms on shards 1-3) because the **data plane** — not Raft —
still funnels through it.

## 2. Code review: where a batch actually waits

The full chain for one HTTP 10k-point batch at RF=3, with the defects marked:

1. **HTTP shard** parses JSON, builds `WriteBatch` → `clusterWriteHook` does
   `smp::submit_to(0)` (timestar_http_server.cpp:554). [D1] The entire batch
   crosses cores to shard 0 regardless of where it arrived.
2. **Shard 0** `ClusterDataPlane::writeReplicated` (cluster_data_plane.cpp:384)
   hashes every series (`SeriesId128::fromSeriesKey` + `virtualShard`), groups
   by owning shard, then dispatches each slice with `co_await` **inside the
   loop** — serial. [D2] Batch latency = SUM of shard slices, not max. Same
   defect in `proposeBatch` (cluster_data_plane.cpp:409), the peer-ingress
   path.
3. **Owning shard** `ReplicatedBatchWriteRouter::write` re-hashes every series
   (again) to group by leader node. [D3] Local slices → concurrent per-VShard
   proposals (already fixed, replicated_vshard_host.cpp:89). Remote slices
   (~2/3 of data on a balanced RF=3/3-node cluster) → `client_.proposeWrite`
   via `ShardRoutingNodeTransport`, which is `submit_to(0)` back to the single
   `DataPlaneRpc` on shard 0 (shard_raft_plane.hpp:43-80). [D4 — the funnel]
   Every cross-node byte transits shard 0's queue in BOTH directions, on both
   nodes.
4. **Wire codec** `write_record.cpp` encodes byte-at-a-time `push_back` with
   no reserve (~160k push_backs per 10k-point slice) and bounds-checks per
   byte on decode; each proposal pays 1 encode + 3 decodes (journal replay
   aside). [D5] Not the cap (CPU is idle) but pure latency on the critical
   path.
5. **Raft layer** is in good shape: group commit with deferred drain
   (raft_group.cpp `proposeAndAwaitApplied` — last waiter flushes everyone
   with one fsync), fire-and-forget sends, per-shard transports with
   SO_REUSEPORT, owning-shard decode. Not the target.
6. **Peer failure handling**: `DataPlaneRpc::clientFor` replaces dead clients
   but during the 200 ms `kReconnectBackoff` deliberately **fails fast on the
   dead connection** (dataplane_rpc.cpp:159-170) — and the router propagates
   that straight to the HTTP client as a failed write. [D6] This is the
   signature of the open intermittent write collapse ("connection is closed"
   bursts, no process ever exited): one transient TCP reset under load turns
   into a window of client-visible 5xx and collapsed bench throughput.

## 3. The refactored model: shard-local writes, node-symmetric planes

Principle: **a write should never rendezvous on a core its data does not live
on.** The request shard splits once, talks straight to owning shards and — for
remote slices — straight out its own per-shard peer client, mirroring exactly
what `b98c1d1` did for the Raft transport. Shard 0 keeps the control plane
(group 0, placement, movement) but disappears from the data path.

HA invariants that must survive every phase (these are the contract, restated
from M3/Phase-5 gates):

- Ack ⇒ durable quorum commit (Raft journal fsync on a majority). Never ack
  from memory.
- No silent partial batch: any slice failing fails the whole write with a
  retryable error.
- Failover correctness: not-leader → retry against the advanced leader map;
  re-application is idempotent (revision = log index, LWW, ADR 0003).
- Fail-closed on unassigned/leaderless VShards.

## 4. Phases

### Phase 1 — Concurrency + funnel removal (routing only, no format changes)

Target: shard-0 profile converges to the other shards; latency stops growing
linearly with connections; RF=3 ≥ 5.5M pts/s.

1a. **Parallelize the scatter loops** [D2]: `writeReplicated` and
    `proposeBatch` dispatch all shard slices concurrently (await-all,
    first-error, all-must-commit semantics preserved — same pattern
    `proposeBatch` in replicated_vshard_host.cpp:89 already uses).
1b. **Shard the data-plane RPC** [D4]: per-shard `DataPlaneRpc` peer clients
    and SO_REUSEPORT listeners on the data-plane port, mirroring the Raft
    transport work (`b98c1d1`). Ingress delivers to the owning shard by
    peeking the slice's shard/vshard header before decode (mirror `0520f07`).
    `ShardRoutingNodeTransport` is deleted the way
    `ShardRoutingRaftTransport` was.
1c. **Write from the request shard** [D1]: `clusterWriteHook` stops hopping to
    shard 0; the request shard splits the batch by owning shard and
    dispatches directly (local slices `invoke_on` the owner; remote slices go
    out this shard's own peer client after 1b). Control-plane reads used for
    routing (leader map, directory) are already shard-local views.

Gates: full unit + socket suites; `TIMESTAR_RAFT_PROFILE` shows shard 0
persist/in_lock/apply within ~2x of other shards; connection-scaling probe
(latency must flatten); RF=3 node-kill mid-bench gate (no loss / no dup);
"400 OK, 0 HTTP errors" rule on every accepted number.

### Phase 2 — Route once, copy once, encode once  [IMPLEMENTED]

Target: cut per-batch critical-path latency; RF=3 p50 @ conns=8 ≤ 60 ms.

2-pre. **Raft listener distribution** (found while implementing Phase 1, fixed here):
    `RaftRpcTransport` pinned its listener per shard, so with reuseport disabled shard
    0 accepted and read ALL inbound Raft traffic and ran every peek/route hop while
    shards 1..N held accept promises that never resolved. Same fix as 1b
    (`connection_distribution`), gated on the same `perShardListener` flag so the
    single-instance users keep the pin. THIS is what closed the shard-0 profile gap.

2a. **Hash once** [D3]: compute `SeriesId128`/`virtualShard` at HTTP parse and
    carry `vshard` on the series ref; every later grouping becomes an integer
    bucket instead of a re-hash of the series key string (currently ≥3 hashes
    per series per write).
2b. **Split once**: one pass at ingress produces per-(target node, owning
    shard) slices; drop the intermediate regroups (shard 0's by-shard map,
    router's by-leader map) that each move every series' strings.
2c. **Bulk codec** [D5]: `write_record.cpp` Writer/Reader move to reserve() +
    per-field appends/bounds-checks (mirror of the agg-partial codec fix
    `2aa909d`); timestamps delta-varint within a series (they are
    monotone per series on the canonical path). Wire format version-gated via
    `kNegotiateVersion` exactly as the query plan's Phase 1b — mixed-version
    clusters must fail closed, not misparse.

Gates: byte-equal apply results vs old codec (round-trip test per value
type, NaN/±Inf/-0.0 included); RAFT_PROFILE in_lock; mixed-version
fail-closed test.

Gate outcomes: v1 encoding proven BYTE-IDENTICAL to the preserved pre-2c writer
(the contract that makes it safe to touch a path whose output is already on disk);
round-trip per type in both formats incl. NaN/±Inf/-0.0/int64 extremes/non-monotone
timestamps; legacy bytes decode on the new reader; socket test proving a forwarded
write speaks the negotiated version (v2 with a current peer, v1 with a peer pinned
to {1,1}, fail-closed with NOTHING applied against a peer with no overlapping
range); live journal back-compat (write on 8514eca at RF=3 incl. 2M bench points,
kill -9 all three, restart on the Phase-2 binary -> 200/200 points readable on every
node, 0 undecodable/fail-stop entries); RAFT_PROFILE within 1.1x on every stage;
node-kill mid-bench (bounded errors, full catch-up) and kill -9 of the whole cluster
(all acked writes survive).

### Phase 3 — Ack path and retry semantics (latency + availability)

3a. **Leader hint on not-leader** (the documented v1 limitation in
    replicated_write_router.hpp): a `false` propose carries the actual leader
    ID back, so the retry goes to the right node instead of spinning on a
    stale placement primary. Removes the one uncovered failover case
    (alive-but-deposed primary with no map change).
3b. **Coordinator-side bounded write retry**: on not-leader or transport
    error, re-resolve leaders and retry (bounded attempts + deadline), the
    write-path analogue of the query path's `kLeaderRetries` — converts
    routine leadership-transfer windows from client 5xx into a latency bump.
    Idempotency makes the retry safe: revisions are stamped once at propose
    (log-index LWW), and a retried batch that already committed re-applies as
    a no-op overwrite.
3c. **Evaluate ack-at-commit vs ack-at-apply**: **CLOSED — NOT WORTH IT, not
    implemented.** `proposeAndAwaitApplied` resolves waiters at APPLY;
    durability exists at COMMIT, so the theoretical saving is the commit→apply
    gap. Measured on the canonical bench with `TIMESTAR_RAFT_PROFILE=1`
    (3 nodes x 4 shards, 37 profile samples):

    | quantity                                   | p50      | p95     | max     |
    |--------------------------------------------|----------|---------|---------|
    | commit latency per proposal (what a client waits) | 30.95 ms | 54.46 ms | 64.76 ms |
    | **commit→apply gap (apply per drain)**     | **0.21 ms** | 1.63 ms | 1.81 ms |

    The gap is **0.7% of the p50 a client actually waits** — far under the
    ~5 ms bar. Note the profiler's raw `apply=` field is per-proposal-SAMPLED
    but accumulates EVERY drain's apply work on that shard (6-60x more drains
    than sampled proposals, including follower-role applies for groups the
    shard replicates but does not lead), so it reads ~36% of commit latency and
    is the wrong denominator; a single waiter's gap is one drain's apply, which
    is the row above. Acking earlier would buy ~0.2 ms and cost the
    read-your-writes story (session tokens ride `appliedIndex`), so it is
    closed rather than deferred.
3d-scope. **What the in-flight bound actually covers, honestly.** `WriteAdmission` is
    charged in `writeSlicesToOwningShards`, i.e. on the REQUEST shard of the RF>1
    replicated path only. It does NOT cover:
    - **peer ingress** (`ShardRaftPlane::proposeBatch{,Hinted}`): a batch forwarded by
      another node is bounded only by `rpc::resource_limits`, which caps a single frame
      (~10.67 MiB) and total estimated in-flight RPC memory, not this node's write
      pipeline. On a 3-node RF=3 cluster ~2/3 of all replication arrives this way, so the
      majority of write memory is outside the budget;
    - **RF=1** (`NodeWriteRouter`), which has no bound at all.
    Extending it to both is deliberately deferred: the ingress side needs the charge to
    be released on the SERVING shard rather than the owning ones, which is a different
    accounting shape, and doing it half-way would give a number that looks like a
    node-wide bound while being a third of one. Until then, read the bound as "what this
    node ORIGINATES", not "what this node holds".

3d. **Backpressure**: bounded in-flight bytes per shard on the data plane
    (the Raft send gate `8192` exists; the data plane has none) surfacing as
    HTTP 503, wired to the existing single-node 503 backpressure convention —
    overload must degrade to explicit pushback, never to timeout storms that
    look like the collapse bug.

Gates: rolling leadership rebalance under sustained writes → zero client
errors; deposed-primary test (transfer leadership away, no placement change,
write must succeed via hint/retry).

Gate outcomes (2026-07-25):

- **Deposed primary — THE discriminating gate.** At RF=3 on THREE nodes every
  node hosts every group, so the router's `LeaderResolver` always knows the
  real leader locally and the stale-primary path is UNREACHABLE; a 3-node
  rebalance storm passes on the pre-Phase-3 binary too. At RF=3 on FIVE nodes a
  coordinator hosts only 2458 of 4096 VShards and falls back to the placement
  primary for the rest, most of which are deposed once leadership balances.
  300 writes to such a node: pre-Phase-3 **207-216/300 accepted (~29% HTTP
  500)**; with 3a+3b **300/300, 0 5xx**. This gate is also what caught the
  advertised-version bug (`b596f2d`) that left the whole hint path dead in
  production with a green suite.
- **Rolling rebalance under sustained writes:** node 3 joined a converged
  2-node cluster (so nodes 1-2 held ~2048 leaderships each against a fair share
  of 1365), then all three nodes were stormed with
  `rebalance-leadership?max=512` for the whole bench. **2216 leadership
  transfers initiated mid-bench, 600/600 requests OK, 0 HTTP errors**,
  4.78 M pts/s, leadership converged to [1364 1368 1364]. (A first version of
  this gate storming an ALREADY-balanced cluster was vacuous — the endpoint
  only hands away leadership held above fair share, so it initiated zero
  transfers. The script now records `transfers_initiated` and says so.)
- **Backpressure** (`test/cluster_gates/backpressure_gate.sh`): at a deliberately
  small 1 MB/shard budget, 16 concurrent batches → **7 x 503, every one carrying
  `Retry-After`, 0 x 500**; a 12-connection bench → 200/200 rejected with the
  budget named in the message ("shard write buffer full (… of 1000000 bytes in
  flight)"), **0 server-side 500s, 0 timeouts, 0 crashes**, and a single small
  write still succeeds throughout. Restarted at the **default** 32 MiB budget the
  same cluster runs the canonical bench clean: **200/200 OK, 5.12 M pts/s, zero
  admission rejections**.

  Sizing this gate is not arbitrary and the script says so: the budget is charged
  on the REQUEST shard (whichever the HTTP connection landed on), so four
  concurrent probes across four shards never collide — sixteen do, by pigeonhole.
  A probe must also be one SERIES (a multi-series batch splits its charge across
  shards) and under the handler's own batch-entry cap (a 160k-entry request is a
  400, not a 503). And recovery cannot be shown by lowering the load on the
  artificial budget, because the bench pipelines several batches even at
  `--connections 1`; it is shown at the default budget instead, which is the
  number an operator actually needs.
- **Node kill mid-bench:** 277 OK / 23 bounded 503s (`last: transport`, after
  the retry budget against a genuinely dead node), restart → full catch-up
  (`peer_caught_up` == `vshards_led` on all three). **kill -9 of the whole
  cluster:** 200/200 acked points readable on every node after restart.
- **Canonical bench (9 runs, 8 accepted):** median **4.98 M pts/s**, p50
  **93.5 ms** — Phase 2 was 5.06 M / 92 ms, i.e. flat within a run-to-run
  spread of 4.33-5.13 M. **Rejected-run rate 1 in 9, against Phase 2's 2 in 6**:
  3b's transport retry absorbs part of the [D6] window, but not all of it — the
  one rejected run showed max latency 1471 ms, i.e. requests that burned the
  full 1.5 s retry deadline and then failed. That is the deliberate trade: in a
  real transport outage a write now blocks up to the deadline instead of
  failing fast, so [D6] costs latency where it used to cost 500s. Phase 4a is
  still required.
- **Adversarial review round (post-implementation), 5 defects fixed:**
  - **CRITICAL, a Phase-3 regression:** `ProposeOutcome` was a REJECT-set, and
    `ReplicatedVShardHost`'s membership check answered with a reject list that was a
    strict SUBSET of what it failed to commit (it proposes nothing, but named only the
    not-hosted VShards). The router derived "uncommitted == rejects" and acked the
    rest — **ack-without-commit**, demonstrated at 60 VShards in a batch with 1 ever
    proposed. Reachable in production because `ClusterDataPlane::start` opens the
    data-plane listener before instantiating the groups, so a joining or restarting node
    serves proposes for seconds while hosting only some of them. Fixed at BOTH levels:
    the host now names every group in the view, and the contract is inverted to a
    **committed-set** (everything dispatched is uncommitted unless explicitly named
    committed), which also disarms a peer naming VShards outside the view. Pinned by
    `ReplicatedBatchWriteRouterTest.StrictSubsetRejectNeverAcks`.
  - `kShardStoppingError` was a bare `runtime_error`, so the `{"writes":[...]}` path did
    not recognise it and reported it as HTTP 200 `"partial"` — a silent partial batch. It
    is now `data::ShardStoppingError`, which also removes the classifier's string match.
  - The 1.5 s deadline was checked only BETWEEN attempts and no RPC carried a timeout, so
    a black-holed peer could hold one attempt (and its in-flight-bytes charge) for
    minutes. Attempts now carry a deadline into `seastar::rpc`'s time_point overload,
    handshake included.
  - `UnassignedVShardError` was mapped to 503 + Retry-After while
    `isRetryableWriteFailure(Unassigned)` is false; now a 500-family status matching the
    enum.
  - `std::stoull` parsed `TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES="32MiB"` as **32 bytes**
    and `"-1"` as SIZE_MAX; strict `from_chars` with full-consumption, and the effective
    budget is logged at startup.
- **Leadership balancing at RF < N looks wrong, but the measurement that prompted
  this was confounded — flagged, NOT concluded (for M5 / the movement owner).**
  What is solid is a CODE READING: `ShardRaftPlane::rebalance` computes
  `fair = totalLed / peers.size()`, where `totalLed` counts only the VShards this
  node HOSTS (`host.leaderOf(vs)` returns `kNoNode` otherwise) while `mine` is the
  node's FULL leadership — a node can only lead what it hosts. At RF < N the
  target is therefore scaled by ~RF/N and the actual is not, so every node
  believes it is permanently above fair share. At RF == N (production, and every
  other gate here) each node hosts everything, the two agree, and the arithmetic
  is correct.

  What is NOT solid: an earlier claim in this document that leadership moves
  "~319 VShards every 2 s indefinitely" is WITHDRAWN — it was measured with
  CheckQuorum temporarily enabled, where transfers were failing and being
  retried, so it says nothing about the balancer. Re-measured on an idle 5-node
  RF=3 cluster with no rebalance calls and CheckQuorum off, the background
  balancer moves leadership in bulk on each pass but the deltas SHRINK
  (586 -> 454 -> 258 VShards) from a skewed start, i.e. it is converging, slowly.
  Whether it reaches fair share or settles somewhere short of it was not measured
  long enough to say. Someone should confirm the arithmetic above against a long
  idle run before treating it as a defect.

  The deposed-primary gate is unaffected either way: with CheckQuorum off it is
  300/300 accepted, 0 5xx. (The 274-285/300 results recorded earlier were also a
  CheckQuorum artifact — slow transfers leaving wide mid-transfer windows — not
  balancer churn.)
- **Pre-existing crash found, NOT introduced here (for Phase 4 / the index owner):**
  a `{"writes":[...]}` batch carrying thousands of DISTINCT SERIES, under
  concurrency, segfaults the node. Symbolized: **`roaring_bitmap_add`**, faulting
  on a garbage address (`si_code 1`), immediately after seastar's memory-pressure
  dump ("Too long queue accumulated") — i.e. CRoaring dereferencing a failed
  allocation in the day-bitmap/postings path, which uses `malloc` and cannot
  throw. It is the INDEX path under memory pressure, not the write path: it
  reproduces on the pre-Phase-3 binary (8079fa6) at 40 x 1.3 MB, and it vanishes
  when the same BYTES are sent as few series with many timestamps. The
  backpressure gate's probe payload is deliberately byte-heavy and series-light
  for exactly this reason — a gate must not depend on the thing it is not
  testing.

### Phase 4 — HA hardening: kill the collapse window [D6]

The open intermittent collapse (3 of ~10 runs, 61k-2.08M pts/s, "connection
is closed", no process exit) is consistent with the deliberate fail-fast
during `kReconnectBackoff`. Whether or not it is the whole story, the window
is real and client-visible.

4a. **Don't fail client writes on a backoff window**: a proposeWrite hitting a
    dead-connection fast-fail is retried through 3b (fresh connection attempt
    after backoff, other slices unaffected) up to the write deadline. A peer
    that is genuinely down still fails the write within the deadline —
    fail-closed is preserved, but a 200 ms TCP blip no longer produces a
    burst of 5xx.
4b. **Connection health**: lightweight keepalive/probe on idle data-plane
    connections so half-dead connections are retired proactively, not
    discovered by the first failing write; reconnect with jitter to avoid
    thundering-herd on peer restart.
4c. **Repro + regression harness**: promote `crashhunt.sh`/`loadrepro.sh`
    into a scripted gate that injects the actual suspected fault (drop the
    TCP connection between nodes mid-bench, e.g. via a proxy or socket kill)
    rather than hoping ambient load reproduces it. Assert: throughput dip
    bounded, zero client errors, no loss/dup after recovery.

Gates: fault-injection round (connection reset mid-bench) → 0 HTTP errors;
node kill → errors only for writes whose deadline truly expired; restart
catch-up verified (existing gate).

### Phase 4 outcome (2026-07-26) — IMPLEMENTED

4a. **Retry pacing vs the reconnect window. The retry could not reach the thing it was
    retrying.** `DataPlaneRpc::clientFor` hands back the DEAD client for the 200 ms
    `kReconnectBackoff` after a connection dies -- deliberate, so a burst of writes to a
    down peer costs one dial and not one per write. The 3b retry paused a FLAT 20 ms
    between attempts, so its whole budget (5 pauses = 100 ms) fit INSIDE one such window:
    all six attempts fast-failed against the same dead socket, the transport was never
    asked to re-dial, and a 200 ms blip against a HEALTHY peer became a client 5xx.

    The fix is the extension `WriteFailure` was designed for -- a PACING table beside the
    policy table. `Transport` and `Overloaded` (the two classes whose cure is waiting for
    something to come back) back off geometrically from the same 20 ms base
    (20/40/80/160/320, capped), spanning ~620 ms across the budget: three reconnect
    windows inside the 1.5 s deadline. The leader-shaped classes keep the flat 20 ms,
    because a `NotLeader` retry goes to a DIFFERENT node on the 3a hint and backing off
    would add hundreds of ms of p99 to every routine rebalance for no availability gain.
    A mixed batch takes the MAXIMUM over the classes it saw. Delays are jittered +/-25%.

    The coupling to the transport constant is a `static_assert` against one shared
    definition (`lib/cluster/reconnect_policy.hpp`), not a comment.

    NOTE for whoever tunes these numbers: for a SLOW peer the pacing barely matters --
    `kAttemptTimeout` (600 ms) dominates and both schedules fit ~3 attempts in the
    deadline. The geometric schedule only changes the case the attempts fail FAST, which
    is exactly the dead-connection case. That is why it costs nothing on the rebalance
    gate and everything on the reset gate.

4b. **Connection health.** (i) Reconnect jitter (+/-50%) shared by both transports:
    un-jittered, a peer restart drops N_shards x N_peers connections at one instant and
    every one re-dials at the same instant, fails together, and stays in lockstep for the
    whole outage. (ii) TCP keepalive (5s idle / 2s interval / 3 probes) on every peer
    connection via seastar's own `rpc::client_options.keepalive`, so a flow that dies
    while idle is retired by the kernel rather than discovered by the first write that
    hangs to its attempt deadline. Deliberately NOT an application-level ping verb: that
    needs a timer per peer per shard, a verb in every wire version, and its own timeout
    policy, and cannot beat the kernel to a flow the kernel has already given up on.
    (iii) Peer registration was two independent try-guarded loops with two separate DNS
    lookups, so a hiccup hitting one and not the other left a permanently ASYMMETRIC view
    of a peer (Raft replicating to it while forwarded writes said "unknown peer", or the
    reverse) that nothing ever re-resolved. Now one resolution feeds every plane,
    all-or-nothing, and unresolved peers go on a retry list a periodic resolver drains.

    Deviation from the plan text: the lazy re-resolution lives in `ClusterDataPlane`, not
    in `clientFor`. `clientFor` is synchronous (it returns a raw client pointer) so it
    cannot await DNS, and it can only see the data plane -- fixing it there would have
    left the Raft half of the asymmetry unaddressed.

4c. **Fault-injection gate** (`test/cluster_gates/fault_injection_gate.sh` +
    `tcp_reset_proxy.py`). See the gate README for the topology and the three
    anti-vacuity assertions, each of which a real earlier run of the gate failed.

Gate outcomes (2026-07-26):

- **Fault injection (the new discriminating gate).** 147 reset rounds destroying 392 peer
  connections mid-bench -> **2000/2000 bench requests OK, 200/200 probe writes OK, 0 HTTP
  errors, 0 server-side 500s, 0 crashes**; 94% of the proxied baseline throughput
  retained; all 200 acked probe points readable **on every node**. The same tree with only
  the three 4a files reverted to `ad77cf3` (4b still present) takes the identical storm
  (147 rounds, 400 connections) and produces **9 bench HTTP errors + 1 probe 5xx**, every
  one `"N VShard slice(s) uncommitted after 6 attempt(s) (last: transport)"`. The cost of
  the fix lands where it is meant to: p99 batch latency 121 -> 175 ms, max 170 -> 346 ms.
- **[D6] rejected-run rate: 0 of 8** canonical runs (100x10k, hosts=1000, conns=8,
  `--smp 4`, RF=3 on 3 nodes, leadership balanced first). Throughputs 4.99/5.25/5.42/5.36/
  5.29/5.60/5.33/5.44 M pts/s (**median 5.34 M**), p50 76-93 ms (**median 82.7 ms**), max
  120-173 ms. Zero server-side 500s and zero fence events across the campaign.
- **Rolling rebalance:** 2216 transfers mid-bench, **600/600 OK, 0 HTTP errors**, 4.86 M
  pts/s, leadership converged to [1364 1368 1364] -- identical to Phase 3.
- **Deposed primary (5 nodes, RF=3):** 16613 transfers, **300/300 accepted, 0 5xx, 0
  500s**.
- **Backpressure:** 16 x 503 all carrying `Retry-After`, 0 x 500; 200/200 rejected under
  the artificial 1 MB budget with the budget named; restarted at the default budget the
  same cluster runs **200/200 OK at 5.07 M pts/s**.
- **Node kill mid-bench:** 285 OK / 15 bounded 503s (`last: transport`, after the retry
  budget against a genuinely dead node), 0 server-side 500s; restart -> full catch-up
  (`peer_caught_up` == `vshards_led` on all three: 1364/1368/1364). **kill -9 of the whole
  cluster:** 200/200 acked points readable on every node after restart.
- Suites: **4097 unit tests / 417 suites green** (run from the build root -- two
  source-inspection tests read `lib/...` relative to CWD and fail from anywhere else),
  **30 socket tests green**.

**MEASUREMENT HAZARD, recorded because it cost a run.** These gates are disk-hungry: the
600-batch rolling-rebalance gate writes ~10 GB per node of Raft journal + TSM, and the
fault gate's two 2000-batch runs wrote 27 GB across three nodes. On this box /tmp is a
per-user-quota tmpfs, so exhausting it does NOT look like a disk-full error -- every
`JournalWriter` fences with `Disk quota exceeded` and the cluster degrades into exactly
the shape a write-path regression would produce. A first rolling-rebalance run showed
**579 HTTP errors and 31 k pts/s** and was pure quota artifact; re-run with space free it
was 600/600 and 4.86 M. Check free space before believing a bad cluster number, and grep
the node logs for `Disk quota exceeded` before blaming the code (the D6 script above
asserts on that count for this reason).

4d. **The `roaring_bitmap_add` crash: ROOT-CAUSED as a use-after-free, not an allocation
    failure. FIXED.**

    `getOrLoadDayBitmapForInsert` (and its postings twin) returned a `roaring::Roaring*`
    pointing INTO the cache and let the caller write through it:

        if ((co_await getOrLoadDayBitmapForInsert(key))->addChecked(localId)) ...

    There is no `co_await` between the two, which is why it read as safe. But the pointer
    is computed INSIDE a coroutine that suspends (`co_await kvGet` on a cold key), so
    `co_return &entry.bitmap` and the caller's `->addChecked` run in DIFFERENT reactor
    tasks. In between, any other insert coroutine on the shard can (a) insert a new key
    and REHASH the cache -- a `tsl::robin_map`, OPEN ADDRESSING, holding
    `roaring::Roaring` BY VALUE, so every bitmap moves -- or (b) cross the memtable
    threshold, running `flushDirtyDayBitmaps()` (which clears the `dirty` flag that
    protects an entry from eviction, and calls runOptimize/shrinkToFit) and then
    `trimDayBitmapCache()`, which ERASES the now-clean entry.

    That explains both halves of the reported signature exactly: memory pressure makes
    the trim routine (hence "always right after the pressure dump"), and distinct SERIES
    count -- not byte count -- makes the rehash routine (hence "vanishes when the same
    bytes arrive as few series with many timestamps"). The day-bitmap path lands first
    because it is entered once per series per day per BATCH, where postings is entered
    once per NEW series.

    Silent second half: an add landing on an entry whose dirty flag had been cleared
    during the suspension was never persisted, so the series disappeared from that day's
    discovery bitmap. A wrong answer, not a crash.

    FIX: `addToDayBitmapForInsert` / `addToPostingsBitmapForInsert` mutate INSIDE, after
    re-finding the entry, with no suspension in between, and re-mark the entry dirty.
    Cardinality comes back BY VALUE. Pinned by
    `DayBitmapSourceInspection.NoBitmapPointerEscapesASuspendingCoroutine`, which fails on
    the pre-fix file (verified by reverting it and re-running).

    **The CRoaring allocator hook was considered and RULED OUT, with evidence.** It would
    not have prevented this crash -- the fault is a UAF, not an OOM -- and it cannot
    deliver the "clean `std::bad_alloc`" it promises:

      * `roaring::Roaring::add` is declared `noexcept` (roaring.hh), so a throwing
        allocator hook terminates the process rather than surfacing an error. An abort
        with a message beats a garbage-address SIGSEGV for forensics, but it is still a
        node crash, not the "clean error" the HA bar asks for.
      * Unwinding out of the hook is not even safe: `array_container_grow` assigns
        `container->capacity = new_capacity` BEFORE its `roaring_realloc`, so a throw
        leaves the container claiming a capacity its array does not have -- subsequent
        adds write out of bounds. A hook would trade a diagnosable crash for silent
        corruption.

    If the allocator hook is wanted anyway (for attribution of roaring's allocations to
    seastar's accounting, which is a real and separate benefit), it must be a
    log-and-abort hook, not a throwing one, and that decision should be recorded as such.

**Filed for the index owner (found during 4d, NOT fixed here):**

- **Day-bitmap membership is lost between memory and disk under concurrency + frequent
  flushes.** With a 4 KB write buffer and concurrent inserts, the warm pre-close
  time-scoped count is EXACTLY right (600/600) and the count after close+reopen is short
  by roughly one chunk (559/600, evenly across days). It needs BOTH concurrency and
  frequent flushes -- the same workload run sequentially persists everything, and at a
  64 KB buffer the loss disappears -- so it is in the flush/compaction path, not the add
  path 4d fixed. It reproduces IDENTICALLY on the pre-4d index code. One-command repro:
  `./test/timestar_unit_test --gtest_also_run_disabled_tests --gtest_filter=
  '*DISABLED_ConcurrentInsertsWithTinyWriteBufferLoseDayMembership'`.
- **The READ-side bitmap accessors still escape.** `getPostingsBitmapByKey` /
  `getDayBitmapByKey` still return `const roaring::Roaring*` out of a suspending
  coroutine, with the same rehash/trim hazard. Their declarations now state the
  consume-immediately contract, and the structural test only forbids the MUTABLE escape.
  The clean fix is to make the cache values heap-stable (`unique_ptr<BitmapEntry>`), which
  removes the rehash half for every holder at once; the eviction half needs a pin.

### Phase 5 — Consensus-layer efficiency (after the funnel is gone)

Only now is the Raft layer plausibly the limiter; re-profile first.

5a. **Heartbeat/message batching per (peer, shard)**: 4096 groups ticking
    every 20 ms ≈ 136k msg/s cluster-wide at idle; frame multiple group
    envelopes per RPC send. Follower hibernation already cuts most of it —
    measure what remains under LOAD, where every group with in-flight
    proposals heartbeats at full rate.
5b. **Shard-level fsync coalescing**: each group's drain does its own
    `persistence_.sync()`; a per-shard sync coalescer (one fdatasync round
    serving all groups that drained in the window — same design as the engine
    WAL group commit `9029b6c`) amortizes journal fsync across groups.
    INVISIBLE on tmpfs benches — validate on a real-disk box or accept it as
    disk-only insurance; ordering contract unchanged (send only after the
    coalesced sync covering the entry completes).
5d. **CheckQuorum needs a leader-transfer bypass before it can be enabled.**
    CheckQuorum is the natural way to stop a partitioned leader from accepting
    proposals it can never commit, and Phase 3 tried to enable it for the data
    plane as a belt to the per-write propose deadline. It had to be reverted:
    with it on, LEADERSHIP TRANSFER BREAKS. TimeoutNow lets the transferee skip
    its own lease, but the vote it sends is an ordinary RequestVote and ours
    carries no transfer/force marker, so every other voter -- still hearing the
    old leader -- hits the disruption guard and silently drops the vote without
    bumping its term. Measured: a transfer completing in 0 tick rounds with
    CheckQuorum off needs a full election timeout via term escalation with it on
    (2.5-5 s at the production 20 ms tick, each with a leaderless window). With
    the balancer firing every 5 s over 4096 groups and reads failing closed after
    ~125 ms of leaderlessness, that is a live availability problem, not a corner
    case. The fix is etcd's: a `campaignTransfer` flag on RequestVote that the
    inLease check honours. It is a Raft WIRE FORMAT change with a mixed-version
    hazard, which is why it belongs here and not in a Phase 3 tail fix. Nothing
    depends on CheckQuorum for safety today -- the propose deadline already
    bounds every write and bounds waiter accumulation.

5c. **(Deferred, design-only) VShard:group consolidation**: hosting 4096 Raft
    groups per node is the root of per-proposal and heartbeat overhead; a
    16:1 VShard-to-group mapping would cut it 16x but coarsens movement
    granularity and touches placement, movement, and snapshot machinery.
    Write the ADR with tradeoffs; do not implement inside this plan.

Gates: re-profile with RAFT_PROFILE under saturation; idle-cluster message
rate; no election-storm regressions (hibernation + wake interactions).

### Phase 6 — Acceptance

- Canonical bench targets: RF=3 ≥ 6.5M pts/s (≥84% of RF=1's 7.75M — one
  quorum round of pipelined overhead, not 2x), RF=1 ≥ 9M (its own funnel
  fixes apply too); CPU utilization must RISE with load (the waiting-bound
  signature gone).
- Latency: p50 ≤ 60 ms @ conns=8; adding connections increases throughput
  until CPU saturates, not just latency.
- HA: node-kill, rolling-restart, leadership-rebalance, and connection-reset
  rounds all inside the bench, all "0 HTTP errors" except bounded
  deadline-expired failures during a true node outage.
- Full five suites; update `cluster_write_perf_jul25` memory and
  `final_perf_figures` with the new canonical table.

## 5. Explicitly out of scope

- TLS / cert rotation work (standing directive; mTLS already landed in X1b
  and is untouched by the transport sharding — the per-shard clients reuse
  the same creds path).
- Client-side (bindings/bench) pipelining changes — server-side only.
- The VShard:group consolidation implementation (ADR only, 5c).

## 6. Risks

- **Removing the shard-0 rendezvous** changes memory ownership of batches
  (allocation locality was the reason for `0520f07`); slices must be built on
  the shard that frees them or use foreign-pointer-safe moves.
- **Concurrent scatter (1a) raises peak in-flight memory** (all shard slices
  alive at once, per connection); the Phase-3d backpressure bound is the
  safety valve — land 3d before removing any existing implicit serialization
  if bench RSS grows.
- **Retry-on-blip (4a) must respect the ack contract**: retries only for
  slices that returned not-leader or transport-error WITHOUT a commit; a
  timeout after an RPC was sent is ambiguous — safe here because re-apply is
  LWW-idempotent, but the revision-stamping path must be audited so a retried
  slice cannot be double-stamped with a new revision.
- **Codec change (2c) is a wire+journal format change**: the Raft journal
  stores encoded commands, so old journals must remain decodable (keep the
  old decoder; version byte selects) and mixed-version peers must negotiate.
- **tmpfs blindness**: fsync-side improvements (5b) cannot be validated on
  this box's /tmp harness; do not claim wins there without a real-disk run.
