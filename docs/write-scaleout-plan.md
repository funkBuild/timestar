# Cluster Write Scale-Out Plan

Status: Phases 1-5 IMPLEMENTED; **Phase 6 (acceptance) RUN AND RECORDED, 2026-07-26.
The plan is closed.**

Phase 6 result: **the availability targets are met, the throughput targets are not, and
the reason is measured.** RF=3 sits at **4.91 M pts/s median (6 runs, 0 errors in 6),
p50 95.9 ms**, against a §4 target of 6.5 M and 60 ms that was written before anything had
profiled the limiter. Phase 5 established what the limiter is — the quorum round trip, on
a cluster ~80 % CPU-idle — and this phase's connection probe confirms it directly:
conns 2 -> 16 doubles points-in-flight and doubles latency at each step while throughput
moves ±3 % and CPU stays flat at 17-20 %. Adding concurrency buys queueing, not service,
so no amount of routing, wire or consensus work inside this plan could have reached 6.5 M.
What DID land: all five live gates green in isolation, `kill -9` mid-bench / rolling
restart / whole-cluster `kill -9` all losing nothing and answering with bounded retryable
503s rather than 500s, all five suites green (4107 / 46 / 33 / 184 / 187), and the
single-node non-clustered path un-regressed at 12.38 M. Campaign arc: **3.9 M @ 155 ms
with a ~1-in-3 500-burst rate -> ~4.9-5.3 M @ ~96 ms with 0 bursts in 30+ recorded runs.**
Full numbers, the targets-vs-achieved table, and the complete carried-debt register are in
"Phase 6 outcome" below.

Phase 5 result: **the consensus layer is not the limiter either, and this phase measured
that rather than assuming it.** Re-profiling first (5-pre) found the cluster still ~80 %
CPU-idle at peak with commit latency dominated by the quorum round trip, and the Raft
message rate IDENTICAL idle and under saturation -- so nothing in the phase could have
moved throughput, and nothing did (median 5.18 M pts/s on the final post-review binary,
against a pre-Phase-5 binary re-benched in the same session at 5.16 M). What the phase delivered is correctness:
the read-side bitmap-cache use-after-free is closed structurally (5.1), catch-up appends
are bounded so the Raft admission bound could drop 1 GiB -> 128 MiB (5.4), and a leader
transfer aimed at a dead peer -- which made a group refuse writes FOREVER -- is now
abandoned. 5a shipped as opt-in AFTER a same-session A/B showed it cost ~5 % of
throughput and nearly doubled idle CPU to buy a frame-rate reduction nothing needed; 5b
was NOT implemented because its premise (a shared per-shard journal writer) does not hold
in the current wiring. Two ADRs (0004, 0005) cover 5c and 5d.
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
3d-scope. **What the in-flight bound actually covers, honestly.** UPDATED by debt D-8:
    it now covers BOTH doors of the RF>1 replicated write path, as two SEPARATE
    per-shard budgets (`cluster::AdmissionClass`):
    - **originated** (`writeSlicesToOwningShards`) — charged on the REQUEST shard, as
      before;
    - **peer ingress** (`proposeSlicesToOwningShards{,Hinted}`, reached from
      `ShardRaftPlane::proposeBatch{,Hinted}`) — charged on the SERVING shard, the one
      the peer's connection landed on, which is where the decoded batch and the whole
      fan-out frame live. This is ~2/3 of all replication traffic on a balanced 3-node
      RF=3 cluster and was previously bounded only by `rpc::resource_limits`, which caps
      one frame (~10.67 MiB) and total estimated in-flight RPC memory rather than this
      node's write pipeline.
    Separate rather than shared on purpose: one counter would let a burst of replication
    503 a client's own writes on a node coordinating almost nothing, and the reverse, with
    which side loses decided by arrival order. Separate budgets make each role's headroom
    a property of that role — at the cost that the node-wide ceiling is now the SUM
    (2 x 32 MiB x shards by default), which is why the default is documented per class
    rather than as a node bound.
    Still NOT covered, and this is the honest remainder:
    - **RF=1** (`NodeWriteRouter`) has no bound at all;
    - **Raft's own memory** — the journal append path, the unstable log, and the apply
      queue behind a committed entry — is downstream of admission and bounded by nothing
      here. A batch is released from the ingress budget when its propose returns; the
      entry it created lives on in the log and in every follower's;
    - **InstallSnapshot** (D-5) and query-side memory, neither of which is a write.
    So read the bound as "what this node's write PIPELINE holds, per role", not "what this
    node holds".

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

Review round (2026-07-26, post-implementation), 2 blockers + 5 items fixed:

- **CRITICAL: `startPeerResolver` passed a COROUTINE LAMBDA as a temporary to
  `with_gate`.** A lambda-coroutine keeps its captures in the CLOSURE OBJECT, which dies
  at the end of the full expression while the coroutine is suspended -- and this one is
  ALWAYS suspended, because `unresolvedPeers_` holds hostnames precisely because they did
  not resolve. On resume it read `this` and `replicated` from freed stack; at RF=1 a
  garbage `replicated` reading true dispatches `invoke_on_all` into a
  `sharded<ShardRaftPlane>` that was never started. This is verbatim the hazard documented
  at cluster_data_plane.cpp's own `startLeadershipBalancer`, which obeys it. Fixed the way
  that comment prescribes: a NAMED member coroutine (`resolvePendingPeers`) launched by a
  plain lambda, whose frame the gate keeps alive. Same function: the discarded possibly-
  exceptional future is now consumed via `then_wrapped` + `ignore_ready_future` (leaving
  the flag set would also have silently killed the loop), and `registerPeer` takes its
  address BY VALUE -- callers passed a reference into a member map the same pass rewrites.
- **MAJOR: the second instance of the 4d defect, in `updateTagHLL`** -- see 4d below.
- The retry sleep is clamped to the remaining deadline: a 320 ms + jitter pause starting
  at t=1499 ms slept ~400 ms past the 1.5 s deadline and then dispatched a 6th attempt the
  between-attempts check was guaranteed to reject. The client outcome was already correct;
  the DEADLINE was not.
- The coupling `static_assert` moved to `replicated_write_router.hpp` so it binds to
  `kMaxAttempts` rather than a literal 6 (dropping it to 4 restores [D6] and used to still
  compile -- verified that it now fails to), and it compares the PESSIMAL jittered values
  (retry span x0.75 = 465 ms vs backoff x1.5 = 300 ms) rather than the nominal ones.
- `jitteredDelay`'s RNG was seeded from the shard id ALONE, so node1/shard2 and
  node2/shard2 drew identical sequences -- the CROSS-NODE herd the jitter exists to break
  was not spread at all. Now mixed with pid + steady_clock at first use.
- Keepalive parameters moved into `reconnect_policy.hpp`, shared by both transports
  instead of duplicated literals.

Gate outcomes (2026-07-26):

- **Fault injection (the new discriminating gate).** Re-run after the review round: 148
  reset rounds destroying 400 peer connections -> 2000/2000 + 200/200, 0 errors, 92% of a
  4.99 M baseline. Original run: 147 reset rounds destroying 392 peer
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
    Cardinality comes back BY VALUE.

    **PROOF, and it is behavioural, not textual.** Reverting `native_index.cpp` to
    `ad77cf3` and running the (enabled) concurrent-insert test reproduces the PRODUCTION
    SEGFAULT SIGNATURE -- a fault on a garbage `si_addr` -- on the pre-fix code, and the
    same test passes on the fix. That is the demonstration; the structural test
    (`DayBitmapSourceInspection.NoBitmapPointerEscapesASuspendingCoroutine`) is a
    regression fence on the SHAPE, not the evidence for the diagnosis, and is written that
    way because the fault is a race between reactor tasks that no unit test can schedule
    on demand.

    **A SECOND INSTANCE of the same defect, on the same hot path, fixed in the review
    round:** `updateTagHLL` bound the result of `co_await getPostingsBitmapByKey(...)` to a
    pointer and then iterated it, justified in-code by "the seed loop below does not
    suspend" -- the exact argument this diagnosis refutes, since the loop runs in a
    different reactor task than the `co_return` that produced the pointer. It is the worse
    of the two: `getPostingsBitmapByKey`'s cold-load branch inserts with `dirty = false`,
    i.e. IMMEDIATELY trim-eligible, and the function is reachable from
    `getOrCreateSeriesId` on the hot write path under exactly the memory pressure the
    production crash needed. The seed is now taken BY VALUE after a re-find (a one-time
    copy, only when a tag value first crosses `kTagHllMinCardinality`). The structural test
    pins that shape too.

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

- **An INDEX-FLUSH DURABILITY defect: data that is correct in memory does not survive a
  close+reopen, under concurrency + frequent flushes. Cause NOT yet confirmed; the
  day-bitmap framing below is the symptom, not the diagnosis.** With a 4 KB write buffer
  and concurrent inserts, the warm pre-close time-scoped count is EXACTLY right (600/600)
  and the count after close+reopen is short by roughly one chunk (559/600, evenly across
  days). It needs BOTH concurrency and frequent flushes -- sequentially the same workload
  persists everything, and at a 64 KB buffer the loss disappears.

  It is probably NOT the day bitmaps themselves. The repro uses ONE measurement, so there
  are only four day-bitmap entries; all four are dirty (hence trim-proof) and are flushed
  at close. The drop conditions in `findSeriesWithMetadataTimeScoped`
  (native_index.cpp ~:2755-2778) make a lost `LOCAL_ID_FORWARD` or `SERIES_METADATA` entry
  the likelier cause -- i.e. the loss is in the memtable flush path generally, and the day
  bitmap is merely how it becomes visible.

  Concrete lead for whoever picks this up: `maybeFlushMemTable` clears every `dirty` flag
  in `flushDirtyDayBitmaps` (:707) and then SUSPENDS TWICE (`flushDirtyMeasurementBlooms`,
  `wal_->append`) before `applyTo(*memtable_)` at :712 -- dirty is cleared BEFORE
  durability, and the trims at :715-716 can evict now-clean entries inside that window.
  Separately, the background flush fiber resets `immutableMemtable_` (:738) before
  `maybeCompact` / `refreshSSTables` (:739-740).

  Reproduces on the pre-4d index code as well -- but note that pre-4d code SEGFAULTS on
  this workload shape, so "identical" is not measurable; what is established is that the
  loss is not something 4d introduced. One-command repro on the current binary:
  `./test/timestar_unit_test --gtest_also_run_disabled_tests --gtest_filter=
  '*DISABLED_ConcurrentInsertsWithTinyWriteBufferLoseDayMembership'`.
- **PHASE 5 DEBT (agreed, not to be fixed inside Phase 4): heap-stable bitmap cache
  values + an eviction pin**, which would retire the read-side "consume immediately"
  contract entirely rather than documenting it; and fault-gate hardening (floor raises, a
  scripted A/B against a pre-4a binary, a combined reset+rebalance gate).
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
5d. **PARTLY DONE (debt D-9): the bypass is built and proven; the flag stays OFF this
    release.** `82591a9` (the bypass: a transfer vote under its own message-type byte,
    honoured at the inLease check), `c5bf5b0` (two things this entry and ADR 0005 both
    missed: a forged TimeoutNow could produce a transfer-flagged campaign from ANY peer,
    and hibernation stretched the LEASE tenfold), `77e0631` (a disable-only override).
    An ON build of the final tree passes the 2216-transfer rolling_rebalance storm with
    zero client errors, so the mechanism works; but node_kill_round measures the guard at
    50-59/400 failed batches and an 11-13 s recovery against 32/400 and 7 s with it off,
    same binary and session, so it is not worth enabling yet (D-29). The tag-8 decoder
    shipping now is what makes enabling it later a one-line flip with no mixed-version
    window (D-30). Original entry follows, unedited, because it is the record of why the
    revert was right.

    **CheckQuorum needs a leader-transfer bypass before it can be enabled.**
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

### Phase 5 outcome (2026-07-26) — IMPLEMENTED (5.1, 5a, 5.4); 5b DEFERRED with cause; 5c/5d ADRs

#### 5-pre. Re-profile: what the limiter is now

3 nodes, RF=3, `--smp 4`, canonical bench (100 x 10k, hosts=1000, conns=8), fresh dirs,
leadership balanced first. CPU from `/proc/<pid>/stat` DELTAS (utime+stime), not `ps`
`%cpu`, which is a lifetime average.

| quantity                          | idle (60 s)  | canonical run | sustained (600 batches) |
|-----------------------------------|--------------|---------------|-------------------------|
| node CPU (of 4 cores)             | 4-5 %        | 17-20 %       | 19-21 %                 |
| Raft envelopes out / shard         | 2724 /s      | ~2724 /s      | 2450-2790 /s            |
| Raft envelopes out / node          | ~10.9 k/s    | ~10.9 k/s     | ~10.6 k/s               |
| Raft bytes out / shard             | 0.18 MB/s    | 0.18 MB/s     | 6-41 MB/s               |
| envelopes per RPC frame            | 1.00         | 1.00          | 1.00                    |

RaftGroup stage profile per proposal (node 1 is the bench's only HTTP target, so it is
coordinator AND replica; nodes 2-3 are replicas only):

| stage (per proposal)  | node 1        | nodes 2-3   |
|-----------------------|---------------|-------------|
| commit latency        | 19-66 ms      | 6-30 ms     |
| in_lock               | 9-30 ms       | 3-14 ms     |
| persist (sync only, summed over ALL drains) | 30-111 ms | 4-27 ms |
| apply (summed over all drains)              | 9-48 ms   | 1-17 ms |
| send                  | 0.3-3.0 ms    | 0.3-1.3 ms  |

**Read of the limiter, stated plainly because it bounds what this phase could deliver:
the cluster is still NOT CPU-bound (~80 % of every core idle at peak) and commit latency
is still dominated by the quorum round trip.** Nothing in Phase 5 shortens a round trip,
so no Phase-5 item could have been expected to move throughput, and none did — exactly
the shape Phase 2's 2a/2b/2c produced for the same reason. Three specific findings:

1. **The Raft message RATE does not respond to load at all** — ~2724 envelopes/s per
   shard idle and under saturation. Load makes messages BIGGER (0.18 -> 6-41 MB/s), not
   more numerous. So heartbeat traffic is a *standing* cost, and 5a's premise ("measure
   what remains under LOAD, where every group with in-flight proposals heartbeats at full
   rate") is wrong in a useful way: there is no load-driven increment to remove. What
   there is, is a constant ~10.9 k envelopes/s per node that hibernation has already cut
   to ~2.7 % of the naive 4096-groups-x-2-peers-x-50 Hz figure.
2. **`persist` is the largest stage, on tmpfs, where fdatasync should be nearly free.**
   Dividing by drains gives ~1.6 ms per sync. That is the 5b target — and see below for
   why 5b could not be implemented as written.
3. **Node 1's stages run 2-3x nodes 2-3's.** That is a BENCH artifact (the bench drives
   one node, which is then coordinator and replica), not a code asymmetry; real clients
   spread. Not chased.

#### 5.1 (P0). Heap-stable bitmap cache values + an eviction pin — DONE

The Phase-4 review left the READ-side accessors in `native_index.cpp` returning a
`const roaring::Roaring*` into a `tsl::robin_map<std::string, BitmapEntry>` (open
addressing, entries by value) out of a coroutine that suspends, under a documented
"consume it immediately" contract. **The contract is unsatisfiable and that is the whole
point**: the invalidating event (rehash, or flush+trim) happens BEFORE the caller
resumes, so there is no instant in which a caller could have consumed it safely. Eight
call sites depended on it.

Fixed structurally: the caches now hold `seastar::lw_shared_ptr<BitmapEntry>`, so a
rehash moves 8-byte handles and never an entry (this kills the rehash half for every
holder at once, including ones not yet written), and the handle IS the eviction pin — a
holder keeps its entry alive even if the map drops it. The trims additionally skip
entries whose handle is not `owned()`, which is a COHERENCE requirement rather than a
lifetime one: an evicted-but-held entry would leave a reader and a concurrent writer on
divergent copies of the same key, and the reader's adds would be the ones lost.
`ensureEntry()` replaces `operator[]`, which would insert a null handle.

The structural test moved with the fix: it now pins the cache's VALUE TYPE (revert that
and `co_return &entry.bitmap` is lethal again with no signature change), forbids raw
const pointers as well as mutable ones, requires both trims to consult `owned()`, and
forbids `operator[]` on the caches.

**Perf: no change.** The day-bitmap path is on the hot insert path and the extra pointer
hop was expected to be noise; it is. Canonical bench 4.99/5.17/5.33 M (median 5.17 M)
against 5.06/5.18/5.33 M (median 5.18 M) before, i.e. the same numbers.

#### 5.2 / 5a. Multi-envelope frames per (peer, shard) — DONE

`send()` buffers the encoded envelope per peer and flushes at the end of the reactor
task-queue round (`seastar::yield()`), which is the natural window: a shard's tick drains
~1000 groups in one go. Bounded at 256 KB / 512 envelopes; the added delay is one
task-queue round.

**It works, and it is DEFAULT OFF, because it cost more than it saved.** Batching does
what 5a claimed: idle frames/s per shard 2724 -> ~700 (envelopes per frame 1.00 -> 3.9,
and 12-85 on shards whose peers hibernate in step). But frame count is not a resource
under pressure here, and the per-message buffering that buys it is. A/B on the SAME BOX,
back to back, four canonical runs each:

| binary                          | idle CPU / node | canonical median |
|---------------------------------|-----------------|------------------|
| pre-Phase-5 (`c052253`)         | 4 %             | 5.16 M pts/s     |
| Phase 5, batching ON            | 7-8 %           | 4.90 M pts/s     |
| Phase 5, batching OFF (default) | 4 %             | 5.16 M pts/s     |

A buffer insert, a gate hold and a `yield()`-scheduled flush task per round, at ~2724
messages/s/shard, nearly DOUBLED idle CPU and cost 5 % of throughput. Turning the default
off restores the baseline exactly, on both numbers.

**How this was nearly missed, which is the transferable part.** The first reading of 5a
was "flat within noise" (5.13 M median vs 5.17 M) — and it was taken hours after its
baseline, during which the box drifted (idle CPU on an UNCHANGING idle cluster read 4 %,
then 5 %, then 7 %, then 8 % across the session). Throughput alone could not resolve a
5 % effect against that drift. **Idle CPU with a fixed idle workload is the sensitive
instrument on this box, and a same-session A/B against the previous binary is the only
way to read it.** Any future phase measuring a few-percent effect should re-bench the
baseline binary in the same session, not compare against a recorded number.

OFF means the pre-5a path EXACTLY — `send()` dispatches immediately, no buffer, no
deferred flush — so the default costs nothing rather than costing a disabled feature's
bookkeeping. The capability verb, wire format, fail-closed probe and all three socket
tests stay; `TIMESTAR_RAFT_BATCH_SENDS=1` turns it on for a deployment where the frame
rate IS the binding cost (many more peers, or a syscall-bound node).

**Mixed versions.** The Raft transport has no negotiation — one deliver verb was the
entire protocol — and seastar answers an unknown verb with a reply a `no_wait` sender
IGNORES. A batch frame sent to an old peer would therefore discard every Raft message in
it *silently*. A new verb id is necessary but NOT sufficient; the sender has to know. So
batching is gated on a WAITED capability verb, probed per CONNECTION (a peer can restart
into an older binary at the same address) and fail-closed on any error. Pinned by a
socket test in which the "peer" registers only the original verb and must receive every
message, one frame each.

#### 5.3 / 5b. Shard-level fsync coalescing — NOT IMPLEMENTED; the premise does not hold

**`JournalWriter` is per VSHARD, not per shard.** `ReplicatedVShardHost::addVShard`
creates one writer per VShard directory (`journalRoot/vshard_N/`), so
`JournalRaftPersistence::sync()` -> `writer.barrier()` is a DMA write plus an fdatasync on
**that VShard's own file**. There is nothing to coalesce onto: each group already syncs
alone, to its own fd. The design 5b describes — "one fdatasync round serving all groups
that drained in the window" — requires a SHARED per-shard writer, which is what
`JournalWriter`'s own header claims exists (*"a per-core JournalWriter (shared by every
group on the core, tagged with the group's VShard)"*) and what the wiring does not do.

This was NOT rushed, deliberately: 5b's ordering contract is the sharpest knife in the
phase, and the correct response to "the premise is false" is to say so rather than ship a
coalescer that coalesces nothing.

**What a real 5b would be, for whoever picks it up.** The recovery path already supports a
shared journal: every record carries its VShard tag and per-VShard sequence, and
`recoverRaftState(records, vshard)` filters a core-wide record set by VShard. So the work
is (a) one writer per shard, opened once, its recovered record set reused by every
`addVShard` on that shard; (b) a coalescer with the WAL group-commit shape — a waiter
joins the NEXT round, the round takes the waiter set and calls `barrier()` in the SAME
reactor task (no suspension between taking the set and entering the barrier), so every
waiter's appends provably precede the barrier that satisfies it; (c) journal retention,
which becomes cluster-wide: a segment can only be dropped once EVERY VShard's records in
it are compacted away, where today a VShard's directory can be deleted outright when it
moves. (c) is the real cost, and it interacts with movement.

Also note the profile: `persist` is the largest stage here at ~1.6 ms per sync **on
tmpfs**. The plan's standing caveat ("INVISIBLE on tmpfs benches") turns out to be too
pessimistic — the O_DIRECT padded-block rewrite that every barrier performs is not free
even when the fsync is. A real 5b is likely worth more than "disk-only insurance".

#### 5.4. Chunked catch-up + tightened Raft admission — DONE, and it found a defect

`sendAppend` put `log_.entriesFrom(nextIndex)` — the entire remaining tail — into ONE
AppendEntries, so a follower returning after a large campaign got a message the size of
the backlog. Since the deliver verb is `no_wait`, an over-limit message is DROPPED WITH NO
REPLY, and the leader re-sends the same oversized append forever: a silent, permanent
one-replica-short group. That is why the inbound bound was 1 GiB, i.e. not a bound.

Capping the append side needs no protocol change — a follower acks the prefix, and
`handleAppendEntriesReply` sends the next chunk while `nextIndex <= lastIndex`.
`RaftOptions::maxAppendEntries` (256) / `maxAppendBytes` (1 MiB) bound it, with the first
entry of a chunk always included so an oversized single entry still replicates. Admission
tightened **1 GiB -> 128 MiB**, sized now for the one producer still unbounded:
InstallSnapshot, which carries a whole VShard snapshot and whose chunking is a protocol
change (offset/done, receiver-side reassembly, a resumable boundary) left for the snapshot
owner. A send-side mirror at 96 MiB refuses and LOGS such a message, so the remaining case
fails visibly instead of vanishing.

**A PRE-EXISTING CONSENSUS DEFECT, found by the new gate and fixed here.**
`RaftNode::propose` returns false while `leadTransferee_` is set — correct, a handoff is
in progress. But `handleAppendEntriesReply` clears it ONLY when the target's matchIndex
reaches lastIndex, so a transfer aimed at a DOWN peer left it set **forever**: the group
keeps its leadership, so no election ever rescues it, and it refuses every write
permanently. There was no abort of any kind. Two fixes:

- `RaftNode::tick()` abandons a transfer after one election timeout (etcd's rule).
  Nothing about it is unsafe — the transfer simply did not happen, and the leader must go
  back to accepting writes. `transferElapsed_` restarts per transfer.
- `ShardRaftPlane::rebalance` targets the peer with the largest leadership deficit, and a
  DEAD peer leads nothing, so it is the most attractive target on EVERY 5 s balancer pass.
  It now refuses to target a peer that is not caught up on that group — also the only
  target that transfers immediately (`transferLeadership` sends TimeoutNow at once rather
  than waiting on a catch-up round trip). Without this, the abort above would convert a
  permanent outage into one repeated every balancer pass.

Both are pinned by deterministic unit tests (`raft_transfer_abort_test.cpp`), which is
where the evidence for the fix lives. Neither is a Phase-5 regression: nothing in this
phase touches the balancer, transfer, or propose paths.

**THE ONE-NODE-DOWN 503s: DIAGNOSED (review round, F1), and the first diagnosis in this
document was WRONG in its central inference.** Writing to a 3-node RF=3 cluster with ONE
NODE DOWN produces a run-to-run-variable share of bounded 503s — 50, 91, 106, 107 and
201 of 400 across five runs, and **111/400 on the pre-Phase-5 binary**, so it is
pre-existing and Phase 5 neither caused it nor is credited with fixing it.

**The mechanism, read end to end rather than inferred from where the log lines landed:**

1. `RaftGroup::proposeAndAwaitApplied` returns a bare `false` for BOTH `role_ != Leader`
   AND `leadTransferee_ != kNoNode` — "ask someone else" and "I am the leader and I am
   standing down" are indistinguishable at the call site.
2. The local sink turned that `false` into
   `SliceReject{vs, g->leader(), NotLeader}`. **When this node IS the leader refusing,
   `g->leader()` is ITSELF.**
3. The router accepted the hint, so `nextHints[vs] = self`, so the slice re-bucketed into
   `localView`, so the next attempt asked **the same refusing group again**. Six attempts,
   **no RPC ever made**, and a 503 labelled `not-leader`.

**The earlier inference here — "every rejection on the coordinator and none on the leader,
therefore the write path is proposing to the WRONG PLACE" — does not follow, and it
pointed the next investigator at the wrong layer.** The coordinator *is* the leader in
this scenario; the evidence cannot distinguish "wrong place" from "right place, refusing".
A stale-leader-map hypothesis was also refuted directly: `leaderOf` is consulted fresh on
every attempt, so a stale map cannot survive a retry — only a hint can, which is exactly
what did.

Fixed here (the fix is diagnostic and routing, not consensus) — and note what it does NOT
change: **F1 fixes the LABEL and the wasted local retry loop, not the RATE.** A write
whose group is genuinely mid-transfer or mid-election still fails after the budget; what
changes is that its attempts now re-resolve instead of re-asking the same refusing group,
and the answer names `LeaderRefused` instead of a manufactured `not-leader`. Phase 6
measured the same 503 band afterwards and identified what sets its size: election windows
multiplied by batch fan-out (D-14), which no relabelling touches.

- a distinct `WriteFailure::LeaderRefused`, so "the leader refused, mid-transfer" can no
  longer be reported as "not-leader";
- the sink never emits a `leaderHint` naming itself, and the router **drops any hint that
  names the target that just rejected** — the general rule, which also covers a remote
  peer naming itself. With no hint the next attempt RE-RESOLVES, which is the only thing
  that can change the answer;
- `ReplicatedVShardHost::proposeRefusedWhileLeader()` counts refusals-while-leader, with
  a rate-limited warn naming the VShard, so the condition is visible without a code read;
- the router's unconditional `noteKind(NotLeader)` is scoped to the case where a target
  named no reason at all — the manufactured half of the label.

Pinned by `ReplicatedBatchWriteRouterTest.SelfNamingHintIsIgnored` (verified
discriminating: reverting the router condition fails it) and
`.RefusalReasonIsReportedNotManufactured`. Re-gated live afterwards on the two
transfer/hint-sensitive gates: `deposed_primary` (18507 transfers, 300/300 accepted, 0
5xx, 0 500s) and `rolling_rebalance` (2216 transfers mid-bench, 600/600 OK, 4.98 M pts/s,
converged to [1364 1368 1364]).

The gate therefore hard-asserts what it is FOR — catch-up, zero server-side 500s, zero
crashes, and an anti-vacuity floor on batches accepted — and reports the 503 count as
ADVISORY, the same way `deposed_primary_gate.sh` treats its accepted-write count and for
the same reason.

#### Review round (2026-07-26, post-implementation): 3 fixes, 2 filed

- **F1 — the one-node-down 503 diagnosis was wrong.** See 5.4 above, rewritten. Distinct
  `LeaderRefused` failure class, self-naming hints dropped at the router, a
  refused-while-leader counter, and the manufactured `NotLeader` label scoped to the case
  it was written for.
- **F2 — the transfer-abandon window was re-armable, which defeated it.**
  `transferLeadership(target)` reset `transferElapsed_ = 0` unconditionally, so ANY caller
  re-requesting the same target faster than one election timeout kept the clock at zero
  forever — and the leadership balancer runs every ~5 s against a 2.5-5 s timeout, which is
  exactly that shape. The abort added earlier in this phase was therefore only being saved
  by the balancer's caught-up target filter. Now a repeat request for the transfer already
  in flight is IGNORED (etcd's behaviour) and only a change of target restarts the window.
  The old test named `EachTransferGetsAFreshWindow` ran no ticks after its second request
  and so asserted nothing about the window; replaced by
  `RepeatingTheSameTransferDoesNotReArmTheAbortWindow` and `ChangingTheTargetRestartsTheWindow`.
- **F3 — undeliverable messages are now refused where they are BUILT.**
  `kMaxRaftSendBytes` moves into `raft_types.hpp` as the one definition, and three
  producers take it: (a) `sendInstallSnapshot` refuses an oversized snapshot WITHOUT
  advancing `nextIndex_` — the optimistic advance turned a transport refusal into a hot
  loop (advance, follower rejects, rewind, re-encode the whole snapshot, refuse, repeat,
  one error log per round trip), and the transport's refusal log is now latched per
  (group, peer) as well; (b) `RaftGroup::propose*` fails CLOSED above
  `kMaxProposalBytes`, because an entry that commits and can never be delivered leaves the
  group permanently one replica short with the offending entry already durable; (c)
  `snapshotVShard` refuses to COMPACT into a snapshot over the bound — compaction discards
  the log prefix the snapshot replaces, so an undeliverable snapshot would destroy the only
  other way to catch a follower up. Reachable only once the snapshot trigger is wired
  (`snapshotVShard` has no production caller today), which is why it is fixed before
  someone wires it.

**Filed, NOT fixed (recorded debt):**

- **F4 — the balancer's caught-up guard is too strict.** It gates a transfer on
  `matchIndexOf(target) == lastIndex()`, i.e. exact equality, so under sustained writes a
  perfectly healthy peer is almost never exactly caught up and leadership balancing
  becomes load-dependent — it converges when the cluster is quiet and stalls when it is
  busy. The property actually wanted is "this peer is ALIVE and replicating", so the gate
  should be a RECENT ACK (a bounded matchIndex lag, or a last-ack timestamp), not
  equality. Low risk, but it changes balancer behaviour under load and wants its own
  rolling-rebalance measurement.
- **F5 — a pre-existing read-path clobber in the index** (`native_index.cpp` ~:1759-1765,
  `getSeriesGroupedByTag`). The batch-insert of KV scan misses does
  `entry->bitmap = readSafe(...); entry->dirty = false;` on an entry a concurrent insert
  may have created and DIRTIED during the scan's suspension — overwriting its adds and
  clearing the flag that would have persisted them. Same shape as the merge-don't-assign
  rule the accessors already follow; the fix is `|=` and leaving `dirty` alone. For the
  index owner, alongside the index-flush durability defect already filed above.

#### 5.5. ADRs (design only)

- `docs/adr/0004-vshard-group-consolidation.md` [5c]. Recommendation: **not now, and not
  for throughput.** Consolidation divides by K everything that scales with GROUP count and
  nothing that scales with data; the node is 80 % CPU-idle, so removing CPU work cannot
  raise throughput here. Its strongest argument is the journal (1024 writers per shard ->
  64), and that is better addressed by the shared per-shard journal above, which is a
  smaller change the journal format was already designed for. The price of consolidation
  is movement granularity — the VShard is the unit of placement/movement/repair/snapshot,
  and consolidating stops the group and the VShard coinciding. One concrete
  recommendation for today: **put an explicit group id in the VShard directory now**; that
  single field is the difference between a rolling migration and a rebuild later.
- `docs/adr/0005-checkquorum-transfer-bypass.md` [5d]. Designs the `campaignTransfer`
  flag, and answers the compatibility question the plan flagged: `encodeEnvelope` has NO
  version field anywhere, so adding a byte to `RequestVote` is a silent-misparse hazard,
  not a compile error. Recommendation: a NEW MESSAGE TYPE byte (an old decoder rejects an
  unknown type and drops the envelope — fail-closed at the wire with no negotiation),
  **plus** gating CheckQuorum's activation on the cluster-wide committed format version
  (`features::FeatureGate`), so the guard is never on while a peer would drop transfer
  votes. Neither alone suffices. Note the ADR also corrects the plan's mechanism
  description: the transferee does not escape the guard by term escalation (the guard
  ignores term entirely) — it waits for the old leader to stop heartbeating.

#### 5.6. Fault-gate hardening — NOT DONE, recorded as debt

The floors in `fault_injection_gate.sh` are still 8 rounds / 8 connections against 147/392
observed, the scripted A/B against a 4a-reverted binary is still manual (the revert set is
named in the gate README but not scripted), and there is no combined
reset+rebalance+high-connection gate. Displaced by 5.4's defect and by writing the new
restart-catch-up gate. Unchanged from the Phase-4 debt entry.

#### Phase 5 gate outcomes (2026-07-26)

- **Suites: 4103 unit tests / 419 suites green** (from 4097/417: +3 chunked-catch-up,
  +3 transfer-abort), **33 socket tests green** (from 30: +3 transport
  batching/legacy-peer).
- **All five live gates green on the final binary**, each run in ISOLATION:
  `fault_injection` (146 reset rounds destroying 398 peer connections -> 2000/2000 +
  200/200, 0 errors, 93 % of the proxied baseline, all 200 probe points readable on every
  node -- against Phase 4's 147/392 and 94 %), `rolling_rebalance` (2216 transfers
  mid-bench, 600/600 OK, 4.87 M pts/s, converged to fair share), `deposed_primary`
  (18088 transfers, 300/300 accepted, 0 5xx, 0 500s), `backpressure` (200/200 rejected
  with the artificial budget named, then 200/200 OK at 5.04 M on the default budget), and
  the new `restart_catchup`.

  **MEASUREMENT HAZARD, second instance.** Run back-to-back as a battery, the fault gate
  FAILED (929/2000 errors, 8 % of baseline throughput, 794 reset rounds) purely because
  free space had fallen 39 G -> 13 G across the preceding gates. Re-run alone with space
  free it passes with 153 rounds and zero errors. The failure mode is self-amplifying and
  therefore very convincing: less headroom -> slower bench -> the 0.3 s resetter fires
  more rounds -> slower still. The Phase-4 note about `Disk quota exceeded` covers the
  quota-fence case; this is the same hazard SHORT of a fence. Run these gates one at a
  time, with the previous run's data dirs deleted.
- **Restart catch-up (new gate, `test/cluster_gates/restart_catchup_gate.sh`):** node 3
  down through 4 M points, then restarted -> caught up on 100 % of the VShards its peers
  lead (on every one of five runs), the probe point readable ON THE RESTARTED NODE, zero
  oversized-message refusals, zero server-side 500s, zero crashes. **Scope, stated
  honestly: the pre-5.4 binary passes it too** — at 400 batches the tail still fit under
  the old 1 GiB bound — so it is a REGRESSION FENCE on the 8x-tightened bound, not a
  demonstration that chunking was required at this scale. It is also what found the
  stuck-transfer defect and the pre-existing coordinator-side 503s above.
- **Canonical bench, FINAL binary (post-review), three accepted runs** (0 HTTP errors,
  0 server-side 500s, 0 quota fences on every run): **4.80 / 5.18 / 5.23 M pts/s, median
  5.18 M**, p50 89-100 ms, idle CPU 4 % per node. Before the review fixes, four runs:
  5.02 / 5.08 / 5.30 / 5.24 M, median 5.16 M. The pre-Phase-5 binary re-benched in the
  SAME session: 4.85 / 5.04 / 5.35 / 5.28 M, **median 5.16 M**, idle CPU 4 % per node.
  **Phase 5 is throughput-neutral, measured against a same-session baseline rather than a
  recorded one** — which is the only way this was resolvable, see 5a above.
- **kill -9 of the whole cluster:** 200/200 acked points readable on every node after
  restart, 0 journal quota fences.
- Intermediate readings, kept because the drift is the lesson: baseline 5.06/5.18/5.33 M
  -> after 5.1 4.99/5.17/5.33 M -> after 5a (batching ON) 4.95/5.13/5.28 M and later
  4.65-5.00 M. Those were taken hours apart from their baselines and are NOT comparable
  across rows; the idle-CPU column is what exposed that (4 % -> 8 % on an unchanging idle
  cluster).

### Phase 6 — Acceptance

The targets this phase was written against, unedited:

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

### Phase 6 outcome (2026-07-26) — RUN AND RECORDED

**The availability targets are met and the throughput targets are not, and the reason
they are not is now measured rather than argued.** Every HA property this plan set out
to establish holds under injected faults; the 6.5 M figure was set in §4 before anything
had profiled the limiter, and Phase 5's re-profile (5-pre) established what it is: the
quorum round trip, on a cluster ~80 % CPU-idle. Nothing in routing, wire format, or
consensus efficiency shortens a round trip, so nothing in Phases 1-5 could have closed
that gap, and the acceptance run confirms it did not.

#### Acceptance table — targets vs achieved

| target (§4, as written)                        | achieved                                     | verdict |
|------------------------------------------------|----------------------------------------------|---------|
| RF=3 ≥ 6.5 M pts/s                              | **4.91 M** median of 6 runs (4.87-5.02)      | **NOT MET** — see "Why the gap is structural" |
| RF=1 (3 nodes, partitioned) ≥ 9 M               | not re-measured this phase (last: 7.75 M, §1)| NOT MEASURED |
| CPU utilization must RISE with load             | flat **17-20 %** of 4 cores at conns 2/4/8/16 | **NOT MET** (and it is the finding, not a miss) |
| p50 ≤ 60 ms @ conns=8                           | **95.9 ms** median (93.1-101.2)              | **NOT MET** (from 155 ms at Phase 0) |
| adding connections raises throughput till CPU saturates | throughput flat ±3 %, latency exactly linear | **NOT MET** — RTT-bound, measured below |
| HA rounds: 0 HTTP errors except bounded deadline-expired during a true outage | all five live gates PASS; fault injection, rebalance, deposed-primary, backpressure **0 errors**; node-outage rounds give **bounded retryable 503s, never 500s, never loss** | **MET for the fault rounds; QUALIFIED for the outage rounds** (see HA below) |
| Full five suites                                | 4107 unit / 46 perf / 33 socket / 184 jest / 187 vitest, **all green** | **MET** |
| single-node non-clustered not regressed         | **12.38 M** median vs an 11.6 M baseline      | **MET** |

#### 1. Canonical bench campaign (6 runs, RF=3 on 3 nodes)

Fresh data dirs per run, leadership converged AND balanced before each, 100 x 10k,
hosts=1000, conns=8, `--smp 4`, `--verify 0`, 10 warmup batches. CPU from
`/proc/<pid>/stat` deltas across the timed run.

| run | pts/s     | p50 ms | p95 ms | p99 ms | max ms | HTTP errors | server 500s | quota fences |
|-----|-----------|--------|--------|--------|--------|-------------|-------------|--------------|
| 1   | 4 870 692 | 101.2  | 135.5  | 148.4  | 164.5  | 0           | 0           | 0            |
| 2   | 4 913 943 |  97.9  | 141.5  | 198.6  | 200.6  | 0           | 0           | 0            |
| 3   | 4 905 239 |  93.1  | 125.9  | 130.4  | 137.5  | 0           | 0           | 0            |
| 4   | 4 960 074 |  93.5  | 145.2  | 159.5  | 194.2  | 0           | 0           | 0            |
| 5   | 4 908 328 |  96.6  | 140.7  | 146.0  | 147.4  | 0           | 0           | 0            |
| 6   | 5 019 692 |  95.3  | 128.4  | 145.9  | 146.1  | 0           | 0           | 0            |

**Median 4.91 M pts/s, median p50 95.9 ms, zero-error rate 6 of 6.** Node CPU 16.5-20.6 %
of 4 cores on every run. Against the bars the Phase-6 battery brief set for THIS campaign
(0-error rate ≥ 5/6, median ≥ 5.0 M — not §4's targets, which are the table above): the
error bar is met, the median bar is missed by 2 %.

**Same-session A/B, because cross-session medians are not comparable on this box** (the
5a lesson: idle CPU on an unchanging idle cluster drifted 4 % -> 8 % within one session).
The pre-Phase-6 binary `11fc459`, rebuilt and re-benched in THIS session, three runs:
**4.958 / 4.967 / 4.917 M, median 4.958 M**, p50 91.3-94.4 ms. So this session's box is
worth ~4.95 M and Phase 6's two commits (a constant and an unreachable catch clause)
change nothing — the 5 % shortfall against Phase 5's recorded 5.18 M is drift, exactly the
effect 5a documented. **Do not read the phase-to-phase medians in the summary table below
as a trend; only the same-session A/B rows are evidence.**

#### 2. Connection-scaling probe

Same cluster shape, conns 2/4/8/16, one run each.

| conns | pts/s     | p50 ms | p99 ms | node CPU (% of 4 cores) | points in flight (thr x p50) |
|-------|-----------|--------|--------|-------------------------|------------------------------|
| 2     | 4 364 455 |  28.0  |  45.4  | 19.2 / 18.1 / 16.3      | 122 k                        |
| 4     | 4 721 678 |  49.9  |  86.2  | 19.5 / 17.6 / 17.4      | 235 k                        |
| 8     | 4 874 935 |  96.0  | 166.7  | 20.2 / 17.3 / 18.1      | 468 k                        |
| 16    | 4 802 504 | 189.6  | 303.7  | 20.1 / 17.2 / 18.2      | 911 k                        |

**Read it honestly: throughput is flat from conns=4 onward (+3 %, then -1.5 %), latency
doubles with every doubling of connections, and CPU does not move at all.** The last
column is the tell — the number of points the system is holding doubles each time, and
the time to get them through doubles with it. Added connections buy queueing, not
service. The plan's acceptance sentence ("adding connections increases throughput until
CPU saturates") describes a CPU-bound system; this one is not, has never been within this
plan's measurements, and 5-pre said so before this probe was run.

At conns=2 the p50 (28 ms) is within noise of the quorum commit latency Phase 3 measured
directly (30.95 ms p50). That is the whole story of the number: **one batch costs one
quorum round, and the round rate is what is fixed.**

#### 3. Live gates — all five PASS, each run in isolation on the final binary

Free space checked ≥ 30 G before each and data dirs deleted after, per the two recorded
measurement hazards. No run logged `Disk quota exceeded`.

| gate | result |
|---|---|
| `fault_injection` | **PASS** — 147 reset rounds destroying 400 peer connections mid-bench -> 2000/2000 bench + 200/200 probe, **0 HTTP errors, 0 server 500s, 0 crashes**; 4.61 M under the storm vs a 4.98 M proxied baseline (**92 %**); all 200 acked probe points readable on all three nodes |
| `rolling_rebalance` | **PASS** — **2216 transfers** initiated mid-bench over 102 rebalance calls, **600/600 OK, 0 errors**, 4.85 M pts/s, converged to [1364 1368 1364] and settled (0 VShards moving) |
| `deposed_primary` (5 nodes, RF=3) | **PASS** — **18 767 transfers**, **300/300 accepted, 0 5xx, 0 500s**, coordinator hosting 2458/4096 (so the stale-primary path is genuinely exercised) |
| `backpressure` | **PASS** — at the artificial 1 MB/shard budget 200/200 rejected with the budget named and **0 500s**, every 503 carrying `Retry-After`, a single small write still succeeding; restarted at the default budget the same cluster runs **200/200 OK at 5.04 M** with zero admission rejections |
| `restart_catchup` | **PASS** — node 3 down through a 400-batch campaign, restarted -> caught up on **100 %** of both peers' leaderships, probe point readable on the restarted node, **0 oversized-message refusals, 0 quota fences, 0 server 500s**. Client 503s during the outage: 137/400 (ADVISORY by design — see below and the gate README) |

#### 4. HA rounds

**(a) `kill -9` of one node mid-bench.** 200 batches against node 1, node 3 killed at
t+3 s, 50 probe writes sent during the outage, then node 3 restarted.
**167 OK / 33 bounded 503s**, every one
`"N VShard slice(s) uncommitted after 6 attempt(s) (last: transport)"`; **0 server-side
500s, 0 crashes**; **50/50 probe writes acked while the node was down**; after restart and
leadership settling, node 3 was caught up on **1040/1040 and 2048/2048** of the two
survivors' leaderships and the probe's 50 points were readable **on the restarted node**.

**(b) Rolling restart under sustained writes (SIGTERM, one node at a time, catch-up
between).** Nodes 2 and 3 restarted while a 900-batch bench drove node 1. Each exited on
SIGTERM in **2 s** and caught up fully after restart (node 2: 129/129 and 127/127; node 3:
656/656 and 3072/3072). Client result: **567 OK / 333 HTTP errors, all bounded retryable
503s (`last: transport`), 0 server-side 500s, 0 connection failures, 0 crashes.**
Restarting the node the bench itself is holding (node 1), measured separately, adds
72 connection failures — a load-balancer property, not a write-path one.

**"Zero client errors" was the expectation and it is NOT what happens. The reason is
structural and worth stating, because it also explains the restart-catch-up gate's
advisory 503 band (50-201 of 400, and 111/400 on the pre-Phase-5 binary):**

- A quorum of 2 of 3 survives one node leaving, so a group whose leader is alive keeps
  committing. What does not survive is the *leadership* the departing node held — ~1/3 of
  4096 groups — and each of those needs a fresh election (2.5-5 s at the 20 ms tick)
  against a 1.5 s per-write deadline.
- **A batch fails if ANY of its slices fails.** The canonical batch (10k points,
  hosts=1000) touches on the order of a thousand VShards, so a per-VShard unavailability
  of even 0.1 % during the election window makes batch failure near-certain. The
  all-or-nothing rule is deliberate (§3: "no silent partial batch") and correct — but it
  means the *client-visible* error rate during a node outage is amplified by batch
  fan-out, and a "0 errors" acceptance bar phrased per REQUEST can never be met by a
  system that fails a request when one of a thousand slices misses a deadline.
- The contract that matters is intact throughout: 503 (retryable) not 500, `Retry-After`
  where the cause is overload, no loss, no duplication, no split brain, and full catch-up
  afterwards. A client that honours the 503 loses nothing.

This is the honest reading of the plan's own qualifier ("except bounded deadline-expired
failures during a true node outage"): those failures are bounded and deadline-expired, and
there are more of them than the sentence implies.

**(c) `kill -9` of the whole cluster.** 200 points written and acked, all three nodes
killed -9, all three restarted: **200/200 readable on every node** (49210, 49211, 49212),
matching pre-kill.

#### 5. Suites — all five green

| suite | result |
|---|---|
| `./test/timestar_unit_test` (from the build root) | **4107 tests / 419 suites, all passed** (1 disabled — the index-flush durability repro, see debt D-3). Since the D-2/D-3 fixes: **4110 / 420**, nothing disabled |
| `./test/timestar_perf_test` | **46 tests / 12 suites, all passed** (rebuilt first: the binary predated this phase's lib changes) |
| `./test/timestar_cluster_socket_test` | **33 tests / 8 suites, all passed** |
| `test_api` jest | **184 tests / 13 suites, all passed** |
| `timestar-nodejs` vitest | **166/166 correctness + 21/21 integration, all passed** |

The JS servers ran single-node and NON-clustered, on ports 18086 and 58086 rather than the
conventional 8086: this box has the user's own docker cluster (`timestar1/2/3`) bound to
8086-8088, and answering a suite from a different binary's server is the stale-test-server
trap. Both suites honour `TIMESTAR_HOST`/`TIMESTAR_PORT`; the correctness suite also needs
`TIMESTAR_DATA_DIR` pointing at its server's directory, without which its flush-verified
placements silently degrade to memory-store-only tests (it fails closed on that, which is
how it was caught).

#### 6. Single-node regression check

Non-clustered, same bench shape: **11.87 / 12.38 / 12.39 M pts/s (median 12.38 M)**, p50
6.9-7.9 ms, 0 errors. Against the 11.6 M baseline in §1, the non-clustered path is **not
regressed** by any of this campaign — it is marginally faster, which is box variation, not
a claim.

The pair of numbers is also the cleanest statement of what replication costs today:
**12.38 M @ 7.5 ms single-node vs 4.91 M @ 95.9 ms at RF=3** — 2.5x the throughput and
13x the latency.

#### Why the 6.5 M gap is structural, not a missing optimization

The target assumed RF=3 should cost "one quorum round of pipelined overhead, not 2x". The
measurements say the first half is right and the second does not follow:

1. **The limiter is the round trip, and the cluster is idle while it waits.** 5-pre:
   ~80 % of every core idle at peak, commit latency 19-66 ms, and — the decisive one —
   the Raft message RATE is IDENTICAL idle and saturated (~2724 envelopes/s/shard). Load
   makes messages bigger, not more numerous. There is no CPU wall to move.
2. **Concurrency does not convert into throughput** (§2 above). Points-in-flight and
   latency both double from conns=2 to 16 while throughput moves ±3 %. So the system is
   not short of offered concurrency; the rate at which quorum rounds complete is fixed,
   and every added connection joins the existing rounds instead of creating new ones.
3. **Therefore throughput ≈ (batch size x rounds in flight) / round time**, and this plan
   changed neither term. Phases 1-2 removed serialization *around* the round (the shard-0
   funnel, the scatter loops, the re-hashes, the codec), which is why they bought
   4.03 -> 5.06 M and 155 -> 92 ms and then stopped buying anything. Phases 3-5 bought
   availability and correctness, which is what they were for.
4. What this phase did NOT separate: whether the round rate is bounded by the tick/drain
   cadence, by the journal barrier, or by the loopback RTT itself. Phase 5's profile
   points at persist (~1.6 ms per sync, the largest stage, on tmpfs) but 1.6 ms does not
   explain a 28 ms round. **Someone should measure the round's own budget before the next
   throughput attempt** — that decomposition is the missing experiment in this whole
   campaign, and every remaining lever below is a guess without it.

#### What would actually move it (all measured or designed, none available cheaply)

- **A real 5b — one journal writer per SHARD with group-commit coalescing.** The only
  lever still inside the round with a measured cost behind it (`persist` is the largest
  stage). Not implemented because the premise 5b was written on is false: `JournalWriter`
  is per VSHARD, so there is nothing to coalesce onto. The work is spelled out in 5.3,
  and its real price is cluster-wide journal retention interacting with movement.
- **Ack-at-commit instead of at-apply.** MEASURED and CLOSED (3c): the commit->apply gap
  is 0.21 ms p50 against a ~31 ms commit — 0.7 % of what a client waits, and it costs the
  read-your-writes story. Not worth it at any point on this curve.
- **VShard:group consolidation (ADR 0004).** Divides by K everything that scales with
  GROUP count and nothing that scales with data. On a node that is 80 % CPU-idle, removing
  CPU work cannot raise throughput. Rejected FOR THROUGHPUT; its real argument is the
  journal, which the shared per-shard writer addresses more cheaply.
- **Deeper client pipelining.** Out of scope by §5 — and §2 above is the measurement that
  matters: 8x more concurrency at the client produced 10 % more throughput and 6.8x the
  latency. More in-flight work at the client is not the missing ingredient.
- **Fewer, larger batches, or clients spread across nodes.** Not attempted here. The bench
  drives ONE node, which is therefore coordinator and replica simultaneously (5-pre found
  its stages run 2-3x the other nodes'); a real fleet spreads. This is the cheapest
  untested hypothesis on the list and it is a BENCH change, not a server change.

#### Campaign summary (Phase 0 -> Phase 6)

Canonical bench throughout: RF=3, 3 nodes, `--smp 4`, 100 x 10k, hosts=1000, conns=8,
fresh dirs, leadership converged. **Medians across phases are NOT strictly comparable
(box drift, see 5a); the same-session A/B rows are the evidence.**

| phase | median pts/s | p50 | rejected-run rate | what it bought |
|-------|--------------|-----|-------------------|----------------|
| 0 (baseline) | 3.83-4.03 M | 155 ms | ~1 in 3 (500 bursts) | — |
| 1 — funnel removal | 4.55 M | 104 ms | — | shard-0 profile outliers cut 2-6x |
| 2 — route/copy/encode once | 5.06 M | 92 ms | 2 in 6 | shard-0 outlier GONE (2-pre, the Raft listener) |
| 3 — ack path + retry semantics | 4.98 M | 93.5 ms | 1 in 9 | deposed primary 29 % 500s -> 0; 503+Retry-After; every retryable failure named |
| 4 — HA hardening [D6] | 5.34 M | 82.7 ms | **0 in 8** | the collapse window CLOSED, and proven by injected RSTs rather than statistics |
| 5 — consensus efficiency | 5.18 M (same-session base 5.16 M) | 89-100 ms | 0 in 3 | throughput-NEUTRAL by design; UAF closed structurally, catch-up chunked, stuck-transfer defect fixed |
| **6 — acceptance** | **4.91 M** (same-session base 4.96 M) | **95.9 ms** | **0 in 6** | the numbers above, honestly recorded |

The arc, stated without decoration: **throughput 3.9 M -> 4.9-5.3 M (~+27 %), p50 155 ms
-> ~96 ms (-38 %), and a ~1-in-3 chance of a 500-burst -> 0 in 30+ recorded runs across
Phases 4-6.** The availability work is the campaign's real product; the throughput work
finished in Phase 2 and everything after it confirmed why.

#### Debt closed in this phase

- **Snapshot envelope headroom** (`401a980`). `sendInstallSnapshot` (raft_node.cpp:283)
  and `snapshotVShard` (replicated_vshard_host.cpp:93) compared a RAW PAYLOAD against
  `kMaxRaftSendBytes` while the transport refuses on the ENCODED ENVELOPE, so a snapshot
  in the band between passed both producer checks and was refused on the wire —
  reinstating the nextIndex hot loop F3a existed to remove, and (for the compaction case)
  after the log prefix had already been discarded. `kMaxRaftPayloadBytes` now states the
  4 MiB reserve once and all three producers take it; a codec test measures the framing
  rather than asserting the constant.
- **`ProposalTooLargeError` -> 413** (`a0633dc`), matching `WriteFrameTooLargeError` and
  `InsertTooLargeException`. It reached the handler and fell through to an opaque 500 —
  and in the `{"writes":[...]}` path to a 200 `"partial"`. Unreachable today; fixed
  because what makes it reachable (a larger batch cap, or the snapshot trigger being
  wired) will not arrive with a reminder.

#### Carried debt register (complete, as of Phase 6)

Everything filed across Phases 3-5 and still open, in one list. Nothing here is a
regression from this campaign unless it says so.

| # | item | pointer | owner / note |
|---|---|---|---|
| ~~D-1~~ | **CLOSED** (`9b0ae8a`, tightened by `6eec634`). Exact equality (`matchIndexOf(target) == log().lastIndex()`) is replaced by two gates, both required: a **lag bound** (within 64 entries, ~one AppendEntries round trip of in-flight work) and a **liveness bound** (`RaftNode::ticksSinceAck` within ONE heartbeat interval — it shipped at three in `9b0ae8a` and the fault gate rejected that; see below). The liveness half is new and is the part a lag bound cannot supply: on a write-IDLE group a dead peer sits at lag **zero** indefinitely — it was caught up when it died and lastIndex never moves again — so it passes any delta test *including the old exact equality*. That hole is pre-existing; this closes it. The clock is one monotonic counter plus a per-peer stamp written where matchIndex already is (O(1) per tick, which matters at 4096 groups/shard every 20 ms), cleared on every role change and deliberately unseeded on `becomeLeader` so a fresh leader reads every peer as dead until it answers. Residual, bounded: a peer dying inside the CURRENT heartbeat round can still be targeted, costing that group its proposals for ONE election timeout before `RaftNode::tick` abandons the transfer (§3.10) — one window on the pass that races the death, not one per pass forever. Shrinking it further means shortening the ABANDON window, which is consensus timing rather than a target filter — see D-20. **THE WINDOW IS ONE HEARTBEAT AND NOT THREE BECAUSE THE FAULT GATE SAID SO** (`6eec634`). At three rounds (1.5 s) `fault_injection_gate.sh` REGRESSED. Three runs, same fault, same box: pre-D-1 **167 rounds / 457 connections → 0 errors, PASS**; D-1 @ 3 heartbeats **165 / 442 → 1 error, FAIL**; D-1 @ 1 heartbeat **169 / 438 → 0 errors, PASS**. The failing run took a marginally *lighter* storm than the passing baseline, so intensity does not explain it. Mechanism: `ClusterDataPlane` runs a PERIODIC balancer, so a pass could target a peer whose connection had just been RST — ack clock not yet decayed, lag bound still holding. `transferLeadership` pins `leadTransferee_`, the group refuses every proposal until the transferee acks, and the abandon bound is one election timeout (2.5–5 s) against a 1.5 s write deadline: one mis-aimed transfer is one failed batch. The old exact-equality guard was *accidentally* immune (a peer whose acks stop falls behind a growing lastIndex at once) — which is worth knowing before anyone relaxes this again. **`rolling_rebalance_gate.sh` is a NULL result:** 2216 transfers / 102 calls / 600 of 600 OK / converged to [1364 1368 1364], and a D-1-reverted binary produces *exactly the same numbers* — its 1000-hosts-over-4096-groups load leaves nearly every group momentarily idle at any pass, so exact equality was rarely binding there (see D-18). Canonical RF=3 bench, same session: HEAD 4.66 / 4.59 M vs pre-D-1 4.46 M, 100/100 and zero errors on all three. Semantics pinned by `RaftPeerLivenessTest` (7 cases). | `lib/cluster/integration/shard_raft_plane.hpp` (`rebalance`); `raft_node.{hpp,cpp}` (`ticksSinceAck`) | movement/M5 |
| D-20 | **The leader-transfer abandon window is one ELECTION timeout (2.5–5 s), against a 1.5 s write deadline.** While `leadTransferee_` is set the group refuses every proposal, so ANY transfer whose target goes unreachable after the balancer picked it costs that group a full batch-deadline's worth of writes. Found while closing D-1: it is what turns a single mis-aimed transfer into a client error, and it is the reason the target filter has to be conservative rather than merely correct. etcd uses one election timeout too, so this is not a bug — but a shorter, transfer-specific abandon bound (a few heartbeats) would make the balancer's target choice much less load-bearing. Consensus timing change; wants its own fault-gate measurement. | `lib/cluster/raft/raft_node.cpp` (`tick`, the §3.10 abandon arm) | consensus owner |
| ~~D-2~~ | **CLOSED** (`ac3ab5e`). F5 — read-path clobber in `getSeriesGroupedByTag`: the batch-insert of KV scan misses assigned over, and un-dirtied, an entry a concurrent insert had created during the scan's suspension. Now merged with `dirty` left alone, and `approxBytes` is set (it stayed 0 before, so a loaded entry was invisible to the trim byte budget). The two read accessors were re-checked and were already correct (their assign runs only when the post-suspension re-find returned end()). Gate `PostingsReadClobberTest.GroupedByTagLoadDoesNotClobberAConcurrentInsert` loses 200/200 pre-fix. | `lib/index/native/native_index.cpp` (`getSeriesGroupedByTag`) | index owner. Pre-existing |
| ~~D-3~~ | **CLOSED** (`35ec34e`), and neither lead in the original filing was the cause. Instrumenting the repro against the three drop conditions isolated it to day-bitmap membership alone (metadata 600/600, LocalIdMap 600 mappings, postings all resolved), with the missing ids the CONTIGUOUS TAIL assigned after the last flush. `close()` guarded its final flush on `!memtable_->empty()`, but `flushMemTable()` and `maybeFlushMemTable()` are the only two callers of flushDirtyBitmaps/DayBitmaps/HLLs/MeasurementBlooms — and maybeFlushMemTable() only runs on a memtable threshold crossing, so close() was the last chance. Those caches are mutated without touching the memtable, so an empty memtable (exactly what the last threshold swap leaves behind) meant the flush was skipped with 4 dirty day-bitmap and 40 dirty postings keys outstanding. `close()`/`compact()` now drop the `!empty()` half of the guard but KEEP the null check (`memtable_` exists only after `open()`, and the server's engine guard closes never-opened indexes on a startup failure — without it that path SIGSEGVs); the postings batch is also applied before the WAL append. Close is now marginally heavier: it always runs the full flush body rather than skipping it. **Still open one level down:** a `kill -9` OR the ordinary 30 s shutdown-timeout `_Exit(1)` (`bin/timestar_http_server.cpp`), which abandons `close()` mid-flight, loses cache adds since the last flush — postings recover via the watermark repair, day bitmaps have no equivalent. | `native_index.cpp` (`close`, `compact`); gate `day_bitmap_concurrent_insert_test.cpp` (`ConcurrentInsertsWithTinyWriteBufferKeepDayMembership`, no longer disabled) | index owner. Pre-existing |
| D-17 | **Dirty index caches can accumulate unbounded between flushes.** `maybeFlushMemTable()` is driven ONLY by memtable size, but the bitmap/day-bitmap/HLL/bloom caches are mutated without writing any KV entry — a workload that re-inserts only ALREADY-KNOWN series (steady state on a fixed fleet) grows the memtable barely or not at all, so the flush may not fire for a long time while dirty entries pile up. They are also trim-exempt (the trims skip dirty entries by design, so the byte budgets do not bound them), and everything outstanding is lost on any non-clean exit. Found while fixing D-3; not a regression. Wants a flush trigger that counts dirty cache bytes or age, not just memtable bytes. | `native_index.cpp` (`maybeFlushMemTable`, `trimBitmapCache`/`trimDayBitmapCache`) | index owner. Pre-existing |
| ~~D-4~~ | **CLOSED for the two halves that were specified** (`3f2463c`); the third is re-filed as D-19. (a) The anti-vacuity floors were 8 rounds / 8 connections against an observed 147 / 392-400 — about 5%, barely more than the "did it fire at all" check they replaced. Now **70 / 180**, roughly half of observed. Half rather than more because the resetter fires on a fixed 0.3 s wall clock while the bench length is machine-dependent, so a faster box legitimately injects fewer rounds and a floor that trips on a fast machine teaches people to ignore the gate; override with `GATE_MIN_RESET_ROUNDS`/`GATE_MIN_RESET_CONNS` rather than editing. (b) The A/B is scripted as `test/cluster_gates/fault_injection_ab.sh` — it creates a `git worktree`, applies `git checkout fcb2a94^ -- write_errors.hpp replicated_write_router.{cpp,hpp}`, builds the server in its own build dir, runs the storm gate against both binaries and asserts the reverted one fails while HEAD passes. Marked **expensive and explicitly not a CI gate** (a fresh comparison build dir builds seastar too). It also guards three things a naive A/B would not: the revert must not be a NO-OP (else the two binaries are identical and every assertion compares a thing to itself), both runs must take a storm within 2x of each other, and the reverted binary's failures must carry the **[D6] signature** (`RetryableWriteError … last: transport`) — necessary because a hunk-level `git revert fcb2a94` CONFLICTS (`d101c07` and `c052253` touch the same lines), so the whole-file checkout reaches past 4a and "it produced errors" alone would be a weaker claim than it looks. A third fix fell out of using the gate this way (`ca8e430`): `kill_cluster` matched `timestar_http_server.*--port $1`, so a run against a comparison binary that had been SAVED under another name matched nothing, left three servers alive and made the next gate abort on "ports still in use" — measured while A/B-ing D-1 against a copy saved as `ts_pre_d1`. (Not `fault_injection_ab.sh`, as an earlier draft of this row claimed: it builds a real `<build>/bin/timestar_http_server` and launches it by full path, which the old pattern did match.) It now matches the port. Note the prefix is NOT unique per gate — 493 covers fault_injection AND deposed_primary, 492 covers backpressure AND rolling_rebalance — so this reaches a same-band gate's cluster by design; the README's run-one-at-a-time rule is what makes that cleanup rather than interference. **Hardened gate re-run: 169 reset rounds / 438 connections destroyed, 2000/2000 bench OK, 200/200 probe OK, 0 errors, 0 server 500s, 91% throughput retained, all 200 probe points readable on every node — GATE PASSED**, with both new floors cleared by ~2.4x. **`fault_injection_ab.sh` has NOT been run end to end** (see D-19). | `test/cluster_gates/fault_injection_gate.sh`, `fault_injection_ab.sh`, `cluster_gate_lib.sh` | write-path owner |
| D-22 | **The D-12 balancer arithmetic has NO unit coverage.** It is verified analytically in the row above and measured live on a 5-node cluster, but nothing pins it in the suite -- so a future edit to `rebalance` can silently restore the ~40 %-low target with every gate still green (the 3-node gates cannot see it by construction). Wants the arithmetic extracted pure, the way `planReadRouting` was for D-13, and tested on three properties: Σexpected == hosted-group count, RF == N reduces to hosted/N for every node, and a non-voter of a group is never chosen as that group's transfer target. | `lib/cluster/integration/shard_raft_plane.hpp` (`rebalance`) | movement/M5 |
| D-23 | **`raft_peer_liveness_test` re-implements `transferrableTo`'s predicate by hand** rather than calling it, so the test and the balancer can drift apart -- and the test would keep passing while the balancer changed. D-12 extracted the predicate as a static member; a scalar overload taking (lastIndex, matchIndex, ticksSinceAck, heartbeatTimeout) would let the test exercise the real one. | `test/unit/cluster/raft_peer_liveness_test.cpp`; `shard_raft_plane.hpp` (`transferrableTo`) | movement/M5 |
| D-24 | **`transfers_initiated` is still inflated, by a different early return than D-12 fixed.** The balancer now refuses non-voter and not-caught-up targets, but `RaftNode::transferLeadership` ALSO returns silently when `leadTransferee_ == target` (the F2 re-arm guard), and the balancer counts that as a transfer. The deposed-primary gate asserts an anti-vacuity FLOOR on this counter, so the inflation makes that assertion weaker than it reads. Wants `transferLeadership` to report whether it actually started a transfer. | `raft_node.cpp:545-567`; counter in `shard_raft_plane.hpp` (`rebalance`); gate `deposed_primary_gate.sh` | consensus owner |
| D-25 | **The RF < N read path has no wire-version negotiation.** `resolveVShards`/`redirects` are optional tails, so a new coordinator talking to an OLD holder fails CLOSED (the holder ignores the resolve list, answers from a possibly-stale replica... or, if the tail changes shape again, refuses to decode) rather than degrading knowingly. The write path negotiates per peer (`kWriteBatchFormatMax`); the query path does not negotiate at all. Only reachable in a mixed-version RF < N cluster, i.e. a rolling upgrade onto D-13. | `lib/cluster/data/node_query.cpp` (the tails); negotiation exists at `dataplane_rpc.cpp` for writes only | query plan |
| D-26 | **Reads and writes now tolerate an election asymmetrically, and nothing records the choice.** After D-14 a write may wait ~5 s (`kElectionDeadline` 6 s) for a group to elect, while a read fails closed after 4 x 25 ms (`kLeaderRetries`) -- a ~50x difference. That is defensible (a failed read is cheap to retry and a caller usually wants an answer or an error fast; a failed write costs the client a whole batch) but it is an accident of two independent decisions, not a stated policy, and it means a one-node outage now shows up as read errors long before write errors. Wants a decision recorded, and probably a modest read-side election tolerance. | `cluster_data_plane.cpp` (`kLeaderRetries`); `replicated_write_router.hpp` (`kElectionDeadline`) | query plan + write-path owner |
| D-27 | **Every live gate but one picks its ports INSIDE the kernel's ephemeral range** (`ip_local_port_range` = 32768-60999 here), so a gate races the kernel for its own ports: a node started earlier dials its peers, the kernel hands one of those outbound sockets the exact port a later-starting node still has to bind, seastar exits on the failed listen, and the gate reports "cluster did not converge" with one node silently missing. It is NOT a rare flake at five nodes -- `deposed_primary_gate.sh` hit it four times in one session (49312 once, then 51312 on three consecutive runs) and was moved to 19310-19314, which fixed it outright. The 3-node gates (492xx/493xx/494xx/496xx) are exposed to the same race with fewer dials, and one of this session's failures cost a full 300 s convergence timeout before the cause was visible in a node log. Fix is mechanical (move each gate below 32768; the `kill_cluster` prefixes move with them) but it touches every gate script, so it is filed rather than done inside a correctness batch. | `test/cluster_gates/*.sh` (ports); the hazard note is on `require_ports_free` in `cluster_gate_lib.sh` | test-infra owner |
| D-21 | **`fault_injection_gate.sh`'s zero-error assertion is not reproducible run to run on this box, in EITHER direction.** Found while gating D-14. Three consecutive runs on the same HEAD binary in one session: 3 errors, 3 errors, then **0** (147-151 reset rounds, 396-405 connections each time) — and the pre-batch control (`6a63f54`) in the same session produced **6** storm errors, **2** probe 5xx AND **257 errors in its QUIET baseline run** (the no-fault run through the proxy, at 994 k pts/s against the usual ~4.95 M), which is a gate-abort-level result from a binary that is supposed to be the good one. So a single failing run of this gate does not identify a regression, and a single passing run does not clear one; the D-4 write-up's "HEAD takes the identical storm with zero" is one draw from a distribution. Nothing here says the gate is wrong to assert zero — it says the assertion needs N runs, or a floor stated as a rate. Wants: the gate to run the storm K times and assert on the total, or an explicit `GATE_MAX_STORM_ERRORS` with a measured distribution behind it. | `test/cluster_gates/fault_injection_gate.sh`; the runs are in the Batch C notes | write-path owner |
| D-19 | **Two fault-gate items left open by D-4.** (i) `fault_injection_ab.sh` has never been executed end to end — its shell, its revert incantation (verified to produce exactly the intended 3-file diff) and its parsing are checked, but the full build-plus-two-storms run was not affordable in the batch that wrote it. First runner should expect tens of minutes for the comparison build. (ii) There is still **no combined reset + rebalance + high-connection gate**; that half of the original D-4 was never in scope for this batch. | `test/cluster_gates/fault_injection_ab.sh` | write-path owner |
| ~~D-5~~ | **CLOSED** (`d180064`). InstallSnapshot is chunked at `kMaxSnapshotChunkBytes` = **4 MiB**, with receiver-side staging, a resumable offset, per-chunk acks and a stall timer. **Chunk size:** the same order as the bounds the other Raft producers already live under (1 MiB of entries per AppendEntries, 256 KiB per batched frame), so it needs no separate allowance anywhere in the chain, and large enough that envelope + RPC + round trip amortize to ~2% (measured against a pessimistic 4096-voter config, whose framing overhead is under 100 KiB). **Resumability:** chunks accumulate in a staging buffer keyed by (index, term); a chunk for any other boundary DISCARDS what is staged rather than splicing (reachable without malice -- the leader compacts again mid-transfer, or leadership moves); offset 0 always restarts clean, which is what makes the leader's restart idempotent; a duplicate or out-of-order chunk is answered with the follower's real offset, never applied. The staging area is DELIBERATELY VOLATILE -- never persisted -- so a follower that dies mid-transfer comes back with no partial at all, which makes a half-installed snapshot unreachable rather than merely unlikely. The install is one atomic step on the chunk that completes the payload. **Stall detection:** the reply now carries `(pendingSnapshotIndex, stagedBytes)`, so a mid-transfer ack is distinguishable from a completed install; `sweepStalledSnapshotTransfers` (leader tick) resends after `snapshotChunkTimeout` FROM THE OFFSET THE FOLLOWER REPORTED, then abandons after `maxSnapshotResends`. **nextIndex_ is correct by construction, which is F3a strengthened rather than preserved:** NOTHING advances it on a send -- only an install-outcome reply does -- so abandonment has nothing to unwind and a lost final chunk costs a retry rather than a lie. **Flow control:** exactly ONE unacked chunk per peer, released by the previous ack (the same pipeline shape as the bounded AppendEntries catch-up); the transfer record's existence is the guard, without which every heartbeat's `bcastAppend -> sendAppend -> sendInstallSnapshot` would re-blast chunk 0 forever and no snapshot would EVER complete. **Mixed versions fail closed:** the chunk fields could NOT be appended inside tag 5 -- `data` is that body's last field and `decodeEnvelope` does not require every byte to be consumed, so an old decoder would take `data`, ignore the trailer, and INSTALL A PREFIX AS COMPLETE, reporting matchIndex for data it does not hold. A real chunk therefore rides a NEW tag (9, reply 10) that an old peer drops whole, while a snapshot that fits in ONE chunk is still emitted under the ORIGINAL tag and body byte-for-byte, so an un-upgraded peer is caught up normally; a completed install always answers under the original reply tag. The decoder also rejects a chunk whose offset/total/done contradict each other (that trio sizes the receiver's staging buffer and triggers the install). **FOUND WHILE TESTING:** while a transfer is in flight that peer is served CHUNKS INSTEAD OF HEARTBEATS (sendAppend hands off and sendInstallSnapshot is a no-op while a chunk is unacked), so a chunk or its resend is the only thing resetting that follower's election clock -- the ordering **heartbeat 25 < snapshotChunkTimeout 50 << electionTimeoutMin 125 ticks** is load-bearing and `ClusterDataPlane::start` now FAILS CLOSED if a future edit breaks it. **THE NEW ARITHMETIC CHAIN (the payoff), stated once in `raft_types.hpp`:** producer payload **28 MiB** (`kMaxRaftPayloadBytes` == `RaftGroup::kMaxProposalBytes`) + envelope headroom **4 MiB** <= send refusal **32 MiB** (was 96) <= peer admission **64 MiB** (was 128). A chunk is 4 MiB, so snapshots no longer bind anything and the chain is sized for the biggest APPEND -- i.e. the largest single log ENTRY, since at least one entry is always sent regardless of `maxAppendBytes`. 28 MiB is generous for that: an entry is ONE VShard's slice of ONE write batch, and the data-plane forward path already 413s a slice over ~10.67 MiB (`kMaxOutboundFrameBytes`); only the LOCAL propose path had no cap, and reaching 28 MiB needs a >28 MiB ENCODED slice concentrated on one VShard out of 4096, which a legitimate batch does not do. That case now gets a clean 413 instead of a 92 MiB Raft entry three nodes must fsync, ship, stage and apply atomically -- so `http_write_handler`'s "unreachable, cosmetic" note on `ProposalTooLargeError` is corrected. **The compaction refusal survives and its threshold RISES** from `kMaxRaftPayloadBytes` (which the retuning would have DROPPED to 28 MiB, refusing more often after the fix meant to enable it) to `kMaxVShardSnapshotBytes` = 128 MiB, restated as a MEMORY bound: the payload is materialized in RAM by the producer, held by the leader and staged in RAM by the receiver, and three copies of an unbounded payload on a fixed pool is an OOM. ~4.5x above the old effective ceiling, 32 chunks. **Gate:** `restart_catchup_gate.sh` extended to force snapshot-based catch-up and to PROVE it was used (400/400 batches with one replica down, 5.97 M pts/s, 8 snapshots taken, then **19 chunks sent / 4 snapshots installed on the restarted node / 0 undeliverable / 0 abandoned**; 15 stall-driven resends among those 19, which is the recovery path working -- a chunk sent while the restarting node is still opening ~1365 journals finds no group yet and is dropped silently). 18 unit tests in `raft_snapshot_chunking_test.cpp`. Residuals D-31, D-32. | `raft_messages.hpp`, `raft_codec.cpp` (tags 9/10), `raft_node.{hpp,cpp}`, `raft_types.hpp` (the chain), `raft_rpc_transport.cpp` (admission), `cluster_data_plane.cpp` (options) | snapshot owner |
| ~~D-6~~ | **CLOSED** (`65e9e35`, corrected by `cdf58cf`). `snapshotVShard` now has a production caller: a per-shard sweep every 5 s (jittered per shard, rotating scan cursor) takes at most **one** snapshot per pass and not the same group twice inside 60 s. **Policy metric -- both, and that is the point:** entries since the boundary bound restart REPLAY TIME (each entry is a WriteBatch to decode and re-apply), applied entry BYTES bound the journal's DISK footprint, and the two diverge by orders of magnitude (a fleet writing 10k-point batches crosses 64 MiB in a few hundred entries; a trickle of single-point writes crosses 8192 entries having written almost nothing). Defaults 8192 / 64 MiB, overridable via `setSnapshotPolicy` and the `TIMESTAR_CLUSTER_SNAPSHOT_*` env knobs. **(a) Cohesion gate** checked ONCE at `startSnapshotTrigger` rather than per group -- `vshardsCohesiveOnCores` is a property of the node and can never become true later, so a non-cohesive core count disables the whole trigger with one legible warning instead of throwing every five seconds. **(b) The truncation boundary, and TWO real holes found by verifying it.** (i) `manifest.snapshotRevision` is the MAX revision in the flushed extents, but a revision is one whole log ENTRY and an entry's points do not all flush together (`applyWrites` buckets a batch by core and issues several `insertBatch` calls, any of which can roll the memory store), so the manifest reports N for a PARTIALLY flushed entry -- truncating AT N discards the entry holding the remainder. Boundary is now `snapshotRevision - 1`, also clamped to `appliedIndex` so the RETURNED value is the one really used. (ii) **WAL->TSM conversions run 6 at a time and COMPLETE OUT OF ORDER**, so the max revision in TSM can sit ABOVE an unconverted store's revisions: store A (rev 1-10) and B (11-20) both rolled with only B converted means TSM holds 11-20 and truncating near it discards the entries for 1-10, whose data is only in RAM. **Measured as 7 of 200 acknowledged points lost across a kill -9, with the three nodes disagreeing about how many** (they had converted different stores). `EngineLocalStore::hasUnconvertedStores` now REFUSES to compact while any rolled store is unconverted; with none, TSM is exactly the rolled stores and the only unflushed data is the active store, which holds a contiguous SUFFIX per VShard -- which is what makes the max flushed revision a boundary at all. Conservative in the safe direction (a continuously-converting shard keeps its log); residual D-35. **(c) Rate limit / stagger:** one per shard per sweep over ~1365 groups, per-shard timer jitter, rotating cursor, 60 s per-group minimum. **(d) Oversize refusal reconciled with D-5** -- see that row: threshold RISES to 128 MiB and is now a memory bound. **(e) Never blocks proposals:** own timer, own gate, a failed sweep is logged and dropped (nothing was compacted -- `compact()` is the last step), and `stop()` cancels the timer and closes the gate BEFORE the registry. **TWO DEFECTS THE FIRST CALLER EXPOSED.** (1) *A produced snapshot was never persisted*: `RaftNode::compact` trims only the in-memory log and the journal is append-only, so with no Snapshot record the boundary did not survive a restart and `recoverRaftState` replayed the entire history -- compaction bounding nothing and reclaiming nothing. `RaftGroup::compactLocked` now persists + syncs before anything depends on the prefix being gone (both crash windows safe). (2) *A coroutine lambda under `with_semaphore` read a DESTROYED closure*: `with_semaphore` moves `func` into a `.then` continuation destroyed as soon as the invocation returns a future -- i.e. at the FIRST suspension -- and a coroutine lambda's frame does not copy its captures, it points at the closure. compact() now suspends twice, so the second resumption read freed memory and `RaftNode::ready()` copied a garbage `Snapshot` out of it (std::bad_alloc on a vector with a nonsense length). Now a non-coroutine lambda delegating to a named member coroutine; residual D-33 for the siblings. **Restart over a compacted journal now works**, where `addVShard` previously threw "snapshot recovery not yet wired" -- correct while nothing compacted, and a fail-closed startup on the FIRST restart after this ships. The payload is deliberately NOT re-installed (it was built from THIS node's own on-disk TSM files at a flushed-only boundary; those survive and `Engine::init` already registered them, so re-installing would `addTSMFile` each AGAIN and count every point twice), but the core IS seeded with it via `seedRecoveredSnapshot` so a restarted leader can still serve a follower below its boundary. **Gate:** new `snapshot_durability_gate.sh`, a same-session A/B -- **compacting 195/195/195 of 200 acked vs 175/175/175 without, and 200/200/200 with no load phase at all**, i.e. compaction is the BETTER arm; see D-36 for the pre-existing loss the absolute bar exposes. Canonical bench 4,986,903 pts/s with `snapshots_taken=0` at the DEFAULT policy, so the trigger costs the write path nothing. 5 unit tests. Residuals D-33, D-34, D-35. | `replicated_vshard_host.{hpp,cpp}`, `engine_local_store.{hpp,cpp}`, `engine_data_state_machine.hpp`, `raft_group.{hpp,cpp}`, `raft_node.{hpp,cpp}` (`seedRecoveredSnapshot`), `cluster_data_plane.cpp`; gate `test/cluster_gates/snapshot_durability_gate.sh` | snapshot owner |
| ~~D-7~~ | **CLOSED** (`054d033`). `data/journal_format.hpp` gates journal emission on group 0's COMMITTED `activeFormatVersion`: `JournalFormatGate::writeBatchFormat()` returns write_record v2 only at/above `kJournalV2ActivationVersion`, v1 otherwise, and `encodeReplicatedCommand`/`encodeWriteCommand` name the gate instead of the v1 literal (which also preserves what that literal was for -- a change to `encodeWriteBatch`'s DEFAULT still cannot silently promote every journal entry). **Why per-peer negotiation cannot serve here:** a log entry goes to voters that never did the pairwise handshake, and the journal OUTLIVES the process -- a cluster restarting on a newer binary replays entries an older one wrote, and a cluster ROLLED BACK replays entries a newer one wrote. **(b) Fail-closed by construction, not by check:** the gate starts at v1, only ever RISES, and rises only on an explicit activation; no config key, env var or code path lets a node decide for itself (one node emitting v2 into a shared group's log makes that log unreadable to every un-upgraded voter in it). Monotonic, because group-0 state is rebuilt by REPLAYING its log and a mid-replay observer can transiently report an older version than the one already committed. **(a) Old journals stay decodable, verified:** `decodeWriteBatch` sniffs the v2 magic and falls back to v1 unconditionally; the gate governs EMISSION only. **(c) The Phase-2 pin is REPLACED, not deleted.** `WriteBatchArmIsPinnedToTheV1JournalFormat` asserted v1 unconditionally -- right while nothing could raise the format. Its successors keep the byte anchor in BOTH directions: gate off => byte-for-byte `encodeWriteBatch(b, V1)` and a modelled pre-v2 decoder accepts it; gate on => byte-for-byte `encodeWriteBatch(b, V2)`, smaller, and the pre-v2 decoder REFUSES it. Plus an old journal decoding with the gate raised, a v2 entry decoding with it lowered, and `encodeWriteCommand` agreeing byte-for-byte on both settings. **(d) "An old binary reads a v2 journal" is UNREACHABLE by ORDERING, which is why it is documented rather than tested:** `FeatureGate::canActivate` requires EVERY current voter to support v before the controller proposes; the activation is a group-0 COMMAND, observable only after it COMMITS; only then does any gate rise -- so the first v2 byte is written strictly after every voter could already read v2, and a binary that cannot read v2 cannot be a voter in a group whose gate has risen. The residual is a DOWNGRADE below the activation, an operator action against a committed cluster decision (lower the activation or re-image), not skew a format can absorb. `integration/journal_format_bridge.hpp` is the one call group 0 makes (`publishJournalFormat`, `invoke_on_all` because the gate is per-shard -- a shard left untold would silently keep emitting v1). **Not yet called from the running server** -- no live group 0 is composed into `ClusterDataPlane` -- which is precisely why no v2 byte can reach a journal today, now as a property of the wiring rather than of a hardcoded literal. This is also the machinery D-30 wants for ADR 0005 mechanism (c); it is now built and unblocks that whenever the guard is enabled. 4 unit tests (`ReplicatedCommandCodec`, `JournalFormatBridge`). | `lib/cluster/data/journal_format.hpp`, `replicated_command.cpp`, `write_record.hpp`, `lib/cluster/integration/journal_format_bridge.hpp` | 2c tail |
| D-9 | **The bypass is BUILT AND PROVEN; CheckQuorum itself stays OFF for this release, on a measurement.** (`82591a9` bypass, `c5bf5b0` forge guard + real-time lease, `77e0631` disable-only override; `5bdaa03` enabled it and the decision below turns it back off.) **What is done and staying in:** (1) ADR 0005 mechanism (b) — `RequestVote::campaignTransfer`, set at exactly one site, carried as a parameter not a member, honoured at the `inLease` guard, riding its own message-type byte so an older peer DROPS the envelope rather than misparsing it (asserted against a modelled pre-flag decoder); (2) a TimeoutNow is honoured only from the PRE-STEP believed leader — without which a forged TimeoutNow at the same term OR a higher one produced a transfer-flagged campaign from any peer and stood every voter's lease down at will (F1, demonstrated in review; vote safety was never affected, the guard was); (3) hibernation credits the passes it skips, so the lease expires in real time. **Transfers under CheckQuorum are proven at scale on this exact code** — an ON build of the final tree took rolling_rebalance's 2216-transfer storm with 600/600 OK, 0 errors and leadership settling at 0 moved. **Why the flag is off anyway:** same binary, same session, node_kill_round.sh, the flag the only difference (via the override) — **OFF 32/400 failed batches / 7 s recovery / 3.88 M pts/s, twice, identical; ON 50/400 / 11 s / 2.60 M and 59/400 / 13 s / 2.12 M.** The guard buys no safety (Raft never depended on it; commit needs a quorum ack and the propose deadline already bounds every write) — it buys PROMPTNESS under partition, and ~1.6-1.8x on the client-visible one-node-down band plus 4-6 s of extra failover is too much to pay for it when single-node failure is the commoner event. **The release ordering is the point:** the tag-8 DECODER ships now, so every node in a rolling upgrade to the NEXT release can already read a transfer vote, and enabling the guard then is one line (`kCheckQuorumDefault`) plus a re-run of node_kill_round — no wire change, no mixed-version window, and ADR mechanism (c) (D-30) becomes optional rather than blocking. Residual band cost and the next fix are D-29. **All five gates re-run on the FINAL (flag-off) binary:** rolling_rebalance PASS (2216 transfers, 600/600 OK, 0 errors, settled at 0 moved, 4.85 M pts/s), node_kill_round PASS (32/400, 7 s, 45/50 probes acked and all 45 readable on both survivors), fault_injection PASS (152 reset rounds / 392 connections, 2000/2000 + 200/200, 0 errors, 91 % of a proxied baseline), deposed_primary PASS (5 nodes, 300/300 accepted, 0 5xx, all five nodes read 300), backpressure PASS (16 x 503 all with Retry-After, 205 logged rejections at a 1 MB budget, 200/200 clean at the default), canonical bench **5,077,261 pts/s, 100/100, 0 HTTP errors, 0 reactor stalls**. The ON build was additionally run through rolling_rebalance for the 2216-transfer storm quoted above. **Scope note:** this is the DATA plane only. Group 0 (the control plane) never sets `checkQuorum` anywhere and so takes the `RaftOptions` default, which is `false` — and that is the right answer for it rather than an oversight: it is ONE group whose leader is the controller, and a controller that cannot reach a quorum is already fenced by the term-epoch machinery (`controller_epoch_fence.hpp`) rather than by a lease. Nothing in this row changes it, and a future change to the data-plane default must not be applied to group 0 by reflex. | `raft_messages.hpp`, `raft_codec.cpp`, `raft_node.{hpp,cpp}`, `raft_group_registry.{hpp,cpp}`, `cluster_data_plane.cpp` (`kCheckQuorumDefault`); `docs/adr/0005-checkquorum-transfer-bypass.md` | consensus owner |
| D-10 | **A real 5b (shared per-shard journal writer + group-commit coalescing)** — the largest measured stage, and the ONE remaining lever inside the quorum round. Design and its real cost (cluster-wide journal retention vs per-VShard directory deletion on movement) written up in 5.3. | plan §5.3 | perf owner |
| D-11 | **VShard:group consolidation** — ADR written, recommendation "not now, and not for throughput". One concrete thing to do TODAY regardless: put an explicit group id in the VShard directory, which is the difference between a rolling migration and a rebuild later. | `docs/adr/0004-vshard-group-consolidation.md` | placement owner |
| ~~D-12~~ | **CLOSED** (`76c388a`), and MEASURED FIRST — twice, on an idle 5-node RF=3 cluster (no writes), sampling the per-node `vshards_led` vector every 30 s. **Before:** phase A (12 min, background balancer only) decayed from a skewed start but never converged and never stopped: at minute 12 it was still moving **26-114 VShards per 30 s** and the spread had stalled at **~300 of a fair 819** (min 666, max 980). Phase B (5 min of `/cluster/rebalance-leadership?max=512` on every node every 5 s) reported **114 961 `transfers_initiated`** against roughly 1 500 VShards that actually changed hands — a 75x inflation, which is the second half of the defect: `RaftNode::transferLeadership` returns silently for a target that is not a VOTER of that group, and at RF=3 on 5 nodes 2 of the 4 candidate peers are not, so about half of every pass's budget was spent on transfers that could not happen while the counter reported them as initiated. **After:** phase A converges in **~60 s to [820 818 820 820 818]** (fair 819, spread 2-3) and then moves **ZERO VShards for the remaining 11 minutes**; phase B moves **0** and reports **782** transfers over the same 5 minutes. So the register's code reading was right about the direction and the earlier withdrawn "319 VShards every 2 s" figure remains withdrawn: the real behaviour was slow non-convergence plus permanent low-grade churn, not a storm. Two fixes, both in `rebalance`: (1) fair share is **per-node and membership-weighted** — `expected[v] = Σ 1/|voters(g)|` over the groups this shard hosts that `v` is a voter of, instead of `totalLed / peers.size()`, which at RF=3/N=5 was ~40% of the truth and made every node believe itself permanently above share; (2) a target must be a **voter of the group being transferred** and is chosen by trying candidates in turn rather than taking whatever the round-robin cursor points at (the caught-up/liveness predicate moved to `transferrableTo` for that). At RF == N every group has every node as a voter, so `expected[v] == hosted/N` for all `v` — the old formula exactly — which is why the 3-node gates are unaffected; `rolling_rebalance` re-run green after the change. | `lib/cluster/integration/shard_raft_plane.hpp` (`rebalance`, `transferrableTo`) | movement/M5 |
| ~~D-13~~ | **CLOSED** (`6314ab8`). Confirmed, and it was total: on a 5-node RF=3 cluster EVERY read failed, permanently, with `1638 VShard(s) have no elected leader` on every node (measured against the pre-fix binary through the extended gate). `gatherLeaders` recorded `host.leaderOf(vs)` for all 4096 VShards, and that answers `kNoNode` for a group the node does not replicate — the same value as "hosted, no leader elected" — so ~1638 of 4096 counted leaderless and `queryReplicated` failed the whole read `QUERY_INCOMPLETE` after its retries. The fix mirrors the WRITE path rather than inventing a second mechanism: `gatherLeaders` now returns ONLY hosted VShards (absent key = "not ours"; present `kNoNode` = "ours and leaderless", still fail-closed), and the rest are routed by the placement directory and named in a new `NodeQueryRequest::resolveVShards`. The holder — which does host them, so its registry knows — answers for what it LEADS and returns `NodeQueryPartial::redirects` for the rest, exactly as a stale-primary write reject carries a leader hint; the coordinator follows the hint within a separate 2-round redirect budget (a redirect is progress, not a wait, so it must not consume the leaderless retry budget) and caches it, so the steady state is one round trip. **The cache MUST be invalidated when its target becomes unreachable, and the first version of this fix did not do that** (found in review, corrected in `a19e666`): a hint was only ever dropped on a REPLY, and a dead node cannot reply, so a redirect that had named node X followed by X dying pinned every subsequent read to a corpse -- `QUERY_INCOMPLETE` naming X, permanently, since the placement map is immutable and the cache has no TTL. That is the project's known one-node-down read-unavailability shape, reintroduced through the cache; no gate covered it (the deposed-primary gate never kills a node, and the kill round is RF == N where the cache is never used). An earlier version of this row claimed "a stale entry costs one round trip" -- true only for a target that is alive but deposed; for a DEAD target it cost the read, forever. Both wire fields are OPTIONAL TAILS written only when non-empty, so an RF == N request and its reply are byte-identical to the pre-D-13 encoding. Contracts held explicitly: a redirected VShard is excluded from the holder's read filter (so it is never both answered here and re-asked elsewhere — no double count), a request whose entire filter was redirected never reaches the local store (an empty filter means "no restriction" downstream), redirects are accepted only for VShards that target was asked about, and a directory entry pointing back at US for a group our registry does not hold counts leaderless rather than being read locally without any leadership check. `counts()` got the same hosted-first gate, so `vshards_leaderless` is meaningful on any topology (still per-node: at RF < N each of the RF holders reports it). **Gate: `deposed_primary_gate.sh` now HARD-asserts reads** — all 5 nodes answer and each counts exactly the accepted writes (300/300), which fails on undercount (a dropped VShard) and on overcount (a double-counted replica) alike; verified DISCRIMINATING by rebuilding the pre-fix binary and re-running (5/5 nodes fail, `1638 VShard(s) have no elected leader`). Unit: `read_routing_test.cpp` (exactly-once routing against a strict-subset directory), `leader_filtered_node_store_test.cpp` (the holder side), codec tails in `node_query_test.cpp`, socket round-trip in `dataplane_rpc_enriched_test.cpp`. | `cluster_data_plane.cpp` (`queryReplicated`, `gatherLeaders`), `data/read_routing.hpp`, `data/leader_filtered_node_store.hpp` | query plan |
| ~~D-14~~ | **MATERIALLY IMPROVED** (`b91e4e4`, refined by `f3ce254`), by changing the election window rather than the all-or-nothing batch rule (both of which the row correctly called load-bearing — neither is touched). The band is now SCRIPTED as `test/cluster_gates/node_kill_round.sh`, which is what makes the numbers re-checkable; the old figures could only be quoted. **Same-session A/B, 400 batches x 10k, node 3 (leading 1364 of 4096) killed at t+3 s, 50 probe writes during the outage:** pre-batch binary (`6a63f54`) **396/400 failed, 4 OK, 10.8 k pts/s**, and the post-outage read FAILED `QUERY_INCOMPLETE "leader node(s) 3 unreachable"`; with D-13+D-12 only, **377/400, 23 OK, 5.5 k pts/s**, read still failed; with the fix, **41/400 failed, 359 OK, 3.22 M pts/s**, both survivors read back all 42 acked probe points, gate PASSED. (Note how much worse than the previously-quoted 33/200 the "before" is on this box under a 400-batch bench: the honest comparison is the same-session control, not the older figure.) Two changes, both bounded: (a) **an election-aware deadline** — when EVERY failure of an attempt is election-shaped (`isElectionWaitFailure`: NotLeader / LeaderRefused / LeadershipLost) the batch may retry to a 6 s cap with 16 attempts, paced 20 ms x3 then 400 ms so the retries span an election instead of burning six attempts in 120 ms. Judged PER ATTEMPT, so one `Transport` failure anywhere reverts the whole batch to the 1.5 s deadline: [D6] keeps its own budget, a dead peer is never waited on for 6 s, and quorum loss (leaderless everywhere, so election-shaped) waits out the cap and then fails closed. (b) **the write path now wakes groups behind an unreachable peer** (`ProposeSink::wakeGroupsLedBy` -> `RaftGroupRegistry::wakeFollowersOf`, local to the shard whose slices failed, rate-limited per peer), and it fires **at GIVE-UP TIME, not per failed attempt** — a distinction found by the reset gate rather than by reasoning. Waking per attempt puts ~1364 groups (a third of the map) on full-rate ticking for 8 s, and a follower ticking at full rate through a storm of dropped heartbeats times out its election and campaigns against a leader that is perfectly alive. Waking only when the batch has actually exhausted its budget means a RESET (absorbed inside the retry budget by design — that is what 4a is for) never touches Raft, while a peer that is really gone is woken within ~1.5 s of the first failed batch. Idle followers tick 1-in-10, which stretches a 2.5-5 s election timeout to 25-50 s; the READ path has always woken them before answering QUERY_INCOMPLETE and the write path had no equivalent, so a killed node's groups sat hibernating and the coordinator kept re-routing to the corpse — which is why the "before" run above never recovered inside the bench. **The cost, deliberately taken:** during a failover, batches that used to fail at 1.5 s now succeed at up to ~5 s, so p99 write latency in the window is worse and a waiting batch holds its `WriteAdmission` charge longer, which can push concurrent writes into 503-Overloaded sooner. Pinned by `ReplicatedBatchWriteRouterTest.{ElectionShapedFailureOutlastsTheBaseBudget, TransportFailureDoesNotGetTheElectionWindow, OneTransportFailureRevokesTheElectionWindow, UnreachablePeerWakesItsGroups, RecoveredBlipNeverWakes, LocalFailureDoesNotWake}` and `WriteRetryPacing.ElectionShapedClassesEscalateAfterTheFastRetries`. `fault_injection_gate.sh` re-run **green** (147 rounds / 396 connections destroyed, 2000/2000 + 200/200 probe, 0 errors, 93 % of a 4.95 M proxied baseline), so the D6 fix does not regress — but see the variance note below before reading a single run of that gate as a verdict. **The band is a DISTRIBUTION, and the two runs recorded for the post-fix binary (41/400 and, on a later run, 52/400) differ by RUN-TO-RUN VARIANCE and nothing else** — the wake path was byte-identical between them, so the 41→52 difference must not be attributed to any code change, and neither number is "the" figure. That is the same lesson D-21 files against the fault gate, and this band wants the same treatment: K runs and an assertion on the aggregate, not a single draw. Residual: the band is not zero and cannot be — a batch still fails if a slice misses even the 6 s cap. | `write_errors.hpp` (pacing + `isElectionWaitFailure`), `replicated_write_router.{hpp,cpp}`, `node_store.hpp`, `replicated_vshard_host.hpp`; gate `test/cluster_gates/node_kill_round.sh` | write-path owner. Pre-existing (111/400 on the pre-Phase-5 binary) |
| ~~D-15~~ | **CLOSED** (`0aa7d89`). The frame is now partitioned into one chain per group id — a stable sort by group id, so each group's records stay contiguous AND in frame order — and the chains run concurrently, bounded at 16. Ordering is owed **per group**, not per frame: messages within a group are order-sensitive, messages for different groups land in different RaftNodes on (often) different shards. Because group→shard is a function, a per-group partition is a refinement of the per-shard one, so two chains can never interleave deliveries into the same node. The no-raw-hook fallback stays sequential (it decodes and delivers on this shard — no round trip to hide, and its callers rely on whole-frame order). Pinned by `RaftRpcTransportTest.BatchDispatchKeepsGroupOrderAndRunsGroupsConcurrently`: 8 groups × 3 messages in ONE frame (asserted), group 1's first delivery blocked on a 40 ms sleep; same-group order must be 1,2,3 and something other than group 1 must finish first. Forcing the bound to 1 reproduces the old behaviour and fails that assertion. **Batching is still default-OFF and is NOT flipped here**; the batching-ON A/B was not re-measured, so whether this narrows the original regression is still open. | `lib/cluster/raft/raft_rpc_transport.cpp` (`kDeliverBatchVerb` handler) | perf owner |
| D-28 | **An uncommitted slice that NO reject names stays election-shaped by omission.** Filed from the Phase-5 review as F10 and not previously written down. `ReplicatedBatchWriteRouter`'s classification records a kind per REJECT; a target that answers `committed == false` with an empty reject list is labelled `NotLeader` (election-shaped, so it may ride the 6 s D-14 window), and a target that names SOME of its uncommitted slices leaves the unnamed ones carrying whatever the last named reject said. Neither is wrong today — both in-tree sinks (`ReplicatedVShardHost`, the hinted shard fan-out) name every slice they fail or none of them, so the omission case is UNREACHABLE — and the direction of the error is wait-longer, never ack: an unclassified slice is still uncommitted and still retried, so no ack can be manufactured by it. It is filed because a future sink that reports a strict subset would silently buy the election window for a failure that is not an election. Wants the unnamed remainder given an explicit class (probably `Transport`, the ambiguous default) rather than inheriting one. | `lib/cluster/data/replicated_write_router.cpp` (the reject/`lastKind` loop) | write-path owner |
| D-29 | **CheckQuorum still costs ~1.6-1.8x on the one-node-down write band, which is why it ships OFF; D-29(b) is DONE and was a win on its own.** (a) **Done** (`c5bf5b0`): the lease no longer depends on how often the driver gets round to a group — `RaftGroupRegistry::tickAll` credits hibernation-skipped passes to `RaftNode::tick(passes)`, so every tick-driven clock expires in real time. **Independently of CheckQuorum this improved failover**, because a dead leader's idle groups no longer wait out a 10x-stretched election timeout: with the guard OFF, node_kill_round went from 49/400 failed batches and an 8 s recovery to **32/400 and 7 s** (twice, identical). (b) **Still open:** with the guard ON the band is **50/400 / 11 s / 2.60 M pts/s and 59/400 / 13 s / 2.12 M** against that 32/400 / 7 s control — same binary, same session, flag the only difference. So something beyond the lease clock is still slower under the guard; the next suspects are the extra campaign round a refused vote costs (a candidate whose vote is dropped must come round at its own timeout) and CheckQuorum's own step-down interacting with who reports LeaderRefused during the window. Wants that measured before the flag is flipped, not after. **History worth keeping (F2):** the first fix for this was a wake hook — a vote the lease dropped un-hibernated the voter — which worked (62-69/400) but re-armed a 400-pass wake window on EVERY dropped vote, so a peer that kept campaigning could keep a group awake indefinitely, an unbounded re-arm reachable by a remote peer. Retired whole by (a); the counter and its registry plumbing are gone. `wakeFollowersOf` remains, but only for GRANULARITY (detection lands up to followerSkip passes late), not for magnitude. | `raft_group_registry.cpp` (`tickAll`), `raft_node.cpp` (`tick`); gate `test/cluster_gates/node_kill_round.sh`; unit `RaftGroupRegistryTest.HibernationDoesNotStretchTheLease` | consensus owner |
| D-30 | **ADR 0005's mechanism (c) — gated activation — is NOT implemented, and the release ordering makes it OPTIONAL rather than blocking.** The ADR wanted (b) AND (c): the new message-type byte makes the WIRE safe (an old voter drops a transfer vote rather than misparsing it), and a cluster-wide committed format version makes the BEHAVIOUR safe by keeping CheckQuorum off until every voter can read that byte. (c) is not built. What replaces it for now is ORDERING: the decoder ships in this release with the guard OFF (D-9), so by the time any release enables the guard, every node it can be rolling-upgraded from already reads tag 8 and there is no mixed-enable window to protect. That holds only while the enable stays a BUILD decision one release later than the decoder — an operator-visible enable knob would break it, which is why the runtime override is disable-only. (c) is still the right end state for any FUTURE guard-like behaviour change, and it is the same machinery D-7 waits on, so they should land together. | `lib/cluster/control/group0_state.hpp` (`activeFormatVersion`), `features::FeatureGate::canActivate`; `cluster_data_plane.cpp` (`kCheckQuorumDefault`) | consensus owner |
| D-31 | **The Raft payload bound is still 7x what a snapshot chunk needs, and the gap is entirely the PROPOSAL bound's.** After D-5 the size chain is 28 MiB payload / 32 MiB send / 64 MiB admission, sized for the largest single log ENTRY (at least one entry is always sent regardless of `maxAppendBytes`); a snapshot chunk is 4 MiB. Closing the rest means an oversized single-VShard slice must be SPLIT across several proposals rather than refused, which is a write-path change -- the slice stops being one atomic entry, and although the ack contract already promises no cross-VShard atomicity, splitting within a VShard is a new statement. Worth doing: the chain could then come down to roughly one chunk plus headroom, i.e. another ~3x. | `raft_types.hpp` (`kMaxRaftPayloadBytes`), `raft_group.hpp` (`kMaxProposalBytes`), the local propose path in `replicated_vshard_host.cpp` | snapshot owner + write-path owner |
| D-32 | **A VShard snapshot payload is materialized in RAM three times over**, which is the only reason `kMaxVShardSnapshotBytes` is 128 MiB rather than a disk-sized number. `buildVShardSnapshot` reads every referenced TSM file into one string, `RaftNode` holds it in `snapshot_` for as long as it is servable, and the receiver stages the whole thing in RAM before installing. Chunking bounded the MESSAGE, not the payload. Streaming to and from DISK (the producer writing chunks as it reads, the receiver staging to a temp file) makes the bound a disk bound and lets compaction proceed on VShards it currently refuses -- and it would also make the staging area survivable across a restart, which today it deliberately is not. | `engine_local_store.cpp` (`buildVShardSnapshot`), `raft_node.{hpp,cpp}` (`snapshot_`, `snapStaging_`) | snapshot owner |
| D-33 | **Every `with_semaphore` body in `raft_group.cpp` is a coroutine lambda, and they are safe only by accident.** `with_semaphore` moves `func` into a `.then` continuation that is destroyed as soon as the invocation returns a future -- i.e. at the FIRST suspension -- and a coroutine lambda's frame does not copy its captures, it holds a POINTER to the closure. Every capture read after a `co_await` is therefore read out of freed memory. `step`, `tick`, `campaign`, `propose`, `proposeConfChange`, `transferLeadership`, `waitApplied`, `readBarrier` and `proposeAndAwaitApplied` survive only because each touches its captures EXCLUSIVELY BEFORE its single `co_await drainReady()` -- a property nothing states, nothing tests, and any future edit can break. `compact` DID break it (D-6): adding a second suspension made `RaftNode::ready()` copy a garbage `Snapshot` out of the dead closure, a std::bad_alloc on a vector with a nonsense length. It is now a non-coroutine lambda delegating to a named member coroutine; the other nine want the same treatment, mechanically. The file's own header comment already warns about this class of bug in a different form. | `lib/cluster/raft/raft_group.cpp` (every `with_semaphore` site) | consensus owner |
| D-34 | **Journal SEGMENT reclamation is unwired, so compaction bounds replay but not disk.** D-6 compacts the in-memory Raft log and persists a Snapshot record, so `recoverRaftState` drops entries at or below the boundary on restart -- replay is bounded. But the sealed journal segments holding those entries are still on disk and nothing deletes them; a long-lived cluster's `cluster_raft/` grows without limit regardless of how often it snapshots. `JournalGc` exists as a brick (`lib/storage/journal_gc.hpp`, with `JournalRetention`) and was never given a caller. Wants the snapshot boundary published as the reclaim floor, plus the copy-forward rule the brick already documents. | `lib/storage/journal_gc.{hpp,cpp}`; producer at `replicated_vshard_host.cpp` (`snapshotVShard`) | snapshot owner |
| D-35 | **Compaction waits for a quiet moment because there is no per-VShard flushed watermark.** `hasUnconvertedStores` refuses to compact while ANY rolled store on the shard is awaiting TSM conversion -- correct (see D-6) and blunt: it is a per-SHARD condition standing in for a per-VSHARD one. Under sustained write load a shard may rarely have zero pending conversions, and every group on it then keeps its log. The precise answer is the highest revision R for which every revision <= R of THAT VShard is in TSM; the cheap route is probably a contiguous-completion watermark over `TSMFileManager::reserveSequenceId`'s store ordering (which already guarantees TSM seq order == store write order even when conversions finish out of order), rather than scanning stores per VShard. | `engine_local_store.cpp` (`hasUnconvertedStores`), `wal_file_manager.hpp` (`memoryStores`), `tsm_file_manager.hpp` (`reserveSequenceId`) | snapshot owner + storage owner |
| D-36 | **ACKNOWLEDGED WRITES ARE UNREADABLE AFTER A kill -9 THAT FOLLOWS A HEAVY WRITE CAMPAIGN. Pre-existing, not snapshot-related, and it violates the ack contract.** Found by `snapshot_durability_gate.sh` while establishing that D-6 does not cost durability. Same session, same box, 200 acked single-point probes, whole cluster kill -9'd and restarted: **with a 150x10k load phase and compaction ON, 195/195/195 readable; with the SAME load and compaction OFF, 175/175/175; with NO load phase at all, 200/200/200.** So kill -9 plus full log replay is sound on its own, the loss appears only after a heavy campaign, and COMPACTING IS THE BETTER ARM -- which is what rules D-6 out as the cause. The suspected mechanism, and the evidence for it, is restart replay tripping `rejectIfIngestBacklogged` inside apply: `EngineDataStateMachine`'s own header already warns that apply() can throw `IngestBacklogException` because it routes through `Engine::insertBatch`, which calls `rejectIfIngestBacklogged` unconditionally, and says "do not read this as never fires". Replaying ~1.5M points at startup is exactly that burst; the entry stays in the log and un-applied, so the point is durable but invisible. The gate's ingest-backlog log-line counts move inversely with readback (control 52 lines / 175 points, subject 26 lines / 195 points), which is consistent but not yet conclusive. **This is an ack-contract violation and should outrank most of this register.** Wants: an apply-path insert that bypasses admission control (admission belongs at PROPOSE time, which is where the design already puts it), and a repro that counts un-applied committed entries directly rather than inferring from a query. The absolute bar is reported by the gate as an advisory rather than asserted, precisely so this is not mistaken for a D-6 regression. | `engine_data_state_machine.cpp` (`apply`), `engine.cpp` (`rejectIfIngestBacklogged` on the insert path); gate `test/cluster_gates/snapshot_durability_gate.sh` | storage owner + snapshot owner. Pre-existing |
| D-18 | **No gate covers a SKEWED write workload against the leadership balancer.** Found while closing D-1: `rolling_rebalance_gate.sh` spreads 1000 hosts over 4096 groups, so at any balancer pass nearly every group is momentarily idle — which is why its numbers are byte-identical on binaries with and without the D-1 fix. The behaviour D-1 is about (matchIndex trailing lastIndex *continuously* on a group under sustained writes) only appears when the load concentrates on a few groups, and nothing measures that. Wants a variant with few series and a per-VShard check that leadership of the HOT groups actually moves, not just that the aggregate transfer count is reached. Until it exists, D-1's production effect is reasoned and unit-pinned, not measured. | `test/cluster_gates/rolling_rebalance_gate.sh` | movement/M5 |
| ~~D-16~~ | **CLOSED** (`d9a7f91`), and the filing undercounted by an order of magnitude. There were not two violations, there were **34 FILES**: `main` (ac3b455) is 100% clean under the pinned clang-format 21.1.6, so every one of them drifted on this branch and CI's `--Werror` would have failed on the lot, not on the named pair. Fixed with the one command (`find lib bin … | xargs clang-format -i`). Whitespace-only apart from two include re-sorts (`SortIncludes` moved `../cluster/data/node_metadata.hpp` and `placement_table.hpp` into their alphabetical slots); with all whitespace stripped, every other changed file's token stream is byte-identical. Tree-wide `--Werror` is now clean, and the pre-push hook agrees. | `lib/http/http_write_handler.cpp` and 33 others | anyone; one command |

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
