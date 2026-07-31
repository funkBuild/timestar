# Cluster write-path gates (write-scaleout Phase 3)

Live, multi-node gates for the replicated write path. They start real servers, drive real
load, and **exit non-zero when the property under test does not hold** — they are gates,
not probes, so they can be run from CI or a release checklist.

| script | proves |
|---|---|
| `deposed_primary_gate.sh` | a write to a VShard whose PLACEMENT PRIMARY is alive but no longer leader is answered with a retryable 503 at worst and NEVER an opaque 500 (write-scaleout 3a/3b), **and** that reads work at all at RF < N: every node answers the same scatter-gather count (debt D-13) |
| `rolling_rebalance_gate.sh` | a leadership rebalance under sustained writes costs latency, not client errors |
| `backpressure_gate.sh` | the per-shard in-flight write bound degrades to 503 + `Retry-After`, never to 500s or timeouts, and the DEFAULT budget never gets in the way |
| `fault_injection_gate.sh` | a BURST of TCP connection resets between two live nodes costs latency, not client errors, and loses/duplicates nothing (write-scaleout 4c) |
| `restart_catchup_gate.sh` | a follower that was DOWN through a large write campaign catches up when it returns, under the tightened Raft admission bound (write-scaleout 5.4) |
| `fault_injection_ab.sh` | **(expensive, on-demand — not a CI gate)** that `fault_injection_gate.sh` DISCRIMINATES: builds the 4a-reverted binary and asserts it fails the same storm HEAD passes |
| `node_kill_round.sh` | `kill -9` of one node MID-BENCH: no 500s, no crashes, every ACKED write readable afterwards on both survivors — and it prints the one-node-down 503 band (debt D-14), which stays advisory |
| `snapshot_durability_gate.sh` | TAKING SNAPSHOTS DOES NOT COST DURABILITY (debt D-6): under a light load, every acked point is readable after the whole cluster is `kill -9`'d and restarted OVER COMPACTED JOURNALS; the heavy-load A/B is advisory |
| `restart_readback_gate.sh` | AN ACKED WRITE IS READABLE AFTER A RESTART THAT FOLLOWS A HEAVY CAMPAIGN (debt D-36). It re-reads REPEATEDLY, which is the whole point: a single read cannot tell 25 lost points from 25 durable ones that apply() has not reached, and that ambiguity is what left D-36 undetermined for a session. A count that climbs is a stall; one that stays flat is loss. Reports `apply_lag_entries` / `apply_failures` / `tick_errors` while it waits |

All of them take an optional server binary as `$1` (default
`build/bin/timestar_http_server`), so a "before" binary can be measured the same way.

## Run them ONE AT A TIME, with the previous run's data dirs deleted

These gates are disk-hungry (the plan doc's MEASUREMENT HAZARD covers the quota-fence
case). Run back-to-back as a battery, `fault_injection_gate.sh` FAILED -- 929/2000 client
errors, 8% of baseline throughput, 794 reset rounds -- purely because free space had
fallen 39 G to 13 G across the preceding gates. Run alone with space free, the same binary
passes with 146 rounds and zero errors.

The failure mode is self-amplifying, which makes it very convincing as a "regression":
less headroom -> slower bench -> the 0.3 s resetter fires more rounds -> slower still. If
a gate fails, re-run it ALONE before believing it.

`restart_catchup_gate.sh` is a REGRESSION FENCE, and it says so in its own header: the
pre-5.4 binary passes it, because at 400 batches the log tail still fits under the old
1 GiB admission bound. What it fences is the 8x-tightened 128 MiB bound. Its client-error
count is ADVISORY for a measured reason -- writing to a 3-node RF=3 cluster with one node
down produces 50-201 of 400 bounded 503s, and the pre-Phase-5 binary produces 111/400, so
the number cannot credit or blame a change. That defect (every rejection on the
COORDINATOR, none on the node actually holding leadership -- i.e. leader RESOLUTION, not
consensus) is filed in the plan doc's Phase 5 outcome.

## Why the topologies differ

`deposed_primary_gate.sh` runs on **19310-19314**, not in the 493xx band the other gates
use, because its ports have to sit BELOW the ephemeral range — see `require_ports_free`'s
note in `cluster_gate_lib.sh`. That also means its `kill_cluster` prefix (193) no longer
collides with `fault_injection_gate.sh`'s.

`snapshot_durability_gate.sh` (19710-19712) and `restart_readback_gate.sh` (19730-19732) sit in the same
sub-ephemeral band for the same reason. `kill_cluster` matches the port as a SUBSTRING of the argv, so the durability gate's
prefix `197` also matches `--port 19730` — its cleanup reaches a readback run's cluster, and
`1973` is a subset of `197`, not a sibling of it. That is another reason the ONE AT A TIME rule
above is a rule and not advice; with only one gate live, anything else in the band is a stray
from a crashed run and killing it is the cleanup you want.

`deposed_primary_gate.sh` uses **five** nodes at RF=3 on purpose. At RF=3 on THREE nodes
every node hosts every Raft group, so the router's `LeaderResolver` always knows the real
leader locally and the stale-primary path is unreachable — a 3-node run passes even on a
binary with no leader hint at all. With five nodes a coordinator hosts only ~3/5 of the
VShards (2458 of 4096, measured) and must fall back to the placement primary for the rest.

That same topology is why this gate now asserts on READS (debt D-13). Until D-13 an
RF < N cluster could not serve a read at all: `gatherLeaders` recorded `leaderOf(vs)` for
every VShard, and that answers kNoNode for a group the node does not replicate — the same
value as "hosted, no leader elected" — so ~1638 VShards counted as leaderless and every
query failed `QUERY_INCOMPLETE` after its retries. The gate could not assert reads, which
is precisely how the defect survived to be found by code reading. The assertion is
DISCRIMINATING, and was re-checked that way: the pre-D-13 binary fails it on all five
nodes with `1638 VShard(s) have no elected leader`, HEAD returns 300/300 from each.

`backpressure_gate.sh` is fussy about sizing, and the script explains each choice
inline. The budget is charged on the REQUEST shard (whichever the HTTP connection
landed on), so four concurrent probes across four shards never collide — sixteen do.
A probe must be ONE series (a multi-series batch splits its charge across shards) and
must stay under the handler's own batch-entry cap (a 160k-entry request is a 400, not
a 503). Recovery is measured after restarting at the DEFAULT budget rather than by
lowering the load on the artificial one: the insert bench pipelines several batches
even at `--connections 1`, so "the load dropped" is not expressible at a budget small
enough for curl to trip.

`deposed_primary_gate.sh` hard-asserts what Phase 3 owns -- zero server-side 500s, zero
crashes, and enough real leadership transfers for the run to be non-vacuous -- plus, since
D-13, that reads work. The accepted-write count is ADVISORY, because a rebalance storm
leaves VShards genuinely mid-transfer and a batch that touches one meets a leader that is
standing down (the `LeaderRefused` band; see the plan doc's D-14 for why batch fan-out
makes a small per-VShard window a visible per-request rate). An earlier version of this
note blamed "~319 VShards moving every 2s at RF < N" -- that figure is WITHDRAWN in the
plan doc (:404): it was measured with CheckQuorum temporarily enabled. Re-measured with it
off, the balancer did NOT converge (26-114 VShards moving per 30 s after 12 idle minutes,
spread stalled at ~300 of a fair 819) -- and that is now FIXED and measured in D-12: it
converges in ~60 s to a spread of 2-3 and then stops. Pre-Phase-3 this gate produced ~29%
opaque HTTP 500s; it now produces zero 500s and a small number of honest 503s.

`rolling_rebalance_gate.sh` starts the third node LAST. `/cluster/rebalance-leadership`
only sheds leadership a node holds ABOVE its fair share, so storming an already-balanced
cluster initiates **zero** transfers and proves nothing. The script asserts on
`transfers_initiated` for exactly that reason.

## `fault_injection_gate.sh` — injecting [D6] rather than waiting for it

Phases 1-3 could only observe the write-collapse window statistically ("2 of 6 runs took
a 2-3 error burst"). This gate produces the fault on demand.

**The fault is a RESET, not a kill, and the distinction is the whole point.** A killed
node refuses connections, so reconnects fail and the write *should* eventually fail
closed — that is the node-kill round, and it is a different property. [D6] is the other
one: the peer is **healthy and its listener is open**, but an established connection
dies. seastar's `rpc::client` never re-dials, so `DataPlaneRpc` must notice and replace
it, and the write retry must OUTLIVE the 200 ms reconnect backoff it does that behind.
Pre-4a it did not — six attempts 20 ms apart all fit inside one backoff window, so every
attempt fast-failed on the same dead socket and the client got a 5xx.

`tcp_reset_proxy.py` carries one peer's traffic and, on `SIGUSR1`, destroys every
connection it holds with a real **RST** (`SO_LINGER {1,0}`), not a FIN — a clean shutdown
is the easy case. Its listening socket stays open throughout, so a reconnect succeeds
immediately; only established connections die.

**Topology.** Nodes 1 and 2 are told node 3 lives at `127.0.0.2` (the proxy); node 3 is
told it lives at `127.0.0.1`, where it binds. Same node ids, same placement, same
replication — only the address nodes 1 and 2 *dial* differs. That asymmetry is the only
way to put a proxy in front of a peer whose HTTP/data/Raft ports are a fixed offset apart
and whose own bind address comes from the same list. Only the data-plane and Raft ports
are proxied: node 3's HTTP listener binds `0.0.0.0`, so a proxy on `127.0.0.2:<http>`
collides with it and node 3 exits on the failed bind (the first version of this gate did
exactly that). The cluster planes only ever dial `port+1000` / `port+2000`.

**Three anti-vacuity assertions**, each of which a real earlier run of this gate failed:

1. `reset rounds` and `connections destroyed` must both be non-trivial. The first version
   gated the resetter on the bench still running, and the bench finished in under a
   second, so the storm never fired and every "0 errors" assertion passed vacuously. The
   bench, the resetter and the probe now run decoupled.
   **The floors are 70 rounds / 180 connections**, roughly half of what a real run
   injects here (147 rounds destroying 392-400 connections). They were 8/8 — about 5% of
   observed, i.e. barely a vacuity check — until D-4. Half leaves room for a faster or
   slower box: the resetter fires on a fixed 0.3 s wall clock while the bench length is
   machine-dependent, so a machine that finishes the bench in half the time legitimately
   injects half the rounds. Override with `GATE_MIN_RESET_ROUNDS` /
   `GATE_MIN_RESET_CONNS` rather than editing, and record the observed counts when you do.
2. Node 3 must LEAD at least 800 VShards. The first node to start wins every election, and
   a converged-but-skewed cluster left node 3 leading 128 of 4096 — 3% of traffic crossing
   the fault. The gate rebalances and waits for fair share first.
3. The baseline run through the proxy must itself be error-free, so a proxy bug cannot be
   read as a server property.

**The proxy is a handicap and the gate says so.** Absolute throughput through a Python
forwarder is not a server number, so the dip is asserted against a QUIET baseline measured
through the same proxy — not against an unproxied figure. (In practice the handicap is
small: 4.96 M pts/s baseline vs 5.0-5.1 M unproxied.)

**It is a discriminating gate, and `fault_injection_ab.sh` re-proves that on demand.**
The same tree with only the three 4a files reverted to `ad77cf3` (`write_errors.hpp`,
`replicated_write_router.{cpp,hpp}` — so 4b's jitter and keepalive, which live in
`reconnect_policy.hpp`, are still present) FAILS it, under an identical fault: 147 reset
rounds destroying 400 peer connections → **9 bench HTTP errors + 1 probe 5xx**, every one
of them `RetryableWriteError: N VShard slice(s) uncommitted after 6 attempt(s) (last:
transport)` — the [D6] signature verbatim. The Phase-4 binary takes the identical storm
(147 rounds, 392 connections) with zero. Throughput was 92% of baseline before and 94%
after, i.e. the fix converts errors into a little latency and costs nothing else.

That was a hand-run session claim with no way to re-check it. It is now scripted
(D-4). `fault_injection_ab.sh` creates a `git worktree`, applies

```
git checkout fcb2a94^ -- lib/cluster/data/write_errors.hpp \
                         lib/cluster/data/replicated_write_router.cpp \
                         lib/cluster/data/replicated_write_router.hpp
```

builds `timestar_http_server` in its own build dir, runs the storm gate against both
binaries in turn, and asserts the reverted one produces client errors **carrying the
[D6] signature** while HEAD produces zero — plus that both runs took a comparable storm
(within 2x), since comparing a heavy storm to a light one says nothing.

Two things to know before running it:

- **It is expensive and it is NOT a CI gate.** A fresh comparison build dir builds
  seastar too (tens of minutes); then it runs the full storm gate twice. Keep
  `GATE_AB_BUILD_DIR` between runs to make the rebuild incremental. CI runs
  `fault_injection_gate.sh`; this validates that gate, on demand.
- **The revert reaches past 4a.** A hunk-level `git revert fcb2a94` conflicts — two later
  commits (`d101c07`, `c052253`) touch the same lines — so the whole-file checkout also
  drops those. That is why the script asserts the *signature* and not merely that errors
  appeared: `last: transport` is specific to the retry-pacing defect.

It never touches your working tree; the revert happens in the worktree.

**Run it more than once before believing either answer (debt D-20).** Measured while
gating D-14: three consecutive runs of this gate on the SAME HEAD binary gave 3 errors, 3
errors, then 0 — with a comparable storm every time (147-151 rounds, 396-405 connections)
— and the pre-batch control binary in the same session produced 6 storm errors, 2 probe
5xx, and 257 errors in its QUIET baseline run (the one with no fault injected at all).
A single failing run does not identify a regression here, and a single passing run does not
clear one.

**Result on the Phase-4 binary:** 147 reset rounds destroying 392 peer connections
mid-bench → **2000/2000 bench requests OK, 200/200 probe writes OK, 0 HTTP errors, 0
server-side 500s, 0 crashes**; throughput 4.68 M vs a 4.96 M baseline (**94% retained**);
all 200 acked probe points readable **on every node**. The cost lands exactly where 4a
intends it to — p99 batch latency 121 ms → 175 ms, max 170 ms → 346 ms.
