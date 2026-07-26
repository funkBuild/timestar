# Cluster write-path gates (write-scaleout Phase 3)

Live, multi-node gates for the replicated write path. They start real servers, drive real
load, and **exit non-zero when the property under test does not hold** — they are gates,
not probes, so they can be run from CI or a release checklist.

| script | proves |
|---|---|
| `deposed_primary_gate.sh` | a write to a VShard whose PLACEMENT PRIMARY is alive but no longer leader is answered with a retryable 503 at worst and NEVER an opaque 500 (write-scaleout 3a/3b) |
| `rolling_rebalance_gate.sh` | a leadership rebalance under sustained writes costs latency, not client errors |
| `backpressure_gate.sh` | the per-shard in-flight write bound degrades to 503 + `Retry-After`, never to 500s or timeouts, and the DEFAULT budget never gets in the way |
| `fault_injection_gate.sh` | a BURST of TCP connection resets between two live nodes costs latency, not client errors, and loses/duplicates nothing (write-scaleout 4c) |
| `restart_catchup_gate.sh` | a follower that was DOWN through a large write campaign catches up when it returns, under the tightened Raft admission bound (write-scaleout 5.4) |

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

`deposed_primary_gate.sh` uses **five** nodes at RF=3 on purpose. At RF=3 on THREE nodes
every node hosts every Raft group, so the router's `LeaderResolver` always knows the real
leader locally and the stale-primary path is unreachable — a 3-node run passes even on a
binary with no leader hint at all. With five nodes a coordinator hosts only ~3/5 of the
VShards and must fall back to the placement primary for the rest.

`backpressure_gate.sh` is fussy about sizing, and the script explains each choice
inline. The budget is charged on the REQUEST shard (whichever the HTTP connection
landed on), so four concurrent probes across four shards never collide — sixteen do.
A probe must be ONE series (a multi-series batch splits its charge across shards) and
must stay under the handler's own batch-entry cap (a 160k-entry request is a 400, not
a 503). Recovery is measured after restarting at the DEFAULT budget rather than by
lowering the load on the artificial one: the insert bench pipelines several batches
even at `--connections 1`, so "the load dropped" is not expressible at a budget small
enough for curl to trip.

`deposed_primary_gate.sh` hard-asserts only what Phase 3 owns -- zero server-side 500s,
zero crashes, and enough real leadership transfers for the run to be non-vacuous. The
accepted-write count is ADVISORY, because a rebalance storm leaves VShards genuinely
mid-transfer and a batch that touches one meets a leader that is standing down (the
`LeaderRefused` band; see the plan doc's D-14 for why batch fan-out makes a small
per-VShard window a visible per-request rate). An earlier version of this note blamed
"~319 VShards moving every 2s at RF < N" -- that figure is WITHDRAWN in the plan doc
(:404): it was measured with CheckQuorum temporarily enabled, and re-measured with it off
the balancer converges, slowly (deltas 586 -> 454 -> 258). Pre-Phase-3 this gate produced ~29%
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
2. Node 3 must LEAD at least 800 VShards. The first node to start wins every election, and
   a converged-but-skewed cluster left node 3 leading 128 of 4096 — 3% of traffic crossing
   the fault. The gate rebalances and waits for fair share first.
3. The baseline run through the proxy must itself be error-free, so a proxy bug cannot be
   read as a server property.

**The proxy is a handicap and the gate says so.** Absolute throughput through a Python
forwarder is not a server number, so the dip is asserted against a QUIET baseline measured
through the same proxy — not against an unproxied figure. (In practice the handicap is
small: 4.96 M pts/s baseline vs 5.0-5.1 M unproxied.)

**It is a discriminating gate, proven by A/B.** The same tree with only the three 4a
files reverted to `ad77cf3` (`write_errors.hpp`, `replicated_write_router.{cpp,hpp}` —
so 4b's jitter and keepalive are still present) FAILS it, under an identical fault:
147 reset rounds destroying 400 peer connections → **9 bench HTTP errors + 1 probe 5xx**,
every one of them `RetryableWriteError: N VShard slice(s) uncommitted after 6 attempt(s)
(last: transport)` — the [D6] signature verbatim. The Phase-4 binary takes the identical
storm (147 rounds, 392 connections) with zero. Throughput was 92% of baseline before and
94% after, i.e. the fix converts errors into a little latency and costs nothing else.

**Result on the Phase-4 binary:** 147 reset rounds destroying 392 peer connections
mid-bench → **2000/2000 bench requests OK, 200/200 probe writes OK, 0 HTTP errors, 0
server-side 500s, 0 crashes**; throughput 4.68 M vs a 4.96 M baseline (**94% retained**);
all 200 acked probe points readable **on every node**. The cost lands exactly where 4a
intends it to — p99 batch latency 121 ms → 175 ms, max 170 ms → 346 ms.
