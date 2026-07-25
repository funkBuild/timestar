# Cluster write-path gates (write-scaleout Phase 3)

Live, multi-node gates for the replicated write path. They start real servers, drive real
load, and **exit non-zero when the property under test does not hold** — they are gates,
not probes, so they can be run from CI or a release checklist.

| script | proves |
|---|---|
| `deposed_primary_gate.sh` | a write to a VShard whose PLACEMENT PRIMARY is alive but no longer leader is answered with a retryable 503 at worst and NEVER an opaque 500 (write-scaleout 3a/3b) |
| `rolling_rebalance_gate.sh` | a leadership rebalance under sustained writes costs latency, not client errors |
| `backpressure_gate.sh` | the per-shard in-flight write bound degrades to 503 + `Retry-After`, never to 500s or timeouts, and the DEFAULT budget never gets in the way |

All three take an optional server binary as `$1` (default
`build/bin/timestar_http_server`), so a "before" binary can be measured the same way.

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
accepted-write count is ADVISORY, because the leadership balancer does not converge at
RF < N (~319 VShards move every 2s on an idle 5-node cluster; see the plan doc), so a few
writes always meet a genuinely mid-transfer VShard. Pre-Phase-3 this gate produced ~29%
opaque HTTP 500s; it now produces zero 500s and a small number of honest 503s.

`rolling_rebalance_gate.sh` starts the third node LAST. `/cluster/rebalance-leadership`
only sheds leadership a node holds ABOVE its fair share, so storming an already-balanced
cluster initiates **zero** transfers and proves nothing. The script asserts on
`transfers_initiated` for exactly that reason.
