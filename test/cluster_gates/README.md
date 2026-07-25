# Cluster write-path gates (write-scaleout Phase 3)

Live, multi-node gates for the replicated write path. They start real servers, drive real
load, and **exit non-zero when the property under test does not hold** — they are gates,
not probes, so they can be run from CI or a release checklist.

| script | proves |
|---|---|
| `deposed_primary_gate.sh` | a write to a VShard whose PLACEMENT PRIMARY is alive but no longer leader succeeds via the leader hint + retry (write-scaleout 3a/3b) |
| `rolling_rebalance_gate.sh` | a leadership rebalance under sustained writes costs latency, not client errors |
| `backpressure_gate.sh` | the per-shard in-flight write bound degrades to 503 + `Retry-After`, never to 500s or timeouts, and throughput recovers |

All three take an optional server binary as `$1` (default
`build/bin/timestar_http_server`), so a "before" binary can be measured the same way.

## Why the topologies differ

`deposed_primary_gate.sh` uses **five** nodes at RF=3 on purpose. At RF=3 on THREE nodes
every node hosts every Raft group, so the router's `LeaderResolver` always knows the real
leader locally and the stale-primary path is unreachable — a 3-node run passes even on a
binary with no leader hint at all. With five nodes a coordinator hosts only ~3/5 of the
VShards and must fall back to the placement primary for the rest.

`rolling_rebalance_gate.sh` starts the third node LAST. `/cluster/rebalance-leadership`
only sheds leadership a node holds ABOVE its fair share, so storming an already-balanced
cluster initiates **zero** transfers and proves nothing. The script asserts on
`transfers_initiated` for exactly that reason.
