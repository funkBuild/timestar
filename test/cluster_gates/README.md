# Cluster write-path gates (write-scaleout Phase 3)

Live, multi-node gates for the replicated write path. They start real servers, drive real
load, and **exit non-zero when the property under test does not hold** — they are gates,
not probes, so they can be run from CI or a release checklist.

| script | proves |
|---|---|
| `deposed_primary_gate.sh` | a write to a VShard whose PLACEMENT PRIMARY is alive but no longer leader is answered with a retryable 503 at worst and NEVER an opaque 500 (write-scaleout 3a/3b), **and** that reads work at all at RF < N: every node answers the same scatter-gather count (debt D-13) |
| `rolling_rebalance_gate.sh` | a leadership rebalance under sustained writes costs latency, not client errors |
| `skewed_rebalance_gate.sh` | the same, under a SKEWED load — 40 series, so the whole campaign lands on ~1% of the 4096 groups and those groups are continuously mid-append while the balancer storms (debt D-18). The concentration is MEASURED per VShard, from the per-VShard journal sizes, so the gate cannot silently decay into a copy of the rolling one |
| `backpressure_gate.sh` | the per-shard in-flight write bound degrades to 503 + `Retry-After`, never to 500s or timeouts, and the DEFAULT budget never gets in the way |
| `fault_injection_gate.sh` | a BURST of TCP connection resets between two live nodes costs latency, not client errors, and loses/duplicates nothing (write-scaleout 4c) |
| `restart_catchup_gate.sh` | a follower whose entire durable root is removed while DOWN catches up through a non-empty chunked snapshot plus the retained suffix, then reads an exact delete correctly; it also bounds transfer abandonment, server RSS, and crashes (write-scaleout 5.4 / CR-FIX-011) |
| `combined_fault_rebalance_gate.sh` | **(debt D-19)** TCP resets **and** a leadership rebalance **and** 4x the connections, all at once — the faults a single-fault gate cannot compose. It is `fault_injection_gate.sh` in combined mode, not a copy. Its finding: under the reset storm `transfers_initiated` collapses to ~0 because D-1's liveness filter refuses the peer behind the fault |
| `fault_injection_ab.sh` | **(expensive, on-demand — not a CI gate)** that `fault_injection_gate.sh` DISCRIMINATES: builds the 4a-reverted binary and asserts it fails the same storm HEAD passes |
| `node_kill_round.sh` | `kill -9` of one node MID-BENCH: no 500s, no crashes, every ACKED write readable afterwards on both survivors — and it prints the one-node-down 503 band (debt D-14), which stays advisory |
| `snapshot_durability_gate.sh` | TAKING SNAPSHOTS DOES NOT COST DURABILITY (debt D-6): a focused hot VShard is snapshotted on every replica, then every acked point is readable exactly once after the whole cluster is `kill -9`'d and restarted OVER COMPACTED JOURNALS |
| `restart_readback_gate.sh` | AN ACKED WRITE IS READABLE AFTER A RESTART THAT FOLLOWS A HEAVY CAMPAIGN (debt D-36). It re-reads REPEATEDLY, which is the whole point: a single read cannot tell 25 lost points from 25 durable ones that apply() has not reached, and that ambiguity is what left D-36 undetermined for a session. A count that climbs is a stall; one that stays flat is loss. Reports `apply_lag_entries` / `apply_failures` / `tick_errors` while it waits |
| `topology_mutation_gate.sh` | the authenticated production Group-0 routes move one non-vacuous VShard away and back twice under sustained unique writes, recover crashes after Engine-generation deletion and journal quarantine, reclaim an expired exact-v1 journal, rematerialize deleted storage from survivors, and reject unauthenticated drain or premature removal without losing or duplicating a contribution |
| `retention_failover_gate.sh` | exact-version clustered retention policy CRUD and tombstone recreation, a controller kill after durable partial fan-out, completion of one global cutoff sequence across all 4,096 VShards by a new Group-0 leader, measurement isolation, and old-controller restart recovery |
| `pattern_delete_failover_gate.sh` | an ambiguous coordinator crash only after another Group-0 voter applies a complete 6,000-target frozen plan, exact retry under a new leader without capturing a newly matching series, changed-body conflict, and killed-node restart recovery |
| `large_snapshot_streaming_gate.sh` | an exact-v1 snapshot of 128 MiB + 1 byte crosses leader hydration, v1 Raft framing and receiver disk staging in chunks no larger than 4 MiB, with a 1 GiB process limit and all temporary data under `build/tmp` |
| `delete_receipt_retirement_gate.sh` | a real canonical exact delete changes four seed points to three on every node, then sustained deletes cross the 1,024-receipt per-VShard capacity on every replica, advance the replicated retirement floor twice, preserve expired/retained retry outcomes, become snapshot-covered, reclaim production-sized sealed v1 journal segments, and retain only the current snapshot sidecar |
| `homogeneous_v1_rejection_gate.sh` | exact-v1 codec and real-socket handshake rejection, plus a production restart over a real acknowledged WAL whose version field is changed to an unsupported value; startup must exit before HTTP and preserve the source byte-for-byte |
| `mtls_peer_identity_gate.sh` | matching per-peer IP SANs converge and commit; with the other voter down, a certificate signed by the cluster CA for the wrong configured endpoint produces bounded 503, while rolling that node to a newly issued certificate for the correct endpoint commits that exact write and restores full health |
| `cluster_backup_restore_gate.sh` | an authenticated live export survives Group-0 leader loss, validates all 4,096 exact-v1 units, rejects wrong-key/missing/corrupt/extra artifacts, resumes a killed offline import, requires every prepared voter for release, rejects source authority, restores under fresh identities, and preserves the acknowledged baseline through full-cluster restart |
| `production_slo_report.sh` | serially runs the node-kill, bounded empty-node catch-up, and skewed-movement gates against one clean candidate, then binds their error-band, recovery, query-latency, snapshot, and movement measurements to the commit, authenticated server and benchmark hashes, resource settings, thresholds, and raw transcripts in one exact-v1 JSON report |
| `multi_host_candidate_preflight.py` | read-only preflight for an already deployed production candidate; proves distinct resolved hosts and failure domains, one exact clean server revision/cluster UUID/map epoch, complete RF=3 hosting and leadership, healthy Group 0, and zero apply/durability faults before and after infrastructure fault arms |

All of them take an optional server binary as `$1` (default
`build/bin/timestar_http_server`), so a "before" binary can be measured the same way.
Every node also receives an explicit `--memory` budget: 2 GiB by default for the
high-volume gates, overrideable with `GATE_SERVER_MEMORY`. Those gates retain four
server reactors, overrideable with `GATE_SERVER_SMP`; this keeps each shard above
the production 256 MiB free-memory admission floor. A 1 GiB/four-reactor process
begins at that floor and is not a valid load-test profile because it correctly
sheds every write.
The memory value is per process, so size the aggregate as
`node count * GATE_SERVER_MEMORY`; leaving it implicit lets every Seastar process
size itself from the whole host and can overcommit a multi-node gate before the
property under test is reached. Capacity qualification may raise both values after
provisioning the aggregate explicitly; a correctness or release run must not
silently reserve 8 GiB for each of three to five local processes. Focused
one-reactor gates pin their existing 1 GiB limit before loading the shared helper.

The benchmark driver is separately fixed at one reactor and 1 GiB by default
(`GATE_BENCH_SMP`, `GATE_BENCH_MEMORY`). It does not need one reactor per server
shard, and leaving its memory implicit used to reserve roughly another 2 GiB.

Every gate also uses `GATE_TMP_ROOT` for durable roots, retained transcripts, and
implicit process temporaries (`TMPDIR`). It defaults to the repository-local
`build/tmp`; override it only with a provisioned disk filesystem. Do not point it
at a memory-backed `/tmp`: the legacy load shapes can write tens of GiB, turning
their disk workload into raw-memory pressure and killing the harness instead of
measuring the intended fault.

`production_slo_report.sh` is the release collector, not another workload. It
requires a clean tracked worktree and runs the three underlying gates serially.
Before the first arm it requires both `timestar_http_server` and
`timestar_insert_bench` to identify their component and embed that exact clean
HEAD revision; after the final arm it proves neither binary's SHA-256 changed.
The report records both executable paths, revisions, and hashes because the
driver controls the offered load, error accounting, and readback verification.
Its report is retained at
`build/tmp/tsgate_slo_report/report.v1.json`, beside the complete arm logs. The
local safety defaults are a 50% post-kill write-error ceiling, 30 s for complete
leader recovery, 2 s for survivor query p99, 360 s for snapshot installation,
750 s for exact catch-up, 10% retained movement throughput, and 5 s for movement
batch p99. Set the corresponding
`GATE_MAX_*` values to the approved deployment SLOs before release
qualification; the report records the actual thresholds so a relaxed run
cannot be mistaken for production evidence.

`topology_mutation_gate.sh` is a low-volume correctness gate and deliberately
pins a smaller 1 GiB per-process default (4 GiB aggregate). Its four durable
roots live under `build/tmp`, not `/tmp`; overriding its budget should be a
recorded exception, and it must still run alone. It also pins a 120-second
server shutdown budget because a graceful stop closes roughly 4,096 Raft
groups; `GATE_SHUTDOWN_TIMEOUT_SECONDS` can override that recorded value. Crash
recovery readiness allows up to 600 seconds for group elections, while server
logs report local VShard-open progress every 256 groups so slow recovery is
distinguishable from a hang.

`retention_failover_gate.sh` uses the same low-volume shape with three 1 GiB,
one-reactor processes (3 GiB aggregate) and `build/tmp` roots. It must run alone
because it intentionally leaves one voter down while the other two propose to
thousands of groups. The gate waits for durable sweep ID/cutoff-record status,
not merely for a node-local controller counter, and removes all roots on exit.

`pattern_delete_failover_gate.sh` also uses three 1 GiB, one-reactor processes
and `build/tmp` roots. Its 6,000 one-point series keep the post-freeze fan-out
observable without load-test volume. It polls a different voter for the exact
plan/target count before killing the coordinator, and removes its three durable
roots plus bounded request workspace on exit.

`delete_receipt_retirement_gate.sh` uses three 1 GiB, one-reactor processes and
`build/tmp` roots. Its 1,100 sequential exact deletes carry a legal 2-KiB series
key so the hot VShard crosses the production 1-MiB private-journal rotation
target without a high-cardinality dataset. It waits through the normal five-
second snapshot sweep and one-minute journal-GC cadence, rejects superseded
canonical sidecar leaks, and removes all roots and its bounded response
workspace on exit.

`homogeneous_v1_rejection_gate.sh` runs one 1-GiB process at a time. Its codec,
socket, and production-restart arms keep every temporary and durable artifact
under `build/tmp`. The live arm changes only the version field of a real
acknowledged WAL, then requires a non-zero startup exit before `/health`, an
explicit unsupported-version diagnostic, and byte-for-byte source preservation.

`mtls_peer_identity_gate.sh` uses three 1-GiB, one-reactor processes and
repository-local roots. It generates a one-run CA and three distinct IP-SAN
identities plus a separately issued rotation certificate under `build/tmp`,
never exports private material, and removes the credentials with the node roots
on exit. The negative arm removes the other voter so the endpoint is required
for quorum; node 1 remains online while rolling the certificate restores the
exact blocked write, then the gate restarts the third voter and requires full
health.

`cluster_backup_restore_gate.sh` also uses at most three 1-GiB, one-reactor
processes. Source and restored clusters run sequentially on the same ports, all
offline negative/import/finalization arms run one process at a time, and every
root and artifact is under `build/tmp`. It generates owner-only exact-v1 test
keys inside that workspace and proves a valid archive is refused under a
different key before any import. The export archive is intentionally on storage
visible to each local process so a new Group-0 leader can resume the same
checkpoint; production multi-host qualification must provide the equivalent
durable shared mount or constrain resume to the original node.

## Run them ONE AT A TIME, with the previous run's data dirs deleted

These gates can be disk-hungry (the plan doc's MEASUREMENT HAZARD covers the quota-fence
case). With the retired tmpfs-era fault profile, running a battery back-to-back made
`fault_injection_gate.sh` fail with 929/2000 client errors, 8% of baseline throughput and
794 reset rounds after free space fell from 39 G to 13 G. That history is why every gate
still preflights its actual scratch filesystem and cleans its own roots; the current
durable profile is much smaller.

The failure mode is self-amplifying, which makes it very convincing as a "regression":
less headroom -> slower bench -> the 0.3 s resetter fires more rounds -> slower still. If
a gate fails, re-run it ALONE before believing it.

**Every gate now deletes its own data dirs in an EXIT trap** (`gate_cleanup` in
`cluster_gate_lib.sh`), so "with the previous run's data dirs deleted" is the default
rather than an instruction. It was not: only `restart_readback_gate.sh` cleaned up, and a
session using the old `/tmp` roots ran several gates back to back and walked that tmpfs
down to nothing. That is worse than a slow bench — exhausting a tmpfs consumes raw memory
and produces "Disk quota exceeded" in every process on the box, including the shell
driving the gate. One session lost its whole harness that way mid-run.

The tail of each node log is copied to
`$GATE_TMP_ROOT/tsgate_<prefix>_tails.log` before the delete, so a failed run is
still diagnosable; `GATE_KEEP_DATA=1` keeps everything when it is not. Check
`df -h "$GATE_TMP_ROOT"` and `ls -d "$GATE_TMP_ROOT"/tsgate_*` before and after
a run anyway — a gate killed between its `mkdir` and its trap leaves dirs behind.

Data reset is a VERIFIED operation, not a best-effort `rm -rf; mkdir`. The shared
`fresh_gate_data_dirs` helper accepts only direct
`$GATE_TMP_ROOT/tsgate_*` roots (never a nested or arbitrary path), retries a
removal race five times, proves every old root is absent, and only then recreates
it. A gate aborts before starting a node if that proof fails. This is
load-bearing for multi-arm gates: `snapshot_durability_gate.sh` once had extra `rm -rf`
calls between arms while the prior arm's post-crash readback servers were still RUNNING.
They predictably reported `Directory not empty`; worse, the script ignored that and went
on to print `GATE PASSED`. Those live deletions are gone. Each next arm now kills the prior
cluster and performs exactly one verified reset inside `run_arm`.

**Budget the disk before a run, not after.** Each bench writes
`batches * batch-size * 10` points and the cluster keeps them at RF=3; measured here that
is ~22 bytes per point per replica, i.e. ~6.7 G per 100 M points. `fault_injection_gate.sh`
runs K+1 benches against ONE cluster and nothing is deleted between them. Its current
1000-by-300, K=3 profile is about 12 M points before replication, or roughly 0.8 GiB at
that measured density; the old 27-GiB peak belonged to the retired tmpfs-era profile. The
gate requires 5 GiB free by default and exposes `GATE_MIN_FREE_GB` for deliberately larger
site headroom. Larger batch/storm overrides automatically raise the minimum from the
measured 22-byte density and cannot use that setting to weaken it. The historical 62 G
`/tmp` tmpfs is no longer used by default.

`restart_catchup_gate.sh` is now a focused correctness and resource gate. It
uses 96 awaited hot-series prefix writes to force the same non-empty VShard
snapshot on both surviving donors, appends one exact delete and eight retained
suffix writes, then starts a voter from a verified empty root. It requires
snapshot installation, exact all-group catch-up, 103-point readback, bounded
abandonment, zero undeliverable snapshots/500s/crashes, and keeps all roots,
responses, logs, and process temporaries under `build/tmp`. This shape replaced
the old four-million-point campaign that could consume tens of GiB without
proving the returning root was empty.

## Running a gate against a FLAGGED server: `GATE_SERVER_ENV`

Every gate prepends `$GATE_SERVER_ENV` to the `env` line that starts each node, so a
server-side flag can be exercised from the invocation instead of by editing a script:

```bash
GATE_SERVER_ENV="TIMESTAR_CLUSTER_SHARED_JOURNAL=1" ./fault_injection_gate.sh
```

Two properties are the point. The gate text stays byte-identical to the one whose recorded
numbers you are quoting against (an edited launch line makes the comparison unquotable, and
tends to survive into the next run unnoticed). And the flag appears in the transcript beside
the result, which is what a register row needs to cite.

It is word-split — a list of `KEY=VAL` pairs, values without spaces — and it is *prepended*,
so a gate's own setting for the same key still wins (`env A=1 A=2` takes the last).
`backpressure_gate.sh` pins `TIMESTAR_CLUSTER_WRITE_INFLIGHT_BYTES` for a reason and this
cannot override it.

`fault_injection_gate.sh` additionally reports the Raft journal's coalescing
(`journal_shared` / `journal_fsyncs` / `journal_sync_requests`, plus `GATE_METRIC
journal_coalescing`) after the storms. It is INFORMATIONAL — nothing asserts on it — and it
exists because that gate is the one debt D-10 named for a shared-journal run: the shared
writer widens a barrier failure's blast radius from one group to every group on the reactor,
and a reset storm is the fault most likely to produce one. The default per-VShard layout
reports 1.00 by construction (each group syncs its own fd; there is nothing to coalesce).

## `topology_mutation_gate.sh` — exact storage crash boundaries

This gate uses the normal server binary and production topology routes. Two
process-only environment variables arm a one-shot exit at a named durable
replica-retirement boundary:

```text
TIMESTAR_UNSAFE_TEST_RETIREMENT_CRASH=engine-wal-generation-deleted|journal-quarantined
TIMESTAR_UNSAFE_TEST_RETIREMENT_VSHARD=0..4095
```

The server rejects a partial pair, an unknown name, an out-of-range VShard, or
either variable outside the explicit local-test shape: Group 0 enabled together
with `development_allow_insecure_transport`. The gate requires exit status 86
and checks the corresponding disk shape, so a missing or renamed checkpoint
cannot silently turn into a happy-path pass. These are test controls, not a
protocol or persisted format; v1 remains the only wire and storage identity.

For the Engine boundary, the hook is also tagged with the operation purpose.
Ordinary snapshot install therefore cannot trigger a failpoint armed for replica
retirement. For the journal boundary, the exit occurs after the atomic rename and
both parent-directory barriers but before the grace marker is created. Restart
must complete each interrupted operation from the committed serving map.

The Engine-boundary run also guards fresh WAL publication. A prior run exposed a
final `.wal` name before its buffered v1 header was durable, so the intentional
exit left an empty file and strict restart correctly refused it. Fresh WALs now
flush the exact v1 header under `.wal.creating` and atomically link the final name
only afterwards; startup safely cleans either private-name crash residue. Thus a
checkpoint exit can leave no final WAL or a valid recoverable one, never an empty
published generation.

## Ports: every gate sits BELOW the kernel's ephemeral range (debt D-27)

A node binds three listeners — HTTP at `P`, the data plane at `P+1000`, Raft at `P+2000` —
and a gate starts its nodes in a burst, each dialling the others. With the gate's own ports
inside `ip_local_port_range` (32768-60999 here) the kernel can hand an earlier node's
OUTBOUND connection the very port a later node still has to bind: seastar exits on the
failed listen, one node silently never comes up, and the gate reports "cluster did not
converge" 300 s later with the cause visible only in a node log. `deposed_primary_gate.sh`
hit it four times in one session (49312 once, then 51312 on three consecutive runs) at five
nodes; the 3-node gates were exposed to the same race with fewer dials.

D-27 moved **every** gate below the range, and `require_ports_free` now ABORTS on a port
inside it (checked against the live kernel range, for all three listeners) so a new gate
cannot reintroduce the race.

| gate | HTTP ports | data (+1000) | Raft (+2000) | `kill_cluster` prefix | data dirs |
|---|---|---|---|---|---|
| `backpressure_gate.sh` | 19210-19212 | 20210-20212 | 21210-21212 | `1921` | `build/tmp/tsgate_bp*` |
| `rolling_rebalance_gate.sh` | 19220-19222 | 20220-20222 | 21220-21222 | `1922` | `build/tmp/tsgate_rb*` |
| `skewed_rebalance_gate.sh` | 19240-19242 | 20240-20242 | 21240-21242 | `1924` | `build/tmp/tsgate_sk*` |
| `deposed_primary_gate.sh` | 19310-19314 | 20310-20314 | 21310-21314 | `1931` | `build/tmp/tsgate_dp*` |
| `fault_injection_gate.sh` | 19410-19412 | 20410-20412 | 21410-21412 | `1941` | `build/tmp/tsgate_fi*` |
| `combined_fault_rebalance_gate.sh` | (runs `fault_injection_gate.sh` in combined mode — same ports, dirs and prefix; never run both at once) |||||
| `restart_catchup_gate.sh` | 19510-19512 | 20510-20512 | 21510-21512 | `1951` | `build/tmp/tsgate_cu*` |
| `node_kill_round.sh` | 19610-19612 | 20610-20612 | 21610-21612 | `1961` | `build/tmp/tsgate_nk*` |
| `snapshot_durability_gate.sh` | 19710-19712 | 20710-20712 | 21710-21712 | `1971` | `build/tmp/tsgate_sd*` |
| `restart_readback_gate.sh` | 19730-19732 | 20730-20732 | 21730-21732 | `1973` | `build/tmp/tsgate_rr*` |
| `topology_mutation_gate.sh` | 19810-19813 | 20810-20813 | 21810-21813 | `1981` | `build/tmp/tsgate_tm*` |
| `retention_failover_gate.sh` | 19830-19832 | 20830-20832 | 21830-21832 | `1983` | `build/tmp/tsgate_rt*` |
| `pattern_delete_failover_gate.sh` | 19850-19852 | 20850-20852 | 21850-21852 | `1985` | `build/tmp/tsgate_pd*` |
| `delete_receipt_retirement_gate.sh` | 19870-19872 | 20870-20872 | 21870-21872 | `1987` | `build/tmp/tsgate_dr*` |
| `homogeneous_v1_rejection_gate.sh` | 19890 | 20890 | 21890 | `1989` | `build/tmp/tsgate_v1*` |
| `mtls_peer_identity_gate.sh` | 19920-19922 | 20920-20922 | 21920-21922 | `1992` | `build/tmp/tsgate_ti*` |
| `cluster_backup_restore_gate.sh` | 19940-19942 | 20940-20942 | 21940-21942 | `1994` | `build/tmp/tsgate_br*` |

The prefixes are now four digits and unique per gate — they used to be three, so `492`
covered both `backpressure` and `rolling_rebalance` and `197` also matched `--port 19730`,
i.e. one gate's cleanup reached another's cluster. `rolling_rebalance_gate.sh`'s data dirs
moved from `tsgate_rr` to `tsgate_rb` in the same change, because they collided with
`restart_readback_gate.sh`'s — which `rm -rf`s them in its cleanup.

None of that is licence to run two gates at once: they fight over data dirs, disk and CPU
long before they fight over a pkill, and the self-amplifying disk failure above is what that
costs.

## Why the topologies differ

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

Both benchmark arms persist their complete stdout/stderr under `GATE_TMP_ROOT` and assert the
benchmark process exit code before parsing request counts. Command substitution used to
discard the only diagnostic when the benchmark died before printing its summary: the
gate then showed an empty anti-vacuity count even though server logs proved that a few
requests had arrived. An abnormal or timed-out client is now a named gate failure with a
retained transcript, never an unexplained empty field.

The gate waits for the public, cluster-aware `/health` contract both before and after its
large deterministic curl probes, and again after the default-budget restart. A balanced
leadership count permits a small transition tolerance and says nothing about apply lag;
the insert benchmark performs its own strict health preflight. Running it in that gap
used to print `ERROR: server health check failed`, return success, and yield no request
summary. The benchmark now exits non-zero for a failed health preflight (and for an
unknown wire format), so no caller can confuse “campaign never ran” with a clean result.

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
   **The floors are 35 rounds / 100 connections PER STORM**, multiplied by K, and they are
   roughly half of what a real durable run injects at the gate's own sizing (66-69 rounds
   destroying 266-276 connections per storm). They were 8/8 — about 5% of the earlier
   profile's observation, i.e.
   barely a vacuity check — until D-4, and 70/180 until the bench came down for K storms
   (D-21; at 2000 batches a storm injects ~147 rounds, which is what the A/B still uses).
   Half leaves room for a faster or slower box: the resetter fires on a fixed 0.3 s wall
   clock while the bench length is machine-dependent, so a machine that finishes the bench
   in half the time legitimately injects half the rounds. Override with
   `GATE_MIN_RESET_ROUNDS` / `GATE_MIN_RESET_CONNS` rather than editing, and record the
   observed counts when you do.
2. Node 3 must LEAD at least 800 VShards. The first node to start wins every election, and
   a converged-but-skewed cluster left node 3 leading 128 of 4096 — 3% of traffic crossing
   the fault. The gate rebalances and waits for fair share first.
3. The baseline run through the proxy must itself be error-free, so a proxy bug cannot be
   read as a server property.

**The proxy is a handicap and the gate says so.** Absolute throughput through a Python
forwarder is not a server number, so the dip is asserted against a QUIET baseline measured
through the same proxy — not against an unproxied figure. The current private-journal
control measured 171 kpts/s with zero errors; the three reset arms retained 85-90%.

**It is a discriminating gate, and `fault_injection_ab.sh` proves that on demand — it has
now actually been run (debt D-19).** It creates a `git worktree` at HEAD, applies a
two-anchor patch that undoes 4a's PACING and nothing else (`writeFailureRetryDelay` stops
doubling for the Transport/Overloaded classes, and the coupling `static_assert` that makes
exactly that a compile error is switched off — that assert *is* the fix's guarantee, so
disabling it is the honest inverse), builds a comparison `timestar_http_server` in its own
build dir beside the repo, runs the storm gate against both binaries in turn, and asserts a
**within-run separation** plus the [D6] signature.

It used to `git checkout fcb2a94^` the three 4a files instead. That still produces exactly
the intended 3-file diff — and then **does not compile**: later work needs
`WriteFailure::LeaderRefused` and `ReplicatedBatchWriteRouter::kElectionDeadline`, which the
pre-4a files do not define. The comparison binary that description implies had never
existed. The patch is also *narrower* than the checkout was, which reached past 4a and
dropped `d101c07`'s and `c052253`'s later fixes in those files.

Measured, two runs at the A/B's own sizing (2000 batches, K=2):

| draw | REVERTED | HEAD | separation | intensity ratio |
|---|---|---|---|---|
| 1 | `[3 10]` = 13, rc=1 | `[1 0]` = 1, rc=0 | 13x | 103% |
| 2 | `[16 11]` = 27, rc=1 | `[3 0]` = 3, rc=0 | 9x | 102% |

**The [D6] signature pins a CLASS both binaries can reach — it is not the discriminator.**
Every reverted-arm failure carried `uncommitted after 6 attempt(s) (last: transport)`, the
BASE attempt budget exhausted on the transport class. So did HEAD's: on one run's retained
logs the reverted arm carried 6 of that exact string and **the HEAD arm carried 2**, at the
same attempt count. HEAD reaches the transport class too, just far less often. The
discrimination is the COUNT separation in the table above, which is the assertion that
fails the script; the signature check is informational on both arms and prints both counts.

**The A/B storms harder than the CI gate, deliberately.** The signal scales with reset
ROUNDS. The current gate uses 1000 timed requests at 300 timestamps and K=3 so the private
durable journals stay out of overload; the A/B pins the same request size but doubles the
request count and uses K=2. The historical 1000-by-10000 A/B draws gave HEAD 0/4/2 against
REVERTED 7/22/4: the first two separated and the third overlapped. Those figures explain
the within-run ratio design, but they are not claimed as calibration for the new durable
profile; rerun the on-demand A/B before changing its ratio or the production gate budget.
Both arms always get the identical setting, and their observed intensity ratio is asserted.

**Why the claim is a ratio and not a threshold.** Two absolute floors were tried and both
were wrong: `3 * GATE_MAX_STORM_ERRORS` = 9 failed a correct REVERTED 7, and the absolute 5
that replaced it then failed on HEAD's own noisy `[0 4 0]`. The two arms of one run share a
box, a disk, a proxy and a storm within 3% of each other, so `REVERTED >= 3x HEAD` (floor 3)
cancels exactly the variance the absolutes kept tripping over. HEAD's own budget and exit
code are ADVISORY here — they are the gate's business, and an unlucky HEAD draw says nothing
about whether the A/B discriminated.

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

**Run it more than once before believing either answer (debt D-20, D-21).** The gate now
runs K storms per run and budgets their TOTAL, and the budget has a measured distribution
behind it: fifteen storm draws of one unchanged HEAD binary gave run totals **0, 1, 2, 0 and
4** (eleven zeros, three ones, one four). The budget is 6. It was 3 after the first nine
draws and a correct binary then drew 4 — the same non-reproducibility at a different
threshold, which is why the draws are printed beside the number. A single failing run still
does not identify a regression, and a single passing one does not clear one; the
discriminating claim is the A/B's within-run ratio above.

**Result on the Phase-4 binary**, five runs at the gate's own sizing (1000 batches, K=3):
**76-84 reset rounds destroying 217-243 peer connections per storm**, run totals of 0, 1, 2,
0 and 4 client errors, **0 server-side 500s and 0 crashes throughout**, every acked probe
point readable **on every node** in every storm, and 84-89% of the proxied baseline
(5.06-5.29 M pts/s) retained. The cost lands exactly where 4a intends it to: latency, not
errors.
