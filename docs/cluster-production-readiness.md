# Cluster production-readiness review and fix-up plan

**Status:** BLOCKED — no clustered deployment mode is approved for production

**Reviewed baseline:** `cluster-design` at `f78e05d` (2026-08-01)

**Remediation commits:** `95c10d2`, `a16b03a`, `d578e81`, `ddab705`,
`8620b9e`, `20639dc`, `ea2511b`, `3ac9899`, `69ac879`, `09a62c5`,
`2e06cb8`, `7151f5d`

**Scope:** The recent VShard/Raft cluster redesign, its production-server
integration, the public HTTP surface, recovery paths, and the release evidence
recorded in the cluster plans.

This is the authoritative release-blocker list for the cluster redesign. The
architecture and integration documents remain useful descriptions of the target
and its milestone sequence, but their older completion statements must not be
used as evidence that the current server is production-ready.

## Implementation progress after the review

Twelve remediation commits are now recorded. Cluster release status remains
**BLOCKED** because group 0/movement, generation-atomic live snapshot
replacement, pattern-delete expansion, replicated retention, the large-snapshot
path, and rolling wire-format compatibility remain open. The four previously
stale live release gates now pass on the same executable candidate and no longer
block release by themselves.

Completed and covered in this pass:

- Unsupported partitioned delete forms, retention mutations, `/subscribe`, and
  unwired read consistency modes fail closed before local work. The M1 Compose
  file is marked development/demo-only.
- Leader reads now take a quorum-confirmed ReadIndex; RF&lt;N routing can retry an
  alternate assigned replica; read RPCs share a bounded deadline.
- Storage-backlog admission runs before proposal, while committed apply/replay
  bypasses transient backlog rejection. Proposal owns its command across the
  admission suspension, preventing temporary-command use-after-free.
- Journal recovery validates cluster UUID, core, embedded/filename segment
  number, and corrupt non-empty finals; prior-process boot IDs remain replayable.
- Replicated server startup requires a configured 128-bit cluster UUID and mTLS
  files by default. The server persists `node.json`, binds the exact static RF +
  ordered peer list, uses a fresh boot ID, and passes the same certificate policy
  to both data-plane and Raft transports. Plaintext requires an explicitly named
  development-only override.
- `/health` now fails on unresolved peers, zero/leaderless hosting, apply lag or
  failures, Raft tick failures, disabled snapshot production, oversize snapshot
  refusal, and undeliverable snapshots. Unsupported SMP layouts fail startup.
- The leadership-rebalance mutation requires the configured bearer token;
  `/cluster/status` remains intentionally readable for liveness diagnostics and
  must be protected at the network/proxy boundary if topology is sensitive.
- The unit harness restores synthetic placement tables after each test. This
  fixes an order-dependent crash where a four-core test mapping leaked into a
  later two-core Engine and routed work to nonexistent core 3.
- Snapshot payload v2 now carries a deterministic series catalog bound by a real
  content hash. Snapshot creation materialises one VShard-pure TSM,
  applies tombstone sidecars, detects a concurrent tombstone mutation, and
  refuses missing/non-canonical metadata. Fresh-replica install reconstructs
  NativeIndex metadata, durable value types, and exact day membership, so normal
  series discovery works after catch-up.
- Snapshot install validates payload VShard, extent/file agreement, VShard
  purity, tombstone absence, wire basenames, catalog/data identity, and content
  hashes. It allocates receiver-local immutable names and isolated staging
  directories. A truly identical resident object is retryable after interruption;
  any different TSM, WAL/memory, or index-only state fails closed until a real
  generation swap exists.
- Partitioned production startup now enables VShard-partitioned compaction before
  the background loop. TSM sequence allocation is restored above both filename
  sequence and data sequence, preventing a post-snapshot local write from being
  ranked permanently below the installed generation.
- VShard compaction and snapshot reads now apply tombstones, detect a concurrent
  tombstone mutation, and produce clean VShard-pure output. Exact-point TSM
  deletes use the same inclusive range contract as tombstone queries, closing a
  pre-existing `[t, t]` delete hole found by the new compaction regression.
- Source-inspection tests use build-provided absolute paths, so their result no
  longer depends on whether the binary is launched by CTest, from `build/test`,
  or from the repository root.
- RF&gt;1 exact-series deletes now route from the public HTTP handler to the
  VShard's current Raft leader and acknowledge only after quorum commit and
  apply. The command path carries corrected-leader hints, attempt deadlines,
  retry classification, v3 wire negotiation, and VShard identity validation at
  originating, peer-ingress, and state-machine boundaries. RF=1 and pattern
  deletes remain fail-closed; a mixed exact/pattern batch is rejected before any
  proposal. Legal 10,000-target batches are capped at 32 concurrent quorum
  waits rather than creating an unbounded proposal fan-out. An ambiguous
  transport or leadership-loss result is never re-proposed: HTTP reports 504,
  `DELETE_OUTCOME_UNKNOWN`, and no `Retry-After`, because a second unbounded
  delete could erase a write ordered after the first attempt.
- Replicated startup now raises a low soft `RLIMIT_NOFILE` to 8,192 when the
  process hard limit permits it and otherwise fails before opening Engine or
  Raft state with a `LimitNOFILE`/`ulimit` diagnostic. `ClusterDataPlane::start`
  unwinds partial sharded startup before preserving its original exception, and
  the server's lifecycle guard remains armed through data-plane/HTTP startup and
  normal shutdown. This prevents an `EMFILE` during VShard journal creation from
  being replaced by Seastar's misleading destructor `SIGILL` trap.
- Live gates now prove their data roots are absent before recreating them, never
  reset data while an arm's servers are still running, and fail if the reset is
  incomplete. Cluster health is required before benchmark load begins, and the
  insert benchmark exits nonzero when its health preflight or argument parsing
  fails. This prevents an empty benchmark transcript from satisfying a gate's
  anti-vacuity checks.

Focused evidence on this pass includes the rebuilt server, unit and socket test
targets, journal negative tests, alternate-replica routing tests, a black-holed
read deadline test, ReadIndex tests, HTTP fail-closed/auth tests, persistent
identity/topology tests, and pre-proposal admission tests. The strict checkboxes
below stay open where their stated multi-process or fault-injection “done when”
evidence has not yet been run.

Final local validation for these remediation commits is green: 4,341/4,341 unit
tests passed with no skips, 45/45 socket-backed cluster tests passed, the
first-pass 56/56 focused
cluster/readiness/identity/admission regressions passed, and the second-pass
24/24 snapshot/compaction regressions passed. The exact-delete pass additionally
covers public HTTP behavior, bounded batch fan-out, leader-hint retry, v3 socket
transport, old-peer refusal, VShard-spoof rejection, and real Raft
write/delete/query apply. The production server links, every cluster-gate shell
script passes `bash -n`, and `git diff --check` passes. The four named live
multi-process gates also pass on `2e06cb8`; the additional topology, security,
large-snapshot, and non-empty live-install gates remain open.

## Decision

The redesign has a substantial replicated write path, and the reviewed tree
passes 4,308 unit tests and 40 socket-backed cluster tests. It is nevertheless a
production **no-go**. Mutations outside `/write`, snapshot recovery, leader-read
safety, topology changes, inter-node identity/security, and several failure
paths do not yet meet the architecture's own contracts.

The result applies to all currently documented clustered modes:

- Milestone-1 full replication is asynchronous, best effort, and has no
  consensus. It is a development/demo mode, not a durable cluster.
- Partitioned RF=1 has no redundancy and still exposes cluster-unaware APIs.
- Partitioned RF>1 has a real Raft write path, but the blockers below prevent a
  production deployment.

Severity means:

- **P0:** can return incorrect/stale data, diverge durable state, defeat cluster
  identity/security, or make supported topology operations unsafe.
- **P1:** can cause unbounded hangs, sustained unavailability, unbounded resource
  growth, or expose a public API with misleading cluster semantics.
- **P2:** release evidence, operability, or conditional hardening required before
  the relevant feature can be enabled.

## Findings

The findings below describe the reviewed baseline and follow-on remediation
reviews. Current mitigations and the remaining evidence needed to close each
item are recorded in the fix-up list.

| ID | Severity | Finding | Production impact |
|---|---|---|---|
| CR-01 | P0 | Deletes bypass Raft | Successful deletes can be absent on a replica and later reappear after failover. |
| CR-02 | P0 | VShard snapshots omit catalog/index and tombstone state | Snapshot catch-up can install data that is undiscoverable or resurrect deleted points. |
| CR-03 | P0 | Live snapshot installation collides with existing TSM ranks | A verified snapshot can be invisible to the running Engine or overwrite paths still backed by old open files. |
| CR-04 | P0 | Group 0, persistent cluster identity, and topology-change wiring are absent | Peer-list edits remap data without movement; safe join, drain, remove, and replace are unavailable. |
| CR-05 | P0 | Leader reads do not take a quorum ReadIndex | A partitioned former leader can serve stale data after a majority elects a new leader. |
| CR-06 | P1 | Ingest backlog admission happens during committed-entry apply | A committed entry can repeatedly fail apply, causing ambiguous client results and repeated persistence/fsync work. |
| CR-07 | P1 | RF&lt;N reads lack replica fallback and RPC deadlines | A dead static primary makes live replicas unusable; a black-holed peer can hang a query indefinitely. |
| CR-08 | P0 | Retention mutations/cutoffs are node-local | Replicas can expire and compact different logical data. |
| CR-09 | P1 | Streaming silently remains node-local | `/subscribe` returns an incomplete stream in a partitioned cluster without warning. |
| CR-10 | P0 | Inter-node mTLS and authenticated operator actions are not wired | Consensus/data traffic is plaintext and unauthenticated; leadership rebalance bypasses HTTP authentication. |
| CR-11 | P0 | Journal identity fields are decoded but not enforced | Renamed, swapped, wrong-core, or foreign-cluster segments can be replayed instead of fencing startup. |
| CR-12 | P1 | Snapshot production has hard scale and SMP topology failure modes | A VShard over 128 MiB or a non-cohesive core count leaves Raft logs growing without bound. |
| CR-13 | P1 | `/health` is not cluster-aware | An orchestrator can send traffic to a leaderless, unresolved, or apply-stalled node reported as healthy. |
| CR-14 | P1 | Requested read-consistency modes are accepted but ignored | `session` and `bounded_staleness` do not implement their advertised contracts or return session envelopes. |
| CR-15 | P2 | The supplied Compose deployment is still the unsafe M1 mode | Operators can mistake a best-effort demo for the redesigned RF=3 deployment. |
| CR-16 | P2 | Required live release gates were stale after later cluster changes | Backpressure, node-kill, restart-catch-up, and snapshot-durability behavior had no same-candidate evidence. |
| CR-17 | P1 | One-node failover still causes client-visible batch failures | Applications without the promised retry-whole-batch behavior can treat an expected node failure as lost writes. |
| CR-18 | P1 | Snapshot payload v2 has no rolling-version negotiation | A mixed-version cluster can fail snapshot catch-up in either direction during upgrade. |
| CR-19 | P0 | Exact-point delete overlap was exclusive at the block minimum | A delete with equal start/end could silently skip a one-point block and leave the point queryable. |
| CR-20 | P0 | Ambiguous replicated deletes were automatically re-proposed | If the first delete committed but its reply was lost, a second log entry could erase a concurrent write ordered after the first attempt. |
| CR-21 | P1 | Replicated startup exhausted ordinary open-file limits and did not unwind partial sharded startup | A default 1,024 soft limit failed around the thousandth VShard journal, then cleanup trapped with `SIGILL`, hiding the actionable `EMFILE` and preventing the node from booting. |
| CR-22 | P2 | Live-gate data reset and benchmark preflight could fail open | A gate could race deletion against running nodes or continue after the benchmark sent no load, invalidating otherwise green release evidence. |

### CR-01 — deletes bypass consensus

Partitioned startup installs hooks for query, write, and metadata only in
[`timestar_http_server.cpp`](../bin/timestar_http_server.cpp). The delete handler
applies directly to the local Engine and then calls the Milestone-1
[`ClusterGateway`](../lib/cluster/integration/cluster_gateway.hpp), whose contract
is asynchronous best-effort HTTP forwarding with swallowed peer errors.

This loses Raft ordering relative to writes and permits a `2xx` response while a
replica is missing the tombstone. The dormant `DeleteRangeKey` command in the
replicated state machine does not help because the public delete path never
proposes it.

The follow-on storage review also found that TSM overlap detection used
`block.minTime < endTime` even though tombstone ranges are inclusive. An exact
delete of a one-point block therefore returned false and persisted no tombstone.
CR-FIX-016 corrects the boundary and covers it through the tombstone-resolving
VShard compaction regression; this does not close the separate Raft-routing gap.

Commit `ddab705` closes that routing gap for exact targets in RF&gt;1 mode. The
HTTP path builds the canonical series key, routes a `DeleteRangeKey` through the
current VShard leader, and returns success only after commit and apply. It does
not claim that the series existed: `seriesDeleted` is explicitly the number of
exact commands committed and applied, avoiding a racy pre-read. Pattern
expansion remains unavailable because it requires a placement-pinned catalog
view; RF=1 and pattern requests return an unsupported response before local
mutation, and a mixed batch is preflighted before any exact command is proposed.
CR-FIX-010 remains open for the named live leader-failure/restart evidence and
for pattern-delete semantics.

### CR-20 — ambiguous delete retries could erase concurrent writes

The shared failure taxonomy already recorded that `LeadershipLost` and
`Transport` are ambiguous: the proposal may have committed even though the
caller received no acknowledgement. That ambiguity is safe for byte-identical
writes under the current LWW contract, but not for a non-revision-bounded
physical range delete. The new command router initially reused the write retry
policy and could
therefore append the same delete again after another client had written into the
range.

Commit `3ac9899` stops after the first ambiguous delete result while preserving
unambiguous not-leader, leader-refused, stopping, and pre-proposal overload
retries. The HTTP contract reports an explicit unknown outcome without automatic
retry advice. This closes the in-request duplicate-delete hazard; durable
operation IDs or revision-bounded tombstones are still required before an
ambiguous client-initiated retry can be declared safe under CR-FIX-010.

### CR-02 and CR-03 — snapshot contents and live installation were unsafe

[`SnapshotPayload`](../lib/cluster/data/snapshot_payload.hpp) carries a manifest
and raw TSM file bytes. The producer in
[`engine_local_store.cpp`](../lib/cluster/integration/engine_local_store.cpp)
passes an all-zero catalog hash and does not export NativeIndex/catalog records.
The round-trip test documents that it verifies the installed data through a
precomputed series ID rather than normal series discovery.

The snapshot reader also requires deletes/tombstones to have already been
materialised into the TSM view, while the producer does not enforce that
precondition or ship tombstone sidecars. The architecture promises a snapshot
containing catalog, index extract, data extents, and tombstone objects; the
current payload does not meet that contract.

Installation copies source filenames into the live TSM directory. If a rank is
already registered, [`TSMFileManager::addTSMFile`](../lib/storage/tsm_file_manager.cpp)
keeps the old open object and closes the new one. In addition, VShard-partitioned
compaction is described in the integration plan but is not enabled by the
production server, so a shipped TSM may contain data outside the target VShard.

The current snapshot-safety pass closes the silent versions of these failures:
payload v2 includes the exact catalog, creation ships a resolved VShard-pure
object, and the receiver uses local names and rejects mixed/non-empty state. It
also makes an exact data publication retry idempotent, which covers the normal
crash/replay boundary between file publication and catalog reconstruction.
CR-FIX-011/012 remain open because data visibility plus index publication is not
a single fenced generation swap, live replacement is deliberately unsupported,
and crash injection has not covered every publication boundary. The present
producer emits at most one file and the receiver rejects multi-file payloads;
removing that restriction requires atomic generation-directory publication.

### CR-18 — snapshot upgrade compatibility is unspecified

Payload v2 is intentionally fail-closed: upgraded production code rejects
legacy catalog-less v1 snapshots, while an older binary cannot decode v2. No
group-0 capability bit, negotiated minimum version, upgrade order, or offline
upgrade requirement currently prevents mixed versions from attempting an
incompatible InstallSnapshot. This is an availability failure rather than
silent state corruption, but it blocks a supported rolling production upgrade.

The exact-delete transport also introduces a v3-only replicated-command verb.
The client negotiates before use and refuses to send it to a v1/v2 peer, so a
mixed cluster fails closed rather than misframing a mutation, but deletes are
unavailable across that version boundary. A supported rolling upgrade therefore
needs the same committed capability floor or a documented offline upgrade rule.

### CR-04 — control-plane and movement wiring are incomplete

[`ClusterRuntime`](../lib/cluster/integration/cluster_runtime.cpp) derives a
fixed epoch-1 placement directly from the configured peer list. Group-0 state,
the persistent [`NodeIdentity`](../lib/cluster/integration/node_identity.hpp),
movement jobs, learners, joint-consensus cutover, and VShard teardown are not
composed into the server. The Raft journal header is currently populated with
hard-coded UUID and boot-ID bytes in
[`replicated_vshard_host.cpp`](../lib/cluster/integration/replicated_vshard_host.cpp).

Changing node count or peer order therefore computes a different map without
moving data. The open D-40 item in
[`write-scaleout-plan.md`](write-scaleout-plan.md) also records that no
single-VShard teardown exists and that reclaim-floor cleanup requires an ordered
protocol before a VShard can safely be re-added.

### CR-05 and CR-14 — read consistency is not production-wired

The normal replicated query path fences only on the local node having applied
what it believes it committed. It does not confirm leadership with a quorum.
[`ReplicaEngineReader`](../lib/cluster/integration/replica_engine_reader.cpp)
contains the required `readBarrier()` use, but that coordinator is not connected
to the default HTTP query path.

The HTTP parser accepts `leader`, `session`, and `bounded_staleness`, but
[`ClusterDataPlane::queryReplicated`](../lib/cluster/integration/cluster_data_plane.cpp)
uses one path for all three. It does not return or consume session envelopes, and
does not enforce `maxReadLagIndex`.

### CR-06 — apply-time backlog rejection

[`EngineDataStateMachine`](../lib/cluster/integration/engine_data_state_machine.hpp)
applies a committed write through `EngineLocalStore` and `Engine::insertBatch`,
where the ordinary ingest backlog guard may throw. The pre-proposal path has
in-flight byte admission but no equivalent check for Engine conversion,
compaction, and retained-store backlog.

The fix must land as one change: reject a retryable overload on the owning shard
before proposal, then make committed-entry apply unconditional. Removing only
the apply check would remove the cluster's only storage-backlog protection;
adding only the propose check would leave committed replay able to fail.

### CR-07 — read failover and deadlines

For a VShard the coordinator does not host,
[`planReadRouting`](../lib/cluster/data/read_routing.hpp) chooses a cached hint or
`placement.front()`. It does not try the remaining replicas after transport
failure. [`DataPlaneRpc::queryNode`](../lib/cluster/data/dataplane_rpc.cpp) has no
attempt deadline, so the query loop's between-round wall-clock budget cannot
interrupt a suspended RPC.

### CR-17 — write failover still depends on client retries

The D-14 record in
[`write-scaleout-plan.md`](write-scaleout-plan.md) is deliberately marked
“materially improved,” not closed. Its same-session node-kill runs still report
a distribution of 41–52 failed batches out of 400 while the surviving replicas
retain all acknowledged probe data. This is not committed-data loss, but it is a
production availability and API-contract gap: the integration plan assigns
retry-the-whole-batch behavior to official clients, and that work/evidence is not
part of the running server release.

### CR-08 and CR-09 — cluster-unaware public features

Retention policies are stored in the accepting node's NativeIndex and local
shard caches. The replicated state's `applyRetention` integration point is a
no-op. `/subscribe` is always registered and has no cluster guard even though
[`clustering.md`](clustering.md) requires rejection or explicit node-local
marking until cluster-aware coordination exists.

### CR-10 — inter-node and operator security

The data-plane transport can accept TLS credentials programmatically, but the
server has no configuration/startup wiring that provides them. Raft RPC uses a
plain listener and has no equivalent TLS composition. The public
`POST /cluster/rebalance-leadership` route is registered directly rather than
through the authenticated route wrappers used by write/query/delete handlers.

### CR-11 — journal fencing is incomplete

[`JournalSegmentHeader`](../lib/storage/journal_segment.hpp) carries cluster
UUID, core number, segment number, and boot ID. Recovery validates only magic
and format version. [`JournalWriter::open`](../lib/storage/journal_writer.cpp)
does not compare the decoded header to the configured identity, current core, or
filename segment number. The hard-coded identity in CR-04 makes this weaker
still.

### CR-12 and CR-13 — resource bounds and readiness

Snapshot production materialises a whole VShard payload in memory and refuses
to compact it above 128 MiB, retaining the Raft log instead. On a core count that
does not divide 4,096, the snapshot trigger logs a warning and disables itself;
startup continues. Both cases permit unbounded journal/replay growth.

The server sets its readiness flag immediately after Engine/data-plane startup.
`/health` checks that flag and local compaction failures, but not leaderless
groups, unresolved peers, apply failures, apply lag, snapshot disablement, or
journal growth. `/cluster/status` exposes some of this state but is not used for
readiness.

The live-gate rerun found a second startup resource boundary: the default
per-VShard journal layout holds one descriptor for each local group, but the
server neither raised nor preflighted `RLIMIT_NOFILE`. With a normal 1,024 soft
limit it reached `EMFILE` at VShard 3,972 while still creating the first core's
groups. The failed `ClusterDataPlane::start` left `sharded<ShardRaftPlane>`
started, so its global destructor used Seastar's assertion trap and every gate
reported `Illegal instruction` instead of the preceding open-file error.
CR-FIX-064 closes both halves; snapshot materialisation and other CR-12 scale
limits remain open.

### CR-15, CR-16, and CR-22 — deployment and evidence gaps

[`docker-compose.cluster.yml`](../docker-compose.cluster.yml) explicitly starts
Milestone-1 full replication and does not enable partitioning or RF=3. It must
not be presented as a production cluster example.

The final register in [`write-scaleout-plan.md`](write-scaleout-plan.md) recorded
4,308/4,308 unit tests and 40/40 socket tests, but the four live gates had not
been rerun after later cluster changes. During the rerun,
`snapshot_durability_gate.sh` also attempted a redundant top-level data reset
while the preceding arm's restarted nodes were still running. The next arm did
perform another reset, but racing removal against live TSM creation made the
evidence unnecessarily mutable. A later backpressure run exposed a second
fail-open path: `timestar_insert_bench` printed a failed health preflight but
returned success, leaving an empty load summary for the shell to parse.

Commits `09a62c5` and `2e06cb8` make data reset and benchmark preflight
fail-closed, preserve benchmark transcripts, and require all nodes to report
healthy before load begins. Backpressure, node-kill, restart-catch-up, and
snapshot-durability then passed one at a time on the exact `2e06cb8` executable
tree. CR-FIX-071 and CR-FIX-077 record the complete results. This closes the
stale-evidence finding, but does not discharge the separate topology, security,
large-snapshot, live-install, retention, or delete-idempotency gates.

## Fix-up task list

Tasks are ordered by dependency and release risk. A checked box means its “done
when” condition and named evidence have both been recorded; code landing alone
is not completion.

### 0. Fence unsafe public behavior immediately

- [x] **CR-FIX-001 — fail closed for delete forms without a safe cluster
  implementation.** Owner: HTTP/data path. **Done when:** the API returns an
  explicit unsupported/conflict response before changing local state, and a
  test proves no authenticated or forwarded-header variant bypasses the guard.
  The guard now remains active for RF=1 and pattern deletes; exact RF&gt;1 targets
  use the partial CR-FIX-010 implementation.
- [x] **CR-FIX-002 — reject retention create/update/delete in partitioned mode
  until CR-FIX-040 is complete.** Owner: HTTP/control plane. **Done when:** no
  node-local policy mutation is possible through the public API in cluster mode.
- [x] **CR-FIX-003 — reject `/subscribe` in partitioned mode.** Owner: streaming.
  **Done when:** the response explicitly reports cluster streaming unsupported,
  and API coverage prevents regression to a node-local success response.
- [x] **CR-FIX-004 — label or move the M1 Compose file to demo-only and add a
  fail-loud production RF=3 example only after this checklist closes.** Owner:
  release/docs. **Done when:** no documented production command starts the
  best-effort gateway by accident.

### 1. Restore replicated data correctness

- [ ] **CR-FIX-010 — route deletes through each VShard's Raft group.** Owner:
  data path. Split targeted and pattern deletes into VShard commands; wait for
  applied quorum semantics; remove the partitioned-mode ClusterGateway fallback.
  **Done when:** concurrent write/delete ordering, leader failure, client retry,
  and replica restart tests show identical tombstone revisions and no
  resurrection.
  **Progress:** exact RF&gt;1 targets are wired end to end through the current
  leader with bounded retries/deadlines and applied-quorum acknowledgement. The
  v3 socket path rejects old peers and VShard-prefix spoofing; state apply
  fail-stops on a cross-VShard command. HTTP tests cover exact and structured
  targets, mixed-batch preflight, retryable `503`, and a 32-proposal concurrency
  cap; a real single-voter Raft test proves write/delete/query ordering. Pattern
  expansion and the named multi-node leader-failure, idempotent client retry,
  and replica-restart gates remain open. Ambiguous in-request retry now fails
  closed with an explicit unknown outcome instead of appending a second delete.
- [ ] **CR-FIX-011 — define a self-contained VShard snapshot format.** Owner:
  snapshot/storage. Include catalog/index extract, data objects, tombstone
  objects or a proven materialised-delete boundary, and real content hashes.
  **Done when:** a node with an empty data directory catches up exclusively by
  snapshot and normal `/query` discovery returns all and only the expected
  series after writes and deletes.
  **Progress:** payload v2 carries a deterministic catalog and real catalog
  hash; its single VShard-pure data object contains the tombstone-resolved
  logical view. Install binds the manifest's data revision to the Raft snapshot
  index, proves catalog/data identity and type, and rebuilds metadata,
  value-type bindings, and exact day postings. Unit coverage now catches up an
  empty Engine and queries through normal discovery after a delete. The public
  multi-process empty-node gate, retention/deletion coverage, format negotiation,
  and a single visibility fence across data plus derived index still remain.
- [ ] **CR-FIX-012 — make live snapshot installation generation-safe and
  atomic.** Owner: storage. Install into unique immutable object names or replace
  the manager's generation under a fence; remove superseded VShard state without
  colliding with open ranks. **Done when:** install onto a non-empty running
  Engine is immediately visible without restart, survives an injected crash at
  every publish step, and cannot expose old/new mixtures.
  **Progress:** peer filenames can no longer select live paths; receiver-local
  sequence/data ranks, per-operation staging, extent/purity validation, and an
  exact byte+extent+logical-hash idempotent retry are implemented. Any different
  resident TSM, WAL/memory, or index-only state fails closed. True non-empty
  replacement, atomic data/index visibility, orphan handling at every injected
  failure point, and generation cleanup remain open.
- [ ] **CR-FIX-013 — enable and verify VShard-partitioned compaction in cluster
  mode.** Owner: storage. **Done when:** production startup enables the setting,
  tests prove generated snapshot files contain only permitted VShard data, and
  mixed legacy files have a documented migration path.
  **Progress:** partitioned server startup enables the mode before starting the
  compaction loop. The partition path now materialises existing tombstone
  sidecars and rejects a concurrent tombstone-generation race; storage/snapshot
  regressions prove mixed tier-0 input is emitted as delete-resolved VShard-pure
  output. The on-disk migration/rollback procedure for existing mixed
  higher-tier files remains open.
- [ ] **CR-FIX-014 — move storage-backlog admission before Raft proposal and
  make apply unconditional.** Owner: storage/write path. **Done when:** an
  overloaded leader returns retryable overload without committing, while
  restart replay under the same backlog advances apply without repeated Ready
  persistence or fsync storms.
  **Progress:** code and focused regression coverage are present; the live
  overload/restart gate in the done condition remains to be recorded. The
  proposal command is owned by value across the admission coroutine's first
  suspension, with regression coverage for temporary commands.
- [ ] **CR-FIX-015 — define and meet the one-node-failure write SLO and ship the
  client retry contract.** Owner: write path/clients/release. Official clients
  must retry the whole idempotent batch after retryable failure or ambiguous
  timeout. **Done when:** K-run `node_kill_round.sh` evidence meets a published
  error/latency bound, all acknowledged writes survive, retried batches appear
  exactly once logically, and unsupported clients receive precise retry
  guidance.
- [x] **CR-FIX-016 — make TSM delete/block overlap inclusive.** Owner: storage.
  Exact-point deletion now recognises a one-point block at the range boundary;
  the VShard-partitioned tombstone regression proves the point is absent from
  the materialised output.
- [x] **CR-FIX-017 — never re-propose an ambiguous non-revision-bounded delete.**
  Owner: data path/API. Transport timeout and leadership-loss tests prove only
  one proposal is attempted; safe leader redirects still retry. HTTP exposes a
  machine-readable unknown outcome and omits `Retry-After`.

### 2. Complete control-plane and identity wiring

- [ ] **CR-FIX-020 — load/create persistent `NodeIdentity` during server
  startup.** Owner: control plane. Bind node ID and data directory to the real
  cluster UUID; generate a unique boot ID per process. **Done when:** a foreign
  volume, duplicate identity, or changed cluster UUID fails closed.
  **Progress:** startup now persists and validates the node/cluster identity,
  stamps the real cluster and boot UUIDs into journals, and refuses an RF/peer
  list change. Duplicate-identity detection across two simultaneously running
  hosts still depends on group 0 and therefore remains open.
- [ ] **CR-FIX-021 — bootstrap and serve group 0 in the production server.**
  Owner: control plane. Make its committed map the source of placement epoch,
  membership, policies, and jobs. **Done when:** static peer-list derivation is
  not authoritative after bootstrap and control-plane loss preserves the last
  committed data-plane map.
- [ ] **CR-FIX-022 — wire resumable join, drain, remove, replace, and VShard
  movement.** Owner: movement/control plane. Include learner catch-up, verified
  snapshot, log catch-up, joint consensus, leadership transfer, cutover, and
  source grace/cleanup. **Done when:** the M5 growth gate runs through the real
  server, resumes after controller/source crashes, and never serves an
  unsynchronised replica.
- [ ] **CR-FIX-023 — implement ordered VShard teardown and reclaim-floor
  retirement.** Owner: movement/snapshot. **Done when:** both per-VShard and
  shared journals reclaim departed groups, re-adding the same VShard cannot
  inherit a destructive old floor, and crash recovery is tested at every
  teardown phase.

### 3. Make reads satisfy their advertised contracts

- [ ] **CR-FIX-030 — put leader reads behind quorum-confirmed ReadIndex.** Owner:
  query/Raft. Narrow queries to relevant VShards and batch barriers where needed
  to avoid one quorum RPC per hosted group. **Done when:** a partitioned former
  leader rejects/fails closed after a majority commits newer data, with a
  dedicated partition gate.
  **Progress:** the production local and peer-facing query stores now invoke
  `readBarrier()` for the requested VShards with bounded concurrency; the named
  multi-process partition gate remains outstanding.
- [ ] **CR-FIX-031 — add RF&lt;N replica fallback and bounded read attempts.**
  Owner: query/transport. Try alternate placement replicas, follow redirects,
  clear stale failure state after a later success, and apply an end-to-end and
  per-attempt deadline. **Done when:** cold-cache reads survive primary death and
  a black-holed peer completes within the documented deadline.
  **Progress:** alternate placement replicas, stale-hint clearing, shared
  deadlines, and a black-hole socket test are implemented. The cold-cache live
  primary-death gate remains outstanding.
- [x] **CR-FIX-032 — either fully wire session/bounded-staleness reads or reject
  those request values.** Owner: query/API. **Done when:** session tokens are
  returned and enforced across nodes, lag bounds are checked against leader
  progress, and partition tests demonstrate fail-closed behavior. Until then,
  only the implemented mode may be accepted by the parser.

### 4. Replicate policies and guard feature completeness

- [ ] **CR-FIX-040 — store retention policies and cutoff decisions through
  group 0/Raft.** Owner: control/storage. Make policy revision and cutoff
  deterministic across replicas; replace the no-op `applyRetention`. **Done
  when:** policy changes, leader failure, restart, and compaction produce the
  same retained dataset on every replica.
- [ ] **CR-FIX-041 — implement cluster-aware streaming before removing the
  CR-FIX-003 guard.** Owner: streaming/query. Coordinate relevant VShard leaders,
  carry commit positions, resume after leadership/placement changes, deduplicate
  at-least-once delivery, and fence backfill-to-live transition. **Done when:**
  the M6 streaming failure scenarios pass over live RPC.

### 5. Enforce transport, operator, and journal security

- [ ] **CR-FIX-050 — require mTLS on both data-plane and Raft RPC.** Owner:
  security/transport. Add certificate/CA configuration, cluster-UUID-bound peer
  identity, hostname/SAN verification, rotation, deadlines, and fail-closed
  startup. **Done when:** plaintext, untrusted CA, wrong cluster, expired cert,
  and wrong peer identity are rejected on both transports.
  **Progress:** configuration, fail-closed default startup, required client
  certificates, CA trust, SAN verification, and both transport compositions are
  wired. Rotation and the full negative security matrix remain open.
- [x] **CR-FIX-051 — authenticate and authorize every cluster operator route.**
  Owner: HTTP/security. Move status and mutating actions into a cluster handler;
  require at least the normal bearer token for mutations and document whether
  topology reads are public. **Done when:** auth-enabled tests prove anonymous
  rebalance and future movement/repair verbs are rejected.
- [x] **CR-FIX-052 — validate every journal header identity field on recovery.**
  Owner: durability. Compare cluster UUID, core number, filename/segment number,
  supported format, and a deliberately defined boot/incarnation transition
  policy before replay. **Done when:** swapped-core, renamed, foreign-cluster,
  impossible/reused-incarnation, and corrupt-final-header negative tests all
  fail closed without deleting valid durable records, while segments from a
  legitimate prior-process crash or restart still replay.

### 6. Bound resources and make readiness truthful

- [ ] **CR-FIX-060 — remove the monolithic 128 MiB VShard snapshot ceiling.**
  Owner: snapshot/storage. Stream from disk through bounded chunks and stage to
  disk on the receiver. **Done when:** a VShard substantially larger than memory
  catches up without OOM and its Raft log compacts.
- [x] **CR-FIX-061 — enforce a supported SMP/core topology at startup or make
  routing/snapshots correct for arbitrary core counts.** Owner: core/storage.
  **Done when:** unsupported core counts fail before accepting traffic, or
  snapshots provably cover all series at 3, 6, and other non-divisor counts.
- [x] **CR-FIX-062 — make readiness cluster-aware.** Owner: operations. Include
  required peer resolution, hosted-group leadership/servability, apply lag and
  failures, snapshot capability, and fatal journal/storage state. **Done when:**
  `/health` returns non-ready for each injected condition and only becomes ready
  after the node can satisfy its configured API contract.
- [ ] **CR-FIX-063 — close or explicitly disable conditional durability debt.**
  Owner: index/journal. Resolve D-39/D-10 before enabling shared journals by
  default. **Done when:** real-disk shared-journal GC evidence demonstrates
  bounded reclamation. **Progress:** D-38 is closed in `7151f5d`: the six
  in-tree crash simulations now use an explicit test-only abandonment boundary
  that cancels timers and drains both background gates and any threshold flush
  without performing a clean WAL close. A deterministic lifecycle regression
  holds each gate open and proves abandonment waits; unsupported destruction
  now fails with an actionable lifecycle diagnostic instead of Seastar's opaque
  `~gate()` SIGILL. Production callers must still `co_await close()`.
- [x] **CR-FIX-064 — make replicated startup open-file-safe and
  exception-safe.** Owner: server/cluster composition. Resolve the descriptor
  requirement before opening Engine/Raft state, raise an ordinary soft limit
  when permitted, fail with an actionable diagnostic when the hard limit is too
  low, and stop partially-started sharded services without replacing the root
  exception with a destructor trap. **Done when:** a soft=1,024/hard&gt;=8,192
  subprocess opens all 4,096 groups and serves HTTP with an effective 8,192
  limit; a hard=1,024 subprocess exits 1 before Engine startup; and lifecycle
  source regressions plus a live three-node gate pass. Completed in `69ac879`.

### 7. Release validation

- [ ] **CR-FIX-070 — add regression tests for every P0/P1 task above.** Owner:
  each component owner. Tests must exercise the public server path, not only an
  isolated library brick, wherever the defect was caused by missing composition.
- [x] **CR-FIX-071 — rerun the four stale live gates one at a time on the final
  candidate.** Owner: release. Record commit, hardware, configuration, free
  space, and complete results for backpressure, node kill, restart catch-up, and
  snapshot durability. **Completed on executable commit `2e06cb8`:** AMD Ryzen
  9 7950X (16 cores/32 threads), 123 GiB RAM, Linux 7.0.0-28-generic, and 62 GiB
  free on `/tmp`. All gates used three loopback nodes, partitioned RF=3,
  `--smp 4`, and the explicitly development-only insecure transport override.
  Backpressure returned 503 plus `Retry-After` for 16/16 deterministic probes,
  explicitly rejected 200/200 overload batches, served a subsequent write, and
  recovered to 200/200 accepted batches at 5,937,134 points/s with no 500s,
  connection failures, admission rejections, or crashes. Node-kill placed 1,364
  leaders on the killed node; 367/400 concurrent batches succeeded, 33 returned
  explicit HTTP errors, 45/50 outage probes were acknowledged, and every
  acknowledged probe was readable on both survivors with no connection
  failures, 500s, or crashes. Restart catch-up accepted 400/400 batches while
  node 3 was down, took eight snapshots, caught node 3 up on all 3,968 groups
  led by the survivors, installed four snapshots from 13 chunks, and reported
  no undeliverable/abandoned transfers, quota fences, 500s, or crashes. Snapshot
  durability preserved all 200/200 acknowledged probes on every node after
  `kill -9`; the fence and subject arms recovered 17 and 35 compacted journals,
  respectively, while the no-snapshot control took zero snapshots. All arms
  reported zero recovery refusals, oversize refusals, quota fences, 500s, and
  crashes.
- [ ] **CR-FIX-072 — run topology and security gates missing from the current
  register.** Owner: release. Required scenarios: partitioned former-leader read,
  RF&lt;N primary death and black hole, empty-node snapshot catch-up with deletes,
  live non-empty snapshot install, cluster grow/shrink/replace, foreign-volume
  startup, and mTLS identity/rotation failures.
- [ ] **CR-FIX-073 — measure multi-host production behavior.** Owner: performance.
  Run real-disk RF=3 ingest/query, node failure, catch-up, snapshot, and sustained
  retention workloads. Publish throughput, p50/p99, client error rate, disk
  growth/reclamation, apply lag, recovery time, and the supported maximum VShard
  size.
- [ ] **CR-FIX-074 — reconcile all cluster documents and examples at release
  candidate.** Owner: release/docs. Update milestone statuses, remove superseded
  ground truth, link each closed task to evidence, and ensure examples use only
  supported production settings.
- [x] **CR-FIX-075 — isolate process-global placement in the unit harness.**
  Owner: tests/core. **Done when:** a test using a synthetic core count restores
  the runtime mapping and the complete two-core suite reaches the later sharded
  Engine tests without routing to a nonexistent core. Verified by the
  4,328-test full-suite run below.
- [ ] **CR-FIX-076 — define snapshot and replicated-command wire-version
  negotiation and upgrade policy.** Owner: control plane/release. Gate v2
  snapshots and v3-only mutation verbs until group 0 or an equivalent handshake
  proves every sender and receiver supports them, or require and document an
  offline upgrade. **Done when:** old-to-new and new-to-old snapshot and command
  attempts follow the documented safe path, mixed-version behavior is covered
  by a multi-process test, and rollback constraints are explicit.
- [x] **CR-FIX-077 — make live-gate orchestration fail closed.** Owner:
  release/tests. Restrict reset targets to direct `/tmp/tsgate_*` roots, retry
  removal and prove absence before recreation, never delete a running arm's
  data, require cluster-aware health before benchmark load, preserve benchmark
  stdout/stderr, and make benchmark preflight failures nonzero. **Done when:**
  helper negative tests reject unsafe paths, every gate passes `bash -n`, an
  unreachable benchmark endpoint exits nonzero, and the four gates in
  CR-FIX-071 pass without reset warnings or empty load transcripts. Completed in
  `09a62c5` and `2e06cb8`.

## Release exit criteria

The cluster can move from **BLOCKED** to a production release candidate only
when all of the following are true:

1. Every P0 and P1 task is closed or the affected public feature is fail-closed
   and explicitly unsupported.
2. A fresh node can recover exclusively from snapshot plus Raft suffix and
   returns the correct discoverable dataset after writes, overwrites, deletes,
   and retention.
3. A partitioned former leader cannot serve a successful stale leader read.
4. Membership/topology changes are driven by committed group-0 state and survive
   crash/restart at every step without dropping below the configured RF.
5. Both inter-node transports authenticate peers and encrypt traffic; operator
   mutation routes enforce authorization.
6. Readiness accurately reflects whether the node can meet its configured read
   and write contracts.
7. The full automated suites, all named live gates, and the new topology,
   snapshot, read-partition, and security gates pass on the exact release commit.
8. The production deployment example, capacity limits, upgrade procedure,
   rollback constraints, and unsupported features are documented from measured
   behavior.

## Review evidence

The review ran the following on the reviewed tree:

```text
timestar_unit_test:            4308/4308 passed
timestar_cluster_socket_test:    40/40 passed
```

The repository was clean before and after the original review. The remediation
is recorded in the commits named at the top of this document. These results
validate the exercised unit and socket paths; they do not supersede the missing
live gates or the remaining production-composition findings above.

Post-remediation validation through `3ac9899`:

```text
timestar_unit_test:              4340/4340 passed (443 suites, -c 2; no skips)
timestar_cluster_socket_test:      45/45 passed (8 suites, -c 2)
first-pass focused regressions:     56/56 passed (15 suites, -c 2)
snapshot/compaction regressions:    24/24 passed (9 suites, -c 2)
exact-delete focused regressions:   11/11 router/HTTP plus wire/Raft paths passed
timestar_http_server:              built successfully
test/cluster_gates/*.sh:           bash -n passed
git diff --check:                  passed
```

Additional validation for `69ac879`:

```text
HTTP startup/lifecycle regressions: 10/10 passed
soft nofile 1024, hard 524288:       raised to 8192; 4096 groups opened; HTTP served; clean exit 0
soft/hard nofile 1024:               actionable pre-Engine refusal; clean exit 1; no SIGILL
backpressure_gate.sh:                passed (subject overload + default-budget restart recovery)
```

Live-gate harness validation for `09a62c5` and `2e06cb8`:

```text
current full unit suite:               4341/4341 passed (443 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
verified gate reset helper:           passed, including unsafe-target rejection
test/cluster_gates/*.sh:              bash -n passed
insert bench invalid format:          nonzero exit with invalid-argument diagnostic
insert bench unreachable health:      nonzero exit with health-preflight diagnostic
backpressure_gate.sh:                 passed on 2e06cb8
node_kill_round.sh:                   passed on 2e06cb8
restart_catchup_gate.sh:              passed on 2e06cb8
snapshot_durability_gate.sh:          passed on 2e06cb8
```

NativeIndex lifecycle validation for `7151f5d`:

```text
targeted lifecycle/recovery tests:       7/7 passed
current full unit suite:             4342/4342 passed (444 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
```
