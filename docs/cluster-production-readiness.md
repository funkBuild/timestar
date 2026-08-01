# Cluster production-readiness review and fix-up plan

**Status:** BLOCKED — no clustered deployment mode is approved for production

**Reviewed baseline:** `cluster-design` at `f78e05d` (2026-08-01)

**Remediation commits:** `95c10d2`, `a16b03a`, `d578e81`, `ddab705`,
`8620b9e`, `20639dc`, `ea2511b`, `3ac9899`, `69ac879`, `09a62c5`,
`2e06cb8`, `7151f5d`, `5b22b81`, `fef4886`, `3d2d607`, `f0e28f0`,
`da55952`, `a1beb94`, `bb5b871`, `e201343`, `6ad2c93`, `9a42d84`,
`41fdc34`, `a58d2a9`, `6a73809`, `81692a4`, `d363348`, `2749027`,
`1f61f49`, `b2c7d0b`, `872f7e1`, `023d9c3`, `d5f4755`, `7f6d7e8`,
`7760ebd`, `6557666`, `c8f28c8`, `445f1f0`, `8b8536d`, `6912dfb`,
`ecb63a5`, `a03fe1d`, `8ae846c`, `9ecd0e6`

**Scope:** The recent VShard/Raft cluster redesign, its production-server
integration, the public HTTP surface, recovery paths, and the release evidence
recorded in the cluster plans.

This is the authoritative release-blocker list for the cluster redesign. The
architecture and integration documents remain useful descriptions of the target
and its milestone sequence, but their older completion statements must not be
used as evidence that the current server is production-ready.

## Implementation progress after the review

Forty-four remediation commits are now recorded. Cluster release status
remains **BLOCKED** because group 0/movement, atomic and retry-safe
pattern-delete semantics, replicated retention, the large-snapshot path,
sustained live receipt-retirement compaction evidence, and rolling wire-format
compatibility remain open. The four previously stale live release gates now pass
on the same executable candidate and no longer block release by themselves.

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
  failures, Raft tick failures, disabled snapshot production, a committed
  format too old to emit self-contained snapshots, oversize snapshot refusal,
  and undeliverable snapshots. `/cluster/status` publishes the minimum local
  `active_cluster_format` and `snapshot_format_ready`. Unsupported SMP layouts
  fail startup.
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
- Snapshot install validates the complete data and catalog payload before its
  first mutation, then replaces a live VShard generation under one Engine-wide
  install serialization point. It quiesces target WAL conversion, durably
  deletes target WAL and TSM generations while preserving foreign data in mixed
  files, publishes a receiver-ranked VShard-pure immutable object, replaces the
  exact catalog/type/day view, retires superseded pure objects, and rolls to a
  fresh WAL generation. Root-object reconciliation plus raw-byte identity makes
  publication retryable after an interrupted directory barrier without
  duplicating or resurrecting an object.
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
- RF&gt;1 exact-series deletes now require a stable 128-bit `Idempotency-Key` and
  client issuance timestamp, group the request's canonical key/ranges into one
  operation per VShard, route each batch to that VShard's current Raft leader,
  and acknowledge only after quorum commit and apply. The state machine retains
  operation ID, batch hash, original apply index, and issuance time; a
  byte-identical retry is a no-op, while conflicting identity reuse fail-stops.
  Payload v4 carries only receipts and the monotonic retirement floor covered by
  its exact Raft snapshot boundary, and local recovery reads that small section
  without copying snapshot objects. Modern receipts are bounded to the most
  recent 1,024 operations per VShard and one hour; a retry below the replicated
  floor is a terminal `409 DELETE_IDEMPOTENCY_EXPIRED`. Snapshot production now
  treats the active store's oldest surviving replicated revision as the exact
  first-unflushed fence. Delete-only state can therefore compact through the
  observed applied prefix without inventing a later point. If a receipt-floor
  advancement is still fenced behind an active write, one conditional
  memory-store rollover publishes that write through the normal bounded
  conversion path and a later sweep advances. The payload's data/log fence is
  then bound to the exact Raft boundary. Commit `8ae846c` resolves the identified
  implementation liveness defect; CR-FIX-065 retains only its sustained live
  delete-heavy/journal-reclamation evidence.
  Pattern discovery infrastructure remains internally tested, but the public
  clustered handler rejects every pattern or mixed-pattern batch before
  expansion and proposal: re-expanding after a partial attempt could add a
  concurrently created series that had no original operation receipt. RF=1
  remains unsupported. Commits `6912dfb`, `ecb63a5`, and `a03fe1d` close the
  named deterministic exact-delete failover, batching, and bounded-retention
  gaps under CR-FIX-010; the external multi-process release gate remains
  distinct.
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
- Every live-gate server now receives an explicit, overrideable Seastar memory
  budget (8 GiB per process by default). Previously each of three to five
  colocated processes sized itself from the whole host, so aggregate allocation
  was neither bounded nor reproducible and could terminate the harness during
  catch-up. The empty-node catch-up gate now deletes and proves absence of the
  returning node's complete durable root and includes surviving plus exactly
  deleted public-path probes. Its first run on the current candidate was
  interrupted during catch-up and is not release evidence; a bounded rerun
  remains required.
- Snapshot installation now has a deterministic read-visibility proof across
  its data-plus-catalog publication interval. The Raft driver keeps
  `appliedIndex` behind the committed snapshot boundary until the complete
  state-machine install future resolves. The production apply fence therefore
  refuses data queries, metadata, and pattern discovery while an install is
  partial, then exposes the rebuilt view only after completion. The storage
  replacement itself is now restart-safe at each durable checkpoint: tests
  discard all in-memory manager and index state before retrying every injected
  boundary, and prove immediate exact visibility without restart after a
  successful non-empty install. This closes CR-FIX-012; CR-FIX-011 retains the
  live empty-node, retention, and format-negotiation release gates.
- Shared-journal GC now scans past an individually pinned segment and reclaims
  later fully released segments instead of allowing one idle VShard to retain
  the reactor's entire physical suffix. Recovery accepts only sequence gaps
  superseded by a later retained snapshot and still fails closed on any
  post-snapshot gap. Shared mode derives a sequential snapshot batch that gives
  every hosted group a turn within a target 15-minute fair scan on supported
  one-, two-, and four-core topologies; private journals retain the existing
  one-snapshot cadence. `/cluster/status` now publishes snapshot-production and
  journal-GC pass, deletion, pin, and copy-forward counters.
- TSM publication now treats the destination directory sync as a mandatory
  durability boundary. WAL conversion and both ordinary and VShard-partitioned
  compaction retain their prior durable generation when that sync fails; direct
  TSM writers propagate the same failure instead of treating it as best effort.
- TSM open, index-validation, and rank-collision failures now abort generation
  registration. Startup cannot serve a partial immutable dataset, and a failed
  WAL conversion or compaction cannot report success and retire its source.
  Planned compaction targets also preserve sequence zero instead of confusing
  that valid first allocation with the direct-call auto-allocation sentinel.
- Compaction now unlinks every source TSM and syncs the directory before
  removing the sources from the live manager or deleting their tombstone
  sidecars. Source unlink failures propagate, a failed directory barrier leaves
  the open sources registered and retryable, and pinned readers retain their
  in-memory tombstone ranges after disk cleanup. This prevents old raw points
  from returning after a failed unlink or crash between sidecar and TSM removal.
- TSM startup now treats an existing tombstone sidecar as part of the immutable
  generation's logical contents. Corrupt, unsupported, or unreadable sidecars
  abort open and registration instead of logging a warning and serving the raw
  TSM without its durable deletions.
- Tombstone publication now writes and fdatasyncs a temporary sidecar, atomically
  renames it over the live name, and syncs the parent directory before reporting
  success. Per-TSM mutation serialization keeps concurrent deletes from changing
  the range vectors or publication paths while another flush is suspended. A
  directory-sync failure remains dirty and is safe to retry.
- WAL segment creation now syncs the containing directory before the segment can
  accept acknowledged writes. Live and startup-recovery retirement unlink
  idempotently and sync the directory before reporting success. A retirement
  retry remembers that its TSM is already durable and registered, so it retries
  only the WAL barrier instead of colliding with a duplicate immutable rank.
- Failed background WAL conversions are retained and retried until publication
  succeeds or shutdown begins. Before publication the store remains queryable
  and visible to snapshot-pending accounting; after publication a separate
  non-query retirement list keeps its resident memory admission-accounted until
  the source WAL directory barrier succeeds. Retry sleeps are shutdown-abortable,
  and shutdown makes one final inline attempt without waiting out the 30-second
  production delay.
- Startup WAL recovery now fails closed if recovered contents cannot be
  published as a live TSM. The source WAL is preserved, no fresh active store is
  created over the visibility hole, and a later clean startup can load any
  valid renamed output, replay the preserved source into a newer rank, retire
  the WAL durably, and finish initialization.
- WAL recovery now distinguishes an incomplete final frame at EOF from a fully
  present corrupt frame. A torn final frame remains discardable, but CRC
  mismatch, invalid length/padding, unknown command/value type, or payload parse
  failure aborts recovery and therefore startup. The source remains intact;
  legacy no-CRC frames are accepted only when they cannot be mistaken for a
  corrupt current-format frame.
- WAL discovery now requires the exact canonical sequence basename and refuses
  startup on every unrecognized `.wal` artifact instead of skipping it or
  accepting a numeric prefix. WAL/store/manager sequences are 64-bit end to end,
  with final exhaustion guarded. Fresh creation uses exclusive create and will
  never truncate a pre-existing nonempty segment; only a zero-length artifact
  from a failed creation barrier can be retried in place.
- Raft journal discovery now treats its per-VShard or per-core directory as an
  exclusive segment namespace. Any non-canonical or non-regular entry fences
  startup and is preserved instead of being silently omitted from replay.
  Segment identities cannot wrap at startup or rotation, and a fresh segment is
  created exclusively rather than truncating a colliding durable path.
- TSM discovery now accepts only the exact current and documented legacy
  immutable-file schemas, rejects non-regular entries instead of following
  symlinks, and validates both physical and data ranks before opening or
  registration. Sequence allocation stops at the shared 60-bit rank boundary,
  and invalid additions cannot leave a half-registered immutable generation.
- NativeIndex WAL recovery now treats its directory as an exclusive canonical
  namespace and permits a torn tail only in the newest generation. Complete
  corruption, a torn sealed generation, sequence discontinuity, symlinks,
  malformed aliases, and identity exhaustion fence startup. Fresh creation is
  exclusive, a recovered empty/torn generation is rotated before reuse, and WAL
  creation/deletion directory barriers are mandatory.
- NativeIndex manifest and SSTable recovery now fail closed on complete manifest
  corruption, malformed records, missing or swapped live SSTables, non-regular
  paths, identity exhaustion, and invalid file metadata. SSTable v2 binds the
  file identity and checksums its bloom/index metadata; legacy v1 files disable
  the untrusted bloom and validate every CRC-protected data block against the
  index before serving. Compaction preserves a durable output across an
  ambiguous manifest-publication failure and syncs obsolete-name cleanup.
- NativeIndex startup now reconciles the complete SSTable namespace against the
  recovered manifest before serving or flushing replayed WAL state. Canonical
  unreferenced flush/compaction outputs and stale manifest temporaries are
  unlinked with a mandatory directory barrier. Non-canonical names, symlinks,
  directories, and other ambiguous artifacts are preserved and fence startup;
  manifest-live generations are never selected for cleanup.

Focused evidence on this pass includes the rebuilt server, unit and socket test
targets, journal negative tests, alternate-replica routing tests, a black-holed
read deadline test, ReadIndex tests, HTTP fail-closed/auth tests, persistent
identity/topology tests, and pre-proposal admission tests. The strict checkboxes
below stay open where their stated multi-process or fault-injection “done when”
evidence has not yet been run.

Final local validation for the pre-snapshot-replacement remediation commits is
green: all 4,416 unit
tests passed at `-c 2`, 47/47 socket-backed cluster tests passed, the
first-pass 56/56 focused
cluster/readiness/identity/admission regressions passed, and the second-pass
24/24 snapshot/compaction regressions passed. The exact-delete pass additionally
covers public HTTP behavior, bounded batch fan-out, leader-hint retry, v3 socket
transport, old-peer refusal, VShard-spoof rejection, and real Raft
write/delete/query apply. The pattern-expansion pass adds checksummed v4 codec,
real v4 socket and old-peer refusal, VShard-scoped NativeIndex discovery,
leadership/apply fencing, redirected-replica exclusion, whole-batch HTTP
preflight, count/byte limits, and partial-fan-out ambiguity coverage. The
production server links, every cluster-gate shell script passes `bash -n`, and
`git diff --check` passes. The four named live multi-process gates also pass on
`2e06cb8`; the additional topology, security, large-snapshot, non-empty
live-install, and delete concurrency/restart gates remain open. The later live
snapshot replacement pass adds 7/7 focused replacement/retirement/compactor
regressions and 11/11 adjacent snapshot, restore, index-extract, metadata, and
WAL regressions, all under `--smp 1 --memory 1G`. The every-checkpoint case was
then rebuilt and rerun separately after strengthening it to reopen the Engine
before every retry.

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
| CR-23 | P0 | TSM publication ignored parent-directory sync failure before deleting the prior durable generation | A crash could discard the renamed output directory entry after its WAL or source TSMs had been removed, losing acknowledged data. |
| CR-24 | P0 | TSM registration swallowed open/index failures and silently selected one duplicate rank | WAL conversion could delete its recoverable source after failing to register the output; restart could serve a partial or filesystem-order-dependent dataset. |
| CR-25 | P0 | TSM source retirement deleted tombstones before the source and swallowed source-unlink failures | A failed unlink or crash could reload the raw source without its deletion ranges; pinned readers could also stop filtering deleted points during compaction. |
| CR-26 | P0 | TSM open discarded invalid or unreadable tombstone sidecars | Startup could serve durably deleted points from the raw immutable file instead of fencing an incomplete logical generation. |
| CR-27 | P0 | Tombstone rewrites truncated the live sidecar and were not serialized or directory-durable | A crash or concurrent delete could corrupt the only durable delete set, lose an acknowledged sidecar name, or publish incomplete ranges. |
| CR-28 | P0 | WAL creation and retirement omitted directory durability and post-publication retries rewrote the TSM | A crash could lose an acknowledged segment or resurrect a converted source; a cleanup retry could then collide with its already-live immutable rank. |
| CR-29 | P0 | A background WAL conversion was abandoned and evicted after two failures | Acknowledged data disappeared from live queries until restart, conversion-pending snapshot fences became falsely clear, and admission resumed while that durable data existed only in an offline WAL. |
| CR-30 | P0 | Startup continued after a recovered WAL failed TSM conversion | The node destroyed the recovered in-memory data, created a fresh active WAL, and reported startup success while acknowledged points remained query-invisible in an offline source file. |
| CR-31 | P0 | WAL recovery discarded fully framed corruption and continued startup | A CRC-invalid or malformed acknowledged command could be omitted from the recovered store while the node served the remaining dataset as complete. |
| CR-32 | P0 | WAL filename identity was permissive and fresh creation truncated collisions | Malformed names could be skipped or aliased to another sequence, 32-bit allocation could wrap recovery order, and a colliding fresh create could erase the only durable acknowledged segment. |
| CR-33 | P0 | Raft journal discovery ignored unrecognized entries and segment creation could truncate after identity wrap | A damaged or partially renamed acknowledged segment could be omitted from replay; exhaustion could wrap to segment zero and overwrite an older durable generation. |
| CR-34 | P0 | TSM discovery accepted numeric-prefix aliases and rank allocation exceeded its 60-bit identity space | Malformed or symlinked immutable files could enter recovery under the wrong identity; an out-of-range data generation could register and fail later queries, while exhaustion could publish an unusable TSM and strand its source WAL. |
| CR-35 | P0 | NativeIndex WAL recovery aliased or skipped durable generations and discarded complete corruption | Acknowledged catalog/postings mutations could disappear while startup served an incomplete index; a colliding fresh generation could also truncate the last durable copy. |
| CR-36 | P0 | NativeIndex manifest/SSTable recovery omitted or destroyed durable generations | Complete manifest corruption could be rewritten as a clean prefix, missing/swapped or metadata-corrupt SSTables could be served as an incomplete index, and compaction could delete a durable output after an ambiguous manifest publication. |
| CR-37 | P0 | NativeIndex startup ignored unreferenced and ambiguous SSTable-namespace artifacts | Crash outputs and obsolete compaction sources could accumulate to disk exhaustion, while a malformed or non-regular possible durable generation was silently omitted instead of fencing incomplete recovery. |
| CR-38 | P2 | Multi-process release gates gave every Seastar node an implicit host-sized memory budget | Three to five colocated nodes could overcommit RAM and terminate the runner before a gate reached its assertions, making the release evidence non-reproducible. |

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
exact commands committed and applied, avoiding a racy pre-read.

Commit `1f61f49` adds bounded RF&gt;1 pattern expansion. A new checksummed v4
data-plane request names the selector, exact VShard subset, leadership-resolution
subset, placement epoch, and result limit. Each answering node confirms
current-term leadership and applied-state catch-up before performing
VShard-prefix NativeIndex scans; the coordinator re-parses and re-hashes every
returned key, follows only authorised redirects, and refuses incomplete,
oversize, old-peer, or placement-changing results. The HTTP layer expands every
selector in the request before the first proposal, deduplicates exact
key/range triples, caps the batch at 10,000 series and 8 MiB of encoded catalog
keys, and limits proposal fan-out to 32.

The next review pass found that this still did not make a broad selector a
retry-safe replicated operation. After a partial attempt, a retry repeated the
catalog scan; a series created in between then appeared as a new exact target
with no receipt from the original attempt and could be deleted. Commit `8b8536d`
therefore removes the production pattern hook and rejects every clustered
pattern or mixed-pattern batch before both expansion and proposal. The bounded,
leader-fenced discovery implementation remains covered as a building block, but
is not a supported mutation path. A durable replicated expansion plan or an
equivalently bounded per-VShard selector command is required to re-enable it.
RF=1 continues to fail closed.

### CR-20 — ambiguous delete retries could erase concurrent writes

The shared failure taxonomy already recorded that `LeadershipLost` and
`Transport` are ambiguous: the proposal may have committed even though the
caller received no acknowledgement. That ambiguity is safe for byte-identical
writes under the current LWW contract, but not for a non-revision-bounded
physical range delete. The new command router initially reused the write retry
policy and could
therefore append the same delete again after another client had written into the
range.

Commit `3ac9899` stopped after the first ambiguous legacy-delete result while
preserving unambiguous not-leader, leader-refused, stopping, and pre-proposal
overload retries. Commit `445f1f0` adds the durable identity it was waiting for:
partitioned HTTP deletes require a non-zero 128-bit `Idempotency-Key`, and the
replicated state machine records the operation ID, command hash, and first
applied index only after storage deletion completes. Commit `ecb63a5` groups a
request's canonical exact targets into one command and operation ID per VShard,
so one legal 10,000-target request cannot consume 10,000 receipts in one group.
Legacy receipts are carried in snapshot payload v3. Commit `a03fe1d` adds the
client-stable issuance time, replicated retirement floor, and payload v4 needed
to bound modern receipts to one hour and 1,024 operations per VShard. Receipt
recovery from a locally produced snapshot skips the large object bodies. A
duplicate after an intervening write is now a state-machine no-op, including
after journal compaction and host restart, so the router may safely retry an
ambiguous exact command. A retry retired by the time or capacity floor is a
typed terminal failure through the real socket path and becomes HTTP `409`, not
a new physical delete. Reusing an operation ID for different target bytes is a
fail-stop invariant breach rather than a false success. Commit `6912dfb` proves
the same operation on three real Engines: a retry by a new leader in a later
term and another retry after that replica reconstructs the receipt from durable
journal replay both preserve a write ordered after the original delete.
Commit `8ae846c` makes that receipt retirement compactable even on delete-only
VShards: the active store supplies the exact first-unflushed fence, and a
conditional rollover drains a surviving active write when it is the only
barrier. CR-FIX-010 remains open for patterns and the external release gate;
CR-FIX-065 retains the sustained live compaction/reclamation measurement.

### CR-23 — TSM publication did not fence deletion of its durable source

The WAL-to-TSM and compaction paths renamed their output into place but treated
failure to sync the destination directory as best effort. Their callers could
then unlink the WAL or source TSM generation. A crash in that window could lose
the new directory entry after the only previously durable copy had been
removed.

Commit `fef4886` makes successful destination-directory sync the publication
boundary for [`TSMFileManager`](../lib/storage/tsm_file_manager.cpp), ordinary
compaction, VShard-partitioned compaction, and direct
[`TSMWriter`](../lib/storage/tsm_writer.cpp) users. A sync failure is propagated
before a WAL or source TSM may be retired. The uncertain output can coexist
with the retained source and be retried or recovered, which may require later
orphan cleanup but does not lose the prior durable generation. Injected failures
cover WAL conversion and both compaction layouts.

### CR-24 — invalid TSM generations were silently omitted

[`TSMFileManager::openTsmFile`](../lib/storage/tsm_file_manager.cpp) logged and
swallowed any open or sparse-index validation failure. During restart this left
the file on disk but served every other generation as if the dataset were
complete. During WAL conversion the same successful return allowed the caller
to remove the WAL even though the new immutable generation was unreadable and
unregistered. Duplicate manager ranks also kept whichever path happened to be
visited first, making visibility depend on filesystem iteration order; live
compaction registration could continue to source deletion after the collision.

Commit `3d2d607` propagates open and index failures and rejects duplicate ranks
in both startup discovery and live registration. Server lifecycle cleanup closes
any generations opened before a shard fails initialization, while conversion
and compaction retain their old durable source. The stricter collision test also
exposed a separate ambiguity where planned sequence zero was interpreted as a
direct-call auto-allocation sentinel; planned targets now carry an explicit flag
and preserve `(tier, sequence)` exactly.

### CR-25 — source retirement could resurrect tombstoned data

[`TSMFileManager::removeTSMFiles`](../lib/storage/tsm_file_manager.cpp) removed
each source from live tracking and deleted its tombstone sidecar before asking
[`TSM::scheduleDelete`](../lib/storage/tsm.cpp) to unlink the TSM. The latter
also deleted the sidecar, caught source-unlink errors, and returned success. A
failed unlink therefore left a raw source name on disk without its deletion
ranges; a crash could produce the same result because neither unlink ordering
nor source absence had a directory-durability boundary. The sidecar removal
also cleared the shared TSM object's in-memory ranges, so a query that had
pinned the source before compaction could return deleted points from its still
open file descriptor.

Commit `f0e28f0` makes retirement a two-phase operation. It first unlinks every
source TSM, propagates failures, and syncs the containing directory while the
sources remain registered and their tombstones remain intact. Only after that
barrier does it remove live tracking and clean up sidecars. Sidecar unlink keeps
the tombstone ranges in memory for pinned readers, and an already-unlinked TSM
is a valid retry after a failed directory sync. An injected sync failure proves
the safe intermediate state and successful idempotent retry.

### CR-26 — invalid tombstones were ignored at startup

[`TSM::loadTombstones`](../lib/storage/tsm_tombstone_integration.cpp) caught any
sidecar load failure, logged a warning, cleared the tombstone manager, and let
`TSM::open` return success. That made a checksum failure, unsupported version,
truncation, or I/O error indistinguishable from “no deletes”: startup registered
the raw TSM and could return points whose deletion was durable only in the
sidecar.

Commit `da55952` propagates a present sidecar's validation or I/O failure with
both TSM and sidecar paths in the diagnostic. The existing TSM open-error path
closes the data file before rethrowing, and the sidecar reader already closes
its descriptor on every exit. A real manager-startup regression pairs a valid
TSM with a corrupt sidecar and proves the generation never becomes live; valid
sidecar persistence and reload remain covered.

### CR-27 — tombstone publication was neither atomic nor serialized

[`TSMTombstone::flush`](../lib/storage/tsm_tombstone.cpp) opened the live
`.tombstone` path with `truncate` and rewrote it across multiple suspending I/O
operations. A crash after truncation could leave the only durable delete set
partial or checksum-invalid. The publication also omitted a parent-directory
sync, so even a fully flushed first sidecar or replacement was not a complete
crash-durability boundary. Finally, separate `TSM::deleteRange` coroutines could
mutate the same range vectors and enter flush concurrently while either was
suspended.

Commit `a1beb94` writes a fixed per-TSM temporary sidecar, fdatasyncs and closes
it, atomically renames it over the live name, and syncs the parent directory
before clearing the dirty state. The full logical mutation and publication are
guarded by a per-TSM semaphore, as are sidecar load and retirement, so cleanup
cannot race an in-flight delete. Injected directory-sync failure proves the
complete renamed file remains retryable, and a held first sync proves a second
delete cannot enter publication until the first finishes; both ranges survive
close and reopen.

### CR-28 — WAL names were not crash-durable across creation or retirement

[`WAL::init`](../lib/storage/wal.cpp) created each new segment but did not sync
its containing directory before writes could be acknowledged. Fdatasyncing the
file contents alone therefore did not complete the new-name durability
boundary. [`WAL::remove`](../lib/storage/wal.cpp) and the separate startup
recovery cleanup unlinked converted segments without syncing the directory, so
a crash could make an old WAL name reappear after its TSM handoff. The live
conversion retry path had a second defect: if TSM publication succeeded but WAL
retirement failed, retry called `writeMemstore` again and collided with the TSM
rank it had already registered instead of retrying cleanup.

Commit `bb5b871` syncs the parent directory during WAL initialisation before the
segment is usable and closes the opened descriptor if that barrier fails. Live
and recovered-source removal now use an idempotent existence-check, unlink, and
directory-sync sequence; an unlink-success/sync-failure retry repeats the
barrier even though the path is already absent, and startup recovery does not
report success before it completes. `MemoryStore` retains the successful TSM
handoff state so a live conversion retry skips publication and performs only
WAL retirement.
Injected failures cover creation cleanup, live unlink retry, recovery-startup
refusal, and one-rank preservation across the post-publication retry.

### CR-29 — repeated WAL conversion failure evicted acknowledged live data

[`WALFileManager::rolloverMemoryStore`](../lib/storage/wal_file_manager.cpp)
made one background conversion attempt and scheduled only one delayed retry. If
both failed, its terminal exception handler erased the rolled `MemoryStore` from
`memoryStores` while preserving only the source WAL on disk. The WAL protected
crash recovery, but the running process could no longer query the acknowledged
points. Both shard-wide and per-VShard pending-conversion predicates also became
false for that store, allowing snapshot/log-compaction decisions to proceed as
if its revisions had reached TSM. Erasure bounded memory only by dropping
acknowledged data from the serving and safety-accounting state, and it released
request-edge backpressure before the WAL had become query-visible TSM.

Commit `e201343` replaces the two-attempt chain with one gate-held, serialized
retry coroutine. Failed publication leaves the store in `memoryStores`, so data
stays queryable and both snapshot fences stay conservative; request admission's
existing 16-store ceiling bounds accumulation. Once TSM publication is durable
and query-visible, the store moves to a separate non-query retirement list to
avoid duplicate results while its memory remains included in the same admission
count until idempotent WAL retirement succeeds. The 30-second retry sleep is
abortable, the gate spans every attempt, and shutdown retries remaining
publication and retirement work inline. Fault injection proves both publication
and WAL-retirement failures continue past the former two-attempt limit, retain
the correct visibility/accounting state, converge without a duplicate TSM rank,
and drain cleanly.

### CR-30 — failed WAL recovery conversion did not fence startup

During [`WALFileManager::init`](../lib/storage/wal_file_manager.cpp), a recovered
WAL was replayed into a temporary `MemoryStore` and converted to TSM. The
`bad_alloc` branch propagated, but every other conversion exception was logged
and swallowed. The loop then released the temporary store, created a fresh
active WAL, and reported recovery and initialization complete. Preserving the
source file protected a future restart but did not make its acknowledged data
visible to the running node; successful reads could therefore return an
incomplete dataset indefinitely.

Commit `6ad2c93` preserves the source and rethrows the original conversion
exception, preventing Engine startup from reaching a serving state without the
recovered generation. Fault injection at the TSM parent-directory barrier proves
the failed output is not registered, the WAL remains, and no fresh active store
is created. The same test then constructs a clean process boundary: a valid
renamed TSM left by the uncertain barrier is loaded, the preserved WAL is replayed
into a newer immutable rank, the source is durably retired, and startup completes
with one active store.

### CR-31 — complete WAL corruption was treated as a discardable tail

[`WALReader::readAll`](../lib/storage/wal.cpp) warned and continued after a
fully present frame failed CRC validation, contained an unknown command or value
type, or failed payload parsing. It also stopped successfully at implausible
length and padding markers. These cases are not equivalent to EOF while reading
the final frame: the affected command may already have been acknowledged, so
continuing startup exposes an incomplete durable history as healthy.

Commit `9a42d84` propagates every complete-frame structural, checksum, type, and
payload error after closing the input resources. `WALFileManager` consequently
preserves the source WAL and aborts startup without registering partial replay
state or creating a fresh active store. EOF during the final incomplete frame
continues to model a crash-torn, unacknowledged tail and retains all earlier
complete commands. The backward-compatible no-CRC reader remains available only
when byte layout makes legacy interpretation unambiguous; an ambiguous frame
fails closed instead of allowing a damaged current CRC prefix to masquerade as
a legacy command. Regressions cover single- and second-frame CRC failures,
manager startup/source preservation, unambiguous legacy replay, and a truncated
final frame.

### CR-32 — WAL discovery and creation did not preserve segment identity

[`WALFileManager::init`](../lib/storage/wal_file_manager.cpp) removed from its
recovery list any `.wal` whose basename `std::stoi` could not parse, allowing
startup to serve without a possibly acknowledged segment. `std::stoi` also accepted a numeric prefix, so a
name such as `0000009014junk.wal` silently assumed sequence 9014. The manager,
store, and WAL then narrowed the path layer's 64-bit sequence to 32 bits, making
eventual wrap reorder simultaneously retained segments. Finally, fresh
[`WAL::init`](../lib/storage/wal.cpp) opened with `truncate` even when the target
already existed and was nonempty, turning any allocator collision into durable
data loss.

Commit `41fdc34` parses and stores WAL sequences as `uint64_t`, consumes the
entire canonical basename, sorts the parsed identity once, and fences startup
without modifying any unrecognized `.wal` artifact. Values above `UINT32_MAX`
recover and allocate their successor normally; `UINT64_MAX` is an explicit
exhaustion error rather than wrap. Fresh creation uses exclusive create when the
path is absent and refuses a pre-existing nonempty file after closing its
descriptor. A zero-length file left by a prior failed creation-directory sync
remains the sole retryable collision because it could not have accepted an
acknowledged command. Regressions preserve real WAL contents across malformed
renames and fresh-create collision, reject numeric-prefix aliases, and recover
through sequence 4,294,967,296.

### CR-33 — Raft journal discovery skipped possible durable segments

[`JournalWriter::open`](../lib/storage/journal_writer.cpp) added only filenames
accepted by `parseSegmentFilename` to recovery and silently ignored every other
directory entry. These journal directories contain no metadata or temporary-file
namespace, so an ignored entry can be an acknowledged segment whose filename was
damaged or partially renamed. Startup would then replay an incomplete Raft
history as though the directory were complete. Independently, the next segment
was calculated with unchecked `uint64_t` addition and `startSegment` used
truncate-on-open. At identity exhaustion, startup or rotation could wrap to zero
and overwrite an older durable segment.

Commit `a58d2a9` requires every journal-directory entry to be a regular file with
the exact canonical segment name. Any other entry is preserved and fences the
writer before replay or fresh creation. Startup and rotation explicitly reject
segment-number exhaustion, fresh segment creation uses exclusive create, and a
directory-sync failure closes the new descriptor while leaving the existing
empty-final recovery protocol intact. Regressions cover a valid acknowledged
segment under a malformed name, a canonical-named non-regular entry, startup at
`UINT64_MAX`, and rotation from the final available identity without creating or
truncating segment zero.

### CR-34 — TSM discovery and allocation did not enforce immutable identity

[`TSM::TSM`](../lib/storage/tsm.cpp) parsed tier and sequence with
prefix-consuming `std::stoull`, so names such as `0_7junk.tsm` and
`0junk_7.tsm` aliased a valid generation. Startup canonicalised `.tsm` entries
before opening them, which also followed a symlink out of the immutable-file
directory. Separately, both rank functions reserve 60 bits for sequence, but
[`TSMFileManager`](../lib/storage/tsm_file_manager.cpp) allocated until
`UINT64_MAX` and registered a file before validating its data rank. A generation
at or above `2^60` could therefore be published or partially registered and
only fail when a later query requested its rank.

Commit `6a73809` parses complete filename components and explicitly preserves
the current standard/compaction layouts plus the three documented legacy
rebalance layouts. Startup rejects non-regular `.tsm` entries without following
them and scans deterministic path order. Physical and data ranks are validated
before file open or manager mutation, tier is bounded to four bits, and all
fresh/reserved sequence allocation stops at the last valid 60-bit identity.
Regressions cover numeric-prefix aliases, symlinked files, an out-of-range data
generation, allocation at the final identity, legacy filename compatibility,
and rejection without partial manager registration.

### CR-35 — NativeIndex WAL recovery could silently omit durable mutations

[`IndexWAL::recover`](../lib/index/native/index_wal.cpp) discovered generations
with prefix-consuming `std::stoull`, skipped unrecognized directory entries,
rebuilt a canonical path instead of replaying the entry it inspected, and
followed symlinks. Replay then classified a fully present CRC-invalid or
malformed frame as a discardable tail, and allowed incomplete tails in old
sealed generations. Fresh open used create-plus-truncate, counters could wrap,
and an empty recovered generation could later be lazily truncated and reused.
Creation and retirement also omitted the directory durability barriers needed
to make the generation namespace crash-safe.

Commit `81692a4` makes the index-WAL directory an exclusive namespace of exact
canonical regular files. Only the newest generation may end in an incomplete
header/body or zero padding; complete corruption, sequence discontinuity, and
any torn sealed generation preserve the source and abort startup. Generation
and record identities reject exhaustion, fresh paths use exclusive creation,
and a recovered empty/torn file is durably retired before accepting new writes.
Creation and deletion now sync the directory, while destructor fallback refuses
to follow or overwrite an unowned collision. Regressions cover corruption,
numeric-prefix aliases, symlinks, nonempty collision, generation exhaustion,
torn sealed generations, and a torn first frame followed by a new write.

### CR-36 — NativeIndex manifest/SSTable recovery could lose durable generations

[`Manifest::recover`](../lib/index/native/manifest.cpp) treated a fully present
CRC-invalid frame like an incomplete tail, stopped at the preceding prefix, and
then rewrote that prefix as a clean snapshot. Complete malformed records could
also mutate part of the recovered state before parsing stopped. At the next
layer, [`NativeIndex::refreshSSTables`](../lib/index/native/native_index.cpp)
logged and skipped a manifest-listed missing file. The SSTable footer protected
neither bloom nor index metadata and carried no embedded file identity, so a
single bloom-bit change could create silent false negatives and two physical
generations could be swapped while retaining plausible manifest metadata.

Compaction had the inverse crash-boundary error: after finishing and syncing its
new SSTable it deleted that output on any later exception, including an
ambiguous manifest flush/fsync result. If the manifest append had reached stable
storage despite the reported error, recovery would then require a file the
failure path had removed. Manifest/SSTable creation also followed non-regular
path collisions, and manifest file-number allocation could wrap.

Commit `d363348` makes manifest record parsing strict and atomic, preserves the
source and fences startup on every complete checksum/structure error, and only
repairs a physically incomplete final header/body. Manifest-listed generations
must all be regular and must match file size, entry count, key range, timestamp,
and—when available—embedded identity. SSTable v2 adds a CRC over bloom/index
metadata and a full 64-bit file identity. Legacy v1 remains readable, but its
unchecksummed bloom is never trusted and every CRC-protected block is validated
against the index once at open. Output collisions refuse symlinks/non-regular
paths, allocation cannot wrap, and manifest/SSTable opens use no-follow flags.
Compaction aborts only an unfinished output; once `finish()` establishes the
durability boundary, any manifest ambiguity preserves the output, and successful
source cleanup includes a parent-directory barrier. Regressions cover complete
manifest corruption and preservation, malformed records, bad magic, exhaustion,
symlink collisions, missing/swapped SSTables, metadata corruption, v1 migration
safety, and publication failure after durable output creation.

### CR-37 — NativeIndex startup did not reconcile its SSTable namespace

The manifest is authoritative for live NativeIndex generations, but startup
previously inspected only the files named by it. A crash after an SSTable became
durable but before manifest publication could therefore leave an unreferenced
canonical output forever. Successful compaction could likewise strand obsolete
sources when post-publication unlink failed. Repeated failures could consume the
volume. More importantly, non-canonical names and non-regular paths were ignored
even though they might represent damaged or partially renamed durable state, so
the node could serve without proving that the on-disk index namespace was
complete.

Commit `2749027` scans the entire NativeIndex directory after strict manifest and
live-SSTable validation but before WAL recovery can flush or the index can serve.
It validates the whole namespace before mutating it, preserves every
manifest-listed generation, removes only exact canonical unreferenced SSTables
and a regular stale `MANIFEST.tmp`, then always syncs the directory. Unknown
names, symlinks, and other non-regular entries are preserved and fence startup.
The reconciliation remains startup-only because an ambiguous live manifest
fsync can leave the in-memory manifest older than the durable manifest; using
that stale in-memory view for a runtime sweep could delete a generation required
on restart. Regressions cover partial and complete orphan outputs, live-file
preservation and queryability, malformed-name fail-closed behavior, symlink
target preservation, and stale manifest-temporary cleanup.

### CR-38 — colocated live gates had no aggregate memory bound

Every gate launched three to five Seastar servers without `--memory`. Each
process therefore derived its allocator size from the same host-wide available
memory rather than from a per-node share. The aggregate was neither bounded nor
stable across machines, and snapshot catch-up adds temporary payload, TSM-open,
index, and compaction pressure exactly when the evidence is most valuable.

The first strengthened empty-directory catch-up run reached 400/400 accepted
bulk batches, nine donor snapshots, zero oversize refusals, and successful exact
probe deletion, but the execution environment terminated the run while the
fresh node was still opening and compacting its restored state. The run produced
no final assertions and is explicitly neither a pass nor evidence of a product
OOM; it demonstrates that the harness itself did not bound or isolate the
resource question.

Commit `b2c7d0b` gives every gate node an explicit 8 GiB default Seastar budget,
overrideable through `GATE_SERVER_MEMORY`, and documents that the aggregate is
`node count * per-node budget`. The same commit makes restart catch-up prove the
returning durable root is empty and adds live plus exactly deleted probes. All
gate scripts pass `bash -n`, and the server advertises the exercised `--memory`
option. CR-FIX-078 remains open until the bounded empty-node gate completes on
the exact release candidate.

### CR-02 and CR-03 — snapshot contents and live installation were unsafe

At the reviewed baseline, `SnapshotPayload` carried a manifest and raw TSM file
bytes, while the producer in `engine_local_store.cpp` passed an all-zero catalog
hash and did not export NativeIndex/catalog records. The round-trip test verified
installed data through a precomputed series ID rather than normal series
discovery.

The baseline snapshot reader also required deletes/tombstones to have already
been materialised into the TSM view, while the producer neither enforced that
precondition nor shipped tombstone sidecars. The architecture promised a
snapshot containing catalog, index extract, data extents, and tombstone objects;
the baseline payload did not meet that contract.

Baseline installation copied source filenames into the live TSM directory. If a
rank was already registered, `TSMFileManager::addTSMFile` kept the old open
object and closed the new one. VShard-partitioned compaction was also described
in the integration plan but not enabled by the production server, so a shipped
TSM could contain data outside the target VShard.

The snapshot-safety and live-replacement passes close these storage failures.
Payload v2 includes the exact catalog, creation ships a resolved VShard-pure
object, and the receiver preflights the whole data/catalog bundle before
mutation. Installation serializes generation replacement, quiesces target WAL
conversion, durably deletes the prior target generation from WALs and every TSM,
preserves foreign VShards in mixed files, publishes a receiver-ranked object,
and reconstructs the exact catalog, value types, and day membership. Exact
metadata scans authoritative surviving primary rows, so replaced catalog
aggregates cannot leave phantom series or measurements.

Raft does not advance `appliedIndex` until the complete install resolves, and
production data, metadata, and catalog reads use the resulting apply-lag fence,
so clients cannot observe an intermediate generation. Restart tests inject at
all six durable replacement checkpoints, reopen the Engine before each retry,
and prove the resulting generation is exact. Separate coverage proves recovery
from a post-rename directory-barrier failure, idempotent byte-exact retry,
synchronous retirement of superseded pure snapshot objects, preservation of
foreign mixed-file and WAL data, immediate visibility on a running non-empty
Engine, and restart of background compaction after its install drain.

This closes CR-FIX-012. CR-FIX-011 remains open for the bounded empty-node live
gate, replicated-retention coverage, and format negotiation. The current
producer deliberately emits at most one file and the receiver rejects
multi-file payloads; chunking and multi-object transport are tracked by
CR-FIX-060 rather than required by the current producer's install contract.

### CR-18 — snapshot upgrade compatibility is unspecified

Payload v2 is intentionally fail-closed: upgraded production code rejects
legacy catalog-less v1 snapshots, while an older binary cannot decode v2.
Legacy exact-delete receipts add payload v3, and bounded receipts plus their
monotonic retirement floor add payload v4; v2 remains byte-identical for
snapshots with no delete state. No group-0 capability bit, negotiated minimum
version, upgrade order, or offline upgrade requirement currently prevents mixed
versions from attempting an incompatible InstallSnapshot. This is an
availability failure rather than silent state corruption, but it blocks a
supported rolling production upgrade.

The exact-delete transport first introduced a v3-only replicated-command verb;
bounded receipts now add command tag 5 and the typed `Expired` RPC outcome. The
client negotiates before use and refuses to send an unsupported command to an
old peer, so a mixed cluster fails closed rather than misframing a mutation, but
deletes are unavailable across that version boundary. Existing payload-v3
receipts have no issuance time, never expire, and consume the 1,024-receipt hard
cap; a VShard containing more than that must fail startup rather than silently
discard deduplication history. An upgrade therefore needs a preflight for that
state plus a committed capability floor, or an explicit offline upgrade and
rollback rule.

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
  The guard remains active for RF=1, old peers, and every RF&gt;1 pattern or
  mixed-pattern request. Exact targets alone use the partial CR-FIX-010
  implementation; the discovery building block is not production-wired because
  retrying its expansion can change the target set.
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
  cap; a real single-voter Raft test proves write/delete/query ordering. Exact
  commands now require a stable HTTP `Idempotency-Key` plus the original Unix-ms
  `Idempotency-Key-Timestamp`; each request's canonical targets are encoded as
  one bounded command per VShard, and the derived batch ID, batch hash, original
  apply index, and issuance time form one replicated receipt. A retry after an
  intervening write is a no-op, ambiguous router outcomes can safely be retried,
  and partial cross-VShard batches return retryable failure. Payload v4 encodes
  retained receipts plus the monotonic retirement floor only when covered by the
  compacted Raft boundary. Modern state is bounded by one hour and 1,024 receipts
  per VShard; retired retries fail terminally as HTTP `409` rather than executing
  again. Tests prove command/receipt codec rejection, conflicting-ID fail-stop,
  snapshot-boundary filtering, lightweight local snapshot recovery, bounded
  retirement, typed rejection through the real RPC socket, and a real
  compacted-journal restart followed by a harmless old-delete retry.
  A three-real-Engine RF=3 test commits the delete, fails over leadership, retries,
  restarts a caught-up replica from its Engine directory and Raft journal, elects
  that replica, and retries again; both retries preserve a later write. The
  previously implemented v4 pattern discovery remains internally covered,
  but production now fails every pattern or mixed-pattern request before
  expansion/proposal because a retry could discover a concurrently created new
  target. The batch codec rejects empty, duplicate, unsorted, oversize, and
  cross-VShard input; HTTP preflights the exact encoded Raft-entry size and caps
  concurrent VShard proposals at 32. Snapshot production refuses an unsafe
  boundary older than the last receipt-floor advancement and exposes
  `snapshots_skipped_delete_state`. `8ae846c` derives an exact boundary from the
  first surviving active revision, advances delete-only snapshots directly, and
  conditionally rolls an active write barrier so the next sweep can compact.
  CR-FIX-065 retains the sustained live workload and journal-reclamation proof.
  CR-FIX-010 remains open for a replicated/frozen pattern plan and the external
  multi-process release gate. `445f1f0`, `8b8536d`, `6912dfb`, `ecb63a5`,
  `a03fe1d`, `8ae846c`.
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
  empty Engine through the real state-machine apply path: normal discovery
  returns the surviving series while a fully deleted series in the same VShard
  is absent from both installed data and the rebuilt catalog. A deterministic
  Raft-driver regression pauses inside the state-machine install and proves the
  committed/applied gap remains visible until both data and catalog work finish;
  a real EngineLocalStore regression proves data queries, metadata, and pattern
  discovery all fail closed during that interval and become visible afterward.
  The public multi-process gate now wipes and proves absence of the returning
  node's entire durable root and drives live/deleted probes, but its first run
  was interrupted and supplies no completion evidence. A bounded rerun,
  replicated-retention coverage (blocked on CR-FIX-040), and format negotiation
  remain. Generation-atomic storage replacement is closed separately by
  CR-FIX-012.
- [x] **CR-FIX-012 — make live snapshot installation generation-safe and
  atomic.** Owner: storage. Install into unique immutable object names or replace
  the manager's generation under a fence; remove superseded VShard state without
  colliding with open ranks. **Done when:** install onto a non-empty running
  Engine is immediately visible without restart, survives an injected crash at
  every publish step, and cannot expose old/new mixtures.
  **Closed:** the complete payload is validated before mutation; installation
  is serialized per Engine; target WAL conversion is quiesced; durable WAL and
  TSM generation tombstones remove all old target state while mixed-file and
  active-WAL foreign VShards remain visible. Publication uses receiver-local
  ranks, reconciles an object left after an interrupted directory barrier, and
  requires raw-byte plus logical/catalog identity before reuse. The exact
  catalog/type/day view is installed under the Raft apply fence, superseded pure
  objects are durably retired, and a fresh WAL generation is forced afterward.
  A live non-empty regression proves immediate query and metadata visibility,
  no old/new mixture, foreign-data preservation, and exact retry without object
  duplication. Six injected durable checkpoints each close and reopen the
  Engine before retry; separate post-rename barrier and second-generation
  retirement tests prove orphan recovery and on-disk cleanup. A compactor
  regression proves its loop and underlying enable flag resume after the
  install drain. `023d9c3`, `d5f4755`, `7f6d7e8`, `7760ebd`, `6557666`, and
  `c8f28c8`.
- [ ] **CR-FIX-013 — enable and verify VShard-partitioned compaction in cluster
  mode.** Owner: storage. **Done when:** production startup enables the setting,
  tests prove generated snapshot files contain only permitted VShard data, and
  mixed legacy files have a documented migration path.
  **Progress:** partitioned server startup enables the mode before starting the
  compaction loop. The partition path now materialises existing tombstone
  sidecars and rejects a concurrent tombstone-generation race; storage/snapshot
  regressions prove mixed tier-0 input is emitted as delete-resolved VShard-pure
  output. Output publication also fails closed before retiring source TSMs if
  the destination directory cannot be synced. The on-disk migration/rollback
  procedure for existing mixed higher-tier files remains open.
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
- [x] **CR-FIX-018 — make TSM generation publication fail closed.** Owner:
  storage. A WAL or source TSM remains authoritative until the renamed output's
  parent directory has synced successfully. Injected directory-sync failures
  prove WAL conversion and both compaction layouts propagate the failure and
  preserve their prior durable generation.
- [x] **CR-FIX-019 — reject invalid or ambiguous immutable generations.**
  Owner: storage. Startup propagates TSM open/index errors and duplicate ranks;
  live registration propagates the same collisions before source retirement.
  WAL-output corruption, corrupt-startup, duplicate-startup, duplicate-live-add,
  and planned-sequence-zero regressions cover the failure boundaries.
- [x] **CR-FIX-019A — make source retirement tombstone-safe and durable.**
  Owner: storage. Source TSM names are unlinked and their directory is synced
  before live registration or tombstone sidecars are removed. Failures remain
  retryable with the source registered; pinned readers keep deletion ranges
  after sidecar cleanup. The injected directory-sync regression covers both the
  failed intermediate state and idempotent completion.
- [x] **CR-FIX-019B — reject incomplete TSM logical generations.** Owner:
  storage. A present tombstone sidecar must load and validate before its TSM can
  be registered. Corrupt-sidecar startup proves the failure propagates without
  exposing raw points; valid sidecars still reload and filter normally.
- [x] **CR-FIX-019C — make tombstone mutation publication atomic, serialized,
  and directory-durable.** Owner: storage. Sidecars publish by synced temporary
  file, atomic rename, and parent-directory sync; per-TSM serialization spans
  mutation through durable publication and cleanup. Injected sync failure and
  concurrent-delete reopen regressions cover retry and complete range survival.
- [x] **CR-FIX-019D — make WAL creation and retirement directory-durable and
  retryable.** Owner: storage. New segment names sync before use; live and
  recovered source unlinks sync before success and tolerate a retry after the
  name is absent. A post-publication retry skips the already-registered TSM and
  completes only the WAL retirement barrier.
- [x] **CR-FIX-019E — retain and continuously retry failed WAL conversions.**
  Owner: storage. One shutdown-aware gate holder owns all publication and
  retirement attempts. Unpublished stores remain queryable and snapshot-pending;
  published stores awaiting WAL retirement remain outside memory queries but
  inside retained-memory admission. Repeated-failure regressions cover both
  states and recovery without duplicate immutable publication.
- [x] **CR-FIX-019F — fail startup when recovered WAL data is not live.**
  Owner: storage. Any recovered-WAL conversion error propagates after preserving
  the source; startup cannot create a fresh active store over missing recovered
  contents. An injected publication-barrier failure also proves the next clean
  startup can load the uncertain output, replay and retire the WAL, and finish.
- [x] **CR-FIX-019G — fail startup on complete WAL corruption.** Owner:
  storage. Fully read frames must validate length, padding, CRC, command/value
  type, and payload before recovery can succeed. EOF in the final incomplete
  frame remains a recoverable torn tail; ambiguous legacy/current framing fails
  closed. Manager-level coverage proves the corrupt source is preserved and no
  partial store becomes live.
- [x] **CR-FIX-019H — enforce WAL segment identity without destructive
  collision handling.** Owner: storage. Recovery accepts only canonical,
  fully consumed 64-bit sequence names and fails on every other `.wal` artifact.
  Allocation cannot wrap, fresh create cannot truncate a nonempty path, and an
  empty failed-create artifact remains safely retryable.
- [x] **CR-FIX-019I — enforce Raft journal segment identity without destructive
  collision handling.** Owner: storage. Recovery treats the journal directory
  as an exclusive namespace and fences on non-canonical or non-regular entries.
  Startup and rotation cannot wrap the 64-bit identity, and fresh segment
  creation cannot truncate an existing path.
- [x] **CR-FIX-019J — enforce TSM immutable identity and rank bounds.** Owner:
  storage. Parsing consumes the exact supported schema, startup refuses
  non-regular immutable entries, both packed ranks validate before open or
  registration, and sequence allocation cannot cross its 60-bit identity space.
- [x] **CR-FIX-019K — fail closed on invalid NativeIndex WAL recovery.** Owner:
  storage. Discovery accepts only canonical regular generations; only the newest
  generation may have an incomplete tail; complete corruption and sequence or
  identity violations fence startup. Creation cannot truncate a collision, an
  empty recovered generation is rotated before reuse, and namespace mutations
  include their directory durability barrier.
- [x] **CR-FIX-019L — bind NativeIndex manifests to complete, validated SSTable
  generations.** Owner: storage. Complete manifest corruption is preserved and
  fences startup; every listed SSTable must exist and match its durable metadata.
  SSTable v2 checksums the bloom/index region and embeds its identity, while v1
  disables the untrusted bloom and validates index/data agreement before use.
  Compaction retains a durable output across ambiguous manifest publication and
  syncs obsolete-name cleanup.
- [x] **CR-FIX-019M — reconcile the NativeIndex recovery namespace.** Owner:
  storage. Startup validates every index-directory entry against the recovered
  manifest before serving, durably reclaims only canonical unreferenced outputs
  and stale manifest temporaries, preserves live generations, and fails closed
  without following or deleting ambiguous paths.

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
  committed data-plane map. **Progress:** the existing group-0 command and
  snapshot decoders now fail closed on unknown trailing data, and a corrupt
  snapshot installation throws without changing the old state or advancing its
  applied boundary. Focused codec/state-machine evidence passes 14/14. The
  production host, dedicated journal, explicit bootstrap ceremony, recovery,
  and committed-state publication remain outstanding.
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
  Owner: index/journal. Resolve D-10 before enabling shared journals by
  default. **Done when:** real-disk shared-journal GC evidence demonstrates
  bounded reclamation. **Progress:** D-38 is closed in `7151f5d`: the six
  in-tree crash simulations now use an explicit test-only abandonment boundary
  that cancels timers and drains both background gates and any threshold flush
  without performing a clean WAL close. A deterministic lifecycle regression
  holds each gate open and proves abandonment waits; unsupported destruction
  now fails with an actionable lifecycle diagnostic instead of Seastar's opaque
  `~gate()` SIGILL. Production callers must still `co_await close()`. D-39 is
  closed in `5b22b81`: shared GC no longer stops at an unrelated pin, recovery
  permits only snapshot-covered holes, and shared snapshot production targets a
  15-minute fair scan without concurrent snapshot materialisation. The status
  endpoint exposes the counters needed for sustained reclamation evidence. The
  remaining D-10 blocker is the real-disk measurement; shared journals remain
  opt-in until it is recorded.
- [x] **CR-FIX-064 — make replicated startup open-file-safe and
  exception-safe.** Owner: server/cluster composition. Resolve the descriptor
  requirement before opening Engine/Raft state, raise an ordinary soft limit
  when permitted, fail with an actionable diagnostic when the hard limit is too
  low, and stop partially-started sharded services without replacing the root
  exception with a destructor trap. **Done when:** a soft=1,024/hard&gt;=8,192
  subprocess opens all 4,096 groups and serves HTTP with an effective 8,192
  limit; a hard=1,024 subprocess exits 1 before Engine startup; and lifecycle
  source regressions plus a live three-node gate pass. Completed in `69ac879`.
- [ ] **CR-FIX-065 — prove receipt retirement cannot starve Raft-log
  compaction.** Owner: snapshot/data. The safety fence in `a03fe1d` defers a
  snapshot when its flushed data boundary predates the entry that advanced the
  delete-receipt floor. A delete-only or delete-heavy VShard may not naturally
  advance that boundary, so repeated safe deferral can still permit unbounded
  journal growth. **Done when:** the implementation advances a safe snapshot
  boundary without inventing data or truncating an unflushed write, and a
  sustained delete/mixed workload proves `snapshots_skipped_delete_state` does
  not grow forever, `snapshots_taken` advances, journal bytes plateau or reclaim,
  and restart cannot re-execute a retired delete over a later write.
  **Implementation complete in `8ae846c`; live evidence remains.** The producer
  now observes pending rolled generations and the active store's oldest
  surviving replicated revision in one storage-core turn. It compacts a
  delete-only applied prefix directly. If receipt retirement is instead blocked
  by an active point, it conditionally rotates that exact active generation and
  waits for the existing bounded WAL-to-TSM conversion, avoiding repeated empty
  rollovers. The payload fence is promoted to `snapshot index + 1`, preserving
  the receiver binding while retaining every unflushed suffix entry. Unit
  regressions prove the first-unflushed boundary, fail-closed missing revisions,
  delete-only advancement, conditional rollover progress without another client
  write, compacted-journal restart, terminal expiry of the retired retry, and
  survival of the later value. A sustained live delete/mixed workload with
  journal-byte plateau/reclamation is still required before checking this row.

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
  negotiation and upgrade policy.** Owner: control plane/release. Gate payload
  v2-v4 snapshots, command tag 5, and the `Expired` RPC outcome until group 0 or
  an equivalent handshake proves every sender and receiver supports them, or
  require and document an offline upgrade. Preflight legacy non-expiring receipt
  counts against the 1,024-per-VShard cap. **Done when:** old-to-new and
  new-to-old snapshot and command attempts follow the documented safe path,
  mixed-version behavior is covered by a multi-process test, and rollback
  constraints are explicit. **Implementation progress (`9ecd0e6`):** the one
  ordered capability line now assigns snapshot payload v2 to activation v2,
  legacy durable receipts/payload v3 to v3, and bounded command tag 5, payload
  v4, and the typed `Expired` result to v5 (v4 remains the node-query redirect
  protocol). `ReplicatedVShardHost` refuses a command before proposal and a
  snapshot before encoding unless the committed per-shard gate is sufficient;
  the data-plane client also refuses a bounded command before framing it for a
  peer below protocol v5. Exact partitioned deletes return HTTP 409
  `CLUSTER_FORMAT_NOT_ACTIVE` until v5 is active. Readiness now fails while the
  local minimum format is below snapshot v2, rather than treating a running but
  permanently refusing snapshot timer as healthy. Codec decoders remain
  unconditional for replay and upgrade. **Still open:** compose and persist
  group 0 in the production server, close the data-voter-set/admission hole,
  publish the committed activation to every shard, preflight legacy receipt
  counts, state the downgrade rule, and run old/new multi-process snapshot and
  command tests. Because the production server has no group-0 bridge caller,
  this fail-closed slice deliberately leaves clustered readiness false and
  bounded deletes unavailable; it does not close this task.
- [x] **CR-FIX-077 — make live-gate orchestration fail closed.** Owner:
  release/tests. Restrict reset targets to direct `/tmp/tsgate_*` roots, retry
  removal and prove absence before recreation, never delete a running arm's
  data, require cluster-aware health before benchmark load, preserve benchmark
  stdout/stderr, and make benchmark preflight failures nonzero. **Done when:**
  helper negative tests reject unsafe paths, every gate passes `bash -n`, an
  unreachable benchmark endpoint exits nonzero, and the four gates in
  CR-FIX-071 pass without reset warnings or empty load transcripts. Completed in
  `09a62c5` and `2e06cb8`.
- [ ] **CR-FIX-078 — bound aggregate memory in colocated live gates.** Owner:
  release/tests. Every server launch now supplies an explicit, overrideable
  8 GiB per-process Seastar memory limit; all ten launch sites and all shell
  scripts pass source/syntax validation. **Done when:** the strengthened
  empty-directory `restart_catchup_gate.sh` completes under that bound, records
  peak RSS and `/tmp` use, installs at least one snapshot on the recreated node,
  and proves both the surviving and exactly deleted public-path probes. The
  pre-limit attempt was interrupted during catch-up and cannot close this task.

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

Shared-journal reclamation validation for `5b22b81`:

```text
targeted GC/replay/scheduler tests:        6/6 passed
current full unit suite:             4346/4346 passed (444 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
timestar_http_server:                    built successfully
git diff --check:                        passed
```

TSM publication durability validation for `fef4886`:

```text
injected directory-sync regressions:       3/3 passed
related writer/manager/compactor/WAL:     68/68 passed
current full unit suite:             4349/4349 passed (444 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
timestar_http_server:                    built successfully
git diff --check:                        passed
```

TSM registration validation for `3d2d607`:

```text
focused failure/sequence-zero regressions: 6/6 passed
related manager/WAL/compaction tests:    72/72 passed
current full unit suite:             4352/4352 passed (444 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
timestar_http_server:                    built successfully
git diff --check:                        passed
```

TSM source-retirement validation for `f0e28f0`:

```text
focused retirement/deletion regressions: 13/13 passed
related manager/compactor/tombstone tests: 100/100 passed
current full unit suite:              4353/4353 passed (444 suites, -c 2)
current socket-backed cluster suite:     45/45 passed (8 suites, -c 2)
timestar_http_server:                    built successfully
git diff --check:                        passed
```

Fail-closed tombstone validation for `da55952`:

```text
focused corrupt/valid/open-cleanup tests: 13/13 passed
related manager/tombstone/compactor tests: 109/109 passed
current full unit suite:               4354/4354 passed (444 suites, -c 2)
current socket-backed cluster suite:      45/45 passed (8 suites, -c 2)
timestar_http_server:                     built successfully
git diff --check:                         passed
```

Atomic tombstone-publication validation for `a1beb94`:

```text
focused publication/retry/concurrency tests: 15/15 passed
related manager/tombstone/compactor tests:  135/135 passed
current full unit suite:                  4357/4357 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

WAL lifecycle durability validation for `bb5b871`:

```text
focused lifecycle/source-boundary checks:    4/4 passed
related WAL/conversion/storage tests:      138/138 passed
current full unit suite:                  4360/4360 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

WAL conversion-retention validation for `e201343`:

```text
focused publication/retirement retry tests:  3/3 passed
related WAL/conversion/storage tests:        96/96 passed
current full unit suite:                  4362/4362 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

Fail-closed WAL startup validation for `6ad2c93`:

```text
focused recovery publication/restart test:   1/1 passed
related WAL/recovery/storage tests:          97/97 passed
current full unit suite:                  4363/4363 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

Fail-closed WAL corruption validation for `9a42d84`:

```text
focused corruption/legacy/torn-tail tests:   5/5 passed
related WAL/recovery/storage tests:        100/100 passed
current full unit suite:                  4366/4366 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

WAL identity/allocation validation for `41fdc34`:

```text
focused name/collision/wide-sequence tests:  5/5 passed
related WAL/recovery/storage tests:        104/104 passed
current full unit suite:                  4370/4370 passed (444 suites, -c 2)
current socket-backed cluster suite:        45/45 passed (8 suites, -c 2)
timestar_http_server:                       built successfully
git diff --check:                           passed
```

Raft journal identity validation for `a58d2a9`:

```text
focused journal-writer identity/recovery tests: 17/17 passed
related writer/sink/GC/Raft persistence tests:  47/47 passed
current full unit suite:                    4374/4374 successful
  passed/skipped:                           4365/9 (444 suites, -c 2)
current socket-backed cluster suite:          45/45 passed (8 suites, -c 2)
timestar_http_server:                         built successfully
git diff --check:                             passed
```

TSM immutable identity/rank validation for `6a73809`:

```text
focused TSM rank/manager suites:              49/49 passed
current full unit suite:                    4380/4380 successful
  passed/skipped:                           4371/9 (444 suites, -c 2)
current socket-backed cluster suite:          45/45 passed (8 suites, -c 2)
timestar_http_server:                         built successfully
git diff --check:                             passed
```

NativeIndex WAL recovery validation for `81692a4`:

```text
focused IndexWAL/NativeIndex recovery suites: 21/21 passed
current full unit suite:                    4387/4387 successful
  passed/skipped:                           4378/9 (444 suites, -c 2)
current socket-backed cluster suite:          45/45 passed (8 suites, -c 2)
timestar_http_server:                         built successfully
git diff --check:                             passed
```

NativeIndex manifest/SSTable generation validation for `d363348`:

```text
focused manifest/SSTable/compaction recovery suites: 65/65 passed
current full unit suite:                            4401/4401 successful
  passed/skipped:                                   4392/9 (444 suites, -c 1)
current socket-backed cluster suite:                  45/45 successful
  passed/skipped:                                     43/2 (8 suites, -c 1)
timestar_http_server:                                 built successfully
git diff --check:                                     passed
```

NativeIndex namespace reconciliation validation for `2749027`:

```text
focused WAL/manifest/SSTable/compaction recovery suites: 68/68 passed
current full unit suite:                                4405/4405 successful
  passed/skipped:                                       4396/9 (444 suites, -c 1)
current socket-backed cluster suite:                      45/45 successful
  passed/skipped:                                         43/2 (8 suites, -c 1)
timestar_http_server:                                     built successfully
git diff --check:                                         passed
```

Replicated pattern-delete expansion validation for `1f61f49`:

```text
focused codec/index/fence/routing/HTTP regressions: 12/12 passed
focused real-socket v4/old-peer regressions:          2/2 passed
current full unit suite:                         4416/4416 passed (445 suites, -c 2)
current socket-backed cluster suite:                47/47 passed (8 suites, -c 2)
timestar_http_server:                               built successfully
git diff --check:                                   passed
```

Empty-snapshot coverage and live-gate memory-bound validation for `b2c7d0b`:

```text
fresh Engine snapshot apply/discovery/delete regression: 1/1 passed (-c 2, --memory 2G)
cluster gate shell syntax:                              all scripts passed bash -n
bounded live-server launches:                           10/10 use --memory
timestar_http_server --help-seastar:                    --memory option present
empty-node live gate:                                   INCOMPLETE; interrupted during catch-up
```

The incomplete live run is not counted as release evidence. Before interruption
it accepted 400/400 campaign batches, produced nine donor snapshots, refused
zero oversize snapshots, and committed the exact probe delete; it did not reach
the caught-up/readback assertions. Its three test roots were about 3 GiB in
aggregate and were reclaimed after preserving diagnostic tails.

Snapshot read-visibility validation for `872f7e1`:

```text
focused snapshot/apply-fence regressions:       11/11 passed (-c 2, --memory 2G)
all RaftGroup + EngineLocalStore regressions:   13/13 passed (-c 2, --memory 2G)
timestar_unit_test:                             built successfully (-j2)
git diff --check:                               passed
```

Atomic live VShard replacement validation for `6557666` and `c8f28c8`:

```text
live replacement/retirement/compactor regressions:       7/7 passed
adjacent snapshot/restore/index/metadata/WAL regressions: 11/11 passed
every durable checkpoint reopened before retry:           1/1 passed
timestar_unit_test:                       built successfully (-j2)
all test processes:                              --smp 1 --memory 1G
git diff --check:                                         passed
```

These tests are deterministic process-reopen fault simulations, not a
multi-process release gate. They close the storage replacement contract in
CR-FIX-012; the bounded empty-node public-path gate remains part of CR-FIX-011
and CR-FIX-078.

Snapshot-durable and bounded exact-delete retry validation for `445f1f0`,
`8b8536d`, `6912dfb`, `ecb63a5`, and `a03fe1d`:

```text
command/snapshot/state-machine/router/HTTP focused pass: 32/32 passed
real compacted-journal receipt recovery and retry:          1/1 passed
three-real-Engine leader-failover/replica-restart retry:     1/1 passed
partitioned exact/pattern fail-closed HTTP pass:           10/10 passed
per-VShard batch codec/state/router/HTTP focused pass:     35/35 passed
bounded receipt/state/snapshot/router/HTTP focused pass:   52/52 passed
typed Expired outcome over the real v3 socket:               1/1 passed
timestar_unit_test:                         built successfully (-j2)
timestar_http_server:                       built successfully (-j2)
timestar_cluster_socket_test:               built successfully (-j2)
all test processes:                                --smp 1 --memory 1G
git diff --check:                                           passed
```

Receipt-retirement compaction implementation validation for `8ae846c`:

```text
VShard fence + full replicated-host suites:       25/25 passed
payload/state-machine/install/WAL compatibility:  27/27 passed
timestar_unit_test:                    built successfully (-j1)
timestar_http_server:                  built successfully (-j1)
all test processes:                           --smp 1 --memory 1G
compiler temporary directory:                         build/tmp
git diff --check:                                      passed
```

This is deterministic implementation and restart coverage, not the sustained
multi-process delete-heavy measurement required by CR-FIX-065. No live servers
were started for this validation; the single-job build and repository-local
temporary directory keep host memory and `/tmp` use bounded.

Format-emission/readiness validation for `9ecd0e6`:

```text
codec/host/group-0/admission/HTTP focused tests: 27/27 passed
write-failure taxonomy:                            3/3 passed
complete DataPlaneRpc + ClusterDataPlane suites: 31/31 passed
additional targeted socket/readiness checks:       4/4 passed
timestar_unit_test:                    built successfully (-j1)
timestar_cluster_socket_test:          built successfully (-j1)
timestar_http_server:                  built successfully (-j1)
all test processes:                           --smp 1 --memory 1G
compiler temporary directory:                         build/tmp
git diff --check:                                      passed
```

No live or multi-process server was started for this validation. The first
combined build requested a nonexistent target name only after the unit and
socket targets had built; the correctly named `timestar_http_server` target was
then built successfully. This is deterministic fail-closed coverage, not the
mixed-binary evidence required to close CR-FIX-076.

This closes the known exact-delete retry corruption path and bounds modern
receipt memory, but not CR-FIX-010 as a whole. Pattern deletes need a replicated
immutable expansion plan before re-enablement, and the external multi-process
release gate remains required even though the deterministic RF=3
leader-failure/replica-restart gate now passes. `8ae846c` removes CR-FIX-065's
known implementation starvation path; the sustained live workload must still
show that snapshots keep advancing and journal bytes plateau or reclaim.
CR-FIX-076 remains an activation blocker: `9ecd0e6` prevents unsafe emission and
false readiness, but production group-0 activation, voter/admission coverage,
legacy-receipt preflight, downgrade policy, and mixed-binary evidence are still
missing.
