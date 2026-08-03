# Cluster production-readiness review

**Status:** BLOCKED — clustered deployment is not approved for production.

**Reviewed baseline:** `cluster-design`, reviewed 2026-08-01 and updated
2026-08-03.

This is the authoritative blocker list for the cluster redesign. Historical
milestone documents describe how the design evolved; they are not release
evidence.

## Current decision on versioning

The project is greenfield. Every protocol and persisted format retains an
explicit version marker, but v1 is the only supported version and its layout is
updated in place. Development data from retired layouts must be recreated.
Rolling upgrades, downgrades, cluster-format activation, capability collection,
and historical decoders are outside the greenfield contract.

See [protocol-versioning.md](protocol-versioning.md) for the complete marker
inventory and rules.

## Completed in the v1 simplification

- [x] Promoted the current typed WriteBatch, replicated-command, delete-receipt,
  snapshot, control-command, group-0 snapshot, TSM, tombstone, NativeIndex
  manifest, and SSTable layouts to v1.
- [x] Added or retained explicit v1 identity at each primary wire and durability
  boundary, including `TSR1` Raft envelopes and the `TSWL` WAL header.
- [x] Removed old encoder/decoder branches, compatibility fallbacks, format
  activation state, capability/inventory RPCs, and cluster-format readiness
  fields, including the retired fields in the production status endpoint.
- [x] Replaced DataPlane version-range negotiation with an exact-v1 connection
  handshake and reject peers that do not speak v1.
- [x] Made Raft use one v1 tag layout, including transfer votes and chunked
  snapshots; removed peer capability probing and old-tag normalization.
- [x] Made WAL recovery require the v1 header and CRC-framed entries; removed
  no-CRC replay.
- [x] Made node-query request and reply fields part of one mandatory layout
  instead of optional compatibility tails.
- [x] Removed the superseded double-only `DataCommand`/`DataStateMachine` stack
  and its duplicated router, replica-read, RF, and movement tests. Raft behavior
  tests now use protocol-neutral or current production state machines.
- [x] Replaced version-cross-product and historical-layout tests with compact v1
  round-trip, bounds, corruption, and unknown-version rejection tests.
- [x] Corrected proposal sizing for the promoted delta-varint v1 layout by
  charging the exact encoded size rather than a retired cross-version ratio.
- [x] Reframed raw and dictionary string blocks as explicit `STR1`/`STD1`
  variants of the same v1 contract and consolidated their shared header and
  full-buffer decode logic.
- [x] Removed the unused core-count rebalancer and its test suite; TSM readers
  now reject its retired filenames and non-canonical numeric aliases. Active
  compaction, snapshot, restore, and pushdown fixtures use canonical v1 names.
- [x] Removed the remaining unused `shard_N` migration planner, rewrite path,
  Engine migration/repartition scaffolding, and their migration-only tests.
  Retired development roots are detected only to fail before mutation; they
  are never decoded or rewritten. Runtime metadata no longer carries redundant
  copies of v1 constants after a footer has already been validated.
- [x] Removed NativeIndex's pre-bitmap migration and tag-value blob fallback;
  current v1 local-ID state restores normally and incomplete state fails closed.
- [x] Bound each `TSMJ1` movement job to its exact source voters and target map
  epoch, made Group 0 create the job and desired placement atomically, and made
  serving-map cutover require that exact job to reach `Done`.
- [x] Wired committed Group-0 serving-map cutover into the running node: the
  durable cache is persisted before the applied boundary advances, every
  reactor owns and updates its own routing directory, stale epochs cannot
  regress routing, exact replay repairs partial fan-out, and conflicting or
  incomplete maps fail closed. Restart selects a newer durable map over the
  static epoch-1 seed, and Group-0 recovery updates the runtime before local
  replica groups are instantiated.
- [x] Register token-authorized joining nodes in every live Raft/data transport
  before committing Group-0 learner membership. Unresolved addresses remain
  durably `Joining` and retry safely; recovered node records repopulate the
  runtime peer directory, and an address change retires cached clients instead
  of continuing to use the old endpoint.
- [x] Materialize a movement destination's data group through an exact-v1 peer
  RPC before learner addition. The destination derives all topology from its
  own committed Group-0 job, rejects stale controller terms, non-active or
  mismatched destinations, and conflicting placement, uses the production Raft
  limits, and validates any recovered stable or joint configuration before the
  group is registered or allowed to tick.
- [x] Run the production Group-0 movement scheduler and remote data-group leader
  actuator. A pass materializes the destination, follows bounded leader hints,
  executes exactly one idempotent transition, rechecks the controller term after
  every suspension, validates the returned job, and commits that one step through
  Group 0 before another pass can continue. Shutdown drains the loop, and status
  exposes pass, failure, and durable-advance counters.
- [x] Add authenticated, intent-only production topology routes for planning a
  VShard move and advancing a node through `Active -> Draining -> Removed`.
  Group 0 derives source voters and the next epoch, rejects backward or skipped
  lifecycle transitions, keeps a node non-removable while any serving-map, job,
  learner, or voter reference remains, and transfers data-group leadership away
  from a replacement victim before removing it.
- [x] Complete bounded whole-node evacuation and application-fenced removal.
  The Group-0 controller scans referenced VShards in canonical order, chooses a
  deterministic failure-domain-safe destination, retains only one current v1
  movement record, and drives one replacement through cutover before planning
  the next. The workflow is restart-safe because every decision is derived from
  committed Group-0 state. A draining Group-0 voter is first demoted to learner;
  removal waits for zero data references, final-map application on the departing
  node, application of its own `Removed` record, and learner eviction.
- [x] Retire a removed local data-group replica after committed serving-map
  cutover. The complete committed Group-0 map, whose publication required the
  exact movement job to reach `Done`, is the cluster-wide retirement
  certificate; teardown cannot depend on a removed victim receiving final
  `Cnew`. The lower-level direct retirement API remains stricter and requires a
  victim-local configuration that excludes this node. Retirement refuses new
  host operations, drains in-flight Raft work, installs a durable empty Engine
  generation, publishes a terminal reclaim floor, closes the writer, and
  atomically moves the journal into an exact-v1, epoch-named quarantine.
  Empty-generation install quiesces the VShard WAL,
  publishes durable memory and mixed-TSM fences, removes exact primary, type,
  postings, and day-discovery index state, immediately unlinks VShard-pure TSM
  objects, and leaves mixed-object bytes for the ordinary tombstone rewrite.
  The active journal remains the retry token until Engine cleanup succeeds. A
  durable marker then starts a fixed 24-hour grace period, after which the
  maintenance sweep deletes the journal generation and publishes
  retirement/reclamation counters. Exact map replay is idempotent; startup uses
  only the durable Group-0 serving map to finish crashes before or after the
  Engine transaction or quarantine rename and restarts the full grace period
  when its marker was not durable. Pre-cutover movement destinations are
  protected from being mistaken for obsolete replicas. Group-0 topology startup
  rejects the optional shared-journal layout until it has an equally safe
  per-group retirement generation protocol.
- [x] Publish every fresh Engine WAL atomically with a valid v1 header. Creation
  writes and flushes the exact header under a canonical `.wal.creating` name,
  publishes it with a no-replace same-directory link, removes the private name,
  and syncs the parent directory. Startup removes only exact reserved creation
  temporaries and rejects non-regular or non-canonical entries. A forced process
  exit can therefore expose either no final WAL or a recoverable final WAL,
  never the empty final file found by the topology crash gate. Focused tests
  cover failure before publication, failure after final-name publication,
  immediate independent recovery before append/close, retry, and refusal to
  truncate an existing generation.
- [x] Exercise exact VShard topology mutation through the production server.
  `topology_mutation_gate.sh` admits four nodes through Group 0, moves a checked
  VShard away and back twice under writes, forces process exit after Engine WAL
  generation deletion and after journal quarantine, recovers both windows,
  expires and reclaims the v1 quarantine, and materializes the deleted replica
  again from survivors. It then drains the replacement node, automatically
  evacuates its only VShard, fences Group-0 learner removal on state-machine
  application, and accepts the final lifecycle transition. The 2026-08-03 run
  reached serving-map epoch 5: 1,212 of 1,220 writes were acknowledged and the
  remaining responses were bounded retryable outcomes during leader movement.
  Every acknowledged point was visible on all four nodes, no node returned more
  points than were attempted, unauthenticated drain was rejected, premature
  removal failed closed, every node reported protocol version 1, and no process
  crashed outside the two asserted exit-86 checkpoints.
- [x] Disable the standalone, local-clock retention sweeper in partitioned mode
  and make the exact-v1 replicated cutoff command measurement-scoped. Replicas
  apply the controller-provided cutoff without consulting local time, walk the
  VShard catalog in bounded pages, and delete only the selected measurement's
  expired prefix. Validation rejects empty, oversized, NUL-containing, and
  zero-cutoff commands; focused tests prove measurement and VShard isolation.
- [x] Replicate retention policy and cutoff decisions. Group 0 now owns bounded
  TTL-only policy cells with exact-version CAS and tombstones, serializes one
  all-VShard sweep at a time, assigns a globally contiguous sweep ID, verifies
  the leader-sampled cutoff against the committed policy, and persists the
  first-unacknowledged VShard cursor. Each batch advances that cursor only after
  every routed cutoff proposal has committed and applied. Exact and superseded
  retries are no-ops; ID collisions or sequence gaps fail-stop. Each VShard
  therefore snapshots only its latest constant-space fence rather than a
  measurement-by-VShard map. Policy mutation for the measurement being swept is
  fenced until completion, and clustered downsampling remains rejected.
- [x] Enable the clustered `PUT/GET/DELETE /retention` API. Mutations reach only
  the current Group-0 leader, return the committed CAS version, reject stale
  versions, and make an exact ambiguous retry idempotent. A deleted cell retains
  its version; JSON and protobuf conflict/not-found responses expose the current
  version so another client can safely recreate the policy. Input and snapshot
  codecs bound policy count, measurement, TTL, cursor, and optional fence state.
- [x] Exercise retention through the production server. The bounded
  `retention_failover_gate.sh` run on 2026-08-03 started three one-reactor,
  1-GiB nodes with roots under `build/tmp`, admitted all three Group-0 voters,
  created/retried/conflicted a policy, killed the controller at durable VShard
  cursor 128, and completed sweep ID 1 under a new leader across all 4,096
  VShards. It removed only the expired target point, preserved another
  measurement and the newer target point, restarted the old leader, recovered
  the sweep/cutoff state, then proved delete version 2 and recreation as version
  3. Observed aggregate RSS was about 350 MiB and each live root about 32 MiB;
  the gate removed all three roots afterward.
- [x] Prove frozen pattern-delete retry through leader loss and restart. The
  bounded `pattern_delete_failover_gate.sh` run on 2026-08-03 wrote and indexed
  6,000 targets, then observed the complete 402,097-byte frozen plan on a second
  Group-0 voter while the original client was still waiting. It killed that
  coordinator, elected a new Group-0 leader, recovered all data-group leaders,
  inserted a newly matching series, and retried with the original identity. The
  retry reused exactly the 6,000 frozen targets and preserved the new match;
  changed request bytes returned the stable 409 conflict. Restarting the killed
  node recovered the same one-plan/6,000-target state without resurrection. The
  three one-reactor nodes stayed within their 1-GiB process limits and the gate
  removed all four `build/tmp` roots afterward.
- [x] Remove the VShard snapshot reactor-memory ceiling without adding another
  protocol version. Production snapshots now encode the exact existing `TSP1`
  v1 bytes directly from immutable Engine objects into owned sidecars, hydrate
  and stage exact `TSR1` v1 InstallSnapshot messages in paced chunks no larger
  than 4 MiB, and decode/install objects from disk through 1-MiB heap buffers
  with a cooperative yield after every buffer. Manifest and catalog metadata
  remain explicitly bounded at 16 MiB and 64 MiB, delete receipts at 1,024, and
  the complete file-backed snapshot at 1 TiB. A received chunk is staged only
  after its Raft term and snapshot boundary pass the preflight fence; the final
  file is size/hash validated and fsynced before it can become Ready state.
  Journal v1 records reference canonical sidecars, recover only the newest
  descriptor, retain the prior file until the replacement descriptor is
  durable, and remove crash-orphan producer/stage/extract files. Engine validates
  the Raft/manifest boundary before mutation. The 2026-08-03 focused run passed
  24 journal/sidecar tests, 40 Raft group/chunk/budget tests, 22
  replicated-host tests, and 18 Engine/payload tests. The dedicated
  one-reactor, 1-GiB gate streamed 128 MiB + 1 byte through leader hydration,
  exact-v1 framing, receiver disk staging, and final validation without a
  reactor-stall report, then returned `build/tmp` to its pre-gate size.
- [x] Prove bounded delete-receipt retirement under sustained load. The
  `delete_receipt_retirement_gate.sh` run on 2026-08-03 sent 1,100 sequential
  exact deletes to one RF=3 VShard, crossing its 1,024-receipt capacity on all
  replicas. Every replica stayed at exactly 1,024 receipts; the replicated
  capacity floor advanced once at the first eviction and again through all 76
  evictions; four real seed points became exactly three on every node, proving
  the canonical exact-series delete changed data rather than merely recording
  a receipt. An evicted retry returned the stable expired `409` while the newest
  retained retry remained an idempotent `200`. All three replicas
  snapshotted through the retirement entry, reported no retirement awaiting a
  snapshot, and reclaimed at least one sealed production-sized private journal
  segment. The gate also exposed and fixed a sidecar-lifetime leak: ordinary
  Raft syncs no longer forget the current durable sidecar, and a replacement is
  unlinked after its new descriptor is durable even if another Raft owner still
  holds the old handle. The corrected run took 7, 4, and 4 snapshots and deleted
  387, 2, and 2 sealed journal segments respectively; each VShard directory
  retained exactly one current canonical v1 sidecar. The three one-reactor,
  1-GiB nodes started at about 108--110 MiB RSS each, and the gate removed its
  repository-local roots afterward.
- [x] Close the bounded empty-node and compacted-restart storage blockers. The
  first empty-root run exposed two independent production defects: automatic
  leadership transfer flooded a recovering voter, and fire-and-forget Raft RPC
  futures accumulated without outbound admission until a 1-GiB Seastar node
  threw `std::bad_alloc`. Periodic balancing is now jittered and limited to 32
  transfers per node, requires exact target log match, pauses for leaderless or
  live-behind recovery, and observes a 30-second recovery quiet period. Raft
  transport now has one v1 delivery verb, always encoded as a per-peer batch
  frame even for one envelope, and admits at most 256 unresolved frames and 64
  MiB of encoded outbound data. There is no scalar fallback or negotiation
  branch. Registry tick order rotates so a bounded queue cannot permanently
  favor low VShard IDs. Separate real-socket tests exercise both admission
  bounds. The gate also corrected a documented exact
  series key from the invalid dot form to canonical
  `measurement,tag=value field`, then proved the delete on a survivor before
  restart so a successful no-op can no longer satisfy it.
- [x] Run both bounded storage gates on the resulting candidate. On 2026-08-03,
  `restart_catchup_gate.sh` removed node 3's complete durable root, compacted the
  same non-empty VShard 1738 snapshot on both survivors, acknowledged 104
  writes, applied one post-snapshot exact delete, installed the snapshot into
  the empty node, caught up its retained suffix, and read exactly 103 points.
  A final rerun through the single v1 batch verb sent 18 chunks, abandoned only
  two bounded attempts, returned no undeliverable snapshot, no 500, and crashed
  no process; the preceding resource sample was about 233 MiB on the recovering
  node and 120--125 MiB per donor. `snapshot_durability_gate.sh` then forced a
  non-empty snapshot on all replicas, killed all three processes, recovered
  three compacted journals, and read all 128 acknowledged points exactly once
  from every node. Both gates used one reactor and 1 GiB per process, stored all
  artifacts under `build/tmp`, and removed their data roots on exit.
- [x] Remove the shared mTLS peer-name override. Both inter-node transports now
  bind outbound certificate SAN verification to each peer's configured or
  Group-0-committed DNS/IP address, retire a cached connection when that identity
  changes, and refuse TLS activation if any registered peer lacks a name. A
  bounded three-node live gate converged with three distinct IP identities,
  returned retryable `503` when node 2 presented node 3's otherwise trusted
  certificate, then committed that exact write after node 2 restarted with a
  separately issued certificate for its own SAN. The negative arm held the
  other voter down so node 2 was required for quorum; node 1 stayed online,
  certificate renewal restored the write without a cluster restart, and the
  third voter then returned to full health. The obsolete `tls_peer_name` config
  and environment paths are removed rather than retained as aliases.

## Remaining production blockers

These are independent of the retired protocol versions and still block a
production deploy.

### P0 — correctness and topology

No open P0 item remains from this review. Production remains blocked on the P1
evidence below.

### P1 — bounded operation and live evidence

- [x] **Rerun empty-node catch-up and snapshot durability gates on the bounded
  candidate.** The exact assertions and resource observations are recorded in
  the completed evidence above. The final release-commit battery remains a P2
  release step, not an unresolved storage implementation blocker.
- [x] **Run the homogeneous-v1 rejection gate.** The 2026-08-03 bounded run
  passed six current-format codec checks and a real-socket data-plane test in
  both directions: an unsupported handshake applied no state, and a production
  client sent no application verb after an unsupported reply. The production
  restart arm acknowledged a write, changed only its WAL version field from 1
  to 2, and observed a non-zero exit before `/health`. Startup named version 2
  as unsupported and preserved the rejected WAL byte-for-byte. One 1-GiB,
  one-reactor process ran at a time and all artifacts were under `build/tmp`.
  There is no fallback, negotiation above v1, or activation path.
- [ ] **Run multi-host topology, security, and fault gates.** The bounded local
  mTLS identity gate now covers positive, trusted-wrong-endpoint, and recovery
  paths. Repeat identity and authenticated operator mutation on distinct hosts,
  then verify network partitions, restart, disk-full behavior, and bounded
  recovery there.
- [ ] **Measure production SLOs.** Record one-node-failure write error band,
  recovery time, query latency, snapshot catch-up, and movement impact using the
  final binary and deployment settings.
- [ ] **Deliver and prove clustered backup/restore.** The existing filesystem
  copy guidance is now explicitly limited to standalone mode: it omitted
  persistent node identity, Group-0/control state, the committed serving map,
  and per-VShard Raft journals, and independent live replica copies are not one
  recovery point. The exact-v1 artifact foundation now encodes portable control
  state, streams and durably stages canonical `TSP1` units, publishes a `TSBK`
  manifest only after all 4,096 units validate, and rejects corrupt, missing,
  extra, aliased, or symlinked entries. Its bounded in-process gate covers an
  interrupted stage/manifest write and restores one real Engine payload with
  exact readback. A local leader capture now obtains a deadline-bounded quorum
  ReadIndex, waits/rolls to a TSP1 boundary at or beyond it, and pins the
  sidecar across supersession and archival. This does not close the blocker:
  the server still needs durable cross-node orchestration over all current
  leaders and generation-one import into a fresh cluster UUID/new Raft
  membership, followed by a live RF=3 recovery gate.

### P2 — release and operations

- [x] Quarantine the obsolete static cluster Compose demo. Its services now live
  behind the explicit `unsafe-static-demo` profile, configuration expansion
  requires `TIMESTAR_ACK_UNSAFE_STATIC_CLUSTER`, container labels state that it
  is unsupported for production, and the file names the partitioned Group-0,
  mTLS, and authenticated-operator requirements it does not demonstrate.
- [ ] Reconcile architecture, API, runbook, and deployment documentation after
  the remaining behavior is complete. The backup document now fails closed for
  clustered use instead of presenting the standalone copy procedure as safe.
- [ ] Record one final serial build, focused unit/socket tests, and each required
  live gate against the exact release commit.

## Release exit criteria

Production approval requires all of the following:

1. Every P0 and P1 item above is closed with code and same-candidate evidence.
2. Writes, reads, exact deletes, pattern deletes, snapshot catch-up, restart, and
   movement remain correct through one voter loss and a network partition.
3. Recovery rejects corrupt, foreign, non-v1, or ambiguous durable state rather
   than serving a partial dataset.
4. Resource limits are explicit for memory, file descriptors, proposal bytes,
   snapshot bytes, journal retention, and background conversion.
5. Both inter-node transports require authenticated peer identity, and every
   mutating operator route is authorized.
6. Documentation describes the shipped behavior and the homogeneous-v1
   greenfield reset without claiming rolling compatibility.

## Verification policy

Builds may compile concurrently, but compiler temporaries must stay under the
disk-backed `build/tmp` rather than the quota-limited `/tmp`. Run memory-heavy
Seastar test processes one at a time with explicit `--smp` and `--memory`
limits. Live multi-process gates use an explicit per-process memory cap, keep
low-volume correctness roots under `build/tmp`, and are not run concurrently on
one host.

The live-directory gate covers stale update rejection, idempotent replay,
same-epoch conflict, incomplete-map rejection, and independent routing views on
two reactor shards.

The dynamic-admission gate covers unresolved-address retry after durable token
consumption, registration-before-learner ordering, peer address replacement on
both transports, malformed port rejection, and idempotent failed-start cleanup.

The movement-control gate covers exact-v1 destination and one-step actuation
socket transport, stale term/job rejection, inactive or non-member actuators
(except the draining victim authorized to finish its own evacuation),
skipped-step and changed-plan rejection, adjacent stable/joint recovery shapes,
refusal before registry insertion when an on-disk configuration conflicts with
the committed Group-0 job, and one durable transition per live-Raft driver call.
The lifecycle tests additionally cover exact forward-only state changes,
idempotent drain/remove retries, and refusal to remove an active or still
referenced node. Focused host tests cover the applied-membership fence, terminal
floor, exact-v1 quarantine, both restart recovery windows, grace-period no-op,
durable generation deletion, and protection of a materialized pre-cutover
destination. They also cover durable empty Engine-generation installation and
removal of a retired VShard from a day-discovery bitmap shared with a live
VShard. The production server gate exercises those routes over HTTP and proves
Engine-data reclaim, journal teardown/reclaim, exact failpoint exit status and
all acknowledged-point readback. It also proves deterministic whole-node
evacuation, application-fenced Group-0 eviction, and successful final node
removal.

The retention unit/socket suites cover exact-v1 command and snapshot framing,
bounds/corruption rejection, policy CAS and active-sweep fencing, the full
4,096-VShard cursor, Group-0 snapshot/crash resume, contiguous data-group sweep
IDs, constant-space fence snapshot/restore, catalog pagination, and measurement
and VShard isolation. The production retention gate adds current-leader HTTP
CAS, controller kill after partial progress, durable all-VShard resume, data
readback, old-controller restart, and tombstone-version recreation.

The delete-receipt suites cover capacity- and time-based retirement, stable
expired/conflict outcomes, snapshot state, recovery, and the write barrier
needed before destructive history can be compacted. The production gate adds
sustained RF=3 load, per-replica receipt/floor/snapshot observability, two
distinct floor advances, exact old/new retry outcomes, sealed journal reclaim,
and single-generation sidecar retention across repeated snapshots.
