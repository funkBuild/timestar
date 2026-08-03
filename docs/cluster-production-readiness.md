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
  removed all four `build/tmp` roots afterward. A later exact-candidate rerun
  received a valid retryable `503` while one VShard was still uncommitted after
  failover, exposing that the gate itself made only one retry. The gate now
  repeats the byte-identical request only after transport errors or `503`, with
  a ten-attempt, 30-second-per-attempt ceiling; it still fails immediately on a
  conflict or any other HTTP outcome.
- [x] Make the local network-reset qualification bounded and diagnostic on the
  required private durable journals. The insert driver no longer retains every
  array payload for a long campaign: it pre-generates only below a conservative
  256-MiB payload budget and otherwise builds after acquiring one of the bounded
  connection slots. This removed the measured approximately 3-GiB reservation
  and `std::bad_alloc` from the former 1,000-by-10,000-by-10 shape under the
  driver's 1-GiB limit. Overflow-safe policy tests pin the boundary. The gate
  now rejects a nonzero or incomplete benchmark result, retains the complete
  failed transcript outside disposable node roots, and hard-kills a benchmark
  that cannot finish its graceful shutdown after the configured deadline.
  Reset intensity is capped at 70 rounds per storm as well as floored at 35: an
  uncapped exact-candidate arm that transiently ran slower injected 154 rounds,
  then amplified its own latency and error count even though Raft admission
  remained empty with zero refusals.
  Its old durable-disk control was invalid: 920/1,000 fault-free requests were
  shed as overloaded. A later 1,500-by-500 profile passed one storm but turned
  successive storms into a storage-overload feedback loop, with errors
  `[1,16,580]` and throughput retention `[83%,48%,13%]`. The replacement keeps
  1,000 requests at 300 timestamps by ten fields. The bounded capped local run
  passed with error vector `[0,1,2]`, exactly 210 reset rounds, 839 destroyed live
  peer connections, 51% worst-arm throughput retention, exact 200-point probe
  readback on every replica after every storm, and zero uncommitted-Raft
  refusals. A later exact run produced `[16,20,0] = 36` retryable `503`s while
  retaining every attempted probe and 46% throughput, proving the old six-error
  ceiling was calibrated to the retired tmpfs profile. The durable ceiling is
  now 60 (at most 2% of 3,000 timed requests), and a complete driver status
  histogram must prove every allowed HTTP failure is `503`. Because give-up can
  move leadership away from the reset proxy, every storm now re-establishes and
  reasserts at least 800 proxied leaders; a corrected diagnostic draw held 1,365
  before each arm and passed `[0,0,0]` with 86% worst retention. System `/tmp`
  remained at 105 MiB; gate data lived and cleaned under `build/tmp`.
- [x] Replace the noisy retry-pacing A/B with a deterministic counterfactual.
  The former sequential live comparison produced a reversed draw: the flat
  20-ms pacing arm had zero errors while HEAD had six at matched reset
  intensity. The replacement still builds the exact two-anchor revert in an
  isolated disk-backed worktree, but runs the arithmetic reconnect-span and
  wall-clock blip behavioral tests against both binaries. The reverted arm must
  fail both and HEAD must pass both; the live gate above remains the end-to-end
  availability and durability proof.
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
- [x] Close the local immutable-TSM integrity blocker in exact v1. Writers now
  record CRC32 for every compressed data block and complete series-index entry,
  a CRC32 for the complete sorted index, and a CRC32-authenticated footer that
  carries the index checksum, maximum revision, and index boundary. Open
  authenticates and structurally validates the complete index before the file
  can register: series IDs must be unique and strictly sorted, block time and
  revision ranges must be valid and ordered, every block must remain in the data
  region, and the footer revision must equal the block maximum. Lazy entry
  rereads authenticate again. Ordinary, batched, pushdown paths that access
  bytes, and zero-copy compaction reads authenticate each block. Focused tests
  cover empty framing, one-shot writer finalization, footer/index corruption,
  mutation between open and lazy load, all block read modes, and
  checksum-consistent out-of-range metadata.
  A separate checksum-consistent fixture rejects a footer revision fence that
  disagrees with the authenticated block maximum.
- [x] Bound NativeIndex local-ID restore memory without narrowing the v1 ID
  space. A persisted high-water counter no longer resizes one contiguous
  reverse vector or reserves capacity proportional to unverified holes.
  Reverse mappings use lazy 4,096-entry chunks, forward-map preallocation is
  capped independently, and the next runtime assignment is installed at its
  exact restored ID rather than appended at the wrong reverse index. Restore
  rejects reserved-zero, conflicting, exhausted, or implausibly distant
  entries. A 50,000,000 counter with sparse mappings and a subsequent new
  assignment now round-trips under the one-reactor, 1-GiB unit-test budget.
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

No open P0 implementation item remains from this review, including local TSM
integrity. Production remains blocked on the P1 evidence below.

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
  paths. The bounded local data-plane reset gate now proves repeated real TCP
  RST recovery, retry classification, exact acknowledged-write readback, and
  throughput retention without saturating the private journal layout. The
  read-only multi-host preflight now binds the deployed nodes to the
  authenticated local SLO candidate and rejects overlapping resolved addresses,
  duplicate failure domains, plaintext/redirected endpoints, mixed
  components/revisions/cluster UUIDs, divergent or overlapping peer maps,
  disabled snapshotting, the optional shared journal, unstable map or lifecycle
  state, incomplete RF=3 totals, or any apply/durability fault. It also binds the
  server's explicit reactor count and aggregate uncommitted-proposal budget to
  the high-volume SLO profile; this avoids rejecting a correct four-reactor
  deployment as though its process-wide 256-MiB budget were a one-reactor
  64-MiB budget. Repeat identity and authenticated operator mutation on distinct
  hosts, then verify network partitions, restart, disk-full behavior, and
  bounded recovery there. The local disk-failure behavior is no longer an
  implementation ambiguity: an append, snapshot-promotion, or sync failure permanently
  quarantines that Raft replica before the failed `Ready` can send, apply, or
  acknowledge. Pending write, membership, read-index, and apply waiters fail
  promptly with a typed retryable error; the replica stops protocol traffic so a
  healthy quorum can elect a replacement. A shared shard-journal fence is visible
  to heartbeat-only groups without requiring another append. Focused ENOSPC tests
  prove no outbound message escapes, subsequent proposals do not mutate the core,
  and the other two voters elect and commit. `/cluster/status` exposes
  `raft_durability_failures` and `control_durability_failed`, and data/control
  readiness fail closed. The remaining item is live distinct-host bidirectional
  partition, disk-fault, and recovery evidence against the release binary, not
  this core behavior.
- [ ] **Measure production SLOs.** Record one-node-failure write error band,
  recovery time, query latency, snapshot catch-up, and movement impact using the
  final binary and deployment settings. The local harness precondition is now
  complete: high-volume gates use a recorded four-reactor/2-GiB server profile,
  focused one-reactor gates retain their 1-GiB pin, and the benchmark driver is
  capped at one reactor/1 GiB. Four reactors at 1 GiB was rejected because it
  begins at the production 256-MiB-per-shard free-memory floor and therefore
  sheds every write. Every gate exports `GATE_TMP_ROOT`/`TMPDIR` to
  repository-local `build/tmp`, checks free space on that actual filesystem for
  high-volume campaigns, and the deterministic pacing-counterfactual build is
  capped at two jobs. This removes the former 24--40 GiB aggregate reservation and
  prevents multi-gigabyte datasets from consuming a `/tmp` tmpfs as raw memory.
  A serial exact-v1 SLO collector now reuses the
  discriminating node-kill, bounded snapshot-catch-up, and skewed-movement gates
  and binds their metrics, thresholds, commit, authenticated server and benchmark
  hashes, resource settings, and raw transcripts into one report. It refuses an
  `unknown`, dirty, stale, or unresolvable embedded revision on either executable
  rather than binding a clean worktree commit to tools built from different
  source, and detects either binary changing during the serial run. Its high-volume arms use
  the recorded 2-GiB/four-reactor profile without overriding the focused
  catch-up arm's 1-GiB/one-reactor bound. Diagnostic node-kill runs also found
  that
  every VShard on one node inherited the same node-id-only election RNG stream,
  synchronizing thousands of campaigns after a failure; production data groups
  now use stable per-node/per-VShard seeds, with all 4,096 streams and the full
  timeout distribution pinned by a regression. Earlier collector attempts also
  exposed and closed two harness defects: a fixed join delay could start the
  movement load before node 3 opened HTTP, and a failed benchmark lost its exit
  status and transcript. Both movement gates now require exact 4,096-group
  convergence plus public health, reject missing or duplicate leader totals,
  and retain failure evidence.

  The complete serial collector passed on 2026-08-03 against clean candidate
  `305a030e394a669601daabe57a58c7d88a1cb487`. Its report authenticated both the
  server (`c5b1a77050af390e246edefd090e0fb4de9699f83cf30e517f0255545edc09f0`)
  and benchmark
  (`086db47d7b9976782dadb725e7b147354b818f91758312c75e0be419ece07319`)
  before and after the campaign. Killing a voter produced 338/400 successful
  batches and 62 retryable failures, preserved all 37 acknowledged outage
  probes on both survivors, recovered leaders for all 4,096 groups in 17.03 s,
  and measured a 27-ms survivor query p99 with zero server 500s or crashes. The proven-empty
  voter installed a non-empty snapshot in 62.36 s and returned the exact
  103-point post-delete result after 66.74 s. The skewed movement control was
  500/500 clean at 71,598 points/s; the storm initiated 3,206 transfers,
  retained 23% throughput with a 1.823-s p99 and 39 bounded retryable failures,
  converged to an even leader distribution, and made every acknowledged hot
  probe readable from all three nodes with zero server 500s or crashes. The
  report and raw transcripts are retained under
  `build/tmp/tsgate_slo_report`; gate data cleaned back to 8.9 MiB and `/tmp`
  remained at 105 MiB. These are bounded local regression results using the
  collector's provisional thresholds, not approved production SLOs. Approved
  thresholds and distinct-host deployment evidence remain open release work.

  The final local collector rerun after the benchmark-memory and network-gate
  fixes passed on exact runtime candidate
  `b9dd41749fd1eb49e891354b4cb9147ec4c9fa43`. A clean full build, 4,398 passed
  unit tests plus nine intentional skips and one disabled test, and 28 passed
  socket tests plus two one-shard skips preceded it. The report authenticated
  server
  `ed3d67721a4dc8b9aeac1f1bfe3e6d3083e89bc02fee1a94f5372a7a6a0db101`
  and benchmark
  `1f79dc373c1aa6f871d1c87f3f79c5da83269092013f2a72073f430020663216`
  before and after the campaign. It measured 57/400 retryable node-kill
  errors, 16.420-s recovery, and 29-ms query p99; 64.380-s snapshot
  installation and 68.800-s exact catch-up; and a movement arm that retained
  58% throughput with 1.757-s p99, 12 benchmark errors, 2,851 transfers, and
  136/150 successful live probes. Every acknowledged probe was durable and no
  arm observed a server 500 or crash. The later `1e76f1d` commit changes only
  gates and documentation; its bounded deterministic retry-pacing
  counterfactual failed both required invariants while HEAD passed both. Local
  implementation qualification is therefore complete. Approved thresholds and
  distinct-host deployment evidence remain open release work.
- [x] **Deliver and prove clustered backup/restore.** The existing filesystem
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
  sidecar across supersession and archival. Exact-v1 peer RPCs now route capture
  requests to the owning reactor and stream that pinned sidecar in bounded,
  hash-identified 1-MiB chunks under bounded expiring sessions. The Group-0
  leader now persists one checksummed exact-v1 export checkpoint beside the
  archive, binds it to the operation/source/complete serving map/portable
  control fence, sequentially retries every current VShard leader, and resumes
  immutable staged units after interruption. Authenticated start/status/cancel
  routes retain resumable progress, and a second quorum fence precedes
  manifest-last publication. The offline
  server path now seeds a
  fresh node's selected data groups at term 1 under new voters, reconstructs a
  scrubbed one-seed Group-0 snapshot under a new cluster UUID, persists the new
  serving-map cache before completion, and resumes crashes from a checksummed
  marker while Engine and networking remain closed. Preparation now exits, and
  ordinary startup remains fenced until the offline finalizer has validated a
  consistent marker from every data voter plus the Group-0 seed and activation
  has persisted that exact release locally. The bounded RF=3 production-server
  gate now covers coordinator loss/resume, concurrent writes, all 4,096 units,
  corrupt/missing/extra rejection, interrupted import and activation, the
  all-voter release fence, fresh identities, old-authority rejection, exact
  three-node readback, and full-cluster restart. The exact-v1 manifest now
  requires HMAC-SHA-256 over all portable control and unit metadata; export and
  restore fail closed without the protected configured key, and the gate adds
  wrong-key rejection. The backup runbook now defines encrypted staging and
  off-site storage, capacity reserve, retention/key-version coupling,
  manifest-last replication, and the complete isolated RF=3 recovery drill.

### P2 — release and operations

- [x] Quarantine the obsolete static cluster Compose demo. Its services now live
  behind the explicit `unsafe-static-demo` profile, configuration expansion
  requires `TIMESTAR_ACK_UNSAFE_STATIC_CLUSTER`, container labels state that it
  is unsupported for production, and the file names the partitioned Group-0,
  mTLS, and authenticated-operator requirements it does not demonstrate.
- [x] Reconcile architecture, API, runbook, and deployment documentation. The
  exact-v1 operations runbook now covers bootstrap, failure domains, enforced
  resource budgets, join/move/drain/remove, monitoring, distinct-host fault
  qualification, and recovery; the backup document fails closed for clustered
  filesystem-copy use. ADR 0001 now describes the shipped private-journal,
  snapshot, barrier, GC, and file-descriptor contract instead of the retired
  per-reactor/4-ms/256-MiB proposal.
- [ ] Record one final serial build, focused unit/socket tests, and each required
  live gate against the exact release commit. Exact runtime candidate `b9dd417`
  completed the full bounded local serial battery: build, unit/socket tests,
  6,000-target pattern-delete failover/restart, three reset storms, combined
  reset plus rebalance, node-kill durability, empty-root snapshot catch-up, and
  skewed movement. Its reset-only run retained 87--89% throughput with 202
  bounded reset rounds, 804 destroyed connections, zero errors, and exact
  replica readback. The combined run admitted 47 typed `503`s under its
  80-error ceiling, retained 36--45% throughput, completed 216 rebalance calls,
  and preserved every acknowledged write with no other status, server 500, or
  crash. The later `1e76f1d` gate/documentation-only commit replaced the noisy
  live pacing A/B with a passing deterministic counterfactual. The remaining
  step is procedural: choose the release identity, build that exact identity,
  repeat the serial battery once, then run the distinct-host arms. It is not an
  unresolved storage or runtime implementation defect.

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
limits. High-volume multi-process gates use 2 GiB per four-reactor server;
focused gates pin one reactor and 1 GiB. All roots and implicit temporaries live
under `GATE_TMP_ROOT` (`build/tmp` by default), and live gates are not run
concurrently on one host. High-volume gates preflight the free space of that
configured filesystem; overrides require an explicitly provisioned disk and
aggregate memory budget.

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

The backup-export coordinator build covers the production server and unit-test
targets with compiler temporaries under `build/tmp`. Focused one-reactor,
1-GiB tests cover portable-control/checkpoint exact-v1 round trips and corruption
rejection, HMAC round trip and structurally valid tamper rejection, protected
key-file loading, complete-file SHA-256 inspection, durable checkpoint conflicts
and retained progress, and explicit refusal of every backup lifecycle route
when authentication is disabled. The bounded production gate runs at most three
one-reactor, 1-GiB processes, observed aggregate RSS below 400 MiB, retained
every artifact under `build/tmp`, and left `/tmp` at 105 MiB. Its 2026-08-03 run
passed live leader-loss resume, 4,096-unit manifest-last publication, wrong-key
and malformed-artifact rejection, killed-import resume, all-voter release,
killed activation, old-authority rejection, exact three-node readback, and a
full restored-cluster restart.

The delete-receipt suites cover capacity- and time-based retirement, stable
expired/conflict outcomes, snapshot state, recovery, and the write barrier
needed before destructive history can be compacted. The production gate adds
sustained RF=3 load, per-replica receipt/floor/snapshot observability, two
distinct floor advances, exact old/new retry outcomes, sealed journal reclaim,
and single-generation sidecar retention across repeated snapshots.
