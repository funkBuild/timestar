# Cluster production-readiness review

**Status:** BLOCKED — clustered deployment is not approved for production.

**Reviewed baseline:** `cluster-design`, reviewed 2026-08-01 and updated
2026-08-02.

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
  now reject its retired filenames and non-canonical numeric aliases.
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

## Remaining production blockers

These are independent of the retired protocol versions and still block a
production deploy.

### P0 — correctness and topology

- [ ] **Complete topology mutation through group 0.** Join, drain, replace,
  remove, VShard movement, ordered teardown, and reclaim-floor publication must
  be exercised through the production server. Group 0 now validates durable
  movement plans/progress, exact one-VShard cutover, and live sharded directory
  publication, dynamic peer registration, and destination data-group creation,
  plus the bounded production scheduler/remote leader actuator and authenticated
  intent-only move/drain/remove routes. The server still does not perform the
  post-cutover local replica teardown, quarantine/grace period, durable-file
  reclamation, or reclaim-floor publication, and the complete workflow still
  needs a multi-process production-server gate. Editing a static peer list is
  not a safe topology operation.
- [ ] **Replicate retention policy and cutoff decisions.** Partitioned mode must
  never let replicas expire or compact different logical ranges. Until then,
  retention mutation must remain fail-closed.
- [ ] **Finish the large-snapshot production path.** Snapshot transfer is
  chunked, but snapshot construction/install still needs a bounded streaming
  path that cannot strand a VShard above the current in-memory payload ceiling.
- [ ] **Prove pattern-delete failover and restart externally.** A multi-process
  gate must cover group-0 leader loss after an ambiguous freeze, retry by the
  same idempotency identity, restart, and exact frozen-plan reuse.

### P1 — bounded operation and live evidence

- [ ] **Prove delete-receipt retirement under sustained load.** A long-running
  delete-heavy gate must demonstrate bounded receipt memory, progress of the
  replicated retirement floor, snapshot eligibility, and Raft journal reclaim.
- [ ] **Rerun empty-node catch-up and snapshot durability gates on the final
  candidate.** Each process must use an explicit memory budget; tests run one at
  a time and must prove the returning node's durable root was actually absent.
- [ ] **Run the homogeneous-v1 rejection gate.** A node or artifact with an
  unknown version must fail before serving; no test should expect fallback,
  negotiation above v1, or activation.
- [ ] **Run multi-host topology, security, and fault gates.** Verify mTLS peer
  identity, authenticated operator mutations, network partitions, restart,
  disk-full behavior, and bounded recovery on distinct hosts.
- [ ] **Measure production SLOs.** Record one-node-failure write error band,
  recovery time, query latency, snapshot catch-up, and movement impact using the
  final binary and deployment settings.

### P2 — release and operations

- [ ] Replace or clearly quarantine demo/static cluster deployment examples so
  they cannot be mistaken for a topology-managed production deployment.
- [ ] Reconcile architecture, API, runbook, and deployment documentation after
  the remaining behavior is complete.
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

Memory-sensitive verification is serial: build with `-j1`, run Seastar tests
with `--smp=1` and an explicit memory limit, and keep temporary files under
`build/tmp`. Live multi-process gates are not run concurrently on one host.

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
referenced node. The production server build pins the authenticated route
composition; the remaining multi-process topology gate must exercise those
routes over HTTP and prove post-cutover teardown/reclaim.
