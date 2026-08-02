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
  fields.
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
- [x] Bound each `TSMJ1` movement job to its exact source voters and target map
  epoch, made Group 0 create the job and desired placement atomically, and made
  serving-map cutover require that exact job to reach `Done`.
- [x] Wired committed Group-0 serving-map cutover into the running node: the
  durable cache is persisted before the applied boundary advances, every
  reactor owns and updates its own routing directory, stale epochs cannot
  regress routing, exact replay repairs partial fan-out, and conflicting or
  incomplete maps fail closed.

## Remaining production blockers

These are independent of the retired protocol versions and still block a
production deploy.

### P0 — correctness and topology

- [ ] **Complete topology mutation through group 0.** Join, drain, replace,
  remove, VShard movement, ordered teardown, and reclaim-floor publication must
  be exercised through the production server. Group 0 now validates durable
  movement plans/progress, exact one-VShard cutover, and live sharded directory
  publication, but the production job driver, dynamic peer/group creation,
  operator API, and teardown/reclaim sequence remain unfinished. Editing a
  static peer list is not a safe topology operation.
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
