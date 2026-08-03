# Deferred cluster tasks

This list contains work deliberately left after the static RF=3 integration.
Production blockers take priority and are authoritative in
[cluster-production-readiness.md](cluster-production-readiness.md).

## Production blockers

- [x] Complete group-0-driven drain, replace, remove, and Engine-data reclaim.
  Ordered local Raft teardown, its terminal reclaim floor, and exact-v1 journal
  quarantine/grace/deletion are wired. Join, destination data-group
  materialization, and the bounded production scheduler/remote leader actuator
  are wired and covered by the bounded production topology gate.
- [x] Replicate exact-version TTL policies through Group 0 and serialize
  constant-space cutoff decisions through all data groups. Cluster mutation is
  enabled after unit, snapshot/replay, controller-failover, restart, and
  tombstone-recreation gates; clustered downsampling remains intentionally
  unsupported in v1.
- [x] Replace monolithic VShard snapshot construction/install buffers with
  exact-v1 file-backed streaming beyond 128 MiB, bounded metadata, paced 4-MiB
  Raft chunks, and crash-safe sidecar recovery/cleanup.
- [x] Prove frozen pattern-delete retry through group-0 leader failover and
  restart in a multi-process gate. The production-server gate makes the first
  response ambiguous only after another voter applies the complete plan, then
  proves exact target reuse, changed-body conflict, and restart recovery.
- [x] Prove bounded delete-receipt retirement, snapshot progress, journal
  reclaim, and one-current-sidecar retention under sustained delete-heavy RF=3
  load.
- [x] Prove homogeneous-v1 fail-closed startup and peer behavior. Unsupported
  data-plane handshakes are fenced before application verbs, and a production
  restart over an unsupported WAL version exits before serving without
  rewriting the source.
- [x] Prove empty-node snapshot catch-up plus retained suffix and full-cluster
  restart over compacted journals with explicit 1-GiB process budgets and
  repository-local temporary storage.
- [ ] Run final multi-host topology, security, durability, and SLO gates against
  the exact release candidate.
- [ ] Deliver a cluster backup/restore workflow over pinned, hash-verified
  VShard snapshot streams. Restore must create a fresh cluster UUID, scrub old
  node/Raft membership, reject partial or corrupt artifacts, resume interrupted
  import safely, and pass exact post-restore readback. The exact-v1 `TSBK` /
  4,096-`TSP1` artifact codec, bounded staging, manifest-last publication,
  portable control-state capture, full-set validation, and in-process readback
  gate are complete. Live leader-pinned capture, fresh-membership import,
  resumable import state, and the RF=3 operator gate remain.

## Feature completion

- [ ] Cluster-aware `/subscribe` with one backfill/live barrier, resumable
  cursor, and no silent loss or duplicate through leader movement.
- [ ] Routing summaries that may prune fan-out only when complete and bound to
  the pinned map epoch.
- [ ] Hierarchical query merge for saturated coordinators with deterministic
  merge order.
- [ ] Public session and bounded-staleness read tokens after end-to-end API and
  partition tests.
- [x] Certificate rotation without a cluster restart. Credentials reload on a
  one-node restart; the bounded mTLS gate replaces one node's key/certificate
  with a separately issued certificate for the same endpoint SAN. With the
  other voter deliberately down, the remaining node stays online and commits
  the exact previously blocked write as soon as the rotated peer returns; the
  third voter then returns to full health. Runtime hot reload remains optional
  future work.

## Operational hardening

- [x] Quarantine the static full-replication Compose example behind an explicit
  unsafe profile, acknowledgement variable, and unsupported-deployment labels.
- [ ] Exercise disk-full, directory-sync failure, corrupt artifact, network
  partition, and restart at every remaining durable boundary.
- [ ] Record supported memory, file-descriptor, snapshot, proposal, journal,
  and background-work budgets.
- [ ] Publish production runbooks for bootstrap, join, drain, replace, backup,
  restore, and disaster recovery.

## Deferred beyond the first release

- hot-series lane splitting;
- automatic VShard-count migration;
- surgical Merkle repair;
- multi-VShard transactions;
- automatic tiering;
- any v2 wire or persisted format.

Version markers remain mandatory, but the greenfield implementation is v1-only
and updates v1 in place. See [protocol-versioning.md](protocol-versioning.md).
