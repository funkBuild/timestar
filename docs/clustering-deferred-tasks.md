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
- [ ] Replace monolithic snapshot construction/install buffers with bounded
  streaming beyond 128 MiB.
- [ ] Prove frozen pattern-delete retry through group-0 leader failover and
  restart in a multi-process gate.
- [ ] Prove bounded delete-receipt retirement, snapshot progress, and journal
  reclaim under sustained delete-heavy load.
- [ ] Run final multi-host topology, security, durability, and SLO gates against
  the exact release candidate.

## Feature completion

- [ ] Cluster-aware `/subscribe` with one backfill/live barrier, resumable
  cursor, and no silent loss or duplicate through leader movement.
- [ ] Backup/restore CLI over pinned VShard snapshot streams, restoring into a
  fresh cluster UUID without resurrecting old membership.
- [ ] Routing summaries that may prune fan-out only when complete and bound to
  the pinned map epoch.
- [ ] Hierarchical query merge for saturated coordinators with deterministic
  merge order.
- [ ] Public session and bounded-staleness read tokens after end-to-end API and
  partition tests.
- [ ] Certificate rotation without a cluster restart.

## Operational hardening

- [ ] Replace or quarantine static/demo deployment examples.
- [ ] Exercise disk-full, directory-sync failure, corrupt artifact, network
  partition, empty-node catch-up, and restart at every durable boundary.
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
