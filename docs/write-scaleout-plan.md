# Cluster write scale-out plan

**Status:** the core shard-local replicated write path is implemented. Remaining
production blockers are tracked in
[cluster-production-readiness.md](cluster-production-readiness.md).

## Objective

Scale writes across reactor shards and VShard leaders without a coordinator
funnel, while preserving these contracts:

- a success means quorum commit and local apply;
- a failed or timed-out attempt never manufactures an acknowledgement;
- retrying a point write is safe under deterministic log-index revisions;
- retrying a delete is safe only through its durable operation receipt;
- admission, proposal, transport, and retry memory are bounded;
- every protocol surface uses the single current v1 schema.

## Current architecture

```text
HTTP batch
  -> parse and canonicalize once
  -> split by VShard
  -> group slices by current leader
  -> local proposal or one RPC per leader
  -> each VShard Raft group commits and applies
  -> retry only uncommitted slices
  -> acknowledge only when every requested slice committed
```

Routing is node-symmetric: any node may coordinate, local and remote leaders use
the same proposal contract, and a leader hint is advisory. A target that rejects
its own slice cannot redirect the retry to itself. Replies are interpreted only
for VShards actually sent to that peer.

The production state machine is `EngineDataStateMachine` over
`ReplicatedCommand`. The old double-only reference command/state-machine/router
stack has been deleted.

## V1 wire and log contract

- Data-plane negotiation advertises and accepts v1 only.
- `WriteBatch` begins with `TSW1` and uses delta-varint timestamps.
- Raft log commands begin with `TSC1`.
- Node-query request/reply schemas always carry their leader-resolution counts;
  there are no optional compatibility tails.
- Raft envelopes begin with `TSR1` and use one current tag layout.
- Unknown versions, tags, flags, truncation, checksum failures, and trailing
  bytes fail closed.

See [protocol-versioning.md](protocol-versioning.md). Development clusters must
be recreated after an incompatible v1 layout update.

## Retry and acknowledgement rules

The base batch deadline is 1.5 seconds and one attempt is bounded to 600 ms.
Election-shaped failures may use the separately bounded election window; other
failures remain on the base schedule.

`committedVShards` is the only signal that removes a slice from the retry set.
Reject records classify and optionally redirect remaining slices; they never
imply commit. A reply about an unrequested VShard is ignored.

Point-write retries produce a later log index and therefore a later revision.
The value is byte-identical, so replicas converge under last-write-wins. An
intervening write to the same series/timestamp may be overwritten by the retry;
the operations overlap because the first attempt was not acknowledged.

Delete retries use an operation ID, request hash, original issuance time, and a
snapshot-durable receipt. A retained duplicate is a state-machine no-op. A
receipt below the replicated retirement floor returns an expired outcome rather
than issuing a new physical delete.

## Resource bounds

The size chain is compile-time checked:

| Boundary | Limit |
|---|---:|
| Data-plane inbound memory estimate | 128 MiB per shard |
| Raft proposal payload | 14 MiB |
| Raft send including envelope headroom | 18 MiB |
| Raft inbound memory estimate | 64 MiB per shard |
| Snapshot chunk | 4 MiB |
| Snapshot object I/O buffer | 1 MiB, cooperatively yielded |
| Snapshot manifest / catalog metadata | 16 MiB / 64 MiB |
| Complete VShard snapshot | 1 TiB file-backed disk admission ceiling |

WriteBatch proposal charging computes the exact v1 encoded bytes without an
allocation. It no longer applies a ratio between retired layouts. A slice that
cannot fit one Raft proposal is rejected locally and terminally before an RPC.

Ingress admission charges decoded work before proposal. Committed apply bypasses
transient front-door backlog admission because committed state must make
progress. Proposal waiters have deadlines, and uncommitted log bytes have both
per-group and per-shard budgets.

## Raft transport

Single-envelope delivery is the default. Optional per-peer batching carries the
same v1 envelopes and changes only dispatch frequency. It can be enabled with
`TIMESTAR_RAFT_BATCH_SENDS=1` where syscall/frame rate is the measured bottleneck.

Snapshot transfer is file-backed, chunked, and paced. Install requests retain
the exact v1 wire layout and carry offset, total, and completion state; replies
carry the accepted offset and staged byte count. The sender hydrates only the
current chunk and the receiver stages it on disk; the complete final sidecar is
size/hash checked and fsynced before Ready publication. Stale terms,
already-applied boundaries, gaps, inconsistent totals, oversized descriptors,
and final size/hash mismatches fail before installation.

CheckQuorum is disabled in the production data plane. Lost-quorum writes still
fail within their bounded proposal deadline. The transfer-vote bypass remains
implemented and tested for direct Raft configurations that enable CheckQuorum;
see [ADR 0005](adr/0005-checkquorum-transfer-bypass.md).

## Completed implementation work

- [x] Removed the cross-core coordinator funnel.
- [x] Split once by VShard and dispatch one borrowed view per leader.
- [x] Encoded remote slices without merging/copying them first.
- [x] Added per-slice commit results, failure classes, and leader hints.
- [x] Bounded attempt, overall retry, receiver proposal, and apply-wait times.
- [x] Added pre-proposal storage admission and unconditional committed apply.
- [x] Added bounded uncommitted-log accounting and expired-waiter reclamation.
- [x] Added chunked snapshot catch-up and bounded inbound Raft memory.
- [x] Added shared-journal reclaim floors and fair snapshot scanning.
- [x] Streamed exact-v1 VShard snapshot construction, transfer, recovery, and
  Engine installation through owned disk sidecars with bounded metadata and
  cooperatively yielded object I/O.
- [x] Added current v1 markers and removed old protocol/layout branches.
- [x] Removed the superseded data model and duplicated tests.

## Remaining work

- [x] Stream complete VShard snapshots so the former 128 MiB full-payload cap
  cannot strand a hot VShard.
- [x] Prove sustained receipt retirement, snapshot progress, journal reclaim,
  and superseded-sidecar cleanup under delete-heavy RF=3 live load.
- [x] Complete production topology changes and ordered VShard teardown through
  group 0.
- [x] Replicate exact-version TTL policies and one globally serialized,
  restart-safe cutoff sequence through every VShard before enabling clustered
  retention mutation.
- [x] Prove ambiguous frozen pattern-delete retry through Group-0 leader loss,
  exact target reuse, changed-body conflict, and killed-node restart.
- [x] Prove exact-v1 fail-closed behavior at codec, real data-plane socket, and
  production restart boundaries, including byte-for-byte preservation of an
  acknowledged WAL carrying an unsupported version.
- [ ] Run final same-candidate multi-process gates with explicit per-process
  memory budgets: node kill, restart catch-up, empty-node snapshot, pattern
  delete failover/restart, and durability fault injection.
- [ ] Measure final multi-host throughput, latency, recovery, and movement SLOs.

## Acceptance gates

1. Kill one RF=3 voter during sustained writes. Every acknowledged point remains
   queryable and all request failures stay inside the documented retry contract.
2. Partition a leader from quorum. Proposals and ReadIndex operations fail in
   bounded time; memory and uncommitted bytes remain bounded.
3. Restart an empty/far-behind voter. It catches up by snapshot plus log replay
   without serving partial state.
4. Retry an ambiguous exact and pattern delete through leader loss and restart.
   The original target set is reused and intervening writes are not erased by a
   duplicate physical delete.
5. Present a non-v1 peer, Raft frame, WAL, snapshot, or immutable format. Startup
   or dispatch fails before state is applied or traffic is served.
6. Run every live gate against the exact release commit, one at a time, with
   explicit memory and file-descriptor budgets.

## Out of scope for the first production release

- hot-series lane splitting;
- multi-VShard transactions;
- automatic cross-version upgrade or downgrade;
- a v2 format before production compatibility requirements are defined.
