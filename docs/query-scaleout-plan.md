# Cluster query scale-out plan

**Status:** leader ReadIndex and bounded alternate-replica routing are integrated.
Public session/bounded-staleness modes remain disabled until their complete API
contract is wired and release-tested.

## Correctness contract

- A successful strict read includes every write acknowledged before its barrier.
- A VShard contributes exactly once, even when attempts are hedged or retried.
- Missing VShards fail the query unless the caller explicitly requested partial
  results; missing identities are returned, never silently omitted.
- A node with committed-but-unapplied entries or a partial snapshot install does
  not answer data, metadata, or discovery as complete.
- All inter-node requests use the one current v1 schema.

## Current path

```text
HTTP query
  -> parse canonical QueryRequest
  -> pin control-map epoch
  -> determine required VShards
  -> select one eligible replica per VShard
  -> bounded local/RPC attempt
  -> retry another assigned replica on failure
  -> merge each VShard contribution once
  -> finalize and serialize
```

`NodeQueryCoordinator` handles the production node fan-out.
`ReplicaEngineReader` serves one Engine-backed replica, and
`ReplicaEngineQueryCoordinator` provides internal replica hedging/retry where
that path is used. The superseded in-memory `ReplicaVShard` and toy replica
coordinator are deleted.

## Consistency modes

### Linearizable

Confirm a quorum ReadIndex at the current group leader, wait for the selected
replica to apply through that index, then query local Engine state. An isolated
former leader cannot complete the quorum round and fails closed.

### Session

Wait until the selected replica's applied index reaches the supplied
`ReadEnvelope` token, then serve locally. The envelope identifies VShard, term,
and applied index. Public API wiring must define token collection for
multi-VShard responses before this mode is exposed.

### Bounded staleness

Fetch the reachable leader's commit index and require
`leaderCommit - localApplied <= maxLagIndex`. This is an index-distance bound,
not a wall-clock guarantee. The leader must be reachable; otherwise the request
fails rather than serving an unbounded stale answer.

## Routing and retry

Replica selection filters unreachable candidates and, for bounded staleness,
candidates already known outside the lag bound. Preference order is locality,
lag, queue depth, then recent error rate. NaN error rates sort last.

One RPC attempt and its v1 handshake share the caller's wall-clock deadline. A
failed wave is fully awaited before the next wave; abandoned Seastar futures are
not treated as cancelled. Only the first successful contribution in preference
order is merged.

Redirects carry VShard, leader hint, and whether the answering node hosts the
group. Node-query request and partial reply always encode the resolve/redirect
counts, including zero. Truncated historical layouts are rejected.

## Apply and snapshot fence

The Raft applied index advances only after `EngineDataStateMachine::apply` or
complete snapshot installation resolves. The production apply fence therefore
blocks:

- data queries;
- metadata enumeration and cardinality;
- pattern-series discovery;
- any response that would otherwise claim completeness.

Snapshot replacement validates data and catalog before mutation and keeps the
fence closed across their publication interval.

## Completed gates

- [x] Quorum ReadIndex rejects a partitioned former leader.
- [x] RF&lt;N routing retries another assigned replica with one deadline.
- [x] Query merge counts each VShard once under retry/hedging.
- [x] Apply lag and snapshot installation fence data and metadata.
- [x] Unknown/malformed v1 query frames fail closed.
- [x] Unwired public consistency modes reject instead of silently degrading.

## Remaining work

- [ ] Define the public multi-VShard session-token response/request shape.
- [ ] Wire session and bounded-staleness parsing, validation, and response
  envelopes end to end before enabling them.
- [ ] Add multi-process partition, leader-loss, lagging-replica, and snapshot
  install tests for each exposed mode.
- [ ] Measure coordinator saturation, broad high-cardinality fan-out, concurrent
  narrow-query latency, and replica load distribution on multiple hosts.
- [ ] Implement cluster-aware streaming with a single backfill/live barrier and
  resumable cursor; node-local `/subscribe` remains disabled in cluster mode.

## V1 policy

The data-plane handshake and all nested query codecs support v1 only. There is
no mixed-version gate or compatibility decoder during greenfield development.
See [protocol-versioning.md](protocol-versioning.md).
