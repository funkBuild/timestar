# ADR 0005 — A leader-transfer bypass for the CheckQuorum disruption guard

**Status:** ACCEPTED and IMPLEMENTED (`82591a9` bypass, `5bdaa03` enable, `277e110` the
interaction this ADR did not predict). CheckQuorum is ON. Mechanism (b) shipped;
mechanism (c) — cluster-wide gated activation — did NOT, and is carried as debt D-30.

**What the gates added to this design.** The bypass was necessary and sufficient for
TRANSFERS (2216 of them under sustained writes, zero client errors, leadership settling
at 0 moved), but enabling CheckQuorum then broke something this ADR never considered:
**hibernation stretches the lease**. An idle follower ticks 1-in-10, so its disruption
guard lasts 25-50 s rather than 2.5-5 s, and a group whose leader has DIED cannot be
voted into a new one for that whole window. Same-session A/B on `node_kill_round.sh`:
49/400 failed batches and an 8 s recovery with CheckQuorum off, **153/400 and 43 s** with
it on. Fixed by waking a group that DROPS a vote under the lease (`277e110`), which
brings the band back to 62-69/400 and a 12 s recovery. The residual — one extra campaign
cycle, because the wake fires on the first dropped vote — is debt D-29.

The lesson for the next reader of this ADR: the lease is not only about what the wire
carries. Any mechanism that slows a group's TICK also slows its lease, and this codebase
has one.

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md),
[Cluster Write Scale-Out Plan](../write-scaleout-plan.md) §4 Phase 5d

**Supersedes:** the revert in `1f2e752` ("revert checkQuorum -- it breaks leadership
transfer") and the DO-NOT-ENABLE comment at `cluster_data_plane.cpp:150`

## Context

CheckQuorum is off. It was enabled during Phase 3 as a belt-and-braces companion to the
per-write propose deadline and had to be reverted, because with it on **leadership
transfer breaks**. This ADR is the design for turning it back on.

### The mechanism, precisely

Two things are called "CheckQuorum" and only one of them is the problem.

1. **Leader step-down** (`RaftNode::checkQuorumOrStepDown`, driven from `tick()`): a
   leader that has not heard from a majority within an election timeout steps down.
   This half is unambiguously good and is not what breaks.
2. **The disruption guard** (`raft_node.cpp:589`): a voter that still hears a valid
   leader refuses to grant a vote and — importantly — **does not even bump its term**:

   ```cpp
   const bool inLease = opts_.checkQuorum && leaderId_ != kNoNode && electionElapsed_ < electionTimeout_;
   if (inLease) return;   // ignore; do not bump term
   ```

`transferLeadership(target)` catches the target up and sends it a `TimeoutNow`. The
transferee's TimeoutNow arm campaigns immediately, skipping its OWN lease and PreVote.
But the vote request it then broadcasts is an **ordinary `RequestVote`**
(`raft_messages.hpp` has no transfer/force marker), and every other voter is still
hearing the old leader's heartbeats — so every one of them takes the `inLease` branch
and drops the vote silently.

The transferee therefore cannot win at term+1. It wins eventually only by term
escalation: it re-campaigns on its own election timeout until its term is far enough
ahead that... no — it never escapes the guard by term alone, because the guard ignores
term entirely. What actually resolves it is the OLD LEADER stepping down (it stops
heartbeating once transfer is in progress), after which the other voters' `electionElapsed_`
exceeds their election timeout, `inLease` goes false, and the next campaign succeeds.

**Measured:** a transfer that completes in 0 tick rounds with CheckQuorum off needs a
full election timeout with it on — 2.5-5 s at the production 20 ms tick
(`electionTimeoutMin/Max = 125/250`), each with a leaderless window inside it.

### Why that is a live availability problem here, not a corner case

- the background leadership balancer fires every ~5 s over 4096 groups;
- the operator endpoint `/cluster/rebalance-leadership` storms transfers deliberately,
  and the Phase-3/4 gates measured 2216 and 16613 transfers in a single run;
- `queryReplicated` fails reads CLOSED after ~125 ms of leaderlessness.

So a 2.5-5 s leaderless window per transfer, multiplied by thousands of transfers, is a
read outage. The Phase-3 gate results recorded as "balancer churn" were later withdrawn
precisely because they were measured with CheckQuorum temporarily on, and what they
actually showed was transfers failing and being retried.

### What is lost while it stays off

Very little, and this is why the revert was correct rather than merely expedient:

- **Safety: nothing.** Raft's safety does not depend on CheckQuorum. A partitioned
  leader cannot commit anything, because commit requires a quorum ack.
- **Availability: nothing.** Every write is bounded by the propose deadline in
  `RaftGroup::proposeAndAwaitApplied`, so a partitioned leader's proposals fail in
  bounded time rather than hanging, and expired-waiter accumulation is bounded by
  (deadline x retry budget) either way.
- **What IS lost:** a partitioned leader keeps *accepting* proposals it can never
  commit until each one's deadline expires, and it keeps believing it is the leader,
  so leader-only reads on the partitioned side are served from a state that may be
  stale. `ReplicaVShard`'s leader-ReadIndex path is protected (a ReadIndex needs a
  quorum heartbeat round, which a partitioned leader cannot get), but the step-down is
  what makes the partitioned side converge on the truth *promptly* instead of at each
  request's own deadline.

That is worth having. It is not worth a multi-second leaderless window per transfer.

## Decision (proposed)

Adopt etcd's fix: a **`campaignTransfer` flag on `RequestVote`** that the `inLease`
check honours.

```cpp
struct RequestVote {
    bool preVote = false;
    bool campaignTransfer = false;   // NEW: "the current leader sent me a TimeoutNow"
    Term term = kNoTerm;
    NodeId candidateId = kNoNode;
    LogIndex lastLogIndex = kNoIndex;
    Term lastLogTerm = kNoTerm;
};
```

Set it when, and only when, the campaign was started by a `TimeoutNow` from the node
this replica currently believes is the leader. The guard becomes:

```cpp
const bool inLease = opts_.checkQuorum && leaderId_ != kNoNode
                     && electionElapsed_ < electionTimeout_
                     && !std::get<RequestVote>(m.payload).campaignTransfer;
```

**Why this is safe, and why it is not a hole.** The flag is not an assertion the voter
verifies; it is a hint. But it can only *bypass a lease*, never grant a vote: every
other vote condition still applies — the candidate's term must be at least the voter's,
its log must be at least as up to date (§5.4.1), and the voter must not have already
voted this term. So the worst a lying peer achieves is what it can already achieve
today with CheckQuorum OFF, which is the current production configuration. The flag
recovers exactly the lease, no more.

It is also *self-limiting*: only the current leader can cause a `campaignTransfer`
campaign, because only the leader sends `TimeoutNow`, and a `TimeoutNow` is accepted
only from the believed leader at a current term. A node cannot elect itself with the
flag; it can only be elected with it by the incumbent.

### The compatibility story — the hard part

**There is no version negotiation on the Raft transport.** `encodeEnvelope` writes a
message-type byte and then that type's fields; there is no version field anywhere in
the envelope, and there never was, because the transport had exactly one verb and one
format. Adding a byte to `RequestVote` is therefore a silent-misparse hazard, not a
compile error: an old decoder reading a new `RequestVote` mis-frames every field after
the new byte and produces a *valid-looking* vote request with garbage term and log
indices. Fail-closed is not automatic here; it has to be designed in.

Three mechanisms were considered.

**(a) A codec version byte in the envelope.** Correct in the long run and the right
place to end up, but it changes EVERY message, so an old node cannot read any new
message — the upgrade would have to be a full-stop restart, which is exactly what
rolling upgrades exist to avoid.

**(b) A new message type byte** (`kRequestVoteTransfer`). Old decoders reject an
unknown type and `decodeEnvelope` returns `nullopt`, so the envelope is DROPPED rather
than misparsed. That is fail-closed at the wire level, and it needs no negotiation at
all. Its weakness is behavioural, not structural: an old voter that drops the transfer
vote simply does not vote, so a transfer during a mixed-version window degrades to
exactly today's broken behaviour — which is only acceptable if CheckQuorum is off
during that window anyway.

**(c) Gate the whole feature on the cluster-wide committed format version.**
`features::FeatureGate::canActivate(v, voters)` already encodes the rule "a group does
not activate a format until EVERY current voter can read it", and activation is a
committed group-0 command. So CheckQuorum + `campaignTransfer` become one feature that
is *off* until group 0 has committed an active format version that implies it.

**Recommendation: (b) AND (c) together**, in that order of implementation.

(b) makes the wire safe on its own — a mixed-version cluster can never misparse, only
drop. (c) makes the BEHAVIOUR safe: since CheckQuorum stays off until the cluster-wide
gate activates, and the gate cannot activate until every voter advertises support, no
node ever runs with the guard on while a peer would drop its transfer votes. Neither
alone is sufficient: (b) without (c) leaves a window where transfers are slow again,
and (c) without (b) trusts a negotiated version to protect a format that has no version
field.

Note the sequencing constraint this creates: **CheckQuorum's activation must be a
committed cluster-wide decision, not a config flag.** A per-node config would let an
operator enable it on one node during a rolling upgrade and reproduce the exact failure
this ADR exists to prevent. It should live beside the codec activation that Phase 2's
2c left open (`activeFormatVersion`), and probably land with it.

## Consequences

### Tests required before enabling

1. **Deterministic core** (`raft_cluster_test.cpp`'s `Network`, no reactor):
   - a transfer with `checkQuorum = true` completes in ONE tick round, not an election
     timeout — the direct regression test for `1f2e752`;
   - a `campaignTransfer` vote from a node the voter does NOT believe is a candidate
     for transfer still obeys every other vote condition: stale term rejected, stale
     log rejected (§5.4.1), double-vote-in-term rejected. This is the "it is only a
     lease bypass" claim, stated as three tests;
   - a partitioned leader with `checkQuorum = true` steps down within an election
     timeout, and the majority side elects — the property being bought.
2. **Codec** (`raft_codec_test.cpp`): round-trip of the new type; and an OLD-format
   `RequestVote` still decoding; and a new-format `RequestVote` presented to the old
   decoder DROPPING rather than misparsing (this is mechanism (b)'s whole claim, and it
   must be asserted, not assumed).
3. **Live gates**, all four existing ones re-run with CheckQuorum on, plus specifically:
   - `rolling_rebalance_gate.sh` — this is the discriminating one. It storms thousands
     of transfers under sustained writes and asserts zero client errors AND that
     leadership settles afterwards. A slow-transfer regression shows up there as
     leadership still moving in bulk long after the storm stops, which is exactly how
     `wait_leadership_settled` is documented to fail;
   - `deposed_primary_gate.sh` at 5 nodes — the 274-285/300 results recorded before the
     revert were a CheckQuorum artifact (wide mid-transfer windows) and must come back
     to 300/300;
   - a mixed-version gate: a cluster of one current and two pre-flag binaries must
     refuse to activate the feature, and must keep transferring leadership at today's
     speed while it is mixed.

### What re-enabling buys, restated

- a partitioned leader stops accepting proposals it cannot commit within one election
  timeout instead of one deadline per write;
- leader-only reads on the losing side of a partition converge to "not leader" promptly
  rather than per-request;
- it removes the standing asymmetry between this implementation and etcd/raft, which
  matters mostly because every future reader of this code will ask why CheckQuorum is
  off and deserve an answer better than a comment.

None of that is a safety property. This is a *responsiveness under partition* change,
and it should be scheduled as one — after the wire-format prerequisites, not before.
