# ADR 0006 — How long a read waits for a leader, and why it is not how long a write waits

**Status:** ACCEPTED, IMPLEMENTED. The constants live in
`lib/cluster/integration/cluster_data_plane.hpp` with three static_asserts; the read side
moved from 100 ms to 1.2 s in `b6bff0d` (debt D-26). The write side is unchanged from
debt D-14.

**Parent design:** [Cluster Architecture and Implementation Plan](../clustering.md),
[Cluster Write Scale-Out Plan](../write-scaleout-plan.md) (debt D-14, D-20, D-26)

**One-line decision:** a replicated READ rides out a leadership **transfer** and
deliberately does **not** ride out an **election**; a replicated WRITE rides out an
election. The two budgets differ by 5x, on purpose, and the direction is asserted by the
compiler.

## Context

A VShard has no leader for two quite different reasons, and they differ by two orders of
magnitude in duration:

| event | how long | how often | who causes it |
| --- | --- | --- | --- |
| leadership **transfer** | single-digit ms; **1 s** worst case (`kRaftTransferTicks`, the §3.10 abandon window, ADR-adjacent debt D-20) | continuously, on every balancer pass | us |
| **election** | 2.5–5 s (`kRaftElectionTicksMin/Max`) | when a node dies or is partitioned | a fault |

Neither a read nor a write can be served for a VShard in either state, so both paths
sleep-and-retry and then fail closed. What they chose to wait was, until D-26, set
independently and written down nowhere:

* **Write** (debt D-14): base deadline 1.5 s, extended to `kElectionDeadline` **6 s** with
  16 attempts when — and only when — every failure of an attempt is election-shaped. That
  change was measured: the one-node-down failed-batch band went from 396/400 to 41/400 and
  throughput from 10.8 k to 3.22 M pts/s.
* **Read**: `4 x 25 ms = 100 ms`, as two function-local `static constexpr`s inside
  `ClusterDataPlane::queryReplicated`, then `QUERY_INCOMPLETE`.

That is a **60x** difference that nothing states, defends or protects. Its visible
consequence is that a one-node outage shows up as read errors long before it shows up as
write errors — a support-facing asymmetry an operator meets before they meet this file.

**The read number was also wrong on its own terms**, which is what turned "record the
decision" into "record it and change one number". Its comment justified itself by the
transfer case ("a transfer completes in milliseconds, so this converts a transient window
into a small latency bump", sized against a measured ~4.6% query failure rate while the
balancer was moving leadership) — but 100 ms does not cover the transfer window it names.
D-20 made the worst-case transfer cost an explicit **1 s** abandon window, during which
the group refuses everything; a read that waits 100 ms fails through every mis-aimed
transfer that has to be abandoned. The read budget was sized against a sentence, and the
sentence had since changed.

## Decision

**1. Keep the asymmetry, and state it as a rule about the two operations rather than about
two numbers.**

> A read waits for the events WE cause. A write waits for the events that happen TO us.

The reasoning is about what a failure costs the client, not about what the cluster is
doing:

* A failed read is **idempotent and cheap to re-issue**, and its caller — a dashboard, an
  alert evaluator, a `POST /query` — usually wants an answer or an error *promptly*. A read
  that blocks for 6 s converts a transient into a client-side timeout, and holds the
  query's memory and its shard's attention while it does.
* A failed write costs the client **a whole re-submitted batch** (10k points on the
  canonical path). That is expensive enough to be worth waiting an election for, and D-14
  measured exactly how expensive not waiting was.
* The read path also has a cheaper local remedy the write path does not: `QUERY_INCOMPLETE`
  is a distinct, documented, retryable error code (see CLAUDE.md, "Incomplete Results Are
  Failures, Never Short Answers"). It means *"this range may hold data that could not be
  read"* and is correct to retry. There is no equivalent "partially accepted" answer for a
  write.

**2. Size the read budget from the event it is claimed to cover, not from a round number.**

`kReadLeaderlessBudget = kReadLeaderRetries (24) x kReadLeaderRetryDelay (50 ms) = 1.2 s`

* `>= raftTicksToWallClock(kRaftTransferTicks)` (1 s) — it covers a transfer, including one
  that is abandoned at the D-20 window, with 200 ms of slack for the round trips inside it.
* `< raftTicksToWallClock(kRaftElectionTicksMin)` (2.5 s) — it does **not** cover an
  election. This is the load-bearing half of the decision: a read must remain the
  fast-failing operation, and a budget that crept past an election timeout would have
  silently adopted the write path's trade.
* `< ReplicatedBatchWriteRouter::kElectionDeadline` (6 s) — the asymmetry keeps its
  direction.

All three are `static_assert`s in `cluster_data_plane.hpp`, which is the one header that can
see the Raft tick period, the Raft timeouts and the write router's deadline at once — the
D-20 pattern, adopted here for the D-20 reason: the relationships span three files and none
of them could see another. 50 ms rather than 25 ms because each retry re-runs
`gatherLeaders()` across every shard; the budget is what matters, and it is cheaper to spend
it in fewer, longer sleeps.

**3. Do not merge the redirect budget into it.** `kReadRedirectRounds` (2) stays separate.
A redirect is *progress* — we learned where the leader is and re-ask immediately, with no
sleep — where a leaderless retry is a *wait*. Charging redirects to the wait budget let the
ordinary RF < N cold-cache round spend the election tolerance before any election mattered.

## Alternatives considered

**(a) Symmetry: give the read the write's 6 s window.** Rejected. It inverts the
cost argument — the operation that is cheap to retry would become the one that blocks
longest — and it makes a leaderless VShard hold a query's memory, and a shard's attention,
for the whole election. It would also change the meaning of `QUERY_INCOMPLETE` in practice:
the code exists so a caller can retry, and a 6 s server-side wait is a worse version of the
retry the caller was going to do anyway, with none of the caller's context (its own
deadline, its own backoff, whether it still wants the answer).

**(b) Symmetry the other way: give the write the read's 100 ms.** Rejected outright — that
is the pre-D-14 behaviour, and D-14 measured it at 396/400 failed batches during a
one-node outage.

**(c) Make it configurable.** Rejected for now, on the D-20 precedent: a knob for a
consensus-timing relationship is a knob for breaking the relationship, and the assertions
are what keep the three numbers honest. If it becomes configurable it must be validated at
startup against the same three inequalities, exactly as `ClusterDataPlane::start()` already
restates the D-20 assertions at runtime for a future config key.

**(d) Election-SHAPED extension for reads, mirroring D-14's per-attempt test.** Rejected as
premature rather than wrong. D-14's mechanism is precise — extend only when *every* failure
of an attempt is election-shaped, so one transport failure reverts the budget — and the read
path could adopt it. But the read path's failure classes are not currently separated the way
`isElectionWaitFailure` separates the write path's (a leaderless VShard and an unreachable
holder take different branches and only the second is even counted), and adding a 6 s read
arm without the classification would be the rejected alternative (a) wearing a condition.
Revisit if RF < N read availability during a failover is measured to be the binding problem
— and note that debt D-41 (below) is a bigger factor in that same measurement.

## Consequences

* A read now spends up to **1.2 s plus one in-flight round** before answering
  `QUERY_INCOMPLETE` where it spent 100 ms. The budget is checked in WALL CLOCK between
  rounds, not as an iteration count, because the sleeps are not the only cost: each round
  also runs `gatherLeaders()` (a sequential `invoke_on` across every shard) and a full
  remote fan-out, so a redirect-churning read counted in iterations could have sailed past
  the 2.5 s election minimum the static_assert claims to exclude — the assertion would have
  been arithmetic rather than behaviour. The residual overshoot is the last dispatched
  round, because the read RPCs themselves are untimed (debt D-41).
* **A doomed read is not free while it waits.** Up to ~24 leadership gathers and up to ~24
  remote fan-outs are issued before it gives up, against a cluster that is already
  struggling. This is the strongest argument for keeping the read budget well under the
  write's, and the reason alternative (a) is not merely "slower".
* During a genuine outage this is ~1.1 s of extra latency on a request that fails anyway.
  That is the cost, taken deliberately: the same 1.2 s is what turns a routine rebalance
  from a user-visible error into a latency bump.
* Reads still fail during an election, by design. **An operator seeing read errors and no
  write errors during a node failure is seeing this decision working**, not a bug — and
  that sentence is the reason this ADR exists.
* The three inequalities are compiler-enforced. Re-tuning any of the four constants
  (`kRaftTickPeriod`, `kRaftTransferTicks`, `kRaftElectionTicksMin`, `kElectionDeadline`)
  into conflict with the read budget now fails the build with a message naming this ADR.

## What this ADR does NOT fix, and it is the larger number

**Debt D-41 (filed with this ADR): an RF < N read has no replica fallback and no transport
retry.** A VShard the coordinator does not host is routed to `VShardDirectory::ownerOf`,
which is `placement[vs].front()` — the **primary only**. If that node is unreachable the
read records it, forgets its hints, and **breaks out of the loop immediately**: zero
retries, no attempt at the other RF-1 replicas, `QUERY_INCOMPLETE`. The write path, by
contrast, treats a transport failure as retryable and re-dispatches it up to 6 times inside
its base deadline.

So the read/write asymmetry an operator actually experiences during a one-node outage is
mostly **not** the election budget this ADR sizes — it is that a dead primary makes its
VShards unreadable from any coordinator that does not host them, no matter how long the
budget is. Raising `kReadLeaderlessBudget` cannot help: every retry re-picks the same dead
primary. The material for the fix is already there (`ControlMap::placement` holds the whole
replica vector, and a non-primary holder that does not lead the group will simply redirect
us), but it is a routing change with a double-count contract to preserve and it wants the
fault gate, so it is filed rather than folded in here.

D-41 also carries the read path's missing **per-attempt bound**: `queryNode` is untimed, so
a peer that accepts the connection and then goes silent hangs a read indefinitely, and the
wall-clock budget above cannot help because it is only checked between rounds. The write
path closed this in write-scaleout 3f (`kAttemptTimeout`, pushed into the RPC itself); the
read path has no equivalent. That is also why the D-25 version handshake deliberately uses
the UNTIMED `versionFor`: a bounded handshake in front of an unbounded query is false
reassurance, not defence in depth.
