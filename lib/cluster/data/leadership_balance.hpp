#pragma once

#include "../raft/raft_types.hpp"  // NodeId, LogIndex, kNoNode

#include <cstdint>

// THE BALANCER'S TRANSFER-ELIGIBILITY PREDICATE, EXTRACTED PURE (debt D-23).
//
// `raft_peer_liveness_test` used to re-implement this predicate by hand -- the 64-entry
// lag bound and the heartbeat comparison, spelled out a second time in the test -- so the
// test and the balancer could drift apart while the test stayed green: the copy kept
// pinning the OLD rule and nothing pinned the new one. Retuning either bound, or dropping
// a half, was therefore an unpinned change that looked pinned.
//
// The predicate now takes exactly what it reads, so a test can call the real one; its only
// other caller, `ShardRaftPlane::transferrableTo`, is a RaftGroup adapter over it and
// nothing else. One implementation is what makes calling it in a test equivalent to
// calling it in the balancer.
namespace timestar::data {

using timestar::raft::LogIndex;

// May `target` be handed leadership of a group right now? (Extracted so the balancer's
// candidate loop can try the NEXT peer when this one is ineligible instead of giving up
// on the group -- see ShardRaftPlane::rebalance. Extracted a second time, as
// this SCALAR overload, so the tests can exercise the real predicate rather than a
// hand-written copy of it -- debt D-23.)
//
// NEVER hand leadership to a peer that is not CAUGHT UP on this group.
//
// A leader with a transfer in flight refuses every proposal until the
// transferee acks up to lastIndex, so targeting a peer that cannot ack costs
// the group its write availability for the whole transfer window. And a DEAD
// peer is the MOST attractive target the loop has: it leads nothing, so its
// deficit is the largest on every pass. Measured on the restart-catch-up
// gate: with one of three nodes down, ~26% of writes failed with
// "1 VShard slice(s) uncommitted ... (last: not-leader)" for as long as it
// stayed down, against a perfectly healthy 2-of-3 quorum.
//
// RaftNode::tick abandons a transfer after `transferTimeout` -- two
// heartbeat intervals, 1 s at the production cadence, since D-20 shortened
// it from one election timeout -- so this is no longer unbounded and no
// longer outlasts the write deadline. A bounded write outage repeated every
// balancer pass is still an outage, though, and a caught-up target is also
// the only one that transfers IMMEDIATELY (transferLeadership sends
// TimeoutNow at once rather than waiting on a catch-up round trip).
//
// THE GUARD USED TO BE EXACT EQUALITY (matchIndex == lastIndex) and that made
// the balancer LOAD-DEPENDENT (debt D-1). On a group taking writes, matchIndex
// trails lastIndex by whatever is in flight essentially all of the time -- the
// leader appends locally, then replicates -- so a perfectly healthy peer failed
// the test on nearly every pass. The balancer converged when the cluster was
// quiet and could only move COLD groups when it was busy, which is exactly
// backwards: hot groups are the ones whose leadership placement matters.
//
// Two independent gates replace it, and BOTH must hold:
//
//   1. LAG BOUND -- within kMaxTransferLagEntries of our log end. This is the
//      "the handoff will complete promptly" half. 64 entries is about one
//      AppendEntries round trip of in-flight work at this batch size, so a
//      peer keeping up passes continuously while one genuinely behind (a
//      restarted node still streaming its backlog) does not. TimeoutNow then
//      fires on the very next ack rather than after a catch-up campaign.
//
//   2. LIVENESS -- replied within the LAST HEARTBEAT ROUND. This is the "the
//      target is actually there" half, and it is the one a pure lag bound
//      CANNOT provide. On an IDLE group a dead peer sits at lag ZERO forever:
//      it was caught up when it died and lastIndex never moves again, so it
//      passes any delta check, including the old exact-equality one. (That
//      hole is therefore not new here -- it is pre-existing, and this closes
//      it.) The heartbeat gives an independent decaying signal: the leader
//      bcastAppends every heartbeatTimeout ticks whether or not there is
//      anything to send, and a live peer answers within an RTT.
//
// THE LIVENESS WINDOW IS ONE HEARTBEAT ROUND, AND IT IS THIS TIGHT FOR A
// MEASURED REASON. It was three rounds (1.5 s at the production 25-tick
// heartbeat) and `fault_injection_gate.sh` regressed: 1 of 2000 bench writes
// failed `RetryableWriteError ... (last: transport)` where the same tree
// without this change took a marginally HEAVIER storm (167 rounds / 457
// connections vs 165 / 442) with zero. The mechanism is specific and it is
// this loop's: the periodic balancer could target a peer whose connection
// had just been RST, because the ack clock had not yet decayed past three rounds
// and the lag bound still held. `transferLeadership` then pins
// `leadTransferee_` and the group refuses EVERY proposal until the transferee
// acks -- and the abandon bound was, WHEN THAT WAS MEASURED, one ELECTION
// timeout (2.5-5 s), far longer than the 1.5 s write deadline. So one
// mis-aimed transfer was one failed batch. The old exact-equality guard was
// accidentally immune: a peer whose acks stopped fell behind a growing
// lastIndex immediately. D-20 has since cut the abandon bound to 1 s, which
// makes a mis-aimed transfer a retry rather than a failed batch -- but that
// is a REASON THIS FILTER STAYS TIGHT, not a licence to relax it: the gate
// result above is the only measurement anyone has of this loop, and it was
// taken at three rounds.
//
// One round is the tightest bound a HEALTHY peer still satisfies -- it answers
// every heartbeat within an RTT, so its clock resets long before the next
// round -- and it cuts the post-reset eligibility window from 1.5 s to 0.5 s.
//
// Residual exposure, bounded and now deliberate: a peer that dies inside the
// current heartbeat round can still be targeted on a pass that races it. That
// costs the group its proposals for one `transferTimeout` (1 s at production
// cadence, was one election timeout before D-20), after which RaftNode::tick
// abandons the transfer (§3.10) and writes resume; the peer then reads stale
// on every later pass and is skipped. One bounded window on the pass that
// races the death, not a window per pass forever -- and since D-20 that
// window fits inside the write deadline, so the batch that races it retries
// into the resumed leader instead of failing.
inline constexpr LogIndex kMaxTransferLagEntries = 64;

inline bool transferrableTo(LogIndex lastIndex, LogIndex matchIndex, uint64_t ticksSinceAck,
                            uint64_t heartbeatTimeout) {
    const bool caughtUp = matchIndex >= lastIndex || (lastIndex - matchIndex) <= kMaxTransferLagEntries;
    const bool live = ticksSinceAck <= heartbeatTimeout;
    return caughtUp && live;
}

}  // namespace timestar::data
