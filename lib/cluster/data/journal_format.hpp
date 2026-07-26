#pragma once

#include "write_record.hpp"  // kWriteBatchFormatV1/V2

#include <cstdint>

namespace timestar::data {

// THE JOURNAL FORMAT GATE (debt D-7).
//
// WHAT PROBLEM THIS SOLVES. A Raft command's bytes become a log ENTRY: replicated to every
// voter and written to every voter's journal. Two consequences, and they are why the
// data-plane's per-peer negotiation cannot be reused here:
//
//   * a voter takes no part in the pairwise data-plane handshake (`kNegotiateVersion`), so
//     there is nothing bilateral to negotiate with -- the leader does not "send" a log
//     entry to a peer it has handshaken with, it appends bytes that N peers must read;
//   * the journal OUTLIVES the process. A cluster restarting on a newer binary replays
//     entries an older one wrote, and -- the direction that actually bites -- a cluster
//     ROLLED BACK to an older binary replays entries a newer one wrote.
//
// So the emission version must be a CLUSTER-WIDE agreement, not a per-connection one, and
// that is exactly what group 0's committed format activation is
// (`Group0State::activeFormatVersion`, proposed only after `features::FeatureGate::
// canActivate` confirms EVERY current voter supports the version -- see
// control/group0_controller.cpp).
//
// FAIL-CLOSED IS THE WHOLE DESIGN. The gate starts at v1 and only ever RISES, and only on
// an explicit activation carrying a COMMITTED group-0 version. A node that has not heard
// an activation -- because it just booted, because group 0 is unreachable, because it is
// mid-join -- emits v1, which every binary that has ever existed can read. There is no
// configuration, environment variable or code path that makes a node emit v2 on its own
// say-so, and that is deliberate: a single node emitting v2 into a shared group's log is
// enough to make that log unreadable to every un-upgraded voter in it.
//
// WHY "AN OLD BINARY READS A NEW JOURNAL" NEEDS NO TEST, AND CANNOT HAVE ONE. It is
// UNREACHABLE, by the activation ORDERING rather than by any decode-side tolerance:
//
//   1. `FeatureGate::canActivate(v, voterVersions)` requires EVERY current voter to
//      advertise support for v before the controller will even propose the activation;
//   2. the activation is a group-0 COMMAND, so it is only observable after it COMMITS --
//      i.e. after a quorum of meta-voters has durably accepted it;
//   3. only then does any node's gate rise, so the first v2 byte is written strictly AFTER
//      every voter could already read v2.
//
// A binary that cannot read v2 therefore cannot be a voter in a group whose gate has
// risen. The residual case is a DOWNGRADE -- rolling a node back to a binary older than
// the activation -- and that is a deliberate operator action against a committed cluster
// decision, not a version skew the format can absorb; it needs the activation lowered (or
// the node re-imaged) first. Stated here because it is the one thing a reader will look
// for and not find as a test.
//
// THE DECODER IS UNCONDITIONALLY BIDIRECTIONAL and must stay that way:
// `decodeWriteBatch` sniffs the v2 magic and falls back to v1 on any failure, so an OLD
// journal is readable forever regardless of what the gate says. The gate governs EMISSION
// only.

// The group-0 `activeFormatVersion` at which the journal may start emitting write_record
// v2. Separate from `kWriteBatchFormatV2` because the two numbering schemes are
// independent: group 0's version counts CLUSTER-WIDE format activations (of which the
// journal codec is one participant), while write_record's counts payload layouts.
constexpr uint32_t kJournalV2ActivationVersion = 2;

// Per-shard, because a Seastar shard is a thread and this is read on the write hot path:
// one thread_local load beats any synchronization. An activation is pushed to every shard
// with `invoke_on_all`, so the shards agree within one hop -- and a shard that has not yet
// been told simply keeps emitting v1, which is safe by construction (see above).
class JournalFormatGate {
public:
    // The write_record format the Raft command path may EMIT right now.
    static uint32_t writeBatchFormat() {
        return activeVersion() >= kJournalV2ActivationVersion ? kWriteBatchFormatV2 : kWriteBatchFormatV1;
    }

    // Apply a COMMITTED group-0 `activeFormatVersion` to this shard. MONOTONIC: a lower
    // version is ignored rather than applied. That is not tidiness -- group-0 state is
    // rebuilt by replaying its log, and a mid-replay observer can transiently see an older
    // value than the one already committed. Lowering the gate on such an observation would
    // emit v1 into a log whose readers had all moved on, which is harmless, and would then
    // RAISE it again, which is a flapping format. Monotonic is the honest reading of
    // "activated": an activation is a decision, not a setting.
    static void activate(uint32_t group0FormatVersion) {
        uint32_t& v = mutableActiveVersion();
        if (group0FormatVersion > v)
            v = group0FormatVersion;
    }

    // The group-0 format version this shard believes is active (1 == nothing activated).
    static uint32_t activeVersion() { return mutableActiveVersion(); }

    // Tests only: drop back to the fail-closed default. Deliberately NOT named `activate`
    // or reachable from configuration -- nothing in production may lower the gate.
    static void resetForTesting() { mutableActiveVersion() = 1; }

private:
    static uint32_t& mutableActiveVersion() {
        static thread_local uint32_t v = 1;  // fail closed: v1 until the cluster says otherwise
        return v;
    }
};

}  // namespace timestar::data
