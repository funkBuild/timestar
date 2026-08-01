#pragma once

#include "../control/group0_state.hpp"  // Group0State
#include "../data/journal_format.hpp"   // JournalFormatGate

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>

namespace timestar::cluster {

// The seam between group 0's COMMITTED format activation and the journal codec's emission
// version (debt D-7).
//
// It exists as a named function rather than a call buried in the group-0 apply path for
// one reason: `JournalFormatGate` is per-shard (thread_local, because it is read on the
// write hot path), so pushing an activation is an `invoke_on_all` -- and that must be the
// ONLY way the gate ever moves. Anything that reaches for
// `JournalFormatGate::activate` directly on one shard leaves the others emitting v1, which
// is safe but silently defeats the activation.
//
// PUSH DIRECTION AND FAIL-CLOSED-NESS. Production calls this only from group 0's
// state-machine observer, after a SetActiveVersion commits or an installed snapshot
// recovers that committed decision. It never lowers the gate (`activate` is monotonic),
// so an observation racing a replay cannot un-activate a format, and a shard that has not
// yet been reached keeps emitting v1. Both directions of uncertainty therefore land on
// "emit the format everyone can read".
//
// Production installs this function as Group0StateMachine's observer. The observer is
// awaited before Raft advances the applied boundary for both commands and snapshots, so
// status cannot report an activation applied while some local shard still emits an older
// format. This publishes a committed decision only; it cannot originate one.
inline seastar::future<> publishJournalFormat(uint32_t version) {
    return seastar::smp::invoke_on_all([version] { data::JournalFormatGate::activate(version); });
}

inline seastar::future<> publishJournalFormat(const control::Group0State& state) {
    return publishJournalFormat(state.activeFormatVersion);
}

}  // namespace timestar::cluster
