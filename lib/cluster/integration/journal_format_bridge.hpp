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
// PUSH DIRECTION AND FAIL-CLOSED-NESS. This is called with a group-0 state the caller has
// OBSERVED as committed -- on becoming the group-0 leader, on applying a SetActiveVersion,
// and on a ReadIndex reconciliation after a missed notification. It never lowers the gate
// (`activate` is monotonic), so an observation racing a replay cannot un-activate a format,
// and a shard that has not yet been reached keeps emitting v1. Both directions of
// uncertainty therefore land on "emit the format everyone can read".
//
// NOT YET CALLED FROM THE RUNNING SERVER, and that is the current fail-closed state rather
// than a gap in this wiring: the data plane the production server starts
// (`ClusterDataPlane`) has no live group 0 attached to it yet (M3's control plane is
// gate-proven but not composed into the server -- see the integration plan's remaining
// milestones). Until it is, no node ever observes an activation, every node's gate stays
// at v1, and no v2 byte can reach a journal. That is exactly the guarantee D-7 was filed
// to obtain; when group 0 is composed in, this is the one call it has to make.
inline seastar::future<> publishJournalFormat(const control::Group0State& state) {
    const uint32_t version = state.activeFormatVersion;
    return seastar::smp::invoke_on_all([version] { data::JournalFormatGate::activate(version); });
}

}  // namespace timestar::cluster
