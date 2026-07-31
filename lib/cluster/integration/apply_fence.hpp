#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace timestar::cluster {

// One hosted group's state as the read fence sees it (debt D-36). Deliberately a plain
// struct over integers and bools: it is the whole interface between the fence's POLICY --
// which is subtle, and which this header exists to make testable -- and the RaftGroup it
// is read out of, which needs a live cluster to construct.
struct FenceGroupState {
    // Is this group still hosted here? A group that leaves (movement) cannot hold a read
    // back: it is no longer part of this node's answer.
    bool hosted = false;
    // Has this group committed something in its CURRENT term? See
    // RaftNode::hasCurrentTermCommit -- without it, commitIndex is not an upper bound on
    // what was acknowledged and "commit minus applied" is a meaningless zero.
    bool hasCurrentTermCommit = false;
    uint64_t commitIndex = 0;
    uint64_t appliedIndex = 0;
};

// THE READ FENCE'S POLICY: which groups must a node wait for before it may answer a
// query, and when has each of them caught up?
//
// The rule the whole thing exists to enforce: a query must see every write acknowledged
// BEFORE it started. An acknowledged write is durable at COMMIT and readable only at
// APPLY, so the gap between those two watermarks is the set of promises the node cannot
// currently keep -- and answering out of it is a silent short answer, which this project
// treats as a failure rather than a result.
//
// TWO REASONS A GROUP IS PENDING, and the second is the one that is easy to miss:
//
//   1. It has committed past what it has applied. Obvious, and the bar is that commit
//      index, sampled once so the wait is BOUNDED -- waiting for "no lag at all" on a
//      node under continuous ingest is a livelock, not a bound.
//
//   2. It has NO CURRENT-TERM COMMIT, in which case its commit index proves nothing and
//      there is no bar to sample yet. commitIndex is not persisted, so a restarted node
//      starts at its snapshot boundary; and a freshly elected leader reports itself
//      leader while `maybeAdvanceCommitAsLeader` still refuses to raise commitIndex until
//      its no-op reaches a majority. In that window commit == applied, so rule 1 alone
//      reports a lag of ZERO on a group holding an entire unapplied recovered log. The
//      first version of this fence had exactly that hole, and it is not a restart-only
//      edge: it reopens on EVERY leadership change.
//
// So a group with no current-term commit is enrolled with NO BAR, and its bar is sampled
// at the moment one appears. That bar is >= the entry-time commit index, which is the
// safe direction, and it is still bounded -- by one election round plus the apply.
class ApplyFencePolicy {
public:
    // Enroll a group at fence entry. Only groups that must be waited on are kept, so an
    // all-caught-up node leaves the policy EMPTY and never suspends.
    void enroll(uint16_t groupId, const FenceGroupState& s) {
        if (!s.hosted)
            return;
        if (!s.hasCurrentTermCommit) {
            pending_[groupId] = std::nullopt;  // no bar can be sampled yet (reason 2)
            return;
        }
        if (s.appliedIndex < s.commitIndex)
            pending_[groupId] = s.commitIndex;  // reason 1: bar is the entry-time commit
    }

    bool clear() const { return pending_.empty(); }
    size_t pendingCount() const { return pending_.size(); }
    // Groups still pending, ascending. For the fail-closed log line and for tests.
    std::vector<uint16_t> pendingGroups() const {
        std::vector<uint16_t> out;
        out.reserve(pending_.size());
        for (const auto& [id, bar] : pending_)
            out.push_back(id);
        return out;
    }
    // Has this group had a bar sampled yet? (Tests; also what distinguishes the two
    // pending reasons in a diagnosis.)
    std::optional<uint64_t> barFor(uint16_t groupId) const {
        auto it = pending_.find(groupId);
        return it == pending_.end() ? std::nullopt : it->second;
    }

    // Re-evaluate every pending group and drop the ones that have caught up. `probe`
    // returns the current FenceGroupState for a group id. Returns true once nothing is
    // pending.
    template <class Probe>
    bool refresh(Probe&& probe) {
        for (auto it = pending_.begin(); it != pending_.end();) {
            const FenceGroupState s = probe(it->first);
            if (!s.hosted) {
                it = pending_.erase(it);  // left this node; not part of our answer
                continue;
            }
            if (!it->second) {
                // Still waiting for a bar to become meaningful.
                if (!s.hasCurrentTermCommit) {
                    ++it;
                    continue;
                }
                it->second = s.commitIndex;
            }
            if (s.appliedIndex >= *it->second)
                it = pending_.erase(it);
            else
                ++it;
        }
        return pending_.empty();
    }

private:
    // group id -> bar (nullopt = no current-term commit yet, so no bar exists).
    std::map<uint16_t, std::optional<uint64_t>> pending_;
};

}  // namespace timestar::cluster
