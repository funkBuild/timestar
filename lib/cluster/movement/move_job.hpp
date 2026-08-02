#pragma once

#include "../../core/vshard.hpp"
#include "../raft/raft_types.hpp"  // NodeId

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace timestar::movement {

using timestar::raft::NodeId;

// A single bounded move (docs/clustering.md §"Adding one machine"): change ONE
// member of ONE VShard group. Either a pure GROW (add `dest` as a new replica) or
// a REPLACE (`dest` takes over from `victim`). Only one member of a group moves at
// once, and -- crucially -- a voter is never removed before its replacement is a
// committed voter, so the group never drops below its replication factor.
enum class MoveStep : uint8_t {
    Planned = 0,       // nothing committed yet
    LearnerAdded = 1,  // dest added as a non-voting learner (RF unchanged)
    CaughtUp = 2,      // dest installed the snapshot + replayed to the cutover index
    Promoted = 3,      // dest promoted to voter (RF temporarily +1 for a replace)
    OldRemoved = 4,    // victim voter removed (replace only) -> back to target RF
    Done = 5,
};

struct MovePlan {
    uint16_t vshard = 0;
    NodeId dest = 0;
    NodeId victim = 0;  // 0 == pure grow (no removal)
    // The group-0 desired-map epoch that authorized this move and the exact
    // stable voter set it was planned from. A restarted controller must not
    // apply an old removal to a group that has since changed for another reason.
    uint64_t mapEpoch = 0;
    std::vector<NodeId> sourceVoters;

    bool isReplace() const { return victim != 0; }
    friend bool operator==(const MovePlan&, const MovePlan&) = default;
    // NOTE: `victim` must NOT be the group's current leader. The plan removes a
    // voter only after its replacement is committed; a leader is removed only
    // after a completed leadership transfer (docs/clustering.md §"Membership
    // changes"). The controller transfers leadership away first (via the
    // LeadershipBalancer, plan step 10) and then issues a follower-victim move --
    // so this brick only ever removes a follower. No two attempts of the same
    // batch double-count: re-application is idempotent (LWW) and Raft applied_index
    // is monotonic, so a re-installed snapshot + replay reaches the same state.
};

// The membership configuration a step commits, given the CURRENT voters/learners.
struct ConfigTarget {
    std::vector<NodeId> voters;
    std::vector<NodeId> learners;
    friend bool operator==(const ConfigTarget&, const ConfigTarget&) = default;
};

// The idempotent, crash-resumable step machine for one move. Pure logic (no I/O):
// the Mover applies each step's config via proposeConfChange and catches the
// learner up with a snapshot in between. Persisted via encode()/decode() into a
// group-0 Job so a controller crash resumes from the retained step.
class MoveJob {
public:
    MoveJob() = default;
    explicit MoveJob(MovePlan plan, MoveStep step = MoveStep::Planned) : plan_(plan), step_(step) {}

    MoveStep step() const { return step_; }
    const MovePlan& plan() const { return plan_; }
    bool done() const { return step_ == MoveStep::Done; }

    std::vector<NodeId> targetVoters() const {
        std::vector<NodeId> target = plan_.sourceVoters;
        if (plan_.isReplace()) {
            auto victim = std::find(target.begin(), target.end(), plan_.victim);
            if (victim != target.end())
                *victim = plan_.dest;  // preserve the placement/primary slot
        } else {
            target.push_back(plan_.dest);
        }
        return target;
    }

    // Structural validity is independent of live group state. The source voter
    // vector is a set (order does not matter), contains the replace victim, and
    // deliberately excludes the destination.
    bool valid() const {
        if (plan_.vshard >= timestar::VIRTUAL_SHARD_COUNT || plan_.dest == raft::kNoNode ||
            plan_.dest == plan_.victim || plan_.mapEpoch == 0 || plan_.sourceVoters.empty() ||
            static_cast<uint8_t>(step_) > static_cast<uint8_t>(MoveStep::Done) ||
            (!plan_.isReplace() && step_ == MoveStep::OldRemoved))
            return false;
        std::vector<NodeId> source = canonical(plan_.sourceVoters);
        if (source.front() == raft::kNoNode || std::adjacent_find(source.begin(), source.end()) != source.end() ||
            contains(source, plan_.dest))
            return false;
        return !plan_.isReplace() || contains(source, plan_.victim);
    }

    // A persisted step may lag one membership commit when the controller dies
    // after Raft applies the transition but before group 0 records the next job
    // payload. Accept exactly those two adjacent forward states—never an
    // arbitrary configuration that merely happens to remain above RF.
    bool acceptsConfig(const std::vector<NodeId>& voters, const std::vector<NodeId>& learners) const {
        if (!valid())
            return false;
        const auto source = canonical(plan_.sourceVoters);
        const auto withLearner = canonical(std::vector<NodeId>{plan_.dest});
        const auto promoted = canonical(withNode(source, plan_.dest));
        const auto final = plan_.isReplace() ? canonical(withoutNode(promoted, plan_.victim)) : promoted;
        const auto liveVoters = canonical(voters);
        const auto liveLearners = canonical(learners);
        const std::vector<NodeId> noLearners;

        switch (step_) {
            case MoveStep::Planned:
                return liveVoters == source && (liveLearners == noLearners || liveLearners == withLearner);
            case MoveStep::LearnerAdded:
                return liveVoters == source && liveLearners == withLearner;
            case MoveStep::CaughtUp:
                return (liveVoters == source && liveLearners == withLearner) ||
                       (liveVoters == promoted && liveLearners == noLearners);
            case MoveStep::Promoted:
                return (liveVoters == promoted || liveVoters == final) && liveLearners == noLearners;
            case MoveStep::OldRemoved:
            case MoveStep::Done:
                return liveVoters == final && liveLearners == noLearners;
        }
        return false;
    }

    // Recovery can stop after the joint entry is durable but before Raft appends
    // or applies the final stable configuration. Destination materialization
    // validates that in-flight shape before registering the recovered group.
    bool acceptsConfig(const std::vector<NodeId>& voters, const std::vector<NodeId>& outgoing,
                       const std::vector<NodeId>& learners) const {
        if (outgoing.empty())
            return acceptsConfig(voters, learners);
        if (!valid())
            return false;
        const auto source = canonical(plan_.sourceVoters);
        const auto promoted = canonical(withNode(source, plan_.dest));
        const auto final = plan_.isReplace() ? canonical(withoutNode(promoted, plan_.victim)) : promoted;
        const auto liveVoters = canonical(voters);
        const auto liveOutgoing = canonical(outgoing);
        const auto liveLearners = canonical(learners);
        const std::vector<NodeId> noLearners;
        const auto destinationLearner = canonical(std::vector<NodeId>{plan_.dest});
        switch (step_) {
            case MoveStep::Planned:
                return liveVoters == source && liveOutgoing == source && liveLearners == destinationLearner;
            case MoveStep::CaughtUp:
                return liveVoters == promoted && liveOutgoing == source && liveLearners == noLearners;
            case MoveStep::Promoted:
                return plan_.isReplace() && liveVoters == final && liveOutgoing == promoted &&
                       liveLearners == noLearners;
            case MoveStep::LearnerAdded:
            case MoveStep::OldRemoved:
            case MoveStep::Done:
                return false;
        }
        return false;
    }

    // The step that must be performed next (the transition OUT of the current
    // step). Done stays Done.
    MoveStep nextStep() const {
        switch (step_) {
            case MoveStep::Planned:
                return MoveStep::LearnerAdded;
            case MoveStep::LearnerAdded:
                return MoveStep::CaughtUp;
            case MoveStep::CaughtUp:
                return MoveStep::Promoted;
            case MoveStep::Promoted:
                return plan_.isReplace() ? MoveStep::OldRemoved : MoveStep::Done;
            case MoveStep::OldRemoved:
                return MoveStep::Done;
            case MoveStep::Done:
                return MoveStep::Done;
        }
        return MoveStep::Done;
    }

    // The membership the NEXT step should commit, given the current voters/learners.
    // Returns nullopt when the next step is NOT a membership change (CaughtUp, which
    // is a snapshot+replay, and Done). Every returned target keeps the group at or
    // above its replication factor and differs from the current voter set by at
    // most one member.
    std::optional<ConfigTarget> nextConfig(const std::vector<NodeId>& voters,
                                           const std::vector<NodeId>& learners) const {
        switch (nextStep()) {
            case MoveStep::LearnerAdded: {
                // Add dest as a learner: voters UNCHANGED (RF preserved), dest joins
                // learners. Idempotent -- adding an existing learner is a no-op set.
                ConfigTarget t{voters, withNode(learners, plan_.dest)};
                return t;
            }
            case MoveStep::Promoted: {
                // Promote dest to voter BEFORE removing any old voter: voters gains
                // dest (RF -> RF+1 for a replace), dest leaves learners.
                ConfigTarget t{withNode(voters, plan_.dest), withoutNode(learners, plan_.dest)};
                return t;
            }
            case MoveStep::OldRemoved: {
                // Only now remove the old voter (replace) -> back to target RF.
                ConfigTarget t{withoutNode(voters, plan_.victim), learners};
                return t;
            }
            case MoveStep::CaughtUp:  // snapshot + replay, not a membership change
            case MoveStep::Done:
            case MoveStep::Planned:
                return std::nullopt;
        }
        return std::nullopt;
    }

    // Monotonic advance: never move a step backwards (a replayed/duplicated
    // transition is a no-op), matching the group-0 UpsertJob contract.
    void advance(MoveStep to) {
        if (static_cast<uint8_t>(to) > static_cast<uint8_t>(step_))
            step_ = to;
    }

    // Invariant a proposed config must satisfy: the voter count never drops below
    // `minRf`, and the voter set differs from the current one by at most one member
    // (add xor remove xor unchanged) -- "only one member of a group moves at once".
    static bool maintainsRf(const std::vector<NodeId>& before, const std::vector<NodeId>& after, size_t minRf) {
        if (after.size() < minRf)
            return false;
        size_t added = 0, removed = 0;
        for (NodeId n : after)
            if (!contains(before, n))
                ++added;
        for (NodeId n : before)
            if (!contains(after, n))
                ++removed;
        return added + removed <= 1;
    }

    // Persist/restore for crash-resumable jobs (payload of a group-0 Job).
    std::string encode() const;
    static std::optional<MoveJob> decode(const std::string& bytes);

private:
    static bool contains(const std::vector<NodeId>& v, NodeId n) { return std::find(v.begin(), v.end(), n) != v.end(); }
    static std::vector<NodeId> withNode(std::vector<NodeId> v, NodeId n) {
        if (!contains(v, n))
            v.push_back(n);
        return v;
    }
    static std::vector<NodeId> withoutNode(std::vector<NodeId> v, NodeId n) {
        v.erase(std::remove(v.begin(), v.end(), n), v.end());
        return v;
    }
    static std::vector<NodeId> canonical(std::vector<NodeId> v) {
        std::sort(v.begin(), v.end());
        return v;
    }

    MovePlan plan_{};
    MoveStep step_ = MoveStep::Planned;
};

}  // namespace timestar::movement
