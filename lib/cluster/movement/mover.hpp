#pragma once

#include "move_job.hpp"

#include <algorithm>
#include <functional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <stdexcept>
#include <vector>

namespace timestar::movement {

// A move violated a safety invariant (would drop below RF, or move more than one
// member at once). The Mover aborts rather than committing an unsafe membership.
class UnsafeMove : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// The group-side operations the Mover drives. Implemented over a real RaftGroup
// (leader) in production/growth tests, or a mock in unit tests. Every operation is
// idempotent so a crash-resumed move re-issuing a step is harmless.
class MoveExecutor {
public:
    virtual ~MoveExecutor() = default;

    // Current committed membership of the group (the leader's view).
    virtual std::vector<NodeId> voters() const = 0;
    virtual std::vector<NodeId> learners() const = 0;

    // Commit `target` via joint consensus and resolve once it has taken effect
    // (the joint transition has left joint and the new voter set is active).
    // Returns false if this node is not the leader (the move is retried/redriven
    // by the current leader). Idempotent: committing the already-active config is
    // a no-op.
    virtual seastar::future<bool> commitConfig(ConfigTarget target) = 0;

    // Ensure `dest` has installed the source snapshot and replayed up to at least
    // the current commit index; resolve with the cutover index reached. Throws if
    // `dest` cannot be caught up.
    virtual seastar::future<uint64_t> catchUp(NodeId dest) = 0;

    // Persist the job's just-advanced step so a controller crash resumes from it.
    virtual seastar::future<> persist(const MoveJob& job) = 0;
};

// Drives one move to completion, one idempotent step at a time, persisting after
// each so a crash resumes from the retained step. Every membership step is checked
// against the RF / one-member-at-a-time invariant BEFORE it is committed; a
// violation aborts the move (UnsafeMove) rather than weakening the group.
class Mover {
public:
    explicit Mover(size_t minRf) : minRf_(minRf) {}

    // `mayProceed` gates movement against the SLO throttle / manual pause / cancel
    // (docs/clustering.md §"Rebalance resource protection"). When it returns false
    // the Mover STOPS at the current committed step -- a safe, forward-only state
    // (RF never dropped, no voter removed before its replacement) -- and returns.
    // Resuming is simply re-running the Mover on the persisted job: because every
    // step is idempotent and forward-only, a cancel/pause after a membership change
    // completes the SAFE FORWARD transition rather than rolling anything back.
    seastar::future<> run(MoveJob& job, MoveExecutor& exec, std::function<bool()> mayProceed = [] { return true; }) {
        const size_t minRf = minRf_;
        if (!job.valid())
            throw UnsafeMove("movement job is structurally invalid");
        while (!job.done()) {
            if (!mayProceed())
                co_return;  // paused/cancelled: stop in a safe forward-committed state
            if (!job.acceptsConfig(exec.voters(), exec.learners()))
                throw UnsafeMove("live membership does not match the movement job's authorized source/step");
            const MoveStep next = job.nextStep();
            if (next == MoveStep::CaughtUp) {
                // Snapshot install + log replay to the cutover. Not a membership
                // change, so RF is untouched.
                co_await exec.catchUp(job.plan().dest);
                job.advance(MoveStep::CaughtUp);
                co_await exec.persist(job);
                continue;
            }
            const std::vector<NodeId> voters = exec.voters();
            const std::vector<NodeId> learners = exec.learners();
            // Defense-in-depth for the promote step: only promote a destination
            // that IS a committed, caught-up learner (or already a voter, for an
            // idempotent re-issue after a crash). If it vanished -- external config
            // drift removed the learner -- refuse rather than add an empty voter
            // that counts toward quorum with no log and stalls commits.
            if (next == MoveStep::Promoted) {
                NodeId dest = job.plan().dest;
                bool isLearner = std::find(learners.begin(), learners.end(), dest) != learners.end();
                bool isVoter = std::find(voters.begin(), voters.end(), dest) != voters.end();
                if (!isLearner && !isVoter)
                    throw UnsafeMove("promote target is not a caught-up learner");
            }
            std::optional<ConfigTarget> target = job.nextConfig(voters, learners);
            if (!target) {
                job.advance(next);  // Done
                co_await exec.persist(job);
                continue;
            }
            // Safety gate: never commit a config that drops the group below RF or
            // changes more than one voter at once.
            if (!MoveJob::maintainsRf(voters, target->voters, minRf))
                throw UnsafeMove("move would drop below RF or move >1 member at once");
            bool committed = co_await exec.commitConfig(*target);
            if (!committed)
                co_return;  // not the leader now: the current leader redrives the job
            job.advance(next);
            co_await exec.persist(job);
        }
    }

private:
    size_t minRf_;
};

}  // namespace timestar::movement
