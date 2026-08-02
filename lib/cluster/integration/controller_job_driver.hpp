#pragma once

#include "../control/control_command.hpp"
#include "../movement/mover.hpp"
#include "raft_move_executor.hpp"

#include <cstdint>
#include <functional>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <string>

namespace timestar::cluster {

// The controller job driver (integration plan M5 task 1): on the group-0 leader, a
// fiber scans Group0State::jobs and drives each undone MoveJob to completion via the
// Mover over the real per-VShard RaftGroup. Each advanced step is persisted back as a
// group-0 UpsertJob (injected here as `persistJob`) so the NEXT group-0 leader resumes
// after a controller crash from the retained step -- the moves are idempotent and
// forward-only, so re-driving a persisted job is safe.
//
// This is the bridge from a persisted control::Job to the movement Mover; the group-0
// proposal side (Group0Controller::proposeCommand(UpsertJob)) is injected so the
// bridge is testable against the real RaftGroup without a full group-0 harness (the
// group-0 propose path is proven in group0_controller_test).
class ControllerJobDriver {
public:
    // Decode only internally consistent persisted movement jobs. Group0State
    // stores the step/done summary beside the payload so operators can inspect
    // jobs without decoding them; disagreement means the durable record is
    // corrupt or was constructed by a buggy controller and must never actuate a
    // membership change.
    static std::optional<movement::MoveJob> decodeMoveJob(const control::Job& job) {
        if (job.id.empty())
            return std::nullopt;
        auto decoded = movement::MoveJob::decode(job.payload);
        if (!decoded || job.step != static_cast<uint32_t>(decoded->step()) || job.done != decoded->done())
            return std::nullopt;
        return decoded;
    }

    // Authorize destination materialization exclusively from the receiver's
    // committed Group-0 state. The caller-supplied job id and controller fence
    // select an existing decision; they do not supply any topology fields.
    static std::optional<movement::MoveJob> authorizeDestination(const control::Group0State& state, NodeId self,
                                                                 const control::EnsureMoveDestinationRequest& request) {
        if (state.clusterUuid != request.clusterUuid || state.controllerTerm != request.controllerTerm ||
            state.controllerLeader != request.controllerLeader || state.mapEpoch == 0)
            return std::nullopt;
        const auto node = state.nodes.find(self);
        if (node == state.nodes.end() || node->second.state != control::NodeState::Active)
            return std::nullopt;
        const auto found = state.jobs.find(request.jobId);
        if (found == state.jobs.end())
            return std::nullopt;
        auto move = decodeMoveJob(found->second);
        if (!move || move->plan().dest != self || move->plan().mapEpoch != state.mapEpoch)
            return std::nullopt;
        const auto desired = state.desiredPlacement.find(move->plan().vshard);
        if (desired == state.desiredPlacement.end() || desired->second != move->targetVoters())
            return std::nullopt;
        const auto serving = state.servingMap.placement.find(move->plan().vshard);
        if (serving == state.servingMap.placement.end())
            return std::nullopt;
        const bool beforeCutover =
            state.servingMap.epoch + 1 == move->plan().mapEpoch && serving->second == move->plan().sourceVoters;
        const bool afterCutover =
            state.servingMap.epoch == move->plan().mapEpoch && serving->second == move->targetVoters() && move->done();
        return beforeCutover || afterCutover ? std::move(move) : std::nullopt;
    }

    // Drive one persisted job whose payload is a MoveJob. Decodes it, runs it against a
    // RaftGroupMoveExecutor over `group` (the data VShard's group, on which this node
    // must lead), and reports each advanced step through `persistJob` as an updated
    // control::Job. Returns the job's final done-state; a payload that is not a valid
    // MoveJob is skipped (returns false without touching the group).
    static seastar::future<bool> driveMoveJob(control::Job job, raft::RaftGroup& group, size_t minRf,
                                              std::function<seastar::future<>(control::Job)> persistJob) {
        auto mj = decodeMoveJob(job);
        if (!mj)
            co_return false;  // corrupt / inconsistent / not a move job -> do not actuate
        const std::string jobId = job.id;
        RaftGroupMoveExecutor exec(group, [jobId, persistJob](const movement::MoveJob& m) {
            // Mirror the advanced MoveJob back into a group-0 Job (jobId preserved,
            // step/done/payload updated) and propose it.
            return persistJob(control::Job{jobId, static_cast<uint32_t>(m.step()), m.done(), m.encode()});
        });
        movement::Mover mover(minRf);
        co_await mover.run(*mj, exec);
        co_return mj->done();
    }
};

}  // namespace timestar::cluster
