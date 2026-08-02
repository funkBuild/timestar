#pragma once

#include "../control/group0_state.hpp"  // control::Job
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
