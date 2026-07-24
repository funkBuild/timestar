#include "engine_data_state_machine.hpp"

#include <seastar/core/coroutine.hh>
#include <stdexcept>

namespace timestar::cluster {

seastar::future<> EngineDataStateMachine::apply(raft::LogEntry entry) {
    // The RaftGroup only calls apply() for Normal, non-empty entries, so this
    // watermark tracks the last DATA entry applied (see appliedIndex() in the
    // header). The type guard below is defensive -- an empty-data entry would decode
    // to nullopt and fail-stop, which we must not do for a control entry.
    appliedIndex_ = entry.index;
    if (entry.type != raft::EntryType::Normal)
        co_return;

    auto cmd = data::decodeReplicatedCommand(entry.data);
    if (!cmd)
        // Fail-stop: a committed entry every replica holds cannot be decoded here.
        // Skipping it would silently diverge this replica; halting is the safe choice.
        throw std::runtime_error("EngineDataStateMachine: undecodable committed entry (fail-stop)");

    if (auto* w = std::get_if<data::WriteBatch>(&*cmd)) {
        // Stamp revisions from the log index (ADR 0003): every point in this entry
        // gets `entry.index`. EngineLocalStore passes revisions through unchanged (the
        // Engine does not re-stamp a non-empty revision vector), so the value is a
        // pure function of the log on every replica.
        for (auto& s : w->series)
            s.revisions.assign(s.timestamps.size(), entry.index);
        co_await store_.applyWrites(std::move(*w));
    } else if (auto* d = std::get_if<data::DeleteRangeKey>(&*cmd)) {
        co_await store_.applyDelete(d->seriesKey, d->startTime, d->endTime);
    } else {
        const auto& rc = std::get<data::RetentionCutoffCmd>(*cmd);
        // VShard-wide retention cutoff (EngineLocalStore::applyRetention is the M1.x/M6
        // wiring point; measurement-scoping is applied there).
        co_await store_.applyRetention(std::string(), rc.cutoffTime);
    }
}

seastar::future<> EngineDataStateMachine::applySnapshot(raft::Snapshot snap) {
    // Production snapshots are Engine::createVShardSnapshot manifests installed via
    // restoreVShardSnapshot (plan M3 §"snapshot()"); wiring the InstallSnapshot
    // payload as a manifest + object stream is a later M3 task. Until then a snapshot
    // install is refused rather than silently dropping state.
    (void)snap;
    return seastar::make_exception_future<>(
        std::runtime_error("EngineDataStateMachine: snapshot install not yet wired (M3)"));
}

}  // namespace timestar::cluster
