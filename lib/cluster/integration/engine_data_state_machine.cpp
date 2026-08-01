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
    // Half the snapshot trigger's input (D-6); see appliedBytesSinceSnapshot().
    appliedBytesSinceSnapshot_ += entry.data.size();
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
        co_await store_.applyCommittedWrites(std::move(*w));
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
    // Decode the self-contained InstallSnapshot payload (manifest + TSM file bytes) and
    // install it into this VShard's Engine core, verify-then-install all-or-nothing. A
    // malformed payload or a failed verification is FAIL-STOP (throws) -- never a silent
    // partial install, which would leave this replica diverged.
    auto payload = data::decodeSnapshotPayload(snap.data);
    if (!payload)
        throw std::runtime_error("EngineDataStateMachine::applySnapshot: undecodable snapshot payload (fail-stop)");
    // The producer deliberately compacts one entry below the highest flushed
    // point revision: entry N can be only partly flushed, so N remains in the
    // retained suffix and replays idempotently. Bind those two independently
    // checksummed envelopes here. Without this check, a valid payload paired
    // with the wrong Raft snapshot metadata could install state ahead of (or
    // unrelated to) the log prefix being discarded.
    if (snap.index == UINT64_MAX || payload->manifest.snapshotRevision != snap.index + 1)
        throw std::runtime_error(
            "EngineDataStateMachine::applySnapshot: data revision does not match Raft snapshot boundary "
            "(fail-stop)");
    const bool installed = co_await store_.installVShardSnapshot(vshard_, std::move(*payload));
    if (!installed)
        throw std::runtime_error(
            "EngineDataStateMachine::applySnapshot: snapshot failed verification and was not installed (fail-stop)");
    // The snapshot subsumes the log up to snap.index; advance the applied watermark so a
    // subsequent apply() of the post-snapshot suffix is correctly ordered.
    appliedIndex_ = snap.index;
    // Everything this counter was measuring is now inside the snapshot (D-6).
    appliedBytesSinceSnapshot_ = 0;
    co_return;
}

}  // namespace timestar::cluster
