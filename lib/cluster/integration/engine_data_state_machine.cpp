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

    // The Raft group is the isolation boundary. A malformed/hostile forwarder
    // must not be able to commit a series command into a different VShard and
    // make replicas apply data outside the group their snapshot/log owns.
    bool targetsThisVShard = true;
    if (auto* w = std::get_if<data::WriteBatch>(&*cmd)) {
        targetsThisVShard = !w->series.empty();
        for (auto& series : w->series)
            targetsThisVShard = targetsThisVShard && data::vshardOf(series) == vshard_.value();
    } else if (const auto* d = std::get_if<data::DeleteRangeKey>(&*cmd)) {
        targetsThisVShard = timestar::virtualShard(SeriesId128::fromSeriesKey(d->seriesKey)) == vshard_.value();
    } else if (const auto* batch = std::get_if<data::DeleteRangeBatch>(&*cmd)) {
        targetsThisVShard = !batch->targets.empty();
        for (const auto& target : batch->targets)
            targetsThisVShard =
                targetsThisVShard &&
                timestar::virtualShard(SeriesId128::fromSeriesKey(target.seriesKey)) == vshard_.value();
    }
    if (!targetsThisVShard)
        throw std::runtime_error("EngineDataStateMachine: committed command targets a different VShard (fail-stop)");

    if (auto* w = std::get_if<data::WriteBatch>(&*cmd)) {
        // Stamp revisions from the log index (ADR 0003): every point in this entry
        // gets `entry.index`. EngineLocalStore passes revisions through unchanged (the
        // Engine does not re-stamp a non-empty revision vector), so the value is a
        // pure function of the log on every replica.
        for (auto& s : w->series)
            s.revisions.assign(s.timestamps.size(), entry.index);
        co_await store_.applyCommittedWrites(std::move(*w));
    } else if (auto* d = std::get_if<data::DeleteRangeKey>(&*cmd)) {
        const bool idempotent = d->operationId != SeriesId128{};
        const uint64_t commandHash = data::deleteRangeCommandHash(*d);
        if (idempotent) {
            if (const auto found = deleteReceipts_.find(d->operationId); found != deleteReceipts_.end()) {
                if (found->second.commandHash != commandHash)
                    throw std::runtime_error(
                        "EngineDataStateMachine: delete operation ID reused for another command (fail-stop)");
                co_return;
            }
        }
        co_await store_.applyDelete(d->seriesKey, d->startTime, d->endTime);
        // Publish the receipt only after every Engine delete effect is durable.
        // A failure before here leaves the Raft entry unapplied; replay uses the
        // same log index and safely completes the same physical mutation.
        if (idempotent)
            deleteReceipts_.emplace(d->operationId,
                                    data::DeleteOperationReceipt{d->operationId, entry.index, commandHash});
    } else if (auto* batch = std::get_if<data::DeleteRangeBatch>(&*cmd)) {
        const uint64_t commandHash = data::deleteRangeCommandHash(*batch);
        if (const auto found = deleteReceipts_.find(batch->operationId); found != deleteReceipts_.end()) {
            if (found->second.commandHash != commandHash)
                throw std::runtime_error(
                    "EngineDataStateMachine: delete operation ID reused for another command (fail-stop)");
            co_return;
        }
        // One Raft entry is the ordering boundary for the whole canonical
        // VShard batch. If applyDelete fails halfway, no receipt is published;
        // replay repeats the prefix before any later entry can apply, so it
        // cannot erase a write ordered after this batch.
        for (const auto& target : batch->targets)
            co_await store_.applyDelete(target.seriesKey, target.startTime, target.endTime);
        deleteReceipts_.emplace(batch->operationId,
                                data::DeleteOperationReceipt{batch->operationId, entry.index, commandHash});
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
    auto receipts = std::move(payload->deleteReceipts);
    const bool installed = co_await store_.installVShardSnapshot(vshard_, std::move(*payload));
    if (!installed)
        throw std::runtime_error(
            "EngineDataStateMachine::applySnapshot: snapshot failed verification and was not installed (fail-stop)");
    restoreDeleteReceipts(std::move(receipts), snap.index);
    // The snapshot subsumes the log up to snap.index; advance the applied watermark so a
    // subsequent apply() of the post-snapshot suffix is correctly ordered.
    appliedIndex_ = snap.index;
    // Everything this counter was measuring is now inside the snapshot (D-6).
    appliedBytesSinceSnapshot_ = 0;
    co_return;
}

std::vector<data::DeleteOperationReceipt> EngineDataStateMachine::deleteReceiptsThrough(uint64_t snapshotIndex) const {
    std::vector<data::DeleteOperationReceipt> receipts;
    receipts.reserve(deleteReceipts_.size());
    for (const auto& [operationId, receipt] : deleteReceipts_)
        if (receipt.appliedIndex <= snapshotIndex)
            receipts.push_back(receipt);
    return receipts;
}

void EngineDataStateMachine::restoreDeleteReceipts(std::vector<data::DeleteOperationReceipt> receipts,
                                                   uint64_t snapshotIndex) {
    std::map<SeriesId128, data::DeleteOperationReceipt> replacement;
    for (auto& receipt : receipts) {
        if (receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 || receipt.appliedIndex > snapshotIndex ||
            !replacement.emplace(receipt.operationId, receipt).second)
            throw std::runtime_error("EngineDataStateMachine: invalid snapshot delete receipt (fail-stop)");
    }
    deleteReceipts_ = std::move(replacement);
}

}  // namespace timestar::cluster
