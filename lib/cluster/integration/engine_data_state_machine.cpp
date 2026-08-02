#include "engine_data_state_machine.hpp"

#include "../data/write_errors.hpp"

#include <algorithm>
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
            targetsThisVShard = targetsThisVShard &&
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
            makeRoomForDeleteReceipt(entry.index);
        }
        co_await store_.applyDelete(d->seriesKey, d->startTime, d->endTime);
        // Publish the receipt only after every Engine delete effect is durable.
        // A failure before here leaves the Raft entry unapplied; replay uses the
        // same log index and safely completes the same physical mutation.
        if (idempotent)
            deleteReceipts_.emplace(d->operationId,
                                    data::DeleteOperationReceipt{d->operationId, entry.index, commandHash, 0});
    } else if (auto* batch = std::get_if<data::DeleteRangeBatch>(&*cmd)) {
        const uint64_t commandHash = data::deleteRangeCommandHash(*batch);
        if (const auto found = deleteReceipts_.find(batch->operationId); found != deleteReceipts_.end()) {
            if (found->second.commandHash != commandHash)
                throw std::runtime_error(
                    "EngineDataStateMachine: delete operation ID reused for another command (fail-stop)");
            co_return;
        }
        if (batch->issuedAtMs != 0) {
            const uint64_t timeFloor = batch->issuedAtMs > data::kDeleteReceiptRetentionMs
                                           ? batch->issuedAtMs - data::kDeleteReceiptRetentionMs
                                           : 0;
            advanceDeleteReceiptFloor(timeFloor, entry.index);
            if (batch->issuedAtMs <= deleteReceiptsRetiredBeforeMs_)
                co_return;
            makeRoomForDeleteReceipt(entry.index);
            if (batch->issuedAtMs <= deleteReceiptsRetiredBeforeMs_)
                co_return;
        } else {
            makeRoomForDeleteReceipt(entry.index);
        }
        // One Raft entry is the ordering boundary for the whole canonical
        // VShard batch. If applyDelete fails halfway, no receipt is published;
        // replay repeats the prefix before any later entry can apply, so it
        // cannot erase a write ordered after this batch.
        for (const auto& target : batch->targets)
            co_await store_.applyDelete(target.seriesKey, target.startTime, target.endTime);
        deleteReceipts_.emplace(batch->operationId, data::DeleteOperationReceipt{batch->operationId, entry.index,
                                                                                 commandHash, batch->issuedAtMs});
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
    // The manifest carries the producer's data/log fence. It is one above the
    // compacted Raft index: entry N remains in the suffix when it could be only
    // partly flushed, while the fence may move beyond the last point revision
    // across an applied prefix represented entirely by durable destructive
    // state. Bind those independently checksummed envelopes here. Without this
    // check, a valid payload paired with the wrong Raft snapshot metadata could
    // install state ahead of (or unrelated to) the log prefix being discarded.
    if (snap.index == UINT64_MAX || payload->manifest.snapshotRevision != snap.index + 1)
        throw std::runtime_error(
            "EngineDataStateMachine::applySnapshot: data revision does not match Raft snapshot boundary "
            "(fail-stop)");
    data::DeleteReceiptSnapshotState deleteState{payload->deleteReceiptsRetiredBeforeMs,
                                                 payload->deleteReceiptsRetiredAtIndex,
                                                 std::move(payload->deleteReceipts)};
    const bool installed = co_await store_.installVShardSnapshot(vshard_, std::move(*payload));
    if (!installed)
        throw std::runtime_error(
            "EngineDataStateMachine::applySnapshot: snapshot failed verification and was not installed (fail-stop)");
    restoreDeleteReceiptState(std::move(deleteState), snap.index);
    // The snapshot subsumes the log up to snap.index; advance the applied watermark so a
    // subsequent apply() of the post-snapshot suffix is correctly ordered.
    appliedIndex_ = snap.index;
    // Everything this counter was measuring is now inside the snapshot (D-6).
    appliedBytesSinceSnapshot_ = 0;
    co_return;
}

data::DeleteReceiptSnapshotState EngineDataStateMachine::deleteReceiptStateThrough(uint64_t snapshotIndex) const {
    if (!canSnapshotDeleteReceiptStateThrough(snapshotIndex))
        throw std::logic_error("EngineDataStateMachine: snapshot boundary precedes retained delete-state retirement");
    data::DeleteReceiptSnapshotState state;
    state.receipts.reserve(deleteReceipts_.size());
    for (const auto& item : deleteReceipts_) {
        const auto& receipt = item.second;
        if (receipt.appliedIndex <= snapshotIndex)
            state.receipts.push_back(receipt);
    }
    if (deleteReceiptsRetiredAtIndex_ != 0 && deleteReceiptsRetiredAtIndex_ <= snapshotIndex) {
        state.retiredBeforeMs = deleteReceiptsRetiredBeforeMs_;
        state.retiredAtIndex = deleteReceiptsRetiredAtIndex_;
    }
    return state;
}

bool EngineDataStateMachine::canSnapshotDeleteReceiptStateThrough(uint64_t snapshotIndex) const {
    return deleteReceiptsRetiredAtIndex_ == 0 || deleteReceiptsRetiredAtIndex_ <= snapshotIndex;
}

void EngineDataStateMachine::restoreDeleteReceiptState(data::DeleteReceiptSnapshotState state, uint64_t snapshotIndex) {
    if ((state.retiredBeforeMs == 0) != (state.retiredAtIndex == 0) || state.retiredAtIndex > snapshotIndex ||
        state.receipts.size() > maxDeleteReceipts_)
        throw std::runtime_error("EngineDataStateMachine: invalid snapshot delete receipt state (fail-stop)");
    std::map<SeriesId128, data::DeleteOperationReceipt> replacement;
    for (auto& receipt : state.receipts) {
        if (receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 || receipt.appliedIndex > snapshotIndex ||
            (receipt.issuedAtMs != 0 && receipt.issuedAtMs <= state.retiredBeforeMs) ||
            !replacement.emplace(receipt.operationId, receipt).second)
            throw std::runtime_error("EngineDataStateMachine: invalid snapshot delete receipt (fail-stop)");
    }
    deleteReceipts_ = std::move(replacement);
    deleteReceiptsRetiredBeforeMs_ = state.retiredBeforeMs;
    deleteReceiptsRetiredAtIndex_ = state.retiredAtIndex;
}

void EngineDataStateMachine::checkDeleteAdmission(const data::DeleteRangeBatch& command) const {
    const uint64_t commandHash = data::deleteRangeCommandHash(command);
    if (const auto found = deleteReceipts_.find(command.operationId); found != deleteReceipts_.end()) {
        if (found->second.commandHash != commandHash)
            throw std::invalid_argument("delete operation ID is already bound to another command");
        return;
    }
    if (command.issuedAtMs == 0) {
        if (deleteReceipts_.size() >= maxDeleteReceipts_ &&
            std::ranges::none_of(deleteReceipts_, [](const auto& item) { return item.second.issuedAtMs != 0; }))
            throw data::WriteOverloadedError("legacy delete receipt capacity is exhausted for this VShard");
        return;
    }
    const uint64_t timeFloor =
        command.issuedAtMs > data::kDeleteReceiptRetentionMs ? command.issuedAtMs - data::kDeleteReceiptRetentionMs : 0;
    const uint64_t prospectiveFloor = std::max(deleteReceiptsRetiredBeforeMs_, timeFloor);
    if (command.issuedAtMs <= prospectiveFloor)
        throw data::DeleteReceiptExpiredError("delete idempotency receipt is outside the retained VShard window");

    size_t receiptsAfterTimeRetirement = 0;
    uint64_t oldestModernReceipt = UINT64_MAX;
    for (const auto& item : deleteReceipts_) {
        const auto& receipt = item.second;
        if (receipt.issuedAtMs == 0 || receipt.issuedAtMs > prospectiveFloor) {
            ++receiptsAfterTimeRetirement;
            if (receipt.issuedAtMs != 0)
                oldestModernReceipt = std::min(oldestModernReceipt, receipt.issuedAtMs);
        }
    }
    if (receiptsAfterTimeRetirement < maxDeleteReceipts_)
        return;
    if (oldestModernReceipt == UINT64_MAX)
        throw data::WriteOverloadedError("legacy delete receipts consume this VShard's bounded capacity");
    if (command.issuedAtMs <= oldestModernReceipt)
        throw data::DeleteReceiptExpiredError("delete idempotency receipt is outside the retained VShard capacity");
}

EngineDataStateMachine::DeleteReceiptStatus EngineDataStateMachine::deleteReceiptStatus(
    const data::DeleteRangeBatch& command) const {
    const uint64_t commandHash = data::deleteRangeCommandHash(command);
    if (const auto found = deleteReceipts_.find(command.operationId); found != deleteReceipts_.end())
        return found->second.commandHash == commandHash ? DeleteReceiptStatus::Retained : DeleteReceiptStatus::Missing;
    if (command.issuedAtMs != 0 && command.issuedAtMs <= deleteReceiptsRetiredBeforeMs_)
        return DeleteReceiptStatus::Expired;
    return DeleteReceiptStatus::Missing;
}

EngineDataStateMachine::DeleteReceiptCounts EngineDataStateMachine::deleteReceiptCounts() const {
    DeleteReceiptCounts counts{0, deleteReceipts_.size()};
    for (const auto& item : deleteReceipts_)
        if (item.second.issuedAtMs == 0)
            ++counts.legacy;
    return counts;
}

void EngineDataStateMachine::advanceDeleteReceiptFloor(uint64_t floorMs, uint64_t appliedIndex) {
    if (floorMs <= deleteReceiptsRetiredBeforeMs_)
        return;
    deleteReceiptsRetiredBeforeMs_ = floorMs;
    deleteReceiptsRetiredAtIndex_ = appliedIndex;
    std::erase_if(deleteReceipts_, [floorMs](const auto& item) {
        return item.second.issuedAtMs != 0 && item.second.issuedAtMs <= floorMs;
    });
}

void EngineDataStateMachine::makeRoomForDeleteReceipt(uint64_t appliedIndex) {
    if (deleteReceipts_.size() < maxDeleteReceipts_)
        return;
    uint64_t oldestModernReceipt = UINT64_MAX;
    for (const auto& item : deleteReceipts_) {
        const auto& receipt = item.second;
        if (receipt.issuedAtMs != 0)
            oldestModernReceipt = std::min(oldestModernReceipt, receipt.issuedAtMs);
    }
    if (oldestModernReceipt == UINT64_MAX)
        throw std::runtime_error(
            "EngineDataStateMachine: legacy delete receipts consume the configured hard cap (fail-stop)");
    advanceDeleteReceiptFloor(oldestModernReceipt, appliedIndex);
}

}  // namespace timestar::cluster
