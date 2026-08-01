#include "journal_replay.hpp"

#include <algorithm>
#include <cstdint>
#include <stdexcept>

namespace timestar {

JournalReplay::JournalReplay(unsigned coreCount) : coreCount_(coreCount) {
    if (coreCount == 0)
        throw std::invalid_argument("JournalReplay: core count must be at least 1");
    byCore_.resize(coreCount);
}

void JournalReplay::fail(std::string detail) {
    failed_ = true;
    failureDetail_ = std::move(detail);
}

bool JournalReplay::ingest(const JournalRecord& record) {
    if (failed_)
        return false;
    if (finalized_)
        throw std::logic_error("JournalReplay::ingest after finalize");

    if (!record.vshard.valid()) {
        fail("out-of-range VShard id in journal record");
        return false;
    }

    buffered_[record.vshard.value()].push_back(record);
    return true;
}

bool JournalReplay::finalize() {
    if (failed_)
        return false;
    if (finalized_)
        return true;
    finalized_ = true;

    // Process VShards in ascending id order for a deterministic routed layout.
    std::vector<uint16_t> vshards;
    vshards.reserve(buffered_.size());
    for (const auto& [vs, records] : buffered_)
        vshards.push_back(vs);
    std::sort(vshards.begin(), vshards.end());

    for (uint16_t vs : vshards) {
        auto& records = buffered_[vs];
        // Order by sequence; physical order is not authoritative (copy-forward).
        std::stable_sort(records.begin(), records.end(),
                         [](const JournalRecord& a, const JournalRecord& b) { return a.vshardSeq < b.vshardSeq; });

        // Shared-journal GC may leave an old segment pinned by ANOTHER VShard while
        // deleting a newer, fully released one. That exposes holes only in records a
        // retained Snapshot has already superseded. Find the newest such boundary up
        // front so the continuity check below can distinguish an expected GC hole from
        // loss in the live post-snapshot suffix.
        uint64_t latestSnapshotSeq = 0;
        for (const JournalRecord& record : records)
            if (record.kind == JournalRecordKind::Snapshot)
                latestSnapshotSeq = std::max(latestSnapshotSeq, record.vshardSeq);

        auto& out = byCore_[ownerCore(VShardId{vs})];
        bool first = true;
        uint64_t prevSeq = 0;
        for (const JournalRecord& record : records) {
            if (!first && record.vshardSeq == prevSeq) {
                // Duplicate sequence. A copy-forward (or its crash-window twin) is
                // a byte-identical copy -> drop it. Anything else is corruption.
                if (record != out.back()) {
                    fail("VShard " + std::to_string(vs) + " conflicting duplicate at sequence " +
                         std::to_string(record.vshardSeq));
                    return false;
                }
                continue;  // exact duplicate: dedupe
            }
            if (!first) {
                // Guard the (physically unreachable) sequence-space top so prevSeq+1
                // cannot wrap; records are sorted and de-duplicated, so any next
                // record here is strictly greater than prevSeq.
                if (prevSeq == UINT64_MAX) {
                    fail("VShard " + std::to_string(vs) + " sequence space exhausted");
                    return false;
                }
                if (record.vshardSeq != prevSeq + 1 && record.vshardSeq > latestSnapshotSeq) {
                    fail("VShard " + std::to_string(vs) + " sequence discontinuity: expected " +
                         std::to_string(prevSeq + 1) + " but saw " + std::to_string(record.vshardSeq));
                    return false;
                }
            }
            out.push_back(record);
            prevSeq = record.vshardSeq;
            first = false;
        }
    }
    return true;
}

const std::vector<JournalRecord>& JournalReplay::recordsForCore(unsigned core) const {
    static const std::vector<JournalRecord> empty;
    if (failed_ || core >= byCore_.size())
        return empty;
    return byCore_[core];
}

}  // namespace timestar
