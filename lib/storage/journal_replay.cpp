#include "journal_replay.hpp"

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

    if (!record.vshard.valid()) {
        fail("out-of-range VShard id in journal record");
        return false;
    }

    const uint16_t vs = record.vshard.value();
    auto it = lastSeqByVShard_.find(vs);
    if (it == lastSeqByVShard_.end()) {
        // First record for this VShard in the retained stream establishes the
        // baseline (the retained log may start mid-sequence after GC).
        lastSeqByVShard_.emplace(vs, record.vshardSeq);
    } else {
        // Every subsequent record must be the gap-free successor; a gap or a
        // regression is loss/corruption and fails the whole replay closed.
        if (record.vshardSeq != it->second + 1) {
            fail("VShard " + std::to_string(vs) + " sequence discontinuity: expected " +
                 std::to_string(it->second + 1) + " but saw " + std::to_string(record.vshardSeq));
            return false;
        }
        it->second = record.vshardSeq;
    }

    byCore_[ownerCore(record.vshard)].push_back(record);
    return true;
}

const std::vector<JournalRecord>& JournalReplay::recordsForCore(unsigned core) const {
    static const std::vector<JournalRecord> empty;
    if (core >= byCore_.size())
        return empty;
    return byCore_[core];
}

}  // namespace timestar
