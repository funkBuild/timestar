#include "raft_journal_persistence.hpp"

#include "raft_config.hpp"

#include <algorithm>
#include <map>
#include <seastar/core/coroutine.hh>

namespace timestar::raft {

namespace {

void putU64(std::string& s, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        s.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

uint64_t getU64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i)
        v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
    return v;
}

// Snapshot payload: [index u64][term u64][configLen u32][config bytes][data...].
std::string encodeSnapshotPayload(const Snapshot& s) {
    std::string out;
    putU64(out, s.index);
    putU64(out, s.term);
    const std::string cfg = encodeConfig(s.config);
    putU64(out, cfg.size());  // length prefix (u64 for simplicity)
    out += cfg;
    out += s.data;
    return out;
}

}  // namespace

JournalRaftPersistence::JournalRaftPersistence(JournalWriter& writer, VShardId vshard, uint64_t startSeq)
    : writer_(writer), vshard_(vshard), nextSeq_(startSeq == 0 ? 1 : startSeq) {}

seastar::future<> JournalRaftPersistence::persistHardState(HardState hs) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    r.kind = JournalRecordKind::HardState;
    r.raftTerm = hs.currentTerm;
    r.payload.reserve(8);
    putU64(r.payload, hs.votedFor);
    return writer_.append(r);
}

seastar::future<> JournalRaftPersistence::persistEntries(std::vector<LogEntry> entries) {
    for (const auto& e : entries) {
        JournalRecord r;
        r.vshard = vshard_;
        r.vshardSeq = nextSeq_++;
        r.kind = (e.type == EntryType::ConfigChange) ? JournalRecordKind::Config : JournalRecordKind::Data;
        r.raftTerm = e.term;
        r.raftIndex = e.index;
        r.payload = e.data;
        co_await writer_.append(r);
    }
}

seastar::future<> JournalRaftPersistence::persistSnapshot(Snapshot snap) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    r.kind = JournalRecordKind::Snapshot;
    r.raftTerm = snap.term;
    r.raftIndex = snap.index;
    r.payload = encodeSnapshotPayload(snap);
    return writer_.append(r);
}

seastar::future<> JournalRaftPersistence::sync() {
    return writer_.barrier();
}

RecoveredRaftState recoverRaftState(const std::vector<JournalRecord>& records, VShardId vshard) {
    // Collect this VShard's records and replay them in vshard_seq order (their
    // true append order, independent of physical position in the shared stream).
    std::vector<const JournalRecord*> mine;
    for (const auto& r : records)
        if (r.vshard.value() == vshard.value())
            mine.push_back(&r);
    std::sort(mine.begin(), mine.end(),
              [](const JournalRecord* a, const JournalRecord* b) { return a->vshardSeq < b->vshardSeq; });

    RecoveredRaftState out;
    std::map<LogIndex, LogEntry> entries;  // ordered by index; a re-append overwrites
    LogIndex snapIndex = kNoIndex;
    Term snapTerm = kNoTerm;

    for (const JournalRecord* r : mine) {
        out.nextSeq = r->vshardSeq + 1;
        switch (r->kind) {
            case JournalRecordKind::HardState:
                out.hardState.currentTerm = r->raftTerm;
                out.hardState.votedFor = (r->payload.size() >= 8) ? getU64(r->payload.data()) : kNoNode;
                break;
            case JournalRecordKind::Data:
            case JournalRecordKind::Config: {
                LogEntry e;
                e.term = r->raftTerm;
                e.index = r->raftIndex;
                e.type = (r->kind == JournalRecordKind::Config) ? EntryType::ConfigChange
                                                                : EntryType::Normal;
                e.data = r->payload;
                // Drop any entries above this index that a prior append left; a
                // lower-index re-append means the higher suffix was superseded.
                entries.erase(entries.upper_bound(r->raftIndex), entries.end());
                entries[r->raftIndex] = std::move(e);
                break;
            }
            case JournalRecordKind::Truncation: {
                // payload = the index to truncate from (inclusive).
                if (r->payload.size() >= 8) {
                    const LogIndex from = getU64(r->payload.data());
                    entries.erase(entries.lower_bound(from), entries.end());
                }
                break;
            }
            case JournalRecordKind::Snapshot:
                snapIndex = r->raftIndex;
                snapTerm = r->raftTerm;
                // Entries at or below the snapshot boundary are compacted away.
                entries.erase(entries.begin(), entries.upper_bound(snapIndex));
                break;
            default:
                break;  // CatalogCreate/Retention are not Raft state
        }
    }

    // Rebuild the log: start from the snapshot boundary (if any), then append the
    // surviving entries in index order.
    if (snapIndex != kNoIndex)
        out.log.restoreFromSnapshot(snapIndex, snapTerm);
    std::vector<LogEntry> ordered;
    ordered.reserve(entries.size());
    for (auto& [idx, e] : entries)
        ordered.push_back(std::move(e));
    if (!ordered.empty())
        out.log.append(std::move(ordered));
    return out;
}

}  // namespace timestar::raft
