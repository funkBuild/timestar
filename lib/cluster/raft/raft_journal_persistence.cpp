#include "raft_journal_persistence.hpp"

#include "raft_config.hpp"

#include <algorithm>
#include <cstring>
#include <map>
#include <seastar/core/coroutine.hh>
#include <stdexcept>

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

// Snapshot payload, v2: [magic 8][flags u8][index u64][term u64][configLen u64][config][data]
// Legacy (pre-provenance): [index u64][term u64][configLen u64][config][data]
//
// The magic prefix is what lets the two be told apart, and a prefix rather than a trailer
// because `data` is the unbounded tail. Legacy records read as produced-here -- the
// pre-D-6 shape, and there are none in the wild since nothing ever compacted before it.
constexpr char kSnapshotRecordMagic[8] = {'T', 'S', 'R', 'S', 'N', 'A', 'P', '1'};
constexpr uint8_t kSnapshotFlagReceivedFromPeer = 0x01;

std::string encodeSnapshotPayload(const Snapshot& s, bool receivedFromPeer) {
    std::string out;
    out.append(kSnapshotRecordMagic, sizeof(kSnapshotRecordMagic));
    out.push_back(static_cast<char>(receivedFromPeer ? kSnapshotFlagReceivedFromPeer : 0));
    putU64(out, s.index);
    putU64(out, s.term);
    const std::string cfg = encodeConfig(s.config);
    putU64(out, cfg.size());  // length prefix (u64 for simplicity)
    out += cfg;
    out += s.data;
    return out;
}

// Inverse of encodeSnapshotPayload. Returns nullopt on any malformed/truncated
// payload (fail closed). `index`/`term` are taken from the record header by the
// caller and cross-checked against the payload.
std::optional<Snapshot> decodeSnapshotPayload(const std::string& p, bool* receivedFromPeer) {
    size_t off = 0;
    if (receivedFromPeer)
        *receivedFromPeer = false;
    if (p.size() >= sizeof(kSnapshotRecordMagic) &&
        std::memcmp(p.data(), kSnapshotRecordMagic, sizeof(kSnapshotRecordMagic)) == 0) {
        off = sizeof(kSnapshotRecordMagic);
        if (p.size() < off + 1)
            return std::nullopt;
        const uint8_t flags = static_cast<uint8_t>(p[off]);
        ++off;
        if (receivedFromPeer)
            *receivedFromPeer = (flags & kSnapshotFlagReceivedFromPeer) != 0;
    }
    if (p.size() < off + 24)
        return std::nullopt;  // index + term + configLen
    Snapshot s;
    s.index = getU64(p.data() + off);
    s.term = getU64(p.data() + off + 8);
    const uint64_t cfgLen = getU64(p.data() + off + 16);
    if (off + 24 + cfgLen > p.size())
        return std::nullopt;
    s.config = decodeConfig(p.substr(off + 24, cfgLen));
    s.data = p.substr(off + 24 + cfgLen);
    return s;
}

}  // namespace

JournalRaftPersistence::JournalRaftPersistence(JournalWriter& writer, VShardId vshard, uint64_t startSeq)
    : owned_(std::in_place, writer), sink_(*owned_), vshard_(vshard), nextSeq_(startSeq == 0 ? 1 : startSeq) {}

JournalRaftPersistence::JournalRaftPersistence(JournalSink& sink, VShardId vshard, uint64_t startSeq)
    : sink_(sink), vshard_(vshard), nextSeq_(startSeq == 0 ? 1 : startSeq) {}

seastar::future<> JournalRaftPersistence::appendFenced(const JournalRecord& r) {
    // A failed append means the bookkeeping above already counted a record that is not in
    // the buffer, so no later sync may promote a sample derived from it. Freeze the floor
    // (debt D-34); the writer fences itself for the durability half.
    try {
        co_await sink_.append(r);
    } catch (...) {
        fenced_ = true;
        throw;
    }
}

uint64_t JournalRaftPersistence::intendedFloor() const {
    // Nothing is releasable until a snapshot exists: without one the whole log is
    // live, and its very first record is the log's own beginning.
    if (lastSnapshotSeq_ == 0)
        return 0;
    uint64_t floor = lastSnapshotSeq_;
    if (lastHardStateSeq_ != 0)
        floor = std::min(floor, lastHardStateSeq_);
    if (!entrySeqs_.empty())
        floor = std::min(floor, entrySeqs_.front().second);
    return floor - 1;  // floor >= 1 here (seqs start at 1), so this cannot underflow
}

void JournalRaftPersistence::seedRetention(JournalRetentionSeed seed) {
    lastHardStateSeq_ = seed.latestHardStateSeq;
    lastSnapshotSeq_ = seed.latestSnapshotSeq;
    entrySeqs_.assign(seed.entrySeqs.begin(), seed.entrySeqs.end());
    // Recovered records ARE durable -- they were read back off the disk -- so the seeded
    // floor is immediately reportable, without waiting for this incarnation to sync.
    durableFloor_ = std::max(durableFloor_, intendedFloor());
}

seastar::future<> JournalRaftPersistence::persistHardState(HardState hs) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    lastHardStateSeq_ = r.vshardSeq;  // the newest HardState record pins the floor (D-34)
    r.kind = JournalRecordKind::HardState;
    r.raftTerm = hs.currentTerm;
    r.payload.reserve(8);
    putU64(r.payload, hs.votedFor);
    co_await appendFenced(r);
}

// NOTE FOR A FUTURE EDITOR (debt D-34). `JournalRecordKind::Truncation` has NO producer
// in this tree -- a suffix truncation is expressed as a RE-APPEND at the lower index, and
// that is what the pop_back below mirrors. If anyone ever adds a real Truncation producer
// it MUST also drop entrySeqs_ at/above the truncation index, exactly as recoverRaftState's
// Truncation case erases `entrySeq`; forgetting to would leave dead records pinning the
// floor (harmless) or, if the mirror drifted the other way, release live ones (not).
seastar::future<> JournalRaftPersistence::persistEntries(std::vector<LogEntry> entries) {
    for (const auto& e : entries) {
        JournalRecord r;
        r.vshard = vshard_;
        r.vshardSeq = nextSeq_++;
        r.kind = (e.type == EntryType::ConfigChange) ? JournalRecordKind::Config : JournalRecordKind::Data;
        r.raftTerm = e.term;
        r.raftIndex = e.index;
        r.payload = e.data;
        // Mirror what recoverRaftState will rebuild (D-34): a re-append at index I
        // supersedes every record at or above I, so those records stop pinning the
        // floor. Keeps entrySeqs_ ascending in BOTH index and seq.
        while (!entrySeqs_.empty() && entrySeqs_.back().first >= e.index)
            entrySeqs_.pop_back();
        entrySeqs_.emplace_back(e.index, r.vshardSeq);
        co_await appendFenced(r);
    }
}

seastar::future<> JournalRaftPersistence::persistSnapshot(Snapshot snap, bool receivedFromPeer) {
    JournalRecord r;
    r.vshard = vshard_;
    r.vshardSeq = nextSeq_++;
    lastSnapshotSeq_ = r.vshardSeq;
    r.kind = JournalRecordKind::Snapshot;
    r.raftTerm = snap.term;
    r.raftIndex = snap.index;
    // Entries at or below the boundary are compacted away, so their records stop
    // pinning the reclaim floor (D-34). This is what usually lets the INTENDED floor
    // move; only a successful sync() promotes it into the reported one.
    while (!entrySeqs_.empty() && entrySeqs_.front().first <= snap.index)
        entrySeqs_.pop_front();
    r.payload = encodeSnapshotPayload(snap, receivedFromPeer);
    co_await appendFenced(r);
}

seastar::future<> JournalRaftPersistence::sync() {
    // SAMPLE THE FLOOR BEFORE THE BARRIER, PROMOTE IT AFTER (debt D-34). At this point
    // every append of this Ready has been awaited, so the records the sample depends on
    // are already in the writer's buffer and barrier() -- a whole-buffer flush -- is
    // guaranteed to cover them. Anything appended AFTER this line lands in a later
    // sample, which is the conservative direction.
    const uint64_t candidate = intendedFloor();
    if (fenced_)
        throw std::runtime_error("JournalRaftPersistence: journal fenced; the reclaim floor is frozen");
    try {
        co_await sink_.sync();
    } catch (...) {
        // The sync did NOT happen. Freeze the floor permanently rather than let a later
        // success promote a sample that assumed these records landed. The writer fences
        // itself too, so every subsequent append/sync throws anyway -- this makes the
        // floor's own invariant independent of that.
        fenced_ = true;
        throw;
    }
    durableFloor_ = std::max(durableFloor_, candidate);
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
    // Parallel to `entries` in EVERY mutation below: the vshard_seq of the record that
    // put each surviving entry there. It is what seeds the reclaim floor (D-34), and it
    // must be derived here rather than guessed later -- a fresh JournalRaftPersistence
    // has no idea which seqs the records already on disk carry.
    std::map<LogIndex, uint64_t> entrySeq;
    LogIndex snapIndex = kNoIndex;
    Term snapTerm = kNoTerm;

    for (const JournalRecord* r : mine) {
        out.nextSeq = r->vshardSeq + 1;
        switch (r->kind) {
            case JournalRecordKind::HardState:
                out.hardState.currentTerm = r->raftTerm;
                out.hardState.votedFor = (r->payload.size() >= 8) ? getU64(r->payload.data()) : kNoNode;
                out.retention.latestHardStateSeq = r->vshardSeq;
                break;
            case JournalRecordKind::Data:
            case JournalRecordKind::Config: {
                LogEntry e;
                e.term = r->raftTerm;
                e.index = r->raftIndex;
                e.type = (r->kind == JournalRecordKind::Config) ? EntryType::ConfigChange : EntryType::Normal;
                e.data = r->payload;
                // Drop any entries above this index that a prior append left; a
                // lower-index re-append means the higher suffix was superseded.
                entries.erase(entries.upper_bound(r->raftIndex), entries.end());
                entrySeq.erase(entrySeq.upper_bound(r->raftIndex), entrySeq.end());
                entries[r->raftIndex] = std::move(e);
                entrySeq[r->raftIndex] = r->vshardSeq;
                break;
            }
            case JournalRecordKind::Truncation: {
                // payload = the index to truncate from (inclusive).
                if (r->payload.size() >= 8) {
                    const LogIndex from = getU64(r->payload.data());
                    entries.erase(entries.lower_bound(from), entries.end());
                    entrySeq.erase(entrySeq.lower_bound(from), entrySeq.end());
                }
                break;
            }
            case JournalRecordKind::Snapshot: {
                snapIndex = r->raftIndex;
                snapTerm = r->raftTerm;
                out.retention.latestSnapshotSeq = r->vshardSeq;
                // Decode the full snapshot (config + state-machine data), not just
                // the header -- the boundary membership and applied state live
                // only here once their entries are compacted away.
                bool fromPeer = false;
                if (auto snap = decodeSnapshotPayload(r->payload, &fromPeer)) {
                    out.snapshot = std::move(*snap);
                    out.snapshotFromPeer = fromPeer;
                } else {
                    Snapshot s;  // fall back to header-only if the payload is bad
                    s.index = snapIndex;
                    s.term = snapTerm;
                    out.snapshot = std::move(s);
                }
                // Entries at or below the snapshot boundary are compacted away.
                entries.erase(entries.begin(), entries.upper_bound(snapIndex));
                entrySeq.erase(entrySeq.begin(), entrySeq.upper_bound(snapIndex));
                break;
            }
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
    out.retention.entrySeqs.reserve(entrySeq.size());
    for (const auto& [idx, seq] : entrySeq)
        out.retention.entrySeqs.emplace_back(idx, seq);
    return out;
}

}  // namespace timestar::raft
