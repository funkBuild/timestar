#include "data_state_machine.hpp"

#include <cstring>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <utility>

namespace timestar::data {

namespace {

struct SW {
    std::string out;
    void u32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
    }
    void dbl(double v) {
        uint64_t b;
        std::memcpy(&b, &v, 8);
        u64(b);
    }
    void series(const SeriesId128& id) { id.appendTo(out); }
};

struct SR {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint32_t u32() {
        if (!avail(4)) {
            ok = false;
            return 0;
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(static_cast<uint8_t>(*p++)) << (8 * i);
        return v;
    }
    uint64_t u64() {
        if (!avail(8)) {
            ok = false;
            return 0;
        }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(static_cast<uint8_t>(*p++)) << (8 * i);
        return v;
    }
    double dbl() {
        uint64_t b = u64();
        double v;
        std::memcpy(&v, &b, 8);
        return v;
    }
    SeriesId128 series() {
        if (!avail(16)) {
            ok = false;
            return {};
        }
        SeriesId128 id = SeriesId128::fromBytes(std::string_view(p, 16));
        p += 16;
        return id;
    }
};

// Per-series cell/tombstone counts are bounded by the remaining byte budget so a
// corrupt snapshot count can never drive an over-allocation.
constexpr uint64_t kCellBytes = 24;  // ts(8)+value(8)+rev(8)
constexpr uint64_t kTombBytes = 24;  // start(8)+end(8)+rev(8)

}  // namespace

void ReplicatedVShardStore::applyWritePoints(const WritePoints& wp) {
    // Each point gets the next monotonic revision, in the batch's own order, so
    // LWW is deterministic and a later log entry always wins over an earlier one.
    for (const auto& pt : wp.points) {
        const uint64_t rev = ++revCounter_;
        points_[pt.series][pt.timestamp] = Cell{pt.value, rev};
    }
}

void ReplicatedVShardStore::applyDeleteRange(const DeleteRange& dr) {
    const uint64_t rev = ++revCounter_;
    auto& toms = tombstones_[dr.series];
    // Coalesce identical ranges: only the MAX revision of a given [start,end]
    // range affects visibility (visible() deletes a point if ANY covering
    // tombstone out-revisions it), so a retried delete of the same range just
    // advances the existing tombstone's revision instead of growing the list
    // without bound under repeated leadership flaps. Distinct ranges stay
    // separate. Deterministic: every replica applies the same log in the same
    // order and coalesces identically. `rev` is monotonic so it is always the new
    // maximum for this range.
    for (auto& t : toms) {
        if (t.startTime == dr.startTime && t.endTime == dr.endTime) {
            t.revision = rev;
            return;
        }
    }
    toms.push_back(Tombstone{dr.startTime, dr.endTime, rev});
}

void ReplicatedVShardStore::applyRetentionCutoff(const RetentionCutoff& rc) {
    // Monotonic: the cutoff only ever advances, so re-applying an older one is a
    // no-op and every replica lands on the same value regardless of order.
    if (rc.cutoffTime > retentionCutoff_)
        retentionCutoff_ = rc.cutoffTime;
}

bool ReplicatedVShardStore::visible(const SeriesId128& series, uint64_t ts, uint64_t rev) const {
    if (ts < retentionCutoff_)
        return false;
    auto it = tombstones_.find(series);
    if (it == tombstones_.end())
        return true;
    for (const auto& t : it->second)
        if (t.revision > rev && ts >= t.startTime && ts <= t.endTime)
            return false;  // deleted by a strictly-later tombstone
    return true;
}

QueryPartial ReplicatedVShardStore::query(const QuerySpec& spec) const {
    QueryPartial part;
    auto scan = [&](const SeriesId128& series, const std::map<uint64_t, Cell>& cells) {
        for (const auto& [ts, cell] : cells) {
            if (ts < spec.startTime || ts > spec.endTime)
                continue;
            if (!visible(series, ts, cell.revision))
                continue;
            if (spec.method == AggMethod::Raw)
                part.raw.push_back(DataPoint{series, ts, cell.value});
            else
                part.perSeries[series].add(cell.value);
        }
    };
    if (spec.series.empty()) {
        for (const auto& [series, cells] : points_)
            scan(series, cells);
    } else {
        for (const auto& s : spec.series) {
            auto it = points_.find(s);
            if (it != points_.end())
                scan(s, it->second);
        }
    }
    return part;
}

std::string ReplicatedVShardStore::encode() const {
    SW w;
    w.u64(revCounter_);
    w.u64(retentionCutoff_);
    w.u32(static_cast<uint32_t>(points_.size()));
    for (const auto& [series, cells] : points_) {
        w.series(series);
        w.u32(static_cast<uint32_t>(cells.size()));
        for (const auto& [ts, cell] : cells) {
            w.u64(ts);
            w.dbl(cell.value);
            w.u64(cell.revision);
        }
    }
    w.u32(static_cast<uint32_t>(tombstones_.size()));
    for (const auto& [series, toms] : tombstones_) {
        w.series(series);
        w.u32(static_cast<uint32_t>(toms.size()));
        for (const auto& t : toms) {
            w.u64(t.startTime);
            w.u64(t.endTime);
            w.u64(t.revision);
        }
    }
    return std::move(w.out);
}

bool ReplicatedVShardStore::loadFrom(const std::string& bytes) {
    // Parse into locals; only commit on a fully-valid payload so a corrupt
    // snapshot leaves the store unchanged (fail closed, never half-applied).
    SR r{bytes.data(), bytes.data() + bytes.size()};
    uint64_t rev = r.u64();
    uint64_t cutoff = r.u64();
    std::map<SeriesId128, std::map<uint64_t, Cell>> pts;
    std::map<SeriesId128, std::vector<Tombstone>> toms;

    uint32_t ns = r.u32();
    if (!r.ok)
        return false;
    for (uint32_t i = 0; i < ns; ++i) {
        SeriesId128 s = r.series();
        uint32_t nc = r.u32();
        if (!r.ok || nc > static_cast<uint64_t>(r.end - r.p) / kCellBytes)
            return false;
        auto& cells = pts[s];
        for (uint32_t c = 0; c < nc; ++c) {
            uint64_t ts = r.u64();
            double v = r.dbl();
            uint64_t cr = r.u64();
            if (!r.ok)
                return false;
            cells[ts] = Cell{v, cr};
        }
    }
    uint32_t nts = r.u32();
    if (!r.ok)
        return false;
    for (uint32_t i = 0; i < nts; ++i) {
        SeriesId128 s = r.series();
        uint32_t nt = r.u32();
        if (!r.ok || nt > static_cast<uint64_t>(r.end - r.p) / kTombBytes)
            return false;
        auto& v = toms[s];
        v.reserve(nt);
        for (uint32_t t = 0; t < nt; ++t) {
            uint64_t st = r.u64();
            uint64_t en = r.u64();
            uint64_t tr = r.u64();
            if (!r.ok)
                return false;
            v.push_back(Tombstone{st, en, tr});
        }
    }
    if (r.p != r.end)
        return false;  // trailing garbage

    revCounter_ = rev;
    retentionCutoff_ = cutoff;
    points_ = std::move(pts);
    tombstones_ = std::move(toms);
    return true;
}

seastar::future<> DataStateMachine::apply(raft::LogEntry entry) {
    auto cmd = decodeDataCommand(entry.data);
    if (!cmd)
        // A committed entry that this replica cannot decode is FATAL: skipping it
        // would silently diverge this replica from the others. Fail-stop, exactly
        // as the group-0 state machine does for control commands.
        throw std::runtime_error("dataplane: undecodable committed data command");
    if (const auto* wp = std::get_if<WritePoints>(&*cmd))
        store_.applyWritePoints(*wp);
    else if (const auto* dr = std::get_if<DeleteRange>(&*cmd))
        store_.applyDeleteRange(*dr);
    else if (const auto* rc = std::get_if<RetentionCutoff>(&*cmd))
        store_.applyRetentionCutoff(*rc);
    appliedIndex_ = entry.index;
    return seastar::make_ready_future<>();
}

seastar::future<> DataStateMachine::applySnapshot(raft::Snapshot snap) {
    if (!store_.loadFrom(snap.data))
        throw std::runtime_error("dataplane: corrupt data snapshot");
    appliedIndex_ = snap.index;
    return seastar::make_ready_future<>();
}

}  // namespace timestar::data
