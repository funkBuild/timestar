#include "replicated_command.hpp"

#include "journal_format.hpp"  // JournalFormatGate (debt D-7)

#include <cstring>

namespace timestar::data {

namespace {

uint64_t fnv1a(const char* p, size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < n; ++i) {
        h ^= static_cast<uint8_t>(p[i]);
        h *= 1099511628211ull;
    }
    return h;
}

void putU32(std::string& out, uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}
void putU64(std::string& out, uint64_t v) {
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}
void putStr(std::string& out, const std::string& s) {
    putU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}

struct Reader {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint8_t u8() {
        if (!ok || !avail(1)) {
            ok = false;
            return 0;
        }
        return static_cast<uint8_t>(*p++);
    }
    uint32_t u32() {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(u8()) << (8 * i);
        return v;
    }
    uint64_t u64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(u8()) << (8 * i);
        return v;
    }
    std::string str() {
        uint32_t n = u32();
        if (!ok || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, n);
        p += n;
        return s;
    }
};

constexpr uint8_t kWrite = 0;
constexpr uint8_t kDelete = 1;
constexpr uint8_t kRetention = 2;

}  // namespace

std::string encodeWriteCommand(const WriteBatch& batch) {
    std::string out;
    out.push_back(static_cast<char>(kWrite));
    putStr(out, encodeWriteBatch(batch, JournalFormatGate::writeBatchFormat()));  // see encodeReplicatedCommand
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<OversizeSlice> firstUnproposableSlice(const VShardBatchView& view, size_t bound) {
    for (const auto* g : view) {
        const size_t n = maxEncodedWriteCommandBytes(g->second);
        if (n > bound)
            return OversizeSlice{g->first, n};
    }
    return std::nullopt;
}

std::string encodeReplicatedCommand(const ReplicatedCommand& cmd) {
    std::string out;
    if (const auto* w = std::get_if<WriteBatch>(&cmd)) {
        out.push_back(static_cast<char>(kWrite));
        // WriteBatch arm reuses the tested encoder as a length-prefixed sub-blob
        // (it carries its own FNV; the outer trailer covers the tag + blob).
        //
        // The version comes from the CLUSTER-WIDE JOURNAL GATE, never from the encoder's
        // default (debt D-7). These bytes become a Raft log entry: replicated to every
        // voter and written to each one's journal, so the format must be one every voter
        // -- and every binary that may later replay that journal -- can read. A voter takes
        // no part in the pairwise data-plane handshake, so per-peer negotiation cannot gate
        // it; the gate is group-0's COMMITTED format activation, which the controller
        // proposes only once FeatureGate::canActivate confirms every voter supports the
        // version. It defaults to v1 and only ever rises, so a node that has heard no
        // activation emits v1 -- fail closed. See data/journal_format.hpp for the full
        // ordering argument (including why "an old binary reads a v2 journal" is
        // unreachable rather than untested).
        //
        // Naming the gate here rather than a literal is also what keeps a change to
        // encodeWriteBatch's DEFAULT from silently promoting the journal format
        // (docs/write-scaleout-plan.md §6).
        putStr(out, encodeWriteBatch(*w, JournalFormatGate::writeBatchFormat()));
    } else if (const auto* d = std::get_if<DeleteRangeKey>(&cmd)) {
        out.push_back(static_cast<char>(kDelete));
        putStr(out, d->seriesKey);
        putU64(out, d->startTime);
        putU64(out, d->endTime);
    } else {
        const auto& r = std::get<RetentionCutoffCmd>(cmd);
        out.push_back(static_cast<char>(kRetention));
        putU64(out, r.cutoffTime);
    }
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<ReplicatedCommand> decodeReplicatedCommand(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    Reader r{bytes.data(), bytes.data() + bodyLen};
    uint8_t tag = r.u8();
    if (!r.ok)
        return std::nullopt;
    ReplicatedCommand cmd;
    if (tag == kWrite) {
        std::string blob = r.str();
        if (!r.ok)
            return std::nullopt;
        auto batch = decodeWriteBatch(blob);
        if (!batch)
            return std::nullopt;
        cmd = std::move(*batch);
    } else if (tag == kDelete) {
        DeleteRangeKey d;
        d.seriesKey = r.str();
        d.startTime = r.u64();
        d.endTime = r.u64();
        if (!r.ok)
            return std::nullopt;
        cmd = std::move(d);
    } else if (tag == kRetention) {
        RetentionCutoffCmd rc;
        rc.cutoffTime = r.u64();
        if (!r.ok)
            return std::nullopt;
        cmd = rc;
    } else {
        return std::nullopt;  // unknown kind
    }
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return cmd;
}

}  // namespace timestar::data
