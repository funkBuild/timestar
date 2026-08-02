#include "replicated_command.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

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

constexpr char kCommandMagic[4] = {'T', 'S', 'C', '1'};
constexpr uint8_t kWrite = 0;
constexpr uint8_t kDeleteBatch = 1;
constexpr uint8_t kRetention = 2;
constexpr size_t kMinimumDeleteRangeTargetBytes = 4 + 8 + 8;

bool validDeleteRangeTargets(const std::vector<DeleteRangeTarget>& targets) {
    if (targets.empty() || targets.size() > kMaxDeleteRangeBatchTargets)
        return false;
    for (size_t i = 0; i < targets.size(); ++i) {
        if (targets[i].seriesKey.empty() || targets[i].startTime > targets[i].endTime ||
            (i != 0 && !(targets[i - 1] < targets[i])))
            return false;
    }
    return true;
}

}  // namespace

std::string encodeWriteCommand(const WriteBatch& batch) {
    std::string out(kCommandMagic, sizeof(kCommandMagic));
    out.push_back(static_cast<char>(kWrite));
    putStr(out, encodeWriteBatch(batch));
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<OversizeSlice> firstUnproposableSlice(const VShardBatchView& view, size_t bound) {
    for (const auto* g : view) {
        const size_t n = encodedWriteCommandBytes(g->second);
        if (n > bound)
            return OversizeSlice{g->first, n};
    }
    return std::nullopt;
}

std::string encodeReplicatedCommand(const ReplicatedCommand& cmd) {
    std::string out(kCommandMagic, sizeof(kCommandMagic));
    if (const auto* w = std::get_if<WriteBatch>(&cmd)) {
        out.push_back(static_cast<char>(kWrite));
        putStr(out, encodeWriteBatch(*w));
    } else if (const auto* batch = std::get_if<DeleteRangeBatch>(&cmd)) {
        if (batch->operationId == SeriesId128{} || batch->issuedAtMs == 0 ||
            !validDeleteRangeTargets(batch->targets))
            throw std::invalid_argument("encodeReplicatedCommand: invalid idempotent delete batch");
        out.push_back(static_cast<char>(kDeleteBatch));
        batch->operationId.appendTo(out);
        putU64(out, batch->issuedAtMs);
        putU32(out, static_cast<uint32_t>(batch->targets.size()));
        for (const auto& target : batch->targets) {
            putStr(out, target.seriesKey);
            putU64(out, target.startTime);
            putU64(out, target.endTime);
        }
    } else {
        const auto& r = std::get<RetentionCutoffCmd>(cmd);
        out.push_back(static_cast<char>(kRetention));
        putU64(out, r.cutoffTime);
    }
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<ReplicatedCommand> decodeReplicatedCommand(const std::string& bytes) {
    if (bytes.size() < sizeof(kCommandMagic) + 1 + 8)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    if (std::memcmp(bytes.data(), kCommandMagic, sizeof(kCommandMagic)) != 0)
        return std::nullopt;
    Reader r{bytes.data() + sizeof(kCommandMagic), bytes.data() + bodyLen};
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
    } else if (tag == kDeleteBatch) {
        DeleteRangeBatch d;
        if (!r.avail(16))
            return std::nullopt;
        d.operationId = SeriesId128::fromBytes(r.p, 16);
        r.p += 16;
        d.issuedAtMs = r.u64();
        if (!r.ok || d.issuedAtMs == 0)
            return std::nullopt;
        const uint32_t count = r.u32();
        if (!r.ok || d.operationId == SeriesId128{} || count == 0 || count > kMaxDeleteRangeBatchTargets ||
            count > static_cast<size_t>(r.end - r.p) / kMinimumDeleteRangeTargetBytes)
            return std::nullopt;
        d.targets.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            DeleteRangeTarget target;
            target.seriesKey = r.str();
            target.startTime = r.u64();
            target.endTime = r.u64();
            if (!r.ok)
                return std::nullopt;
            d.targets.push_back(std::move(target));
        }
        if (!validDeleteRangeTargets(d.targets))
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

uint64_t deleteRangeCommandHash(const DeleteRangeBatch& command) {
    std::string canonical;
    size_t bytes = 4 + 8;
    for (const auto& target : command.targets)
        bytes += 4 + target.seriesKey.size() + 16;
    canonical.reserve(bytes);
    putU64(canonical, command.issuedAtMs);
    putU32(canonical, static_cast<uint32_t>(command.targets.size()));
    for (const auto& target : command.targets) {
        putStr(canonical, target.seriesKey);
        putU64(canonical, target.startTime);
        putU64(canonical, target.endTime);
    }
    return fnv1a(canonical.data(), canonical.size());
}

}  // namespace timestar::data
