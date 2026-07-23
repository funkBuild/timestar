#include "vshard_extent_map.hpp"

#include "byte_codec.hpp"
#include "crc32.hpp"

#include <span>

namespace timestar {

namespace {
constexpr uint32_t kMagic = 0x56584d50;  // "VXMP"
constexpr uint32_t kFormatVersion = 1;
constexpr size_t kHeaderBytes = 12;            // magic + version + count
constexpr size_t kEntryBytes = 2 + 8 + 8 + 8;  // vshard u16, fileId u64, minRev u64, maxRev u64
constexpr size_t kCrcBytes = 4;
}  // namespace

void VShardExtentMap::add(VShardId vshard, const VShardExtent& extent) {
    RevisionRange& range = byVShard_[vshard.value()][extent.fileId];
    range.merge(extent.revRange);  // same (vshard, file) added twice -> union
}

std::vector<VShardExtent> VShardExtentMap::extents(VShardId vshard) const {
    std::vector<VShardExtent> out;
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return out;
    out.reserve(it->second.size());
    for (const auto& [fileId, range] : it->second)  // std::map -> ascending fileId
        out.push_back(VShardExtent{fileId, range});
    return out;
}

RevisionRange VShardExtentMap::revRange(VShardId vshard) const {
    RevisionRange range;
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return range;
    for (const auto& [fileId, r] : it->second)
        range.merge(r);
    return range;
}

std::vector<VShardId> VShardExtentMap::vshards() const {
    std::vector<VShardId> out;
    out.reserve(byVShard_.size());
    for (const auto& [vs, extents] : byVShard_)  // ascending
        out.push_back(VShardId{vs});
    return out;
}

size_t VShardExtentMap::countExtents() const {
    size_t n = 0;
    for (const auto& [vs, extents] : byVShard_)
        n += extents.size();
    return n;
}

std::string VShardExtentMap::encode() const {
    const size_t entries = countExtents();
    std::string out;
    out.reserve(kHeaderBytes + entries * kEntryBytes + kCrcBytes);
    codec::putU32(out, kMagic);
    codec::putU32(out, kFormatVersion);
    codec::putU32(out, static_cast<uint32_t>(entries));
    for (const auto& [vs, extents] : byVShard_) {      // ascending vshard
        for (const auto& [fileId, range] : extents) {  // ascending fileId
            codec::putU16(out, vs);
            codec::putU64(out, fileId);
            // An empty range serialises as [0,0] and decodes back to [0,0] (not
            // empty); a VShard extent always has some concrete range in practice.
            codec::putU64(out, range.empty ? 0 : range.minRev);
            codec::putU64(out, range.empty ? 0 : range.maxRev);
        }
    }
    const uint32_t crc = CRC32::compute(out.data(), out.size());
    codec::putU32(out, crc);
    return out;
}

std::optional<VShardExtentMap> VShardExtentMap::decode(std::string_view bytes) {
    if (bytes.size() < kHeaderBytes + kCrcBytes)
        return std::nullopt;

    const size_t bodyLen = bytes.size() - kCrcBytes;
    codec::Reader crcReader(std::span<const char>(bytes.data() + bodyLen, kCrcBytes));
    const uint32_t storedCrc = crcReader.u32();
    if (!crcReader.ok() || CRC32::compute(bytes.data(), bodyLen) != storedCrc)
        return std::nullopt;

    codec::Reader reader(std::span<const char>(bytes.data(), bodyLen));
    const uint32_t magic = reader.u32();
    const uint32_t version = reader.u32();
    const uint32_t count = reader.u32();
    if (!reader.ok() || magic != kMagic || version != kFormatVersion)
        return std::nullopt;
    if (bodyLen != kHeaderBytes + static_cast<size_t>(count) * kEntryBytes)
        return std::nullopt;

    VShardExtentMap map;
    bool havePrev = false;
    uint16_t prevVs = 0;
    uint64_t prevFile = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const uint16_t vs = reader.u16();
        const uint64_t fileId = reader.u64();
        const uint64_t minRev = reader.u64();
        const uint64_t maxRev = reader.u64();
        if (!reader.ok())
            return std::nullopt;
        if (vs >= VIRTUAL_SHARD_COUNT)
            return std::nullopt;
        if (maxRev < minRev)
            return std::nullopt;  // malformed range
        // Enforce the canonical ascending (vshard, fileId) order so a corrupt or
        // non-canonical stream is rejected rather than silently reordered.
        if (havePrev && (vs < prevVs || (vs == prevVs && fileId <= prevFile)))
            return std::nullopt;
        havePrev = true;
        prevVs = vs;
        prevFile = fileId;
        map.byVShard_[vs][fileId] = RevisionRange{minRev, maxRev, /*empty=*/false};
    }
    if (!reader.exhausted())
        return std::nullopt;
    return map;
}

}  // namespace timestar
