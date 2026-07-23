#include "vshard_manifest.hpp"

#include "byte_codec.hpp"
#include "crc32.hpp"

#include <algorithm>
#include <span>

namespace timestar {

namespace {
constexpr uint32_t kMagic = 0x56534d46;  // "VSMF"
constexpr uint32_t kFormatVersion = 1;
// magic + version + count, then entries, then a trailing CRC over all of it.
constexpr size_t kHeaderBytes = 12;
constexpr size_t kEntryBytes = 2 + 8 + 8;  // vshard u16, appliedSeq u64, releasedSeq u64
constexpr size_t kCrcBytes = 4;

uint64_t clampReleased(uint64_t releasedRaw, uint64_t applied) {
    return std::min(releasedRaw, applied);
}
}  // namespace

void VShardManifest::setApplied(VShardId vshard, uint64_t appliedSeq) {
    auto& raw = byVShard_[vshard.value()];
    raw.appliedSeq = std::max(raw.appliedSeq, appliedSeq);
}

void VShardManifest::setReleased(VShardId vshard, uint64_t releasedSeq) {
    auto& raw = byVShard_[vshard.value()];
    raw.releasedSeqRaw = std::max(raw.releasedSeqRaw, releasedSeq);
}

VShardWatermarks VShardManifest::get(VShardId vshard) const {
    auto it = byVShard_.find(vshard.value());
    if (it == byVShard_.end())
        return {};
    return {it->second.appliedSeq, clampReleased(it->second.releasedSeqRaw, it->second.appliedSeq)};
}

std::string VShardManifest::encode() const {
    std::string out;
    out.reserve(kHeaderBytes + byVShard_.size() * kEntryBytes + kCrcBytes);
    codec::putU32(out, kMagic);
    codec::putU32(out, kFormatVersion);
    codec::putU32(out, static_cast<uint32_t>(byVShard_.size()));
    for (const auto& [vs, raw] : byVShard_) {  // std::map iterates in ascending key order
        codec::putU16(out, vs);
        codec::putU64(out, raw.appliedSeq);
        codec::putU64(out, clampReleased(raw.releasedSeqRaw, raw.appliedSeq));
    }
    const uint32_t crc = CRC32::compute(out.data(), out.size());
    codec::putU32(out, crc);
    return out;
}

std::optional<VShardManifest> VShardManifest::decode(std::string_view bytes) {
    if (bytes.size() < kHeaderBytes + kCrcBytes)
        return std::nullopt;

    // Verify the trailing CRC covers everything before it.
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
    // The body must be exactly the header plus `count` fixed-size entries.
    if (bodyLen != kHeaderBytes + static_cast<size_t>(count) * kEntryBytes)
        return std::nullopt;

    VShardManifest manifest;
    uint32_t prevVs = 0;
    bool havePrev = false;
    for (uint32_t i = 0; i < count; ++i) {
        const uint16_t vs = reader.u16();
        const uint64_t applied = reader.u64();
        const uint64_t released = reader.u64();
        if (!reader.ok())
            return std::nullopt;
        if (vs >= VIRTUAL_SHARD_COUNT)
            return std::nullopt;  // out-of-range VShard id
        if (havePrev && vs <= prevVs)
            return std::nullopt;  // unordered or duplicate entry -> not a canonical manifest
        if (released > applied)
            return std::nullopt;  // encode() always clamps; a violation is corruption
        prevVs = vs;
        havePrev = true;
        manifest.byVShard_[vs] = Raw{applied, released};
    }
    if (!reader.exhausted())
        return std::nullopt;
    return manifest;
}

}  // namespace timestar
