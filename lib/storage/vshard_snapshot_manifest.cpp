#include "vshard_snapshot_manifest.hpp"

#include "byte_codec.hpp"
#include "crc32.hpp"

#include <algorithm>
#include <cctype>
#include <span>

namespace timestar {

namespace {
constexpr uint32_t kMagic = 0x56534e50;  // "VSNP"
constexpr uint32_t kFormatVersion = 1;
constexpr size_t kHashHexLen = 32;  // 128-bit hash as hex

bool isHex32(const std::string& s) {
    if (s.size() != kHashHexLen)
        return false;
    // Canonical lowercase hex only (the verification hash is produced with %016llx,
    // which is lowercase), so two case-variant hashes can't both be "valid".
    return std::all_of(s.begin(), s.end(),
                       [](unsigned char c) { return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'); });
}
}  // namespace

bool VShardSnapshotManifest::valid() const {
    if (!vshard.valid())
        return false;
    if (!isHex32(verificationHash) || !isHex32(catalogHash))
        return false;

    uint64_t prevFile = 0;
    bool haveFile = false;
    for (const auto& e : dataExtents) {
        if (e.revRange.empty || e.revRange.maxRev < e.revRange.minRev)
            return false;
        if (e.revRange.maxRev > snapshotRevision)
            return false;  // a snapshot cannot reference a revision past its watermark
        if (haveFile && e.fileId <= prevFile)
            return false;  // ordered, no duplicate file
        haveFile = true;
        prevFile = e.fileId;
    }

    uint64_t prevTomb = 0;
    bool haveTomb = false;
    for (uint64_t t : tombstoneObjectIds) {
        if (haveTomb && t <= prevTomb)
            return false;  // strictly ascending, unique
        haveTomb = true;
        prevTomb = t;
    }
    return true;
}

std::string VShardSnapshotManifest::encode() const {
    std::string out;
    codec::putU32(out, kMagic);
    codec::putU32(out, kFormatVersion);
    codec::putU16(out, vshard.value());
    codec::putU64(out, snapshotRevision);
    codec::putStr(out, verificationHash);
    codec::putStr(out, catalogHash);
    codec::putU32(out, static_cast<uint32_t>(dataExtents.size()));
    for (const auto& e : dataExtents) {
        codec::putU64(out, e.fileId);
        codec::putU64(out, e.revRange.empty ? 0 : e.revRange.minRev);
        codec::putU64(out, e.revRange.empty ? 0 : e.revRange.maxRev);
    }
    codec::putU32(out, static_cast<uint32_t>(tombstoneObjectIds.size()));
    for (uint64_t t : tombstoneObjectIds)
        codec::putU64(out, t);
    const uint32_t crc = CRC32::compute(out.data(), out.size());
    codec::putU32(out, crc);
    return out;
}

std::optional<VShardSnapshotManifest> VShardSnapshotManifest::decode(std::string_view bytes) {
    constexpr size_t kCrcBytes = 4;
    if (bytes.size() < kCrcBytes)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - kCrcBytes;
    codec::Reader crcReader(std::span<const char>(bytes.data() + bodyLen, kCrcBytes));
    const uint32_t storedCrc = crcReader.u32();
    if (!crcReader.ok() || CRC32::compute(bytes.data(), bodyLen) != storedCrc)
        return std::nullopt;

    codec::Reader r(std::span<const char>(bytes.data(), bodyLen));
    const uint32_t magic = r.u32();
    const uint32_t version = r.u32();
    if (!r.ok() || magic != kMagic || version != kFormatVersion)
        return std::nullopt;

    VShardSnapshotManifest m;
    const uint16_t vs = r.u16();
    m.snapshotRevision = r.u64();
    m.verificationHash = r.str();
    m.catalogHash = r.str();
    if (!r.ok() || vs >= VIRTUAL_SHARD_COUNT)
        return std::nullopt;
    m.vshard = VShardId{vs};

    const uint32_t extentCount = r.u32();
    if (!r.ok())
        return std::nullopt;
    // Do NOT reserve() off the untrusted count: a crafted huge count (with a
    // recomputed CRC) would attempt a giant allocation and throw bad_alloc out of
    // decode(), violating the nullopt-on-corruption contract. The per-iteration
    // r.ok() check fails closed on the first short read instead.
    for (uint32_t i = 0; i < extentCount; ++i) {
        VShardExtent e;
        e.fileId = r.u64();
        const uint64_t minRev = r.u64();
        const uint64_t maxRev = r.u64();
        if (!r.ok())
            return std::nullopt;
        e.revRange = RevisionRange{minRev, maxRev, /*empty=*/false};
        m.dataExtents.push_back(e);
    }

    const uint32_t tombCount = r.u32();
    if (!r.ok())
        return std::nullopt;
    // Same reasoning as extents above: no reserve() off an untrusted count.
    for (uint32_t i = 0; i < tombCount; ++i) {
        const uint64_t t = r.u64();
        if (!r.ok())
            return std::nullopt;
        m.tombstoneObjectIds.push_back(t);
    }

    if (!r.exhausted())
        return std::nullopt;
    // Structural validation catches ordering / hash-shape / watermark violations
    // that the byte framing alone would accept.
    if (!m.valid())
        return std::nullopt;
    return m;
}

}  // namespace timestar
