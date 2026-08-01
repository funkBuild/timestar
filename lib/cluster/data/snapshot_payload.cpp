#include "snapshot_payload.hpp"

#include "../../storage/series_catalog.hpp"

#include <cstring>
#include <stdexcept>

namespace timestar::data {

namespace {

constexpr uint32_t kPayloadMagic = 0x32505354;  // "TSP2" little-endian
constexpr uint32_t kPayloadVersionV2 = 2;
constexpr uint32_t kPayloadVersionV3 = 3;
constexpr size_t kDeleteReceiptBytes = 16 + 8 + 8;

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
// A length-prefixed blob (u64 length -> file bytes can exceed 4GB in principle;
// manifests are small but use the same framing).
void putBlob(std::string& out, const std::string& s) {
    putU64(out, s.size());
    out.append(s);
}

struct Reader {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint32_t u32() {
        if (!ok || !avail(4)) {
            ok = false;
            return 0;
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(static_cast<uint8_t>(*p++)) << (8 * i);
        return v;
    }
    uint64_t u64() {
        if (!ok || !avail(8)) {
            ok = false;
            return 0;
        }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(static_cast<uint8_t>(*p++)) << (8 * i);
        return v;
    }
    std::string blob() {
        uint64_t n = u64();
        if (!ok || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, n);
        p += n;
        return s;
    }
    bool skipBlob() {
        uint64_t n = u64();
        if (!ok || !avail(n)) {
            ok = false;
            return false;
        }
        p += n;
        return true;
    }
    SeriesId128 seriesId() {
        if (!ok || !avail(16)) {
            ok = false;
            return {};
        }
        SeriesId128 id = SeriesId128::fromBytes(p, 16);
        p += 16;
        return id;
    }
};

bool validDeleteReceipts(const SnapshotPayload& payload) {
    if (!payload.deleteReceipts.empty() && payload.catalog.empty())
        return false;
    SeriesId128 previous{};
    bool first = true;
    for (const auto& receipt : payload.deleteReceipts) {
        if (receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 ||
            receipt.appliedIndex >= payload.manifest.snapshotRevision || (!first && !(previous < receipt.operationId)))
            return false;
        first = false;
        previous = receipt.operationId;
    }
    return true;
}

void putDeleteReceipts(std::string& out, const std::vector<DeleteOperationReceipt>& receipts) {
    if (receipts.size() > UINT32_MAX)
        throw std::invalid_argument("encodeSnapshotPayload: too many delete receipts");
    putU32(out, static_cast<uint32_t>(receipts.size()));
    for (const auto& receipt : receipts) {
        receipt.operationId.appendTo(out);
        putU64(out, receipt.appliedIndex);
        putU64(out, receipt.commandHash);
    }
}

bool readDeleteReceipts(Reader& r, const VShardSnapshotManifest& manifest,
                        std::vector<DeleteOperationReceipt>& receipts) {
    const uint32_t count = r.u32();
    if (!r.ok || count > static_cast<uint64_t>(r.end - r.p) / kDeleteReceiptBytes)
        return false;
    receipts.reserve(count);
    SeriesId128 previous{};
    for (uint32_t i = 0; i < count; ++i) {
        DeleteOperationReceipt receipt;
        receipt.operationId = r.seriesId();
        receipt.appliedIndex = r.u64();
        receipt.commandHash = r.u64();
        if (!r.ok || receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 ||
            receipt.appliedIndex >= manifest.snapshotRevision || (i != 0 && !(previous < receipt.operationId)))
            return false;
        previous = receipt.operationId;
        receipts.push_back(receipt);
    }
    return true;
}

}  // namespace

std::string encodeSnapshotPayload(const SnapshotPayload& payload) {
    std::string out;
    if (!payload.catalog.empty()) {
        if (!payload.manifest.valid() ||
            timestar::SeriesCatalog::snapshotHash(payload.catalog) != payload.manifest.catalogHash ||
            !timestar::SeriesCatalog::loadSnapshot(
                std::span<const char>(payload.catalog.data(), payload.catalog.size())) ||
            !validDeleteReceipts(payload))
            throw std::invalid_argument("encodeSnapshotPayload: catalog/hash mismatch");
        putU32(out, kPayloadMagic);
        putU32(out, payload.deleteReceipts.empty() ? kPayloadVersionV2 : kPayloadVersionV3);
    } else if (!payload.deleteReceipts.empty()) {
        throw std::invalid_argument("encodeSnapshotPayload: delete receipts require a v2+ catalog");
    }
    putBlob(out, payload.manifest.encode());
    if (!payload.catalog.empty()) {
        putBlob(out, payload.catalog);
        if (!payload.deleteReceipts.empty())
            putDeleteReceipts(out, payload.deleteReceipts);
    }
    putU32(out, static_cast<uint32_t>(payload.files.size()));
    for (const auto& f : payload.files) {
        putBlob(out, f.name);
        putBlob(out, f.bytes);
    }
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::string encodeSnapshotPayload(SnapshotPayload&& payload) {
    const std::string manifest = payload.manifest.encode();
    // Exactly what the const& overload will have written, computed before writing a byte:
    // manifest blob + file count + per file (name blob + bytes blob) + the FNV trailer.
    const bool versioned = !payload.catalog.empty();
    const bool v3 = !payload.deleteReceipts.empty();
    if (versioned && (!payload.manifest.valid() ||
                      timestar::SeriesCatalog::snapshotHash(payload.catalog) != payload.manifest.catalogHash ||
                      !timestar::SeriesCatalog::loadSnapshot(
                          std::span<const char>(payload.catalog.data(), payload.catalog.size())) ||
                      !validDeleteReceipts(payload)))
        throw std::invalid_argument("encodeSnapshotPayload: catalog/hash mismatch");
    if (!versioned && v3)
        throw std::invalid_argument("encodeSnapshotPayload: delete receipts require a v2+ catalog");
    size_t total = (versioned ? 8 + 8 + payload.catalog.size() : 0) + 8 + manifest.size() + 4 + 8;
    if (v3)
        total += 4 + payload.deleteReceipts.size() * kDeleteReceiptBytes;
    for (const auto& f : payload.files)
        total += 8 + f.name.size() + 8 + f.bytes.size();

    std::string out;
    out.reserve(total);
    if (versioned) {
        putU32(out, kPayloadMagic);
        putU32(out, v3 ? kPayloadVersionV3 : kPayloadVersionV2);
    }
    putBlob(out, manifest);
    if (versioned) {
        putBlob(out, payload.catalog);
        std::string().swap(payload.catalog);
        if (v3)
            putDeleteReceipts(out, payload.deleteReceipts);
    }
    putU32(out, static_cast<uint32_t>(payload.files.size()));
    for (auto& f : payload.files) {
        putBlob(out, f.name);
        putBlob(out, f.bytes);
        // Release as we go: this is the whole point of the overload. Without the swap the
        // input is still fully resident when the last file is appended, which is the 2x
        // peak the const& overload has.
        std::string().swap(f.bytes);
    }
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    Reader r{bytes.data(), bytes.data() + bodyLen};
    SnapshotPayload out;
    uint32_t version = 0;
    if (bodyLen >= 8) {
        Reader probe = r;
        if (probe.u32() == kPayloadMagic) {
            version = probe.u32();
            if ((version != kPayloadVersionV2 && version != kPayloadVersionV3) || !probe.ok)
                return std::nullopt;
            r = probe;
        }
    }
    std::string manifestBlob = r.blob();
    if (!r.ok)
        return std::nullopt;
    auto m = VShardSnapshotManifest::decode(manifestBlob);
    if (!m)
        return std::nullopt;  // corrupt or structurally-invalid manifest
    out.manifest = std::move(*m);

    if (version != 0) {
        out.catalog = r.blob();
        if (!r.ok || out.catalog.empty() ||
            timestar::SeriesCatalog::snapshotHash(out.catalog) != out.manifest.catalogHash ||
            !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(out.catalog.data(), out.catalog.size())))
            return std::nullopt;
    }

    if (version == kPayloadVersionV3 && !readDeleteReceipts(r, out.manifest, out.deleteReceipts))
        return std::nullopt;

    uint32_t nfiles = r.u32();
    // Each file is >= two 8-byte length prefixes; reject an inflated count.
    if (!r.ok || nfiles > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    out.files.reserve(nfiles);
    for (uint32_t i = 0; i < nfiles; ++i) {
        SnapshotFile f;
        f.name = r.blob();
        f.bytes = r.blob();
        if (!r.ok)
            return std::nullopt;
        out.files.push_back(std::move(f));
    }
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return out;
}

std::optional<std::vector<DeleteOperationReceipt>> decodeSnapshotDeleteReceipts(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    Reader r{bytes.data(), bytes.data() + bodyLen};
    uint32_t version = 0;
    if (bodyLen >= 8) {
        Reader probe = r;
        if (probe.u32() == kPayloadMagic) {
            version = probe.u32();
            if ((version != kPayloadVersionV2 && version != kPayloadVersionV3) || !probe.ok)
                return std::nullopt;
            r = probe;
        }
    }

    const std::string manifestBytes = r.blob();
    auto manifest = VShardSnapshotManifest::decode(manifestBytes);
    if (!r.ok || !manifest)
        return std::nullopt;
    if (version != 0 && !r.skipBlob())
        return std::nullopt;

    std::vector<DeleteOperationReceipt> receipts;
    if (version == kPayloadVersionV3 && !readDeleteReceipts(r, *manifest, receipts))
        return std::nullopt;

    const uint32_t files = r.u32();
    if (!r.ok || files > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    for (uint32_t i = 0; i < files; ++i)
        if (!r.skipBlob() || !r.skipBlob())
            return std::nullopt;
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return receipts;
}

}  // namespace timestar::data
