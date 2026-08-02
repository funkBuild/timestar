#include "snapshot_payload.hpp"

#include "../../storage/series_catalog.hpp"
#include "replicated_command.hpp"

#include <cstring>
#include <stdexcept>

namespace timestar::data {

namespace {

constexpr uint32_t kPayloadMagic = 0x31505354;  // "TSP1" little-endian
constexpr size_t kDeleteReceiptBytes = 16 + 8 + 8 + 8;

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

    std::string blob(size_t maxBytes = std::numeric_limits<size_t>::max()) {
        const uint64_t n = u64();
        if (!ok || n > maxBytes || !avail(n)) {
            ok = false;
            return {};
        }
        std::string s(p, n);
        p += n;
        return s;
    }

    bool skipBlob() {
        const uint64_t n = u64();
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
        const SeriesId128 id = SeriesId128::fromBytes(p, 16);
        p += 16;
        return id;
    }
};

bool validDeleteReceipts(const SnapshotPayload& payload) {
    if ((payload.deleteReceiptsRetiredBeforeMs == 0) != (payload.deleteReceiptsRetiredAtIndex == 0) ||
        payload.deleteReceiptsRetiredAtIndex >= payload.manifest.snapshotRevision ||
        payload.deleteReceipts.size() > kMaxDeleteReceiptsPerVShard)
        return false;
    SeriesId128 previous{};
    for (size_t i = 0; i < payload.deleteReceipts.size(); ++i) {
        const auto& receipt = payload.deleteReceipts[i];
        if (receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 ||
            receipt.appliedIndex >= payload.manifest.snapshotRevision || receipt.issuedAtMs == 0 ||
            receipt.issuedAtMs <= payload.deleteReceiptsRetiredBeforeMs ||
            (i != 0 && !(previous < receipt.operationId)))
            return false;
        previous = receipt.operationId;
    }
    return true;
}

bool validRetentionCutoff(const SnapshotPayload& payload) {
    if (!payload.retentionCutoff)
        return true;
    const auto& cutoff = *payload.retentionCutoff;
    return cutoff.sweepId != 0 && validRetentionMeasurement(cutoff.measurement) && cutoff.policyVersion != 0 &&
           cutoff.cutoffTime != 0 && cutoff.appliedIndex != 0 &&
           cutoff.appliedIndex < payload.manifest.snapshotRevision;
}

void putDeleteReceipts(std::string& out, const std::vector<DeleteOperationReceipt>& receipts) {
    if (receipts.size() > UINT32_MAX)
        throw std::invalid_argument("encodeSnapshotPayload: too many delete receipts");
    putU32(out, static_cast<uint32_t>(receipts.size()));
    for (const auto& receipt : receipts) {
        receipt.operationId.appendTo(out);
        putU64(out, receipt.appliedIndex);
        putU64(out, receipt.commandHash);
        putU64(out, receipt.issuedAtMs);
    }
}

bool readDeleteReceipts(Reader& r, const VShardSnapshotManifest& manifest,
                        std::vector<DeleteOperationReceipt>& receipts, uint64_t retiredBeforeMs) {
    const uint32_t count = r.u32();
    if (!r.ok || count > kMaxDeleteReceiptsPerVShard ||
        count > static_cast<uint64_t>(r.end - r.p) / kDeleteReceiptBytes)
        return false;
    receipts.reserve(count);
    SeriesId128 previous{};
    for (uint32_t i = 0; i < count; ++i) {
        DeleteOperationReceipt receipt;
        receipt.operationId = r.seriesId();
        receipt.appliedIndex = r.u64();
        receipt.commandHash = r.u64();
        receipt.issuedAtMs = r.u64();
        if (!r.ok || receipt.operationId == SeriesId128{} || receipt.appliedIndex == 0 ||
            receipt.appliedIndex >= manifest.snapshotRevision || receipt.issuedAtMs == 0 ||
            receipt.issuedAtMs <= retiredBeforeMs || (i != 0 && !(previous < receipt.operationId)))
            return false;
        previous = receipt.operationId;
        receipts.push_back(receipt);
    }
    return true;
}

void putRetentionCutoff(std::string& out, const std::optional<RetentionCutoffSnapshotState>& cutoff) {
    putU32(out, cutoff ? 1 : 0);
    if (!cutoff)
        return;
    putU64(out, cutoff->sweepId);
    putBlob(out, cutoff->measurement);
    putU64(out, cutoff->policyVersion);
    putU64(out, cutoff->cutoffTime);
    putU64(out, cutoff->appliedIndex);
}

bool readRetentionCutoff(Reader& r, const VShardSnapshotManifest& manifest,
                         std::optional<RetentionCutoffSnapshotState>& cutoff) {
    const uint32_t present = r.u32();
    if (!r.ok || present > 1)
        return false;
    if (!present)
        return true;
    RetentionCutoffSnapshotState decoded;
    decoded.sweepId = r.u64();
    decoded.measurement = r.blob(kMaxRetentionMeasurementBytes);
    decoded.policyVersion = r.u64();
    decoded.cutoffTime = r.u64();
    decoded.appliedIndex = r.u64();
    if (!r.ok || decoded.sweepId == 0 || !validRetentionMeasurement(decoded.measurement) ||
        decoded.policyVersion == 0 || decoded.cutoffTime == 0 || decoded.appliedIndex == 0 ||
        decoded.appliedIndex >= manifest.snapshotRevision)
        return false;
    cutoff = std::move(decoded);
    return true;
}

bool checkTrailer(const std::string& bytes, size_t& bodyLen) {
    if (bytes.size() < 4 + 8)
        return false;
    bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    return fnv1a(bytes.data(), bodyLen) == stored;
}

}  // namespace

std::string encodeSnapshotPayload(const SnapshotPayload& payload) {
    return encodeSnapshotPayload(SnapshotPayload(payload));
}

std::string encodeSnapshotPayload(SnapshotPayload&& payload) {
    if (payload.catalog.empty() || !payload.manifest.valid() ||
        timestar::SeriesCatalog::snapshotHash(payload.catalog) != payload.manifest.catalogHash ||
        !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(payload.catalog.data(), payload.catalog.size())) ||
        !validDeleteReceipts(payload) || !validRetentionCutoff(payload))
        throw std::invalid_argument("encodeSnapshotPayload: invalid v1 snapshot state");

    const std::string manifest = payload.manifest.encode();
    size_t total = 4 + 8 + manifest.size() + 8 + payload.catalog.size() + 16 + 4 +
                   payload.deleteReceipts.size() * kDeleteReceiptBytes + 4 + 4 + 8;
    if (payload.retentionCutoff)
        total += 8 + payload.retentionCutoff->measurement.size() + 32;
    for (const auto& file : payload.files)
        total += 8 + file.name.size() + 8 + file.bytes.size();

    std::string out;
    out.reserve(total);
    putU32(out, kPayloadMagic);
    putBlob(out, manifest);
    putBlob(out, payload.catalog);
    std::string().swap(payload.catalog);
    putU64(out, payload.deleteReceiptsRetiredBeforeMs);
    putU64(out, payload.deleteReceiptsRetiredAtIndex);
    putDeleteReceipts(out, payload.deleteReceipts);
    putRetentionCutoff(out, payload.retentionCutoff);
    putU32(out, static_cast<uint32_t>(payload.files.size()));
    for (auto& file : payload.files) {
        putBlob(out, file.name);
        putBlob(out, file.bytes);
        std::string().swap(file.bytes);
    }
    putU64(out, fnv1a(out.data(), out.size()));
    return out;
}

std::optional<SnapshotPayload> decodeSnapshotPayload(const std::string& bytes) {
    size_t bodyLen = 0;
    if (!checkTrailer(bytes, bodyLen))
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    if (r.u32() != kPayloadMagic)
        return std::nullopt;

    SnapshotPayload out;
    auto manifest = VShardSnapshotManifest::decode(r.blob());
    if (!r.ok || !manifest)
        return std::nullopt;
    out.manifest = std::move(*manifest);
    out.catalog = r.blob();
    if (!r.ok || out.catalog.empty() ||
        timestar::SeriesCatalog::snapshotHash(out.catalog) != out.manifest.catalogHash ||
        !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(out.catalog.data(), out.catalog.size())))
        return std::nullopt;

    out.deleteReceiptsRetiredBeforeMs = r.u64();
    out.deleteReceiptsRetiredAtIndex = r.u64();
    if (!r.ok || (out.deleteReceiptsRetiredBeforeMs == 0) != (out.deleteReceiptsRetiredAtIndex == 0) ||
        out.deleteReceiptsRetiredAtIndex >= out.manifest.snapshotRevision ||
        !readDeleteReceipts(r, out.manifest, out.deleteReceipts, out.deleteReceiptsRetiredBeforeMs))
        return std::nullopt;
    if (!readRetentionCutoff(r, out.manifest, out.retentionCutoff))
        return std::nullopt;

    const uint32_t fileCount = r.u32();
    if (!r.ok || fileCount > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    out.files.reserve(fileCount);
    for (uint32_t i = 0; i < fileCount; ++i) {
        SnapshotFile file{r.blob(), r.blob()};
        if (!r.ok)
            return std::nullopt;
        out.files.push_back(std::move(file));
    }
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return out;
}

std::optional<DataStateMachineSnapshotState> decodeSnapshotStateMachineState(const std::string& bytes) {
    size_t bodyLen = 0;
    if (!checkTrailer(bytes, bodyLen))
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    if (r.u32() != kPayloadMagic)
        return std::nullopt;
    auto manifest = VShardSnapshotManifest::decode(r.blob());
    if (!r.ok || !manifest || !r.skipBlob())
        return std::nullopt;

    DataStateMachineSnapshotState state;
    state.deleteReceipts.retiredBeforeMs = r.u64();
    state.deleteReceipts.retiredAtIndex = r.u64();
    if (!r.ok || (state.deleteReceipts.retiredBeforeMs == 0) != (state.deleteReceipts.retiredAtIndex == 0) ||
        state.deleteReceipts.retiredAtIndex >= manifest->snapshotRevision ||
        !readDeleteReceipts(r, *manifest, state.deleteReceipts.receipts, state.deleteReceipts.retiredBeforeMs) ||
        !readRetentionCutoff(r, *manifest, state.retentionCutoff))
        return std::nullopt;

    const uint32_t fileCount = r.u32();
    if (!r.ok || fileCount > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    for (uint32_t i = 0; i < fileCount; ++i)
        if (!r.skipBlob() || !r.skipBlob())
            return std::nullopt;
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return state;
}

std::optional<std::vector<DeleteOperationReceipt>> decodeSnapshotDeleteReceipts(const std::string& bytes) {
    auto state = decodeSnapshotDeleteReceiptState(bytes);
    if (!state)
        return std::nullopt;
    return std::move(state->receipts);
}

std::optional<DeleteReceiptSnapshotState> decodeSnapshotDeleteReceiptState(const std::string& bytes) {
    auto state = decodeSnapshotStateMachineState(bytes);
    if (!state)
        return std::nullopt;
    return std::move(state->deleteReceipts);
}

}  // namespace timestar::data
