#include "snapshot_payload.hpp"

#include "../../storage/series_catalog.hpp"
#include "replicated_command.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstring>
#include <fstream>
#include <limits>
#include <seastar/core/thread.hh>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

namespace timestar::data {

namespace {

constexpr uint32_t kPayloadMagic = 0x31505354;  // "TSP1" little-endian
constexpr size_t kDeleteReceiptBytes = 16 + 8 + 8 + 8;
constexpr size_t kMaxSnapshotManifestBytes = size_t{16} << 20;
constexpr size_t kMaxSnapshotCatalogBytes = size_t{64} << 20;
constexpr size_t kMaxSnapshotObjectNameBytes = 4096;
constexpr uint64_t kMaxEncodedSnapshotBytes = uint64_t{1} << 40;

uint64_t fnvExtend(uint64_t hash, const char* data, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= 1099511628211ull;
    }
    return hash;
}

uint64_t fnv1a(const char* p, size_t n) {
    return fnvExtend(1469598103934665603ull, p, n);
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
    size_t remaining() const { return static_cast<size_t>(end - p); }

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

    bool skipBlob(size_t maxBytes = std::numeric_limits<size_t>::max()) {
        const uint64_t n = u64();
        if (!ok || n > maxBytes || !avail(n)) {
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

template <typename Payload>
bool validDeleteReceipts(const Payload& payload) {
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

template <typename Payload>
bool validRetentionCutoff(const Payload& payload) {
    if (!payload.retentionCutoff)
        return true;
    const auto& cutoff = *payload.retentionCutoff;
    return cutoff.sweepId != 0 && validRetentionMeasurement(cutoff.measurement) && cutoff.policyVersion != 0 &&
           cutoff.cutoffTime != 0 && cutoff.appliedIndex != 0 &&
           cutoff.appliedIndex < payload.manifest.snapshotRevision;
}

template <typename Payload>
bool validSnapshotObjectNames(const Payload& payload) {
    if (payload.files.size() > UINT32_MAX || payload.files.size() != payload.manifest.dataExtents.size())
        return false;
    std::set<std::string> names;
    for (const auto& file : payload.files) {
        const std::filesystem::path name(file.name);
        if (file.name.empty() || file.name.size() > kMaxSnapshotObjectNameBytes || name.has_parent_path() ||
            name.filename() != name || name.extension() != ".tsm" || !names.insert(file.name).second)
            return false;
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
        putU64(out, receipt.issuedAtMs);
    }
}

template <typename Input>
bool readDeleteReceipts(Input& r, const VShardSnapshotManifest& manifest, std::vector<DeleteOperationReceipt>& receipts,
                        uint64_t retiredBeforeMs) {
    const uint32_t count = r.u32();
    if (!r.ok || count > kMaxDeleteReceiptsPerVShard ||
        count > static_cast<uint64_t>(r.remaining()) / kDeleteReceiptBytes)
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

template <typename Input>
bool readRetentionCutoff(Input& r, const VShardSnapshotManifest& manifest,
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

void removeOwnedPath(const std::filesystem::path& path, bool owned) {
    if (!owned || path.empty())
        return;
    std::error_code ec;
    std::filesystem::remove(path, ec);
}

class StreamWriter {
public:
    explicit StreamWriter(const std::filesystem::path& path) : out_(path, std::ios::binary | std::ios::trunc) {
        if (!out_)
            throw std::runtime_error("cannot create TSP1 snapshot " + path.string());
    }

    void bytes(const char* data, size_t size, bool hash = true) {
        size_t offset = 0;
        while (offset < size) {
            const size_t count = std::min<size_t>(size_t{1} << 20, size - offset);
            out_.write(data + offset, static_cast<std::streamsize>(count));
            if (!out_)
                throw std::runtime_error("write failed for TSP1 snapshot");
            if (hash)
                hash_ = fnvExtend(hash_, data + offset, count);
            size_ += count;
            offset += count;
            if (size >= (size_t{1} << 20))
                seastar::thread::yield();
        }
    }
    void u32(uint32_t value, bool hash = true) {
        char bytes[4];
        for (int i = 0; i < 4; ++i)
            bytes[i] = static_cast<char>((value >> (8 * i)) & 0xff);
        this->bytes(bytes, sizeof(bytes), hash);
    }
    void u64(uint64_t value, bool hash = true) {
        char bytes[8];
        for (int i = 0; i < 8; ++i)
            bytes[i] = static_cast<char>((value >> (8 * i)) & 0xff);
        this->bytes(bytes, sizeof(bytes), hash);
    }
    void blob(const std::string& value) {
        u64(value.size());
        bytes(value.data(), value.size());
    }
    void fileBlob(const SnapshotFilePath& file) {
        u64(file.size);
        std::ifstream in(file.path, std::ios::binary);
        if (!in)
            throw std::runtime_error("cannot open snapshot object " + file.path.string());
        std::vector<char> buffer(size_t{1} << 20);
        uint64_t copied = 0;
        while (copied < file.size) {
            const size_t want = static_cast<size_t>(std::min<uint64_t>(buffer.size(), file.size - copied));
            in.read(buffer.data(), static_cast<std::streamsize>(want));
            const size_t got = static_cast<size_t>(in.gcount());
            if (got != want)
                throw std::runtime_error("snapshot object is truncated: " + file.path.string());
            bytes(buffer.data(), got);
            copied += got;
        }
        char extra = 0;
        if (in.read(&extra, 1) || in.gcount() != 0)
            throw std::runtime_error("snapshot object is longer than its declared size: " + file.path.string());
    }
    void finish() {
        out_.flush();
        if (!out_)
            throw std::runtime_error("flush failed for TSP1 snapshot");
    }
    uint64_t hash() const { return hash_; }
    uint64_t size() const { return size_; }

private:
    std::ofstream out_;
    uint64_t hash_ = 1469598103934665603ull;
    uint64_t size_ = 0;
};

class StreamReader {
public:
    StreamReader(const std::filesystem::path& path, uint64_t limit) : in_(path, std::ios::binary), remaining_(limit) {
        if (!in_)
            ok = false;
    }

    // Hashing reader for a complete TSP1 file. The size is taken from the same
    // opened stream which is subsequently parsed, avoiding a path stat/open
    // race. The final eight bytes remain outside `remaining_` as the trailer.
    explicit StreamReader(const std::filesystem::path& path) : in_(path, std::ios::binary), hashReads_(true) {
        if (!in_) {
            ok = false;
            return;
        }
        in_.seekg(0, std::ios::end);
        const std::streampos end = in_.tellg();
        if (!in_ || end < std::streampos{12}) {
            ok = false;
            return;
        }
        encodedSize_ = static_cast<uint64_t>(end);
        remaining_ = encodedSize_ - 8;
        in_.seekg(0, std::ios::beg);
        if (!in_)
            ok = false;
    }

    bool ok = true;  // Reader-compatible field used by shared helpers
    size_t remaining() const {
        return remaining_ > std::numeric_limits<size_t>::max() ? std::numeric_limits<size_t>::max()
                                                               : static_cast<size_t>(remaining_);
    }
    uint64_t encodedSize() const { return encodedSize_; }
    uint32_t u32() {
        char data[4]{};
        if (!read(data, sizeof(data)))
            return 0;
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i)
            value |= static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << (8 * i);
        return value;
    }
    uint64_t u64() {
        char data[8]{};
        if (!read(data, sizeof(data)))
            return 0;
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i)
            value |= static_cast<uint64_t>(static_cast<uint8_t>(data[i])) << (8 * i);
        return value;
    }
    std::string blob(size_t maxBytes = std::numeric_limits<size_t>::max()) {
        const uint64_t size = u64();
        if (!ok || size > maxBytes || size > remaining_) {
            ok = false;
            return {};
        }
        std::string value(static_cast<size_t>(size), '\0');
        if (!read(value.data(), value.size()))
            return {};
        return value;
    }
    bool skipBlob(size_t maxBytes = std::numeric_limits<size_t>::max()) {
        const uint64_t size = u64();
        if (size > maxBytes) {
            ok = false;
            return false;
        }
        return skip(size);
    }
    SeriesId128 seriesId() {
        char data[16]{};
        if (!read(data, sizeof(data)))
            return {};
        return SeriesId128::fromBytes(data, sizeof(data));
    }
    bool copyBlob(const std::filesystem::path& output, uint64_t& size) {
        size = u64();
        if (!ok || size > remaining_) {
            ok = false;
            return false;
        }
        std::ofstream out(output, std::ios::binary | std::ios::trunc);
        if (!out) {
            ok = false;
            return false;
        }
        std::vector<char> buffer(size_t{1} << 20);
        uint64_t copied = 0;
        while (copied < size) {
            const size_t want = static_cast<size_t>(std::min<uint64_t>(buffer.size(), size - copied));
            if (!read(buffer.data(), want))
                return false;
            out.write(buffer.data(), static_cast<std::streamsize>(want));
            if (!out) {
                ok = false;
                return false;
            }
            copied += want;
        }
        out.flush();
        if (!out)
            ok = false;
        return ok;
    }

    std::optional<uint64_t> finishAndVerifyTrailer() {
        if (!ok || !hashReads_ || remaining_ != 0)
            return std::nullopt;
        char trailer[8]{};
        in_.read(trailer, sizeof(trailer));
        if (in_.gcount() != static_cast<std::streamsize>(sizeof(trailer))) {
            ok = false;
            return std::nullopt;
        }
        uint64_t stored = 0;
        for (int i = 0; i < 8; ++i)
            stored |= static_cast<uint64_t>(static_cast<uint8_t>(trailer[i])) << (8 * i);
        if (stored != hash_ || in_.peek() != std::char_traits<char>::eof()) {
            ok = false;
            return std::nullopt;
        }
        return fnvExtend(hash_, trailer, sizeof(trailer));
    }

private:
    bool read(char* output, size_t size) {
        if (!ok || size > remaining_) {
            ok = false;
            return false;
        }
        if (size == 0)
            return true;
        size_t offset = 0;
        while (offset < size) {
            const size_t count = std::min<size_t>(size_t{1} << 20, size - offset);
            in_.read(output + offset, static_cast<std::streamsize>(count));
            if (static_cast<size_t>(in_.gcount()) != count) {
                ok = false;
                return false;
            }
            if (hashReads_)
                hash_ = fnvExtend(hash_, output + offset, count);
            offset += count;
            if (size >= (size_t{1} << 20))
                seastar::thread::yield();
        }
        remaining_ -= size;
        return true;
    }
    bool skip(uint64_t size) {
        if (!ok || size > remaining_) {
            ok = false;
            return false;
        }
        if (hashReads_) {
            std::array<char, 64u << 10> buffer{};
            uint64_t skipped = 0;
            while (skipped < size) {
                const size_t count = static_cast<size_t>(std::min<uint64_t>(buffer.size(), size - skipped));
                if (!read(buffer.data(), count))
                    return false;
                skipped += count;
                seastar::thread::yield();
            }
            return true;
        }
        in_.seekg(static_cast<std::streamoff>(size), std::ios::cur);
        if (!in_) {
            ok = false;
            return false;
        }
        remaining_ -= size;
        return true;
    }

    std::ifstream in_;
    uint64_t remaining_ = 0;
    bool hashReads_ = false;
    uint64_t hash_ = 1469598103934665603ull;
    uint64_t encodedSize_ = 0;
};

std::optional<uint64_t> verifiedFileBodyLength(const std::filesystem::path& path) {
    std::error_code ec;
    const uint64_t size = std::filesystem::file_size(path, ec);
    if (ec || size < 12)
        return std::nullopt;
    const uint64_t body = size - 8;
    std::ifstream in(path, std::ios::binary);
    if (!in)
        return std::nullopt;
    std::vector<char> buffer(size_t{1} << 20);
    uint64_t hash = 1469598103934665603ull;
    uint64_t readBytes = 0;
    while (readBytes < body) {
        const size_t want = static_cast<size_t>(std::min<uint64_t>(buffer.size(), body - readBytes));
        in.read(buffer.data(), static_cast<std::streamsize>(want));
        if (static_cast<size_t>(in.gcount()) != want)
            return std::nullopt;
        for (size_t i = 0; i < want; ++i) {
            hash ^= static_cast<uint8_t>(buffer[i]);
            hash *= 1099511628211ull;
        }
        readBytes += want;
        seastar::thread::yield();
    }
    char trailer[8]{};
    in.read(trailer, sizeof(trailer));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(trailer)))
        return std::nullopt;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(trailer[i])) << (8 * i);
    return stored == hash ? std::optional<uint64_t>(body) : std::nullopt;
}

}  // namespace

std::string encodeSnapshotPayload(const SnapshotPayload& payload) {
    return encodeSnapshotPayload(SnapshotPayload(payload));
}

std::string encodeSnapshotPayload(SnapshotPayload&& payload) {
    if (payload.catalog.empty() || !payload.manifest.valid() ||
        timestar::SeriesCatalog::snapshotHash(payload.catalog) != payload.manifest.catalogHash ||
        !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(payload.catalog.data(), payload.catalog.size())) ||
        !validDeleteReceipts(payload) || !validRetentionCutoff(payload) ||
        payload.catalog.size() > kMaxSnapshotCatalogBytes || !validSnapshotObjectNames(payload))
        throw std::invalid_argument("encodeSnapshotPayload: invalid v1 snapshot state");

    const std::string manifest = payload.manifest.encode();
    if (manifest.size() > kMaxSnapshotManifestBytes)
        throw std::invalid_argument("encodeSnapshotPayload: snapshot manifest is too large");
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
    auto manifest = VShardSnapshotManifest::decode(r.blob(kMaxSnapshotManifestBytes));
    if (!r.ok || !manifest)
        return std::nullopt;
    out.manifest = std::move(*manifest);
    out.catalog = r.blob(kMaxSnapshotCatalogBytes);
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
    if (!r.ok || fileCount != out.manifest.dataExtents.size() || fileCount > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    out.files.reserve(fileCount);
    for (uint32_t i = 0; i < fileCount; ++i) {
        SnapshotFile file{r.blob(kMaxSnapshotObjectNameBytes), r.blob()};
        const std::filesystem::path name(file.name);
        if (!r.ok || file.name.empty() || name.has_parent_path() || name.filename() != name ||
            name.extension() != ".tsm")
            return std::nullopt;
        out.files.push_back(std::move(file));
    }
    if (!r.ok || r.p != r.end || !validSnapshotObjectNames(out))
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
    auto manifest = VShardSnapshotManifest::decode(r.blob(kMaxSnapshotManifestBytes));
    if (!r.ok || !manifest || !r.skipBlob(kMaxSnapshotCatalogBytes))
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
    if (!r.ok || fileCount != manifest->dataExtents.size() || fileCount > static_cast<uint64_t>(r.end - r.p) / 16)
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

SnapshotFilePath::SnapshotFilePath(SnapshotFilePath&& other) noexcept
    : name(std::move(other.name)),
      path(std::move(other.path)),
      size(other.size),
      removeOnDestroy(std::exchange(other.removeOnDestroy, false)) {}

SnapshotFilePath& SnapshotFilePath::operator=(SnapshotFilePath&& other) noexcept {
    if (this != &other) {
        removeOwnedPath(path, removeOnDestroy);
        name = std::move(other.name);
        path = std::move(other.path);
        size = other.size;
        removeOnDestroy = std::exchange(other.removeOnDestroy, false);
    }
    return *this;
}

SnapshotFilePath::~SnapshotFilePath() {
    removeOwnedPath(path, removeOnDestroy);
}

EncodedSnapshotFile::EncodedSnapshotFile(EncodedSnapshotFile&& other) noexcept
    : path(std::move(other.path)),
      size(other.size),
      hash(other.hash),
      removeOnDestroy(std::exchange(other.removeOnDestroy, false)) {}

EncodedSnapshotFile& EncodedSnapshotFile::operator=(EncodedSnapshotFile&& other) noexcept {
    if (this != &other) {
        removeOwnedPath(path, removeOnDestroy);
        path = std::move(other.path);
        size = other.size;
        hash = other.hash;
        removeOnDestroy = std::exchange(other.removeOnDestroy, false);
    }
    return *this;
}

EncodedSnapshotFile::~EncodedSnapshotFile() {
    removeOwnedPath(path, removeOnDestroy);
}

seastar::future<EncodedSnapshotFile> encodeSnapshotPayloadFile(SnapshotPayloadFile payload,
                                                               std::filesystem::path outputPath) {
    if (payload.catalog.empty() || payload.catalog.size() > kMaxSnapshotCatalogBytes || !payload.manifest.valid() ||
        timestar::SeriesCatalog::snapshotHash(payload.catalog) != payload.manifest.catalogHash ||
        !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(payload.catalog.data(), payload.catalog.size())) ||
        !validDeleteReceipts(payload) || !validRetentionCutoff(payload) || !validSnapshotObjectNames(payload))
        throw std::invalid_argument("encodeSnapshotPayloadFile: invalid v1 snapshot state");

    std::string manifestBytes = payload.manifest.encode();
    if (manifestBytes.size() > kMaxSnapshotManifestBytes)
        throw std::invalid_argument("encodeSnapshotPayloadFile: snapshot manifest is too large");

    for (const auto& file : payload.files) {
        const std::filesystem::path wireName(file.name);
        std::error_code ec;
        if (file.name.empty() || wireName.has_parent_path() || wireName.filename() != wireName ||
            wireName.extension() != ".tsm" || !std::filesystem::is_regular_file(file.path, ec) || ec ||
            std::filesystem::file_size(file.path, ec) != file.size || ec)
            throw std::invalid_argument("encodeSnapshotPayloadFile: invalid snapshot object");
    }

    std::optional<EncodedSnapshotFile> encoded;
    std::exception_ptr error;
    try {
        encoded = co_await seastar::async(
            [payload = std::move(payload), manifestBytes = std::move(manifestBytes), outputPath]() mutable {
                if (!outputPath.parent_path().empty())
                    std::filesystem::create_directories(outputPath.parent_path());
                StreamWriter writer(outputPath);
                writer.u32(kPayloadMagic);
                writer.blob(manifestBytes);
                writer.blob(payload.catalog);
                writer.u64(payload.deleteReceiptsRetiredBeforeMs);
                writer.u64(payload.deleteReceiptsRetiredAtIndex);
                writer.u32(static_cast<uint32_t>(payload.deleteReceipts.size()));
                for (const auto& receipt : payload.deleteReceipts) {
                    std::string id;
                    id.reserve(16);
                    receipt.operationId.appendTo(id);
                    writer.bytes(id.data(), id.size());
                    writer.u64(receipt.appliedIndex);
                    writer.u64(receipt.commandHash);
                    writer.u64(receipt.issuedAtMs);
                }
                writer.u32(payload.retentionCutoff ? 1 : 0);
                if (payload.retentionCutoff) {
                    writer.u64(payload.retentionCutoff->sweepId);
                    writer.blob(payload.retentionCutoff->measurement);
                    writer.u64(payload.retentionCutoff->policyVersion);
                    writer.u64(payload.retentionCutoff->cutoffTime);
                    writer.u64(payload.retentionCutoff->appliedIndex);
                }
                writer.u32(static_cast<uint32_t>(payload.files.size()));
                for (const auto& file : payload.files) {
                    writer.blob(file.name);
                    writer.fileBlob(file);
                }
                const uint64_t bodyHash = writer.hash();
                writer.u64(bodyHash);  // trailer is not part of bodyHash; it is part of whole-file hash
                writer.finish();
                EncodedSnapshotFile out;
                out.path = outputPath;
                out.size = writer.size();
                out.hash = writer.hash();
                return out;
            });
    } catch (...) {
        error = std::current_exception();
    }
    if (error) {
        co_await seastar::async([outputPath] {
            std::error_code ec;
            std::filesystem::remove(outputPath, ec);
        });
        std::rethrow_exception(error);
    }
    co_return std::move(*encoded);
}

seastar::future<std::optional<SnapshotPayloadFile>> decodeSnapshotPayloadFile(
    const std::filesystem::path& encodedPath, const std::filesystem::path& extractionDirectory) {
    co_return co_await seastar::async([encodedPath, extractionDirectory]() -> std::optional<SnapshotPayloadFile> {
        const auto bodyLen = verifiedFileBodyLength(encodedPath);
        if (!bodyLen)
            return std::nullopt;
        StreamReader reader(encodedPath, *bodyLen);
        if (reader.u32() != kPayloadMagic)
            return std::nullopt;

        SnapshotPayloadFile out;
        auto manifest = VShardSnapshotManifest::decode(reader.blob(kMaxSnapshotManifestBytes));
        if (!reader.ok || !manifest)
            return std::nullopt;
        out.manifest = std::move(*manifest);
        out.catalog = reader.blob(kMaxSnapshotCatalogBytes);
        if (!reader.ok || out.catalog.empty() ||
            timestar::SeriesCatalog::snapshotHash(out.catalog) != out.manifest.catalogHash ||
            !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(out.catalog.data(), out.catalog.size())))
            return std::nullopt;

        out.deleteReceiptsRetiredBeforeMs = reader.u64();
        out.deleteReceiptsRetiredAtIndex = reader.u64();
        if (!reader.ok || (out.deleteReceiptsRetiredBeforeMs == 0) != (out.deleteReceiptsRetiredAtIndex == 0) ||
            out.deleteReceiptsRetiredAtIndex >= out.manifest.snapshotRevision ||
            !readDeleteReceipts(reader, out.manifest, out.deleteReceipts, out.deleteReceiptsRetiredBeforeMs) ||
            !readRetentionCutoff(reader, out.manifest, out.retentionCutoff))
            return std::nullopt;

        const uint32_t fileCount = reader.u32();
        if (!reader.ok || fileCount != out.manifest.dataExtents.size() || fileCount > reader.remaining() / 16)
            return std::nullopt;
        std::filesystem::create_directories(extractionDirectory);
        static std::atomic<uint64_t> nonce{0};
        const uint64_t generation = ++nonce;
        std::set<std::string> wireNames;
        out.files.reserve(fileCount);
        for (uint32_t i = 0; i < fileCount; ++i) {
            std::string name = reader.blob(kMaxSnapshotObjectNameBytes);
            const std::filesystem::path wireName(name);
            if (!reader.ok || name.empty() || wireName.has_parent_path() || wireName.filename() != wireName ||
                wireName.extension() != ".tsm" || !wireNames.insert(name).second)
                return std::nullopt;
            const auto path = extractionDirectory /
                              ("snapshot_v1_extract_" + std::to_string(generation) + "_" + std::to_string(i) + ".tsm");
            uint64_t size = 0;
            if (!reader.copyBlob(path, size)) {
                std::error_code ec;
                std::filesystem::remove(path, ec);
                return std::nullopt;
            }
            out.files.emplace_back(std::move(name), path, size);
        }
        if (!reader.ok || reader.remaining() != 0)
            return std::nullopt;
        return out;
    });
}

seastar::future<std::optional<DataStateMachineSnapshotState>> decodeSnapshotStateMachineStateFile(
    const std::filesystem::path& encodedPath) {
    co_return co_await seastar::async([encodedPath]() -> std::optional<DataStateMachineSnapshotState> {
        const auto bodyLen = verifiedFileBodyLength(encodedPath);
        if (!bodyLen)
            return std::nullopt;
        StreamReader reader(encodedPath, *bodyLen);
        if (reader.u32() != kPayloadMagic)
            return std::nullopt;
        auto manifest = VShardSnapshotManifest::decode(reader.blob(kMaxSnapshotManifestBytes));
        if (!reader.ok || !manifest || !reader.skipBlob(kMaxSnapshotCatalogBytes))
            return std::nullopt;

        DataStateMachineSnapshotState state;
        state.deleteReceipts.retiredBeforeMs = reader.u64();
        state.deleteReceipts.retiredAtIndex = reader.u64();
        if (!reader.ok || (state.deleteReceipts.retiredBeforeMs == 0) != (state.deleteReceipts.retiredAtIndex == 0) ||
            state.deleteReceipts.retiredAtIndex >= manifest->snapshotRevision ||
            !readDeleteReceipts(reader, *manifest, state.deleteReceipts.receipts,
                                state.deleteReceipts.retiredBeforeMs) ||
            !readRetentionCutoff(reader, *manifest, state.retentionCutoff))
            return std::nullopt;
        const uint32_t fileCount = reader.u32();
        if (!reader.ok || fileCount != manifest->dataExtents.size() || fileCount > reader.remaining() / 16)
            return std::nullopt;
        for (uint32_t i = 0; i < fileCount; ++i)
            if (!reader.skipBlob() || !reader.skipBlob())
                return std::nullopt;
        if (!reader.ok || reader.remaining() != 0)
            return std::nullopt;
        return state;
    });
}

seastar::future<std::optional<SnapshotPayloadFileInfo>> inspectSnapshotPayloadFile(
    const std::filesystem::path& encodedPath) {
    co_return co_await seastar::async([encodedPath]() -> std::optional<SnapshotPayloadFileInfo> {
        std::error_code statusError;
        if (std::filesystem::symlink_status(encodedPath, statusError).type() != std::filesystem::file_type::regular ||
            statusError)
            return std::nullopt;
        StreamReader reader(encodedPath);
        if (!reader.ok || reader.encodedSize() > kMaxEncodedSnapshotBytes || reader.u32() != kPayloadMagic)
            return std::nullopt;

        auto manifest = VShardSnapshotManifest::decode(reader.blob(kMaxSnapshotManifestBytes));
        if (!reader.ok || !manifest)
            return std::nullopt;
        const std::string catalog = reader.blob(kMaxSnapshotCatalogBytes);
        if (!reader.ok || catalog.empty() || timestar::SeriesCatalog::snapshotHash(catalog) != manifest->catalogHash ||
            !timestar::SeriesCatalog::loadSnapshot(std::span<const char>(catalog.data(), catalog.size())))
            return std::nullopt;

        SnapshotPayloadFile metadata;
        metadata.manifest = *manifest;
        metadata.deleteReceiptsRetiredBeforeMs = reader.u64();
        metadata.deleteReceiptsRetiredAtIndex = reader.u64();
        if (!reader.ok ||
            (metadata.deleteReceiptsRetiredBeforeMs == 0) != (metadata.deleteReceiptsRetiredAtIndex == 0) ||
            metadata.deleteReceiptsRetiredAtIndex >= manifest->snapshotRevision ||
            !readDeleteReceipts(reader, *manifest, metadata.deleteReceipts, metadata.deleteReceiptsRetiredBeforeMs) ||
            !readRetentionCutoff(reader, *manifest, metadata.retentionCutoff))
            return std::nullopt;

        const uint32_t fileCount = reader.u32();
        if (!reader.ok || fileCount != manifest->dataExtents.size() || fileCount > reader.remaining() / 16)
            return std::nullopt;
        std::set<std::string> names;
        for (uint32_t i = 0; i < fileCount; ++i) {
            const std::string name = reader.blob(kMaxSnapshotObjectNameBytes);
            const std::filesystem::path wireName(name);
            if (!reader.ok || name.empty() || wireName.has_parent_path() || wireName.filename() != wireName ||
                wireName.extension() != ".tsm" || !names.insert(name).second || !reader.skipBlob())
                return std::nullopt;
        }
        const auto encodedHash = reader.finishAndVerifyTrailer();
        if (!encodedHash)
            return std::nullopt;
        return SnapshotPayloadFileInfo{std::move(*manifest), reader.encodedSize(), *encodedHash};
    });
}

}  // namespace timestar::data
