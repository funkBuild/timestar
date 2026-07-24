#include "snapshot_payload.hpp"

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
};

}  // namespace

std::string encodeSnapshotPayload(const SnapshotPayload& payload) {
    std::string out;
    putBlob(out, payload.manifest.encode());
    putU32(out, static_cast<uint32_t>(payload.files.size()));
    for (const auto& f : payload.files) {
        putBlob(out, f.name);
        putBlob(out, f.bytes);
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
    std::string manifestBlob = r.blob();
    if (!r.ok)
        return std::nullopt;
    auto m = VShardSnapshotManifest::decode(manifestBlob);
    if (!m)
        return std::nullopt;  // corrupt or structurally-invalid manifest
    out.manifest = std::move(*m);

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

}  // namespace timestar::data
