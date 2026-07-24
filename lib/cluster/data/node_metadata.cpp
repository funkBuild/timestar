#include "node_metadata.hpp"

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

struct Writer {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void u64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            u8((v >> (8 * i)) & 0xff);
    }
    void dbl(double v) {
        uint64_t b;
        std::memcpy(&b, &v, 8);
        u64(b);
    }
    void str(const std::string& s) {
        u32(static_cast<uint32_t>(s.size()));
        out.append(s);
    }
    std::string finish() {
        u64(fnv1a(out.data(), out.size()));
        return std::move(out);
    }
};

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
    double dbl() {
        uint64_t b = u64();
        double d;
        std::memcpy(&d, &b, 8);
        return d;
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

// Verify + strip the 8-byte FNV trailer; returns body length or npos.
size_t verify(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::string::npos;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    return fnv1a(bytes.data(), bodyLen) == stored ? bodyLen : std::string::npos;
}

}  // namespace

std::string encodeMetadataRequest(const MetadataRequest& req) {
    Writer w;
    w.u8(static_cast<uint8_t>(req.kind));
    w.str(req.measurement);
    w.str(req.tagKey);
    w.str(req.tagValue);
    return w.finish();
}

std::optional<MetadataRequest> decodeMetadataRequest(const std::string& bytes) {
    size_t bodyLen = verify(bytes);
    if (bodyLen == std::string::npos)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    MetadataRequest req;
    uint8_t kind = r.u8();
    if (!r.ok || kind > static_cast<uint8_t>(MetadataKind::TagCardinality))
        return std::nullopt;
    req.kind = static_cast<MetadataKind>(kind);
    req.measurement = r.str();
    req.tagKey = r.str();
    req.tagValue = r.str();
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return req;
}

std::string encodeMetadataResult(const MetadataResult& res) {
    Writer w;
    w.u32(static_cast<uint32_t>(res.items.size()));
    for (const auto& s : res.items)
        w.str(s);
    w.dbl(res.cardinality);
    return w.finish();
}

std::optional<MetadataResult> decodeMetadataResult(const std::string& bytes) {
    size_t bodyLen = verify(bytes);
    if (bodyLen == std::string::npos)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    MetadataResult res;
    uint32_t n = r.u32();
    if (!r.ok || n > static_cast<uint64_t>(r.end - r.p) / 4)  // each string >= 4B length prefix
        return std::nullopt;
    res.items.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        res.items.push_back(r.str());
        if (!r.ok)
            return std::nullopt;
    }
    res.cardinality = r.dbl();
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return res;
}

}  // namespace timestar::data
