#include "node_query.hpp"

#include "../../core/field_values.hpp"  // FieldValues
#include "agg_partial_codec.hpp"        // encodePartials/decodePartials

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
    void u16(uint16_t v) {
        u8(v & 0xff);
        u8((v >> 8) & 0xff);
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
    void strvec(const std::vector<std::string>& v) {
        u32(static_cast<uint32_t>(v.size()));
        for (const auto& s : v)
            str(s);
    }
    void strmap(const std::map<std::string, std::string>& m) {
        u32(static_cast<uint32_t>(m.size()));
        for (const auto& [k, v] : m) {
            str(k);
            str(v);
        }
    }
};

struct Reader {
    const char* p;
    const char* end;
    bool ok = true;
    bool avail(size_t n) const { return static_cast<size_t>(end - p) >= n; }
    uint8_t u8() {
        if (!avail(1)) {
            ok = false;
            return 0;
        }
        return static_cast<uint8_t>(*p++);
    }
    uint16_t u16() {
        uint16_t a = u8();
        uint16_t b = u8();
        return static_cast<uint16_t>(a | (b << 8));
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
        double v;
        std::memcpy(&v, &b, 8);
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
    std::vector<std::string> strvec() {
        uint32_t n = u32();
        std::vector<std::string> v;
        if (!ok || n > static_cast<uint64_t>(end - p) / 4)  // each string >= 4B length prefix
            ok = false;
        if (!ok)
            return v;
        v.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            v.push_back(str());
            if (!ok)
                break;
        }
        return v;
    }
    std::map<std::string, std::string> strmap() {
        uint32_t n = u32();
        std::map<std::string, std::string> m;
        if (!ok || n > static_cast<uint64_t>(end - p) / 8)  // each entry >= 8B (two length prefixes)
            ok = false;
        for (uint32_t i = 0; i < n && ok; ++i) {
            std::string k = str();
            std::string v = str();
            if (ok)
                m.emplace(std::move(k), std::move(v));
        }
        return m;
    }
};

void encodeRequest(Writer& w, const timestar::QueryRequest& q) {
    w.u8(static_cast<uint8_t>(q.aggregation));
    w.str(q.measurement);
    w.strvec(q.fields);
    w.strmap(q.scopes);
    w.strvec(q.groupByTags);
    w.u64(q.startTime);
    w.u64(q.endTime);
    w.u64(q.aggregationInterval);
    w.u64(q.bucketAnchor);
    w.u8(q.booleansAsNumeric ? 1 : 0);
    // Read consistency (M4): the serving node needs the mode to decide whether it may
    // answer as a replica and how fresh it must be, so it MUST cross the wire (like
    // every other field that changes behaviour).
    w.u8(static_cast<uint8_t>(q.readConsistency));
    w.u64(q.maxReadLagIndex);
}

bool decodeRequest(Reader& r, timestar::QueryRequest& q) {
    // Range-check the method: a hostile (but checksum-valid) byte must not hand
    // executeQuery an out-of-range scoped-enum value. SPREAD is the last method;
    // referencing it keeps the bound correct if methods are added.
    uint8_t method = r.u8();
    if (!r.ok || method > static_cast<uint8_t>(timestar::AggregationMethod::SPREAD))
        return false;
    q.aggregation = static_cast<timestar::AggregationMethod>(method);
    q.measurement = r.str();
    q.fields = r.strvec();
    q.scopes = r.strmap();
    q.groupByTags = r.strvec();
    q.startTime = r.u64();
    q.endTime = r.u64();
    q.aggregationInterval = r.u64();
    q.bucketAnchor = r.u64();
    q.booleansAsNumeric = r.u8() != 0;
    // Range-check the consistency mode like the method: a checksum-valid but hostile
    // byte must not yield an out-of-range scoped enum. BoundedStaleness is the last.
    uint8_t consistency = r.u8();
    if (!r.ok || consistency > static_cast<uint8_t>(timestar::ReadConsistencyMode::BoundedStaleness))
        return false;
    q.readConsistency = static_cast<timestar::ReadConsistencyMode>(consistency);
    q.maxReadLagIndex = r.u64();
    return r.ok;
}

// FieldValues = variant<vector<double>, vector<bool>, vector<string>, vector<int64_t>>.
void encodeFieldValues(Writer& w, const FieldValues& fv) {
    w.u8(static_cast<uint8_t>(fv.index()));
    switch (fv.index()) {
        case 0:
            for (double v : std::get<0>(fv))
                w.dbl(v);
            break;
        case 1:
            for (bool v : std::get<1>(fv))
                w.u8(v ? 1 : 0);
            break;
        case 2:
            for (const std::string& v : std::get<2>(fv))
                w.str(v);
            break;
        case 3:
            for (int64_t v : std::get<3>(fv))
                w.u64(static_cast<uint64_t>(v));
            break;
    }
}

bool decodeFieldValues(Reader& r, uint32_t count, FieldValues& fv) {
    uint8_t idx = r.u8();
    if (!r.ok)
        return false;
    switch (idx) {
        case 0: {
            std::vector<double> v;
            v.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
                v.push_back(r.dbl());
            fv = std::move(v);
            break;
        }
        case 1: {
            std::vector<bool> v;
            v.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
                v.push_back(r.u8() != 0);
            fv = std::move(v);
            break;
        }
        case 2: {
            std::vector<std::string> v;
            v.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
                v.push_back(r.str());
            fv = std::move(v);
            break;
        }
        case 3: {
            std::vector<int64_t> v;
            v.reserve(count);
            for (uint32_t i = 0; i < count; ++i)
                v.push_back(static_cast<int64_t>(r.u64()));
            fv = std::move(v);
            break;
        }
        default:
            return false;  // unknown variant tag
    }
    return r.ok;
}

std::string finish(Writer& w) {
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

// Verify + strip the 8-byte FNV trailer; returns the body length or npos.
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

std::string encodeNodeQueryRequest(const NodeQueryRequest& req) {
    Writer w;
    encodeRequest(w, req.request);
    w.u32(static_cast<uint32_t>(req.vshards.size()));
    for (uint16_t v : req.vshards)
        w.u16(v);
    w.u64(req.taskId);
    w.u64(req.mapEpoch);
    return finish(w);
}

std::optional<NodeQueryRequest> decodeNodeQueryRequest(const std::string& bytes) {
    size_t bodyLen = verify(bytes);
    if (bodyLen == std::string::npos)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    NodeQueryRequest req;
    if (!decodeRequest(r, req.request))
        return std::nullopt;
    uint32_t nv = r.u32();
    if (!r.ok || nv > static_cast<uint64_t>(r.end - r.p) / 2)
        return std::nullopt;
    req.vshards.reserve(nv);
    for (uint32_t i = 0; i < nv; ++i)
        req.vshards.push_back(r.u16());
    req.taskId = r.u64();
    req.mapEpoch = r.u64();
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return req;
}

std::string encodeNodeQueryPartial(const NodeQueryPartial& partial) {
    Writer w;
    // Numeric partials as a length-prefixed sub-blob (encodePartials carries its
    // own FNV; str() gives it a bounds-checked length prefix here).
    w.str(encodePartials(partial.partials));
    // Non-numeric series (passthrough, one field per series), same wire shape as
    // before.
    w.u32(static_cast<uint32_t>(partial.nonNumeric.size()));
    for (const auto& s : partial.nonNumeric) {
        w.str(s.measurement);
        w.strmap(s.tags);
        w.u32(static_cast<uint32_t>(s.fields.size()));
        for (const auto& [name, tv] : s.fields) {
            w.str(name);
            w.u32(static_cast<uint32_t>(tv.first.size()));  // timestamp count
            for (uint64_t ts : tv.first)
                w.u64(ts);
            encodeFieldValues(w, tv.second);
        }
    }
    w.u64(partial.seriesFound);
    w.strvec(partial.incompleteReasons);
    return finish(w);
}

std::optional<NodeQueryPartial> decodeNodeQueryPartial(const std::string& bytes) {
    size_t bodyLen = verify(bytes);
    if (bodyLen == std::string::npos)
        return std::nullopt;
    Reader r{bytes.data(), bytes.data() + bodyLen};
    NodeQueryPartial partial;
    // Numeric partials sub-blob (its own FNV verified inside decodePartials).
    std::string partialsBlob = r.str();
    if (!r.ok)
        return std::nullopt;
    auto decodedPartials = decodePartials(partialsBlob);
    if (!decodedPartials)
        return std::nullopt;
    partial.partials = std::move(*decodedPartials);

    uint32_t ns = r.u32();
    // Min bytes per SeriesResult: measurement len(4) + tags count(4) + fields
    // count(4) = 12. Bounds the reserve; every field read below is still avail-checked.
    if (!r.ok || ns > static_cast<uint64_t>(r.end - r.p) / 12)
        return std::nullopt;
    partial.nonNumeric.reserve(ns);
    for (uint32_t i = 0; i < ns; ++i) {
        timestar::http::SeriesResult s;
        s.measurement = r.str();
        s.tags = r.strmap();
        uint32_t nf = r.u32();
        // Min bytes per field: name len(4) + nts(4) + type byte(1) = 9.
        if (!r.ok || nf > static_cast<uint64_t>(r.end - r.p) / 9)
            return std::nullopt;
        for (uint32_t f = 0; f < nf; ++f) {
            std::string name = r.str();
            uint32_t nts = r.u32();
            if (!r.ok || nts > static_cast<uint64_t>(r.end - r.p) / 8)
                return std::nullopt;
            std::vector<uint64_t> ts;
            ts.reserve(nts);
            for (uint32_t k = 0; k < nts; ++k)
                ts.push_back(r.u64());
            FieldValues fv;
            if (!decodeFieldValues(r, nts, fv))
                return std::nullopt;
            s.fields.emplace(std::move(name), std::make_pair(std::move(ts), std::move(fv)));
        }
        if (!r.ok)
            return std::nullopt;
        partial.nonNumeric.push_back(std::move(s));
    }
    partial.seriesFound = r.u64();
    partial.incompleteReasons = r.strvec();
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return partial;
}

}  // namespace timestar::data
