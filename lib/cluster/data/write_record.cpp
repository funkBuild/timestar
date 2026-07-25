#include "write_record.hpp"

#include <algorithm>
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
};

}  // namespace

bool WriteSeries::consistent() const {
    const size_t n = timestamps.size();
    if (!revisions.empty() && revisions.size() != n)
        return false;
    switch (type) {
        case TSMValueType::Float:
            return values.index() == 0 && std::get<0>(values).size() == n;
        case TSMValueType::Integer:
            return values.index() == 1 && std::get<1>(values).size() == n;
        case TSMValueType::Boolean:
            return values.index() == 2 && std::get<2>(values).size() == n;
        case TSMValueType::String:
            return values.index() == 3 && std::get<3>(values).size() == n;
    }
    return false;
}

VShardBatches splitByVShard(WriteBatch batch) {
    VShardBatches out;
    if (batch.series.empty())
        return out;
    // vshard -> index into `out`. A direct-indexed table (4096 x int32 = 16 KB, one
    // fill per split) rather than a hash map: a 10k-point HTTP batch can touch a
    // thousand distinct VShards, and this runs once per batch now.
    std::vector<int32_t> slot(timestar::VIRTUAL_SHARD_COUNT, -1);
    out.reserve(std::min<size_t>(batch.series.size(), timestar::VIRTUAL_SHARD_COUNT));
    for (auto& s : batch.series) {
        const uint16_t vs = vshardOf(s);
        int32_t& idx = slot[vs];
        if (idx < 0) {
            idx = static_cast<int32_t>(out.size());
            out.push_back({vs, WriteBatch{}});
            out.back().second.schemaVersion = batch.schemaVersion;
        }
        out[static_cast<size_t>(idx)].second.series.push_back(std::move(s));
    }
    return out;
}

WriteBatch mergeVShardBatches(VShardBatches groups) {
    WriteBatch out;
    size_t n = 0;
    for (const auto& [vs, b] : groups)
        n += b.series.size();
    out.series.reserve(n);
    for (auto& [vs, b] : groups) {
        out.schemaVersion = b.schemaVersion;  // identical across groups of one batch
        for (auto& s : b.series)
            out.series.push_back(std::move(s));
    }
    return out;
}

std::string encodeWriteBatch(const WriteBatch& batch) {
    Writer w;
    w.u64(batch.schemaVersion);
    w.u32(static_cast<uint32_t>(batch.series.size()));
    for (const auto& s : batch.series) {
        w.u8(static_cast<uint8_t>(s.type));
        w.str(s.seriesKey);
        const uint32_t count = static_cast<uint32_t>(s.timestamps.size());
        w.u32(count);
        for (uint64_t ts : s.timestamps)
            w.u64(ts);
        switch (s.type) {
            case TSMValueType::Float:
                for (double v : std::get<0>(s.values))
                    w.dbl(v);
                break;
            case TSMValueType::Integer:
                for (int64_t v : std::get<1>(s.values))
                    w.u64(static_cast<uint64_t>(v));
                break;
            case TSMValueType::Boolean:
                for (bool v : std::get<2>(s.values))
                    w.u8(v ? 1 : 0);
                break;
            case TSMValueType::String:
                for (const std::string& v : std::get<3>(s.values))
                    w.str(v);
                break;
        }
        w.u32(static_cast<uint32_t>(s.revisions.size()));
        for (uint64_t r : s.revisions)
            w.u64(r);
    }
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    Reader r{bytes.data(), bytes.data() + bodyLen};
    WriteBatch batch;
    batch.schemaVersion = r.u64();
    uint32_t ns = r.u32();
    // Each series needs at least type(1)+keyLen(4)+count(4)+revCount(4) = 13 bytes.
    if (!r.ok || ns > static_cast<uint64_t>(r.end - r.p) / 13)
        return std::nullopt;
    batch.series.reserve(ns);
    for (uint32_t i = 0; i < ns; ++i) {
        // s.vshard stays kUnroutedVShard: the routing hint is never on the wire, so a
        // decoded series is routed by re-deriving it from seriesKey (see vshardOf).
        WriteSeries s;
        uint8_t t = r.u8();
        if (!r.ok || t > static_cast<uint8_t>(TSMValueType::Integer))
            return std::nullopt;
        s.type = static_cast<TSMValueType>(t);
        s.seriesKey = r.str();
        uint32_t count = r.u32();
        if (!r.ok || count > static_cast<uint64_t>(r.end - r.p) / 8)  // each ts is 8B (floor bound)
            return std::nullopt;
        s.timestamps.reserve(count);
        for (uint32_t k = 0; k < count; ++k)
            s.timestamps.push_back(r.u64());
        switch (s.type) {
            case TSMValueType::Float: {
                std::vector<double> v;
                v.reserve(count);
                for (uint32_t k = 0; k < count; ++k)
                    v.push_back(r.dbl());
                s.values = std::move(v);
                break;
            }
            case TSMValueType::Integer: {
                std::vector<int64_t> v;
                v.reserve(count);
                for (uint32_t k = 0; k < count; ++k)
                    v.push_back(static_cast<int64_t>(r.u64()));
                s.values = std::move(v);
                break;
            }
            case TSMValueType::Boolean: {
                std::vector<bool> v;
                v.reserve(count);
                for (uint32_t k = 0; k < count; ++k)
                    v.push_back(r.u8() != 0);
                s.values = std::move(v);
                break;
            }
            case TSMValueType::String: {
                std::vector<std::string> v;
                v.reserve(count);
                for (uint32_t k = 0; k < count; ++k)
                    v.push_back(r.str());
                s.values = std::move(v);
                break;
            }
        }
        uint32_t nrev = r.u32();
        if (!r.ok || (nrev != 0 && nrev != count) || nrev > static_cast<uint64_t>(r.end - r.p) / 8)
            return std::nullopt;
        s.revisions.reserve(nrev);
        for (uint32_t k = 0; k < nrev; ++k)
            s.revisions.push_back(r.u64());
        if (!r.ok || !s.consistent())
            return std::nullopt;
        batch.series.push_back(std::move(s));
    }
    if (r.p != r.end)
        return std::nullopt;  // trailing garbage
    return batch;
}

}  // namespace timestar::data
