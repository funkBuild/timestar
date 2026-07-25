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

// ---------------------------------------------------------------------------
// Wire/journal formats. BOTH are decodable forever -- the Raft journal stores
// encoded commands, so a cluster restarting on a newer binary replays entries
// written by an older one, and a rolling upgrade has peers of both vintages.
//
// v1 (original, unversioned): little-endian, every integer fixed width.
//     u64 schemaVersion | u32 seriesCount | series* | u64 fnv1a(body)
//     series := u8 type | u32 keyLen | key | u32 count | count*u64 ts |
//               values | u32 revCount | revCount*u64
// v2 ("TSW2" magic prefix): identical EXCEPT that a series' timestamps are
//     delta-encoded -- first as a fixed u64, the rest as zigzag varints of the
//     delta from the previous one. Timestamps are monotone per series on the
//     canonical path, so the 8 bytes/point become 1-3 and a float point drops
//     from 16 wire bytes to ~10.
//
// The value columns are byte-identical in both: doubles are stored as raw IEEE
// bits, so NaN payloads, +/-Inf and -0.0 survive exactly, and int64 keeps full
// 64-bit precision.
constexpr char kV2Magic[4] = {'T', 'S', 'W', '2'};
constexpr size_t kTrailerBytes = 8;  // fnv1a checksum

// A varint is at most 10 bytes (64 bits / 7).
constexpr size_t kMaxVarintBytes = 10;

// Bulk writer: one append (and at most one reallocation, given reserve()) per
// FIELD instead of one bounds-checked push_back per BYTE. The v1 byte layout is
// unchanged -- this is the same treatment the agg-partial codec got in 2aa909d.
// A 10k-point slice used to cost ~160k push_backs.
struct Writer {
    std::string out;
    void u8(uint8_t v) { out.push_back(static_cast<char>(v)); }
    void u32(uint32_t v) {
        char b[4];
        for (int i = 0; i < 4; ++i)
            b[i] = static_cast<char>((v >> (8 * i)) & 0xff);
        out.append(b, 4);
    }
    void u64(uint64_t v) {
        char b[8];
        for (int i = 0; i < 8; ++i)
            b[i] = static_cast<char>((v >> (8 * i)) & 0xff);
        out.append(b, 8);
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
    // Whole fixed-width column in one resize + fill: no per-element capacity check.
    void u64Column(const uint64_t* p, size_t n) {
        const size_t off = out.size();
        out.resize(off + n * 8);
        char* d = out.data() + off;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t v = p[i];
            for (int b = 0; b < 8; ++b)
                d[i * 8 + b] = static_cast<char>((v >> (8 * b)) & 0xff);
        }
    }
    void dblColumn(const double* p, size_t n) {
        const size_t off = out.size();
        out.resize(off + n * 8);
        char* d = out.data() + off;
        for (size_t i = 0; i < n; ++i) {
            uint64_t v;
            std::memcpy(&v, &p[i], 8);
            for (int b = 0; b < 8; ++b)
                d[i * 8 + b] = static_cast<char>((v >> (8 * b)) & 0xff);
        }
    }
    void varint(uint64_t v) {
        char b[kMaxVarintBytes];
        size_t n = 0;
        while (v >= 0x80) {
            b[n++] = static_cast<char>(static_cast<uint8_t>(v) | 0x80);
            v >>= 7;
        }
        b[n++] = static_cast<char>(static_cast<uint8_t>(v));
        out.append(b, n);
    }
    // Zigzag so a negative delta (a non-monotone timestamp) stays short.
    void zigzag(int64_t v) { varint((static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63)); }
};

// Bulk reader: ONE bounds check per field (or per column) instead of one per
// byte, and the same ok=false / zero-result contract on a short buffer. Every
// count is bound-checked against the bytes actually remaining BEFORE it is used
// to reserve or loop, so a hostile frame can neither over-allocate nor over-read.
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
        if (!ok || !avail(4)) {
            ok = false;
            return 0;
        }
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
        p += 4;
        return v;
    }
    uint64_t u64() {
        if (!ok || !avail(8)) {
            ok = false;
            return 0;
        }
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
        p += 8;
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
    // A count field can never exceed the remaining bytes divided by the minimum
    // per-element size -- reject an inflated count before reserving/looping.
    bool boundCount(uint64_t n, size_t minPer) {
        if (!ok || n > static_cast<uint64_t>(end - p) / (minPer ? minPer : 1)) {
            ok = false;
            return false;
        }
        return true;
    }
    // Fixed-width columns: one bounds check for the whole column.
    void u64Column(std::vector<uint64_t>& out, uint32_t n) {
        if (!ok || !avail(static_cast<size_t>(n) * 8)) {
            ok = false;
            return;
        }
        out.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
            uint64_t v = 0;
            for (int b = 0; b < 8; ++b)
                v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i * 8 + b])) << (8 * b);
            out[i] = v;
        }
        p += static_cast<size_t>(n) * 8;
    }
    void dblColumn(std::vector<double>& out, uint32_t n) {
        if (!ok || !avail(static_cast<size_t>(n) * 8)) {
            ok = false;
            return;
        }
        out.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
            uint64_t v = 0;
            for (int b = 0; b < 8; ++b)
                v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i * 8 + b])) << (8 * b);
            std::memcpy(&out[i], &v, 8);
        }
        p += static_cast<size_t>(n) * 8;
    }
    uint64_t varint() {
        uint64_t v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (!ok || !avail(1)) {
                ok = false;
                return 0;
            }
            const uint8_t b = static_cast<uint8_t>(*p++);
            v |= static_cast<uint64_t>(b & 0x7f) << shift;
            if ((b & 0x80) == 0)
                return v;
        }
        ok = false;  // > 10 continuation bytes: malformed
        return 0;
    }
    int64_t zigzag() {
        const uint64_t v = varint();
        return static_cast<int64_t>((v >> 1) ^ (~(v & 1) + 1));
    }
};

// Exact encoded size of `batch` in v1. Used to reserve once up front: v1 hits it
// exactly, and v2 is smaller in every realistic case (varint deltas beat 8-byte
// timestamps unless timestamps jump by >2^49 between adjacent points), so this is
// a good single-allocation target for both.
size_t v1EncodedSize(const WriteBatch& batch) {
    size_t n = 8 + 4 + kTrailerBytes;
    for (const auto& s : batch.series) {
        const size_t count = s.timestamps.size();
        n += 1 + 4 + s.seriesKey.size() + 4 + count * 8 + 4 + s.revisions.size() * 8;
        switch (s.type) {
            case TSMValueType::Float:
            case TSMValueType::Integer:
                n += count * 8;
                break;
            case TSMValueType::Boolean:
                n += count;
                break;
            case TSMValueType::String:
                for (const std::string& v : std::get<3>(s.values))
                    n += 4 + v.size();
                break;
        }
    }
    return n;
}

// One series' body, shared by both versions -- only the timestamp column differs.
void encodeSeries(Writer& w, const WriteSeries& s, uint32_t version) {
    w.u8(static_cast<uint8_t>(s.type));
    w.str(s.seriesKey);
    const uint32_t count = static_cast<uint32_t>(s.timestamps.size());
    w.u32(count);
    if (version >= kWriteBatchFormatV2) {
        if (count > 0) {
            w.u64(s.timestamps[0]);
            for (uint32_t i = 1; i < count; ++i)
                w.zigzag(static_cast<int64_t>(s.timestamps[i] - s.timestamps[i - 1]));
        }
    } else {
        w.u64Column(s.timestamps.data(), count);
    }
    switch (s.type) {
        case TSMValueType::Float: {
            const auto& v = std::get<0>(s.values);
            w.dblColumn(v.data(), v.size());
            break;
        }
        case TSMValueType::Integer: {
            const auto& v = std::get<1>(s.values);
            // int64 -> u64 is value-preserving two's complement; the column is
            // written as raw 64-bit words, so precision is exact in both directions.
            w.u64Column(reinterpret_cast<const uint64_t*>(v.data()), v.size());
            break;
        }
        case TSMValueType::Boolean: {
            const auto& v = std::get<2>(s.values);
            // vector<bool> is a bitfield -- no contiguous buffer to bulk-copy, but the
            // output still grows in one resize.
            const size_t off = w.out.size();
            w.out.resize(off + v.size());
            for (size_t i = 0; i < v.size(); ++i)
                w.out[off + i] = v[i] ? '\1' : '\0';
            break;
        }
        case TSMValueType::String:
            for (const std::string& v : std::get<3>(s.values))
                w.str(v);
            break;
    }
    w.u32(static_cast<uint32_t>(s.revisions.size()));
    w.u64Column(s.revisions.data(), s.revisions.size());
}

// One series' body. Returns false on any malformed/inconsistent input.
bool decodeSeries(Reader& r, WriteSeries& s, uint32_t version) {
    uint8_t t = r.u8();
    if (!r.ok || t > static_cast<uint8_t>(TSMValueType::Integer))
        return false;
    s.type = static_cast<TSMValueType>(t);
    s.seriesKey = r.str();
    uint32_t count = r.u32();
    if (version >= kWriteBatchFormatV2) {
        // A delta is >= 1 byte, so the count cannot exceed the bytes remaining.
        if (!r.boundCount(count, 1))
            return false;
        s.timestamps.reserve(count);
        uint64_t prev = 0;
        for (uint32_t k = 0; k < count; ++k) {
            prev = (k == 0) ? r.u64() : prev + static_cast<uint64_t>(r.zigzag());
            if (!r.ok)
                return false;
            s.timestamps.push_back(prev);
        }
    } else {
        if (!r.boundCount(count, 8))  // each ts is 8B (floor bound)
            return false;
        r.u64Column(s.timestamps, count);
    }
    switch (s.type) {
        case TSMValueType::Float: {
            std::vector<double> v;
            r.dblColumn(v, count);
            s.values = std::move(v);
            break;
        }
        case TSMValueType::Integer: {
            std::vector<uint64_t> raw;
            r.u64Column(raw, count);
            std::vector<int64_t> v(raw.size());
            for (size_t i = 0; i < raw.size(); ++i)
                v[i] = static_cast<int64_t>(raw[i]);
            s.values = std::move(v);
            break;
        }
        case TSMValueType::Boolean: {
            if (!r.ok || !r.avail(count)) {
                r.ok = false;
                return false;
            }
            std::vector<bool> v(count);
            for (uint32_t k = 0; k < count; ++k)
                v[k] = r.p[k] != 0;
            r.p += count;
            s.values = std::move(v);
            break;
        }
        case TSMValueType::String: {
            std::vector<std::string> v;
            if (!r.boundCount(count, 4))  // each string carries a 4-byte length prefix
                return false;
            v.reserve(count);
            for (uint32_t k = 0; k < count; ++k)
                v.push_back(r.str());
            s.values = std::move(v);
            break;
        }
    }
    uint32_t nrev = r.u32();
    if (!r.ok || (nrev != 0 && nrev != count) || !r.boundCount(nrev, 8))
        return false;
    r.u64Column(s.revisions, nrev);
    return r.ok && s.consistent();
}

std::optional<WriteBatch> decodeBody(const char* body, size_t bodyLen, uint32_t version) {
    Reader r{body, body + bodyLen};
    WriteBatch batch;
    batch.schemaVersion = r.u64();
    uint32_t ns = r.u32();
    // Each series needs at least type(1)+keyLen(4)+count(4)+revCount(4) = 13 bytes.
    if (!r.boundCount(ns, 13))
        return std::nullopt;
    batch.series.reserve(ns);
    for (uint32_t i = 0; i < ns; ++i) {
        // s.vshard stays kUnroutedVShard: the routing hint is never on the wire, so a
        // decoded series is routed by re-deriving it from seriesKey (see vshardOf).
        WriteSeries s;
        if (!decodeSeries(r, s, version))
            return std::nullopt;
        batch.series.push_back(std::move(s));
    }
    if (!r.ok || r.p != r.end)
        return std::nullopt;  // trailing garbage
    return batch;
}

// Verify the fnv trailer and hand back the body span (checksum covers everything
// before the trailer, magic included).
bool checkTrailer(const std::string& bytes, size_t& bodyLen) {
    if (bytes.size() < kTrailerBytes)
        return false;
    bodyLen = bytes.size() - kTrailerBytes;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    return fnv1a(bytes.data(), bodyLen) == stored;
}

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
    return encodeWriteBatch(batch, kWriteBatchFormatV1);
}

std::string encodeWriteBatch(const WriteBatch& batch, uint32_t version) {
    // Emit the highest format we know that the caller says the reader accepts. A
    // caller never gets a format newer than it asked for, so an unknown (future)
    // version degrades to the newest we can actually write rather than mis-framing.
    const uint32_t v = version >= kWriteBatchFormatV2 ? kWriteBatchFormatV2 : kWriteBatchFormatV1;
    Writer w;
    w.out.reserve(v1EncodedSize(batch));
    if (v >= kWriteBatchFormatV2)
        w.out.append(kV2Magic, sizeof(kV2Magic));
    w.u64(batch.schemaVersion);
    w.u32(static_cast<uint32_t>(batch.series.size()));
    for (const auto& s : batch.series)
        encodeSeries(w, s, v);
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes) {
    size_t bodyLen = 0;
    if (!checkTrailer(bytes, bodyLen))
        return std::nullopt;
    // Version sniff. A v2 frame is self-identifying; anything else is v1 (which has
    // no version field at all -- it starts straight in on schemaVersion). If a v1
    // frame's schemaVersion low bytes ever happened to spell the magic, the v2 decode
    // fails and we still fall back to v1, so the sniff can only ever be an
    // optimisation, never a way to lose a decodable frame.
    if (bodyLen >= sizeof(kV2Magic) && std::memcmp(bytes.data(), kV2Magic, sizeof(kV2Magic)) == 0) {
        if (auto v2 = decodeBody(bytes.data() + sizeof(kV2Magic), bodyLen - sizeof(kV2Magic), kWriteBatchFormatV2))
            return v2;
    }
    return decodeBody(bytes.data(), bodyLen, kWriteBatchFormatV1);
}

}  // namespace timestar::data
