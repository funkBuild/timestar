#include "write_record.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

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
// v1 is explicitly framed with "TSW1". Timestamps are delta encoded: the first
// is a fixed u64 and the rest are zigzag varints. Doubles are stored as raw IEEE
// bits, so NaN payloads, +/-Inf and -0.0 survive exactly, and int64 keeps full
// 64-bit precision.
constexpr char kV1Magic[4] = {'T', 'S', 'W', '1'};
static_assert(sizeof(kV1Magic) == kWriteBatchV1MagicBytes, "the charge arithmetic counts this magic");
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

// How many elements a decoder may pre-reserve on the STRENGTH OF A DECLARED COUNT
// alone, before the bytes backing them have actually been consumed.
//
// A count is bound-checked against the bytes remaining, which stops an over-READ but
// NOT an over-ALLOCATION whenever the in-memory element is bigger than its minimum
// wire footprint: a delta timestamp is >= 1 wire byte but 8 bytes resident (8x),
// and a string is >= 4 wire bytes but sizeof(std::string) = 32 resident (8x). A
// checksum-valid 16 MiB frame declaring 16.7M timestamps allocated ~134 MB up
// front -- the frame's own size is the only thing an inbound RPC bounds, and neither
// DataPlaneRpc nor RaftRpcTransport sets rpc::resource_limits today.
//
// So: reserve at most this many elements up front and let push_back grow
// geometrically as bytes are ACTUALLY consumed. Peak allocation then tracks what was
// really decoded (within the vector's own 2x growth slack) instead of what a frame
// merely claimed, and a frame that lies is rejected having allocated ~32 KB. The
// residual 8-bytes-resident-per-wire-byte ratio of a genuinely dense frame is
// inherent to delta encoding and is a matter for an inbound frame-size limit, not for
// the decoder.
constexpr size_t kMaxPrereserveElems = 4096;

// Bulk reader: ONE bounds check per field (or per column) instead of one per
// byte, and the same ok=false / zero-result contract on a short buffer. Every
// count is bound-checked against the bytes actually remaining BEFORE it is used to
// loop -- and every element-count reserve is ADDITIONALLY capped (see
// kMaxPrereserveElems), because the bounds check alone does not bound memory.
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

size_t varintBytes(uint64_t value) {
    size_t bytes = 1;
    while (value >= 0x80) {
        value >>= 7;
        ++bytes;
    }
    return bytes;
}

size_t encodedSeriesBytes(const WriteSeries& s) {
    if (!s.consistent())
        throw std::invalid_argument("inconsistent WriteSeries");
    const size_t count = s.timestamps.size();
    size_t n = 1 + 4 + s.seriesKey.size() + 4 + (count == 0 ? 0 : 8) + 4 + s.revisions.size() * 8;
    for (size_t i = 1; i < count; ++i) {
        const auto delta = static_cast<int64_t>(s.timestamps[i] - s.timestamps[i - 1]);
        const uint64_t zigzag = (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
        n += varintBytes(zigzag);
    }
    switch (s.type) {
        case TSMValueType::Float:
            n += std::get<0>(s.values).size() * 8;
            break;
        case TSMValueType::Integer:
            n += std::get<1>(s.values).size() * 8;
            break;
        case TSMValueType::Boolean:
            n += std::get<2>(s.values).size();
            break;
        case TSMValueType::String:
            for (const std::string& value : std::get<3>(s.values))
                n += 4 + value.size();
            break;
    }
    return n;
}

void encodeSeries(Writer& w, const WriteSeries& s) {
    w.u8(static_cast<uint8_t>(s.type));
    w.str(s.seriesKey);
    const uint32_t count = static_cast<uint32_t>(s.timestamps.size());
    w.u32(count);
    if (count > 0) {
        w.u64(s.timestamps[0]);
        for (uint32_t i = 1; i < count; ++i)
            w.zigzag(static_cast<int64_t>(s.timestamps[i] - s.timestamps[i - 1]));
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

// The minimum number of WIRE bytes one POINT of this series costs -- timestamp plus
// value, which is what a declared count must actually be paid for.
//
// Bounding the count by the timestamp alone lets a dense frame amplify: a delta is
// >= 1 wire byte but 8 bytes resident, so a 16 MiB frame could declare 16.7M
// timestamps, build 134 MB of them, and only THEN fail on the value column it could
// never have contained. Every point owes a value too, so charge for it up front: a
// float point is >= 9 wire bytes, and the same frame can now declare at most ~1.86M
// of them. This rejects nothing legitimate -- it is a true lower bound on what the
// format requires -- and it caps resident growth at ~2x the frame for the densest
// legal frame (16 resident bytes per >= 9 wire bytes) instead of 8x.
//
// It is exactly tight for every value type: a densest-legal frame pays
// precisely this many bytes per point (the first timestamp costs 8 rather than 1, which
// only ever leaves slack). DO NOT RAISE IT. In particular do not add the revision
// column's 8 bytes: revisions are OPTIONAL (`nrev == 0 || nrev == count`), and a batch
// on the write path carries none at all -- charging for them would reject every real
// pre-apply frame. Any addition here must be something EVERY point provably pays for.
size_t minWireBytesPerPoint(TSMValueType type) {
    const size_t ts = 1;
    size_t value = 0;
    switch (type) {
        case TSMValueType::Float:
        case TSMValueType::Integer:
            value = 8;
            break;
        case TSMValueType::Boolean:
            value = 1;
            break;
        case TSMValueType::String:
            value = 4;  // the length prefix; the bytes themselves may be empty
            break;
    }
    return ts + value;
}

// One series' body. Returns false on any malformed/inconsistent input.
bool decodeSeries(Reader& r, WriteSeries& s) {
    uint8_t t = r.u8();
    if (!r.ok || t > static_cast<uint8_t>(TSMValueType::Integer))
        return false;
    s.type = static_cast<TSMValueType>(t);
    s.seriesKey = r.str();
    uint32_t count = r.u32();
    // Charge the count for a whole POINT (timestamp + value), not just its timestamp:
    // see minWireBytesPerPoint. The reserve is ALSO capped and grown as bytes are
    // really consumed (kMaxPrereserveElems), so neither the bound nor the reserve
    // trusts a declared count on its own.
    if (!r.boundCount(count, minWireBytesPerPoint(s.type)))
        return false;
    s.timestamps.reserve(std::min<size_t>(count, kMaxPrereserveElems));
    uint64_t prev = 0;
    for (uint32_t k = 0; k < count; ++k) {
        prev = (k == 0) ? r.u64() : prev + static_cast<uint64_t>(r.zigzag());
        if (!r.ok)
            return false;
        s.timestamps.push_back(prev);
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
            // 4 wire bytes (the length prefix) buys 32 resident, so cap the reserve.
            v.reserve(std::min<size_t>(count, kMaxPrereserveElems));
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

std::optional<WriteBatch> decodeBody(const char* body, size_t bodyLen) {
    Reader r{body, body + bodyLen};
    WriteBatch batch;
    batch.schemaVersion = r.u64();
    uint32_t ns = r.u32();
    // Each series needs at least type(1)+keyLen(4)+count(4)+revCount(4) = 13 bytes --
    // against sizeof(WriteSeries) = 144 resident, so this reserve is capped too.
    if (!r.boundCount(ns, 13))
        return std::nullopt;
    batch.series.reserve(std::min<size_t>(ns, kMaxPrereserveElems));
    for (uint32_t i = 0; i < ns; ++i) {
        // s.vshard stays kUnroutedVShard: the routing hint is never on the wire, so a
        // decoded series is routed by re-deriving it from seriesKey (see vshardOf).
        WriteSeries s;
        if (!decodeSeries(r, s))
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

VShardBatchView viewOf(const VShardBatches& groups) {
    VShardBatchView view;
    view.reserve(groups.size());
    for (const auto& g : groups)
        view.push_back(&g);
    return view;
}

namespace {
size_t seriesResidentBytes(const WriteSeries& s) {
    size_t n = s.seriesKey.size() + s.timestamps.size() * 8 + s.revisions.size() * 8;
    switch (s.type) {
        case TSMValueType::Float:
            n += std::get<0>(s.values).size() * 8;
            break;
        case TSMValueType::Integer:
            n += std::get<1>(s.values).size() * 8;
            break;
        case TSMValueType::Boolean:
            n += std::get<2>(s.values).size();
            break;
        case TSMValueType::String:
            for (const std::string& v : std::get<3>(s.values))
                n += v.size() + sizeof(std::string);
            break;
    }
    return n;
}
}  // namespace

size_t approxResidentBytes(const WriteBatch& batch) {
    size_t n = 0;
    for (const auto& s : batch.series)
        n += seriesResidentBytes(s);
    return n;
}

size_t approxResidentBytes(const VShardBatchView& view) {
    size_t n = 0;
    for (const auto* g : view)
        n += approxResidentBytes(g->second);
    return n;
}

size_t encodedWriteBatchBytes(const WriteBatch& batch) {
    size_t bytes = sizeof(kV1Magic) + 8 + 4 + kTrailerBytes;
    for (const auto& s : batch.series)
        bytes += encodedSeriesBytes(s);
    return bytes;
}

std::string encodeWriteBatch(const WriteBatch& batch) {
    return encodeWriteBatch(batch, kWriteBatchFormatV1);
}

std::string encodeWriteBatch(const WriteBatch& batch, uint32_t version) {
    if (version != kWriteBatchFormatV1)
        throw std::invalid_argument("unsupported WriteBatch format version");
    Writer w;
    w.out.reserve(encodedWriteBatchBytes(batch));
    w.out.append(kV1Magic, sizeof(kV1Magic));
    w.u64(batch.schemaVersion);
    w.u32(static_cast<uint32_t>(batch.series.size()));
    for (const auto& s : batch.series)
        encodeSeries(w, s);
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

std::string encodeWriteBatch(const VShardBatchView& view, uint32_t version) {
    if (version != kWriteBatchFormatV1)
        throw std::invalid_argument("unsupported WriteBatch format version");
    size_t nSeries = 0, reserve = sizeof(kV1Magic) + 8 + 4 + kTrailerBytes;
    uint64_t schemaVersion = 0;
    for (const auto* g : view) {
        nSeries += g->second.series.size();
        // Identical across the groups of one batch (splitByVShard copies it into each).
        schemaVersion = g->second.schemaVersion;
        for (const auto& series : g->second.series)
            reserve += encodedSeriesBytes(series);
    }
    Writer w;
    w.out.reserve(reserve);
    w.out.append(kV1Magic, sizeof(kV1Magic));
    w.u64(schemaVersion);
    w.u32(static_cast<uint32_t>(nSeries));
    for (const auto* g : view)
        for (const auto& s : g->second.series)
            encodeSeries(w, s);
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

std::optional<WriteBatch> decodeWriteBatch(const std::string& bytes) {
    size_t bodyLen = 0;
    if (!checkTrailer(bytes, bodyLen))
        return std::nullopt;
    if (bodyLen < sizeof(kV1Magic) || std::memcmp(bytes.data(), kV1Magic, sizeof(kV1Magic)) != 0)
        return std::nullopt;
    return decodeBody(bytes.data() + sizeof(kV1Magic), bodyLen - sizeof(kV1Magic));
}

}  // namespace timestar::data
