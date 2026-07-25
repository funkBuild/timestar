#include "agg_partial_codec.hpp"

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
    // Little-endian, byte-for-byte identical to the previous per-byte loops -- this
    // only removes 4/8 separate bounds-checked push_back calls per field. A grouped
    // full-range cluster query ships ~730k points of partials from the remote nodes,
    // and unlike the write path query CPU is on the critical path.
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
    void dvec(const std::vector<double>& v) {
        u32(static_cast<uint32_t>(v.size()));
        for (double d : v)
            dbl(d);
    }
    void u64vec(const std::vector<uint64_t>& v) {
        u32(static_cast<uint32_t>(v.size()));
        for (uint64_t x : v)
            u64(x);
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
    // One bounds check per field instead of one per byte. Same little-endian layout,
    // and the same ok=false / zero result on a short buffer.
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
    // A count field can never exceed the remaining bytes divided by the minimum
    // per-element size -- reject an inflated count before reserving/looping.
    bool boundCount(uint32_t n, size_t minPer) {
        if (!ok || n > static_cast<uint64_t>(end - p) / (minPer ? minPer : 1)) {
            ok = false;
            return false;
        }
        return true;
    }
    std::vector<double> dvec() {
        uint32_t n = u32();
        std::vector<double> v;
        if (!boundCount(n, 8))
            return v;
        v.reserve(n);
        for (uint32_t i = 0; i < n; ++i)
            v.push_back(dbl());
        return v;
    }
    std::vector<uint64_t> u64vec() {
        uint32_t n = u32();
        std::vector<uint64_t> v;
        if (!boundCount(n, 8))
            return v;
        v.reserve(n);
        for (uint32_t i = 0; i < n; ++i)
            v.push_back(u64());
        return v;
    }
};

// AggregationState: 5 doubles + u64 + double + u64 + count(u64) + 2 doubles +
// rawValues(dvec) + 2 flag bytes.
void encodeState(Writer& w, const AggregationState& s) {
    w.dbl(s.sum);
    w.dbl(s.sumCompensation);
    w.dbl(s.min);
    w.dbl(s.max);
    w.dbl(s.latest);
    w.u64(s.latestTimestamp);
    w.dbl(s.first);
    w.u64(s.firstTimestamp);
    w.u64(static_cast<uint64_t>(s.count));
    w.dbl(s.m2);
    w.dbl(s.mean);
    w.dvec(s.rawValues);
    w.u8(s.rawValuesSaturated ? 1 : 0);
    w.u8(s.collectRaw ? 1 : 0);
}

AggregationState decodeState(Reader& r) {
    AggregationState s;
    s.sum = r.dbl();
    s.sumCompensation = r.dbl();
    s.min = r.dbl();
    s.max = r.dbl();
    s.latest = r.dbl();
    s.latestTimestamp = r.u64();
    s.first = r.dbl();
    s.firstTimestamp = r.u64();
    s.count = static_cast<size_t>(r.u64());
    s.m2 = r.dbl();
    s.mean = r.dbl();
    s.rawValues = r.dvec();
    s.rawValuesSaturated = r.u8() != 0;
    s.collectRaw = r.u8() != 0;
    return s;
}

// Minimum encoded size of one AggregationState (all-empty rawValues): 11 fixed
// 8-byte fields + a 4-byte rawValues count + 2 flag bytes = 94 bytes. Used to bound
// vector<AggregationState> and bucketStates counts.
constexpr size_t kMinStateBytes = 11 * 8 + 4 + 2;

void encodePartial(Writer& w, const PartialAggregationResult& p) {
    w.str(p.measurement);
    w.str(p.fieldName);
    w.str(p.groupKey);
    w.u64(static_cast<uint64_t>(p.groupKeyHash));
    // cachedTags
    w.u32(static_cast<uint32_t>(p.cachedTags.size()));
    for (const auto& [k, v] : p.cachedTags) {
        w.str(k);
        w.str(v);
    }
    w.u64(static_cast<uint64_t>(p.totalPoints));
    // bucketStates
    w.u32(static_cast<uint32_t>(p.bucketStates.size()));
    for (const auto& [bucket, st] : p.bucketStates) {
        w.u64(bucket);
        encodeState(w, st);
    }
    // sortedTimestamps / sortedStates / sortedValues
    w.u64vec(p.sortedTimestamps);
    w.u32(static_cast<uint32_t>(p.sortedStates.size()));
    for (const auto& st : p.sortedStates)
        encodeState(w, st);
    w.dvec(p.sortedValues);
    // collapsedState (optional)
    if (p.collapsedState) {
        w.u8(1);
        encodeState(w, *p.collapsedState);
    } else {
        w.u8(0);
    }
}

bool decodePartial(Reader& r, PartialAggregationResult& p) {
    p.measurement = r.str();
    p.fieldName = r.str();
    p.groupKey = r.str();
    p.groupKeyHash = static_cast<size_t>(r.u64());
    uint32_t ntags = r.u32();
    if (!r.boundCount(ntags, 8))  // each tag entry >= two 4-byte length prefixes
        return false;
    for (uint32_t i = 0; i < ntags && r.ok; ++i) {
        std::string k = r.str();
        std::string v = r.str();
        p.cachedTags.emplace(std::move(k), std::move(v));
    }
    p.totalPoints = static_cast<size_t>(r.u64());
    uint32_t nbuckets = r.u32();
    if (!r.boundCount(nbuckets, 8 + kMinStateBytes))  // bucket u64 + a state
        return false;
    for (uint32_t i = 0; i < nbuckets && r.ok; ++i) {
        uint64_t bucket = r.u64();
        AggregationState st = decodeState(r);
        if (!r.ok)
            return false;
        p.bucketStates.emplace(bucket, std::move(st));
    }
    p.sortedTimestamps = r.u64vec();
    uint32_t nstates = r.u32();
    if (!r.boundCount(nstates, kMinStateBytes))
        return false;
    p.sortedStates.reserve(nstates);
    for (uint32_t i = 0; i < nstates && r.ok; ++i) {
        AggregationState st = decodeState(r);
        if (!r.ok)
            return false;
        p.sortedStates.push_back(std::move(st));
    }
    p.sortedValues = r.dvec();
    uint8_t hasCollapsed = r.u8();
    if (!r.ok)
        return false;
    if (hasCollapsed) {
        AggregationState st = decodeState(r);
        if (!r.ok)
            return false;
        p.collapsedState = std::move(st);
    }
    return r.ok;
}

}  // namespace

std::string encodePartials(const std::vector<PartialAggregationResult>& partials) {
    Writer w;
    w.u32(static_cast<uint32_t>(partials.size()));
    for (const auto& p : partials)
        encodePartial(w, p);
    w.u64(fnv1a(w.out.data(), w.out.size()));
    return std::move(w.out);
}

std::optional<std::vector<PartialAggregationResult>> decodePartials(const std::string& bytes) {
    if (bytes.size() < 8)
        return std::nullopt;
    size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;

    Reader r{bytes.data(), bytes.data() + bodyLen};
    uint32_t n = r.u32();
    // Minimum bytes per partial: 3 string len prefixes + groupKeyHash(8) +
    // cachedTags count(4) + totalPoints(8) + bucketStates count(4) +
    // sortedTimestamps count(4) + sortedStates count(4) + sortedValues count(4) +
    // collapsed flag(1) = 4*3 + 8 + 4 + 8 + 4 + 4 + 4 + 4 + 1 = 49.
    if (!r.boundCount(n, 49))
        return std::nullopt;
    std::vector<PartialAggregationResult> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        PartialAggregationResult p;
        if (!decodePartial(r, p) || !r.ok)
            return std::nullopt;
        out.push_back(std::move(p));
    }
    if (!r.ok || r.p != r.end)
        return std::nullopt;
    return out;
}

}  // namespace timestar::data
