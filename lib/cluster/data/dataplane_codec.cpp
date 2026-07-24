#include "dataplane_codec.hpp"

#include <cstring>

namespace timestar::data {

namespace {

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
    void series(const SeriesId128& id) { id.appendTo(out); }  // 16 raw bytes
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
    SeriesId128 series() {
        if (!avail(16)) {
            ok = false;
            return {};
        }
        SeriesId128 id = SeriesId128::fromBytes(std::string_view(p, 16));
        p += 16;
        return id;
    }
};

// A DataPoint is 16 + 8 + 8 = 32 bytes on the wire; an AggState entry is
// 16 + 8 + 8*3 = 48 bytes. Used for non-wrapping count bounds.
constexpr uint64_t kPointBytes = 32;
constexpr uint64_t kAggEntryBytes = 48;

}  // namespace

std::string encodePoints(const std::vector<DataPoint>& points) {
    Writer w;
    w.u32(static_cast<uint32_t>(points.size()));
    for (const auto& p : points) {
        w.series(p.series);
        w.u64(p.timestamp);
        w.dbl(p.value);
    }
    return std::move(w.out);
}

std::optional<std::vector<DataPoint>> decodePoints(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    uint32_t n = r.u32();
    if (!r.ok || n > static_cast<uint64_t>(r.end - r.p) / kPointBytes)
        return std::nullopt;
    std::vector<DataPoint> out;
    out.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        DataPoint p;
        p.series = r.series();
        p.timestamp = r.u64();
        p.value = r.dbl();
        if (!r.ok)
            return std::nullopt;
        out.push_back(p);
    }
    return out;
}

std::string encodeQuerySpec(const QuerySpec& spec) {
    Writer w;
    w.u64(spec.startTime);
    w.u64(spec.endTime);
    w.u8(static_cast<uint8_t>(spec.method));
    w.u32(static_cast<uint32_t>(spec.series.size()));
    for (const auto& s : spec.series)
        w.series(s);
    return std::move(w.out);
}

std::optional<QuerySpec> decodeQuerySpec(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    QuerySpec spec;
    spec.startTime = r.u64();
    spec.endTime = r.u64();
    // Validate the method byte before casting: the codec's contract is that a
    // corrupt/hostile frame can never fabricate an out-of-range enum. AggMethod
    // runs Raw(0)..Max(5); reject anything past it rather than let it reach
    // queryLocal as an undefined enumerator.
    uint8_t methodByte = r.u8();
    if (!r.ok || methodByte > static_cast<uint8_t>(AggMethod::Max))
        return std::nullopt;
    spec.method = static_cast<AggMethod>(methodByte);
    uint32_t n = r.u32();
    if (!r.ok || n > static_cast<uint64_t>(r.end - r.p) / 16)
        return std::nullopt;
    spec.series.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        spec.series.push_back(r.series());
        if (!r.ok)
            return std::nullopt;
    }
    return spec;
}

std::string encodeQueryPartial(const QueryPartial& part) {
    Writer w;
    w.u32(static_cast<uint32_t>(part.raw.size()));
    for (const auto& p : part.raw) {
        w.series(p.series);
        w.u64(p.timestamp);
        w.dbl(p.value);
    }
    w.u32(static_cast<uint32_t>(part.perSeries.size()));
    for (const auto& [s, st] : part.perSeries) {
        w.series(s);
        w.u64(st.count);
        w.dbl(st.sum);
        w.dbl(st.min);
        w.dbl(st.max);
    }
    return std::move(w.out);
}

std::optional<QueryPartial> decodeQueryPartial(const std::string& bytes) {
    Reader r{bytes.data(), bytes.data() + bytes.size()};
    QueryPartial part;
    uint32_t nraw = r.u32();
    if (!r.ok || nraw > static_cast<uint64_t>(r.end - r.p) / kPointBytes)
        return std::nullopt;
    part.raw.reserve(nraw);
    for (uint32_t i = 0; i < nraw; ++i) {
        DataPoint p;
        p.series = r.series();
        p.timestamp = r.u64();
        p.value = r.dbl();
        if (!r.ok)
            return std::nullopt;
        part.raw.push_back(p);
    }
    uint32_t nagg = r.u32();
    if (!r.ok || nagg > static_cast<uint64_t>(r.end - r.p) / kAggEntryBytes)
        return std::nullopt;
    for (uint32_t i = 0; i < nagg; ++i) {
        SeriesId128 s = r.series();
        AggState st;
        st.count = r.u64();
        st.sum = r.dbl();
        st.min = r.dbl();
        st.max = r.dbl();
        if (!r.ok)
            return std::nullopt;
        part.perSeries[s] = st;
    }
    return part;
}

}  // namespace timestar::data
