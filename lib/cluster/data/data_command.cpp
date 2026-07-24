#include "data_command.hpp"

#include <cstring>

namespace timestar::data {

namespace {

// FNV-1a 64-bit over a byte span -- the record checksum. Not cryptographic; it
// only needs to catch a corrupt/truncated frame that still parses structurally.
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

enum Kind : uint8_t { kWritePoints = 1, kDeleteRange = 2, kRetentionCutoff = 3 };

// A DataPoint is 16 + 8 + 8 = 32 bytes on the wire; used for non-wrapping count
// bounds so a hostile count field can never drive an over-allocation.
constexpr uint64_t kPointBytes = 32;

void finish(Writer& w) {
    w.u64(fnv1a(w.out.data(), w.out.size()));
}

}  // namespace

std::string encodeDataCommand(const DataCommand& cmd) {
    Writer w;
    if (const auto* wp = std::get_if<WritePoints>(&cmd)) {
        w.u8(kWritePoints);
        w.u64(wp->schemaVersion);
        w.u32(static_cast<uint32_t>(wp->points.size()));
        for (const auto& pt : wp->points) {
            w.series(pt.series);
            w.u64(pt.timestamp);
            w.dbl(pt.value);
        }
    } else if (const auto* dr = std::get_if<DeleteRange>(&cmd)) {
        w.u8(kDeleteRange);
        w.series(dr->series);
        w.u64(dr->startTime);
        w.u64(dr->endTime);
    } else if (const auto* rc = std::get_if<RetentionCutoff>(&cmd)) {
        w.u8(kRetentionCutoff);
        w.u64(rc->cutoffTime);
    }
    finish(w);
    return std::move(w.out);
}

std::optional<DataCommand> decodeDataCommand(const std::string& bytes) {
    // A valid frame is at least a kind byte plus the 8-byte trailing checksum.
    if (bytes.size() < 9)
        return std::nullopt;
    const size_t bodyLen = bytes.size() - 8;
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodyLen + i])) << (8 * i);
    if (fnv1a(bytes.data(), bodyLen) != stored)
        return std::nullopt;  // corrupt/truncated frame that still parsed structurally

    Reader r{bytes.data(), bytes.data() + bodyLen};
    const uint8_t kind = r.u8();
    if (kind == kWritePoints) {
        WritePoints wp;
        wp.schemaVersion = r.u64();
        uint32_t n = r.u32();
        if (!r.ok || n > static_cast<uint64_t>(r.end - r.p) / kPointBytes)
            return std::nullopt;
        wp.points.reserve(n);
        for (uint32_t i = 0; i < n; ++i) {
            DataPoint pt;
            pt.series = r.series();
            pt.timestamp = r.u64();
            pt.value = r.dbl();
            if (!r.ok)
                return std::nullopt;
            wp.points.push_back(pt);
        }
        if (r.p != r.end)
            return std::nullopt;  // trailing garbage
        return DataCommand{std::move(wp)};
    }
    if (kind == kDeleteRange) {
        DeleteRange dr;
        dr.series = r.series();
        dr.startTime = r.u64();
        dr.endTime = r.u64();
        if (!r.ok || r.p != r.end)
            return std::nullopt;
        return DataCommand{dr};
    }
    if (kind == kRetentionCutoff) {
        RetentionCutoff rc;
        rc.cutoffTime = r.u64();
        if (!r.ok || r.p != r.end)
            return std::nullopt;
        return DataCommand{rc};
    }
    return std::nullopt;  // unknown kind
}

}  // namespace timestar::data
