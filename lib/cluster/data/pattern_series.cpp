#include "pattern_series.hpp"

#include "../../core/placement_table.hpp"

#include <algorithm>
#include <limits>

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
    void u8(uint8_t value) { out.push_back(static_cast<char>(value)); }
    void u16(uint16_t value) {
        u8(value & 0xff);
        u8((value >> 8) & 0xff);
    }
    void u32(uint32_t value) {
        for (int i = 0; i < 4; ++i)
            u8((value >> (8 * i)) & 0xff);
    }
    void u64(uint64_t value) {
        for (int i = 0; i < 8; ++i)
            u8((value >> (8 * i)) & 0xff);
    }
    void str(const std::string& value) {
        if (value.size() > std::numeric_limits<uint32_t>::max())
            throw std::length_error("pattern-series string exceeds wire limit");
        u32(static_cast<uint32_t>(value.size()));
        out.append(value);
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
    uint16_t u16() {
        uint16_t value = u8();
        value |= static_cast<uint16_t>(u8()) << 8;
        return value;
    }
    uint32_t u32() {
        uint32_t value = 0;
        for (int i = 0; i < 4; ++i)
            value |= static_cast<uint32_t>(u8()) << (8 * i);
        return value;
    }
    uint64_t u64() {
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i)
            value |= static_cast<uint64_t>(u8()) << (8 * i);
        return value;
    }
    std::string str() {
        const uint32_t size = u32();
        if (!ok || !avail(size)) {
            ok = false;
            return {};
        }
        std::string value(p, size);
        p += size;
        return value;
    }
};

size_t verifiedBodySize(const std::string& bytes) {
    if (bytes.size() < sizeof(uint64_t) || bytes.size() > kMaxOutboundFrameBytes)
        return std::string::npos;
    const size_t bodySize = bytes.size() - sizeof(uint64_t);
    uint64_t stored = 0;
    for (int i = 0; i < 8; ++i)
        stored |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[bodySize + i])) << (8 * i);
    return stored == fnv1a(bytes.data(), bodySize) ? bodySize : std::string::npos;
}

bool canonicalVShards(const std::vector<uint16_t>& vshards) {
    return !vshards.empty() && vshards.size() <= timestar::VIRTUAL_SHARD_COUNT &&
           std::is_sorted(vshards.begin(), vshards.end()) &&
           std::adjacent_find(vshards.begin(), vshards.end()) == vshards.end() &&
           vshards.back() < timestar::VIRTUAL_SHARD_COUNT;
}

bool canonicalResolveSet(const std::vector<uint16_t>& resolve, const std::vector<uint16_t>& vshards) {
    return std::is_sorted(resolve.begin(), resolve.end()) &&
           std::adjacent_find(resolve.begin(), resolve.end()) == resolve.end() &&
           std::includes(vshards.begin(), vshards.end(), resolve.begin(), resolve.end());
}

void writeStrings(Writer& writer, const std::vector<std::string>& values) {
    if (values.size() > std::numeric_limits<uint32_t>::max())
        throw std::length_error("pattern-series vector exceeds wire limit");
    writer.u32(static_cast<uint32_t>(values.size()));
    for (const auto& value : values)
        writer.str(value);
}

bool readStrings(Reader& reader, std::vector<std::string>& values,
                 uint32_t maxCount = std::numeric_limits<uint32_t>::max()) {
    const uint32_t count = reader.u32();
    if (!reader.ok || count > maxCount || count > static_cast<uint64_t>(reader.end - reader.p) / 4)
        return false;
    values.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        values.push_back(reader.str());
        if (!reader.ok)
            return false;
    }
    return true;
}

void writeVShards(Writer& writer, const std::vector<uint16_t>& values) {
    writer.u32(static_cast<uint32_t>(values.size()));
    for (uint16_t value : values)
        writer.u16(value);
}

bool readVShards(Reader& reader, std::vector<uint16_t>& values) {
    const uint32_t count = reader.u32();
    if (!reader.ok || count > timestar::VIRTUAL_SHARD_COUNT || count > static_cast<uint64_t>(reader.end - reader.p) / 2)
        return false;
    values.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        values.push_back(reader.u16());
    return reader.ok;
}

}  // namespace

std::string encodePatternSeriesRequest(const PatternSeriesRequest& request) {
    if (!canonicalVShards(request.vshards) || !canonicalResolveSet(request.resolveVShards, request.vshards) ||
        request.selector.measurement.empty() || request.selector.tags.size() > kPatternSeriesMaxResults ||
        request.selector.fields.size() > kPatternSeriesMaxResults || request.maxSeries == 0 ||
        request.maxSeries > kPatternSeriesMaxResults)
        throw std::invalid_argument("non-canonical pattern-series request");

    if (request.selector.tags.size() > std::numeric_limits<uint32_t>::max())
        throw std::length_error("pattern-series tag set exceeds wire limit");

    Writer writer;
    writer.str(request.selector.measurement);
    writer.u32(static_cast<uint32_t>(request.selector.tags.size()));
    for (const auto& [key, value] : request.selector.tags) {
        writer.str(key);
        writer.str(value);
    }
    writeStrings(writer, request.selector.fields);
    writeVShards(writer, request.vshards);
    writeVShards(writer, request.resolveVShards);
    writer.u64(request.mapEpoch);
    writer.u32(request.maxSeries);
    auto encoded = writer.finish();
    if (encoded.size() > kMaxOutboundFrameBytes)
        throw std::length_error("pattern-series request exceeds the inter-node frame limit");
    return encoded;
}

std::optional<PatternSeriesRequest> decodePatternSeriesRequest(const std::string& bytes) {
    const size_t bodySize = verifiedBodySize(bytes);
    if (bodySize == std::string::npos)
        return std::nullopt;
    Reader reader{bytes.data(), bytes.data() + bodySize};
    PatternSeriesRequest request;
    request.selector.measurement = reader.str();
    const uint32_t tagCount = reader.u32();
    if (!reader.ok || tagCount > kPatternSeriesMaxResults ||
        tagCount > static_cast<uint64_t>(reader.end - reader.p) / 8)
        return std::nullopt;
    for (uint32_t i = 0; i < tagCount; ++i) {
        auto key = reader.str();
        auto value = reader.str();
        auto [_, inserted] = request.selector.tags.emplace(std::move(key), std::move(value));
        if (!reader.ok || !inserted)
            return std::nullopt;
    }
    if (!readStrings(reader, request.selector.fields, kPatternSeriesMaxResults) ||
        !readVShards(reader, request.vshards) || !readVShards(reader, request.resolveVShards))
        return std::nullopt;
    request.mapEpoch = reader.u64();
    request.maxSeries = reader.u32();
    if (!reader.ok || reader.p != reader.end || request.selector.measurement.empty() || request.maxSeries == 0 ||
        request.maxSeries > kPatternSeriesMaxResults || !canonicalVShards(request.vshards) ||
        !canonicalResolveSet(request.resolveVShards, request.vshards))
        return std::nullopt;
    return request;
}

std::string encodePatternSeriesResult(const PatternSeriesResult& result) {
    if (result.redirects.size() > std::numeric_limits<uint32_t>::max())
        throw std::length_error("pattern-series redirect set exceeds wire limit");
    if (result.seriesKeys.size() > kPatternSeriesMaxResults || result.redirects.size() > timestar::VIRTUAL_SHARD_COUNT)
        throw std::length_error("pattern-series result exceeds structural safety limits");
    if (result.limitExceeded && !result.seriesKeys.empty())
        throw std::invalid_argument("limit-exceeded pattern-series result must not retain keys");
    size_t encodedKeyBytes = 0;
    for (const auto& key : result.seriesKeys) {
        const size_t encodedBytes = sizeof(uint32_t) + key.size();
        if (encodedBytes > kPatternSeriesMaxKeyBytes || encodedKeyBytes > kPatternSeriesMaxKeyBytes - encodedBytes)
            throw std::length_error("pattern-series key result exceeds encoded-byte safety limit");
        encodedKeyBytes += encodedBytes;
    }

    Writer writer;
    writer.u8(result.limitExceeded ? 1 : 0);
    writeStrings(writer, result.seriesKeys);
    writer.u32(static_cast<uint32_t>(result.redirects.size()));
    for (const auto& redirect : result.redirects) {
        writer.u16(redirect.vshard);
        writer.u64(redirect.leader);
        writer.u8(redirect.hosted ? 1 : 0);
    }
    auto encoded = writer.finish();
    if (encoded.size() > kMaxOutboundFrameBytes)
        throw std::length_error("pattern-series result exceeds the inter-node frame limit");
    return encoded;
}

std::optional<PatternSeriesResult> decodePatternSeriesResult(const std::string& bytes) {
    const size_t bodySize = verifiedBodySize(bytes);
    if (bodySize == std::string::npos)
        return std::nullopt;
    Reader reader{bytes.data(), bytes.data() + bodySize};
    PatternSeriesResult result;
    const uint8_t exceeded = reader.u8();
    if (exceeded > 1 || !readStrings(reader, result.seriesKeys, kPatternSeriesMaxResults))
        return std::nullopt;
    result.limitExceeded = exceeded != 0;
    const uint32_t redirectCount = reader.u32();
    if (!reader.ok || redirectCount > timestar::VIRTUAL_SHARD_COUNT ||
        redirectCount > static_cast<uint64_t>(reader.end - reader.p) / 11)
        return std::nullopt;
    result.redirects.reserve(redirectCount);
    for (uint32_t i = 0; i < redirectCount; ++i) {
        VShardRedirect redirect;
        redirect.vshard = reader.u16();
        redirect.leader = reader.u64();
        const uint8_t hosted = reader.u8();
        if (!reader.ok || hosted > 1 || redirect.vshard >= timestar::VIRTUAL_SHARD_COUNT)
            return std::nullopt;
        redirect.hosted = hosted != 0;
        result.redirects.push_back(redirect);
    }
    size_t encodedKeyBytes = 0;
    for (const auto& key : result.seriesKeys) {
        const size_t encodedBytes = sizeof(uint32_t) + key.size();
        if (encodedBytes > kPatternSeriesMaxKeyBytes || encodedKeyBytes > kPatternSeriesMaxKeyBytes - encodedBytes)
            return std::nullopt;
        encodedKeyBytes += encodedBytes;
    }
    if (!reader.ok || reader.p != reader.end || (result.limitExceeded && !result.seriesKeys.empty()))
        return std::nullopt;
    return result;
}

}  // namespace timestar::data
