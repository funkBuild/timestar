#include "key_encoding.hpp"

#include "key_encoding_simd.hpp"

#include <charconv>
#include <cstring>
#include <stdexcept>

namespace timestar::index::keys {

// Append escaped component directly to output — no temporary string allocation.
// Uses SIMD to scan for escape characters in 16-32 byte chunks.
static void appendEscaped(std::string& out, const std::string& s) {
    // SIMD scan for first escape character
    size_t firstEscape =
        (s.size() >= simd::kSimdThreshold) ? simd::findFirstEscapeChar(s.data(), s.size()) : [&]() -> size_t {
        for (size_t i = 0; i < s.size(); ++i) {
            char c = s[i];
            if (c == '\\' || c == ',' || c == '=' || c == ' ')
                return i;
        }
        return s.size();
    }();

    if (firstEscape == s.size()) {
        // No escaping needed — fast append
        out.append(s);
        return;
    }

    out.reserve(out.size() + s.size() + s.size() / 4);
    // Append the clean prefix in one shot
    out.append(s.data(), firstEscape);
    // Escape the rest
    for (size_t i = firstEscape; i < s.size(); ++i) {
        switch (s[i]) {
            case '\\':
                out.append("\\\\", 2);
                break;
            case ',':
                out.append("\\,", 2);
                break;
            case '=':
                out.append("\\=", 2);
                break;
            case ' ':
                out.append("\\ ", 2);
                break;
            default:
                out.push_back(s[i]);
                break;
        }
    }
}

std::string escapeKeyComponent(const std::string& s) {
    std::string out;
    appendEscaped(out, s);
    return out;
}

std::string encodeSeriesKey(const std::string& measurement, const std::map<std::string, std::string>& tags,
                            const std::string& field) {
    size_t estimatedSize = 1 + measurement.size() + field.size() + 1;
    for (const auto& [k, v] : tags) {
        estimatedSize += 1 + k.size() + 1 + v.size();
    }

    std::string key;
    key.reserve(estimatedSize + estimatedSize / 4);
    key.push_back(static_cast<char>(SERIES_INDEX));
    appendEscaped(key, measurement);

    for (const auto& tag : tags) {
        key.push_back(',');
        appendEscaped(key, tag.first);
        key.push_back('=');
        appendEscaped(key, tag.second);
    }

    key.push_back(' ');
    appendEscaped(key, field);
    return key;
}

std::string encodeMeasurementFieldsKey(const std::string& measurement) {
    std::string key;
    key.reserve(1 + measurement.size());
    key.push_back(static_cast<char>(MEASUREMENT_FIELDS));
    key += measurement;
    return key;
}

std::string encodeMeasurementTagsKey(const std::string& measurement) {
    std::string key;
    key.reserve(1 + measurement.size());
    key.push_back(static_cast<char>(MEASUREMENT_TAGS));
    key += measurement;
    return key;
}

std::string encodeTagValuesKey(const std::string& measurement, const std::string& tagKey) {
    if (measurement.find('\0') != std::string::npos || tagKey.find('\0') != std::string::npos) {
        throw std::invalid_argument("Measurement or tag key contains null byte");
    }
    std::string key;
    key.reserve(1 + measurement.size() + 1 + tagKey.size());
    key.push_back(static_cast<char>(TAG_VALUES));
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    return key;
}

std::string encodeSeriesMetadataKey(const SeriesId128& seriesId) {
    std::string key;
    key.reserve(1 + 16);
    key.push_back(static_cast<char>(SERIES_METADATA));
    seriesId.appendTo(key);
    return key;
}

std::string encodeFieldTypeKey(const std::string& measurement, const std::string& field) {
    if (measurement.find('\0') != std::string::npos || field.find('\0') != std::string::npos) {
        throw std::invalid_argument("Key component must not contain null bytes");
    }
    std::string key;
    key.reserve(1 + measurement.size() + 1 + field.size());
    key.push_back(static_cast<char>(FIELD_TYPE));
    key += measurement;
    key.push_back('\0');
    key += field;
    return key;
}

std::string encodeMeasurementSeriesKey(const std::string& measurement, const SeriesId128& seriesId) {
    if (measurement.find('\0') != std::string::npos) {
        throw std::invalid_argument("Key component must not contain null bytes");
    }
    std::string key;
    key.reserve(1 + measurement.size() + 1 + 16);
    key.push_back(static_cast<char>(MEASUREMENT_SERIES));
    key += measurement;
    key.push_back('\0');
    seriesId.appendTo(key);
    return key;
}

std::string encodeMeasurementSeriesPrefix(const std::string& measurement) {
    std::string prefix;
    prefix.reserve(1 + measurement.size() + 1);
    prefix.push_back(static_cast<char>(MEASUREMENT_SERIES));
    prefix += measurement;
    prefix.push_back('\0');
    return prefix;
}

std::string encodeMeasurementFieldSeriesKey(const std::string& measurement, const std::string& field,
                                            const SeriesId128& seriesId) {
    if (measurement.find('\0') != std::string::npos || field.find('\0') != std::string::npos) {
        throw std::invalid_argument("Key component must not contain null bytes");
    }
    std::string key;
    key.reserve(1 + measurement.size() + 1 + field.size() + 1 + 16);
    key.push_back(static_cast<char>(MEASUREMENT_FIELD_SERIES));
    key += measurement;
    key.push_back('\0');
    key += field;
    key.push_back('\0');
    seriesId.appendTo(key);
    return key;
}

std::string encodeRetentionPolicyKey(const std::string& measurement) {
    std::string key;
    key.reserve(1 + measurement.size());
    key.push_back(static_cast<char>(RETENTION_POLICY));
    key += measurement;
    return key;
}

std::string encodeSeriesId(const SeriesId128& seriesId) {
    return seriesId.toBytes();
}

SeriesId128 decodeSeriesId(const std::string& encoded) {
    return SeriesId128::fromBytes(encoded);
}

SeriesId128 decodeSeriesId(const char* data, size_t len) {
    return SeriesId128::fromBytes(data, len);
}

std::string encodeSeriesMetadata(const SeriesMetadata& metadata) {
    if (metadata.measurement.length() > 10000 || metadata.field.length() > 10000) {
        throw std::runtime_error("SeriesMetadata has suspiciously long strings");
    }
    if (metadata.tags.size() > 1000) {
        throw std::runtime_error("SeriesMetadata has too many tags");
    }

    // Convert tag count to string without std::to_string allocation
    char tagCountBuf[16];
    auto [tcEnd, tcEc] = std::to_chars(tagCountBuf, tagCountBuf + sizeof(tagCountBuf), metadata.tags.size());
    size_t tagCountLen = static_cast<size_t>(tcEnd - tagCountBuf);

    size_t totalSize = metadata.measurement.size() + 1 + metadata.field.size() + 1 + tagCountLen + 1;
    for (const auto& [k, v] : metadata.tags) {
        if (k.length() > 1000 || v.length() > 1000) {
            throw std::runtime_error("SeriesMetadata tag key/value too long");
        }
        totalSize += k.size() + 1 + v.size() + 1;
    }

    std::string result;
    result.reserve(totalSize);
    result.append(metadata.measurement);
    result.push_back('\0');
    result.append(metadata.field);
    result.push_back('\0');
    result.append(tagCountBuf, tagCountLen);
    result.push_back('\0');

    for (const auto& [k, v] : metadata.tags) {
        result.append(k);
        result.push_back('\0');
        result.append(v);
        result.push_back('\0');
    }

    return result;
}

SeriesMetadata decodeSeriesMetadata(std::string_view encoded) {
    return decodeSeriesMetadata(encoded.data(), encoded.size());
}

SeriesMetadata decodeSeriesMetadata(const char* rawData, size_t rawLen) {
    SeriesMetadata metadata;
    const char* p = rawData;
    const char* end = rawData + rawLen;

    // Fast null-byte scanner using memchr
    auto nextField = [&]() -> std::string_view {
        if (p >= end)
            return {};
        const char* nul = static_cast<const char*>(std::memchr(p, '\0', static_cast<size_t>(end - p)));
        if (!nul) {
            std::string_view field(p, static_cast<size_t>(end - p));
            p = end;
            return field;
        }
        std::string_view field(p, static_cast<size_t>(nul - p));
        p = nul + 1;
        return field;
    };

    metadata.measurement = std::string(nextField());
    metadata.field = std::string(nextField());

    std::string_view sizeStr = nextField();
    if (sizeStr.empty())
        return metadata;

    size_t tagCount = 0;
    auto [ptr, ec] = std::from_chars(sizeStr.data(), sizeStr.data() + sizeStr.size(), tagCount);
    if (ec != std::errc()) {
        throw std::runtime_error("Failed to parse tag count in series metadata");
    }
    if (tagCount > 1000) {
        throw std::runtime_error("tagCount exceeds maximum of 1000");
    }

    // Tags are stored in sorted order (from std::map during encoding).
    // Use emplace_hint at end() for O(1) amortized insertion.
    for (size_t i = 0; i < tagCount; ++i) {
        std::string_view key = nextField();
        std::string_view value = nextField();
        metadata.tags.emplace_hint(metadata.tags.end(), std::string(key), std::string(value));
    }

    return metadata;
}

std::string encodeStringSet(const std::set<std::string>& strings) {
    std::string result;
    result.reserve(strings.size() * (sizeof(uint32_t) + 32));
    for (const auto& str : strings) {
        uint32_t len = static_cast<uint32_t>(str.length());
        result.append(reinterpret_cast<const char*>(&len), sizeof(len));
        result.append(str.data(), len);
    }
    return result;
}

std::set<std::string> decodeStringSet(std::string_view encoded) {
    std::set<std::string> result;
    const char* data = encoded.data();
    size_t size = encoded.size();
    size_t offset = 0;

    while (offset + sizeof(uint32_t) <= size) {
        uint32_t len;
        std::memcpy(&len, data + offset, sizeof(len));
        offset += sizeof(uint32_t);

        if (offset + len > size)
            break;

        result.emplace(data + offset, len);
        offset += len;
    }

    return result;
}

// ============================================================================
// Phase 2: Local ID / Postings bitmap key encoding
// ============================================================================

std::string encodeLocalId(uint32_t localId) {
    std::string result(4, '\0');
    std::memcpy(result.data(), &localId, 4);  // Little-endian on x86
    return result;
}

uint32_t decodeLocalId(std::string_view encoded) {
    if (encoded.size() < 4) {
        throw std::runtime_error("Invalid local ID encoding: too short");
    }
    uint32_t localId;
    std::memcpy(&localId, encoded.data(), 4);
    return localId;
}

std::string encodeLocalIdForwardKey(uint32_t localId) {
    std::string key;
    key.reserve(5);
    key.push_back(static_cast<char>(LOCAL_ID_FORWARD));
    key.append(encodeLocalId(localId));
    return key;
}

std::string encodeLocalIdCounterKey() {
    return std::string(1, static_cast<char>(LOCAL_ID_COUNTER));
}

// Shared validation for functions that use \0 as separator
static void validateNoNullBytes(std::initializer_list<std::string_view> components) {
    for (auto sv : components) {
        if (sv.find('\0') != std::string_view::npos) {
            throw std::invalid_argument("Key component must not contain null bytes");
        }
    }
}

std::string encodePostingsBitmapKey(const std::string& measurement, const std::string& tagKey,
                                    const std::string& tagValue) {
    validateNoNullBytes({measurement, tagKey, tagValue});
    std::string key;
    key.reserve(1 + measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    key.push_back(static_cast<char>(POSTINGS_BITMAP));
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    key += tagValue;
    return key;
}

std::string encodePostingsBitmapPrefix(const std::string& measurement, const std::string& tagKey) {
    validateNoNullBytes({measurement, tagKey});
    std::string key;
    key.reserve(1 + measurement.size() + 1 + tagKey.size() + 1);
    key.push_back(static_cast<char>(POSTINGS_BITMAP));
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    return key;
}

// ============================================================================
// Phase 3: Time-scoped day bitmap key encoding
// ============================================================================

std::string encodeDayBitmapKey(const std::string& measurement, uint32_t day) {
    validateNoNullBytes({measurement});
    std::string key;
    key.reserve(1 + measurement.size() + 1 + 4);
    key.push_back(static_cast<char>(TIME_SERIES_DAY));
    key += measurement;
    key.push_back('\0');
    key.append(reinterpret_cast<const char*>(&day), 4);  // Little-endian on x86
    return key;
}

std::string encodeDayBitmapPrefix(const std::string& measurement) {
    validateNoNullBytes({measurement});
    std::string prefix;
    prefix.reserve(1 + measurement.size() + 1);
    prefix.push_back(static_cast<char>(TIME_SERIES_DAY));
    prefix += measurement;
    prefix.push_back('\0');
    return prefix;
}

uint32_t decodeDayFromDayBitmapKey(std::string_view key) {
    if (key.size() < 4) {
        throw std::runtime_error("Invalid day bitmap key: too short");
    }
    uint32_t day;
    std::memcpy(&day, key.data() + key.size() - 4, 4);
    return day;
}

// ============================================================================
// Phase 4: Cardinality HLL + measurement bloom key encoding
// ============================================================================

std::string encodeCardinalityHLLKey(const std::string& measurement) {
    validateNoNullBytes({measurement});
    std::string key;
    key.reserve(1 + measurement.size() + 1);
    key.push_back(static_cast<char>(CARDINALITY_HLL));
    key += measurement;
    key.push_back('\0');
    return key;
}

std::string encodeCardinalityHLLKey(const std::string& measurement, const std::string& tagKey,
                                    const std::string& tagValue) {
    validateNoNullBytes({measurement, tagKey, tagValue});
    std::string key;
    key.reserve(1 + measurement.size() + 1 + tagKey.size() + 1 + tagValue.size());
    key.push_back(static_cast<char>(CARDINALITY_HLL));
    key += measurement;
    key.push_back('\0');
    key += tagKey;
    key.push_back('\0');
    key += tagValue;
    return key;
}

std::string encodeCardinalityHLLPrefix(const std::string& measurement) {
    validateNoNullBytes({measurement});
    std::string key;
    key.reserve(1 + measurement.size() + 1);
    key.push_back(static_cast<char>(CARDINALITY_HLL));
    key += measurement;
    key.push_back('\0');
    return key;
}

std::string encodeMeasurementBloomKey(const std::string& measurement) {
    validateNoNullBytes({measurement});
    std::string key;
    key.reserve(1 + measurement.size() + 1);
    key.push_back(static_cast<char>(MEASUREMENT_BLOOM));
    key += measurement;
    key.push_back('\0');
    return key;
}

}  // namespace timestar::index::keys
