#include "key_encoding.hpp"

#include <charconv>
#include <cstring>
#include <stdexcept>

namespace timestar::index::keys {

std::string escapeKeyComponent(const std::string& s) {
    bool needsEscape = false;
    for (char c : s) {
        if (c == '\\' || c == ',' || c == '=' || c == ' ') {
            needsEscape = true;
            break;
        }
    }
    if (!needsEscape) return s;

    std::string out;
    out.reserve(s.size() + s.size() / 4);
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case ',': out += "\\,"; break;
            case '=': out += "\\="; break;
            case ' ': out += "\\ "; break;
            default: out += c; break;
        }
    }
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
    key += escapeKeyComponent(measurement);

    for (const auto& tag : tags) {
        key += ',';
        key += escapeKeyComponent(tag.first);
        key += '=';
        key += escapeKeyComponent(tag.second);
    }

    key += ' ';
    key += escapeKeyComponent(field);
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
    std::string key;
    key.reserve(1 + measurement.size() + 1 + field.size());
    key.push_back(static_cast<char>(FIELD_TYPE));
    key += measurement;
    key.push_back('\0');
    key += field;
    return key;
}

std::string encodeMeasurementSeriesKey(const std::string& measurement, const SeriesId128& seriesId) {
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

    std::string tagCountStr = std::to_string(metadata.tags.size());
    size_t totalSize = metadata.measurement.size() + 1 + metadata.field.size() + 1 + tagCountStr.size() + 1;
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
    result.append(tagCountStr);
    result.push_back('\0');

    for (const auto& [k, v] : metadata.tags) {
        result.append(k);
        result.push_back('\0');
        result.append(v);
        result.push_back('\0');
    }

    return result;
}

SeriesMetadata decodeSeriesMetadata(const std::string& encoded) {
    return decodeSeriesMetadata(encoded.data(), encoded.size());
}

SeriesMetadata decodeSeriesMetadata(const char* rawData, size_t rawLen) {
    SeriesMetadata metadata;
    std::string_view data(rawData, rawLen);
    size_t pos = 0;

    auto nextField = [&]() -> std::string_view {
        if (pos >= data.size()) return {};
        size_t end = data.find('\0', pos);
        if (end == std::string_view::npos) {
            std::string_view field = data.substr(pos);
            pos = data.size();
            return field;
        }
        std::string_view field = data.substr(pos, end - pos);
        pos = end + 1;
        return field;
    };

    metadata.measurement = std::string(nextField());
    metadata.field = std::string(nextField());

    std::string_view sizeStr = nextField();
    if (sizeStr.empty()) return metadata;

    size_t tagCount = 0;
    auto [ptr, ec] = std::from_chars(sizeStr.data(), sizeStr.data() + sizeStr.size(), tagCount);
    if (ec != std::errc()) {
        throw std::runtime_error("Failed to parse tag count in series metadata");
    }
    if (tagCount > 1000) {
        throw std::runtime_error("tagCount exceeds maximum of 1000");
    }

    for (size_t i = 0; i < tagCount; ++i) {
        std::string_view key = nextField();
        std::string_view value = nextField();
        metadata.tags[std::string(key)] = std::string(value);
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

std::set<std::string> decodeStringSet(const std::string& encoded) {
    std::set<std::string> result;
    const char* data = encoded.data();
    size_t size = encoded.size();
    size_t offset = 0;

    while (offset + sizeof(uint32_t) <= size) {
        uint32_t len;
        std::memcpy(&len, data + offset, sizeof(len));
        offset += sizeof(uint32_t);

        if (offset + len > size) break;

        result.emplace(data + offset, len);
        offset += len;
    }

    return result;
}

}  // namespace timestar::index::keys
