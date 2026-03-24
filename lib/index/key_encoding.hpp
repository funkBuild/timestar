#pragma once

#include "index_backend.hpp"
#include "series_id.hpp"

#include <map>
#include <set>
#include <string>
#include <string_view>

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__,
              "Key encoding assumes little-endian byte order for binary fields");

namespace timestar::index::keys {

// --- Key encoding functions ---
// These produce the binary keys used by NativeIndex.

std::string encodeSeriesKey(const std::string& measurement, const std::map<std::string, std::string>& tags,
                            const std::string& field);

std::string encodeMeasurementFieldsKey(const std::string& measurement);
std::string encodeMeasurementTagsKey(const std::string& measurement);
std::string encodeTagValuesKey(const std::string& measurement, const std::string& tagKey);
std::string encodeSeriesMetadataKey(const SeriesId128& seriesId);
std::string encodeFieldTypeKey(const std::string& measurement, const std::string& field);
std::string encodeMeasurementSeriesKey(const std::string& measurement, const SeriesId128& seriesId);
std::string encodeMeasurementSeriesPrefix(const std::string& measurement);
std::string encodeMeasurementFieldSeriesKey(const std::string& measurement, const std::string& field,
                                            const SeriesId128& seriesId);
std::string encodeRetentionPolicyKey(const std::string& measurement);

// --- Value encoding/decoding ---

std::string encodeSeriesId(const SeriesId128& seriesId);
SeriesId128 decodeSeriesId(const std::string& encoded);
SeriesId128 decodeSeriesId(const char* data, size_t len);

std::string encodeSeriesMetadata(const SeriesMetadata& metadata);
SeriesMetadata decodeSeriesMetadata(const char* data, size_t len);
SeriesMetadata decodeSeriesMetadata(std::string_view encoded);

std::string encodeStringSet(const std::set<std::string>& strings);
std::set<std::string> decodeStringSet(std::string_view encoded);

std::string escapeKeyComponent(const std::string& input);

// --- Phase 2: Local ID / Postings bitmap key encoding ---

std::string encodeLocalIdForwardKey(uint32_t localId);
std::string encodeLocalIdCounterKey();
std::string encodePostingsBitmapKey(const std::string& measurement, const std::string& tagKey,
                                    const std::string& tagValue);
std::string encodePostingsBitmapPrefix(const std::string& measurement, const std::string& tagKey);
std::string encodeLocalId(uint32_t localId);
uint32_t decodeLocalId(std::string_view encoded);

// --- Phase 3: Time-scoped day bitmap key encoding ---

constexpr uint64_t NS_PER_DAY = 86400ULL * 1'000'000'000ULL;

inline uint32_t dayBucketFromNs(uint64_t timestampNs) {
    return static_cast<uint32_t>(timestampNs / NS_PER_DAY);
}

std::string encodeDayBitmapPrefix(const std::string& measurement);
uint32_t decodeDayFromDayBitmapKey(std::string_view key);

// --- Phase 4: Cardinality HLL + measurement bloom key encoding ---

std::string encodeCardinalityHLLKey(const std::string& measurement);
std::string encodeCardinalityHLLKey(const std::string& measurement, const std::string& tagKey,
                                    const std::string& tagValue);
std::string encodeCardinalityHLLPrefix(const std::string& measurement);
std::string encodeMeasurementBloomKey(const std::string& measurement);

}  // namespace timestar::index::keys
