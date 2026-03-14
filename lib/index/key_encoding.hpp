#ifndef KEY_ENCODING_H_INCLUDED
#define KEY_ENCODING_H_INCLUDED

#include "index_backend.hpp"
#include "series_id.hpp"

#include <map>
#include <set>
#include <string>
#include <string_view>

namespace timestar::index::keys {

// --- Key encoding functions ---
// These produce the same binary keys as LevelDBIndex's private methods.
// Used by both LevelDBIndex and NativeIndex.

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

}  // namespace timestar::index::keys

#endif  // KEY_ENCODING_H_INCLUDED
