#ifndef SERIES_KEY_HPP_INCLUDED
#define SERIES_KEY_HPP_INCLUDED

#include <map>
#include <string>

namespace timestar {

// Build a canonical series key from measurement + sorted tags + field.
// Format: "measurement,tag1=val1,tag2=val2 field"
// Used for deduplication, sharding, and series identification.
inline std::string buildSeriesKey(const std::string& measurement,
                                   const std::map<std::string, std::string>& tags,
                                   const std::string& field) {
    size_t totalSize = measurement.size() + 1 + field.size();
    for (const auto& [k, v] : tags) {
        totalSize += 1 + k.size() + 1 + v.size();
    }

    std::string key;
    key.reserve(totalSize);
    key.append(measurement);
    for (const auto& [k, v] : tags) {
        key += ',';
        key.append(k);
        key += '=';
        key.append(v);
    }
    key += ' ';
    key.append(field);
    return key;
}

} // namespace timestar

#endif // SERIES_KEY_HPP_INCLUDED
