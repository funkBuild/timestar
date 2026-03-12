#pragma once

#include "line_parser.hpp"
#include "series_id.hpp"

#include <map>
#include <memory>
#include <optional>
#include <vector>

/*
  weather,location=us-midwest temperature=82 1465839830100400200
    |    -------------------- --------------  |
    |             |             |             |
    |             |             |             |
  +-----------+--------+-+---------+-+---------+
  |measurement|,tag_set| |field_set| |timestamp|
  +-----------+--------+-+---------+-+---------+
*/

template <class T>
class TimeStarInsert {
private:
    mutable std::string _seriesKey;
    mutable bool _seriesKeyCache = false;
    mutable SeriesId128 _seriesId128;
    mutable bool _seriesId128Cache = false;

    // Shared (refcounted) tags and timestamps for multi-field points.
    // When set, these take precedence over the owned `tags` and `timestamps`
    // members for read operations (seriesKey, seriesId128, getTags,
    // getTimestamps).  This allows N inserts from the same multi-field point
    // to share a single allocation instead of copying tags/timestamps N times.
    std::shared_ptr<const std::map<std::string, std::string>> _sharedTags;
    std::shared_ptr<const std::vector<uint64_t>> _sharedTimestamps;

public:
    // Cached estimated WAL size. Set by WAL::estimateInsertSize() on first call
    // and reused on subsequent calls. Valid as long as seriesKey, timestamps, and
    // values don't change (true throughout the insert pipeline after construction).
    // Mutable to allow const callers to populate it; reset by setSharedTags()
    // and setSharedTimestamps() when the underlying data changes.
    mutable std::optional<size_t> _cachedEstimatedSize;

    std::string measurement;
    std::string field;
    std::map<std::string, std::string> tags;

    std::vector<uint64_t> timestamps;
    std::vector<T> values;

    TimeStarInsert(std::string _measurement, std::string _field) : measurement(_measurement), field(_field) {}

    static TimeStarInsert<T> fromSeriesKey(std::string seriesId) {
        TimeStarInsert<T> insert("", "");
        SeriesKeyParser parser(seriesId);

        insert.measurement = parser.measurement;
        insert.field = parser.field;

        for (auto const& [key, val] : parser.tags) {
            insert.addTag(std::string(key), std::string(val));
        }

        return insert;
    }

    void addTag(std::string key, std::string value) { tags.insert({key, value}); }

    void addValue(uint64_t timestamp, T value) {
        timestamps.push_back(timestamp);
        values.push_back(value);
    }

    // Set shared tags (refcounted, zero-copy across multi-field inserts).
    // Invalidates the seriesKey cache since tags have changed.
    void setSharedTags(std::shared_ptr<const std::map<std::string, std::string>> sharedTags) {
        _sharedTags = std::move(sharedTags);
        _seriesKeyCache = false;
        _seriesId128Cache = false;
        _cachedEstimatedSize.reset();  // series key length changes
    }

    // Set shared timestamps (refcounted, zero-copy across multi-field inserts).
    void setSharedTimestamps(std::shared_ptr<const std::vector<uint64_t>> sharedTimestamps) {
        _sharedTimestamps = std::move(sharedTimestamps);
        _cachedEstimatedSize.reset();  // timestamp count changes
    }

    // Pre-populate the series key cache to avoid recomputation in WAL encoding.
    // Call after setSharedTags() when you already have the series key string.
    void setCachedSeriesKey(std::string key) {
        _seriesKey = std::move(key);
        _seriesKeyCache = true;
        _seriesId128Cache = false;
    }

    // Pre-populate the series ID cache to avoid recomputation.
    void setCachedSeriesId128(SeriesId128 id) {
        _seriesId128 = id;
        _seriesId128Cache = true;
    }

    // Read-only access to tags. Returns shared tags if set, otherwise owned tags.
    const std::map<std::string, std::string>& getTags() const {
        if (_sharedTags)
            return *_sharedTags;
        return tags;
    }

    // Read-only access to timestamps. Returns shared timestamps if set, otherwise owned.
    const std::vector<uint64_t>& getTimestamps() const {
        if (_sharedTimestamps)
            return *_sharedTimestamps;
        return timestamps;
    }

    // Move timestamps out for consumption (e.g. MemoryStore insert).
    // If shared, copies into owned member first so the shared_ptr is not modified.
    std::vector<uint64_t> takeTimestamps() {
        if (_sharedTimestamps) {
            // Materialize shared data into owned member, then release the shared ref
            timestamps = *_sharedTimestamps;
            _sharedTimestamps.reset();
        }
        return std::move(timestamps);
    }

    /*
      # measurement, tag set, field key
      h2o_level, location=santa_monica, h2o_feet
    */

    const std::string& seriesKey() const {
        // measurement, tag set, field key
        // h2o_level, location=santa_monica, host=my-host-1 h2o_feet
        if (!_seriesKeyCache) {
            const auto& effectiveTags = getTags();
            // Pre-compute total size and reserve to avoid reallocations
            size_t totalSize = measurement.size() + 1 + field.size();  // +1 for space
            for (const auto& [key, value] : effectiveTags) {
                totalSize += 1 + key.size() + 1 + value.size();  // ,key=value
            }
            _seriesKey.clear();
            _seriesKey.reserve(totalSize);
            _seriesKey = measurement;
            for (const auto& [key, value] : effectiveTags) {
                _seriesKey += ',';
                _seriesKey += key;
                _seriesKey += '=';
                _seriesKey += value;
            }
            _seriesKey += ' ';
            _seriesKey += field;
            _seriesKeyCache = true;
            _seriesId128Cache = false;
        }

        return _seriesKey;
    }

    SeriesId128 seriesId128() const {
        if (!_seriesId128Cache) {
            _seriesId128 = SeriesId128::fromSeriesKey(seriesKey());
            _seriesId128Cache = true;
        }
        return _seriesId128;
    }
};
