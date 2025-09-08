#ifndef __LEVELDB_INDEX_H_INCLUDED__
#define __LEVELDB_INDEX_H_INCLUDED__

#include <string>
#include <vector>
#include <set>
#include <memory>
#include <optional>

#include "tsdb_value.hpp"
#include "line_parser.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

#include <leveldb/db.h>
#include <leveldb/options.h>
#include <leveldb/write_batch.h>

enum IndexKeyType : uint8_t {
    SERIES_INDEX = 0x01,        // series_key -> series_id
    MEASUREMENT_FIELDS = 0x02,   // measurement -> fields set
    MEASUREMENT_TAGS = 0x03,     // measurement -> tag keys set  
    TAG_VALUES = 0x04,          // measurement+tag_key -> values set
    SERIES_METADATA = 0x05,     // series_id -> metadata
    TAG_INDEX = 0x06,           // measurement+tag_key+tag_value -> series_ids
    GROUP_BY_INDEX = 0x07,      // measurement+tag_key+tag_value -> series_ids (for group-by)
    FIELD_STATS = 0x08,         // series_id+field -> stats
    FIELD_TYPE = 0x09           // measurement+field -> field type (float, bool, string, integer)
};

// Metadata for a time series
struct SeriesMetadata {
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::string field;
};

class LevelDBIndex {
private:
    std::unique_ptr<leveldb::DB> db;
    int shardId;
    std::string indexPath;
    
    // Helper methods for key encoding
    std::string encodeSeriesKey(const std::string& measurement, 
                               const std::map<std::string, std::string>& tags,
                               const std::string& field);
    std::string encodeMeasurementFieldsKey(const std::string& measurement);
    std::string encodeMeasurementTagsKey(const std::string& measurement);
    std::string encodeTagValuesKey(const std::string& measurement, const std::string& tagKey);
    std::string encodeSeriesMetadataKey(uint64_t seriesId);
    std::string encodeFieldTypeKey(const std::string& measurement, const std::string& field);
    
    // Helper methods for value encoding/decoding
    std::string encodeStringSet(const std::set<std::string>& strings);
    std::set<std::string> decodeStringSet(const std::string& encoded);
    std::string encodeSeriesId(uint64_t seriesId);
    uint64_t decodeSeriesId(const std::string& encoded);
    std::string encodeSeriesMetadata(const SeriesMetadata& metadata);
    SeriesMetadata decodeSeriesMetadata(const std::string& encoded);
    
    // Next series ID counter
    uint64_t nextSeriesId = 1;
    std::string SERIES_COUNTER_KEY = "\x00SERIES_COUNTER";
    
    seastar::future<> loadSeriesCounter();

public:
    LevelDBIndex(int shardId);
    ~LevelDBIndex();
    
    seastar::future<> open();
    seastar::future<> close();
    
    // Series indexing - core functionality
    seastar::future<uint64_t> getOrCreateSeriesId(std::string measurement,
                                                  std::map<std::string, std::string> tags, 
                                                  std::string field);
    
    seastar::future<std::optional<uint64_t>> getSeriesId(const std::string& measurement,
                                                         const std::map<std::string, std::string>& tags,
                                                         const std::string& field);
    
    // Get metadata for a series by ID
    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(uint64_t seriesId);
    
    // Measurement metadata indexing
    seastar::future<> addField(const std::string& measurement, const std::string& field);
    seastar::future<> addTag(const std::string& measurement, const std::string& tagKey, const std::string& tagValue);
    
    // Field type management
    seastar::future<> setFieldType(const std::string& measurement, const std::string& field, const std::string& type);
    seastar::future<std::string> getFieldType(const std::string& measurement, const std::string& field);
    
    // Query support - get metadata for measurements
    seastar::future<std::set<std::string>> getAllMeasurements();
    seastar::future<std::set<std::string>> getFields(const std::string& measurement);
    seastar::future<std::set<std::string>> getTags(const std::string& measurement);
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement, const std::string& tagKey);
    
    // Bulk operations for insert batching
    template<class T>
    seastar::future<uint64_t> indexInsert(const TSDBInsert<T>& insert);
    
    // Series discovery for queries
    seastar::future<std::vector<uint64_t>> findSeries(const std::string& measurement,
                                                      const std::map<std::string, std::string>& tagFilters = {});
    
    // Find series by single tag (optimized)
    seastar::future<std::vector<uint64_t>> findSeriesByTag(const std::string& measurement,
                                                           const std::string& tagKey,
                                                           const std::string& tagValue);
    
    // Group series by tag value for aggregations
    seastar::future<std::map<std::string, std::vector<uint64_t>>> 
        getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey);
    
    // Field statistics for query optimization
    struct FieldStats {
        std::string dataType;
        int64_t minTime;
        int64_t maxTime;
        uint64_t pointCount;
    };
    
    seastar::future<> updateFieldStats(uint64_t seriesId, const std::string& field,
                                       const FieldStats& stats);
    
    seastar::future<std::optional<FieldStats>> getFieldStats(uint64_t seriesId, 
                                                             const std::string& field);
    
    // Compaction support - get all series for a measurement
    seastar::future<std::vector<uint64_t>> getAllSeriesForMeasurement(const std::string& measurement);
    
    // Debug/maintenance
    seastar::future<size_t> getSeriesCount();
    seastar::future<> compact();
};

#endif