#ifndef __METADATA_INDEX_H_INCLUDED__
#define __METADATA_INDEX_H_INCLUDED__

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <optional>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/sstring.hh>

// Series metadata structure
struct SeriesMetadata {
    uint64_t seriesId;
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::vector<std::string> fields;
    int64_t minTime;
    int64_t maxTime;
    uint32_t shardId;
    
    // Serialization
    std::string serialize() const;
    static SeriesMetadata deserialize(const std::string& data);
    
    // Generate series key from measurement + tags + field
    static std::string generateSeriesKey(const std::string& measurement, 
                                        const std::map<std::string, std::string>& tags,
                                        const std::string& field);
};

// Field statistics
struct FieldStats {
    std::string dataType;  // "float", "bool", "string"
    double minValue;
    double maxValue;
    uint64_t pointCount;
    
    std::string serialize() const;
    static FieldStats deserialize(const std::string& data);
};

class MetadataIndex {
private:
    std::unique_ptr<leveldb::DB> db;
    std::string dbPath;
    std::atomic<uint64_t> nextSeriesId{1};
    
    // Helper methods for key generation
    static std::string seriesKey(uint64_t seriesId);
    static std::string measurementKey(const std::string& measurement, uint64_t seriesId);
    static std::string tagKey(const std::string& measurement, const std::string& tagKey, 
                              const std::string& tagValue, uint64_t seriesId);
    static std::string compositeTagKey(const std::string& measurement, 
                                       const std::map<std::string, std::string>& tags, 
                                       uint64_t seriesId);
    static std::string fieldKey(const std::string& measurement, const std::string& field, 
                                uint64_t seriesId);
    static std::string groupByKey(const std::string& measurement, const std::string& tagKey, 
                                  const std::string& tagValue);
    static std::string seriesLookupKey(const std::string& seriesKey);
    
    // Generate all tag subsets for composite index
    std::vector<std::string> generateTagSubsets(const std::map<std::string, std::string>& tags);
    
    // Build sorted tag string for consistent keys
    static std::string buildSortedTagString(const std::map<std::string, std::string>& tags);
    
public:
    MetadataIndex(const std::string& path);
    ~MetadataIndex();
    
    // Initialize the index
    seastar::future<> init();
    
    // Close the index
    seastar::future<> close();
    
    // Get or create series ID for a series
    seastar::future<uint64_t> getOrCreateSeriesId(const std::string& measurement,
                                                  const std::map<std::string, std::string>& tags,
                                                  const std::string& field);
    
    // Update series metadata
    seastar::future<> updateSeriesMetadata(const SeriesMetadata& metadata);
    
    // Find series by measurement
    seastar::future<std::vector<uint64_t>> findSeriesByMeasurement(const std::string& measurement);
    
    // Find series by single tag
    seastar::future<std::vector<uint64_t>> findSeriesByTag(const std::string& measurement,
                                                           const std::string& tagKey,
                                                           const std::string& tagValue);
    
    // Find series by multiple tags
    seastar::future<std::vector<uint64_t>> findSeriesByTags(const std::string& measurement,
                                                            const std::map<std::string, std::string>& tags);
    
    // Get series metadata
    seastar::future<std::optional<SeriesMetadata>> getSeriesMetadata(uint64_t seriesId);
    
    // Get all fields for a measurement
    seastar::future<std::set<std::string>> getMeasurementFields(const std::string& measurement);
    
    // Get all tag keys for a measurement
    seastar::future<std::set<std::string>> getMeasurementTagKeys(const std::string& measurement);
    
    // Get all tag values for a measurement and tag key
    seastar::future<std::set<std::string>> getTagValues(const std::string& measurement,
                                                        const std::string& tagKey);
    
    // Update field statistics
    seastar::future<> updateFieldStats(const std::string& measurement,
                                       const std::string& field,
                                       uint64_t seriesId,
                                       const FieldStats& stats);
    
    // Get series IDs grouped by tag value
    seastar::future<std::map<std::string, std::vector<uint64_t>>> 
        getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey);
    
    // Delete series metadata (for cleanup)
    seastar::future<> deleteSeries(uint64_t seriesId);
    
    // Get database statistics (for monitoring)
    std::string getStats() const;
};

// Global metadata index instance (singleton per node, not per shard)
extern std::unique_ptr<MetadataIndex> globalMetadataIndex;

// Initialize global metadata index
seastar::future<> initGlobalMetadataIndex(const std::string& basePath);

// Shutdown global metadata index
seastar::future<> shutdownGlobalMetadataIndex();

#endif // __METADATA_INDEX_H_INCLUDED__