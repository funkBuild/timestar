#ifndef METADATA_INDEX_SYNC_H_INCLUDED
#define METADATA_INDEX_SYNC_H_INCLUDED

// Synchronous version of MetadataIndex for testing without Seastar runtime

#include <string>
#include <vector>
#include <map>
#include <set>
#include <memory>
#include <optional>
#include <atomic>
#include <leveldb/db.h>
#include <leveldb/write_batch.h>

// Reuse the same data structures
struct SeriesMetadataSync {
    uint64_t seriesId;
    std::string measurement;
    std::map<std::string, std::string> tags;
    std::vector<std::string> fields;
    int64_t minTime;
    int64_t maxTime;
    uint32_t shardId;
    
    std::string serialize() const;
    static SeriesMetadataSync deserialize(const std::string& data);
    
    static std::string generateSeriesKey(const std::string& measurement, 
                                        const std::map<std::string, std::string>& tags,
                                        const std::string& field);
};

struct FieldStatsSync {
    std::string dataType;
    double minValue;
    double maxValue;
    uint64_t pointCount;
    
    std::string serialize() const;
    static FieldStatsSync deserialize(const std::string& data);
};

class MetadataIndexSync {
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
    
    std::vector<std::string> generateTagSubsets(const std::map<std::string, std::string>& tags);
    static std::string buildSortedTagString(const std::map<std::string, std::string>& tags);
    
public:
    MetadataIndexSync(const std::string& path);
    ~MetadataIndexSync();
    
    void init();
    void close();
    
    uint64_t getOrCreateSeriesId(const std::string& measurement,
                                 const std::map<std::string, std::string>& tags,
                                 const std::string& field);
    
    void updateSeriesMetadata(const SeriesMetadataSync& metadata);
    
    std::vector<uint64_t> findSeriesByMeasurement(const std::string& measurement);
    
    std::vector<uint64_t> findSeriesByTag(const std::string& measurement,
                                          const std::string& tagKey,
                                          const std::string& tagValue);
    
    std::vector<uint64_t> findSeriesByTags(const std::string& measurement,
                                           const std::map<std::string, std::string>& tags);
    
    std::optional<SeriesMetadataSync> getSeriesMetadata(uint64_t seriesId);
    
    std::set<std::string> getMeasurementFields(const std::string& measurement);
    std::set<std::string> getMeasurementTagKeys(const std::string& measurement);
    std::set<std::string> getTagValues(const std::string& measurement, const std::string& tagKey);
    
    void updateFieldStats(const std::string& measurement,
                         const std::string& field,
                         uint64_t seriesId,
                         const FieldStatsSync& stats);
    
    std::map<std::string, std::vector<uint64_t>> 
        getSeriesGroupedByTag(const std::string& measurement, const std::string& tagKey);
    
    void deleteSeries(uint64_t seriesId);
    
    std::string getStats() const;
};

#endif // METADATA_INDEX_SYNC_H_INCLUDED