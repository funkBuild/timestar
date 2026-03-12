#ifndef TSM_TOMBSTONE_H_INCLUDED
#define TSM_TOMBSTONE_H_INCLUDED

#include "series_id.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/temporary_buffer.hh>
#include <set>
#include <string>
#include <vector>

namespace timestar {

// Magic number for tombstone files ('TSMT' in hex)
constexpr uint32_t TOMBSTONE_MAGIC = 0x54534D54;
constexpr uint32_t TOMBSTONE_VERSION = 2;
constexpr uint32_t TOMBSTONE_VERSION_V1 = 1;  // Legacy version with uint64_t seriesId

// Individual tombstone entry
struct TombstoneEntry {
    SeriesId128 seriesId;  // Series identifier (16 bytes)
    uint64_t startTime;    // Start of deletion range in nanoseconds (8 bytes)
    uint64_t endTime;      // End of deletion range in nanoseconds (8 bytes)
    uint32_t checksum;     // CRC32 checksum of entry (4 bytes)

    // Total size: 36 bytes (16+8+8+4)
    static constexpr size_t SIZE = 36;

    // V1 entry size for backward compatibility (uint64_t seriesId was 8 bytes)
    static constexpr size_t V1_SIZE = 28;

    // Comparison operators for sorting
    bool operator<(const TombstoneEntry& other) const {
        if (seriesId != other.seriesId)
            return seriesId < other.seriesId;
        if (startTime != other.startTime)
            return startTime < other.startTime;
        return endTime < other.endTime;
    }

    bool operator==(const TombstoneEntry& other) const {
        return seriesId == other.seriesId && startTime == other.startTime && endTime == other.endTime;
    }
};

// Header structure for tombstone file
struct TombstoneHeader {
    uint32_t magic;           // Magic number (4 bytes)
    uint32_t version;         // Version (4 bytes)
    uint32_t entryCount;      // Number of entries (4 bytes)
    uint32_t headerChecksum;  // Header checksum (4 bytes)

    // Total size: 16 bytes
    static constexpr size_t SIZE = 16;
};

// Forward declaration
class TSM;

class TSMTombstone {
private:
    std::string tombstonePath;
    std::vector<TombstoneEntry> entries;

    // Index for fast lookups: seriesId -> vector of (startTime, endTime) pairs
    std::map<SeriesId128, std::vector<std::pair<uint64_t, uint64_t>>> seriesRanges;

    seastar::file tombstoneFile;
    bool isOpen = false;
    bool isDirty = false;

    // Helper methods
    uint32_t calculateChecksum(const TombstoneEntry& entry) const;
    uint32_t calculateHeaderChecksum(const TombstoneHeader& header) const;
    uint64_t calculateFileChecksum() const;
    void rebuildIndex();
    void sortAndMergeEntries();

public:
    TSMTombstone(const std::string& path);
    ~TSMTombstone() = default;

    // File operations
    seastar::future<> load();
    seastar::future<> flush();
    seastar::future<> close();
    seastar::future<bool> exists() const;
    seastar::future<> remove();

    // Add a new tombstone with verification
    // Returns true if tombstone was added, false if verification failed
    seastar::future<bool> addTombstone(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                       TSM* tsmFile = nullptr  // Optional: for verification
    );

    // Check if a specific point is tombstoned
    bool isDeleted(const SeriesId128& seriesId, uint64_t timestamp) const;

    // Check if any part of a range is tombstoned
    bool hasDeletedRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) const;

    // Get all tombstone ranges for a series
    std::vector<std::pair<uint64_t, uint64_t>> getTombstoneRanges(const SeriesId128& seriesId) const;

    // Get all tombstoned series IDs
    std::set<SeriesId128> getTombstonedSeries() const;

    // Filter timestamps and values based on tombstones
    template <typename T>
    std::pair<std::vector<uint64_t>, std::vector<T>> filterTombstoned(const SeriesId128& seriesId,
                                                                      const std::vector<uint64_t>& timestamps,
                                                                      const std::vector<T>& values) const;

    // Merge with another tombstone file (used during compaction)
    void merge(const TSMTombstone& other);

    // Remove tombstones that are no longer needed after compaction
    void compact(uint64_t minTime, uint64_t maxTime);

    // Statistics
    size_t getEntryCount() const { return entries.size(); }
    size_t getSeriesCount() const { return seriesRanges.size(); }
    uint64_t getFileSize() const;

    // Testing helpers
    const std::vector<TombstoneEntry>& getEntries() const { return entries; }
    void clearEntries() {
        entries.clear();
        seriesRanges.clear();
        isDirty = true;
    }
};

// Template implementation for filtering
template <typename T>
std::pair<std::vector<uint64_t>, std::vector<T>> TSMTombstone::filterTombstoned(const SeriesId128& seriesId,
                                                                                const std::vector<uint64_t>& timestamps,
                                                                                const std::vector<T>& values) const {
    if (timestamps.size() != values.size()) {
        throw std::invalid_argument("Timestamps and values must have same size");
    }

    // Get tombstone ranges for this series
    auto it = seriesRanges.find(seriesId);
    if (it == seriesRanges.end() || it->second.empty()) {
        // No tombstones for this series, return as-is
        return {timestamps, values};
    }

    const auto& ranges = it->second;
    std::vector<uint64_t> filteredTimestamps;
    std::vector<T> filteredValues;

    filteredTimestamps.reserve(timestamps.size());
    filteredValues.reserve(values.size());

    // Filter out tombstoned points
    for (size_t i = 0; i < timestamps.size(); ++i) {
        bool isTombstoned = false;
        uint64_t ts = timestamps[i];

        // Binary search for applicable tombstone range
        auto rangeIt = std::lower_bound(ranges.begin(), ranges.end(), std::make_pair(ts + 1, uint64_t(0)));

        if (rangeIt != ranges.begin()) {
            --rangeIt;
            if (ts >= rangeIt->first && ts <= rangeIt->second) {
                isTombstoned = true;
            }
        }

        if (!isTombstoned) {
            filteredTimestamps.push_back(timestamps[i]);
            filteredValues.push_back(values[i]);
        }
    }

    return {filteredTimestamps, filteredValues};
}

}  // namespace timestar

#endif  // TSM_TOMBSTONE_H_INCLUDED