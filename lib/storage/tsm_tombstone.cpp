#include "tsm_tombstone.hpp"
#include "tsm.hpp"
#include <seastar/core/fstream.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>
#include <algorithm>
#include <cstring>

namespace tsdb {

TSMTombstone::TSMTombstone(const std::string& path) 
    : tombstonePath(path) {
}

// Simple CRC32 implementation for checksums
static uint32_t crc32_table[256];
static bool crc32_table_initialized = false;

static void init_crc32_table() {
    if (crc32_table_initialized) return;
    
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int j = 0; j < 8; j++) {
            c = (c >> 1) ^ ((c & 1) ? 0xEDB88320 : 0);
        }
        crc32_table[i] = c;
    }
    crc32_table_initialized = true;
}

static uint32_t calculate_crc32(const void* data, size_t len) {
    init_crc32_table();
    
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    
    for (size_t i = 0; i < len; i++) {
        crc = (crc >> 8) ^ crc32_table[(crc & 0xFF) ^ bytes[i]];
    }
    
    return ~crc;
}

uint32_t TSMTombstone::calculateChecksum(const TombstoneEntry& entry) const {
    // Calculate checksum of the entry data (excluding checksum field)
    return calculate_crc32(&entry, TombstoneEntry::SIZE - sizeof(uint32_t));
}

uint32_t TSMTombstone::calculateHeaderChecksum(const TombstoneHeader& header) const {
    // Calculate checksum of header (excluding checksum field)
    return calculate_crc32(&header, TombstoneHeader::SIZE - sizeof(uint32_t));
}

uint64_t TSMTombstone::calculateFileChecksum() const {
    // Calculate checksum of all entries
    uint64_t checksum = 0xFFFFFFFFFFFFFFFF;
    
    for (const auto& entry : entries) {
        // Simple 64-bit checksum by combining CRC32 values
        uint32_t entryCrc = calculate_crc32(&entry, TombstoneEntry::SIZE);
        checksum = ((checksum << 32) | (checksum >> 32)) ^ entryCrc;
    }
    
    return checksum;
}

void TSMTombstone::rebuildIndex() {
    seriesRanges.clear();
    
    for (const auto& entry : entries) {
        seriesRanges[entry.seriesId].push_back({entry.startTime, entry.endTime});
    }
    
    // Sort ranges for each series for efficient binary search
    for (auto& [seriesId, ranges] : seriesRanges) {
        std::sort(ranges.begin(), ranges.end());
    }
}

void TSMTombstone::sortAndMergeEntries() {
    if (entries.empty()) return;
    
    // Sort entries by series ID, then start time
    std::sort(entries.begin(), entries.end());
    
    // Merge overlapping and adjacent ranges
    std::vector<TombstoneEntry> merged;
    
    TombstoneEntry current = entries[0];
    
    for (size_t i = 1; i < entries.size(); ++i) {
        const auto& entry = entries[i];
        
        if (entry.seriesId == current.seriesId &&
            entry.startTime <= current.endTime + 1) {
            // Overlapping or adjacent range, merge
            current.endTime = std::max(current.endTime, entry.endTime);
        } else {
            // Different series or non-adjacent range
            current.checksum = calculateChecksum(current);
            merged.push_back(current);
            current = entry;
        }
    }
    
    // Add the last entry
    current.checksum = calculateChecksum(current);
    merged.push_back(current);
    
    entries = std::move(merged);
    isDirty = true;
}

seastar::future<> TSMTombstone::load() {
    try {
        auto exists = co_await seastar::file_exists(tombstonePath);
        if (!exists) {
            // File doesn't exist, nothing to load
            co_return;
        }
        
        tombstoneFile = co_await seastar::open_file_dma(
            tombstonePath,
            seastar::open_flags::ro
        );
        
        isOpen = true;
        
        // Get file size
        auto stat = co_await tombstoneFile.stat();
        auto fileSize = stat.st_size;
        
        if (fileSize < TombstoneHeader::SIZE) {
            // File too small to contain valid header
            co_await close();
            throw std::runtime_error("Tombstone file too small for header");
        }
        
        // Read header
        auto headerBuf = co_await tombstoneFile.dma_read_exactly<char>(0, TombstoneHeader::SIZE);
        TombstoneHeader header;
        std::memcpy(&header, headerBuf.get(), TombstoneHeader::SIZE);
        
        // Verify magic number and version
        if (header.magic != TOMBSTONE_MAGIC) {
            co_await close();
            throw std::runtime_error("Invalid tombstone file magic number");
        }
        
        if (header.version != TOMBSTONE_VERSION) {
            co_await close();
            throw std::runtime_error("Unsupported tombstone file version");
        }
        
        // Verify header checksum
        uint32_t expectedChecksum = header.headerChecksum;
        header.headerChecksum = 0;
        uint32_t actualChecksum = calculateHeaderChecksum(header);
        
        if (expectedChecksum != actualChecksum) {
            co_await close();
            throw std::runtime_error("Tombstone header checksum mismatch");
        }
        
        // Calculate expected file size
        size_t expectedSize = TombstoneHeader::SIZE + 
                             (header.entryCount * TombstoneEntry::SIZE) + 
                             sizeof(uint64_t);  // Footer checksum
        
        if (fileSize != expectedSize) {
            co_await close();
            throw std::runtime_error("Tombstone file size mismatch");
        }
        
        // Read entries
        entries.clear();
        entries.reserve(header.entryCount);
        
        size_t offset = TombstoneHeader::SIZE;
        for (uint32_t i = 0; i < header.entryCount; ++i) {
            auto entryBuf = co_await tombstoneFile.dma_read_exactly<char>(
                offset, TombstoneEntry::SIZE);
            
            TombstoneEntry entry;
            std::memcpy(&entry, entryBuf.get(), TombstoneEntry::SIZE);
            
            // Verify entry checksum
            uint32_t expectedEntryChecksum = entry.checksum;
            entry.checksum = 0;
            uint32_t actualEntryChecksum = calculateChecksum(entry);
            entry.checksum = expectedEntryChecksum;
            
            if (expectedEntryChecksum != actualEntryChecksum) {
                co_await close();
                throw std::runtime_error("Tombstone entry checksum mismatch at index " + 
                                       std::to_string(i));
            }
            
            entries.push_back(entry);
            offset += TombstoneEntry::SIZE;
        }
        
        // Read and verify file checksum
        auto footerBuf = co_await tombstoneFile.dma_read_exactly<char>(
            offset, sizeof(uint64_t));
        uint64_t fileChecksum;
        std::memcpy(&fileChecksum, footerBuf.get(), sizeof(uint64_t));
        
        uint64_t actualFileChecksum = calculateFileChecksum();
        if (fileChecksum != actualFileChecksum) {
            co_await close();
            throw std::runtime_error("Tombstone file checksum mismatch");
        }
        
        // Rebuild index for fast lookups
        rebuildIndex();
        
        co_await close();
        isDirty = false;
        
    } catch (const std::exception& e) {
        // Can't use co_await in catch block - flag for cleanup
        // Will close outside catch block
        throw std::runtime_error("Failed to load tombstone file: " + 
                                std::string(e.what()));
    }
}

seastar::future<> TSMTombstone::flush() {
    if (!isDirty && entries.empty()) {
        // Nothing to flush
        co_return;
    }
    
    try {
        // Sort and merge entries before writing
        sortAndMergeEntries();
        
        // Open file for writing
        tombstoneFile = co_await seastar::open_file_dma(
            tombstonePath,
            seastar::open_flags::rw | seastar::open_flags::create | 
            seastar::open_flags::truncate
        );
        
        isOpen = true;
        
        // Prepare header
        TombstoneHeader header;
        header.magic = TOMBSTONE_MAGIC;
        header.version = TOMBSTONE_VERSION;
        header.entryCount = entries.size();
        header.headerChecksum = 0;
        header.headerChecksum = calculateHeaderChecksum(header);
        
        // Write header
        auto headerBuf = seastar::temporary_buffer<char>::aligned(
            4096, TombstoneHeader::SIZE);  // Use 4K alignment
        std::memcpy(headerBuf.get_write(), &header, TombstoneHeader::SIZE);
        co_await tombstoneFile.dma_write(0, headerBuf.get(), TombstoneHeader::SIZE);
        
        // Write entries
        size_t offset = TombstoneHeader::SIZE;
        for (auto& entry : entries) {
            // Update checksum
            entry.checksum = 0;
            entry.checksum = calculateChecksum(entry);
            
            auto entryBuf = seastar::temporary_buffer<char>::aligned(
                4096, TombstoneEntry::SIZE);  // Use 4K alignment
            std::memcpy(entryBuf.get_write(), &entry, TombstoneEntry::SIZE);
            co_await tombstoneFile.dma_write(offset, entryBuf.get(), TombstoneEntry::SIZE);
            
            offset += TombstoneEntry::SIZE;
        }
        
        // Write file checksum
        uint64_t fileChecksum = calculateFileChecksum();
        auto footerBuf = seastar::temporary_buffer<char>::aligned(
            4096, sizeof(uint64_t));  // Use 4K alignment
        std::memcpy(footerBuf.get_write(), &fileChecksum, sizeof(uint64_t));
        co_await tombstoneFile.dma_write(offset, footerBuf.get(), sizeof(uint64_t));
        
        // Ensure data is written to disk
        co_await tombstoneFile.flush();
        co_await close();
        
        isDirty = false;
        
    } catch (const std::exception& e) {
        // Can't use co_await in catch block
        throw std::runtime_error("Failed to flush tombstone file: " + 
                                std::string(e.what()));
    }
}

seastar::future<> TSMTombstone::close() {
    if (isOpen) {
        co_await tombstoneFile.close();
        isOpen = false;
    }
}

seastar::future<bool> TSMTombstone::exists() const {
    return seastar::file_exists(tombstonePath);
}

seastar::future<> TSMTombstone::remove() {
    if (isOpen) {
        co_await close();
    }
    
    auto fileExists = co_await exists();
    if (fileExists) {
        co_await seastar::remove_file(tombstonePath);
    }
    
    entries.clear();
    seriesRanges.clear();
    isDirty = false;
}

seastar::future<bool> TSMTombstone::addTombstone(
    uint64_t seriesId,
    uint64_t startTime,
    uint64_t endTime,
    TSM* tsmFile) {
    
    // Validate time range
    if (startTime > endTime) {
        co_return false;
    }
    
    // Optional: Verify the data exists in TSM file
    if (tsmFile != nullptr) {
        // TODO: Implement verification once TSM interface is updated
        // For now, we'll trust the caller
    }
    
    // Create new entry
    TombstoneEntry entry;
    entry.seriesId = seriesId;
    entry.startTime = startTime;
    entry.endTime = endTime;
    entry.checksum = calculateChecksum(entry);
    
    // Add to entries
    entries.push_back(entry);
    
    // Update index
    seriesRanges[seriesId].push_back({startTime, endTime});
    
    isDirty = true;
    co_return true;
}

bool TSMTombstone::isDeleted(uint64_t seriesId, uint64_t timestamp) const {
    auto it = seriesRanges.find(seriesId);
    if (it == seriesRanges.end()) {
        return false;
    }
    
    const auto& ranges = it->second;
    
    // Binary search for applicable range
    auto rangeIt = std::lower_bound(ranges.begin(), ranges.end(),
        std::make_pair(timestamp + 1, uint64_t(0)));
    
    if (rangeIt != ranges.begin()) {
        --rangeIt;
        if (timestamp >= rangeIt->first && timestamp <= rangeIt->second) {
            return true;
        }
    }
    
    return false;
}

bool TSMTombstone::hasDeletedRange(uint64_t seriesId, 
                                   uint64_t startTime, 
                                   uint64_t endTime) const {
    auto it = seriesRanges.find(seriesId);
    if (it == seriesRanges.end()) {
        return false;
    }
    
    const auto& ranges = it->second;
    
    for (const auto& [rangeStart, rangeEnd] : ranges) {
        // Check if ranges overlap
        if (rangeStart <= endTime && rangeEnd >= startTime) {
            return true;
        }
        // Early exit if we've passed the query range
        if (rangeStart > endTime) {
            break;
        }
    }
    
    return false;
}

std::vector<std::pair<uint64_t, uint64_t>> 
TSMTombstone::getTombstoneRanges(uint64_t seriesId) const {
    auto it = seriesRanges.find(seriesId);
    if (it == seriesRanges.end()) {
        return {};
    }
    return it->second;
}

std::set<uint64_t> TSMTombstone::getTombstonedSeries() const {
    std::set<uint64_t> series;
    for (const auto& [seriesId, _] : seriesRanges) {
        series.insert(seriesId);
    }
    return series;
}

void TSMTombstone::merge(const TSMTombstone& other) {
    // Add all entries from other tombstone
    for (const auto& entry : other.entries) {
        entries.push_back(entry);
    }
    
    // Mark as dirty and rebuild index on next sort
    isDirty = true;
    
    // Rebuild index immediately for queries
    sortAndMergeEntries();
    rebuildIndex();
}

void TSMTombstone::compact(uint64_t minTime, uint64_t maxTime) {
    std::vector<TombstoneEntry> retained;
    
    for (const auto& entry : entries) {
        // Keep tombstones that are not completely within the compacted range
        if (!(entry.startTime >= minTime && entry.endTime <= maxTime)) {
            retained.push_back(entry);
        }
    }
    
    entries = std::move(retained);
    isDirty = true;
    rebuildIndex();
}

uint64_t TSMTombstone::getFileSize() const {
    if (entries.empty()) {
        return 0;
    }
    
    return TombstoneHeader::SIZE + 
           (entries.size() * TombstoneEntry::SIZE) + 
           sizeof(uint64_t);  // Footer checksum
}

} // namespace tsdb