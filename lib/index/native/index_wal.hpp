#ifndef NATIVE_INDEX_WAL_H_INCLUDED
#define NATIVE_INDEX_WAL_H_INCLUDED

#include "write_batch.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <string>

namespace timestar::index {

class MemTable;

// Write-ahead log for the native index. Ensures MemTable durability.
//
// Record format:
//   record_length   (uint32_t, LE)  - length of CRC + sequence + payload
//   crc32           (uint32_t, LE)  - CRC32 of sequence + payload
//   sequence_number (uint64_t, LE)  - monotonically increasing
//   payload         (variable)      - serialized WriteBatch
class IndexWAL {
public:
    // Open or create a WAL file in the given directory.
    static seastar::future<IndexWAL> open(std::string directory);

    // Append a write batch to the WAL. Returns after the data is fsynced.
    seastar::future<> append(const IndexWriteBatch& batch);

    // Replay all records into a MemTable. Used during recovery.
    seastar::future<uint64_t> replay(MemTable& target);

    // Rotate: close current WAL, start a new file.
    // Returns the path of the old WAL file (caller can delete after flush).
    seastar::future<std::string> rotate();

    // Delete a WAL file by path (after MemTable has been flushed to SSTable).
    static seastar::future<> deleteFile(const std::string& path);

    seastar::future<> close();

    uint64_t sequenceNumber() const { return sequence_; }
    const std::string& currentPath() const { return currentPath_; }

private:
    IndexWAL() = default;

    seastar::future<> openFile(const std::string& path);

    std::string directory_;
    std::string currentPath_;
    uint64_t sequence_ = 0;
    uint64_t walGeneration_ = 0;

    static uint32_t computeCrc32(const char* data, size_t len);
    static std::string walFileName(const std::string& dir, uint64_t generation);
};

}  // namespace timestar::index

#endif  // NATIVE_INDEX_WAL_H_INCLUDED
