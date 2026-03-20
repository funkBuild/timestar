#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// Tracks the set of SSTable files and their levels.
// Persisted as an append-only manifest file with periodic snapshots.
//
// Manifest file format:
//   [Snapshot record] (periodic, contains full file set)
//   [Delta records]   (add-file, remove-file)
//
// Record format:
//   record_type  (uint8_t)  0=Snapshot, 1=AddFile, 2=RemoveFile
//   payload      (variable, depends on type)
//
// All I/O uses Seastar's native async DMA file operations.
class Manifest {
public:
    static seastar::future<Manifest> open(std::string directory);

    // Current file set
    const std::vector<SSTableMetadata>& files() const { return files_; }
    std::vector<SSTableMetadata> filesAtLevel(int level) const;

    // Mutations (appended to manifest file)
    seastar::future<> addFile(const SSTableMetadata& info);
    seastar::future<> removeFiles(const std::vector<uint64_t>& fileNumbers);

    // Atomic add+remove: writes both records in a single append+fsync.
    // Prevents crash-window where both old and new data exist in the manifest.
    seastar::future<> atomicReplaceFiles(const SSTableMetadata& newFile, const std::vector<uint64_t>& removeFileNums);

    // Write a full snapshot (compacts the manifest file)
    seastar::future<> writeSnapshot();

    // Recovery: rebuild file set from manifest
    seastar::future<> recover();

    seastar::future<> close();

    // File number generator (monotonically increasing)
    uint64_t nextFileNumber() { return nextFileNumber_++; }
    uint64_t currentFileNumber() const { return nextFileNumber_; }

    const std::string& directory() const { return directory_; }

private:
    Manifest() = default;

    // Append a length-prefixed frame to the manifest file using DMA I/O.
    seastar::future<> appendFrame(const std::string& frame);

    // Open (or reopen) the manifest file handle for appending.
    seastar::future<> openFileForAppend();

    std::string serializeSnapshot() const;
    std::string serializeAddFile(const SSTableMetadata& info) const;
    std::string serializeRemoveFile(uint64_t fileNumber) const;

    std::string directory_;
    std::string manifestPath_;
    uint64_t nextFileNumber_ = 1;
    std::vector<SSTableMetadata> files_;

    // DMA file handle kept open for append writes
    seastar::file file_;
    bool fileOpen_ = false;
    size_t dmaAlign_ = 0;
    uint64_t writeOffset_ = 0;  // Logical (actual data) offset in the file

    enum RecordType : uint8_t { Snapshot = 0, AddFile = 1, RemoveFile = 2 };
};

}  // namespace timestar::index
