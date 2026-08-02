#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <limits>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// Tracks the set of SSTable files and their levels.
// Persisted as an append-only manifest file with periodic snapshots.
//
// Manifest file format (v1):
//   [Header (8 bytes): magic "TSMF" (uint32 LE) + version=1 (uint32 LE)]
//   [Snapshot frame] (periodic, contains full file set)
//   [Delta frames]   (add-file, remove-file)
//
// Frame format:
//   record_len   (uint32 LE)  length of the record payload
//   record_crc   (uint32 LE)  CRC32 over the record payload
//   record       (record_len bytes)
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

    // Crash-safe add+remove: writes the add first and all removals in one
    // append+fsync. A durable prefix may retain redundant old files, but can
    // never remove a source before naming the replacement.
    seastar::future<> atomicReplaceFiles(const SSTableMetadata& newFile, const std::vector<uint64_t>& removeFileNums);

    // Write a full snapshot (compacts the manifest file)
    seastar::future<> writeSnapshot();

    // Recovery: rebuild file set from manifest
    seastar::future<> recover();

    seastar::future<> close();

    // File number generator (monotonically increasing)
    uint64_t nextFileNumber() {
        if (nextFileNumber_ == std::numeric_limits<uint64_t>::max()) {
            throw std::overflow_error("Manifest SSTable file number exhausted");
        }
        return nextFileNumber_++;
    }
    uint64_t currentFileNumber() const { return nextFileNumber_; }

    const std::string& directory() const { return directory_; }

    // v1 header: magic "TSMF" + version. Little-endian fixed32 encoding.
    static constexpr uint32_t MANIFEST_MAGIC = 0x464D5354;  // "TSMF"
    static constexpr uint32_t MANIFEST_VERSION = 1;
    static constexpr size_t MANIFEST_HEADER_SIZE = 8;

private:
    Manifest() = default;

    // Append a length-prefixed frame to the manifest file using DMA I/O.
    seastar::future<> appendFrame(const std::string& frame);

    // Append one CRC-framed record ([len][crc][record]) to out.
    static void appendRecordFrame(std::string& out, const std::string& record);

    // Apply a single decoded record (type byte + payload) to in-memory state.
    void applyRecord(const char* rp, const char* rend);

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

    // Set when recover() stopped before consuming a physically incomplete tail.
    // Complete corruption throws and is preserved; open() rewrites only a
    // provably torn tail so new appends never land after unreachable bytes.
    bool recoveryTruncated_ = false;

    enum RecordType : uint8_t { Snapshot = 0, AddFile = 1, RemoveFile = 2 };
};

}  // namespace timestar::index
