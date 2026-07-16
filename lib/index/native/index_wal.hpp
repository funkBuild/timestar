#pragma once

#include "write_batch.hpp"

#include <cstdint>
#include <memory>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/future.hh>
#include <seastar/core/semaphore.hh>
#include <string>
#include <vector>

namespace timestar::index {

class MemTable;

// Write-ahead log for the native index. Ensures MemTable durability.
//
// Record format:
//   record_length   (uint32_t, LE)  - length of CRC + sequence + payload
//   crc32           (uint32_t, LE)  - CRC32 of sequence + payload
//   sequence_number (uint64_t, LE)  - monotonically increasing
//   payload         (variable)      - serialized WriteBatch
//
// Uses Seastar-native DMA I/O with manual alignment for writes.
// No thread-pool crossings (unlike the old seastar::async + std::ofstream pattern).
class IndexWAL {
public:
    ~IndexWAL();
    IndexWAL(IndexWAL&&) noexcept = default;
    IndexWAL& operator=(IndexWAL&&) noexcept = default;

    // Open or create a WAL file in the given directory.
    static seastar::future<IndexWAL> open(std::string directory);

    // Append a write batch to the WAL buffer. Data is made durable by the
    // owner's periodic sync() (bounded window), by rotate(), and by close() —
    // NOT on every append. Callers needing immediate durability call sync().
    seastar::future<> append(const IndexWriteBatch& batch);

    // Make everything appended so far durable (flush buffer + padded tail +
    // fsync) without disturbing append state. Cheap no-op when clean.
    seastar::future<> sync();

    // Replay all records into a MemTable. Used during recovery.
    // Does NOT delete replayed files — the caller must first make the replayed
    // data durable (flush to SSTable), then call purgeReplayedFiles().
    seastar::future<uint64_t> replay(MemTable& target);

    // Delete older WAL generations consumed by replay(). Only call after the
    // replayed data has been made durable elsewhere (e.g. SSTable flush).
    seastar::future<> purgeReplayedFiles();

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

    // Open/reopen the Seastar DMA file for the current WAL.
    seastar::future<> openFile();

    std::string directory_;
    std::string currentPath_;
    std::vector<std::string> oldWalPaths_;  // Older WAL generations found on open(), replayed before current
    uint64_t sequence_ = 0;
    uint64_t walGeneration_ = 0;

    // Seastar DMA file handle and write positions
    std::optional<seastar::file> walFile_;
    uint64_t writePos_ = 0;     // Logical bytes written (for truncate)
    uint64_t dmaWritePos_ = 0;  // Physical DMA-aligned write offset
    size_t dmaAlignment_ = 4096;

    // Write buffer — accumulates records, flushed on sync()/rotate()/close() or when full.
    // WAL files are typically small (< 4MB before MemTable flush triggers rotation).
    // Large buffer minimizes DMA write frequency during bulk insert bursts.
    std::string buffer_;
    std::string tailBuf_;  // Partial DMA block from previous flush (avoids disk read-back)
    static constexpr size_t BUFFER_CAPACITY = 1024 * 1024;  // 1 MB

    // Serializes flushBuffer/flushTail/sync/rotate/close against each other.
    // Coroutines interleave at every co_await on the same shard; two in-flight
    // flushes would both compute write offsets from a stale dmaWritePos_ and
    // corrupt the log (unique_ptr keeps IndexWAL movable).
    std::unique_ptr<seastar::semaphore> writeSem_ = std::make_unique<seastar::semaphore>(1);
    bool needsSync_ = false;  // Set on append; cleared when a sync/rotate/close makes data durable

    seastar::future<> flushBuffer();
    seastar::future<> flushTail();  // Write remaining partial DMA block before close/rotate

    // Replay a single WAL file into the target MemTable. Returns records replayed.
    seastar::future<uint64_t> replayOneFile(const std::string& path, MemTable& target);

    static uint32_t computeCrc32(const char* data, size_t len);
    static std::string walFileName(const std::string& dir, uint64_t generation);
};

}  // namespace timestar::index
