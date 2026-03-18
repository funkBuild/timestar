// wal.cpp — stream-based, unaligned I/O version (fixed build errors)

#include "wal.hpp"

#include "aligned_buffer.hpp"
#include "bool_encoder_rle.hpp"
#include "crc32.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"
#include "string_encoder.hpp"
#include "tsm.hpp"
#include "zigzag.hpp"

#include <array>
#include <chrono>
#include <filesystem>
#include <optional>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// ------------------------ WAL ------------------------

WAL::WAL(unsigned int _sequenceNumber) : sequenceNumber(_sequenceNumber) {}

WAL::~WAL() {
    if (!_closed) {
        timestar::wal_log.warn("WAL destroyed without close() for seq {}", sequenceNumber);
    }
}

seastar::future<> WAL::init(MemoryStore* /*store*/, bool isRecovery) {
    std::string filename = sequenceNumberToFilename(sequenceNumber);

    // Ensure directory exists (blocking call, run off reactor thread)
    bool fileExisted = false;
    co_await seastar::async([&] {
        try {
            fs::create_directories(fs::path(filename).parent_path());
        } catch (...) {
            // best-effort
        }
        fileExisted = fs::exists(filename);
    });

    std::string_view filenameView{filename};

    // Determine open flags based on whether this is recovery or fresh creation
    seastar::open_flags openFlags;
    if (isRecovery) {
        // Recovery mode: open existing file for append, don't truncate
        if (!fileExisted) {
            timestar::wal_log.error("WAL file {} does not exist for recovery", filename);
            throw std::runtime_error("WAL file not found for recovery");
        }
        openFlags = seastar::open_flags::rw | seastar::open_flags::create;
        timestar::wal_log.debug("Opening WAL {} for recovery", filename);
    } else {
        // Fresh creation mode: always truncate to start with empty file
        if (fileExisted) {
            timestar::wal_log.warn(
                "WAL file {} already exists when creating new WAL. "
                "Truncating to start fresh.",
                filename);
        }
        openFlags = seastar::open_flags::rw | seastar::open_flags::create | seastar::open_flags::truncate;
        timestar::wal_log.debug("Creating fresh WAL {}", filename);
    }

    walFile = co_await seastar::open_file_dma(filenameView, openFlags);

    if (!walFile) {
        throw std::runtime_error("Failed to open WAL file: " + filename);
    }

    timestar::wal_log.debug("WAL file opened: {}", filename);

    // Get current file size
    filePos = co_await walFile.size();
    currentSize = filePos;

    if (!isRecovery && currentSize > 0) {
        timestar::wal_log.error("Fresh WAL {} has non-zero size {} after truncate - this is unexpected", filename,
                                currentSize);
    }

    // Capture DMA alignment from the file so we can pad before flushing
    _dma_alignment = walFile.disk_write_dma_alignment();
    _unflushed_bytes = 0;

    // Build buffered output stream.  The buffer size MUST be a multiple of
    // _dma_alignment so that automatic full-buffer flushes always land on
    // aligned positions.  We use file_output_stream_options to set this
    // explicitly (default 65536 is fine, it's a multiple of 4096).
    seastar::file_output_stream_options opts;
    opts.buffer_size = 262144;  // 256 KiB — larger buffer reduces flush frequency under high write load
    opts.write_behind = 4;      // Allow 4 buffers in-flight concurrently to overlap I/O
    opts.preallocation_size = 16 * 1024 * 1024;  // Pre-allocate 16 MiB (WAL max size) to reduce fragmentation
    auto s = co_await seastar::make_file_output_stream(walFile, opts);
    out.emplace(std::move(s));

    timestar::wal_log.debug("WAL stream init: pos={}, dma_alignment={}", filePos, _dma_alignment);
}

std::string WAL::sequenceNumberToFilename(unsigned int sequenceNumber) {
    std::string path = "shard_" + std::to_string(seastar::this_shard_id()) + "/";
    std::string sequenceNumStr = std::to_string(sequenceNumber);
    size_t padLen = sequenceNumStr.length() >= 10 ? 0 : 10 - sequenceNumStr.length();
    std::string filename = path + std::string(padLen, '0').append(sequenceNumStr).append(".wal");
    return filename;
}

seastar::future<> WAL::finalFlush() {
    if (!out)
        co_return;
    try {
        // Pad to DMA alignment before the final flush
        co_await padToAlignment();
        co_await out->flush();  // drains buffers and fsyncs the sink
        _unflushed_bytes = 0;
        timestar::wal_log.debug("WAL final flush completed, filePos={}", filePos);
    } catch (const std::exception& e) {
        timestar::wal_log.error("WAL final flush error: {}", e.what());
    }
}

seastar::future<> WAL::close() {
    // Prevent double-close
    if (_closed || !walFile) {
        timestar::wal_log.debug("WAL seq={} already closed or invalid file", sequenceNumber);
        co_return;
    }

    _closed = true;
    timestar::wal_log.debug("WAL seq={} starting close", sequenceNumber);

    // Wait for all in-flight operations to complete before closing
    timestar::wal_log.debug("WAL seq={} waiting for in-flight operations to complete", sequenceNumber);
    co_await _io_gate.close();
    timestar::wal_log.debug("WAL seq={} all operations completed", sequenceNumber);

    // Flush any buffered data to disk before closing the stream.
    // Without this, data may sit in the output_stream buffer or OS page cache
    // and never reach durable storage, violating WAL durability guarantees.
    if (out) {
        timestar::wal_log.debug("WAL seq={} performing final flush before close", sequenceNumber);
        try {
            co_await padToAlignment();
            co_await out->flush();
            _unflushed_bytes = 0;
            timestar::wal_log.debug("WAL seq={} final flush completed before close", sequenceNumber);
        } catch (const std::exception& e) {
            timestar::wal_log.error("WAL seq={} final flush error during close: {}", sequenceNumber, e.what());
        }
    }

    // Close the output stream before destroying it
    // Note: closing the stream also closes the underlying file
    if (out) {
        timestar::wal_log.debug("WAL seq={} closing output stream", sequenceNumber);
        try {
            co_await out->close();
            timestar::wal_log.debug("WAL seq={} output stream and file closed", sequenceNumber);
        } catch (const std::exception& e) {
            timestar::wal_log.error("WAL seq={} output stream close error: {}", sequenceNumber, e.what());
        }
        out.reset();
    } else if (walFile) {
        // Only close file directly if stream wasn't created
        timestar::wal_log.debug("WAL seq={} closing file directly", sequenceNumber);
        try {
            co_await walFile.flush();
            co_await walFile.close();
            timestar::wal_log.debug("WAL seq={} file close completed", sequenceNumber);
        } catch (const std::exception& e) {
            timestar::wal_log.error("WAL seq={} file close error: {}", sequenceNumber, e.what());
            // Don't rethrow during shutdown
        }
    }

    timestar::wal_log.debug("WAL seq={} closed successfully", sequenceNumber);
}

seastar::future<unsigned long> WAL::size() {
    auto s = co_await walFile.size();
    co_return static_cast<unsigned long>(s);
}

seastar::future<> WAL::remove() {
    // Ensure file is closed before removal to avoid dangling fd
    if (!_closed) {
        co_await close();
    }
    std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);
    co_await seastar::remove_file(filename);
}

// Write structured padding so that the total bytes written to the output stream
// since the last flush are a multiple of _dma_alignment.  This ensures the
// underlying file_data_sink_impl's write position stays aligned after the
// flush, preventing the DMA alignment assertion failure on subsequent writes.
//
// Padding format (Issue #11 fix):
//   [uint32_t  0]          <-- padding marker (entryLength == 0)
//   [uint32_t  skipBytes]  <-- number of additional zero bytes following
//   [skipBytes zero bytes]
//
// Total padding is always >= 8 bytes and a multiple of _dma_alignment above
// the current position.  If the natural gap to the next alignment boundary is
// less than 8 bytes, we target the boundary *after* that so the 8-byte header
// always fits.  The entire payload (header + zero body) is assembled into a
// static buffer (zero-initialised once) and issued as a single write.
seastar::future<> WAL::padToAlignment() {
    if (!out || _dma_alignment <= 1)
        co_return;

    const size_t remainder = _unflushed_bytes % _dma_alignment;
    if (remainder == 0)
        co_return;  // already aligned

    size_t padding = _dma_alignment - remainder;

    // The structured header needs 8 bytes (4-byte zero marker + 4-byte skip
    // count).  If the gap to the nearest alignment boundary is < 8 bytes, step
    // to the *next* boundary so the header fits while keeping
    // (_unflushed_bytes + padding) aligned.
    if (padding < 8) {
        padding += _dma_alignment;
    }

    // Thread-local buffer, zero-initialised once per shard thread.
    // Each Seastar shard runs on its own OS thread, so thread_local ensures
    // no cross-shard data race on the mutable padding header bytes.
    // Maximum padding is (_dma_alignment * 2 - 1) bytes; 8192 covers the
    // common 4096-byte DMA alignment with headroom.
    static constexpr size_t PAD_BUF_SIZE = 8192;
    static thread_local char buf[PAD_BUF_SIZE] = {};  // zero-initialised once per thread

    // Safety: if an exotic filesystem reports a very large DMA alignment,
    // fall back to a heap buffer rather than overflowing.
    char* pad_ptr = buf;
    std::vector<char> heap_buf;
    if (padding > PAD_BUF_SIZE) [[unlikely]] {
        heap_buf.resize(padding, '\0');
        pad_ptr = heap_buf.data();
    }

    // Write the structured header into the (reusable) buffer.
    // Bytes 0-3: zero marker (always 0, so no need to re-zero).
    // Bytes 4-7: skipBytes count.
    const uint32_t skipBytes = static_cast<uint32_t>(padding - 8);
    std::memcpy(pad_ptr + 4, &skipBytes, sizeof(skipBytes));

    co_await out->write(pad_ptr, padding);

    _unflushed_bytes += padding;
    filePos += padding;
    currentSize += padding;
}

// Flush pending buffered bytes in the output stream
seastar::future<> WAL::flushBlock() {
    if (!out)
        co_return;
    try {
#if TIMESTAR_LOG_INSERT_PATH
        auto startTime = std::chrono::steady_clock::now();
#endif

        // Pad to DMA alignment before flushing to keep file sink position aligned
        co_await padToAlignment();
        co_await out->flush();
        _unflushed_bytes = 0;  // reset after successful flush

#if TIMESTAR_LOG_INSERT_PATH
        auto endTime = std::chrono::steady_clock::now();
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
        LOG_INSERT_PATH(timestar::wal_log, debug, "WAL stream flush: pos={}, took={}ms", filePos, ms);
        if (ms > 100) {
            timestar::wal_log.warn("WAL flush took {}ms - potential I/O bottleneck", ms);
        }
#endif
    } catch (const std::exception& e) {
        timestar::wal_log.error("WAL flush error: {}", e.what());
        throw;  // Rethrow to preserve WAL durability guarantees
    }
}

template <class T>
size_t WAL::estimateInsertSize(TimeStarInsert<T>& insertRequest) {
    // Return cached result if available. The estimate depends on seriesKey,
    // timestamps count, and values -- all immutable once the insert is
    // constructed and entering the pipeline. The cache is invalidated by
    // setSharedTags() and setSharedTimestamps() if those are called.
    if (insertRequest._cachedEstimatedSize.has_value()) {
        return *insertRequest._cachedEstimatedSize;
    }

    const size_t count = insertRequest.getTimestamps().size();

    // Early termination: empty inputs have zero WAL cost.
    if (count == 0) [[unlikely]] {
        insertRequest._cachedEstimatedSize = 0;
        return 0;
    }

    // Fixed overhead: length prefix (4) + CRC32 (4) + WAL type (1) +
    // SeriesId128 (16) + value type (1) + timestamp count (4) +
    // encoded timestamp size prefix (4) + encoded value size prefix (4)
    constexpr size_t FIXED_OVERHEAD = 4 + 4 + 1 + 16 + 1 + 4 + 4 + 4;

    size_t estimatedSize = FIXED_OVERHEAD;

    // Series key string (length-prefixed)
    const std::string& seriesKey = insertRequest.seriesKey();
    estimatedSize += 4 + seriesKey.size();

    // Timestamps: apply adaptive compression ratio to raw size.
    // Raw size = count * 8 bytes (uint64_t). The ratio is learned from
    // recent encodeInsertEntry() calls via EMA.
    const size_t rawTsSize = count * sizeof(uint64_t);
    size_t estimatedTsSize = static_cast<size_t>(static_cast<double>(rawTsSize) * _compressionStats.timestampRatio);
    // Floor: at least 2 bytes per timestamp (delta-of-delta minimum overhead)
    estimatedTsSize = std::max(estimatedTsSize, count * 2);
    estimatedSize += estimatedTsSize;

    // Values: apply type-specific adaptive compression ratio.
    if constexpr (std::is_same_v<T, double>) {
        const size_t rawFloatSize = count * sizeof(double);
        size_t estimatedValSize = static_cast<size_t>(static_cast<double>(rawFloatSize) * _compressionStats.floatRatio);
        // Floor: at least 2 bytes per float (XOR minimum overhead)
        estimatedValSize = std::max(estimatedValSize, count * 2);
        estimatedSize += estimatedValSize;
    } else if constexpr (std::is_same_v<T, bool>) {
        const size_t rawBoolSize = count * sizeof(uint8_t);
        size_t estimatedValSize = static_cast<size_t>(static_cast<double>(rawBoolSize) * _compressionStats.boolRatio);
        // Floor: at least 1 byte per 8 bools (bit-packing minimum)
        estimatedValSize = std::max(estimatedValSize, (count + 7) / 8);
        estimatedSize += estimatedValSize;
    } else if constexpr (std::is_same_v<T, std::string>) {
        // For strings, compute raw size as sum of string lengths + 4 bytes per prefix
        size_t rawStringSize = 0;
        for (const auto& s : insertRequest.values) {
            rawStringSize += 4 + s.size();
        }
        size_t estimatedValSize =
            static_cast<size_t>(static_cast<double>(rawStringSize) * _compressionStats.stringRatio);
        // Floor: at least count bytes (minimum zstd overhead)
        estimatedValSize = std::max(estimatedValSize, count);
        estimatedSize += estimatedValSize;
    } else if constexpr (std::is_same_v<T, int64_t>) {
        const size_t rawIntSize = count * sizeof(int64_t);
        size_t estimatedValSize = static_cast<size_t>(static_cast<double>(rawIntSize) * _compressionStats.integerRatio);
        // Floor: at least 2 bytes per value (ZigZag + FFOR minimum overhead)
        estimatedValSize = std::max(estimatedValSize, count * 2);
        estimatedSize += estimatedValSize;
    }

    // Apply safety margin to avoid underestimation near the 16MB WAL limit.
    estimatedSize = static_cast<size_t>(static_cast<double>(estimatedSize) * CompressionStats::SAFETY_MARGIN);

    // Cache the result for subsequent calls on this insert
    insertRequest._cachedEstimatedSize = estimatedSize;

    return estimatedSize;
}

// Encode a single WAL insert entry (header + payload + CRC) into the provided
// buffer.  Shared by insert() and insertBatch() to avoid duplicating the
// encoding logic.  All allocations go into the caller's buffer; the only
// temporaries are the encoder outputs (which are memcpy'd in and destroyed).
//
// On-disk format per entry (unchanged, recovery-compatible):
//   [uint32_t entryLength]  -- CRC + payload size
//   [uint32_t CRC32]        -- over the payload bytes
//   [payload bytes ...]
template <class T>
void WAL::encodeInsertEntry(AlignedBuffer& buffer, TimeStarInsert<T>& insertRequest) {
    // Record where this entry starts so we can backpatch the header
    const size_t entryStart = buffer.size();

    // Write header placeholders (will be patched after encoding)
    buffer.write(static_cast<uint32_t>(0));  // placeholder: entryLength
    buffer.write(static_cast<uint32_t>(0));  // placeholder: CRC32

    const size_t payloadStart = buffer.size();

    // --- Payload directly into buffer ---
    buffer.write(static_cast<uint8_t>(WALType::Write));

    // Store SeriesId128 (fixed 16 bytes) -- write raw bytes directly from the
    // std::array, avoiding the toBytes() std::string allocation.
    const auto seriesId = insertRequest.seriesId128();
    const auto& rawId = seriesId.getRawData();
    buffer.write_array(rawId.data(), rawId.size());

    // Store series key string for recovery (length-prefixed).
    // Use const reference to avoid copying the cached string.
    const std::string& seriesKey = insertRequest.seriesKey();
    buffer.write(static_cast<uint32_t>(seriesKey.size()));
    buffer.write(seriesKey);

    // Value type
    buffer.write(static_cast<uint8_t>(TSM::getValueType<T>()));

    // Number of timestamps
    const auto& tsVec = insertRequest.getTimestamps();
    const size_t count = tsVec.size();
    buffer.write(static_cast<uint32_t>(count));

    // Compute raw sizes for compression ratio feedback
    const size_t rawTsSize = count * sizeof(uint64_t);

    // Encoded timestamps -- write directly into buffer (zero intermediate alloc)
    size_t encodedTsSize;
    {
        const size_t sizePos = buffer.size();
        buffer.write(static_cast<uint32_t>(0));  // placeholder for encoded size

        const size_t startPos = buffer.size();
        IntegerEncoder::encodeInto(tsVec, buffer);
        encodedTsSize = buffer.size() - startPos;
        buffer.writeAt(sizePos, static_cast<uint32_t>(encodedTsSize));
    }

    // Feed back actual timestamp compression ratio
    _compressionStats.updateTimestamp(rawTsSize, encodedTsSize);

    // Encoded values -- write directly into buffer (zero intermediate alloc)
    {
        const size_t sizePos = buffer.size();
        buffer.write(static_cast<uint32_t>(0));  // placeholder for encoded size

        const size_t startPos = buffer.size();

        if constexpr (std::is_same_v<T, double>) {
            const size_t rawValSize = count * sizeof(double);
            FloatEncoder::encodeInto(insertRequest.values, buffer);
            const size_t encodedValSize = buffer.size() - startPos;
            buffer.writeAt(sizePos, static_cast<uint32_t>(encodedValSize));
            _compressionStats.updateFloat(rawValSize, encodedValSize);
        } else if constexpr (std::is_same_v<T, bool>) {
            const size_t rawValSize = count * sizeof(uint8_t);
            BoolEncoderRLE::encodeInto(insertRequest.values, buffer);
            const size_t encodedValSize = buffer.size() - startPos;
            buffer.writeAt(sizePos, static_cast<uint32_t>(encodedValSize));
            _compressionStats.updateBool(rawValSize, encodedValSize);
        } else if constexpr (std::is_same_v<T, std::string>) {
            // Raw size for strings: sum of (4-byte prefix + string length) per entry
            size_t rawValSize = 0;
            for (const auto& s : insertRequest.values) {
                rawValSize += 4 + s.size();
            }
            StringEncoder::encodeInto(insertRequest.values, buffer);
            const size_t encodedValSize = buffer.size() - startPos;
            buffer.writeAt(sizePos, static_cast<uint32_t>(encodedValSize));
            _compressionStats.updateString(rawValSize, encodedValSize);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            const size_t rawValSize = count * sizeof(int64_t);
            // ZigZag encode int64 → uint64 using thread-local scratch buffer, then FFOR encode
            static thread_local std::vector<uint64_t> zigzagScratch;
            zigzagScratch.resize(count);
            ZigZag::zigzagEncodeInto(insertRequest.values, zigzagScratch.data());
            IntegerEncoder::encodeInto(zigzagScratch, buffer);
            const size_t encodedValSize = buffer.size() - startPos;
            buffer.writeAt(sizePos, static_cast<uint32_t>(encodedValSize));
            _compressionStats.updateInteger(rawValSize, encodedValSize);
        } else {
            static_assert(sizeof(T) == 0, "Unsupported WAL value type");
        }
    }

    // --- Backpatch header fields in-place ---
    const size_t payloadSize = buffer.size() - payloadStart;
    const uint32_t entryLength = static_cast<uint32_t>(sizeof(uint32_t) + payloadSize);  // CRC + payload
    buffer.writeAt(entryStart, entryLength);

    // Single-pass bulk CRC over the complete payload (cache-friendly sequential scan)
    uint32_t crc = CRC32::compute(buffer.data.data() + payloadStart, payloadSize);
    buffer.writeAt(entryStart + sizeof(uint32_t), crc);
}

template <class T>
seastar::future<WALInsertResult> WAL::insert(TimeStarInsert<T>& insertRequest) {
    // --- Encoding phase (no lock needed) ---
    // Reuse thread-local buffer: capacity persists across inserts, eliminating
    // per-insert heap allocation. clear() resets size but preserves capacity.
    static thread_local AlignedBuffer buffer;
    buffer.clear();
    buffer.reserve(estimateInsertSize(insertRequest));

    encodeInsertEntry(buffer, insertRequest);

    const size_t dataSize = buffer.size();

    // --- I/O phase (under lock) ---
    // Hold the gate BEFORE the semaphore so that WAL::close() (which calls
    // _io_gate.close()) will wait for coroutines queued on _io_sem rather
    // than racing past them and closing the stream while they're pending.
    auto gate_holder = _io_gate.hold();
    auto units = co_await seastar::get_units(_io_sem, 1);

    LOG_INSERT_PATH(timestar::wal_log, debug, "WAL::insert - dataSize={}, currentSize={}", dataSize, currentSize);

    // Respect the 16MiB WAL limit BEFORE writing
    const size_t projectedSize = currentSize + dataSize;
    if (projectedSize > MAX_WAL_SIZE) {
        timestar::wal_log.debug(
            "WAL::insert - Would exceed 16MB limit (current={}, "
            "dataSize={}, projected={}), signaling rollover needed",
            currentSize, dataSize, projectedSize);
        co_return WALInsertResult::RolloverNeeded;
    }

    // Stream write
    try {
        if (out) {
            co_await out->write(reinterpret_cast<const char*>(buffer.data.data()), dataSize);
        } else {
            throw std::runtime_error("WAL output stream is null");
        }
        // Update positions and size
        filePos += dataSize;
        currentSize += dataSize;
        _unflushed_bytes += dataSize;

        // Only flush immediately if configured for immediate mode
        if (requiresImmediateFlush) {
            co_await padToAlignment();
            co_await out->flush();
            _unflushed_bytes = 0;
        }
    } catch (const std::exception& e) {
        timestar::wal_log.error("WAL::insert write failed: {}", e.what());
        throw;
    }

    co_return WALInsertResult::Success;
}

template <class T>
seastar::future<WALInsertResult> WAL::insertBatch(std::vector<TimeStarInsert<T>>& insertRequests) {
    if (insertRequests.empty()) {
        co_return WALInsertResult::Success;
    }
    if (insertRequests.size() == 1) {
        co_return co_await insert(insertRequests[0]);
    }

    // --- Encoding phase (no lock needed) ---
    // Single pre-allocated buffer: compute estimated total size, allocate once,
    // encode all entries sequentially into the shared buffer, then write once.
    // This eliminates N separate allocations, N moves, and N stream writes.

    // First pass: compute total estimated size for pre-allocation
    size_t estimatedTotal = 0;
    for (auto& req : insertRequests) {
        estimatedTotal += estimateInsertSize(req);
    }

    // Reuse thread-local buffer (same one as insert() — safe because the
    // semaphore guarantees only one WAL write runs at a time per shard).
    static thread_local AlignedBuffer buffer;
    buffer.clear();
    buffer.reserve(estimatedTotal);

    // Second pass: encode each insert directly into the shared buffer
    for (auto& insertRequest : insertRequests) {
        encodeInsertEntry(buffer, insertRequest);
    }

    const size_t totalSize = buffer.size();

    // --- I/O phase (under lock) ---
    // Hold the gate BEFORE the semaphore (see insert() for rationale).
    auto gate_holder = _io_gate.hold();
    auto units = co_await seastar::get_units(_io_sem, 1);

    LOG_INSERT_PATH(timestar::wal_log, debug, "WAL::insertBatch - {} entries, total size={}, currentSize={}",
                    insertRequests.size(), totalSize, currentSize);

    // WAL limit check (under lock so currentSize is authoritative)
    const size_t projected = currentSize + totalSize;
    if (projected > MAX_WAL_SIZE) {
        timestar::wal_log.debug(
            "WAL::insertBatch - Would exceed 16MB limit (current={}, total={}, "
            "projected={}), signaling rollover",
            currentSize, totalSize, projected);
        co_return WALInsertResult::RolloverNeeded;
    }

    try {
        if (!out) {
            throw std::runtime_error("WAL output stream is null in insertBatch");
        }
        // Single write for the entire batch
        co_await out->write(reinterpret_cast<const char*>(buffer.data.data()), totalSize);
        // Only update positions atomically after the write succeeds
        filePos += totalSize;
        currentSize += totalSize;
        _unflushed_bytes += totalSize;

        // Only flush immediately if configured for immediate mode
        if (requiresImmediateFlush) {
            co_await padToAlignment();
            co_await out->flush();
            _unflushed_bytes = 0;
        }
    } catch (const std::exception& e) {
        timestar::wal_log.error("WAL::insertBatch write failed: {}", e.what());
        throw;
    }

    co_return WALInsertResult::Success;
}

seastar::future<> WAL::deleteRange(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime) {
    // --- Encoding phase (no lock needed) ---
    // Single-buffer encoding: write header placeholders, then payload, then
    // patch the length and CRC in-place to avoid a second buffer + copy.
    AlignedBuffer buffer;

    constexpr size_t LENGTH_OFFSET = 0;
    constexpr size_t CRC_OFFSET = sizeof(uint32_t);
    constexpr size_t PAYLOAD_OFFSET = 2 * sizeof(uint32_t);
    buffer.write((uint32_t)0);  // placeholder: entryLength
    buffer.write((uint32_t)0);  // placeholder: CRC32

    // --- Payload directly into buffer ---
    // Type
    buffer.write(static_cast<uint8_t>(WALType::DeleteRange));

    // Series ID (fixed 16 bytes) — write raw bytes directly, avoiding
    // the toBytes() std::string allocation.
    const auto& rawId = seriesId.getRawData();
    buffer.write_array(rawId.data(), rawId.size());

    // Time range
    buffer.write(startTime);
    buffer.write(endTime);

    // --- Patch header fields in-place ---
    const size_t payloadSize = buffer.size() - PAYLOAD_OFFSET;
    const uint32_t entryLength = static_cast<uint32_t>(sizeof(uint32_t) + payloadSize);
    std::memcpy(buffer.data.data() + LENGTH_OFFSET, &entryLength, sizeof(uint32_t));

    uint32_t crc = CRC32::compute(buffer.data.data() + PAYLOAD_OFFSET, payloadSize);
    std::memcpy(buffer.data.data() + CRC_OFFSET, &crc, sizeof(uint32_t));

    const size_t n = buffer.size();

    // --- I/O phase (under lock) ---
    // Hold the gate BEFORE the semaphore (see insert() for rationale).
    auto gate_holder = _io_gate.hold();
    auto units = co_await seastar::get_units(_io_sem, 1);

    try {
        if (!out) {
            throw std::runtime_error("WAL output stream is null in deleteRange");
        }
        co_await out->write(reinterpret_cast<const char*>(buffer.data.data()), n);
        filePos += n;
        currentSize += n;
        _unflushed_bytes += n;

        // Only flush immediately if configured for immediate mode
        if (requiresImmediateFlush) {
            co_await padToAlignment();
            co_await out->flush();
            _unflushed_bytes = 0;
        }
    } catch (const std::exception& e) {
        timestar::wal_log.error("WAL::deleteRange write failed: {}", e.what());
        throw;
    }

    LOG_INSERT_PATH(timestar::wal_log, debug,
                    "WAL deleteRange written: series={}, startTime={}, "
                    "endTime={}, size={} bytes",
                    seriesId.toHex(), startTime, endTime, n);
}

seastar::future<> WAL::remove(unsigned int sequenceNumber) {
    std::string filename = WAL::sequenceNumberToFilename(sequenceNumber);

    try {
        co_await seastar::remove_file(filename);
        timestar::wal_log.debug("WAL file {} deleted", filename);
    } catch (const std::exception& e) {
        // File may not exist; log but don't propagate
        timestar::wal_log.debug("WAL file {} removal failed (may not exist): {}", filename, e.what());
    }
}

// ------------------------ WALReader ------------------------

WALReader::WALReader(std::string _filename) : filename(std::move(_filename)) {}

seastar::future<> WALReader::readAll(MemoryStore* store) {
    std::string_view filenameView{filename};
    walFile = co_await seastar::open_file_dma(filenameView, seastar::open_flags::ro);

    if (!walFile) {
        timestar::wal_log.error("Failed to open WAL file: {}", filename);
        throw std::runtime_error("Failed opening WAL file");
    }

    length = co_await walFile.size();
    if (length == 0) {
        timestar::wal_log.info("WAL recovery complete: 0 entries read, 0 partial entries discarded");
        co_await walFile.close();
        co_return;
    }

    timestar::wal_log.info("WAL recovery starting for file {} with size {} bytes", filename, length);

    size_t entriesRead = 0;
    size_t partialEntries = 0;
    bool stopRecovery = false;

    // Stream the file; no aligned over-reads
    auto in = seastar::make_file_input_stream(walFile);

    // read_exact helper using read_up_to (no trim_front needed)
    auto read_exact = [&in](void* dst, size_t n) -> seastar::future<bool> {
        size_t got = 0;
        while (got < n) {
            auto chunk = co_await in.read_up_to(n - got);
            if (chunk.empty()) {
                co_return false;  // EOF before n bytes
            }
            std::memcpy(static_cast<char*>(dst) + got, chunk.get(), chunk.size());
            got += chunk.size();
        }
        co_return true;
    };

    std::exception_ptr recoveryException;
    try {
        while (true) {
            uint32_t entryLength = 0;
            bool have = co_await read_exact(&entryLength, sizeof(entryLength));
            if (!have) {
                break;  // normal EOF
            }

            // Skip DMA alignment padding.
            //
            // New format (Issue #11 fix): padding is written as
            //   [uint32_t  0]          <-- padding marker (already consumed above)
            //   [uint32_t  skipBytes]  <-- number of additional zero bytes to skip
            //   [skipBytes zero bytes]
            //
            // Backward compatibility with old format (raw zero-filled padding):
            //   Old WAL files wrote only zero bytes for padding. In that case
            //   skipBytes will itself be 0 (since the next 4 bytes are also zero).
            //   We simply loop back and read the next uint32_t, continuing until
            //   we hit a non-zero entryLength or EOF.
            if (entryLength == 0) {
                uint32_t skipBytes = 0;
                if (!(co_await read_exact(&skipBytes, sizeof(skipBytes)))) {
                    break;  // EOF within padding header
                }

                if (skipBytes > 0) {
                    // New-format padding: skip exactly skipBytes additional zero bytes.
                    static constexpr uint32_t MAX_SKIP = 16 * 1024 * 1024;
                    if (skipBytes > MAX_SKIP) {
                        timestar::wal_log.warn(
                            "WAL recovery: unreasonable padding skip count {}, "
                            "stopping recovery",
                            skipBytes);
                        partialEntries++;
                        break;
                    }
                    size_t remaining = skipBytes;
                    while (remaining > 0) {
                        char discard[4096];
                        size_t toRead = std::min(remaining, sizeof(discard));
                        if (!(co_await read_exact(discard, toRead))) {
                            break;  // EOF inside padding body
                        }
                        remaining -= toRead;
                    }
                }
                // For old-format padding (skipBytes == 0), we just consumed 8 zero
                // bytes total (the marker + skipBytes).  Loop back to read the next
                // entryLength; eventually we will reach a non-zero entry or EOF.
                continue;
            }

            // sanity
            static constexpr uint32_t MIN_ENTRY_SIZE = 4;
            static constexpr uint32_t MAX_ENTRY_SIZE = 10 * 1024 * 1024;  // 10MB

            if (entryLength < MIN_ENTRY_SIZE || entryLength > MAX_ENTRY_SIZE) {
                timestar::wal_log.warn("WAL recovery: suspicious entry length {}, stopping recovery", entryLength);
                partialEntries++;
                break;
            }

            std::vector<char> entry(entryLength);
            if (!(co_await read_exact(entry.data(), entry.size()))) {
                timestar::wal_log.warn("WAL recovery: partial tail entry ({} bytes), discarding", entryLength);
                partialEntries++;
                break;
            }

            // ---- CRC32 verification ----
            // New format: [crc32 (4 bytes)][payload...]
            // entryLength includes the CRC, so payload = entryLength - 4
            // Old format: [WALType byte][payload...] (no CRC)
            //
            // Detection: if entryLength >= 8, first check for old format by looking
            // at byte positions: old format has WALType at byte 0 (not byte 4),
            // while new format has CRC at bytes 0-3 and WALType at byte 4.
            // If old format is ruled out, verify CRC; mismatch -> corruption.
            const uint8_t* rawEntry = reinterpret_cast<const uint8_t*>(entry.data());
            const uint8_t* payloadPtr = rawEntry;
            size_t payloadSize = entryLength;

            if (entryLength >= 8) {
                uint32_t storedCrc;
                std::memcpy(&storedCrc, rawEntry, sizeof(uint32_t));

                const uint8_t* crcPayload = rawEntry + 4;
                size_t crcPayloadSize = entryLength - 4;
                uint32_t computedCrc = CRC32::compute(crcPayload, crcPayloadSize);

                // Check if the first byte looks like an old-format entry (valid WALType
                // at position 0 with no CRC prefix). In old format the WALType is the
                // very first byte; in new format the first 4 bytes are the CRC and the
                // WALType sits at byte 4.  We detect old format when byte 0 is a valid
                // WALType AND byte 4 is NOT a valid WALType (byte 4 would be SeriesId128
                // data in old format, not a WALType).
                uint8_t firstByte = rawEntry[0];
                bool byte0IsType = firstByte <= static_cast<uint8_t>(WALType::Close);
                bool byte4IsType = (entryLength > 4) && (rawEntry[4] <= static_cast<uint8_t>(WALType::Close));

                if (byte0IsType && !byte4IsType) {
                    // Old format entry (no CRC prefix) - skip CRC verification
                    timestar::wal_log.debug("WAL recovery: old format entry detected (no CRC), type={}", firstByte);
                    // payloadPtr and payloadSize already point to full entry
                } else if (storedCrc == computedCrc) {
                    // New format with valid CRC - use payload after CRC
                    payloadPtr = crcPayload;
                    payloadSize = crcPayloadSize;
                    timestar::wal_log.trace("WAL recovery: CRC32 verified for entry");
                } else {
                    // New-format entry with CRC mismatch: reject as corrupted.
                    // Do NOT fall through to old-format parsing, which would silently
                    // accept corrupted data.
                    timestar::wal_log.warn(
                        "WAL recovery: CRC32 mismatch (stored=0x{:08X}, computed=0x{:08X}), "
                        "discarding corrupt entry",
                        storedCrc, computedCrc);
                    partialEntries++;
                    continue;
                }
            }
            // If entryLength < 8, it's too small for new format; treat as old format

            Slice entrySlice(payloadPtr, payloadSize);
            uint8_t type = entrySlice.read<uint8_t>();

            switch (static_cast<WALType>(type)) {
                case WALType::Write: {
                    try {
                        // Read fixed 16-byte SeriesId128
                        std::string seriesIdBytes = entrySlice.readString(16);
                        SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);

                        // Read series key string (length-prefixed)
                        uint32_t seriesKeyLen = entrySlice.read<uint32_t>();
                        if (seriesKeyLen > entrySlice.remaining()) {
                            throw std::runtime_error("seriesKeyLen " + std::to_string(seriesKeyLen) +
                                                     " exceeds remaining " + std::to_string(entrySlice.remaining()));
                        }
                        std::string seriesKey = entrySlice.readString(seriesKeyLen);

                        uint8_t valueType = entrySlice.read<uint8_t>();

                        switch (static_cast<WALValueType>(valueType)) {
                            case WALValueType::Float: {
                                TimeStarInsert<double> insertReq = readSeries<double>(entrySlice, seriesKey);
                                store->insertMemory(std::move(insertReq));
                            } break;
                            case WALValueType::Boolean: {
                                TimeStarInsert<bool> insertReq = readSeries<bool>(entrySlice, seriesKey);
                                store->insertMemory(std::move(insertReq));
                            } break;
                            case WALValueType::String: {
                                TimeStarInsert<std::string> insertReq = readSeries<std::string>(entrySlice, seriesKey);
                                store->insertMemory(std::move(insertReq));
                            } break;
                            case WALValueType::Integer: {
                                TimeStarInsert<int64_t> insertReq = readSeries<int64_t>(entrySlice, seriesKey);
                                store->insertMemory(std::move(insertReq));
                            } break;
                            default:
                                timestar::wal_log.warn("WAL recovery: unknown value type {} in entry, skipping", valueType);
                                partialEntries++;
                                continue;
                        }
                        entriesRead++;
                    } catch (const std::exception& e) {
                        timestar::wal_log.error("WAL recovery: Failed to read Write entry: {}", e.what());
                        partialEntries++;
                        continue;
                    }
                } break;

                case WALType::Close: {
                    co_await store->close();
                    entriesRead++;
                } break;

                case WALType::DeleteRange: {
                    // Read fixed 16-byte SeriesId128
                    std::string seriesIdBytes = entrySlice.readString(16);
                    SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);

                    uint64_t startTime = entrySlice.read<uint64_t>();
                    uint64_t endTime = entrySlice.read<uint64_t>();
                    timestar::wal_log.debug("WAL recovery: DeleteRange for series={}, startTime={}, endTime={}",
                                            seriesId.toHex(), startTime, endTime);
                    store->deleteRange(seriesId, startTime, endTime);
                    entriesRead++;
                } break;

                default:
                    timestar::wal_log.warn("WAL recovery: unknown entry type {}, stopping recovery",
                                           static_cast<int>(type));
                    partialEntries++;
                    stopRecovery = true;
                    break;
            }

            if (stopRecovery) {
                break;
            }
        }

    } catch (...) {
        recoveryException = std::current_exception();
    }

    timestar::wal_log.info("WAL recovery complete: {} entries read, {} partial entries discarded", entriesRead,
                           partialEntries);

    // Close the input stream & file — runs on both normal and exception paths
    try {
        co_await in.close();
    } catch (...) {}
    try {
        co_await walFile.close();
    } catch (...) {}

    if (recoveryException) {
        std::rethrow_exception(recoveryException);
    }
}

template <class T>
TimeStarInsert<T> WALReader::readSeries(Slice& walSlice, const std::string& seriesKey) {
    TimeStarInsert<T> insertReq = TimeStarInsert<T>::fromSeriesKey(seriesKey);

    uint32_t timestampsCount = walSlice.read<uint32_t>();
    uint32_t encodedTimestampsSize = walSlice.read<uint32_t>();

    static constexpr uint32_t MAX_TIMESTAMPS = 10000000;            // 10M
    static constexpr uint32_t MAX_ENCODED_SIZE = 50 * 1024 * 1024;  // 50MiB

    if (timestampsCount > MAX_TIMESTAMPS) {
        timestar::wal_log.error("WAL recovery: Invalid timestamps count {} for '{}'", timestampsCount, seriesKey);
        throw std::runtime_error("Invalid timestamps count in WAL entry");
    }
    if (encodedTimestampsSize > MAX_ENCODED_SIZE) {
        timestar::wal_log.error("WAL recovery: Invalid encoded timestamps size {} for '{}'", encodedTimestampsSize,
                                seriesKey);
        throw std::runtime_error("Invalid encoded timestamps size in WAL entry");
    }

    auto timestampsSlice = walSlice.getSlice(encodedTimestampsSize);
    auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampsCount, insertReq.timestamps);

    uint32_t valueByteSize = walSlice.read<uint32_t>();
    if (valueByteSize > MAX_ENCODED_SIZE) {
        timestar::wal_log.error("WAL recovery: Invalid value size {} for '{}'", valueByteSize, seriesKey);
        throw std::runtime_error("Invalid value byte size in WAL entry");
    }

    if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = walSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
    } else if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = walSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, insertReq.values);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = walSlice.getSlice(valueByteSize);
        StringEncoder::decode(valuesSlice, timestampsCount, nSkipped, nTimestamps, insertReq.values);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = walSlice.getSlice(valueByteSize);
        std::vector<uint64_t> rawUint;
        IntegerEncoder::decode(valuesSlice, timestampsCount, rawUint);
        // ZigZag decode uint64 → int64, skipping values that correspond to filtered timestamps
        insertReq.values.reserve(nTimestamps);
        for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < rawUint.size(); ++i) {
            insertReq.values.push_back(ZigZag::zigzagDecode(rawUint[i]));
        }
    } else {
        static_assert(sizeof(T) == 0, "Unsupported WAL value type");
    }

    return insertReq;
}

// Explicit instantiations
template seastar::future<WALInsertResult> WAL::insert<double>(TimeStarInsert<double>& insertRequest);
template seastar::future<WALInsertResult> WAL::insert<bool>(TimeStarInsert<bool>& insertRequest);
template seastar::future<WALInsertResult> WAL::insert<std::string>(TimeStarInsert<std::string>& insertRequest);
template seastar::future<WALInsertResult> WAL::insert<int64_t>(TimeStarInsert<int64_t>& insertRequest);
template size_t WAL::estimateInsertSize<double>(TimeStarInsert<double>& insertRequest);
template size_t WAL::estimateInsertSize<bool>(TimeStarInsert<bool>& insertRequest);
template size_t WAL::estimateInsertSize<std::string>(TimeStarInsert<std::string>& insertRequest);
template size_t WAL::estimateInsertSize<int64_t>(TimeStarInsert<int64_t>& insertRequest);
template seastar::future<WALInsertResult> WAL::insertBatch<double>(std::vector<TimeStarInsert<double>>& insertRequests);
template seastar::future<WALInsertResult> WAL::insertBatch<bool>(std::vector<TimeStarInsert<bool>>& insertRequests);
template seastar::future<WALInsertResult> WAL::insertBatch<std::string>(
    std::vector<TimeStarInsert<std::string>>& insertRequests);
template seastar::future<WALInsertResult> WAL::insertBatch<int64_t>(
    std::vector<TimeStarInsert<int64_t>>& insertRequests);