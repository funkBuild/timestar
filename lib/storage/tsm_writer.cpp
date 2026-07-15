#include "tsm_writer.hpp"

#include "bool_encoder_rle.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "logger.hpp"
#include "logging_config.hpp"
#include "series_id.hpp"
#include "string_encoder.hpp"
#include "tsm.hpp"
#include "../query/simd_aggregator.hpp"
#include "value_type_dispatch.hpp"

#include <cassert>
#include "zigzag.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/thread.hh>
#include <stdexcept>
#include <system_error>
#include <variant>

TSMWriter::TSMWriter(std::string _filename) {
    filename = std::move(_filename);
    writeHeader();
}

void TSMWriter::writeHeader() {
    std::string magic("TASM");
    buffer.write(magic);
    buffer.write(TSM_VERSION);  // Version 2: universal block stats for all types
}

template <class T>
void TSMWriter::writeSeries(TSMValueType seriesType, const SeriesId128& seriesId,
                            const std::vector<uint64_t>& timestamps, const std::vector<T>& values) {
    if (timestamps.size() != values.size()) {
        throw std::invalid_argument("TSMWriter::writeSeries: timestamps (" + std::to_string(timestamps.size()) +
                                    ") and values (" + std::to_string(values.size()) + ") size mismatch");
    }
    // serializes a single series into one or more blocks. After each block, append an index entry.
    // Block size is config-driven via storage.max_points_per_block (default 1000).
    TSMIndexEntry indexEntry;
    indexEntry.seriesId = seriesId;
    indexEntry.seriesType = seriesType;

    // Phase 3: For String series, try building a dictionary from all values.
    // If valid, blocks will use dictionary encoding (varint IDs instead of raw strings).
    StringEncoder::Dictionary stringDict;
    if constexpr (std::is_same_v<T, std::string>) {
        stringDict = StringEncoder::buildDictionary(values);
        if (stringDict.valid) {
            indexEntry.stringDictionary =
                std::make_shared<const std::vector<std::string>>(std::move(stringDict.entries));
        }
    }

    size_t offset = 0;
    size_t blockCount = 0;

    while (offset < timestamps.size()) {
        const size_t end = std::min(timestamps.size(), (size_t)(offset + MaxPointsPerBlock()));
        size_t blockSize = end - offset;

        if (blockCount == 0) {
            LOG_INSERT_PATH(timestar::tsm_log, debug,
                            "Creating blocks for series '{}' ({} total points, up to {} per block)", seriesId.toHex(),
                            timestamps.size(), MaxPointsPerBlock());
        }

        LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing block {} ({} points)", blockCount, blockSize);
        blockCount++;

        // Zero-copy: pass span sub-ranges directly to writeBlock, avoiding
        // two vector copies per block (timestamps + values).
        // vector<bool> is a bitset without .data(), so bool inlines its encoding.
        if constexpr (std::is_same_v<T, bool>) {
            // Bool path: vector<bool> can't create span, so encode inline
            std::span<const uint64_t> tsSpan(timestamps.data() + offset, blockSize);
            std::vector<bool> blockValues(values.begin() + offset, values.begin() + end);
            size_t blockStartOffset = buffer.size();
            buffer.write((uint8_t)seriesType);
            buffer.write((uint32_t)blockSize);
            // Encode timestamps directly into the output buffer (see writeBlock).
            const size_t tsLenOffset = buffer.size();
            buffer.write((uint32_t)0);  // placeholder: compressed timestamp byte length
            const size_t tsBytes = IntegerEncoder::encodeInto(tsSpan, buffer);
            buffer.writeAt<uint32_t>(tsLenOffset, (uint32_t)tsBytes);
            BoolEncoderRLE::encodeInto(blockValues, buffer);

            writeIndexBlock(tsSpan, values, offset, blockSize, indexEntry, blockStartOffset);
        } else {
            std::span<const uint64_t> tsSpan(timestamps.data() + offset, blockSize);
            std::span<const T> valSpan(values.data() + offset, blockSize);
            writeBlock(seriesType, seriesId, tsSpan, valSpan, indexEntry);
        }

        offset += MaxPointsPerBlock();
    }

    // Phase 4A: Insert into map (keeps sorted automatically)
    indexEntries[seriesId] = std::move(indexEntry);
}

template <class T>
void TSMWriter::writeBlock(TSMValueType seriesType, const SeriesId128& seriesId, std::span<const uint64_t> timestamps,
                           std::span<const T> values, TSMIndexEntry& indexEntry) {
    size_t blockStartOffset = buffer.size();

    buffer.write((uint8_t)seriesType);          // uint8_t fieldType
    buffer.write((uint32_t)timestamps.size());  // uint32_t timestamp entries count
    // Encode timestamps straight into the output buffer instead of allocating a
    // fresh 4KB-page-aligned AlignedBuffer and copying it in a second time. Reserve a
    // 4-byte length slot, encode in place, then back-patch the byte length. encode()
    // and encodeInto() share encodeImpl(), so the on-disk bytes are identical.
    const size_t tsLenOffset = buffer.size();
    buffer.write((uint32_t)0);  // placeholder: compressed timestamp partition length in bytes
    const size_t tsBytes = IntegerEncoder::encodeInto(timestamps, buffer);
    buffer.writeAt<uint32_t>(tsLenOffset, (uint32_t)tsBytes);

    if constexpr (std::is_same_v<T, double>) {
        // Encode directly into the output buffer. The previous code routed through
        // FloatEncoder::encode() -> CompressedBuffer, which itself calls encodeInto
        // into a temporary AlignedBuffer and then copies word-by-word into the
        // CompressedBuffer, which buffer.write() then copies a third time. encodeInto
        // writes once. We then zero-pad to an 8-byte boundary so the on-disk value
        // region stays byte-identical to the old CompressedBuffer (vector<uint64_t>)
        // layout — preserving format compatibility and the zero-copy CompressedSlice
        // word-read invariant on decode.
        size_t beforeSize = buffer.size();
        FloatEncoder::encodeInto(values, buffer);
        size_t pad = (8 - ((buffer.size() - beforeSize) % 8)) % 8;
        for (size_t i = 0; i < pad; ++i)
            buffer.write(static_cast<uint8_t>(0));
    } else if constexpr (std::is_same_v<T, std::string>) {
        // Phase 3: Use dictionary encoding if dictionary is available on the index entry.
        // encodeInto/encodeDictionaryInto write header + compressed bytes straight into
        // the output buffer — no intermediate AlignedBuffer + copy per block. The bytes
        // are identical to the old encode()+write() path.
        if (indexEntry.stringDictionary && !indexEntry.stringDictionary->empty()) {
            StringEncoder::Dictionary dict;
            dict.entries = *indexEntry.stringDictionary;
            dict.valid = true;
            StringEncoder::encodeDictionaryInto(values, dict, buffer, compressionLevel_);
        } else {
            StringEncoder::encodeInto(values, buffer, compressionLevel_);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        // Use thread-local scratch buffer to avoid per-block heap allocation
        static thread_local std::vector<uint64_t> zigzagScratch;
        zigzagScratch.resize(values.size());
        ZigZag::zigzagEncodeInto(values, zigzagScratch.data());
        // Encode straight into the output buffer (no temp AlignedBuffer + second copy).
        // The int64 value region carries no length prefix — its extent is the block
        // size minus the header+timestamp bytes — so encodeInto's bytes are identical
        // to the previous encode()+write().
        IntegerEncoder::encodeInto(zigzagScratch, buffer);
    } else {
        // bool never reaches writeBlock: writeSeries inlines bool encoding because
        // vector<bool> cannot form a std::span — this static_assert guards it.
        static_assert(sizeof(T) == 0, "Unsupported TSM value type");
    }

    // Compute block-level stats per type
    if constexpr (std::is_same_v<T, double>) {
        writeIndexBlock(timestamps, values, indexEntry, blockStartOffset);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        writeIndexBlock(timestamps, values, indexEntry, blockStartOffset);
    } else {
        // String: base index block only (blockCount set in writeIndex serialization via separate overload)
        writeIndexBlock(timestamps, indexEntry, blockStartOffset);
    }
}

void TSMWriter::writeIndexBlock(std::span<const uint64_t> timestamps, TSMIndexEntry& indexEntry,
                                size_t blockStartOffset) {
    if (timestamps.empty()) {
        return;  // No data to index
    }
    // Timestamps are sorted on every writeSeries path (writeAllSeries sorts;
    // the compactor merges sorted runs), so min/max are the endpoints — no
    // O(N) scan needed. minmax_element's first-min/last-max convention matches
    // front/back exactly, including duplicate timestamps.
    assert(std::is_sorted(timestamps.begin(), timestamps.end()));
    const uint64_t blockMinTime = timestamps.front();
    const uint64_t blockMaxTime = timestamps.back();
    size_t blockSize = buffer.size() - blockStartOffset;

    if (blockSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("TSM block size " + std::to_string(blockSize) + " exceeds uint32_t maximum (" +
                                  std::to_string(std::numeric_limits<uint32_t>::max()) +
                                  "); block would be truncated in the index");
    }

    TSMIndexBlock indexBlock;
    indexBlock.minTime = blockMinTime;
    indexBlock.maxTime = blockMaxTime;
    indexBlock.offset = blockStartOffset;
    indexBlock.size = static_cast<uint32_t>(blockSize);
    // V2: set blockCount for all types (enables COUNT pushdown for String)
    indexBlock.blockCount = static_cast<uint32_t>(timestamps.size());

    indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

void TSMWriter::writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const double> values,
                                TSMIndexEntry& indexEntry, size_t blockStartOffset) {
    if (timestamps.empty()) {
        return;
    }
    // Timestamps are sorted on every writeSeries path (writeAllSeries sorts;
    // the compactor merges sorted runs), so min/max are the endpoints — no
    // O(N) scan needed. minmax_element's first-min/last-max convention matches
    // front/back exactly, including duplicate timestamps.
    assert(std::is_sorted(timestamps.begin(), timestamps.end()));
    const uint64_t blockMinTime = timestamps.front();
    const uint64_t blockMaxTime = timestamps.back();
    size_t blockSize = buffer.size() - blockStartOffset;

    if (blockSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("TSM block size " + std::to_string(blockSize) + " exceeds uint32_t maximum (" +
                                  std::to_string(std::numeric_limits<uint32_t>::max()) +
                                  "); block would be truncated in the index");
    }

    TSMIndexBlock indexBlock;
    indexBlock.minTime = blockMinTime;
    indexBlock.maxTime = blockMaxTime;
    indexBlock.offset = blockStartOffset;
    indexBlock.size = static_cast<uint32_t>(blockSize);

    // Compute block-level statistics for Float series.
    // Two-pass approach: first pass computes sum/min/max (vectorizable),
    // second pass computes M2 using the known mean (no per-element division).
    // NaN values are skipped for stats (sum/min/max/mean/m2) but blockCount
    // still reflects the total number of values (including NaN) since the
    // decoder needs the full count.
    const size_t n = values.size();
    double sum = 0.0;
    double bmin = std::numeric_limits<double>::max();
    double bmax = std::numeric_limits<double>::lowest();
    size_t validCount = 0;
    double m2 = 0.0;

    // Fast path: TSM block data is NaN-free by invariant (NaN filtered during
    // write), so a fused SIMD sum/min/max pass + a SIMD variance pass replace
    // the two branchy scalar loops. A NaN sum detects invariant violations and
    // falls back to the NaN-skipping scalar path below.
    timestar::simd::SimdAggregator::calculateSumMinMax(values.data(), n, sum, bmin, bmax);
    if (!std::isnan(sum)) [[likely]] {
        validCount = n;
        const double mean = sum / static_cast<double>(n);
        // calculateVariance returns population variance (M2/n)
        m2 = timestar::simd::SimdAggregator::calculateVariance(values.data(), n, mean) * static_cast<double>(n);
    } else {
        sum = 0.0;
        bmin = std::numeric_limits<double>::max();
        bmax = std::numeric_limits<double>::lowest();
        for (size_t i = 0; i < n; ++i) {
            double v = values[i];
            if (std::isnan(v))
                continue;
            ++validCount;
            sum += v;
            if (v < bmin)
                bmin = v;
            if (v > bmax)
                bmax = v;
        }
        // If all values are NaN, use sentinel values indicating no valid stats.
        if (validCount == 0) {
            sum = 0.0;
            bmin = std::numeric_limits<double>::quiet_NaN();
            bmax = std::numeric_limits<double>::quiet_NaN();
        }
        // M2 = Σ(xi - mean)² over non-NaN values.
        if (validCount > 0) {
            double mean = sum / static_cast<double>(validCount);
            for (size_t i = 0; i < n; ++i) {
                if (std::isnan(values[i]))
                    continue;
                double delta = values[i] - mean;
                m2 += delta * delta;
            }
        }
    }
    indexBlock.blockSum = sum;
    indexBlock.blockMin = bmin;
    indexBlock.blockMax = bmax;
    indexBlock.blockCount = static_cast<uint32_t>(n);
    indexBlock.blockM2 = m2;
    // firstValue/latestValue: values at min/max timestamp positions.
    // Sorted timestamps: first = index 0, latest = index n-1.
    size_t firstIdx = 0;
    size_t latestIdx = timestamps.size() - 1;
    indexBlock.blockFirstValue = values[firstIdx];
    indexBlock.blockLatestValue = values[latestIdx];
    indexBlock.hasExtendedStats = true;

    indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

// Integer-specific writeIndexBlock: compute sum/min/max/first/latest as int64, store in TSMIndexBlock
void TSMWriter::writeIndexBlock(std::span<const uint64_t> timestamps, std::span<const int64_t> values,
                                TSMIndexEntry& indexEntry, size_t blockStartOffset) {
    if (timestamps.empty())
        return;
    // Timestamps are sorted on every writeSeries path (writeAllSeries sorts;
    // the compactor merges sorted runs), so min/max are the endpoints — no
    // O(N) scan needed. minmax_element's first-min/last-max convention matches
    // front/back exactly, including duplicate timestamps.
    assert(std::is_sorted(timestamps.begin(), timestamps.end()));
    const uint64_t blockMinTime = timestamps.front();
    const uint64_t blockMaxTime = timestamps.back();
    size_t blockSize = buffer.size() - blockStartOffset;
    if (blockSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("TSM block size exceeds uint32_t maximum");
    }

    TSMIndexBlock indexBlock;
    indexBlock.minTime = blockMinTime;
    indexBlock.maxTime = blockMaxTime;
    indexBlock.offset = blockStartOffset;
    indexBlock.size = static_cast<uint32_t>(blockSize);

    const size_t n = values.size();
    double sum = 0.0;
    int64_t bmin = std::numeric_limits<int64_t>::max();
    int64_t bmax = std::numeric_limits<int64_t>::min();
    for (size_t i = 0; i < n; ++i) {
        int64_t v = values[i];
        sum += static_cast<double>(v);
        if (v < bmin)
            bmin = v;
        if (v > bmax)
            bmax = v;
    }
    indexBlock.blockSum = sum;
    indexBlock.blockMin = static_cast<double>(bmin);
    indexBlock.blockMax = static_cast<double>(bmax);
    indexBlock.blockCount = static_cast<uint32_t>(n);

    // first/latest at min/max timestamp positions (sorted: endpoints)
    size_t firstIdx = 0;
    size_t latestIdx = timestamps.size() - 1;
    indexBlock.blockFirstValue = static_cast<double>(values[firstIdx]);
    indexBlock.blockLatestValue = static_cast<double>(values[latestIdx]);
    indexBlock.hasExtendedStats = true;

    indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

// Boolean-specific writeIndexBlock: compute trueCount, first/latest values
void TSMWriter::writeIndexBlock(std::span<const uint64_t> timestamps, const std::vector<bool>& values, size_t valOffset,
                                size_t valCount, TSMIndexEntry& indexEntry, size_t blockStartOffset) {
    if (timestamps.empty())
        return;
    // Timestamps are sorted on every writeSeries path (writeAllSeries sorts;
    // the compactor merges sorted runs), so min/max are the endpoints — no
    // O(N) scan needed. minmax_element's first-min/last-max convention matches
    // front/back exactly, including duplicate timestamps.
    assert(std::is_sorted(timestamps.begin(), timestamps.end()));
    const uint64_t blockMinTime = timestamps.front();
    const uint64_t blockMaxTime = timestamps.back();
    size_t blockSize = buffer.size() - blockStartOffset;
    if (blockSize > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("TSM block size exceeds uint32_t maximum");
    }

    TSMIndexBlock indexBlock;
    indexBlock.minTime = blockMinTime;
    indexBlock.maxTime = blockMaxTime;
    indexBlock.offset = blockStartOffset;
    indexBlock.size = static_cast<uint32_t>(blockSize);
    indexBlock.blockCount = static_cast<uint32_t>(valCount);

    uint32_t trueCount = 0;
#ifdef __GLIBCXX__
    // libstdc++ fast path: popcount the packed words directly instead of
    // reading one bit proxy per value (same storage access pattern as
    // BoolEncoderRLE::encodeInto).
    {
        static_assert(sizeof(unsigned long) == 8, "word popcount assumes 64-bit unsigned long");
        auto it = values.begin() + static_cast<std::ptrdiff_t>(valOffset);
        const unsigned long* wordPtr = it._M_p;
        const unsigned int bitOff = it._M_offset;
        const size_t totalBits = valCount + bitOff;
        const size_t fullWords = totalBits / 64;
        const unsigned int tailBits = totalBits % 64;

        for (size_t w = 0; w < fullWords; ++w) {
            unsigned long word = wordPtr[w];
            if (w == 0 && bitOff != 0)
                word &= ~0UL << bitOff;
            trueCount += static_cast<uint32_t>(__builtin_popcountl(word));
        }
        if (tailBits > 0) {
            unsigned long word = wordPtr[fullWords] & ((1UL << tailBits) - 1UL);
            if (fullWords == 0 && bitOff != 0)
                word &= ~0UL << bitOff;
            trueCount += static_cast<uint32_t>(__builtin_popcountl(word));
        }
    }
#else
    for (size_t i = 0; i < valCount; ++i) {
        if (values[valOffset + i])
            trueCount++;
    }
#endif
    indexBlock.boolTrueCount = trueCount;
    indexBlock.blockSum = static_cast<double>(trueCount);
    indexBlock.blockMin = (trueCount < indexBlock.blockCount) ? 0.0 : 1.0;
    indexBlock.blockMax = (trueCount > 0) ? 1.0 : 0.0;

    // first/latest at min/max timestamp positions (sorted: endpoints)
    size_t firstIdx = 0;
    size_t latestIdx = timestamps.size() - 1;
    indexBlock.boolFirstValue = values[valOffset + firstIdx];
    indexBlock.boolLatestValue = values[valOffset + latestIdx];
    indexBlock.blockFirstValue = indexBlock.boolFirstValue ? 1.0 : 0.0;
    indexBlock.blockLatestValue = indexBlock.boolLatestValue ? 1.0 : 0.0;
    indexBlock.hasExtendedStats = true;

    indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

void TSMWriter::writeCompressedBlockWithStats(TSMValueType seriesType, const SeriesId128& seriesId,
                                              seastar::temporary_buffer<uint8_t>&& compressedData,
                                              const TSMIndexBlock& srcBlock) {
    size_t blockStartOffset = buffer.size();
    buffer.write_bytes(reinterpret_cast<const char*>(compressedData.get()), compressedData.size());

    auto& indexEntry = indexEntries[seriesId];
    if (indexEntry.seriesId != seriesId) {
        indexEntry.seriesId = seriesId;
        indexEntry.seriesType = seriesType;
    }

    if (compressedData.size() > std::numeric_limits<uint32_t>::max()) {
        throw std::overflow_error("TSM compressed block size exceeds uint32_t maximum");
    }

    TSMIndexBlock indexBlock;
    indexBlock.minTime = srcBlock.minTime;
    indexBlock.maxTime = srcBlock.maxTime;
    indexBlock.offset = blockStartOffset;
    indexBlock.size = static_cast<uint32_t>(compressedData.size());
    // Carry forward block stats from source file (all types)
    indexBlock.blockSum = srcBlock.blockSum;
    indexBlock.blockMin = srcBlock.blockMin;
    indexBlock.blockMax = srcBlock.blockMax;
    indexBlock.blockCount = srcBlock.blockCount;
    indexBlock.blockM2 = srcBlock.blockM2;
    indexBlock.blockFirstValue = srcBlock.blockFirstValue;
    indexBlock.blockLatestValue = srcBlock.blockLatestValue;
    indexBlock.hasExtendedStats = srcBlock.hasExtendedStats;
    // Boolean-specific
    indexBlock.boolTrueCount = srcBlock.boolTrueCount;
    indexBlock.boolFirstValue = srcBlock.boolFirstValue;
    indexBlock.boolLatestValue = srcBlock.boolLatestValue;

    indexEntry.indexBlocks.push_back(std::move(indexBlock));
}

void TSMWriter::writeIndex() {
    // std::map maintains sorted order automatically
    size_t indexStartOffset = buffer.size();
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Index starts at offset: {} ({:#x}), writing {} index entries",
                    indexStartOffset, indexStartOffset, indexEntries.size());

    // Iterate directly - already sorted by SeriesId128
    for (auto const& [seriesId, indexEntry] : indexEntries) {
        // Write SeriesId128 as 16 bytes (no length prefix needed since it's fixed size).
        // write_bytes straight from the raw 16-byte array avoids the per-series
        // std::string that toBytes() would allocate.
        const auto& seriesIdRaw = indexEntry.seriesId.getRawData();
        buffer.write_bytes(reinterpret_cast<const char*>(seriesIdRaw.data()), seriesIdRaw.size());

        // Block type
        buffer.write((uint8_t)indexEntry.seriesType);  // uint8_t fieldType

        // num of index entries
        if (indexEntry.indexBlocks.size() > std::numeric_limits<uint16_t>::max()) {
            throw std::runtime_error("Too many blocks for series in TSM file: " +
                                     std::to_string(indexEntry.indexBlocks.size()));
        }
        buffer.write(static_cast<uint16_t>(indexEntry.indexBlocks.size()));

        // for each block — per-type stats (V2 format)
        for (auto const& block : indexEntry.indexBlocks) {
            buffer.write(block.minTime);  // minTime
            buffer.write(block.maxTime);  // maxTime
            buffer.write(block.offset);   // byte offset from start of file
            buffer.write(block.size);     // block size
            if (indexEntry.seriesType == TSMValueType::Float) {
                // Float: 80 bytes (28 base + 52 stats)
                buffer.write(block.blockSum);
                buffer.write(block.blockMin);
                buffer.write(block.blockMax);
                buffer.write(block.blockCount);
                buffer.write(block.blockM2);
                buffer.write(block.blockFirstValue);
                buffer.write(block.blockLatestValue);
            } else if (indexEntry.seriesType == TSMValueType::Integer) {
                // Integer: 72 bytes (28 base + count(4) + sum/min/max/first/latest as int64)
                // Clamp double→int64 to avoid undefined behavior when values exceed INT64 range
                // (blockSum of large int64 values can exceed 2^63, and values >= 2^53 lose precision)
                auto safeToInt64 = [](double v) -> int64_t {
                    constexpr double maxSafe = static_cast<double>(std::numeric_limits<int64_t>::max());
                    constexpr double minSafe = static_cast<double>(std::numeric_limits<int64_t>::min());
                    if (v >= maxSafe)
                        return std::numeric_limits<int64_t>::max();
                    if (v <= minSafe)
                        return std::numeric_limits<int64_t>::min();
                    return static_cast<int64_t>(v);
                };
                buffer.write(block.blockCount);
                buffer.write(safeToInt64(block.blockSum));
                buffer.write(safeToInt64(block.blockMin));
                buffer.write(safeToInt64(block.blockMax));
                buffer.write(safeToInt64(block.blockFirstValue));
                buffer.write(safeToInt64(block.blockLatestValue));
            } else if (indexEntry.seriesType == TSMValueType::Boolean) {
                // Boolean: 40 bytes (28 base + count(4) + trueCount(4) + first(1) + latest(1) + pad(2))
                buffer.write(block.blockCount);
                buffer.write(block.boolTrueCount);
                buffer.write(static_cast<uint8_t>(block.boolFirstValue ? 1 : 0));
                buffer.write(static_cast<uint8_t>(block.boolLatestValue ? 1 : 0));
                buffer.write(static_cast<uint16_t>(0));  // padding
            } else if (indexEntry.seriesType == TSMValueType::String) {
                // String: 32 bytes (28 base + count(4))
                buffer.write(block.blockCount);
            }
        }

        // Phase 3: Write string dictionary after block metadata for String series.
        // Format: dictSize(4) + dictData(dictSize bytes)
        // dictSize == 0 means no dictionary (raw encoding used).
        if (indexEntry.seriesType == TSMValueType::String) {
            if (indexEntry.stringDictionary && !indexEntry.stringDictionary->empty()) {
                StringEncoder::Dictionary dict;
                dict.entries = *indexEntry.stringDictionary;
                dict.valid = true;
                AlignedBuffer serialized = StringEncoder::serializeDictionary(dict);
                buffer.write(static_cast<uint32_t>(serialized.size()));
                buffer.write(serialized);
            } else {
                buffer.write(static_cast<uint32_t>(0));  // no dictionary
            }
        }
    }

    buffer.write(static_cast<uint64_t>(indexStartOffset));
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Wrote index offset: {} ({:#x}), final buffer size: {}", indexStartOffset,
                    indexStartOffset, buffer.size());
}

// fsync the parent directory to ensure a newly-created file's directory
// entry is durable.  Without this, a crash after file fsync but before
// the directory is flushed can lose the file entirely on ext4/XFS.
static void fsyncParentDir(const std::string& filepath) {
    auto slash = filepath.rfind('/');
    std::string dir = (slash != std::string::npos) ? filepath.substr(0, slash) : ".";
    int dirfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dirfd < 0)
        return;  // best-effort
    ::fsync(dirfd);
    ::close(dirfd);
}

void TSMWriter::close() {
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Writing file: {}, buffer size: {} ({:#x}), capacity: {}", filename,
                    buffer.size(), buffer.size(), buffer.capacity());

    // Use raw POSIX I/O instead of std::ofstream to avoid the C++ stdio
    // buffering layer (which chunks large writes into 8KB pieces internally).
    // We write the entire buffer in one shot from a contiguous allocation,
    // so a single write() syscall is all that's needed.
    //
    // O_WRONLY | O_CREAT | O_TRUNC: create or truncate, write-only.
    // Mode 0644: owner rw, group/other r.
    int fd = ::open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::system_error(errno, std::system_category(), "TSMWriter::close: failed to open " + filename);
    }

    // Hint to the kernel that we will write this file sequentially.
    // This allows read-ahead to be disabled and improves write coalescing.
    // POSIX_FADV_SEQUENTIAL is a no-op on failure, so ignore errors.
    ::posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);

    // Write the entire buffer contents in a single large write().
    // For files > 2GB we loop to handle partial writes, but in practice
    // TSM files are well under this limit.
    const char* ptr = reinterpret_cast<const char*>(buffer.data.data());
    size_t remaining = buffer.size();
    while (remaining > 0) {
        ssize_t written = ::write(fd, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR)
                continue;
            int err = errno;
            ::close(fd);
            throw std::system_error(err, std::system_category(), "TSMWriter::close: write failed for " + filename);
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }

    // fsync on the same fd to ensure durability before returning.
    // Reusing the write fd avoids an extra open() syscall.
    int fsync_ret;
    do {
        fsync_ret = ::fsync(fd);
    } while (fsync_ret < 0 && errno == EINTR);
    if (fsync_ret < 0) {
        int err = errno;
        ::close(fd);
        throw std::system_error(err, std::system_category(), "TSMWriter::close: fsync failed for " + filename);
    }

    if (::close(fd) < 0) {
        timestar::tsm_log.warn("TSMWriter::close: close() failed for {}: {} (errno={})", filename, std::strerror(errno),
                               errno);
    }

    // Ensure the directory entry for this new file is durable.
    fsyncParentDir(filename);

    LOG_INSERT_PATH(timestar::tsm_log, debug, "File written successfully: {}", filename);
}

seastar::future<> TSMWriter::closeDMA() {
    LOG_INSERT_PATH(timestar::tsm_log, debug, "DMA writing file: {}, buffer size: {} ({:#x}), capacity: {}", filename,
                    buffer.size(), buffer.size(), buffer.capacity());

    const size_t dataSize = buffer.size();

    // Open the file for DMA writes (create or truncate, write-only)
    std::string_view filenameView{filename};
    auto file = co_await seastar::open_file_dma(
        filenameView, seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate);

    // Ensure we close the file on any exit path (success or exception).
    // GCC 14 does not support co_await in catch blocks, so we use a
    // scope guard pattern: capture any exception, close outside the
    // handler, then rethrow.
    std::exception_ptr writeError;
    try {
        const size_t dmaAlign = file.disk_write_dma_alignment();

        // Pad the data size up to DMA alignment boundary.
        // The file will contain extra zero bytes at the end, but TSM readers
        // use the index offset (last 8 bytes of the *logical* data) to find
        // the index, so they never read past the logical end.  After the
        // write we truncate the file to the exact logical size.
        const size_t paddedSize = (dataSize + dmaAlign - 1) & ~(dmaAlign - 1);

        // The AlignedBuffer's underlying vector uses dma_default_init_allocator
        // which guarantees the base address is aligned to DMA_ALIGNMENT (4096).
        // This satisfies Seastar's memory_dma_alignment requirement, so we can
        // write directly from buffer.data without an intermediate memcpy.
        //
        // We only need to ensure the padding region (beyond dataSize, up to
        // paddedSize) is accessible and zero-filled.  Resize the vector to
        // the padded size -- the allocator default-initializes, so we must
        // explicitly zero the padding bytes for deterministic content.
        const size_t prevCapacity = buffer.data.size();
        if (paddedSize > prevCapacity) {
            buffer.data.resize(paddedSize);
        }
        if (paddedSize > dataSize) {
            std::memset(buffer.data.data() + dataSize, 0, paddedSize - dataSize);
        }

        const char* writePtr = reinterpret_cast<const char*>(buffer.data.data());

        // Write the entire buffer in a single DMA write.  Seastar's
        // dma_write may return a short write count, so loop until all
        // bytes are written.  In practice, for aligned writes up to
        // disk_write_max_length (~1GB), this completes in one call.
        size_t written = 0;
        while (written < paddedSize) {
            size_t n = co_await file.dma_write(written, writePtr + written, paddedSize - written);
            if (n == 0) {
                throw std::runtime_error("TSMWriter::closeDMA: dma_write returned 0 for " + filename);
            }
            written += n;
        }

        // Truncate to exact logical size to remove DMA padding bytes.
        // This ensures the on-disk file is byte-identical to what close() produces.
        if (paddedSize != dataSize) {
            co_await file.truncate(dataSize);
        }

        // Flush for durability (equivalent to fsync)
        co_await file.flush();

    } catch (...) {
        writeError = std::current_exception();
    }

    // Always close the file handle
    co_await file.close();

    if (writeError) {
        std::rethrow_exception(writeError);
    }

    // Ensure the directory entry for this new file is durable.
    // Use Seastar's async file API instead of blocking ::open()+::fsync() on the reactor.
    {
        auto slash = filename.rfind('/');
        std::string dir = (slash != std::string::npos) ? filename.substr(0, slash) : ".";
        try {
            auto dirFile = co_await seastar::open_directory(dir);
            co_await dirFile.flush();
            co_await dirFile.close();
        } catch (...) {
            // best-effort directory sync — non-fatal
        }
    }

    LOG_INSERT_PATH(timestar::tsm_log, debug, "DMA file written successfully: {}", filename);
}

void TSMWriter::writeAllSeries(TSMWriter& writer, seastar::shared_ptr<MemoryStore> store) {
    // Pre-reserve the output buffer from the store's point count. Without
    // this, an ~8MB file accretes through ~11 geometric-doubling reallocs of
    // the page-aligned buffer, each a full copy (~2x the file size memcpy'd).
    // ~9 bytes/point covers encoded timestamps+values for numeric series
    // (strings may still grow the buffer, which falls back to doubling), plus
    // per-series index overhead. Over-reservation is short-lived and bounded
    // well below the raw memstore footprint (16+ bytes/point).
    {
        size_t totalPoints = 0;
        for (auto it = store.get()->series.begin(); it != store.get()->series.end(); ++it) {
            totalPoints += std::visit([](const auto& s) { return s.timestamps.size(); }, it.value());
        }
        writer.buffer.reserve(4096 + totalPoints * 9 + store.get()->series.size() * 160);
    }

    for (auto it = store.get()->series.begin(); it != store.get()->series.end(); ++it) {
        const auto& seriesKey = it->first;
        auto& memStore = it.value();
        TSMValueType seriesType = (TSMValueType)memStore.index();

        size_t seriesPoints = std::visit([](const auto& s) { return s.timestamps.size(); }, memStore);
        LOG_INSERT_PATH(timestar::tsm_log, debug, "Processing series '{}' with {} points, type={}", seriesKey.toHex(),
                        seriesPoints, static_cast<int>(seriesType));

        SeriesId128 seriesId = seriesKey;

        try {
            timestar::dispatchValueType(seriesType, [&]<class T>() {
                auto& series = std::get<InMemorySeries<T>>(memStore);
                series.sort();
                LOG_INSERT_PATH(timestar::tsm_log, trace, "Writing series '{}' (type {}) with {} points",
                                seriesKey.toHex(), static_cast<int>(seriesType), series.timestamps.size());
                writer.writeSeries(seriesType, seriesId, series.timestamps, series.values);
            });
        } catch (const std::bad_alloc& e) {
            timestar::tsm_log.error("BAD_ALLOC when processing series '{}' with {} points", seriesKey.toHex(),
                                    seriesPoints);
            throw;
        } catch (const std::exception& e) {
            timestar::tsm_log.error("ERROR processing series '{}': {}", seriesKey.toHex(), e.what());
            throw;
        }

        // Yield after each series to prevent reactor stalls during background
        // TSM conversion. sort() + writeSeries() with ALP encoding can take
        // 50-150ms per series, which exceeds the reactor's stall threshold.
        seastar::thread::maybe_yield();
    }
}

void TSMWriter::run(seastar::shared_ptr<MemoryStore> store, std::string filename) {
    LOG_INSERT_PATH(timestar::tsm_log, info, "Starting TSM write to file: {}, memory store has {} series", filename,
                    store.get()->series.size());

    TSMWriter writer(filename);
    writeAllSeries(writer, store);

    LOG_INSERT_PATH(timestar::tsm_log, debug, "Writing index...");
    writer.writeIndex();
    LOG_INSERT_PATH(timestar::tsm_log, debug, "Closing file...");
    writer.close();
    LOG_INSERT_PATH(timestar::tsm_log, info, "TSM write complete: {}", filename);
}

seastar::future<> TSMWriter::runAsync(seastar::shared_ptr<MemoryStore> store, std::string filename) {
    LOG_INSERT_PATH(timestar::tsm_log, info, "Starting async TSM write to file: {}, memory store has {} series",
                    filename, store.get()->series.size());

    // Run the CPU-bound sort + encode work in a Seastar thread (off the reactor)
    // to avoid multi-hundred-ms reactor stalls. seastar::async() uses a Seastar-
    // managed thread that can block without stalling the reactor. The resulting
    // TSMWriter buffer is then written to disk via async DMA I/O on the reactor.
    TSMWriter writer(filename);
    co_await seastar::async([&writer, &store] {
        writeAllSeries(writer, store);
        writer.writeIndex();
    });

    LOG_INSERT_PATH(timestar::tsm_log, debug, "Closing file via DMA...");
    co_await writer.closeDMA();
    LOG_INSERT_PATH(timestar::tsm_log, info, "Async TSM write complete: {}", filename);
}

// Template instantiations
template void TSMWriter::writeSeries<double>(TSMValueType seriesType, const SeriesId128& seriesId,
                                             const std::vector<uint64_t>& timestamps,
                                             const std::vector<double>& values);
template void TSMWriter::writeSeries<bool>(TSMValueType seriesType, const SeriesId128& seriesId,
                                           const std::vector<uint64_t>& timestamps, const std::vector<bool>& values);
template void TSMWriter::writeSeries<std::string>(TSMValueType seriesType, const SeriesId128& seriesId,
                                                  const std::vector<uint64_t>& timestamps,
                                                  const std::vector<std::string>& values);
template void TSMWriter::writeSeries<int64_t>(TSMValueType seriesType, const SeriesId128& seriesId,
                                              const std::vector<uint64_t>& timestamps,
                                              const std::vector<int64_t>& values);

