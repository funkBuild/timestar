#include "tsm.hpp"

#include "bool_encoder_rle.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "logger.hpp"
#include "slice_buffer.hpp"
#include "string_encoder.hpp"
#include "zigzag.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/seastar.hh>
#include <string_view>

using Clock = std::chrono::high_resolution_clock;

// Block header: uint8_t type + uint32_t timestampSize + uint32_t timestampBytes
static constexpr size_t BLOCK_HEADER_SIZE = sizeof(uint8_t) + 2 * sizeof(uint32_t);  // 9 bytes

// Phase 3: Thread-local string dictionary for use during block decode.
// Set by readSeriesBatched/readSingleBlock before decoding string blocks,
// consumed by decodeBlock. Avoids threading dictionary through template params.
static thread_local const std::vector<std::string>* tlStringDict = nullptr;

// Phase 0: Check whether a block's time range overlaps any tombstone range.
// tombstoneRanges must be sorted by start time (as returned by getTombstoneRanges).
// Uses binary search: O(log T) per block instead of scanning all tombstones.
static bool blockOverlapsTombstones(uint64_t blockMin, uint64_t blockMax,
                                    const std::vector<std::pair<uint64_t, uint64_t>>& tombstoneRanges) {
    if (tombstoneRanges.empty())
        return false;
    // Find first tombstone range whose start > blockMax — all earlier ranges potentially overlap.
    // Then check backwards: any range whose end >= blockMin overlaps the block.
    auto it = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                               std::make_pair(blockMax, std::numeric_limits<uint64_t>::max()));
    // Check all ranges before 'it' — the one just before could overlap
    if (it != tombstoneRanges.begin()) {
        --it;
        // Walk backwards checking for overlap: range overlaps block if range.end >= blockMin && range.start <= blockMax
        // Since ranges are sorted by start, once range.start > blockMax we're past the block.
        // We go backwards until range.end < blockMin.
        for (auto rit = it;; --rit) {
            if (rit->second >= blockMin && rit->first <= blockMax)
                return true;
            if (rit == tombstoneRanges.begin())
                break;
        }
    }
    return false;
}

// Decode a float block directly into thread-local scratch buffers and fold into
// a BlockAggregator, avoiding per-block unique_ptr<TSMBlock> heap allocation.
// Returns number of decoded points (0 on error or empty block).
static size_t decodeBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                                        timestar::BlockAggregator& aggregator) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;

    // Reuse thread-local scratch buffers to avoid per-block allocation
    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    tsScratch.clear();
    valScratch.clear();
    tsScratch.reserve(timestampSize);

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, tsScratch, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;
    if (valueByteSize > blockSize)
        return 0;

    auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
    FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, valScratch);

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

// Decode an Integer block directly into thread-local scratch buffers and fold into
// a BlockAggregator (as double), avoiding per-block unique_ptr<TSMBlock> heap allocation.
// Returns number of decoded points (0 on error or empty block).
static size_t decodeIntegerBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime,
                                               uint64_t endTime, timestar::BlockAggregator& aggregator) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;

    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    tsScratch.clear();
    valScratch.clear();
    tsScratch.reserve(timestampSize);

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, tsScratch, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;
    if (valueByteSize > blockSize)
        return 0;

    // Decode zigzag-encoded integers
    static thread_local std::vector<uint64_t> zigzagScratch;
    zigzagScratch.clear();
    zigzagScratch.reserve(nSkipped + nTimestamps);

    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    IntegerEncoder::decode(valuesSlice, nSkipped + nTimestamps, zigzagScratch);

    // Convert zigzag int64 to double for the aggregator
    valScratch.reserve(nTimestamps);
    for (size_t i = nSkipped; i < zigzagScratch.size(); ++i) {
        valScratch.push_back(static_cast<double>(ZigZag::zigzagDecode(zigzagScratch[i])));
    }

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

// Decode a Boolean block directly into thread-local scratch buffers and fold into
// a BlockAggregator (true=1.0, false=0.0).
static size_t decodeBoolBlockIntoAggregator(const uint8_t* data, uint32_t blockSize, uint64_t startTime,
                                            uint64_t endTime, timestar::BlockAggregator& aggregator) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;

    static thread_local std::vector<uint64_t> tsScratch;
    static thread_local std::vector<double> valScratch;
    tsScratch.clear();
    valScratch.clear();
    tsScratch.reserve(timestampSize);

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, tsScratch, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;
    if (valueByteSize > blockSize)
        return 0;

    // Decode RLE-encoded booleans
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    std::vector<bool> boolValues;
    BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, boolValues);

    // Convert bools to double (1.0/0.0)
    valScratch.reserve(nTimestamps);
    for (size_t i = 0; i < boolValues.size(); ++i) {
        valScratch.push_back(boolValues[i] ? 1.0 : 0.0);
    }

    aggregator.addPoints(tsScratch, valScratch);
    return tsScratch.size();
}

// COUNT-only block decode: decode timestamps only, skip value decompression.
// Folds timestamp counts into the BlockAggregator's per-bucket counters.
static size_t decodeBlockCountOnly(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                                   timestar::BlockAggregator& aggregator) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;

    static thread_local std::vector<uint64_t> tsScratch;
    tsScratch.clear();
    tsScratch.reserve(timestampSize);

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, tsScratch, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    // Skip value decode entirely — only count timestamps per bucket
    aggregator.addTimestampsOnly(tsScratch);
    return tsScratch.size();
}

TSM::TSM(std::string _absoluteFilePath) {
    size_t filenameEndIndex = _absoluteFilePath.find_last_of(".");
    if (filenameEndIndex == std::string::npos)
        throw std::runtime_error("TSM invalid filename (no extension): " + _absoluteFilePath);
    size_t filenameStartIndex = _absoluteFilePath.find_last_of("/") + 1;

    std::string filename = _absoluteFilePath.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex);

    size_t underscoreIndex = filename.find_last_of("_");
    if (underscoreIndex == std::string::npos)
        throw std::runtime_error("TSM invalid filename:" + filename);

    try {
        tierNum = std::stoull(filename.substr(0, underscoreIndex));
        seqNum = std::stoull(filename.substr(underscoreIndex + 1));

        timestar::tsm_log.debug("tierNum={} seqNum={}", tierNum, seqNum);
    } catch (const std::exception&) {
        throw std::runtime_error("TSM invalid filename:" + filename);
    }

    filePath = _absoluteFilePath;
}

seastar::future<> TSM::open() {
    std::string_view filePathView{filePath};
    tsmFile = co_await seastar::open_file_dma(filePathView, seastar::open_flags::ro);

    if (!tsmFile) {
        timestar::tsm_log.error("TSM unable to open: {}", filePath);
        throw std::runtime_error("TSM unable to open:" + filePath);
    }

    // Clean up file handle on any failure after successful open.
    // GCC 14 does not support co_await in catch blocks, so we capture
    // the exception, close outside the handler, then rethrow.
    std::exception_ptr openError;
    try {
        length = co_await tsmFile.size();

        // Read and validate file header (magic "TASM" + 1-byte version)
        if (length >= 5) {
            auto hdrBuf = co_await tsmFile.dma_read_exactly<uint8_t>(0, 5);
            // Validate magic bytes "TASM"
            if (hdrBuf.get()[0] != 'T' || hdrBuf.get()[1] != 'A' ||
                hdrBuf.get()[2] != 'S' || hdrBuf.get()[3] != 'M') {
                throw std::runtime_error("Not a TSM file (bad magic): " + filePath);
            }
            fileVersion = hdrBuf.get()[4];
            if (fileVersion < TSM_VERSION_MIN || fileVersion > TSM_VERSION) {
                throw std::runtime_error("Unsupported TSM file version " + std::to_string(fileVersion) +
                                         " (supported: " + std::to_string(TSM_VERSION_MIN) + "-" +
                                         std::to_string(TSM_VERSION) + "): " + filePath);
            }
        }

        // Use lazy loading: read sparse index + bloom filter (not full index).
        // Minimum valid file: 5-byte header + 8-byte index offset footer = 13 bytes.
        if (length < 13) {
            throw std::runtime_error("TSM file too small (" + std::to_string(length) +
                                     " bytes, minimum 13): " + filePath);
        }
        co_await readSparseIndex();

        // Load tombstones if they exist
        co_await loadTombstones();
    } catch (...) {
        openError = std::current_exception();
    }

    if (openError) {
        // Close the leaked file handle before rethrowing
        try {
            co_await tsmFile.close();
        } catch (...) {
            // Ignore close errors during cleanup to avoid masking the original exception
        }
        std::rethrow_exception(openError);
    }
};

seastar::future<> TSM::close() {
    if (tsmFile) {
        co_await tsmFile.close();
    }
    co_return;
}

uint64_t TSM::rankAsInteger() {
    if (tierNum >= 16) {
        throw std::overflow_error("TSM::rankAsInteger: tierNum " + std::to_string(tierNum) +
                                  " >= 16 would overflow in (tierNum << 60)");
    }
    constexpr uint64_t maxSeqNum = (uint64_t{1} << 60) - 1;
    uint64_t safeSeqNum = seqNum;
    if (seqNum > maxSeqNum) {
        timestar::tsm_log.warn(
            "TSM::rankAsInteger: seqNum {} exceeds 60-bit limit, masking to prevent tier bit corruption", seqNum);
        safeSeqNum = seqNum & maxSeqNum;
    }
    return (tierNum << 60) | safeSeqNum;
}

// Lazy loading: read sparse index + build bloom filter
seastar::future<> TSM::readSparseIndex() {
    // Read index offset (last 8 bytes of file)
    auto indexOffsetBuf = co_await tsmFile.dma_read_exactly<uint8_t>(length - sizeof(uint64_t), sizeof(uint64_t));
    uint64_t indexOffset;
    std::memcpy(&indexOffset, indexOffsetBuf.get(), sizeof(uint64_t));

    // Validate indexOffset is within file bounds
    if (indexOffset >= length - sizeof(uint64_t)) {
        throw std::runtime_error("Corrupted TSM file: indexOffset " + std::to_string(indexOffset) +
                                 " is out of bounds (file size: " + std::to_string(length) + "): " + filePath);
    }

    // Read entire index section
    auto indexBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexOffset, length - indexOffset - sizeof(uint64_t));
    Slice indexSlice(indexBuf.get(), indexBuf.size());

    // First pass: Parse index to collect series and build sparse index
    // We need the actual count before initializing the bloom filter
    std::vector<SeriesId128> seriesIds;

    while (indexSlice.offset < indexSlice.length_) {
        uint64_t entryStartOffset = indexOffset + indexSlice.offset;

        // Read series ID (16 bytes) — zero-copy from index buffer
        SeriesId128 seriesId =
            SeriesId128::fromBytes(reinterpret_cast<const char*>(indexSlice.data + indexSlice.offset), 16);
        indexSlice.offset += 16;

        // Read type (1 byte) and block count (2 bytes)
        uint8_t type = indexSlice.read<uint8_t>();
        uint16_t blockCount = indexSlice.read<uint16_t>();

        // Block size depends on type and file version
        auto seriesType = static_cast<TSMValueType>(type);
        size_t perBlockBytes = indexBlockBytes(seriesType, fileVersion);

        // Validate blockCount against remaining index data to prevent
        // reads past the end of the index on malformed files.
        size_t blockBytes = static_cast<size_t>(blockCount) * perBlockBytes;
        if (blockBytes > indexSlice.bytesLeft()) {
            throw std::runtime_error("TSM index corrupt: blockCount " + std::to_string(blockCount) + " requires " +
                                     std::to_string(blockBytes) + " bytes but only " +
                                     std::to_string(indexSlice.bytesLeft()) + " remain");
        }

        // Peek at first/last block metadata from the index for sparse lookups.
        uint64_t seriesMinTime = 0;
        uint64_t seriesMaxTime = 0;
        double firstValue = 0.0;
        double latestValue = 0.0;
        bool hasExtStats = false;
        bool boolFirst = false;
        bool boolLatest = false;

        if (blockCount > 0) {
            size_t blockStart = indexSlice.offset;
            size_t lastBlockStart = blockStart + (blockCount - 1) * perBlockBytes;

            // First block: minTime at offset 0
            std::memcpy(&seriesMinTime, indexSlice.data + blockStart, sizeof(uint64_t));
            // Last block: maxTime at offset 8
            std::memcpy(&seriesMaxTime, indexSlice.data + lastBlockStart + 8, sizeof(uint64_t));

            // Float: blockFirstValue at offset 64, blockLatestValue at offset 72
            if (seriesType == TSMValueType::Float) {
                std::memcpy(&firstValue, indexSlice.data + blockStart + 64, sizeof(double));
                std::memcpy(&latestValue, indexSlice.data + lastBlockStart + 72, sizeof(double));
                hasExtStats = true;
            }
            // Integer (V2): first/latest as int64 at offset 56 and 64 within block
            // Layout: minTime(8) maxTime(8) offset(8) size(4) count(4) sum(8) min(8) max(8) first(8) latest(8)
            //         0         8          16        24      28       32      40      48      56       64
            else if (seriesType == TSMValueType::Integer && fileVersion >= 2) {
                int64_t intFirst, intLatest;
                std::memcpy(&intFirst, indexSlice.data + blockStart + 56, sizeof(int64_t));
                std::memcpy(&intLatest, indexSlice.data + lastBlockStart + 64, sizeof(int64_t));
                firstValue = static_cast<double>(intFirst);
                latestValue = static_cast<double>(intLatest);
                hasExtStats = true;
            }
            // Boolean (V2): first/latest as uint8 at offset 36 and 37
            else if (seriesType == TSMValueType::Boolean && fileVersion >= 2) {
                boolFirst = (indexSlice.data[blockStart + 36] != 0);
                boolLatest = (indexSlice.data[lastBlockStart + 37] != 0);
                firstValue = boolFirst ? 1.0 : 0.0;
                latestValue = boolLatest ? 1.0 : 0.0;
                hasExtStats = true;
            }
        }

        // Skip over the blocks (don't parse them yet)
        indexSlice.offset += blockBytes;

        // Phase 3: Skip over string dictionary if present (V2 String series)
        uint32_t dictBytes = 0;
        if (seriesType == TSMValueType::String && fileVersion >= 2) {
            if (indexSlice.offset + 4 <= indexSlice.length_) {
                std::memcpy(&dictBytes, indexSlice.data + indexSlice.offset, 4);
                indexSlice.offset += 4 + dictBytes;
            }
        }

        // Calculate total entry size (header + blocks + optional dictionary)
        uint32_t entrySize = 16 + 1 + 2 + static_cast<uint32_t>(blockBytes);
        if (seriesType == TSMValueType::String && fileVersion >= 2) {
            entrySize += 4 + dictBytes;
        }

        // Store sparse entry with time bounds + first/latest values
        SparseIndexEntry sparseEntry{.seriesId = seriesId,
                                     .fileOffset = entryStartOffset,
                                     .entrySize = entrySize,
                                     .seriesType = seriesType,
                                     .minTime = seriesMinTime,
                                     .maxTime = seriesMaxTime,
                                     .firstValue = firstValue,
                                     .latestValue = latestValue,
                                     .hasExtendedStats = hasExtStats,
                                     .boolFirstValue = boolFirst,
                                     .boolLatestValue = boolLatest};
        sparseIndex.insert({seriesId, sparseEntry});

        // Collect series ID for bloom filter
        seriesIds.push_back(seriesId);
    }

    // Now initialize bloom filter with the ACTUAL series count
    // This is critical for higher-tier TSM files that may have 100K+ series
    size_t actualSeriesCount = seriesIds.size();

    bloom_parameters params;
    params.projected_element_count = std::max(actualSeriesCount, size_t{1});
    params.false_positive_probability = bloomFpr();
    params.compute_optimal_parameters();
    seriesBloomFilter = bloom_filter(params);

    // Add all series to the properly-sized bloom filter
    for (const auto& seriesId : seriesIds) {
        seriesBloomFilter.insert(seriesId.getRawData());
    }

    timestar::tsm_log.info("Loaded sparse index for {} (tier {}): {} series, bloom filter: {} bytes", filePath, tierNum,
                           sparseIndex.size(), seriesBloomFilter.size());
}

// Lazy load full index entry for a series (single DMA read)
seastar::future<TSMIndexEntry*> TSM::getFullIndexEntry(const SeriesId128& seriesId) {
    // Step 1: Bloom filter check (fast, in-memory)
    if (!seriesBloomFilter.contains(seriesId.getRawData())) {
        // Definitely not in this file
        co_return nullptr;
    }

    // Step 2: Check cache (promote to front on hit)
    auto cacheIt = fullIndexCache.find(seriesId);
    if (cacheIt != fullIndexCache.end()) {
        // Move to front of LRU list (most recently used)
        lruList.splice(lruList.begin(), lruList, cacheIt->second);
        auto& entry = cacheIt->second->second;
        // Phase 3: Set thread-local dictionary for string block decoding
        if (entry.seriesType == TSMValueType::String) {
            tlStringDict = entry.stringDictionary.empty() ? nullptr : &entry.stringDictionary;
        }
        co_return &entry;
    }

    // Step 3: Sparse index lookup
    auto sparseIt = sparseIndex.find(seriesId);
    if (sparseIt == sparseIndex.end()) {
        // False positive from bloom filter (0.1% chance)
        timestar::tsm_log.trace("Bloom filter false positive for series {}", seriesId.toHex());
        co_return nullptr;
    }

    // Step 4: Single DMA read for entire series entry
    const auto& sparse = sparseIt->second;
    auto entryBuf = co_await tsmFile.dma_read_exactly<uint8_t>(sparse.fileOffset, sparse.entrySize);

    // Step 5: Parse the full entry
    Slice entrySlice(entryBuf.get(), entryBuf.size());

    TSMIndexEntry fullEntry;

    // Parse series ID (verify) — zero-copy from entry buffer
    fullEntry.seriesId = SeriesId128::fromBytes(reinterpret_cast<const char*>(entrySlice.data + entrySlice.offset), 16);
    entrySlice.offset += 16;

    // Parse type and block count
    fullEntry.seriesType = static_cast<TSMValueType>(entrySlice.read<uint8_t>());
    uint16_t blockCount = entrySlice.read<uint16_t>();

    // Parse all blocks — per-type stats depend on file version
    fullEntry.indexBlocks.reserve(blockCount);
    for (uint16_t i = 0; i < blockCount; i++) {
        TSMIndexBlock block;
        block.minTime = entrySlice.read<uint64_t>();
        block.maxTime = entrySlice.read<uint64_t>();
        block.offset = entrySlice.read<uint64_t>();
        block.size = entrySlice.read<uint32_t>();
        if (fullEntry.seriesType == TSMValueType::Float) {
            block.blockSum = entrySlice.read<double>();
            block.blockMin = entrySlice.read<double>();
            block.blockMax = entrySlice.read<double>();
            block.blockCount = entrySlice.read<uint32_t>();
            block.blockM2 = entrySlice.read<double>();
            block.blockFirstValue = entrySlice.read<double>();
            block.blockLatestValue = entrySlice.read<double>();
            block.hasExtendedStats = true;
        } else if (fileVersion >= 2) {
            // V2: all non-Float types have at least blockCount
            if (fullEntry.seriesType == TSMValueType::Integer) {
                // 72 bytes: 28 base + count(4) + sum(8) + min(8) + max(8) + first(8) + latest(8)
                block.blockCount = entrySlice.read<uint32_t>();
                int64_t intSum = entrySlice.read<int64_t>();
                int64_t intMin = entrySlice.read<int64_t>();
                int64_t intMax = entrySlice.read<int64_t>();
                int64_t intFirst = entrySlice.read<int64_t>();
                int64_t intLatest = entrySlice.read<int64_t>();
                block.blockSum = static_cast<double>(intSum);
                block.blockMin = static_cast<double>(intMin);
                block.blockMax = static_cast<double>(intMax);
                block.blockFirstValue = static_cast<double>(intFirst);
                block.blockLatestValue = static_cast<double>(intLatest);
                block.hasExtendedStats = true;
            } else if (fullEntry.seriesType == TSMValueType::Boolean) {
                // 40 bytes: 28 base + count(4) + trueCount(4) + firstValue(1) + latestValue(1) + pad(2)
                block.blockCount = entrySlice.read<uint32_t>();
                block.boolTrueCount = entrySlice.read<uint32_t>();
                block.boolFirstValue = (entrySlice.read<uint8_t>() != 0);
                block.boolLatestValue = (entrySlice.read<uint8_t>() != 0);
                entrySlice.offset += 2;  // skip padding
                // Convert for aggregator compatibility
                block.blockSum = static_cast<double>(block.boolTrueCount);
                block.blockMin = (block.boolTrueCount < block.blockCount) ? 0.0 : 1.0;
                block.blockMax = (block.boolTrueCount > 0) ? 1.0 : 0.0;
                block.blockFirstValue = block.boolFirstValue ? 1.0 : 0.0;
                block.blockLatestValue = block.boolLatestValue ? 1.0 : 0.0;
                block.hasExtendedStats = true;
            } else if (fullEntry.seriesType == TSMValueType::String) {
                // 32 bytes: 28 base + count(4)
                block.blockCount = entrySlice.read<uint32_t>();
                // No value stats for strings — blockCount enables COUNT pushdown
            }
        }
        fullEntry.indexBlocks.push_back(block);
    }

    // Phase 3: Parse string dictionary if present
    if (fullEntry.seriesType == TSMValueType::String && fileVersion >= 2 &&
        entrySlice.offset + 4 <= entrySlice.length_) {
        uint32_t dictSize = entrySlice.read<uint32_t>();
        if (dictSize > 0 && entrySlice.offset + dictSize <= entrySlice.length_) {
            auto dict = StringEncoder::deserializeDictionary(entrySlice, dictSize);
            if (dict.valid) {
                fullEntry.stringDictionary = std::move(dict.entries);
            }
        }
    }

    // Step 6: Cache it with LRU eviction.
    //
    // Concurrency note (Seastar cooperative model):
    // The co_await at Step 4 above is a suspension point.  While this coroutine
    // was suspended waiting for DMA I/O, another coroutine running on the same
    // shard (e.g. a parallel prefetchFullIndexEntries call) could have already
    // populated the cache for this exact seriesId.  Re-checking here prevents
    // a double-insert: two LRU list nodes for the same key with only one map
    // entry, causing the orphaned node to later evict the map entry prematurely.
    //
    // There is NO race between the size() check and the mutation below because
    // there is no co_await between them — the Seastar reactor cannot schedule
    // another task between these two lines, making the check-then-mutate
    // sequence effectively atomic within a shard.
    {
        auto existingIt = fullIndexCache.find(seriesId);
        if (existingIt != fullIndexCache.end()) {
            // Another coroutine beat us to it while we were suspended on DMA I/O.
            // Promote to front of LRU and return the already-cached entry.
            lruList.splice(lruList.begin(), lruList, existingIt->second);
            co_return &existingIt->second->second;
        }
    }

    size_t entryBytes = estimateEntryBytes(fullEntry);
    // Evict LRU entries until under byte budget
    while (!lruList.empty() && fullIndexCacheBytes + entryBytes > maxCacheBytes()) {
        auto& lruEntry = lruList.back();
        fullIndexCacheBytes -= estimateEntryBytes(lruEntry.second);
        fullIndexCache.erase(lruEntry.first);
        lruList.pop_back();
    }
    // Insert at front of LRU list (most recently used)
    lruList.emplace_front(seriesId, std::move(fullEntry));
    fullIndexCache[seriesId] = lruList.begin();
    fullIndexCacheBytes += entryBytes;

    timestar::tsm_log.trace("Loaded full index entry for series {} ({} blocks)", seriesId.toHex(), blockCount);

    // Phase 3: Set thread-local dictionary for string block decoding
    auto& cachedEntry = lruList.front().second;
    if (cachedEntry.seriesType == TSMValueType::String) {
        tlStringDict = cachedEntry.stringDictionary.empty() ? nullptr : &cachedEntry.stringDictionary;
    }

    co_return &cachedEntry;
}

// Bulk prefetch: identify cache misses and issue coalesced DMA reads.
// Sorts entries by file offset and merges adjacent reads into larger chunks
// to reduce syscall count. A 256KB coalesce window typically merges 10-50
// individual reads into 1-2 DMA operations.
seastar::future<> TSM::prefetchFullIndexEntries(const std::vector<SeriesId128>& seriesIds) {
    // Collect entries that need fetching, with their file offsets for sorting
    struct FetchEntry {
        SeriesId128 id;
        uint64_t offset;
        uint32_t size;
    };
    std::vector<FetchEntry> toFetch;
    toFetch.reserve(seriesIds.size());

    for (const auto& seriesId : seriesIds) {
        if (fullIndexCache.find(seriesId) != fullIndexCache.end())
            continue;
        if (!seriesBloomFilter.contains(seriesId.getRawData()))
            continue;
        auto sparseIt = sparseIndex.find(seriesId);
        if (sparseIt == sparseIndex.end())
            continue;
        toFetch.push_back({seriesId, sparseIt->second.fileOffset, sparseIt->second.entrySize});
    }

    if (toFetch.empty())
        co_return;

    // Sort by file offset to enable coalescing
    std::sort(toFetch.begin(), toFetch.end(),
              [](const FetchEntry& a, const FetchEntry& b) { return a.offset < b.offset; });

    // Coalesce adjacent entries into read groups.
    // A gap <= COALESCE_GAP between entries gets included in a single read.
    static constexpr uint64_t COALESCE_GAP = 64 * 1024;  // 64KB gap threshold

    struct ReadGroup {
        uint64_t startOffset;
        uint64_t endOffset;  // exclusive
        std::vector<const FetchEntry*> entries;
    };

    std::vector<ReadGroup> groups;
    groups.push_back({toFetch[0].offset, toFetch[0].offset + toFetch[0].size, {&toFetch[0]}});

    for (size_t i = 1; i < toFetch.size(); ++i) {
        auto& cur = groups.back();
        uint64_t entryEnd = toFetch[i].offset + toFetch[i].size;
        if (toFetch[i].offset <= cur.endOffset + COALESCE_GAP) {
            // Coalesce: extend the current group
            cur.endOffset = std::max(cur.endOffset, entryEnd);
            cur.entries.push_back(&toFetch[i]);
        } else {
            // New group
            groups.push_back({toFetch[i].offset, entryEnd, {&toFetch[i]}});
        }
    }

    // Read each group with a single DMA read and parse all entries from the buffer
    for (auto& group : groups) {
        uint64_t readSize = group.endOffset - group.startOffset;
        seastar::temporary_buffer<char> buf;
        bool readFailed = false;
        try {
            buf = co_await tsmFile.dma_read_exactly<char>(group.startOffset, readSize);
        } catch (...) {
            readFailed = true;
        }
        if (readFailed) {
            // Fallback: read entries individually
            for (const auto* entry : group.entries) {
                co_await getFullIndexEntry(entry->id).discard_result();
            }
            continue;
        }

        for (const auto* entry : group.entries) {
            // Skip if another coroutine cached it while we were reading
            if (fullIndexCache.find(entry->id) != fullIndexCache.end())
                continue;

            uint64_t localOffset = entry->offset - group.startOffset;
            if (localOffset + entry->size > buf.size())
                continue;

            Slice entrySlice(reinterpret_cast<const uint8_t*>(buf.get() + localOffset), entry->size);

            TSMIndexEntry fullEntry;
            // Zero-copy series ID from entry buffer
            fullEntry.seriesId =
                SeriesId128::fromBytes(reinterpret_cast<const char*>(entrySlice.data + entrySlice.offset), 16);
            entrySlice.offset += 16;
            fullEntry.seriesType = static_cast<TSMValueType>(entrySlice.read<uint8_t>());
            uint16_t blockCount = entrySlice.read<uint16_t>();

            fullEntry.indexBlocks.reserve(blockCount);

            for (uint16_t b = 0; b < blockCount; ++b) {
                TSMIndexBlock block;
                block.minTime = entrySlice.read<uint64_t>();
                block.maxTime = entrySlice.read<uint64_t>();
                block.offset = entrySlice.read<uint64_t>();
                block.size = entrySlice.read<uint32_t>();
                if (fullEntry.seriesType == TSMValueType::Float) {
                    block.blockSum = entrySlice.read<double>();
                    block.blockMin = entrySlice.read<double>();
                    block.blockMax = entrySlice.read<double>();
                    block.blockCount = entrySlice.read<uint32_t>();
                    block.blockM2 = entrySlice.read<double>();
                    block.blockFirstValue = entrySlice.read<double>();
                    block.blockLatestValue = entrySlice.read<double>();
                    block.hasExtendedStats = true;
                } else if (fileVersion >= 2) {
                    if (fullEntry.seriesType == TSMValueType::Integer) {
                        block.blockCount = entrySlice.read<uint32_t>();
                        int64_t intSum = entrySlice.read<int64_t>();
                        int64_t intMin = entrySlice.read<int64_t>();
                        int64_t intMax = entrySlice.read<int64_t>();
                        int64_t intFirst = entrySlice.read<int64_t>();
                        int64_t intLatest = entrySlice.read<int64_t>();
                        block.blockSum = static_cast<double>(intSum);
                        block.blockMin = static_cast<double>(intMin);
                        block.blockMax = static_cast<double>(intMax);
                        block.blockFirstValue = static_cast<double>(intFirst);
                        block.blockLatestValue = static_cast<double>(intLatest);
                        block.hasExtendedStats = true;
                    } else if (fullEntry.seriesType == TSMValueType::Boolean) {
                        block.blockCount = entrySlice.read<uint32_t>();
                        block.boolTrueCount = entrySlice.read<uint32_t>();
                        block.boolFirstValue = (entrySlice.read<uint8_t>() != 0);
                        block.boolLatestValue = (entrySlice.read<uint8_t>() != 0);
                        entrySlice.offset += 2;
                        block.blockSum = static_cast<double>(block.boolTrueCount);
                        block.blockMin = (block.boolTrueCount < block.blockCount) ? 0.0 : 1.0;
                        block.blockMax = (block.boolTrueCount > 0) ? 1.0 : 0.0;
                        block.blockFirstValue = block.boolFirstValue ? 1.0 : 0.0;
                        block.blockLatestValue = block.boolLatestValue ? 1.0 : 0.0;
                        block.hasExtendedStats = true;
                    } else if (fullEntry.seriesType == TSMValueType::String) {
                        block.blockCount = entrySlice.read<uint32_t>();
                    }
                }
                fullEntry.indexBlocks.push_back(block);
            }

            // Phase 3: Parse string dictionary
            if (fullEntry.seriesType == TSMValueType::String && fileVersion >= 2 &&
                entrySlice.offset + 4 <= entrySlice.length_) {
                uint32_t dictSize = entrySlice.read<uint32_t>();
                if (dictSize > 0 && entrySlice.offset + dictSize <= entrySlice.length_) {
                    auto dict = StringEncoder::deserializeDictionary(entrySlice, dictSize);
                    if (dict.valid) {
                        fullEntry.stringDictionary = std::move(dict.entries);
                    }
                }
            }

            // Cache the entry
            size_t entryBytes = estimateEntryBytes(fullEntry);
            while (!lruList.empty() && fullIndexCacheBytes + entryBytes > maxCacheBytes()) {
                auto& lruEntry = lruList.back();
                fullIndexCacheBytes -= estimateEntryBytes(lruEntry.second);
                fullIndexCache.erase(lruEntry.first);
                lruList.pop_back();
            }
            lruList.emplace_front(entry->id, std::move(fullEntry));
            fullIndexCache[entry->id] = lruList.begin();
            fullIndexCacheBytes += entryBytes;
        }
    }
}

template <class T>
seastar::future<> TSM::readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                  TSMResult<T>& results) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);

    if (!indexEntry) {
        co_return;  // Series not in this file
    }

    // Phase 3: Set thread-local dictionary for string block decoding
    if constexpr (std::is_same_v<T, std::string>) {
        tlStringDict = indexEntry->stringDictionary.empty() ? nullptr : &indexEntry->stringDictionary;
    }

    // Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    std::copy_if(indexEntry->indexBlocks.begin(), indexEntry->indexBlocks.end(), std::back_inserter(blocksToScan),
                 [endTime, startTime](const TSMIndexBlock& indexBlock) {
                     return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
                 });

    // Pre-allocate a slot per block so each coroutine writes to its own index,
    // avoiding the data race where multiple coroutines push_back concurrently
    // on the shared results.blocks vector after co_await suspension points.
    std::vector<std::unique_ptr<TSMBlock<T>>> blockSlots(blocksToScan.size());
    size_t slotIdx = 0;

    co_await seastar::parallel_for_each(blocksToScan, [&](TSMIndexBlock indexBlock) -> seastar::future<> {
        // Capture slot index before any suspension point (safe: lambda invocations
        // are sequential in cooperative scheduling before the first co_await)
        size_t mySlot = slotIdx++;
        auto block = co_await readSingleBlock<T>(indexBlock, startTime, endTime);
        if (block && !block->timestamps.empty()) {
            blockSlots[mySlot] = std::move(block);
        }
    });

    // Collect non-null results into the output TSMResult (single-threaded, safe)
    for (auto& block : blockSlots) {
        if (block) {
            results.appendBlock(std::move(block));
        }
    }

    // parallel_for_each does not guarantee ordering, so sort blocks by start time
    results.sort();
}

std::optional<TSMValueType> TSM::getSeriesType(const SeriesId128& seriesId) {
    // Check bloom filter first
    if (!seriesBloomFilter.contains(seriesId.getRawData())) {
        return {};
    }

    // Check full index cache (populated after getFullIndexEntry is called)
    auto it = fullIndexCache.find(seriesId);
    if (it != fullIndexCache.end()) {
        // Promote to front of LRU list on access
        lruList.splice(lruList.begin(), lruList, it->second);
        return it->second->second.seriesType;
    }

    // Fall back to sparse index which stores the type from the initial index parse
    auto sparseIt = sparseIndex.find(seriesId);
    if (sparseIt != sparseIndex.end()) {
        return sparseIt->second.seriesType;
    }

    return {};
}

// Phase 1.1: Get index blocks without reading data
// Note: This returns blocks only if already loaded in cache
// For guaranteed results, caller should await getFullIndexEntry() first
std::vector<TSMIndexBlock> TSM::getSeriesBlocks(const SeriesId128& seriesId) const {
    // Check cache (promote to front on hit)
    auto it = fullIndexCache.find(seriesId);
    if (it != fullIndexCache.end()) {
        lruList.splice(lruList.begin(), lruList, it->second);
        return it->second->second.indexBlocks;
    }

    // Not in cache - return empty
    return {};
}

// Phase 1.1: Read a single block and return it (not appending to results)
template <class T>
seastar::future<std::unique_ptr<TSMBlock<T>>> TSM::readSingleBlock(const TSMIndexBlock& indexBlock, uint64_t startTime,
                                                                   uint64_t endTime) {
    // Bug #8 fix: Capture tlStringDict BEFORE co_await — another coroutine
    // on this thread could overwrite the thread-local during DMA suspension.
    const std::vector<std::string>* localDict = tlStringDict;

    auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
    Slice blockSlice(blockBuf.get(), blockBuf.size());

    if (indexBlock.size < BLOCK_HEADER_SIZE) {
        throw std::runtime_error("TSM block too small: " + std::to_string(indexBlock.size));
    }

    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > indexBlock.size - BLOCK_HEADER_SIZE) {
        throw std::runtime_error("TSM block timestampBytes exceeds block size");
    }

    // Validate that the block's stored type matches the template parameter
    TSMValueType expectedType = getValueType<T>();
    if (static_cast<TSMValueType>(blockType) != expectedType) {
        throw std::runtime_error("TSM block type mismatch: block contains type " + std::to_string(blockType) +
                                 " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
    }

    auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime);
    size_t valueByteSize = indexBlock.size - timestampBytes - BLOCK_HEADER_SIZE;

    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Phase 3: Check if dictionary-encoded (STR2 magic)
        // Bug #8 fix: Use localDict (captured before co_await) instead of tlStringDict
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && localDict && !localDict->empty()) {
            StringEncoder::Dictionary dict;
            dict.entries = *localDict;
            dict.valid = true;
            StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, dict,
                                            blockResults->values);
        } else {
            StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Reuse thread-local scratch buffer to avoid per-block heap allocation
        static thread_local std::vector<uint64_t> rawUintScratch;
        rawUintScratch.clear();
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        blockResults->values.reserve(nTimestamps);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            blockResults->values.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
    }

    co_return blockResults;
}

template seastar::future<> TSM::readSeries<double>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                   TSMResult<double>& results);
template seastar::future<> TSM::readSeries<bool>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                 TSMResult<bool>& results);
template seastar::future<> TSM::readSeries<std::string>(const SeriesId128& seriesId, uint64_t startTime,
                                                        uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readSeries<int64_t>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                    TSMResult<int64_t>& results);

// Phase 1.1: Template instantiations for readSingleBlock
template seastar::future<std::unique_ptr<TSMBlock<double>>> TSM::readSingleBlock<double>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<bool>>> TSM::readSingleBlock<bool>(const TSMIndexBlock& indexBlock,
                                                                                     uint64_t startTime,
                                                                                     uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<std::string>>> TSM::readSingleBlock<std::string>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<int64_t>>> TSM::readSingleBlock<int64_t>(
    const TSMIndexBlock& indexBlock, uint64_t startTime, uint64_t endTime);

// Phase 2: Read compressed block bytes directly without decompression
seastar::future<seastar::temporary_buffer<uint8_t>> TSM::readCompressedBlock(const TSMIndexBlock& indexBlock) {
    // Read the entire block as-is (already compressed)
    // No decompression - just read the raw bytes for zero-copy transfer
    auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
    co_return blockBuf;
}

seastar::future<> TSM::scheduleDelete() {
    // Do NOT close the file handle here — concurrent readers may still
    // hold shared_ptr<TSM> references with in-flight DMA reads.
    // Unix unlink is safe while the fd is open: reads continue via the
    // open fd, inode freed when the last fd (via ~seastar::file) closes.

    // Delete associated tombstone file first (no-op if none exists)
    co_await deleteTombstoneFile();

    // Delete the physical file using async Seastar I/O (not blocking std::filesystem::remove)
    try {
        co_await seastar::remove_file(filePath);
        timestar::tsm_log.info("TSM file deleted: {}", filePath);
    } catch (const std::exception& e) {
        timestar::tsm_log.error("Failed to delete TSM file {}: {}", filePath, e.what());
    }

    co_return;
}

// Group blocks into contiguous batches for optimized I/O
std::vector<BlockBatch> TSM::groupContiguousBlocks(const std::vector<TSMIndexBlock>& blocks) const {
    std::vector<BlockBatch> batches;
    if (blocks.empty()) {
        return batches;
    }

    // Maximum batch size to avoid excessive memory usage (16MB)
    constexpr uint64_t MAX_BATCH_SIZE = 16 * 1024 * 1024;

    BlockBatch currentBatch;
    currentBatch.startOffset = blocks[0].offset;
    currentBatch.totalSize = blocks[0].size;
    currentBatch.blocks.push_back(blocks[0]);

    for (size_t i = 1; i < blocks.size(); ++i) {
        uint64_t expectedOffset = currentBatch.startOffset + currentBatch.totalSize;
        uint64_t newBatchSize = currentBatch.totalSize + blocks[i].size;

        // Check if contiguous and within size limit
        if (blocks[i].offset == expectedOffset && newBatchSize <= MAX_BATCH_SIZE) {
            // Contiguous and fits - add to current batch
            currentBatch.blocks.push_back(blocks[i]);
            currentBatch.totalSize += blocks[i].size;
        } else {
            // Gap detected or size limit - finalize current batch and start new one
            batches.push_back(std::move(currentBatch));
            currentBatch = BlockBatch();
            currentBatch.startOffset = blocks[i].offset;
            currentBatch.totalSize = blocks[i].size;
            currentBatch.blocks.push_back(blocks[i]);
        }
    }

    // Don't forget the last batch
    batches.push_back(std::move(currentBatch));

    timestar::tsm_log.debug("Grouped {} blocks into {} batches", blocks.size(), batches.size());

    return batches;
}

// Extract block decoding logic for reuse
template <class T>
std::unique_ptr<TSMBlock<T>> TSM::decodeBlock(Slice& blockSlice, uint32_t blockSize, uint64_t startTime,
                                              uint64_t endTime) {
    // Defensive check: ensure we have at least header size
    if (blockSize < BLOCK_HEADER_SIZE) {
        timestar::tsm_log.error("Block size {} is too small for header", blockSize);
        return nullptr;
    }

    // Parse block header (type + timestampSize + timestampBytes)
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    // Validate that the block's stored type matches the template parameter
    TSMValueType expectedType = getValueType<T>();
    if (static_cast<TSMValueType>(blockType) != expectedType) {
        throw std::runtime_error("TSM block type mismatch: block contains type " + std::to_string(blockType) +
                                 " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
    }

    // Defensive check: ensure timestampBytes is reasonable
    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE) {
        timestar::tsm_log.error("Timestamp bytes {} exceeds block size {} - {}", timestampBytes, blockSize,
                                BLOCK_HEADER_SIZE);
        return nullptr;
    }

    // Decode timestamps with time-range filtering
    auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime);

    // Decode values based on type
    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;

    // Defensive check
    if (valueByteSize > blockSize) {
        timestar::tsm_log.error("Value byte size {} is invalid for block size {}", valueByteSize, blockSize);
        return nullptr;
    }

    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && tlStringDict && !tlStringDict->empty()) {
            StringEncoder::Dictionary dict;
            dict.entries = *tlStringDict;
            dict.valid = true;
            StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, dict,
                                            blockResults->values);
        } else {
            StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        // Reuse thread-local scratch buffer to avoid per-block heap allocation
        static thread_local std::vector<uint64_t> rawUintScratch;
        rawUintScratch.clear();
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        blockResults->values.reserve(nTimestamps);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            blockResults->values.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
    }

    return blockResults;
}

// Decode a block directly into flat output vectors (no TSMBlock heap allocation).
// Returns the number of points decoded.
template <class T>
static size_t decodeBlockFlat(const uint8_t* data, uint32_t blockSize, uint64_t startTime, uint64_t endTime,
                              std::vector<uint64_t>& outTimestamps, std::vector<T>& outValues) {
    if (blockSize < BLOCK_HEADER_SIZE)
        return 0;

    Slice blockSlice(data, blockSize);
    auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
    [[maybe_unused]] uint8_t blockType = headerSlice.read<uint8_t>();
    uint32_t timestampSize = headerSlice.read<uint32_t>();
    uint32_t timestampBytes = headerSlice.read<uint32_t>();

    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE)
        return 0;

    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] =
        IntegerEncoder::decode(timestampsSlice, timestampSize, outTimestamps, startTime, endTime);

    if (nTimestamps == 0)
        return 0;

    size_t valueByteSize = blockSize - timestampBytes - BLOCK_HEADER_SIZE;
    if (valueByteSize > blockSize)
        return 0;

    if constexpr (std::is_same_v<T, double>) {
        auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
        FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, outValues);
    } else if constexpr (std::is_same_v<T, bool>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, outValues);
    } else if constexpr (std::is_same_v<T, std::string>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        if (StringEncoder::isDictionaryEncoded(valuesSlice) && tlStringDict && !tlStringDict->empty()) {
            StringEncoder::Dictionary dict;
            dict.entries = *tlStringDict;
            dict.valid = true;
            StringEncoder::decodeDictionary(valuesSlice, timestampSize, nSkipped, nTimestamps, dict, outValues);
        } else {
            StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, outValues);
        }
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        static thread_local std::vector<uint64_t> rawUintScratch;
        rawUintScratch.clear();
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUintScratch);
        size_t end = std::min(nSkipped + nTimestamps, rawUintScratch.size());
        for (size_t i = nSkipped; i < end; ++i) {
            outValues.push_back(ZigZag::zigzagDecode(rawUintScratch[i]));
        }
    }

    return nTimestamps;
}

// Read multiple contiguous blocks with a single I/O operation.
// Decodes all blocks in the batch directly into flat timestamp/value vectors
// stored in a single TSMBlock, eliminating per-block heap allocations.
template <class T>
seastar::future<> TSM::readBlockBatch(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                      TSMResult<T>& results) {
    // Single large DMA read for entire batch
    auto batchBuf = co_await tsmFile.dma_read_exactly<uint8_t>(batch.startOffset, batch.totalSize);

    // Decode all blocks in this batch into a single flat TSMBlock.
    // Blocks within a batch are contiguous and sorted by offset (= time order),
    // so appending sequentially produces sorted output.
    auto flatBlock = std::make_unique<TSMBlock<T>>(0);

    uint32_t bufferOffset = 0;
    for (const auto& block : batch.blocks) {
        decodeBlockFlat<T>(batchBuf.get() + bufferOffset, block.size, startTime, endTime, flatBlock->timestamps,
                           flatBlock->values);
        bufferOffset += block.size;
    }

    if (!flatBlock->timestamps.empty()) {
        results.appendBlock(std::move(flatBlock));
    }

    co_return;
}

// Optimized series read using block batching
template <class T>
seastar::future<> TSM::readSeriesBatched(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                         TSMResult<T>& results) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return;  // Series not in this file
    }

    // Phase 3: Set thread-local dictionary for string block decoding
    if constexpr (std::is_same_v<T, std::string>) {
        tlStringDict = indexEntry->stringDictionary.empty() ? nullptr : &indexEntry->stringDictionary;
    }

    // Step 1: Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    std::copy_if(indexEntry->indexBlocks.begin(), indexEntry->indexBlocks.end(), std::back_inserter(blocksToScan),
                 [endTime, startTime](const TSMIndexBlock& indexBlock) {
                     return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
                 });

    if (blocksToScan.empty()) {
        co_return;
    }

    // Step 2: Blocks should already be sorted by offset from index, but verify
    // (TSM writer ensures blocks are written in order)

    // Step 3: Group into contiguous batches
    auto batches = groupContiguousBlocks(blocksToScan);

    // Log batching efficiency for performance monitoring
    size_t totalBlocks = blocksToScan.size();
    size_t totalBatches = batches.size();
    if (totalBlocks > 1) {
        double batchEfficiency = (double)totalBlocks / totalBatches;
        timestar::tsm_log.debug("Batch read efficiency: {:.1f} blocks/batch ({} blocks → {} batches)", batchEfficiency,
                                totalBlocks, totalBatches);
    }

    // Step 4: Single-batch fast path (the common case — blocks are contiguous).
    // Avoids parallel_for_each overhead and per-batch TSMResult intermediaries.
    if (batches.size() == 1) {
        co_await readBlockBatch(batches[0], startTime, endTime, results);
    } else {
        // Multiple batches: read in parallel with per-batch slot isolation.
        std::vector<TSMResult<T>> batchResults;
        batchResults.reserve(batches.size());
        for (size_t i = 0; i < batches.size(); ++i) {
            batchResults.emplace_back(0);
        }
        size_t batchSlotIdx = 0;

        co_await seastar::parallel_for_each(batches, [&](const BlockBatch& batch) -> seastar::future<> {
            size_t mySlot = batchSlotIdx++;
            co_await readBlockBatch(batch, startTime, endTime, batchResults[mySlot]);
        });

        for (auto& batchResult : batchResults) {
            for (auto& block : batchResult.blocks) {
                results.appendBlock(std::move(block));
            }
        }
    }

    // parallel_for_each does not guarantee ordering, so sort blocks by start time
    results.sort();

    co_return;
}

// Pushdown aggregation: decode float blocks and fold directly into a
// BlockAggregator, bypassing TSMResult/QueryResult materialisation.
// Handles tombstone filtering inline.  Returns the number of points folded.
seastar::future<size_t> TSM::aggregateSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                             timestar::BlockAggregator& aggregator) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return 0;
    }

    // Float, Integer, and Boolean support pushdown aggregation; String does not
    if (indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    for (const auto& block : indexEntry->indexBlocks) {
        if (block.minTime <= endTime && startTime <= block.maxTime) {
            blocksToScan.push_back(block);
        }
    }
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Group into contiguous batches for efficient I/O
    auto batches = groupContiguousBlocks(blocksToScan);

    // Pre-fetch tombstone ranges once (empty vector if no tombstones)
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }
    const bool hasTombstoneRanges = !tombstoneRanges.empty();

    size_t totalPoints = 0;

    // Process batches.  parallel_for_each is safe here because all shared
    // mutable state (aggregator, totalPoints) is only touched synchronously
    // between co_await suspension points — Seastar's cooperative scheduling
    // guarantees at most one coroutine runs at a time on a given shard.
    const bool countOnly = aggregator.isCountOnly();

    co_await seastar::parallel_for_each(batches, [&](const BlockBatch& batch) -> seastar::future<> {
        // Pre-scan: determine which blocks can skip decode via block stats.
        // Cache the result to avoid re-evaluating after DMA read.
        const size_t blockCount = batch.blocks.size();
        bool anyNeedsDecode = false;

        // Use stack allocation for small batches (typical), heap for large
        bool stackFlags[64];
        std::unique_ptr<bool[]> heapFlags;
        bool* skippedFlags;
        if (blockCount <= 64) {
            skippedFlags = stackFlags;
        } else {
            heapFlags = std::make_unique<bool[]>(blockCount);
            skippedFlags = heapFlags.get();
        }

        for (size_t bi = 0; bi < blockCount; ++bi) {
            const auto& block = batch.blocks[bi];
            bool blockFullyContained = (block.minTime >= startTime && block.maxTime <= endTime);
            // Phase 0: per-block tombstone check instead of global hasTombstoneRanges gate
            bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
            bool canSkip = !blockHasTombstones && blockFullyContained && block.blockCount > 0 &&
                           aggregator.canUseBlockStats(block.minTime, block.maxTime, block.hasExtendedStats);
            skippedFlags[bi] = canSkip;
            if (canSkip) {
                aggregator.addBlockStats(block.blockSum, block.blockMin, block.blockMax, block.blockCount,
                                         block.minTime, block.maxTime, block.blockM2, block.blockFirstValue,
                                         block.blockLatestValue);
                totalPoints += block.blockCount;
            } else {
                anyNeedsDecode = true;
            }
        }

        if (!anyNeedsDecode) {
            co_return;  // All blocks in this batch were handled via stats
        }

        // Single DMA read for the entire batch
        auto batchBuf = co_await tsmFile.dma_read_exactly<uint8_t>(batch.startOffset, batch.totalSize);

        uint32_t bufferOffset = 0;
        for (size_t bi = 0; bi < blockCount; ++bi) {
            const auto& block = batch.blocks[bi];
            if (skippedFlags[bi]) {
                bufferOffset += block.size;
                continue;
            }

            // Phase 0: per-block tombstone check instead of global gate
            bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
            if (!blockHasTombstones) {
                // Fast path: no tombstones for this block — decode into scratch buffers and fold
                size_t n;
                if (countOnly) {
                    n = decodeBlockCountOnly(batchBuf.get() + bufferOffset, block.size, startTime, endTime, aggregator);
                } else if (seriesType == TSMValueType::Float) {
                    n = decodeBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, startTime, endTime,
                                                  aggregator);
                } else if (seriesType == TSMValueType::Integer) {
                    n = decodeIntegerBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, startTime, endTime,
                                                         aggregator);
                } else {
                    // Boolean
                    n = decodeBoolBlockIntoAggregator(batchBuf.get() + bufferOffset, block.size, startTime, endTime,
                                                      aggregator);
                }
                totalPoints += n;
            } else {
                // Tombstone path: need per-point filtering, use full decode.
                Slice blockSlice(batchBuf.get() + bufferOffset, block.size);
                if (seriesType == TSMValueType::Float) {
                    auto blockResult = decodeBlock<double>(blockSlice, block.size, startTime, endTime);
                    if (blockResult && !blockResult->timestamps.empty()) {
                        const auto& ts = blockResult->timestamps;
                        const auto& vals = blockResult->values;
                        for (size_t i = 0; i < ts.size(); ++i) {
                            uint64_t t = ts[i];
                            auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                            std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                            bool isTombstoned = false;
                            if (rangeIt != tombstoneRanges.begin()) {
                                --rangeIt;
                                if (t >= rangeIt->first && t <= rangeIt->second)
                                    isTombstoned = true;
                            }
                            if (!isTombstoned) {
                                aggregator.addPoint(t, vals[i]);
                                totalPoints++;
                            }
                        }
                    }
                } else if (seriesType == TSMValueType::Integer) {
                    auto blockResult = decodeBlock<int64_t>(blockSlice, block.size, startTime, endTime);
                    if (blockResult && !blockResult->timestamps.empty()) {
                        const auto& ts = blockResult->timestamps;
                        const auto& vals = blockResult->values;
                        for (size_t i = 0; i < ts.size(); ++i) {
                            uint64_t t = ts[i];
                            auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                            std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                            bool isTombstoned = false;
                            if (rangeIt != tombstoneRanges.begin()) {
                                --rangeIt;
                                if (t >= rangeIt->first && t <= rangeIt->second)
                                    isTombstoned = true;
                            }
                            if (!isTombstoned) {
                                aggregator.addPoint(t, static_cast<double>(vals[i]));
                                totalPoints++;
                            }
                        }
                    }
                } else {
                    // Boolean
                    auto blockResult = decodeBlock<bool>(blockSlice, block.size, startTime, endTime);
                    if (blockResult && !blockResult->timestamps.empty()) {
                        const auto& ts = blockResult->timestamps;
                        const auto& vals = blockResult->values;
                        for (size_t i = 0; i < ts.size(); ++i) {
                            uint64_t t = ts[i];
                            auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                            std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                            bool isTombstoned = false;
                            if (rangeIt != tombstoneRanges.begin()) {
                                --rangeIt;
                                if (t >= rangeIt->first && t <= rangeIt->second)
                                    isTombstoned = true;
                            }
                            if (!isTombstoned) {
                                aggregator.addPoint(t, vals[i] ? 1.0 : 0.0);
                                totalPoints++;
                            }
                        }
                    }
                }
            }

            bufferOffset += block.size;
        }
    });

    co_return totalPoints;
}

seastar::future<size_t> TSM::aggregateSeriesSelective(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                      timestar::BlockAggregator& aggregator, bool reverse,
                                                      size_t maxPoints) {
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry || indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    for (const auto& block : indexEntry->indexBlocks) {
        if (block.minTime <= endTime && startTime <= block.maxTime) {
            blocksToScan.push_back(block);
        }
    }
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Fast path: for LATEST/FIRST with maxPoints=1 and extended stats,
    // use the block-level latestValue/firstValue directly from the index entry.
    // This avoids reading any data blocks from disk — pure metadata lookup.
    // Only valid when the block's extreme timestamp falls within the query range.
    if (maxPoints == 1 && !hasTombstones()) {
        if (reverse) {
            // LATEST: find the block with the highest maxTime that's within range
            const TSMIndexBlock* bestBlock = nullptr;
            for (const auto& block : blocksToScan) {
                if (!bestBlock || block.maxTime > bestBlock->maxTime) {
                    bestBlock = &block;
                }
            }
            // Only use stats shortcut if the block's maxTime is within the query range.
            // If maxTime > endTime, the actual latest point within range requires reading the block.
            if (bestBlock && bestBlock->hasExtendedStats && bestBlock->maxTime <= endTime) {
                aggregator.addPoint(bestBlock->maxTime, bestBlock->blockLatestValue);
                co_return 1;
            }
        } else {
            // FIRST: find the block with the lowest minTime that's within range
            const TSMIndexBlock* bestBlock = nullptr;
            for (const auto& block : blocksToScan) {
                if (!bestBlock || block.minTime < bestBlock->minTime) {
                    bestBlock = &block;
                }
            }
            if (bestBlock && bestBlock->hasExtendedStats && bestBlock->minTime >= startTime) {
                aggregator.addPoint(bestBlock->minTime, bestBlock->blockFirstValue);
                co_return 1;
            }
        }
    }

    // Pre-fetch tombstone ranges once
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }
    const bool hasTombstoneRanges = !tombstoneRanges.empty();

    size_t totalPoints = 0;
    size_t count = blocksToScan.size();

    // Helper lambda: iterate decoded points, optionally filtering tombstones, in fwd/rev order.
    auto processPoints = [&](const std::vector<uint64_t>& ts, const auto& vals, auto toDouble) {
        size_t pointCount = ts.size();
        for (size_t pi = 0; pi < pointCount && totalPoints < maxPoints; ++pi) {
            size_t j = reverse ? (pointCount - 1 - pi) : pi;
            uint64_t t = ts[j];
            if (hasTombstoneRanges) {
                auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                if (rangeIt != tombstoneRanges.begin()) {
                    --rangeIt;
                    if (t >= rangeIt->first && t <= rangeIt->second)
                        continue;
                }
            }
            aggregator.addPoint(t, toDouble(vals[j]));
            totalPoints++;
        }
    };

    for (size_t i = 0; i < count && totalPoints < maxPoints; ++i) {
        size_t idx = reverse ? (count - 1 - i) : i;
        const auto& block = blocksToScan[idx];

        if (seriesType == TSMValueType::Float) {
            auto blockResult = co_await readSingleBlock<double>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            processPoints(blockResult->timestamps, blockResult->values, [](double v) { return v; });
        } else if (seriesType == TSMValueType::Integer) {
            auto blockResult = co_await readSingleBlock<int64_t>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            processPoints(blockResult->timestamps, blockResult->values,
                          [](int64_t v) { return static_cast<double>(v); });
        } else {
            // Boolean
            auto blockResult = co_await readSingleBlock<bool>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            processPoints(blockResult->timestamps, blockResult->values, [](bool v) { return v ? 1.0 : 0.0; });
        }
    }

    co_return totalPoints;
}

seastar::future<size_t> TSM::aggregateSeriesBucketed(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime,
                                                     timestar::BlockAggregator& aggregator, bool reverse,
                                                     uint64_t interval, std::unordered_set<uint64_t>& filledBuckets,
                                                     size_t totalBuckets) {
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry || indexEntry->seriesType == TSMValueType::String) {
        co_return 0;
    }

    const auto seriesType = indexEntry->seriesType;

    // Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    for (const auto& block : indexEntry->indexBlocks) {
        if (block.minTime <= endTime && startTime <= block.maxTime) {
            blocksToScan.push_back(block);
        }
    }
    if (blocksToScan.empty()) {
        co_return 0;
    }

    // Pre-fetch tombstone ranges once
    std::vector<std::pair<uint64_t, uint64_t>> tombstoneRanges;
    if (hasTombstones()) {
        tombstoneRanges = tombstones->getTombstoneRanges(seriesId);
    }
    const bool hasTombstoneRanges = !tombstoneRanges.empty();

    size_t totalPoints = 0;
    size_t count = blocksToScan.size();

    for (size_t i = 0; i < count; ++i) {
        // Early termination: all buckets filled
        if (filledBuckets.size() >= totalBuckets) {
            break;
        }

        size_t idx = reverse ? (count - 1 - i) : i;
        const auto& block = blocksToScan[idx];

        // Check if all buckets spanned by this block are already filled.
        // Use the same bucket formula as BlockAggregator: (t / interval) * interval
        uint64_t blockStart = std::max(block.minTime, startTime);
        uint64_t blockEnd = std::min(block.maxTime, endTime);
        uint64_t firstBucket = (blockStart / interval) * interval;
        uint64_t lastBucket = (blockEnd / interval) * interval;

        bool allBlockBucketsFilled = true;
        for (uint64_t b = firstBucket; b <= lastBucket; b += interval) {
            if (filledBuckets.find(b) == filledBuckets.end()) {
                allBlockBucketsFilled = false;
                break;
            }
        }
        if (allBlockBucketsFilled) {
            continue;  // Skip this block entirely
        }

        // Extended-stats shortcut: when block has extended stats and fits in one
        // bucket, extract the first/latest endpoint directly without DMA
        // read + decode.  This preserves 1-point-per-bucket semantics.
        // Phase 0: per-block tombstone check instead of global gate
        bool blockFullyContained = (block.minTime >= startTime && block.maxTime <= endTime);
        bool blockHasTombstones = blockOverlapsTombstones(block.minTime, block.maxTime, tombstoneRanges);
        if (!blockHasTombstones && blockFullyContained && block.hasExtendedStats) {
            uint64_t bFirst = (block.minTime / interval) * interval;
            uint64_t bLast = (block.maxTime / interval) * interval;
            if (bFirst == bLast) {
                // Single-bucket block: use the relevant endpoint
                if (filledBuckets.find(bFirst) == filledBuckets.end()) {
                    if (reverse) {
                        aggregator.addPoint(block.maxTime, block.blockLatestValue);
                    } else {
                        aggregator.addPoint(block.minTime, block.blockFirstValue);
                    }
                    filledBuckets.insert(bFirst);
                    totalPoints++;
                }
                continue;
            }
        }

        // Helper lambda for bucketed point processing with tombstone filtering
        auto processBucketedPoints = [&](const std::vector<uint64_t>& ts, auto toDouble) {
            size_t pointCount = ts.size();
            for (size_t pi = 0; pi < pointCount; ++pi) {
                size_t j = reverse ? (pointCount - 1 - pi) : pi;
                uint64_t t = ts[j];
                if (hasTombstoneRanges) {
                    auto rangeIt = std::upper_bound(tombstoneRanges.begin(), tombstoneRanges.end(),
                                                    std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                    if (rangeIt != tombstoneRanges.begin()) {
                        --rangeIt;
                        if (t >= rangeIt->first && t <= rangeIt->second)
                            continue;
                    }
                }
                uint64_t bucketKey = (t / interval) * interval;
                if (filledBuckets.find(bucketKey) != filledBuckets.end())
                    continue;
                aggregator.addPoint(t, toDouble(j));
                filledBuckets.insert(bucketKey);
                totalPoints++;
            }
        };

        if (seriesType == TSMValueType::Float) {
            auto blockResult = co_await readSingleBlock<double>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            const auto& vals = blockResult->values;
            processBucketedPoints(blockResult->timestamps, [&vals](size_t j) { return vals[j]; });
        } else if (seriesType == TSMValueType::Integer) {
            auto blockResult = co_await readSingleBlock<int64_t>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            const auto& vals = blockResult->values;
            processBucketedPoints(blockResult->timestamps, [&vals](size_t j) { return static_cast<double>(vals[j]); });
        } else {
            // Boolean
            auto blockResult = co_await readSingleBlock<bool>(block, startTime, endTime);
            if (!blockResult || blockResult->timestamps.empty())
                continue;
            const auto& vals = blockResult->values;
            processBucketedPoints(blockResult->timestamps, [&vals](size_t j) { return vals[j] ? 1.0 : 0.0; });
        }
    }

    co_return totalPoints;
}

// Template instantiations for batched reads
template seastar::future<> TSM::readSeriesBatched<double>(const SeriesId128& seriesId, uint64_t startTime,
                                                          uint64_t endTime, TSMResult<double>& results);
template seastar::future<> TSM::readSeriesBatched<bool>(const SeriesId128& seriesId, uint64_t startTime,
                                                        uint64_t endTime, TSMResult<bool>& results);
template seastar::future<> TSM::readSeriesBatched<std::string>(const SeriesId128& seriesId, uint64_t startTime,
                                                               uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readSeriesBatched<int64_t>(const SeriesId128& seriesId, uint64_t startTime,
                                                           uint64_t endTime, TSMResult<int64_t>& results);

template seastar::future<> TSM::readBlockBatch<double>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                       TSMResult<double>& results);
template seastar::future<> TSM::readBlockBatch<bool>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                     TSMResult<bool>& results);
template seastar::future<> TSM::readBlockBatch<std::string>(const BlockBatch& batch, uint64_t startTime,
                                                            uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readBlockBatch<int64_t>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime,
                                                        TSMResult<int64_t>& results);

template std::unique_ptr<TSMBlock<double>> TSM::decodeBlock<double>(Slice& blockSlice, uint32_t blockSize,
                                                                    uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<bool>> TSM::decodeBlock<bool>(Slice& blockSlice, uint32_t blockSize,
                                                                uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<std::string>> TSM::decodeBlock<std::string>(Slice& blockSlice, uint32_t blockSize,
                                                                              uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<int64_t>> TSM::decodeBlock<int64_t>(Slice& blockSlice, uint32_t blockSize,
                                                                      uint64_t startTime, uint64_t endTime);
