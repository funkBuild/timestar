#include "tsm.hpp"
#include "float_encoder.hpp"
#include "integer_encoder.hpp"
#include "bool_encoder_rle.hpp"
#include "string_encoder.hpp"
#include "zigzag.hpp"
#include "slice_buffer.hpp"
#include "logger.hpp"

#include <filesystem>
#include <algorithm>
#include <string_view>

#include <chrono>

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/loop.hh>

typedef std::chrono::high_resolution_clock Clock;

// Block header: uint8_t type + uint32_t timestampSize + uint32_t timestampBytes
static constexpr size_t BLOCK_HEADER_SIZE = sizeof(uint8_t) + 2 * sizeof(uint32_t); // 9 bytes



TSM::TSM(std::string _absoluteFilePath){
  size_t filenameEndIndex = _absoluteFilePath.find_last_of(".");
  if (filenameEndIndex == std::string::npos)
    throw std::runtime_error("TSM invalid filename (no extension): " + _absoluteFilePath);
  size_t filenameStartIndex = _absoluteFilePath.find_last_of("/") + 1;

  std::string filename = _absoluteFilePath.substr(filenameStartIndex, filenameEndIndex - filenameStartIndex);

  size_t underscoreIndex = filename.find_last_of("_");
  if(underscoreIndex == std::string::npos)
    throw std::runtime_error("TSM invalid filename:" + filename);

  try {
    tierNum = std::stoull(filename.substr(0, underscoreIndex));
    seqNum = std::stoull(filename.substr(underscoreIndex+1));

    timestar::tsm_log.debug("tierNum={} seqNum={}", tierNum, seqNum);
  } catch(const std::exception&) {
    throw std::runtime_error("TSM invalid filename:" + filename);
  }

  filePath = _absoluteFilePath;
}

seastar::future<> TSM::open(){
  std::string_view filePathView{ filePath };
  tsmFile = co_await seastar::open_file_dma(filePathView, seastar::open_flags::ro);

  if(!tsmFile){
    timestar::tsm_log.error("TSM unable to open: {}", filePath);
    throw std::runtime_error("TSM unable to open:" + filePath);
  }

  // Clean up file handle on any failure after successful open.
  // GCC 14 does not support co_await in catch blocks, so we capture
  // the exception, close outside the handler, then rethrow.
  std::exception_ptr openError;
  try {
    length = co_await tsmFile.size();

    // Use lazy loading: read sparse index + bloom filter (not full index)
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

uint64_t TSM::rankAsInteger(){
  if (tierNum >= 16) {
    throw std::overflow_error("TSM::rankAsInteger: tierNum " + std::to_string(tierNum) +
                              " >= 16 would overflow in (tierNum << 60)");
  }
  constexpr uint64_t maxSeqNum = (uint64_t{1} << 60) - 1;
  uint64_t safeSeqNum = seqNum;
  if (seqNum > maxSeqNum) {
    timestar::tsm_log.warn("TSM::rankAsInteger: seqNum {} exceeds 60-bit limit, masking to prevent tier bit corruption", seqNum);
    safeSeqNum = seqNum & maxSeqNum;
  }
  return (tierNum << 60) | safeSeqNum;
}

// Lazy loading: read sparse index + build bloom filter
seastar::future<> TSM::readSparseIndex(){
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

  while(indexSlice.offset < indexSlice.length_){
    uint64_t entryStartOffset = indexOffset + indexSlice.offset;

    // Read series ID (16 bytes)
    std::string seriesIdBytes = indexSlice.readString(16);
    SeriesId128 seriesId = SeriesId128::fromBytes(seriesIdBytes);

    // Read type (1 byte) and block count (2 bytes)
    uint8_t type = indexSlice.read<uint8_t>();
    uint16_t blockCount = indexSlice.read<uint16_t>();

    // Validate blockCount against remaining index data to prevent
    // reads past the end of the index on malformed files.
    size_t blockBytes = static_cast<size_t>(blockCount) * 28;
    if (blockBytes > indexSlice.bytesLeft()) {
      throw std::runtime_error(
          "TSM index corrupt: blockCount " + std::to_string(blockCount) +
          " requires " + std::to_string(blockBytes) +
          " bytes but only " + std::to_string(indexSlice.bytesLeft()) + " remain");
    }

    // Calculate size of this entry
    // Each block: minTime(8) + maxTime(8) + offset(8) + size(4) = 28 bytes
    uint32_t entrySize = 16 + 1 + 2 + (blockCount * 28);

    // Skip over the blocks (don't parse them yet)
    indexSlice.offset += blockCount * 28;

    // Store sparse entry (including type for fast getSeriesType lookups)
    SparseIndexEntry sparseEntry{
      .seriesId = seriesId,
      .fileOffset = entryStartOffset,
      .entrySize = entrySize,
      .seriesType = static_cast<TSMValueType>(type)
    };
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
  for(const auto& seriesId : seriesIds) {
    seriesBloomFilter.insert(seriesId.getRawData());
  }

  timestar::tsm_log.info("Loaded sparse index for {} (tier {}): {} series, bloom filter: {} bytes",
                     filePath, tierNum, sparseIndex.size(), seriesBloomFilter.size());
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
    co_return &cacheIt->second->second;
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
  auto entryBuf = co_await tsmFile.dma_read_exactly<uint8_t>(
    sparse.fileOffset,
    sparse.entrySize
  );

  // Step 5: Parse the full entry
  Slice entrySlice(entryBuf.get(), entryBuf.size());

  TSMIndexEntry fullEntry;

  // Parse series ID (verify)
  std::string seriesIdBytes = entrySlice.readString(16);
  fullEntry.seriesId = SeriesId128::fromBytes(seriesIdBytes);

  // Parse type and block count
  fullEntry.seriesType = (TSMValueType)entrySlice.read<uint8_t>();
  uint16_t blockCount = entrySlice.read<uint16_t>();

  // Parse all blocks
  fullEntry.indexBlocks.reserve(blockCount);
  for (uint16_t i = 0; i < blockCount; i++) {
    TSMIndexBlock block;
    block.minTime = entrySlice.read<uint64_t>();
    block.maxTime = entrySlice.read<uint64_t>();
    block.offset = entrySlice.read<uint64_t>();
    block.size = entrySlice.read<uint32_t>();
    fullEntry.indexBlocks.push_back(block);
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

  if (fullIndexCache.size() >= maxCacheEntries()) {
    // Evict least recently used entry (back of list)
    auto& lruEntry = lruList.back();
    fullIndexCache.erase(lruEntry.first);
    lruList.pop_back();
  }
  // Insert at front of LRU list (most recently used)
  lruList.emplace_front(seriesId, std::move(fullEntry));
  fullIndexCache[seriesId] = lruList.begin();

  timestar::tsm_log.trace("Loaded full index entry for series {} ({} blocks)",
                      seriesId.toHex(), blockCount);

  co_return &lruList.front().second;
}

// Bulk prefetch: identify cache misses and issue all DMA reads in parallel.
// Warms the full index cache so subsequent getFullIndexEntry() calls are cache hits.
seastar::future<> TSM::prefetchFullIndexEntries(const std::vector<SeriesId128>& seriesIds) {
  // Filter to only series that (a) pass bloom filter, (b) exist in sparse index,
  // and (c) are not already cached. This avoids unnecessary I/O.
  std::vector<SeriesId128> toFetch;
  for (const auto& seriesId : seriesIds) {
    if (fullIndexCache.find(seriesId) != fullIndexCache.end()) continue;
    if (!seriesBloomFilter.contains(seriesId.getRawData())) continue;
    if (sparseIndex.find(seriesId) == sparseIndex.end()) continue;
    toFetch.push_back(seriesId);
  }

  if (toFetch.empty()) co_return;

  // Issue all DMA reads in parallel via getFullIndexEntry (handles parse + caching)
  co_await seastar::parallel_for_each(toFetch, [this](const SeriesId128& seriesId) {
    return getFullIndexEntry(seriesId).discard_result();
  });
}

template <class T>
seastar::future<> TSM::readSeries(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<T> &results){
  // Get full index entry (uses bloom filter + sparse index + lazy load)
  auto* indexEntry = co_await getFullIndexEntry(seriesId);

  if (!indexEntry) {
    co_return; // Series not in this file
  }

  // Filter blocks by time range
  std::vector<TSMIndexBlock> blocksToScan;
  std::copy_if(
    indexEntry->indexBlocks.begin(),
    indexEntry->indexBlocks.end(),
    std::back_inserter(blocksToScan),
    [endTime, startTime](const TSMIndexBlock& indexBlock){
      return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
    }
  );

  // Pre-allocate a slot per block so each coroutine writes to its own index,
  // avoiding the data race where multiple coroutines push_back concurrently
  // on the shared results.blocks vector after co_await suspension points.
  std::vector<std::unique_ptr<TSMBlock<T>>> blockSlots(blocksToScan.size());
  size_t slotIdx = 0;

  co_await seastar::parallel_for_each(blocksToScan, [&] (TSMIndexBlock indexBlock) -> seastar::future<> {
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
      results.appendBlock(block);
    }
  }

  // parallel_for_each does not guarantee ordering, so sort blocks by start time
  results.sort();
}


std::optional<TSMValueType> TSM::getSeriesType(const SeriesId128& seriesId){
  // Check bloom filter first
  if (!seriesBloomFilter.contains(seriesId.getRawData())) {
    return {};
  }

  // Check full index cache (populated after getFullIndexEntry is called)
  auto it = fullIndexCache.find(seriesId);
  if(it != fullIndexCache.end()) {
    // Promote to front of LRU list on access
    lruList.splice(lruList.begin(), lruList, it->second);
    return it->second->second.seriesType;
  }

  // Fall back to sparse index which stores the type from the initial index parse
  auto sparseIt = sparseIndex.find(seriesId);
  if(sparseIt != sparseIndex.end()) {
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
seastar::future<std::unique_ptr<TSMBlock<T>>> TSM::readSingleBlock(
    const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime) {

  auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
  Slice blockSlice(blockBuf.get(), blockBuf.size());

  auto headerSlice = blockSlice.getSlice(BLOCK_HEADER_SIZE);
  uint8_t blockType = headerSlice.read<uint8_t>();
  uint32_t timestampSize = headerSlice.read<uint32_t>();
  uint32_t timestampBytes = headerSlice.read<uint32_t>();

  // Validate that the block's stored type matches the template parameter
  TSMValueType expectedType = getValueType<T>();
  if (static_cast<TSMValueType>(blockType) != expectedType) {
    throw std::runtime_error(
        "TSM block type mismatch: block contains type " + std::to_string(blockType) +
        " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
  }

  auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
  auto timestampsSlice = blockSlice.getSlice(timestampBytes);
  auto [nSkipped, nTimestamps] = IntegerEncoder::decode(timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime);
  size_t valueByteSize = indexBlock.size - timestampBytes - BLOCK_HEADER_SIZE;

  if constexpr (std::is_same_v<T, double>) {
    auto valuesSlice = blockSlice.getCompressedSlice(valueByteSize);
    FloatDecoder::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
  } else if constexpr (std::is_same_v<T, bool>) {
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    BoolEncoderRLE::decode(valuesSlice, nSkipped, nTimestamps, blockResults->values);
  } else if constexpr (std::is_same_v<T, std::string>) {
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
  } else if constexpr (std::is_same_v<T, int64_t>) {
    auto valuesSlice = blockSlice.getSlice(valueByteSize);
    std::vector<uint64_t> rawUint;
    IntegerEncoder::decode(valuesSlice, timestampSize, rawUint);
    blockResults->values.reserve(nTimestamps);
    for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < rawUint.size(); ++i) {
      blockResults->values.push_back(ZigZag::zigzagDecode(rawUint[i]));
    }
  }

  co_return blockResults;
}

template seastar::future<> TSM::readSeries<double>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<double> &results);
template seastar::future<> TSM::readSeries<bool>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<bool> &results);
template seastar::future<> TSM::readSeries<std::string>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<std::string> &results);
template seastar::future<> TSM::readSeries<int64_t>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<int64_t> &results);

// Phase 1.1: Template instantiations for readSingleBlock
template seastar::future<std::unique_ptr<TSMBlock<double>>> TSM::readSingleBlock<double>(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<bool>>> TSM::readSingleBlock<bool>(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<std::string>>> TSM::readSingleBlock<std::string>(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime);
template seastar::future<std::unique_ptr<TSMBlock<int64_t>>> TSM::readSingleBlock<int64_t>(const TSMIndexBlock &indexBlock, uint64_t startTime, uint64_t endTime);

// Phase 2: Read compressed block bytes directly without decompression
seastar::future<seastar::temporary_buffer<uint8_t>> TSM::readCompressedBlock(const TSMIndexBlock &indexBlock) {
    // Read the entire block as-is (already compressed)
    // No decompression - just read the raw bytes for zero-copy transfer
    auto blockBuf = co_await tsmFile.dma_read_exactly<uint8_t>(indexBlock.offset, indexBlock.size);
    co_return blockBuf;
}

seastar::future<> TSM::scheduleDelete() {
    // Close the file if it's open
    if (tsmFile) {
        co_await tsmFile.close();
    }

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
std::unique_ptr<TSMBlock<T>> TSM::decodeBlock(
    Slice& blockSlice,
    uint32_t blockSize,
    uint64_t startTime,
    uint64_t endTime
) {
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
        throw std::runtime_error(
            "TSM block type mismatch: block contains type " + std::to_string(blockType) +
            " but reader expects type " + std::to_string(static_cast<uint8_t>(expectedType)));
    }

    // Defensive check: ensure timestampBytes is reasonable
    if (timestampBytes > blockSize - BLOCK_HEADER_SIZE) {
        timestar::tsm_log.error("Timestamp bytes {} exceeds block size {} - {}", timestampBytes, blockSize, BLOCK_HEADER_SIZE);
        return nullptr;
    }

    // Decode timestamps with time-range filtering
    auto blockResults = std::make_unique<TSMBlock<T>>(timestampSize);
    auto timestampsSlice = blockSlice.getSlice(timestampBytes);
    auto [nSkipped, nTimestamps] = IntegerEncoder::decode(
        timestampsSlice, timestampSize, blockResults->timestamps, startTime, endTime
    );

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
        StringEncoder::decode(valuesSlice, timestampSize, nSkipped, nTimestamps, blockResults->values);
    } else if constexpr (std::is_same_v<T, int64_t>) {
        auto valuesSlice = blockSlice.getSlice(valueByteSize);
        std::vector<uint64_t> rawUint;
        IntegerEncoder::decode(valuesSlice, timestampSize, rawUint);
        blockResults->values.reserve(nTimestamps);
        for (size_t i = nSkipped; i < nSkipped + nTimestamps && i < rawUint.size(); ++i) {
            blockResults->values.push_back(ZigZag::zigzagDecode(rawUint[i]));
        }
    }

    return blockResults;
}

// Read multiple contiguous blocks with a single I/O operation
template <class T>
seastar::future<> TSM::readBlockBatch(
    const BlockBatch& batch,
    uint64_t startTime,
    uint64_t endTime,
    TSMResult<T>& results
) {
    // Performance tracking
    auto batchReadStart = std::chrono::high_resolution_clock::now();

    // Single large DMA read for entire batch
    auto batchBuf = co_await tsmFile.dma_read_exactly<uint8_t>(
        batch.startOffset,
        batch.totalSize
    );

    auto batchReadEnd = std::chrono::high_resolution_clock::now();
    auto readDuration = std::chrono::duration<double, std::milli>(batchReadEnd - batchReadStart);

    // Process each block from the batch buffer
    uint32_t bufferOffset = 0;
    size_t blocksDecoded = 0;
    for (const auto& block : batch.blocks) {
        // Create slice into batch buffer for this specific block
        Slice blockSlice(batchBuf.get() + bufferOffset, block.size);

        // Decode block using extracted logic
        auto blockResult = decodeBlock<T>(blockSlice, block.size, startTime, endTime);

        // Only append if block has data after time filtering
        if (blockResult && !blockResult->timestamps.empty()) {
            results.appendBlock(blockResult);
            blocksDecoded++;
        }

        bufferOffset += block.size;
    }

    timestar::tsm_log.trace("Batch read: {} blocks, {} bytes in {:.2f}ms ({:.1f} MB/s)",
                       batch.blocks.size(), batch.totalSize, readDuration.count(),
                       (batch.totalSize / 1024.0 / 1024.0) / (readDuration.count() / 1000.0));

    co_return;
}

// Optimized series read using block batching
template <class T>
seastar::future<> TSM::readSeriesBatched(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime,
    TSMResult<T>& results
) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return; // Series not in this file
    }

    // Step 1: Filter blocks by time range
    std::vector<TSMIndexBlock> blocksToScan;
    std::copy_if(
        indexEntry->indexBlocks.begin(),
        indexEntry->indexBlocks.end(),
        std::back_inserter(blocksToScan),
        [endTime, startTime](const TSMIndexBlock& indexBlock) {
            return indexBlock.minTime <= endTime && startTime <= indexBlock.maxTime;
        }
    );

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
        timestar::tsm_log.debug("Batch read efficiency: {:.1f} blocks/batch ({} blocks → {} batches)",
                           batchEfficiency, totalBlocks, totalBatches);
    }

    // Step 4: Read each batch (parallel execution for non-contiguous batches).
    // Each batch produces its own local TSMResult to avoid the data race where
    // multiple coroutines call results.appendBlock() concurrently after co_await
    // suspension points in parallel_for_each.
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

    // Merge all batch results into the output (single-threaded, safe)
    for (auto& batchResult : batchResults) {
        for (auto& block : batchResult.blocks) {
            results.appendBlock(block);
        }
    }

    // parallel_for_each does not guarantee ordering, so sort blocks by start time
    results.sort();

    co_return;
}

// Pushdown aggregation: decode float blocks and fold directly into a
// BlockAggregator, bypassing TSMResult/QueryResult materialisation.
// Handles tombstone filtering inline.  Returns the number of points folded.
seastar::future<size_t> TSM::aggregateSeries(
    const SeriesId128& seriesId,
    uint64_t startTime,
    uint64_t endTime,
    timestar::BlockAggregator& aggregator
) {
    // Get full index entry (uses bloom filter + sparse index + lazy load)
    auto* indexEntry = co_await getFullIndexEntry(seriesId);
    if (!indexEntry) {
        co_return 0;
    }

    // Only float series can be pushdown-aggregated
    if (indexEntry->seriesType != TSMValueType::Float) {
        co_return 0;
    }

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
    co_await seastar::parallel_for_each(batches, [&](const BlockBatch& batch) -> seastar::future<> {
        // Single DMA read for the entire batch
        auto batchBuf = co_await tsmFile.dma_read_exactly<uint8_t>(
            batch.startOffset, batch.totalSize);

        uint32_t bufferOffset = 0;
        for (const auto& block : batch.blocks) {
            Slice blockSlice(batchBuf.get() + bufferOffset, block.size);
            auto blockResult = decodeBlock<double>(blockSlice, block.size, startTime, endTime);

            if (blockResult && !blockResult->timestamps.empty()) {
                const auto& ts = blockResult->timestamps;
                const auto& vals = blockResult->values;

                if (!hasTombstoneRanges) {
                    // Fast path: no tombstones — fold entire block at once
                    aggregator.addPoints(ts, vals);
                    totalPoints += ts.size();
                } else {
                    // Filter tombstoned points while folding
                    for (size_t i = 0; i < ts.size(); ++i) {
                        uint64_t t = ts[i];
                        auto rangeIt = std::upper_bound(
                            tombstoneRanges.begin(), tombstoneRanges.end(),
                            std::make_pair(t, std::numeric_limits<uint64_t>::max()));
                        bool isTombstoned = false;
                        if (rangeIt != tombstoneRanges.begin()) {
                            --rangeIt;
                            if (t >= rangeIt->first && t <= rangeIt->second) {
                                isTombstoned = true;
                            }
                        }
                        if (!isTombstoned) {
                            aggregator.addPoint(t, vals[i]);
                            totalPoints++;
                        }
                    }
                }
            }

            bufferOffset += block.size;
        }
    });

    co_return totalPoints;
}

// Template instantiations for batched reads
template seastar::future<> TSM::readSeriesBatched<double>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<double>& results);
template seastar::future<> TSM::readSeriesBatched<bool>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<bool>& results);
template seastar::future<> TSM::readSeriesBatched<std::string>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readSeriesBatched<int64_t>(const SeriesId128& seriesId, uint64_t startTime, uint64_t endTime, TSMResult<int64_t>& results);

template seastar::future<> TSM::readBlockBatch<double>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime, TSMResult<double>& results);
template seastar::future<> TSM::readBlockBatch<bool>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime, TSMResult<bool>& results);
template seastar::future<> TSM::readBlockBatch<std::string>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime, TSMResult<std::string>& results);
template seastar::future<> TSM::readBlockBatch<int64_t>(const BlockBatch& batch, uint64_t startTime, uint64_t endTime, TSMResult<int64_t>& results);

template std::unique_ptr<TSMBlock<double>> TSM::decodeBlock<double>(Slice& blockSlice, uint32_t blockSize, uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<bool>> TSM::decodeBlock<bool>(Slice& blockSlice, uint32_t blockSize, uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<std::string>> TSM::decodeBlock<std::string>(Slice& blockSlice, uint32_t blockSize, uint64_t startTime, uint64_t endTime);
template std::unique_ptr<TSMBlock<int64_t>> TSM::decodeBlock<int64_t>(Slice& blockSlice, uint32_t blockSize, uint64_t startTime, uint64_t endTime);

