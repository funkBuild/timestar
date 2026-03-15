#include "sstable.hpp"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <zstd.h>

#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <unistd.h>

namespace timestar::index {

// --- Helper: encode/decode fixed integers ---

static void encodeFixed32(char* buf, uint32_t v) {
    buf[0] = static_cast<char>(v & 0xff);
    buf[1] = static_cast<char>((v >> 8) & 0xff);
    buf[2] = static_cast<char>((v >> 16) & 0xff);
    buf[3] = static_cast<char>((v >> 24) & 0xff);
}

static void encodeFixed64(char* buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf[i] = static_cast<char>((v >> (i * 8)) & 0xff);
    }
}

static uint32_t decodeFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<uint8_t>(p[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
}

static uint64_t decodeFixed64(const char* p) {
    uint64_t result = 0;
    for (int i = 0; i < 8; ++i) {
        result |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
    }
    return result;
}

// --- SSTableWriter (Step 3: streaming writes) ---

seastar::future<SSTableWriter> SSTableWriter::create(std::string filename, int blockSize, int bloomBitsPerKey,
                                                       int compressionLevel) {
    SSTableWriter writer;
    writer.filename_ = filename;
    writer.blockSize_ = blockSize;
    writer.compressionLevel_ = compressionLevel;
    writer.bloom_ = BloomFilter(bloomBitsPerKey);
    writer.currentBlock_ = BlockBuilder(16);

    // Step 3: Open file at creation time for streaming writes
    std::string_view filenameView{filename};
    writer.file_ = co_await seastar::open_file_dma(
        filenameView, seastar::open_flags::wo | seastar::open_flags::create | seastar::open_flags::truncate);
    writer.dmaAlign_ = writer.file_.disk_write_dma_alignment();
    writer.fileOpen_ = true;

    co_return std::move(writer);
}

void SSTableWriter::add(std::string_view key, std::string_view value) {
    if (entryCount_ == 0) {
        firstKey_.assign(key.data(), key.size());
    }
    lastKey_.assign(key.data(), key.size());

    if (currentBlock_.empty()) {
        currentBlockFirstKey_.assign(key.data(), key.size());
    }

    bloom_.addKey(key);
    currentBlock_.add(key, value);
    ++entryCount_;

    // Flush block to in-memory buffer when it exceeds the target size
    if (currentBlock_.currentSize() >= static_cast<size_t>(blockSize_)) {
        flushBlock();
    }
}

seastar::future<> SSTableWriter::flushPending() {
    co_await maybeStreamFlush();
}

void SSTableWriter::flushBlock() {
    if (currentBlock_.empty()) return;

    auto rawBlock = currentBlock_.finish();

    // Compress with zstd (level configured at create time: 1=fast for L0, 3=better for L1+)
    size_t maxCompressed = ZSTD_compressBound(rawBlock.size());
    std::string compressed(maxCompressed, '\0');
    size_t compressedSize = ZSTD_compress(compressed.data(), maxCompressed,
                                           rawBlock.data(), rawBlock.size(), compressionLevel_);
    if (ZSTD_isError(compressedSize)) {
        throw std::runtime_error(std::string("SSTable zstd compression failed: ") + ZSTD_getErrorName(compressedSize));
    }
    compressed.resize(compressedSize);

    // Prepend uncompressed size as fixed32 so reader can allocate decompression buffer
    char sizeBuf[4];
    encodeFixed32(sizeBuf, static_cast<uint32_t>(rawBlock.size()));

    // Record index entry (offset is within the logical data stream)
    IndexEntry entry;
    entry.firstKey = std::move(currentBlockFirstKey_);
    entry.offset = fileOffset_;
    entry.size = static_cast<uint32_t>(4 + compressed.size());
    index_.push_back(std::move(entry));

    // Append to write buffer
    pendingData_.append(sizeBuf, 4);
    pendingData_.append(compressed);
    fileOffset_ += 4 + compressed.size();

    currentBlock_.reset();
}

seastar::future<> SSTableWriter::maybeStreamFlush() {
    if (pendingData_.size() >= STREAM_FLUSH_THRESHOLD) {
        co_await streamFlush();
    }
}

seastar::future<> SSTableWriter::streamFlush() {
    if (pendingData_.empty() || !fileOpen_) co_return;

    // Round down to DMA alignment boundary — write only complete aligned chunks.
    // Remaining bytes stay in pendingData_ for the next flush or finish().
    size_t chunkSize = pendingData_.size() & ~(dmaAlign_ - 1);
    if (chunkSize == 0) co_return;

    auto buf = seastar::temporary_buffer<char>::aligned(dmaAlign_, chunkSize);
    std::memcpy(buf.get_write(), pendingData_.data(), chunkSize);

    size_t written = 0;
    while (written < chunkSize) {
        auto n = co_await file_.dma_write(diskOffset_ + written, buf.get() + written, chunkSize - written);
        if (n == 0) throw std::runtime_error("SSTable dma_write returned 0");
        written += n;
    }

    diskOffset_ += chunkSize;

    // Remove flushed bytes from the front of the buffer
    pendingData_.erase(0, chunkSize);
}

seastar::future<SSTableMetadata> SSTableWriter::finish() {
    // Flush any remaining entries to buffer
    flushBlock();

    // Build bloom filter and append to buffer
    bloom_.build();
    std::string bloomData;
    bloom_.serializeTo(bloomData);
    uint64_t bloomOffset = fileOffset_;
    pendingData_.append(bloomData);
    fileOffset_ += bloomData.size();

    // Build index block and append to buffer
    uint64_t indexOffset = fileOffset_;
    {
        char buf[4];
        encodeFixed32(buf, static_cast<uint32_t>(index_.size()));
        pendingData_.append(buf, 4);
        fileOffset_ += 4;

        for (const auto& entry : index_) {
            encodeFixed32(buf, static_cast<uint32_t>(entry.firstKey.size()));
            pendingData_.append(buf, 4);
            pendingData_.append(entry.firstKey);

            char buf8[8];
            encodeFixed64(buf8, entry.offset);
            pendingData_.append(buf8, 8);

            encodeFixed32(buf, entry.size);
            pendingData_.append(buf, 4);

            fileOffset_ += 4 + entry.firstKey.size() + 8 + 4;
        }
    }
    uint64_t indexSize = fileOffset_ - indexOffset;

    // Build footer and append to buffer
    char footer[SSTABLE_FOOTER_SIZE];
    encodeFixed64(footer + 0, bloomOffset);
    encodeFixed64(footer + 8, bloomData.size());
    encodeFixed64(footer + 16, indexOffset);
    encodeFixed64(footer + 24, indexSize);
    encodeFixed64(footer + 32, entryCount_);
    encodeFixed32(footer + 40, SSTABLE_MAGIC);
    encodeFixed32(footer + 44, SSTABLE_VERSION);
    pendingData_.append(footer, SSTABLE_FOOTER_SIZE);
    fileOffset_ += SSTABLE_FOOTER_SIZE;

    if (fileOffset_ == 0 || !fileOpen_) {
        if (fileOpen_) {
            co_await file_.close();
            fileOpen_ = false;
        }
        co_return SSTableMetadata{0, 0, 0, "", ""};
    }

    // Final DMA write of remaining buffer (bloom + index + footer + any leftover data blocks)
    std::exception_ptr err;
    try {
        const size_t dataSize = pendingData_.size();
        const size_t paddedSize = (diskOffset_ + dataSize + dmaAlign_ - 1) & ~(dmaAlign_ - 1);
        const size_t writeSize = paddedSize - diskOffset_;

        auto buf = seastar::temporary_buffer<char>::aligned(dmaAlign_, writeSize);
        std::memset(buf.get_write(), 0, writeSize);
        std::memcpy(buf.get_write(), pendingData_.data(), dataSize);

        size_t written = 0;
        while (written < writeSize) {
            auto n = co_await file_.dma_write(diskOffset_ + written, buf.get() + written, writeSize - written);
            if (n == 0) throw std::runtime_error("SSTable dma_write returned 0");
            written += n;
        }

        if (paddedSize != fileOffset_) {
            co_await file_.truncate(fileOffset_);
        }
        co_await file_.flush();
    } catch (...) {
        err = std::current_exception();
    }

    co_await file_.close();
    fileOpen_ = false;
    if (err) std::rethrow_exception(err);

    // Free the buffer
    pendingData_.clear();
    pendingData_.shrink_to_fit();

    SSTableMetadata meta;
    meta.entryCount = entryCount_;
    meta.fileSize = fileOffset_;
    meta.minKey = firstKey_;
    meta.maxKey = lastKey_;
    co_return meta;
}

seastar::future<> SSTableWriter::abort() {
    if (fileOpen_) {
        co_await file_.close();
        fileOpen_ = false;
    }
}

// --- SSTableReader (Step 1: lazy block loading, Step 2: block cache) ---

seastar::future<std::unique_ptr<SSTableReader>> SSTableReader::open(std::string filename, BlockCache* cache) {
    auto reader = std::unique_ptr<SSTableReader>(new SSTableReader());
    reader->filename_ = filename;
    reader->blockCache_ = cache;
    reader->cacheId_ = cache ? BlockCache::nextCacheId() : 0;

    auto file = co_await seastar::open_file_dma(filename, seastar::open_flags::ro);

    auto fileSize = co_await file.size();
    if (fileSize < SSTABLE_FOOTER_SIZE) {
        co_await file.close();
        throw std::runtime_error("SSTable file too small: " + filename);
    }

    // Read only the metadata tail (footer + bloom + index), NOT the entire file.
    // Data blocks are read from disk on demand via the block cache.
    // This bounds memory to O(bloom_size + index_size) per SSTable instead of O(file_size).

    // First, read the footer to learn bloom/index offsets.
    const size_t dmaAlign = file.disk_read_dma_alignment();
    const size_t footerReadOffset = fileSize - SSTABLE_FOOTER_SIZE;
    const size_t alignedFooterOffset = footerReadOffset & ~(dmaAlign - 1);
    const size_t footerReadSize = ((fileSize - alignedFooterOffset) + dmaAlign - 1) & ~(dmaAlign - 1);
    auto footerBuf = seastar::temporary_buffer<char>::aligned(dmaAlign, footerReadSize);
    size_t footerRead = 0;
    while (footerRead < footerReadSize) {
        auto n = co_await file.dma_read(alignedFooterOffset + footerRead,
                                                   footerBuf.get_write() + footerRead,
                                                   footerReadSize - footerRead);
        if (n == 0) break;
        footerRead += n;
    }

    const char* fp = footerBuf.get() + (footerReadOffset - alignedFooterOffset);
    uint64_t bloomOffset = decodeFixed64(fp + 0);
    uint64_t bloomSize = decodeFixed64(fp + 8);
    uint64_t indexOffset = decodeFixed64(fp + 16);
    uint64_t indexSize = decodeFixed64(fp + 24);
    uint64_t entryCount = decodeFixed64(fp + 32);
    uint32_t magic = decodeFixed32(fp + 40);
    uint32_t version = decodeFixed32(fp + 44);

    if (magic != SSTABLE_MAGIC) {
        co_await file.close();
        throw std::runtime_error("SSTable bad magic: " + filename);
    }
    if (version != SSTABLE_VERSION) {
        co_await file.close();
        throw std::runtime_error("SSTable unsupported version: " + filename);
    }

    reader->metadata_.entryCount = entryCount;
    reader->metadata_.fileSize = fileSize;

    // Read bloom + index in a single DMA read (they're contiguous at the end of the file).
    uint64_t metaStart = std::min(bloomOffset, indexOffset);
    uint64_t metaEnd = fileSize - SSTABLE_FOOTER_SIZE;
    if (metaStart < metaEnd) {
        size_t metaSize = metaEnd - metaStart;
        size_t alignedMetaOffset = metaStart & ~(dmaAlign - 1);
        size_t alignedMetaSize = ((metaEnd - alignedMetaOffset) + dmaAlign - 1) & ~(dmaAlign - 1);
        auto metaBuf = seastar::temporary_buffer<char>::aligned(dmaAlign, alignedMetaSize);
        size_t metaRead = 0;
        while (metaRead < alignedMetaSize) {
            auto n = co_await file.dma_read(alignedMetaOffset + metaRead,
                                                       metaBuf.get_write() + metaRead,
                                                       alignedMetaSize - metaRead);
            if (n == 0) break;
            metaRead += n;
        }

        const char* metaBase = metaBuf.get() + (metaStart - alignedMetaOffset);

        // Parse bloom filter
        if (bloomSize > 0 && bloomOffset >= metaStart && bloomOffset + bloomSize <= metaEnd) {
            reader->bloom_ = BloomFilter::deserializeFrom(
                std::string_view(metaBase + (bloomOffset - metaStart), bloomSize));
        } else {
            reader->bloom_ = BloomFilter::createNull();
        }

        // Parse index block
        if (indexSize > 0 && indexOffset >= metaStart && indexOffset + indexSize <= metaEnd) {
            const char* ip = metaBase + (indexOffset - metaStart);
            const char* iend = ip + indexSize;

            if (ip + 4 > iend) throw std::runtime_error("SSTable index truncated");
            uint32_t numEntries = decodeFixed32(ip);
            ip += 4;

            reader->index_.reserve(numEntries);
            for (uint32_t i = 0; i < numEntries; ++i) {
                if (ip + 4 > iend) throw std::runtime_error("SSTable index entry truncated");
                uint32_t keyLen = decodeFixed32(ip);
                ip += 4;

                if (ip + keyLen > iend) throw std::runtime_error("SSTable index key truncated");
                std::string firstKey(ip, keyLen);
                ip += keyLen;

                if (ip + 12 > iend) throw std::runtime_error("SSTable index offset truncated");
                uint64_t offset = decodeFixed64(ip);
                ip += 8;
                uint32_t size = decodeFixed32(ip);
                ip += 4;

                reader->index_.push_back({std::move(firstKey), offset, size});
            }

            if (!reader->index_.empty()) {
                reader->metadata_.minKey = reader->index_.front().firstKey;
            }
        }
    } else {
        reader->bloom_ = BloomFilter::createNull();
    }

    // Close the Seastar DMA file handle (metadata reading is done).
    // Open a plain POSIX fd for synchronous pread() of data blocks on cache miss.
    co_await file.close();

    reader->readFd_ = ::open(filename.c_str(), O_RDONLY);
    if (reader->readFd_ < 0) {
        throw std::runtime_error("SSTable failed to open POSIX fd: " + filename);
    }
    // Hint to kernel: random access pattern (block reads are scattered by key hash).
    // This disables overly aggressive readahead that wastes I/O bandwidth.
    ::posix_fadvise(reader->readFd_, 0, 0, POSIX_FADV_RANDOM);

    co_return std::move(reader);
}

std::string SSTableReader::decompressBlock(size_t blockIndex) const {
    if (blockIndex >= index_.size()) {
        throw std::runtime_error("SSTable block index out of range");
    }
    const auto& entry = index_[blockIndex];
    if (entry.size < 4) {
        throw std::runtime_error("SSTable block too small");
    }

    // Read the compressed block from disk using pread() (synchronous).
    // The block cache ensures this path is only hit on cache misses.
    std::string compressedBuf(entry.size, '\0');
    ssize_t bytesRead = ::pread(readFd_, compressedBuf.data(), entry.size, static_cast<off_t>(entry.offset));
    if (bytesRead < 0 || static_cast<size_t>(bytesRead) < entry.size) {
        throw std::runtime_error("SSTable pread failed for block " + std::to_string(blockIndex) +
                                 " in " + filename_);
    }

    uint32_t uncompressedSize = decodeFixed32(compressedBuf.data());
    std::string result(uncompressedSize, '\0');
    size_t decompSize = ZSTD_decompress(result.data(), uncompressedSize,
                                         compressedBuf.data() + 4, entry.size - 4);
    if (ZSTD_isError(decompSize)) {
        throw std::runtime_error(std::string("SSTable zstd decompression failed: ") + ZSTD_getErrorName(decompSize));
    }
    return result;
}

size_t SSTableReader::findBlock(std::string_view key) const {
    size_t lo = 0, hi = index_.size();
    while (lo + 1 < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (index_[mid].firstKey <= std::string_view(key)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    return lo;
}

const std::string& SSTableReader::getDecompressedBlock(size_t idx, std::string& fallback) const {
    if (blockCache_) {
        auto* cached = blockCache_->get(cacheId_, idx);
        if (cached) return *cached;
        fallback = decompressBlock(idx);
        return blockCache_->put(cacheId_, idx, fallback);
    }
    fallback = decompressBlock(idx);
    return fallback;
}

std::optional<std::string> SSTableReader::get(std::string_view key) {
    // Bloom filter check
    if (!bloom_.mayContain(key)) {
        return std::nullopt;
    }

    if (index_.empty()) {
        return std::nullopt;
    }

    // Binary search index to find the right data block.
    size_t lo = findBlock(key);

    // Step 1: Decompress block on demand (Step 2: via block cache if available)
    std::string localBlock;
    const auto& blockData = getDecompressedBlock(lo, localBlock);
    BlockReader blockReader(blockData);
    if (!blockReader.valid()) {
        return std::nullopt;
    }

    auto it = blockReader.newIterator();
    it.seek(key);
    if (it.valid() && it.key() == key) {
        return std::string(it.value());
    }

    return std::nullopt;
}

bool SSTableReader::contains(std::string_view key) {
    // Bloom filter check
    if (!bloom_.mayContain(key)) {
        return false;
    }

    if (index_.empty()) {
        return false;
    }

    size_t lo = findBlock(key);

    std::string localBlock;
    const auto& blockData = getDecompressedBlock(lo, localBlock);
    BlockReader blockReader(blockData);
    if (!blockReader.valid()) {
        return false;
    }

    auto it = blockReader.newIterator();
    it.seek(key);
    return it.valid() && it.key() == key;
}

seastar::future<> SSTableReader::close() {
    // Step 2: Proactively evict this reader's blocks from the shared cache
    if (blockCache_ && cacheId_ != 0) {
        blockCache_->evict(cacheId_);
    }
    // Close the POSIX fd used for synchronous block reads
    if (readFd_ >= 0) {
        ::close(readFd_);
        readFd_ = -1;
    }
    return seastar::make_ready_future<>();
}

// --- SSTableReader::Iterator ---

SSTableReader::Iterator::Iterator(SSTableReader* reader) : reader_(reader) {}

void SSTableReader::Iterator::loadBlock(size_t idx) {
    if (idx >= reader_->index_.size()) {
        valid_ = false;
        return;
    }
    blockIndex_ = idx;
    // Step 1: Decompress block on demand into owned blockData_.
    // Step 2: Check block cache first if available.
    if (reader_->blockCache_) {
        auto* cached = reader_->blockCache_->get(reader_->cacheId_, idx);
        if (cached) {
            // Cache hit — create BlockReader on cached data (stable pointer)
            blockReader_ = std::make_unique<BlockReader>(*cached);
        } else {
            // Cache miss — decompress and cache
            blockData_ = reader_->decompressBlock(idx);
            const auto& ref = reader_->blockCache_->put(reader_->cacheId_, idx, blockData_);
            blockReader_ = std::make_unique<BlockReader>(ref);
        }
    } else {
        // No cache — decompress into owned blockData_
        blockData_ = reader_->decompressBlock(idx);
        blockReader_ = std::make_unique<BlockReader>(blockData_);
    }
    if (!blockReader_->valid()) {
        valid_ = false;
        return;
    }
    blockIter_ = blockReader_->newIterator();
}

void SSTableReader::Iterator::updateFromBlockIter() {
    if (blockIter_.valid()) {
        key_.assign(blockIter_.key().data(), blockIter_.key().size());
        value_ = blockIter_.value();
        valid_ = true;
    } else {
        valid_ = false;
    }
}

void SSTableReader::Iterator::seekToFirst() {
    if (reader_->index_.empty()) {
        valid_ = false;
        return;
    }
    loadBlock(0);
    if (blockReader_ && blockReader_->valid()) {
        blockIter_.seekToFirst();
        updateFromBlockIter();
    }
}

void SSTableReader::Iterator::seek(std::string_view target) {
    if (reader_->index_.empty()) {
        valid_ = false;
        return;
    }

    // Binary search for the right block
    size_t lo = reader_->findBlock(target);

    loadBlock(lo);
    if (!blockReader_ || !blockReader_->valid()) {
        valid_ = false;
        return;
    }
    blockIter_.seek(target);
    updateFromBlockIter();

    // If not found in this block, try the next block
    if (!valid_ && lo + 1 < reader_->index_.size()) {
        loadBlock(lo + 1);
        if (blockReader_ && blockReader_->valid()) {
            blockIter_.seekToFirst();
            updateFromBlockIter();
        }
    }
}

void SSTableReader::Iterator::next() {
    if (!valid_) return;

    blockIter_.next();
    updateFromBlockIter();

    // If current block exhausted, move to next block
    if (!valid_ && blockIndex_ + 1 < reader_->index_.size()) {
        loadBlock(blockIndex_ + 1);
        if (blockReader_ && blockReader_->valid()) {
            blockIter_.seekToFirst();
            updateFromBlockIter();
        }
    }
}

std::unique_ptr<SSTableReader::Iterator> SSTableReader::newIterator() {
    return std::make_unique<Iterator>(this);
}

}  // namespace timestar::index
