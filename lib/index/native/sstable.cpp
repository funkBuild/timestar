#include "sstable.hpp"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>
#include <snappy.h>

#include <cstring>
#include <stdexcept>

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

// --- SSTableWriter ---

seastar::future<SSTableWriter> SSTableWriter::create(std::string filename, int blockSize, int bloomBitsPerKey) {
    SSTableWriter writer;
    writer.filename_ = filename;
    writer.blockSize_ = blockSize;
    writer.bloom_ = BloomFilter(bloomBitsPerKey);
    writer.currentBlock_ = BlockBuilder(16);

    writer.file_ = co_await seastar::open_file_dma(filename, seastar::open_flags::wo | seastar::open_flags::create |
                                                                  seastar::open_flags::truncate);
    co_return std::move(writer);
}

seastar::future<> SSTableWriter::add(std::string_view key, std::string_view value) {
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

    // Flush block when it exceeds the target size
    if (currentBlock_.currentSize() >= static_cast<size_t>(blockSize_)) {
        co_await flushBlock();
    }
}

seastar::future<> SSTableWriter::flushBlock() {
    if (currentBlock_.empty()) co_return;

    auto rawBlock = currentBlock_.finish();

    // Compress with Snappy
    std::string compressed;
    snappy::Compress(rawBlock.data(), rawBlock.size(), &compressed);

    // Prepend uncompressed size as fixed32 so reader can allocate decompression buffer
    std::string blockData;
    blockData.resize(4);
    encodeFixed32(blockData.data(), static_cast<uint32_t>(rawBlock.size()));
    blockData.append(compressed);

    // Record index entry
    IndexEntry entry;
    entry.firstKey = std::move(currentBlockFirstKey_);
    entry.offset = fileOffset_;
    entry.size = static_cast<uint32_t>(blockData.size());
    index_.push_back(std::move(entry));

    // DMA write requires aligned buffer — use temporary_buffer
    auto alignedBuf = seastar::temporary_buffer<char>::aligned(4096, blockData.size());
    std::memcpy(alignedBuf.get_write(), blockData.data(), blockData.size());
    co_await file_.dma_write(fileOffset_, alignedBuf.get(), alignedBuf.size());
    fileOffset_ += blockData.size();

    currentBlock_.reset();
}

seastar::future<SSTableMetadata> SSTableWriter::finish() {
    // Flush any remaining entries
    co_await flushBlock();

    // Build and write bloom filter
    bloom_.build();
    std::string bloomData;
    bloom_.serializeTo(bloomData);
    uint64_t bloomOffset = fileOffset_;

    if (!bloomData.empty()) {
        auto bloomBuf = seastar::temporary_buffer<char>::aligned(4096, bloomData.size());
        std::memcpy(bloomBuf.get_write(), bloomData.data(), bloomData.size());
        co_await file_.dma_write(fileOffset_, bloomBuf.get(), bloomBuf.size());
        fileOffset_ += bloomData.size();
    }

    // Write index block: [entry_count (4 bytes)] then per entry:
    //   [key_len (4 bytes)] [key] [offset (8 bytes)] [size (4 bytes)]
    uint64_t indexOffset = fileOffset_;
    std::string indexData;
    {
        char buf[4];
        encodeFixed32(buf, static_cast<uint32_t>(index_.size()));
        indexData.append(buf, 4);

        for (const auto& entry : index_) {
            encodeFixed32(buf, static_cast<uint32_t>(entry.firstKey.size()));
            indexData.append(buf, 4);
            indexData.append(entry.firstKey);

            char buf8[8];
            encodeFixed64(buf8, entry.offset);
            indexData.append(buf8, 8);

            encodeFixed32(buf, entry.size);
            indexData.append(buf, 4);
        }
    }

    if (!indexData.empty()) {
        auto indexBuf = seastar::temporary_buffer<char>::aligned(4096, indexData.size());
        std::memcpy(indexBuf.get_write(), indexData.data(), indexData.size());
        co_await file_.dma_write(fileOffset_, indexBuf.get(), indexBuf.size());
        fileOffset_ += indexData.size();
    }

    // Write footer (48 bytes)
    char footer[SSTABLE_FOOTER_SIZE];
    encodeFixed64(footer + 0, bloomOffset);
    encodeFixed64(footer + 8, bloomData.size());
    encodeFixed64(footer + 16, indexOffset);
    encodeFixed64(footer + 24, indexData.size());
    encodeFixed64(footer + 32, entryCount_);
    encodeFixed32(footer + 40, SSTABLE_MAGIC);
    encodeFixed32(footer + 44, SSTABLE_VERSION);

    auto footerBuf = seastar::temporary_buffer<char>::aligned(4096, SSTABLE_FOOTER_SIZE);
    std::memcpy(footerBuf.get_write(), footer, SSTABLE_FOOTER_SIZE);
    co_await file_.dma_write(fileOffset_, footerBuf.get(), footerBuf.size());
    fileOffset_ += SSTABLE_FOOTER_SIZE;

    // Truncate file to exact size and flush
    co_await file_.truncate(fileOffset_);
    co_await file_.flush();
    co_await file_.close();

    SSTableMetadata meta;
    meta.entryCount = entryCount_;
    meta.fileSize = fileOffset_;
    meta.minKey = firstKey_;
    meta.maxKey = lastKey_;
    co_return meta;
}

seastar::future<> SSTableWriter::abort() {
    co_await file_.close();
    co_await seastar::remove_file(filename_);
}

// --- SSTableReader ---

seastar::future<std::unique_ptr<SSTableReader>> SSTableReader::open(std::string filename) {
    auto reader = std::unique_ptr<SSTableReader>(new SSTableReader());
    reader->filename_ = filename;
    reader->file_ = co_await seastar::open_file_dma(filename, seastar::open_flags::ro);

    auto fileSize = co_await reader->file_.size();
    if (fileSize < SSTABLE_FOOTER_SIZE) {
        throw std::runtime_error("SSTable file too small: " + filename);
    }

    // Read footer
    auto footerBuf = seastar::temporary_buffer<char>::aligned(4096, SSTABLE_FOOTER_SIZE);
    auto footerRead =
        co_await reader->file_.dma_read(fileSize - SSTABLE_FOOTER_SIZE, footerBuf.get_write(), footerBuf.size());
    if (footerRead < SSTABLE_FOOTER_SIZE) {
        throw std::runtime_error("SSTable footer read incomplete: " + filename);
    }

    const char* fp = footerBuf.get();
    uint64_t bloomOffset = decodeFixed64(fp + 0);
    uint64_t bloomSize = decodeFixed64(fp + 8);
    uint64_t indexOffset = decodeFixed64(fp + 16);
    uint64_t indexSize = decodeFixed64(fp + 24);
    uint64_t entryCount = decodeFixed64(fp + 32);
    uint32_t magic = decodeFixed32(fp + 40);
    uint32_t version = decodeFixed32(fp + 44);

    if (magic != SSTABLE_MAGIC) {
        throw std::runtime_error("SSTable bad magic: " + filename);
    }
    if (version != SSTABLE_VERSION) {
        throw std::runtime_error("SSTable unsupported version: " + filename);
    }

    reader->metadata_.entryCount = entryCount;
    reader->metadata_.fileSize = fileSize;

    // Read bloom filter
    if (bloomSize > 0) {
        auto bloomBuf = seastar::temporary_buffer<char>::aligned(4096, bloomSize);
        co_await reader->file_.dma_read(bloomOffset, bloomBuf.get_write(), bloomBuf.size());
        reader->bloom_ = BloomFilter::deserializeFrom(std::string_view(bloomBuf.get(), bloomSize));
    } else {
        reader->bloom_ = BloomFilter::createNull();
    }

    // Read index block
    if (indexSize > 0) {
        auto indexBuf = seastar::temporary_buffer<char>::aligned(4096, indexSize);
        co_await reader->file_.dma_read(indexOffset, indexBuf.get_write(), indexBuf.size());

        const char* ip = indexBuf.get();
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
            // Max key is the last key in the last block, but we don't store that.
            // We leave maxKey empty for now.
        }
    }

    co_return std::move(reader);
}

seastar::future<std::string> SSTableReader::readBlock(size_t blockIndex) {
    if (blockIndex >= index_.size()) {
        throw std::runtime_error("SSTable block index out of range");
    }

    const auto& entry = index_[blockIndex];
    auto buf = seastar::temporary_buffer<char>::aligned(4096, entry.size);
    auto bytesRead = co_await file_.dma_read(entry.offset, buf.get_write(), buf.size());
    if (bytesRead < entry.size) {
        throw std::runtime_error("SSTable block read incomplete");
    }

    // First 4 bytes: uncompressed size
    if (entry.size < 4) {
        throw std::runtime_error("SSTable block too small");
    }
    uint32_t uncompressedSize = decodeFixed32(buf.get());

    // Decompress
    std::string decompressed;
    decompressed.resize(uncompressedSize);
    if (!snappy::RawUncompress(buf.get() + 4, entry.size - 4, decompressed.data())) {
        throw std::runtime_error("SSTable Snappy decompression failed");
    }

    co_return decompressed;
}

seastar::future<std::optional<std::string>> SSTableReader::get(std::string_view key) {
    // Bloom filter check
    if (!bloom_.mayContain(key)) {
        co_return std::nullopt;
    }

    if (index_.empty()) {
        co_return std::nullopt;
    }

    // Binary search index to find the right data block.
    // Find the last block whose firstKey <= key.
    size_t lo = 0, hi = index_.size();
    while (lo + 1 < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (index_[mid].firstKey <= std::string_view(key)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    // Read and search the block
    auto blockData = co_await readBlock(lo);
    BlockReader reader(blockData);
    if (!reader.valid()) {
        co_return std::nullopt;
    }

    auto it = reader.newIterator();
    it.seek(key);
    if (it.valid() && it.key() == key) {
        co_return std::string(it.value());
    }

    co_return std::nullopt;
}

seastar::future<> SSTableReader::close() {
    co_await file_.close();
}

// --- SSTableReader::Iterator ---

SSTableReader::Iterator::Iterator(SSTableReader* reader) : reader_(reader) {}

seastar::future<> SSTableReader::Iterator::loadBlock(size_t idx) {
    if (idx >= reader_->index_.size()) {
        valid_ = false;
        co_return;
    }
    blockIndex_ = idx;
    blockData_ = co_await reader_->readBlock(idx);
    blockReader_ = std::make_unique<BlockReader>(blockData_);
    if (!blockReader_->valid()) {
        valid_ = false;
        co_return;
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

seastar::future<> SSTableReader::Iterator::seekToFirst() {
    if (reader_->index_.empty()) {
        valid_ = false;
        co_return;
    }
    co_await loadBlock(0);
    if (blockReader_ && blockReader_->valid()) {
        blockIter_.seekToFirst();
        updateFromBlockIter();
    }
}

seastar::future<> SSTableReader::Iterator::seek(std::string_view target) {
    if (reader_->index_.empty()) {
        valid_ = false;
        co_return;
    }

    // Binary search for the right block
    size_t lo = 0, hi = reader_->index_.size();
    while (lo + 1 < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (reader_->index_[mid].firstKey <= std::string_view(target)) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    co_await loadBlock(lo);
    if (!blockReader_ || !blockReader_->valid()) {
        valid_ = false;
        co_return;
    }
    blockIter_.seek(target);
    updateFromBlockIter();

    // If not found in this block, try the next block
    if (!valid_ && lo + 1 < reader_->index_.size()) {
        co_await loadBlock(lo + 1);
        if (blockReader_ && blockReader_->valid()) {
            blockIter_.seekToFirst();
            updateFromBlockIter();
        }
    }
}

seastar::future<> SSTableReader::Iterator::next() {
    if (!valid_) co_return;

    blockIter_.next();
    updateFromBlockIter();

    // If current block exhausted, move to next block
    if (!valid_ && blockIndex_ + 1 < reader_->index_.size()) {
        co_await loadBlock(blockIndex_ + 1);
        if (blockReader_ && blockReader_->valid()) {
            blockIter_.seekToFirst();
            updateFromBlockIter();
        }
    }
}

seastar::future<std::unique_ptr<SSTableReader::Iterator>> SSTableReader::newIterator() {
    co_return std::make_unique<Iterator>(this);
}

}  // namespace timestar::index
