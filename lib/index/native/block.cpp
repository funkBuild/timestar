#include "block.hpp"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <stdexcept>

namespace timestar::index {

// --- Varint encoding (same format as LevelDB/protobuf) ---

void BlockBuilder::appendVarint32(std::string& buf, uint32_t value) {
    while (value >= 0x80) {
        buf.push_back(static_cast<char>(value | 0x80));
        value >>= 7;
    }
    buf.push_back(static_cast<char>(value));
}

// --- BlockBuilder ---

BlockBuilder::BlockBuilder(int restart_interval) : restartInterval_(restart_interval) {
    assert(restart_interval >= 1);
    restartOffsets_.push_back(0);  // First entry is always a restart point
}

void BlockBuilder::add(std::string_view key, std::string_view value) {
    assert(!key.empty());

    uint32_t shared = 0;
    if (entryCount_ % restartInterval_ == 0) {
        // Restart point: store full key
        restartOffsets_.push_back(static_cast<uint32_t>(buffer_.size()));
    } else {
        // Compute shared prefix with previous key
        size_t minLen = std::min(key.size(), lastKey_.size());
        while (shared < minLen && key[shared] == lastKey_[shared]) {
            ++shared;
        }
    }

    uint32_t unshared = static_cast<uint32_t>(key.size()) - shared;
    uint32_t valLen = static_cast<uint32_t>(value.size());

    // Encode: shared_prefix_len | unshared_key_len | value_len | key_suffix | value
    appendVarint32(buffer_, shared);
    appendVarint32(buffer_, unshared);
    appendVarint32(buffer_, valLen);
    buffer_.append(key.data() + shared, unshared);
    buffer_.append(value.data(), valLen);

    lastKey_.assign(key.data(), key.size());
    ++entryCount_;
}

std::string BlockBuilder::finish() {
    // Remove the initial dummy restart offset (0) if we have real entries
    // Actually the first restart is always at offset 0, which is correct.
    // But we added 0 in constructor AND added 0 again for first entry.
    // Fix: the constructor pushes 0, and the first add() also pushes buffer_.size()=0.
    // So we have a duplicate. Let's deduplicate.
    // Actually let's just not push in constructor and handle it properly:
    // Re-examine: constructor pushes 0, first add sees entryCount_==0, so entryCount_ % interval == 0,
    // and pushes buffer_.size() which is also 0. So we get [0, 0, ...].
    // Fix this by removing the constructor push and letting add() handle it.

    // Remove duplicate first restart if present
    if (restartOffsets_.size() >= 2 && restartOffsets_[0] == 0 && restartOffsets_[1] == 0) {
        restartOffsets_.erase(restartOffsets_.begin());
    }

    // Append restart offsets as fixed32 little-endian
    for (uint32_t offset : restartOffsets_) {
        char buf[4];
        buf[0] = static_cast<char>(offset & 0xff);
        buf[1] = static_cast<char>((offset >> 8) & 0xff);
        buf[2] = static_cast<char>((offset >> 16) & 0xff);
        buf[3] = static_cast<char>((offset >> 24) & 0xff);
        buffer_.append(buf, 4);
    }

    // Append restart count as fixed32
    uint32_t count = static_cast<uint32_t>(restartOffsets_.size());
    char buf[4];
    buf[0] = static_cast<char>(count & 0xff);
    buf[1] = static_cast<char>((count >> 8) & 0xff);
    buf[2] = static_cast<char>((count >> 16) & 0xff);
    buf[3] = static_cast<char>((count >> 24) & 0xff);
    buffer_.append(buf, 4);

    return std::move(buffer_);
}

size_t BlockBuilder::currentSize() const {
    // Entries + estimated restart footer
    return buffer_.size() + restartOffsets_.size() * sizeof(uint32_t) + sizeof(uint32_t);
}

void BlockBuilder::reset() {
    buffer_.clear();
    lastKey_.clear();
    restartOffsets_.clear();
    restartOffsets_.push_back(0);
    entryCount_ = 0;
}

// --- BlockReader ---

uint32_t BlockReader::decodeFixed32(const char* p) {
    return static_cast<uint32_t>(static_cast<uint8_t>(p[0])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24);
}

bool BlockReader::decodeVarint32(const char*& p, const char* limit, uint32_t& result) {
    result = 0;
    for (int shift = 0; shift < 35 && p < limit; shift += 7) {
        uint8_t byte = static_cast<uint8_t>(*p);
        ++p;
        result |= (static_cast<uint32_t>(byte & 0x7f)) << shift;
        if ((byte & 0x80) == 0) {
            return true;
        }
    }
    return false;
}

BlockReader::BlockReader(std::string_view data) : data_(data) {
    // Need at least 4 bytes for restart_count
    if (data.size() < sizeof(uint32_t)) {
        return;
    }

    // Read restart count from last 4 bytes
    restartCount_ = decodeFixed32(data.data() + data.size() - sizeof(uint32_t));

    // Validate: need restartCount * 4 bytes for offsets + 4 bytes for count
    size_t restartFooterSize = static_cast<size_t>(restartCount_) * sizeof(uint32_t) + sizeof(uint32_t);
    if (restartFooterSize > data.size()) {
        return;
    }

    dataSize_ = data.size() - restartFooterSize;
    restarts_ = reinterpret_cast<const uint32_t*>(data.data() + dataSize_);
    valid_ = true;
}

BlockReader::Iterator BlockReader::newIterator() const {
    return Iterator(this);
}

// --- BlockReader::Iterator ---

BlockReader::Iterator::Iterator(const BlockReader* reader) : reader_(reader) {}

bool BlockReader::Iterator::decodeEntry() {
    if (!reader_ || offset_ >= reader_->dataSize_) {
        valid_ = false;
        return false;
    }

    const char* p = reader_->data_.data() + offset_;
    const char* limit = reader_->data_.data() + reader_->dataSize_;

    uint32_t shared, unshared, valLen;
    if (!BlockReader::decodeVarint32(p, limit, shared) || !BlockReader::decodeVarint32(p, limit, unshared) ||
        !BlockReader::decodeVarint32(p, limit, valLen)) {
        valid_ = false;
        return false;
    }

    // Validate bounds
    if (static_cast<size_t>(p - reader_->data_.data()) + unshared + valLen > reader_->dataSize_) {
        valid_ = false;
        return false;
    }

    // Reconstruct key: keep shared prefix from previous key, append suffix
    if (shared > key_.size()) {
        valid_ = false;
        return false;
    }
    key_.resize(shared);
    key_.append(p, unshared);
    p += unshared;

    value_ = std::string_view(p, valLen);
    p += valLen;

    offset_ = static_cast<size_t>(p - reader_->data_.data());
    valid_ = true;
    return true;
}

void BlockReader::Iterator::seekToRestartPoint(size_t restartIndex) {
    if (!reader_ || restartIndex >= reader_->restartCount_) {
        valid_ = false;
        return;
    }
    offset_ = BlockReader::decodeFixed32(reinterpret_cast<const char*>(&reader_->restarts_[restartIndex]));
    key_.clear();  // Restart points always store the full key (shared=0)
    decodeEntry();
}

void BlockReader::Iterator::seekToFirst() {
    if (!reader_ || !reader_->valid_ || reader_->dataSize_ == 0) {
        valid_ = false;
        return;
    }
    offset_ = 0;
    key_.clear();
    decodeEntry();
}

void BlockReader::Iterator::next() {
    if (!valid_) return;
    decodeEntry();
}

void BlockReader::Iterator::seek(std::string_view target) {
    if (!reader_ || !reader_->valid_ || reader_->restartCount_ == 0) {
        valid_ = false;
        return;
    }

    // Binary search over restart points to find the last restart point
    // whose key is <= target.
    size_t lo = 0;
    size_t hi = reader_->restartCount_;

    while (lo + 1 < hi) {
        size_t mid = lo + (hi - lo) / 2;
        // Decode the key at restart point mid
        uint32_t restartOffset =
            BlockReader::decodeFixed32(reinterpret_cast<const char*>(&reader_->restarts_[mid]));
        const char* p = reader_->data_.data() + restartOffset;
        const char* limit = reader_->data_.data() + reader_->dataSize_;

        uint32_t shared, unshared, valLen;
        if (!BlockReader::decodeVarint32(p, limit, shared) || !BlockReader::decodeVarint32(p, limit, unshared) ||
            !BlockReader::decodeVarint32(p, limit, valLen)) {
            valid_ = false;
            return;
        }
        // At restart points, shared must be 0
        std::string_view restartKey(p, unshared);
        if (restartKey < target) {
            lo = mid;
        } else {
            hi = mid;
        }
    }

    // Position at the restart point and linear scan to find target
    seekToRestartPoint(lo);
    while (valid_ && key_ < target) {
        next();
    }
}

}  // namespace timestar::index
