#include "bloom_filter.hpp"

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <cstring>

namespace timestar::index {

BloomFilter::BloomFilter(int bits_per_key) : bitsPerKey_(bits_per_key) {
    // k = bits_per_key * ln(2) ≈ bits_per_key * 0.6931
    k_ = std::max(1, static_cast<int>(std::round(bits_per_key * 0.6931)));
    // Cap k at 30 to avoid excessive probing
    k_ = std::min(k_, 30);
}

void BloomFilter::addKey(std::string_view key) {
    // Store the 64-bit hash for later use in build()
    uint64_t h = XXH3_64bits(key.data(), key.size());
    hashes_.push_back(h);
    ++numKeys_;
}

void BloomFilter::build() {
    if (numKeys_ == 0) {
        filter_.clear();
        built_ = true;
        return;
    }

    // Total bits = numKeys * bitsPerKey, rounded up to whole bytes, minimum 64 bits
    size_t numBits = std::max(static_cast<size_t>(64), numKeys_ * bitsPerKey_);
    // Round up to next byte boundary
    size_t numBytes = (numBits + 7) / 8;
    numBits = numBytes * 8;

    filter_.assign(numBytes, 0);

    for (uint64_t h : hashes_) {
        // Split the 64-bit hash into two 32-bit values for double hashing
        uint32_t h1 = static_cast<uint32_t>(h);
        uint32_t h2 = static_cast<uint32_t>(h >> 32);

        for (int i = 0; i < k_; ++i) {
            size_t bitPos = (h1 + static_cast<uint64_t>(i) * h2) % numBits;
            setBit(bitPos);
        }
    }

    // Free the hash storage — no longer needed after build
    hashes_.clear();
    hashes_.shrink_to_fit();
    built_ = true;
}

bool BloomFilter::mayContain(std::string_view key) const {
    if (isNull_) return true;
    if (filter_.empty()) return false;

    size_t numBits = filter_.size() * 8;

    uint64_t h = XXH3_64bits(key.data(), key.size());
    uint32_t h1 = static_cast<uint32_t>(h);
    uint32_t h2 = static_cast<uint32_t>(h >> 32);

    for (int i = 0; i < k_; ++i) {
        size_t bitPos = (h1 + static_cast<uint64_t>(i) * h2) % numBits;
        if (!getBit(bitPos)) {
            return false;
        }
    }
    return true;
}

std::pair<uint32_t, uint32_t> BloomFilter::hashKey(std::string_view key) {
    uint64_t h = XXH3_64bits(key.data(), key.size());
    return {static_cast<uint32_t>(h), static_cast<uint32_t>(h >> 32)};
}

void BloomFilter::serializeTo(std::string& output) const {
    // Format: [k (1 byte)] [filter_size (4 bytes LE)] [filter_data]
    output.push_back(static_cast<char>(k_));

    uint32_t size = static_cast<uint32_t>(filter_.size());
    output.push_back(static_cast<char>(size & 0xff));
    output.push_back(static_cast<char>((size >> 8) & 0xff));
    output.push_back(static_cast<char>((size >> 16) & 0xff));
    output.push_back(static_cast<char>((size >> 24) & 0xff));

    output.append(reinterpret_cast<const char*>(filter_.data()), filter_.size());
}

BloomFilter BloomFilter::deserializeFrom(std::string_view data) {
    // Need at least 5 bytes: k(1) + size(4)
    if (data.size() < 5) {
        return createNull();
    }

    int k = static_cast<uint8_t>(data[0]);
    uint32_t size = static_cast<uint32_t>(static_cast<uint8_t>(data[1])) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[3])) << 16) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[4])) << 24);

    if (data.size() < 5 + size) {
        return createNull();
    }

    BloomFilter bf;
    bf.k_ = k;
    bf.filter_.assign(reinterpret_cast<const uint8_t*>(data.data() + 5),
                      reinterpret_cast<const uint8_t*>(data.data() + 5 + size));
    bf.built_ = true;
    return bf;
}

BloomFilter BloomFilter::createNull() {
    BloomFilter bf;
    bf.isNull_ = true;
    bf.built_ = true;
    return bf;
}

}  // namespace timestar::index
