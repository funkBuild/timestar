#include "bloom_filter.hpp"

#include "bloom_filter_simd.hpp"

#include <xxhash.h>

#include <algorithm>
#include <cassert>
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
    assert(!built_ && "addKey() called after build() — filter is immutable");
    uint64_t h = XXH3_64bits(key.data(), key.size());
    hashes_.push_back(h);
    ++numKeys_;
}

// Fast modulo replacement: maps a 32-bit value uniformly to [0, n) without division.
// Uses the "fastrange" trick with __uint128_t to avoid overflow when n > UINT32_MAX.
static inline size_t fastRange(uint32_t x, size_t n) {
    return static_cast<size_t>(static_cast<__uint128_t>(x) * n >> 32);
}

void BloomFilter::build() {
    if (numKeys_ == 0) {
        filter_.clear();
        built_ = true;
        return;
    }

    // Total bits = numKeys * bitsPerKey, rounded up to 64-bit words, minimum 64 bits
    size_t numBits = std::max(static_cast<size_t>(64), numKeys_ * bitsPerKey_);
    // Round up to next 64-bit word boundary for word-based storage
    size_t numWords = (numBits + 63) / 64;
    numBits = numWords * 64;
    filterBytes_ = numWords * 8;  // Serialize all uint64_t words as bytes

    filter_.assign(numWords, 0);

    // SIMD path: use Highway-accelerated batch build when the filter is large
    // enough to amortize SIMD dispatch overhead. The SIMD kernel computes all k
    // probe positions per hash using vectorized multiply+shift (replacing k
    // sequential __uint128_t multiplications with SIMD lanes).
    //
    // Note: the SIMD fastRange uses 64-bit multiply (not __uint128_t), which is
    // exact when numBits < 2^32 (i.e., < 512M keys at 10 bits/key). For larger
    // filters, fall back to scalar __uint128_t path. In practice, NativeIndex
    // measurement bloom filters have at most ~100K keys.
    if (filterBytes_ >= simd::kBloomSimdThreshold && numBits <= 0xFFFFFFFFULL) {
        simd::bloomBuildBatch(filter_.data(), numBits, k_, hashes_.data(), hashes_.size());
    } else {
        for (uint64_t h : hashes_) {
            uint32_t h1 = static_cast<uint32_t>(h);
            uint32_t h2 = static_cast<uint32_t>(h >> 32);

            for (int i = 0; i < k_; ++i) {
                size_t bitPos = fastRange(h1 + static_cast<uint32_t>(i) * h2, numBits);
                setBit(bitPos);
            }
        }
    }

    hashes_.clear();
    hashes_.shrink_to_fit();
    built_ = true;
}

bool BloomFilter::mayContain(std::string_view key) const {
    if (isNull_)
        return true;
    if (filter_.empty())
        return false;

    size_t numBits = filter_.size() * 64;

    uint64_t h = XXH3_64bits(key.data(), key.size());
    uint32_t h1 = static_cast<uint32_t>(h);
    uint32_t h2 = static_cast<uint32_t>(h >> 32);

    // SIMD path: gather all k probe bytes and test with single SIMD AND+compare.
    // This replaces k sequential getBit() calls (each with a conditional branch
    // that can mispredict on the final probe) with a branchless SIMD test.
    // The threshold check ensures we only use SIMD for non-trivial filters
    // where the dispatch overhead is amortized.
    if (filterBytes_ >= simd::kBloomSimdThreshold) {
        return simd::bloomMayContain(filter_.data(), numBits, k_, h1, h2);
    }

    // Scalar path for tiny filters (< 64 bytes)
    for (int i = 0; i < k_; ++i) {
        size_t bitPos = fastRange(h1 + static_cast<uint32_t>(i) * h2, numBits);
        if (!getBit(bitPos)) {
            return false;
        }
    }
    return true;
}

uint64_t BloomFilter::bitCount() const {
    if (filter_.empty())
        return 0;
    return simd::bloomPopcount(filter_.data(), filter_.size());
}

double BloomFilter::density() const {
    size_t numBits = filter_.size() * 64;
    if (numBits == 0)
        return 0.0;
    return static_cast<double>(bitCount()) / static_cast<double>(numBits);
}

std::pair<uint32_t, uint32_t> BloomFilter::hashKey(std::string_view key) {
    uint64_t h = XXH3_64bits(key.data(), key.size());
    return {static_cast<uint32_t>(h), static_cast<uint32_t>(h >> 32)};
}

void BloomFilter::serializeTo(std::string& output) const {
    // Format: [k (1 byte)] [filter_size (4 bytes LE)] [filter_data]
    // Serialize as bytes for on-disk compatibility even though internal storage is uint64_t
    output.push_back(static_cast<char>(k_));

    uint32_t size = static_cast<uint32_t>(filterBytes_);
    output.push_back(static_cast<char>(size & 0xff));
    output.push_back(static_cast<char>((size >> 8) & 0xff));
    output.push_back(static_cast<char>((size >> 16) & 0xff));
    output.push_back(static_cast<char>((size >> 24) & 0xff));

    output.append(reinterpret_cast<const char*>(filter_.data()), filterBytes_);
}

BloomFilter BloomFilter::deserializeFrom(std::string_view data) {
    // Need at least 5 bytes: k(1) + size(4)
    if (data.size() < 5) {
        return createNull();
    }

    int k = static_cast<uint8_t>(data[0]);
    if (k == 0 || k > 30) {
        return createNull();  // Invalid k from corrupted data
    }
    uint32_t size = static_cast<uint32_t>(static_cast<uint8_t>(data[1])) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[3])) << 16) |
                    (static_cast<uint32_t>(static_cast<uint8_t>(data[4])) << 24);

    if (data.size() < 5 + size) {
        return createNull();
    }

    BloomFilter bf;
    bf.k_ = k;
    bf.filterBytes_ = size;
    // Load bytes into word-based storage (round up to whole uint64_t)
    size_t numWords = (size + 7) / 8;
    bf.filter_.assign(numWords, 0);
    std::memcpy(bf.filter_.data(), data.data() + 5, size);
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
