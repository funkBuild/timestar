#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace timestar::index {

// A split bloom filter for SSTable point-lookup acceleration.
// Uses double-hashing with xxHash to generate k probe positions.
//
// Build phase: call addKey() for each key, then build() to finalize.
// Query phase: call mayContain() to test membership.
// Serialization: serializeTo()/deserializeFrom() for SSTable embedding.
class BloomFilter {
public:
    // bits_per_key: number of bits per key in the filter. Higher values
    // give lower false positive rates. Typical: 10-15.
    // k (number of hash functions) is derived as bits_per_key * ln(2) ≈ bits_per_key * 0.69.
    explicit BloomFilter(int bits_per_key = 10);

    // Build phase: add keys before calling build().
    void addKey(std::string_view key);

    // Finalize the filter. After this, addKey() must not be called.
    void build();

    // Query: returns true if the key might be in the set (false positives possible),
    // false if the key is definitely NOT in the set.
    bool mayContain(std::string_view key) const;

    // Number of keys added.
    size_t keyCount() const { return numKeys_; }

    // Size of the filter in bytes (after build).
    size_t filterSize() const { return filterBytes_; }

    // SIMD-accelerated popcount: total number of set bits in the filter.
    // Uses Highway PopulationCount on uint64_t lanes for maximum throughput.
    uint64_t bitCount() const;

    // Filter density: fraction of bits that are set (0.0 = empty, 1.0 = full).
    // Useful for diagnostics and estimating false positive rate.
    // Actual FPR ≈ density^k where k is the number of hash functions.
    double density() const;

    // Serialization: writes the filter to a string buffer.
    // Format: [k (1 byte)] [filter_size (4 bytes LE)] [filter_data]
    void serializeTo(std::string& output) const;

    // Deserialization: reconstructs a filter from serialized data.
    // Returns an empty filter on invalid data.
    static BloomFilter deserializeFrom(std::string_view data);

    // Create an empty/null filter that always returns true for mayContain().
    static BloomFilter createNull();

    bool isNull() const { return isNull_; }

private:
    int bitsPerKey_;
    int k_;  // Number of hash functions
    size_t numKeys_ = 0;
    std::vector<uint64_t> filter_;  // Word-based for faster bit ops
    size_t filterBytes_ = 0;        // Actual byte count (for serialization)
    std::vector<uint64_t> hashes_;  // Stored during build phase
    bool built_ = false;
    bool isNull_ = false;

    // Hash a key and return the two hash values used for double-hashing.
    static std::pair<uint32_t, uint32_t> hashKey(std::string_view key);

    // Probe positions using double hashing: h(i) = (h1 + i * h2) % numBits
    void setBit(size_t pos) { filter_[pos / 64] |= (1ULL << (pos % 64)); }
    bool getBit(size_t pos) const { return (filter_[pos / 64] & (1ULL << (pos % 64))) != 0; }
};

}  // namespace timestar::index
