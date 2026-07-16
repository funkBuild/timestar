// StorageBloomFilter — minimal bloom filter for TSM series lookup.
//
// Replaces the third-party Arash Partow Open Bloom Filter with a clean
// implementation using xxHash for high-quality hashing and double-hashing
// (Kirsch-Mitzenmacker) for probe generation.
//
// The bloom filter is NOT serialized to disk; it is rebuilt in memory from
// the sparse index each time a TSM file is opened.  Therefore there is no
// backward-compatibility constraint on the bit layout.
//
// NOTE: This filter is distinct from lib/index/native/bloom_filter.{hpp,cpp}
// (timestar::index::BloomFilter), which is the NativeIndex SSTable bloom with
// SIMD batch-build/probe kernels and a serialized on-disk format. The two are
// intentionally separate: this one is header-only, unserialized, and rebuilt
// per TSM file open; the index one has durability and hash-once (mayContainHash)
// API constraints. A former SIMD companion (bloom_filter_simd.*) providing
// batch-contains, popcount, and bitwise-merge kernels was removed in Jul 2026:
// production only ever used insert()/contains()/size().
//
// Why individual insert()/contains() are NOT SIMD-accelerated:
//   Each call hashes one key (xxHash already uses internal SIMD) then probes
//   k random bit positions scattered across the table. Setting/checking
//   individual bits at computed positions is inherently scalar — there is no
//   data parallelism across probe positions within a single key. The cost is
//   dominated by the hash computation (which xxHash already optimizes with
//   intrinsics) and k random byte loads (which are cache-limited, not
//   compute-limited). SIMD gather instructions could load k bytes in one op,
//   but the positions are arbitrary and the k is small (typically 7), so the
//   gather setup cost exceeds the benefit.

#pragma once

#include <xxhash.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

// --------------------------------------------------------------------------
// bloom_parameters — configuration for bloom_filter construction
// --------------------------------------------------------------------------
class bloom_parameters {
public:
    // The approximate number of elements to be inserted.
    unsigned long long projected_element_count = 10000;

    // Target false positive probability (e.g. 0.001 for 0.1%).
    double false_positive_probability = 1.0 / 10000.0;

    // Seed for hash generation (deterministic across runs).
    unsigned long long random_seed = 0xA5A5A5A55A5A5A5AULL;

    struct optimal_parameters_t {
        unsigned int number_of_hashes = 0;
        unsigned long long table_size = 0;  // in bits
    };
    optimal_parameters_t optimal_parameters;

    // Compute optimal k (hash count) and m (bit count) from the standard
    // Bloom filter formulas:
    //   m = -n * ln(p) / (ln(2)^2)
    //   k = (m / n) * ln(2)
    bool compute_optimal_parameters() {
        if (projected_element_count == 0 || false_positive_probability <= 0.0 || false_positive_probability >= 1.0) {
            return false;
        }

        const double n = static_cast<double>(projected_element_count);
        const double ln2 = 0.6931471805599453;
        const double ln2sq = ln2 * ln2;

        // Optimal number of bits
        double m = -(n * std::log(false_positive_probability)) / ln2sq;
        // Round up to a multiple of 8 (byte boundary)
        auto mInt = static_cast<unsigned long long>(std::ceil(m));
        mInt += (8 - (mInt % 8)) % 8;
        // Enforce a minimum of 64 bits (8 bytes)
        if (mInt < 64)
            mInt = 64;

        // Optimal number of hash functions
        double k = (static_cast<double>(mInt) / n) * ln2;
        auto kInt = static_cast<unsigned int>(std::round(k));
        if (kInt < 1)
            kInt = 1;

        optimal_parameters.number_of_hashes = kInt;
        optimal_parameters.table_size = mInt;
        return true;
    }
};

// --------------------------------------------------------------------------
// bloom_filter — the filter itself
// --------------------------------------------------------------------------
class bloom_filter {
public:
    using cell_type = unsigned char;

    // Default-constructed filter: empty, contains() always returns false.
    bloom_filter() = default;

    // Construct from pre-computed parameters.
    explicit bloom_filter(const bloom_parameters& p)
        : tableSizeBits_(p.optimal_parameters.table_size),
          numHashes_(p.optimal_parameters.number_of_hashes),
          seed_(p.random_seed),
          bitTable_(tableSizeBits_ / 8, 0) {}

    // Insert a raw byte key.
    void insert(const unsigned char* key, std::size_t length) {
        auto [h1, h2] = computeHashes(key, length);
        for (unsigned i = 0; i < numHashes_; ++i) {
            std::size_t pos = (h1 + static_cast<uint64_t>(i) * h2) % tableSizeBits_;
            bitTable_[pos / 8] |= static_cast<unsigned char>(1u << (pos % 8));
        }
        ++insertedCount_;
    }

    // Insert from std::string.
    void insert(const std::string& key) { insert(reinterpret_cast<const unsigned char*>(key.data()), key.size()); }

    // Insert from string_view.
    void insert(std::string_view key) { insert(reinterpret_cast<const unsigned char*>(key.data()), key.size()); }

    // Insert from const char* + length.
    void insert(const char* data, std::size_t length) { insert(reinterpret_cast<const unsigned char*>(data), length); }

    // Insert a POD type by its raw byte representation.
    template <typename T>
        requires(std::is_trivially_copyable_v<T> && !std::is_same_v<std::decay_t<T>, std::string> &&
                 !std::is_same_v<std::decay_t<T>, std::string_view> && !std::is_pointer_v<std::decay_t<T>>)
    void insert(const T& t) {
        insert(reinterpret_cast<const unsigned char*>(&t), sizeof(T));
    }

    // Query: returns true if the key MIGHT be in the set.
    [[nodiscard]] bool contains(const unsigned char* key, std::size_t length) const {
        if (tableSizeBits_ == 0)
            return false;
        auto [h1, h2] = computeHashes(key, length);
        for (unsigned i = 0; i < numHashes_; ++i) {
            std::size_t pos = (h1 + static_cast<uint64_t>(i) * h2) % tableSizeBits_;
            if ((bitTable_[pos / 8] & (1u << (pos % 8))) == 0) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] bool contains(const std::string& key) const {
        return contains(reinterpret_cast<const unsigned char*>(key.data()), key.size());
    }

    [[nodiscard]] bool contains(std::string_view key) const {
        return contains(reinterpret_cast<const unsigned char*>(key.data()), key.size());
    }

    [[nodiscard]] bool contains(const char* data, std::size_t length) const {
        return contains(reinterpret_cast<const unsigned char*>(data), length);
    }

    template <typename T>
        requires(std::is_trivially_copyable_v<T> && !std::is_same_v<std::decay_t<T>, std::string> &&
                 !std::is_same_v<std::decay_t<T>, std::string_view> && !std::is_pointer_v<std::decay_t<T>>)
    [[nodiscard]] bool contains(const T& t) const {
        return contains(reinterpret_cast<const unsigned char*>(&t), sizeof(T));
    }

    // Clear all bits, resetting the filter to empty.
    void clear() {
        std::fill(bitTable_.begin(), bitTable_.end(), static_cast<unsigned char>(0));
        insertedCount_ = 0;
    }

    // Size of the bit table in bits.
    [[nodiscard]] unsigned long long size() const { return tableSizeBits_; }

    // Number of elements inserted.
    [[nodiscard]] unsigned long long element_count() const { return insertedCount_; }

    // Effective false positive probability given current fill.
    [[nodiscard]] double effective_fpp() const {
        if (tableSizeBits_ == 0)
            return 1.0;
        return std::pow(1.0 - std::exp(-1.0 * numHashes_ * insertedCount_ / static_cast<double>(tableSizeBits_)),
                        static_cast<double>(numHashes_));
    }

    // Raw table access (for tests and potential serialization).
    [[nodiscard]] const cell_type* table() const { return bitTable_.data(); }
    [[nodiscard]] std::size_t table_size_bytes() const { return bitTable_.size(); }

    // Number of hash functions.
    [[nodiscard]] unsigned int hash_count() const { return numHashes_; }

    // Seed used for hash computation.
    [[nodiscard]] unsigned long long seed() const { return seed_; }

    // Equality (same configuration + same bits).
    [[nodiscard]] bool operator==(const bloom_filter& o) const {
        return tableSizeBits_ == o.tableSizeBits_ && numHashes_ == o.numHashes_ && seed_ == o.seed_ &&
               bitTable_ == o.bitTable_;
    }
    [[nodiscard]] bool operator!=(const bloom_filter& o) const { return !(*this == o); }

    // Returns true if the filter is uninitialised (zero-size).
    [[nodiscard]] bool operator!() const { return tableSizeBits_ == 0; }

private:
    // Compute two independent 64-bit hashes using XXH3.
    // These feed the Kirsch-Mitzenmacker double-hashing scheme:
    //   probe(i) = (h1 + i * h2) % m
    std::pair<uint64_t, uint64_t> computeHashes(const unsigned char* key, std::size_t length) const {
        XXH128_hash_t h = XXH3_128bits_withSeed(key, length, static_cast<uint64_t>(seed_));
        return {h.low64, h.high64 | 1u};  // h2 is forced odd to ensure full period
    }

    unsigned long long tableSizeBits_ = 0;
    unsigned int numHashes_ = 0;
    unsigned long long seed_ = 0;
    unsigned long long insertedCount_ = 0;
    std::vector<unsigned char> bitTable_;
};
