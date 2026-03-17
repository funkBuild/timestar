#pragma once

#include <xxhash.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>

namespace timestar::index {

// HyperLogLog cardinality estimator (Flajolet et al., 2007).
// 14-bit precision: 16384 registers = 16KB per sketch, ~0.8% standard error.
// Uses xxHash64 for hashing.
class HyperLogLog {
public:
    static constexpr int PRECISION = 14;
    static constexpr size_t NUM_REGISTERS = 1u << PRECISION;
    static constexpr size_t SERIALIZED_SIZE = NUM_REGISTERS;  // 16KB

    HyperLogLog() { registers_.fill(0); }

    // Add a uint32_t value (e.g. LocalId).
    void add(uint32_t value) {
        uint64_t h = XXH3_64bits(&value, sizeof(value));
        addHash(h);
    }

    // Add an arbitrary key (hashed internally).
    void add(std::string_view key) {
        uint64_t h = XXH3_64bits(key.data(), key.size());
        addHash(h);
    }

    // Cardinality estimate.
    double estimate() const {
        // Harmonic mean of 2^(-register[j])
        double sum = 0.0;
        int zeros = 0;
        for (size_t i = 0; i < NUM_REGISTERS; ++i) {
            sum += 1.0 / static_cast<double>(1ULL << registers_[i]);
            if (registers_[i] == 0) ++zeros;
        }

        // alpha_m constant for m = 16384
        constexpr double alpha = 0.7213 / (1.0 + 1.079 / static_cast<double>(NUM_REGISTERS));
        double raw = alpha * static_cast<double>(NUM_REGISTERS) * static_cast<double>(NUM_REGISTERS) / sum;

        // Small range correction (linear counting)
        if (raw <= 2.5 * static_cast<double>(NUM_REGISTERS) && zeros > 0) {
            return static_cast<double>(NUM_REGISTERS) * std::log(static_cast<double>(NUM_REGISTERS) / static_cast<double>(zeros));
        }

        return raw;
    }

    // Merge another HLL sketch (union = max of registers).
    void merge(const HyperLogLog& other) {
        for (size_t i = 0; i < NUM_REGISTERS; ++i) {
            registers_[i] = std::max(registers_[i], other.registers_[i]);
        }
    }

    // Serialize: append 16KB register array to output.
    void serialize(std::string& out) const {
        out.append(reinterpret_cast<const char*>(registers_.data()), SERIALIZED_SIZE);
    }

    // Deserialize from exactly SERIALIZED_SIZE bytes.
    static HyperLogLog deserialize(std::string_view data) {
        HyperLogLog hll;
        if (data.size() >= SERIALIZED_SIZE) {
            std::memcpy(hll.registers_.data(), data.data(), SERIALIZED_SIZE);
        }
        return hll;
    }

    // True if all registers are zero (no data added).
    bool empty() const {
        for (size_t i = 0; i < NUM_REGISTERS; ++i) {
            if (registers_[i] != 0) return false;
        }
        return true;
    }

private:
    std::array<uint8_t, NUM_REGISTERS> registers_;

    void addHash(uint64_t h) {
        // Upper PRECISION bits select the register
        uint32_t idx = static_cast<uint32_t>(h >> (64 - PRECISION));
        // Remaining bits determine the leading-zero count + 1
        uint64_t remaining = (h << PRECISION) | (1ULL << (PRECISION - 1));  // Guard bit
        uint8_t rank = static_cast<uint8_t>(std::countl_zero(remaining) + 1);
        registers_[idx] = std::max(registers_[idx], rank);
    }
};

}  // namespace timestar::index
