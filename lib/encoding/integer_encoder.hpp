#ifndef INTEGER_ENCODER_H_INCLUDED
#define INTEGER_ENCODER_H_INCLUDED

#include <vector>
#include <cstdint>
#include <span>
#include <string>
#include "aligned_buffer.hpp"
#include "slice_buffer.hpp"

/**
 * Main IntegerEncoder class that automatically selects the best implementation
 * based on CPU capabilities.
 */
class IntegerEncoder {
public:
    /**
     * Encode uint64_t values using the best available implementation.
     * Accepts std::span for zero-copy sub-range encoding; std::vector
     * converts implicitly.
     */
    static AlignedBuffer encode(std::span<const uint64_t> values);

    /**
     * Encode directly into an existing AlignedBuffer (zero-copy for WAL path).
     * Eliminates the intermediate buffer allocation and copy.
     * Returns the number of bytes written to the target buffer.
     */
    static size_t encodeInto(std::span<const uint64_t> values, AlignedBuffer &target);

    /**
     * Decode compressed data back to uint64_t values using the best available implementation
     */
    static std::pair<size_t, size_t> decode(Slice &encoded, unsigned int timestampSize,
                                           std::vector<uint64_t> &values,
                                           uint64_t startTime = 0,
                                           uint64_t maxTime = UINT64_MAX);

    /**
     * Get the name of the encoder implementation being used
     */
    static std::string getImplementationName();

    /**
     * Check which implementations are available
     */
    static bool hasAVX512();
    static bool hasAVX2();

    /**
     * Force a specific implementation (for testing)
     */
    enum Implementation {
        AUTO,     // Automatically select best
        BASIC,    // Force basic implementation
        SIMD,     // Force SIMD AVX2
        AVX512,   // Force AVX-512
        FFOR      // Force FFOR (Frame-of-Reference) encoding
    };

    static void setImplementation(Implementation impl);

private:
    static Implementation selectBestImplementation();
    static thread_local Implementation s_forced_impl;
};

#endif // INTEGER_ENCODER_H_INCLUDED