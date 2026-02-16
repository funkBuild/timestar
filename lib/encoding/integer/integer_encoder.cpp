#include "integer_encoder.hpp"
#include "../zigzag.hpp"
#include "../simple16.hpp"
#include <iostream>
#include <limits>

// Basic implementation without SIMD - optimized with loop unrolling

AlignedBuffer IntegerEncoderBasic::encode(const std::vector<uint64_t> &values) {
    if (values.empty()) {
        return AlignedBuffer();
    }

    // Pre-allocate with estimated size for better performance
    std::vector<uint64_t> encoded;
    encoded.reserve(values.size() + 2);

    uint64_t start_value = values[0];
    encoded.push_back(start_value);

    if (values.size() == 1) {
        return Simple16::encode(encoded);
    }

    int64_t delta = static_cast<int64_t>(values[1]) - static_cast<int64_t>(values[0]);
    uint64_t first_delta = ZigZag::zigzagEncode(delta);
    encoded.push_back(first_delta);

    // Optimized loop with unrolling for better performance
    const size_t size = values.size();
    size_t i = 2;

    // Process 4 values at a time when possible
    for (; i + 3 < size; i += 4) {
        int64_t d0 = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i-1])) - (static_cast<int64_t>(values[i-1]) - static_cast<int64_t>(values[i-2]));
        int64_t d1 = (static_cast<int64_t>(values[i+1]) - static_cast<int64_t>(values[i])) - (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i-1]));
        int64_t d2 = (static_cast<int64_t>(values[i+2]) - static_cast<int64_t>(values[i+1])) - (static_cast<int64_t>(values[i+1]) - static_cast<int64_t>(values[i]));
        int64_t d3 = (static_cast<int64_t>(values[i+3]) - static_cast<int64_t>(values[i+2])) - (static_cast<int64_t>(values[i+2]) - static_cast<int64_t>(values[i+1]));

        encoded.push_back(ZigZag::zigzagEncode(d0));
        encoded.push_back(ZigZag::zigzagEncode(d1));
        encoded.push_back(ZigZag::zigzagEncode(d2));
        encoded.push_back(ZigZag::zigzagEncode(d3));
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t D = (static_cast<int64_t>(values[i]) - static_cast<int64_t>(values[i-1])) - (static_cast<int64_t>(values[i-1]) - static_cast<int64_t>(values[i-2]));
        uint64_t encD = ZigZag::zigzagEncode(D);
        encoded.push_back(encD);
    }

    return Simple16::encode(encoded);
}

std::pair<size_t, size_t> IntegerEncoderBasic::decode(Slice &encoded, unsigned int timestampSize,
                                                      std::vector<uint64_t> &values,
                                                      uint64_t minTime, uint64_t maxTime) {
    // Optimized memory allocation
    const size_t current_size = values.size();
    const size_t estimated_new = timestampSize;

    if (values.capacity() < current_size + estimated_new) {
        values.reserve(current_size + estimated_new + (estimated_new >> 2)); // 25% extra
    }

    std::vector<uint64_t> deltaValues = Simple16::decode(encoded, timestampSize);

    size_t nSkipped = 0, nAdded = 0;

    if (deltaValues.empty()) {
        return {nSkipped, nAdded};
    }

    uint64_t last_decoded = deltaValues[0];

    // First value
    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    if (deltaValues.size() < 2) {
        return {nSkipped, nAdded};
    }

    // Second value
    int64_t delta = ZigZag::zigzagDecode(deltaValues[1]);
    last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

    if (last_decoded < minTime) {
        nSkipped++;
    } else if (last_decoded <= maxTime) {
        values.push_back(last_decoded);
        nAdded++;
    } else {
        return {nSkipped, nAdded};
    }

    // Main loop - unroll by 4 for better performance
    const size_t size = deltaValues.size();
    size_t i = 2;

    // Process 4 values at a time
    for (; i + 3 < size; i += 4) {
        // Decode all 4 delta-of-deltas
        int64_t dd0 = ZigZag::zigzagDecode(deltaValues[i]);
        int64_t dd1 = ZigZag::zigzagDecode(deltaValues[i+1]);
        int64_t dd2 = ZigZag::zigzagDecode(deltaValues[i+2]);
        int64_t dd3 = ZigZag::zigzagDecode(deltaValues[i+3]);

        // Reconstruct values
        delta += dd0;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd1;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd2;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }

        delta += dd3;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);
        if (last_decoded < minTime) {
            nSkipped++;
        } else if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        } else {
            values.push_back(last_decoded);
            nAdded++;
        }
    }

    // Handle remaining values
    for (; i < size; i++) {
        int64_t encD = ZigZag::zigzagDecode(deltaValues[i]);
        delta += encD;
        last_decoded = static_cast<uint64_t>(static_cast<int64_t>(last_decoded) + delta);

        if (last_decoded < minTime) {
            nSkipped++;
            continue;
        }

        if (last_decoded > maxTime) {
            return {nSkipped, nAdded};
        }

        values.push_back(last_decoded);
        nAdded++;
    }

    return {nSkipped, nAdded};
}