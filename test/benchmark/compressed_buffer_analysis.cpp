#include "encoding/float_encoder.hpp"
#include "storage/compressed_buffer.hpp"

#include <chrono>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
#include <vector>

using namespace std::chrono;

// Profile the actual usage pattern from float encoder
void analyze_float_encoder_pattern() {
    std::cout << "\n=== Float Encoder Write Pattern Analysis ===" << std::endl;

    // Generate realistic float data
    std::vector<double> test_data;
    for (int i = 0; i < 10000; i++) {
        test_data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
    }

    // Track write patterns
    struct WriteOp {
        int bits;
        int count;
    };
    std::map<int, int> write_histogram;
    int total_writes = 0;
    int control_bits_written = 0;
    int data_bits_written = 0;

    // Simulate encoding to count operations
    uint64_t last_value = *((uint64_t*)&test_data[0]);
    int data_bits = 0;
    int prev_lzb = -1;
    int prev_tzb = -1;

    write_histogram[64]++;  // Initial value
    total_writes++;

    for (size_t i = 1; i < test_data.size(); i++) {
        const uint64_t current_value = *((uint64_t*)&test_data[i]);
        const uint64_t xor_value = current_value ^ last_value;

        if (xor_value == 0) {
            write_histogram[1]++;  // writeFixed<0b0, 1>
            control_bits_written += 1;
            total_writes++;
        } else {
            int lzb = 0;
            for (int i = 63; i >= 0; i--) {
                if ((xor_value >> i) & 1)
                    break;
                lzb++;
            }
            int tzb = 0;
            for (int i = 0; i < 64; i++) {
                if ((xor_value >> i) & 1)
                    break;
                tzb++;
            }

            if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                write_histogram[2]++;  // writeFixed<0b01, 2>
                control_bits_written += 2;
                total_writes++;
            } else {
                if (lzb > 31)
                    lzb = 31;
                data_bits = 64 - lzb - tzb;

                write_histogram[2]++;  // writeFixed<0b11, 2>
                write_histogram[5]++;  // write<5>(lzb)
                write_histogram[6]++;  // write<6>(data_bits)
                control_bits_written += 2 + 5 + 6;
                total_writes += 3;

                prev_lzb = lzb;
                prev_tzb = tzb;
            }

            write_histogram[data_bits]++;  // write(xor_value >> prev_tzb, data_bits)
            data_bits_written += data_bits;
            total_writes++;
        }

        last_value = current_value;
    }

    std::cout << "\nWrite Operations Summary:" << std::endl;
    std::cout << "  Total writes: " << total_writes << std::endl;
    std::cout << "  Writes per value: " << (float)total_writes / test_data.size() << std::endl;
    std::cout << "  Control bits: " << control_bits_written << " ("
              << (100.0 * control_bits_written / (control_bits_written + data_bits_written)) << "%)" << std::endl;
    std::cout << "  Data bits: " << data_bits_written << " ("
              << (100.0 * data_bits_written / (control_bits_written + data_bits_written)) << "%)" << std::endl;

    std::cout << "\nWrite Size Distribution:" << std::endl;
    for (auto& [bits, count] : write_histogram) {
        if (count > 0) {
            std::cout << "  " << std::setw(2) << bits << "-bit: " << std::setw(6) << count << " (" << std::setw(5)
                      << std::fixed << std::setprecision(1) << (100.0 * count / total_writes) << "%)" << std::endl;
        }
    }

    // Calculate overhead
    int small_writes = write_histogram[1] + write_histogram[2] + write_histogram[5] + write_histogram[6];
    std::cout << "\nSmall writes (≤6 bits): " << small_writes << " (" << (100.0 * small_writes / total_writes)
              << "% of all writes)" << std::endl;
}

// Test batched write performance
void test_batched_writes() {
    std::cout << "\n=== Batched Write Performance Test ===" << std::endl;

    const size_t iterations = 1000000;

    // Test 1: Many small writes (current pattern)
    {
        CompressedBuffer buffer;
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            buffer.writeFixed<0b11, 2>();
            buffer.write<5>(i & 0x1F);
            buffer.write<6>(i & 0x3F);
        }
        auto end = high_resolution_clock::now();
        double ms = duration_cast<microseconds>(end - start).count() / 1000.0;
        std::cout << "  3 small writes: " << ms << " ms" << std::endl;
    }

    // Test 2: Single combined write (13 bits total)
    {
        CompressedBuffer buffer;
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            uint64_t combined = (0b11 << 11) | ((i & 0x1F) << 6) | (i & 0x3F);
            buffer.write(combined, 13);
        }
        auto end = high_resolution_clock::now();
        double ms = duration_cast<microseconds>(end - start).count() / 1000.0;
        std::cout << "  1 combined write: " << ms << " ms" << std::endl;
    }
}

// Test pre-calculation optimization
void test_precalculation() {
    std::cout << "\n=== Pre-calculation Optimization Test ===" << std::endl;

    // Generate test data
    std::vector<double> test_data;
    for (int i = 0; i < 10000; i++) {
        test_data.push_back(20.0 + sin(i * 0.1) * 5.0);
    }

    // Method 1: Direct encoding (current)
    {
        auto start = high_resolution_clock::now();
        CompressedBuffer buffer = FloatEncoder::encode(test_data);
        auto end = high_resolution_clock::now();
        double ms = duration_cast<microseconds>(end - start).count() / 1000.0;
        std::cout << "  Direct encoding: " << ms << " ms (" << buffer.dataByteSize() << " bytes)" << std::endl;
    }

    // Method 2: Two-pass with pre-calculated size
    {
        auto start = high_resolution_clock::now();

        // First pass: calculate total bits needed
        size_t total_bits = 64;  // First value
        uint64_t last_value = *((uint64_t*)&test_data[0]);
        int data_bits = 0;
        int prev_lzb = -1, prev_tzb = -1;

        for (size_t i = 1; i < test_data.size(); i++) {
            const uint64_t current_value = *((uint64_t*)&test_data[i]);
            const uint64_t xor_value = current_value ^ last_value;

            if (xor_value == 0) {
                total_bits += 1;
            } else {
                auto lzb = __builtin_clzll(xor_value);
                auto tzb = __builtin_ctzll(xor_value);

                if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                    total_bits += 2;
                } else {
                    if (lzb > 31)
                        lzb = 31;
                    data_bits = 64 - lzb - tzb;
                    total_bits += 2 + 5 + 6;
                    prev_lzb = lzb;
                    prev_tzb = tzb;
                }
                total_bits += data_bits;
            }
            last_value = current_value;
        }

        // Second pass: encode with pre-allocated buffer
        CompressedBuffer buffer;
        buffer.reserve((total_bits + 63) / 64);
        CompressedBuffer encoded = FloatEncoder::encode(test_data);

        auto end = high_resolution_clock::now();
        double ms = duration_cast<microseconds>(end - start).count() / 1000.0;
        std::cout << "  Two-pass with pre-allocation: " << ms << " ms" << std::endl;
    }
}

int main() {
    std::cout << "CompressedBuffer Deep Analysis" << std::endl;
    std::cout << "==============================" << std::endl;

    analyze_float_encoder_pattern();
    test_batched_writes();
    test_precalculation();

    std::cout << "\n=== Optimization Recommendations ===" << std::endl;
    std::cout << "1. Batch small control bit writes into single operations" << std::endl;
    std::cout << "2. Use staging buffer for writes < 8 bits" << std::endl;
    std::cout << "3. Consider two-pass encoding with size pre-calculation" << std::endl;
    std::cout << "4. Template specialize for common bit patterns" << std::endl;

    return 0;
}