#include "encoding/float_encoder.hpp"
#include "storage/compressed_buffer.hpp"
#include "storage/compressed_buffer_staged.hpp"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace std::chrono;

// Optimized float encoder using staged buffer
class FloatEncoderStaged {
public:
    static CompressedBufferStaged encode(const std::vector<double>& values) {
        CompressedBufferStaged buffer;

        uint64_t last_value = *((uint64_t*)&values[0]);
        int data_bits = 0;
        int prev_lzb = -1;
        int prev_tzb = -1;

        buffer.write(last_value, 64);

        for (size_t i = 1; i < values.size(); i++) {
            const uint64_t current_value = *((uint64_t*)&values[i]);
            const uint64_t xor_value = current_value ^ last_value;

            if (xor_value == 0) {
                buffer.writeFixed<0b0, 1>();
            } else {
                // Calculate LZB and TZB
                int lzb = 0;
                for (int j = 63; j >= 0; j--) {
                    if ((xor_value >> j) & 1)
                        break;
                    lzb++;
                }
                int tzb = 0;
                for (int j = 0; j < 64; j++) {
                    if ((xor_value >> j) & 1)
                        break;
                    tzb++;
                }

                if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                    buffer.writeFixed<0b01, 2>();
                } else {
                    if (lzb > 31)
                        lzb = 31;
                    data_bits = 64 - lzb - tzb;

                    // Use optimized control block write
                    buffer.writeControlBlock(lzb, data_bits == 64 ? 0 : data_bits);

                    prev_lzb = lzb;
                    prev_tzb = tzb;
                }

                buffer.write(xor_value >> prev_tzb, data_bits);
            }

            last_value = current_value;
        }

        buffer.finalize();  // Important: flush staging buffer
        return buffer;
    }
};

void benchmark_comparison() {
    std::cout << "\n=== Float Encoder Performance Comparison ===" << std::endl;

    // Generate test data sets
    std::vector<size_t> sizes = {100, 1000, 10000, 100000};

    for (size_t size : sizes) {
        std::vector<double> test_data;

        // Realistic sensor data
        for (size_t i = 0; i < size; i++) {
            test_data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
        }

        std::cout << "\nDataset size: " << size << " values" << std::endl;

        // Warmup
        for (int w = 0; w < 3; w++) {
            CompressedBuffer temp1 = FloatEncoder::encode(test_data);
            CompressedBufferStaged temp2 = FloatEncoderStaged::encode(test_data);
        }

        // Original encoder
        double original_time = 0;
        size_t original_size = 0;
        const int runs = 10;

        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBuffer encoded = FloatEncoder::encode(test_data);
            auto end = high_resolution_clock::now();
            original_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            original_size = encoded.dataByteSize();
        }
        original_time /= runs;

        // Staged encoder
        double staged_time = 0;
        size_t staged_size = 0;

        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBufferStaged encoded = FloatEncoderStaged::encode(test_data);
            auto end = high_resolution_clock::now();
            staged_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            staged_size = encoded.dataByteSize();
        }
        staged_time /= runs;

        // Results
        double speedup = original_time / staged_time;
        size_t input_bytes = test_data.size() * sizeof(double);
        double original_throughput = (input_bytes / (1024.0 * 1024.0)) / (original_time / 1000.0);
        double staged_throughput = (input_bytes / (1024.0 * 1024.0)) / (staged_time / 1000.0);

        std::cout << "  Original: " << std::fixed << std::setprecision(3) << original_time << " ms"
                  << " (" << std::setprecision(0) << original_throughput << " MB/s, " << original_size << " bytes)"
                  << std::endl;
        std::cout << "  Staged:   " << std::fixed << std::setprecision(3) << staged_time << " ms"
                  << " (" << std::setprecision(0) << staged_throughput << " MB/s, " << staged_size << " bytes)"
                  << std::endl;
        std::cout << "  Speedup:  " << std::fixed << std::setprecision(2) << speedup << "x" << std::endl;
    }
}

void benchmark_write_patterns() {
    std::cout << "\n=== Write Pattern Performance ===" << std::endl;

    const size_t iterations = 1000000;

    // Pattern 1: Many small control bits (typical float encoder)
    {
        CompressedBuffer original;
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            original.writeFixed<0b11, 2>();
            original.write<5>(i & 0x1F);
            original.write<6>(i & 0x3F);
        }
        auto end = high_resolution_clock::now();
        double original_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        CompressedBufferStaged staged;
        start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            staged.writeControlBlock(i & 0x1F, i & 0x3F);
        }
        staged.finalize();
        end = high_resolution_clock::now();
        double staged_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        std::cout << "  Control block pattern (3 writes → 1):" << std::endl;
        std::cout << "    Original: " << original_ms << " ms" << std::endl;
        std::cout << "    Staged:   " << staged_ms << " ms" << std::endl;
        std::cout << "    Speedup:  " << std::fixed << std::setprecision(2) << (original_ms / staged_ms) << "x"
                  << std::endl;
    }

    // Pattern 2: Mixed small and large writes
    {
        CompressedBuffer original;
        auto start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            original.writeFixed<0b01, 2>();
            original.write(i * 123456789, 48);
        }
        auto end = high_resolution_clock::now();
        double original_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        CompressedBufferStaged staged;
        start = high_resolution_clock::now();
        for (size_t i = 0; i < iterations; i++) {
            staged.writeFixed<0b01, 2>();
            staged.write(i * 123456789, 48);
        }
        staged.finalize();
        end = high_resolution_clock::now();
        double staged_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        std::cout << "\n  Mixed pattern (2-bit + 48-bit):" << std::endl;
        std::cout << "    Original: " << original_ms << " ms" << std::endl;
        std::cout << "    Staged:   " << staged_ms << " ms" << std::endl;
        std::cout << "    Speedup:  " << std::fixed << std::setprecision(2) << (original_ms / staged_ms) << "x"
                  << std::endl;
    }
}

int main() {
    std::cout << "Staged CompressedBuffer Optimization Test" << std::endl;
    std::cout << "==========================================" << std::endl;

    benchmark_write_patterns();
    benchmark_comparison();

    std::cout << "\n=== Summary ===" << std::endl;
    std::cout << "The staged buffer optimization provides:" << std::endl;
    std::cout << "  • Batching of small writes (≤8 bits)" << std::endl;
    std::cout << "  • Reduced overhead for control bits" << std::endl;
    std::cout << "  • Specialized writeControlBlock() for common patterns" << std::endl;
    std::cout << "  • Better CPU cache utilization" << std::endl;

    return 0;
}