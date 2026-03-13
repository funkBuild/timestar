#include "encoding/float_encoder.hpp"
#include "storage/compressed_buffer.hpp"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace std::chrono;

class BenchmarkTimer {
    high_resolution_clock::time_point start;

public:
    BenchmarkTimer() : start(high_resolution_clock::now()) {}

    double elapsed_ms() {
        auto end = high_resolution_clock::now();
        return duration_cast<microseconds>(end - start).count() / 1000.0;
    }
};

void benchmark_bit_writes() {
    std::cout << "\n=== Bit Write Performance ===" << std::endl;

    const size_t iterations = 1000000;
    const size_t warmup = 10000;

    // Warmup
    {
        CompressedBuffer buffer;
        for (size_t i = 0; i < warmup; ++i) {
            buffer.write(i & 0xFF, 8);
        }
    }

    // Test different bit widths
    std::vector<int> bit_widths = {1, 5, 8, 16, 32, 48, 64};

    for (int bits : bit_widths) {
        CompressedBuffer buffer;
        BenchmarkTimer timer;

        uint64_t mask = (bits == 64) ? 0xFFFFFFFFFFFFFFFF : ((1ULL << bits) - 1);
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(i & mask, bits);
        }

        double ms = timer.elapsed_ms();
        size_t total_bits = iterations * bits;
        size_t total_bytes = total_bits / 8;
        double mb_per_sec = (total_bytes / (1024.0 * 1024.0)) / (ms / 1000.0);

        std::cout << std::setw(2) << bits << "-bit writes: " << std::fixed << std::setprecision(2) << std::setw(8) << ms
                  << " ms, " << std::setw(10) << mb_per_sec << " MB/s" << std::endl;
    }
}

void benchmark_mixed_operations() {
    std::cout << "\n=== Mixed Bit Operations ===" << std::endl;

    const size_t iterations = 100000;

    CompressedBuffer buffer;
    BenchmarkTimer timer;

    // Simulate float encoder pattern: mix of fixed and variable bit writes
    for (size_t i = 0; i < iterations; ++i) {
        // Control bits (like float encoder)
        if (i % 3 == 0) {
            buffer.write(0b11, 2);      // New window
            buffer.write(i & 0x1F, 5);  // LZB
            buffer.write(i & 0x3F, 6);  // Data bits
        } else {
            buffer.write(0b01, 2);  // Reuse window
        }
        // Data (variable bits, simulating mantissa)
        buffer.write(i * 123456789, 48);
    }

    double ms = timer.elapsed_ms();
    size_t avg_bits_per_iter = (2 + 48) + (5 + 6) / 3;  // Average bits written
    size_t total_bytes = (iterations * avg_bits_per_iter) / 8;
    double mb_per_sec = (total_bytes / (1024.0 * 1024.0)) / (ms / 1000.0);

    std::cout << "Mixed operations: " << std::fixed << std::setprecision(2) << ms << " ms, " << mb_per_sec << " MB/s"
              << std::endl;
}

void benchmark_float_encoder() {
    std::cout << "\n=== Float Encoder Performance ===" << std::endl;

    const size_t data_size = 100000;
    const size_t runs = 10;

    // Generate test data
    std::vector<double> test_data;
    std::mt19937 gen(12345);
    std::uniform_real_distribution<> dis(-1000000.0, 1000000.0);

    for (size_t i = 0; i < data_size; ++i) {
        test_data.push_back(dis(gen));
    }

    // Warmup
    for (int i = 0; i < 3; ++i) {
        CompressedBuffer encoded = FloatEncoder::encode(test_data);
    }

    // Benchmark encoding
    double total_encode_ms = 0;
    size_t total_encoded_size = 0;

    for (size_t run = 0; run < runs; ++run) {
        BenchmarkTimer timer;
        CompressedBuffer encoded = FloatEncoder::encode(test_data);
        total_encode_ms += timer.elapsed_ms();
        total_encoded_size = encoded.dataByteSize();
    }

    double avg_encode_ms = total_encode_ms / runs;
    size_t input_bytes = test_data.size() * sizeof(double);
    double encode_throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_encode_ms / 1000.0);
    double compression_ratio = (double)input_bytes / total_encoded_size;

    std::cout << "Float encoding (" << data_size << " values):" << std::endl;
    std::cout << "  Time:         " << std::fixed << std::setprecision(2) << avg_encode_ms << " ms" << std::endl;
    std::cout << "  Throughput:   " << encode_throughput << " MB/s" << std::endl;
    std::cout << "  Compression:  " << std::setprecision(1) << compression_ratio << ":1" << std::endl;
}

void benchmark_buffer_growth() {
    std::cout << "\n=== Buffer Growth Pattern ===" << std::endl;

    CompressedBuffer buffer;
    std::vector<size_t> capacities;

    // Track capacity growth
    for (size_t i = 0; i < 1000; ++i) {
        size_t old_cap = buffer.data.capacity();
        buffer.write(0xAAAAAAAAAAAAAAAA, 64);
        size_t new_cap = buffer.data.capacity();

        if (new_cap != old_cap) {
            capacities.push_back(new_cap);
            if (capacities.size() <= 10) {
                std::cout << "  After " << i << " writes: capacity = " << new_cap << std::endl;
            }
        }
    }

    std::cout << "  Total reallocations: " << capacities.size() << std::endl;
    std::cout << "  Final capacity: " << buffer.data.capacity() << std::endl;
    std::cout << "  Final size: " << buffer.data.size() << std::endl;
    std::cout << "  Efficiency: " << std::fixed << std::setprecision(1)
              << (100.0 * buffer.data.size() / buffer.data.capacity()) << "%" << std::endl;
}

int main() {
    std::cout << "CompressedBuffer Performance Benchmark" << std::endl;
    std::cout << "======================================" << std::endl;

    benchmark_bit_writes();
    benchmark_mixed_operations();
    benchmark_float_encoder();
    benchmark_buffer_growth();

    std::cout << "\n=== Benchmark Complete ===" << std::endl;
    return 0;
}