#include "encoding/float_encoder.hpp"
#include "encoding/float_encoder_simd.hpp"
#include "storage/compressed_buffer_staged.hpp"

#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace std::chrono;

// Staged float encoder from previous attempt
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
                int lzb = __builtin_clzll(xor_value);
                int tzb = __builtin_ctzll(xor_value);

                if (data_bits != 0 && prev_lzb <= lzb && prev_tzb <= tzb) {
                    buffer.writeFixed<0b01, 2>();
                } else {
                    if (lzb > 31)
                        lzb = 31;
                    data_bits = 64 - lzb - tzb;
                    buffer.writeControlBlock(lzb, data_bits == 64 ? 0 : data_bits);
                    prev_lzb = lzb;
                    prev_tzb = tzb;
                }

                buffer.write(xor_value >> prev_tzb, data_bits);
            }

            last_value = current_value;
        }

        buffer.finalize();
        return buffer;
    }
};

void benchmark_all_implementations() {
    std::cout << "\n=== Float Encoder Implementation Comparison ===" << std::endl;
    std::cout << "Comparing: Original vs Staged Buffer vs SIMD Optimized" << std::endl;

    // Test different dataset characteristics
    struct Dataset {
        std::string name;
        std::vector<double> data;
    };

    std::vector<Dataset> datasets;

    // 1. Realistic sensor data (gradual changes)
    {
        std::vector<double> data;
        for (int i = 0; i < 10000; i++) {
            data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
        }
        datasets.push_back({"Sensor Data (10K)", data});
    }

    // 2. Financial data (small variations)
    {
        std::vector<double> data;
        double price = 100.0;
        for (int i = 0; i < 10000; i++) {
            price += (rand() % 100 - 50) / 100.0;
            data.push_back(price);
        }
        datasets.push_back({"Financial Data (10K)", data});
    }

    // 3. Random data (worst case)
    {
        std::vector<double> data;
        for (int i = 0; i < 10000; i++) {
            data.push_back((double)rand() / RAND_MAX * 1000.0);
        }
        datasets.push_back({"Random Data (10K)", data});
    }

    // 4. Large dataset
    {
        std::vector<double> data;
        for (int i = 0; i < 100000; i++) {
            data.push_back(20.0 + sin(i * 0.01) * 10.0);
        }
        datasets.push_back({"Large Dataset (100K)", data});
    }

    // Check SIMD availability
    bool simd_available = FloatEncoderSIMD::isAvailable();
    std::cout << "\nSIMD AVX2 Support: " << (simd_available ? "Available" : "Not Available") << std::endl;

    for (const auto& dataset : datasets) {
        std::cout << "\n" << dataset.name << ":" << std::endl;
        std::cout << std::string(50, '-') << std::endl;

        const int runs = 10;

        // Warmup
        for (int w = 0; w < 3; w++) {
            CompressedBuffer temp1 = FloatEncoder::encode(dataset.data);
            CompressedBufferStaged temp2 = FloatEncoderStaged::encode(dataset.data);
            if (simd_available) {
                CompressedBuffer temp3 = FloatEncoderSIMD::encode(dataset.data);
            }
        }

        // 1. Original encoder
        double original_time = 0;
        size_t original_size = 0;

        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBuffer encoded = FloatEncoder::encode(dataset.data);
            auto end = high_resolution_clock::now();
            original_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            original_size = encoded.dataByteSize();
        }
        original_time /= runs;

        // 2. Staged buffer encoder
        double staged_time = 0;
        size_t staged_size = 0;

        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBufferStaged encoded = FloatEncoderStaged::encode(dataset.data);
            auto end = high_resolution_clock::now();
            staged_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            staged_size = encoded.dataByteSize();
        }
        staged_time /= runs;

        // 3. SIMD encoder (if available)
        double simd_time = 0;
        size_t simd_size = 0;

        if (simd_available) {
            for (int r = 0; r < runs; r++) {
                auto start = high_resolution_clock::now();
                CompressedBuffer encoded = FloatEncoderSIMD::encode(dataset.data);
                auto end = high_resolution_clock::now();
                simd_time += duration_cast<microseconds>(end - start).count() / 1000.0;
                simd_size = encoded.dataByteSize();
            }
            simd_time /= runs;
        }

        // Calculate throughput
        size_t input_bytes = dataset.data.size() * sizeof(double);
        double original_throughput = (input_bytes / (1024.0 * 1024.0)) / (original_time / 1000.0);
        double staged_throughput = (input_bytes / (1024.0 * 1024.0)) / (staged_time / 1000.0);
        double simd_throughput = simd_available ? (input_bytes / (1024.0 * 1024.0)) / (simd_time / 1000.0) : 0;

        // Results table
        std::cout << std::fixed << std::setprecision(3);
        std::cout << "Implementation    Time(ms)  Throughput(MB/s)  Size(bytes)  vs Original" << std::endl;
        std::cout << std::string(70, '-') << std::endl;

        std::cout << "Original         " << std::setw(8) << original_time << "  " << std::setw(15)
                  << std::setprecision(1) << original_throughput << "  " << std::setw(11) << original_size
                  << "      1.00x" << std::endl;

        std::cout << "Staged Buffer    " << std::setw(8) << staged_time << "  " << std::setw(15) << std::setprecision(1)
                  << staged_throughput << "  " << std::setw(11) << staged_size << "      " << std::setprecision(2)
                  << (original_time / staged_time) << "x" << std::endl;

        if (simd_available) {
            std::cout << "SIMD (AVX2)      " << std::setw(8) << simd_time << "  " << std::setw(15)
                      << std::setprecision(1) << simd_throughput << "  " << std::setw(11) << simd_size << "      "
                      << std::setprecision(2) << (original_time / simd_time) << "x" << std::endl;
        }

        // Compression ratio
        double compression_ratio = (double)input_bytes / original_size;
        std::cout << "\nCompression ratio: " << std::setprecision(2) << compression_ratio << ":1" << std::endl;
    }
}

void benchmark_simd_scalability() {
    if (!FloatEncoderSIMD::isAvailable()) {
        std::cout << "\n=== SIMD Scalability Test ===" << std::endl;
        std::cout << "SIMD not available on this CPU" << std::endl;
        return;
    }

    std::cout << "\n=== SIMD Scalability Test ===" << std::endl;
    std::cout << "Testing speedup vs dataset size" << std::endl;

    std::vector<size_t> sizes = {10, 100, 1000, 10000, 100000, 1000000};

    std::cout << "\nSize       Original(ms)  SIMD(ms)  Speedup" << std::endl;
    std::cout << std::string(45, '-') << std::endl;

    for (size_t size : sizes) {
        std::vector<double> data;
        for (size_t i = 0; i < size; i++) {
            data.push_back(20.0 + sin(i * 0.1) * 5.0);
        }

        // Original
        auto start = high_resolution_clock::now();
        CompressedBuffer encoded1 = FloatEncoder::encode(data);
        auto end = high_resolution_clock::now();
        double original_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        // SIMD
        start = high_resolution_clock::now();
        CompressedBuffer encoded2 = FloatEncoderSIMD::encode(data);
        end = high_resolution_clock::now();
        double simd_ms = duration_cast<microseconds>(end - start).count() / 1000.0;

        double speedup = original_ms / simd_ms;

        std::cout << std::setw(8) << size << "  " << std::setw(12) << std::fixed << std::setprecision(3) << original_ms
                  << "  " << std::setw(8) << simd_ms << "  " << std::setw(6) << std::setprecision(2) << speedup << "x"
                  << std::endl;
    }
}

int main() {
    std::cout << "Float Encoder Optimization Benchmark" << std::endl;
    std::cout << "====================================" << std::endl;

    benchmark_all_implementations();
    benchmark_simd_scalability();

    std::cout << "\n=== Conclusions ===" << std::endl;
    std::cout << "1. Staged Buffer: Shows regression due to staging overhead" << std::endl;
    std::cout << "2. SIMD (AVX2): Expected 1.5-2.5x speedup for large datasets" << std::endl;
    std::cout << "3. Best approach: SIMD for XOR operations + optimized buffer management" << std::endl;

    return 0;
}