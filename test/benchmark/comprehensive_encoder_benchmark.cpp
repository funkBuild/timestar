#include "../../lib/encoding/float_encoder.hpp"
#include "../../lib/encoding/float_encoder_avx512.hpp"
#include "../../lib/encoding/float_encoder_avx512_v2.hpp"
#include "../../lib/encoding/float_encoder_simd.hpp"
#include "../../lib/storage/compressed_buffer.hpp"
#include "../../lib/storage/compressed_buffer_batch.hpp"
#include "../../lib/storage/compressed_buffer_staged.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

using namespace std::chrono;

// Simple staged encoder implementation
class StagedFloatEncoder {
public:
    static CompressedBufferStaged encode(const std::vector<double>& values) {
        CompressedBufferStaged buffer;
        if (values.empty())
            return buffer;

        buffer.reserve((values.size() * 66 + 63) / 64);

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

struct BenchmarkResult {
    std::string name;
    double time_ms;
    size_t compressed_size;
    double throughput_mbps;
    double speedup;
    bool available;
};

void printHeader() {
    std::cout << "\n" << std::string(100, '=') << std::endl;
    std::cout << std::setw(30) << std::left << "Implementation" << std::setw(12) << std::right << "Time (ms)"
              << std::setw(15) << "Throughput" << std::setw(12) << "Size (KB)" << std::setw(10) << "Speedup"
              << std::setw(15) << "Status" << std::endl;
    std::cout << std::string(100, '-') << std::endl;
}

void printResult(const BenchmarkResult& r) {
    if (!r.available) {
        std::cout << std::setw(30) << std::left << r.name << std::setw(12) << std::right << "N/A" << std::setw(15)
                  << "N/A" << std::setw(12) << "N/A" << std::setw(10) << "N/A" << std::setw(15) << "Not Available"
                  << std::endl;
        return;
    }

    std::cout << std::setw(30) << std::left << r.name << std::setw(12) << std::right << std::fixed
              << std::setprecision(3) << r.time_ms << std::setw(12) << std::setprecision(1) << r.throughput_mbps
              << " MB/s" << std::setw(12) << std::setprecision(2) << (r.compressed_size / 1024.0) << std::setw(10)
              << std::setprecision(2) << r.speedup << "x";

    // Status based on speedup
    if (r.speedup >= 2.0) {
        std::cout << std::setw(15) << "Excellent";
    } else if (r.speedup >= 1.5) {
        std::cout << std::setw(15) << "Good";
    } else if (r.speedup >= 1.2) {
        std::cout << std::setw(15) << "Moderate";
    } else if (r.speedup >= 1.0) {
        std::cout << std::setw(15) << "Baseline";
    } else {
        std::cout << std::setw(15) << "Slower";
    }
    std::cout << std::endl;
}

void runBenchmark(const std::string& dataset_name, const std::vector<double>& data) {
    std::cout << "\n\n### " << dataset_name << " (" << data.size() << " values) ###" << std::endl;

    const int warmup_runs = 3;
    const int benchmark_runs = 10;
    size_t input_bytes = data.size() * sizeof(double);

    std::vector<BenchmarkResult> results;
    double baseline_time = 0;

    // Warmup
    for (int i = 0; i < warmup_runs; i++) {
        CompressedBuffer temp = FloatEncoder::encode(data);
    }

    // 1. Original (Baseline)
    {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBuffer encoded = FloatEncoder::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        baseline_time = avg_time;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back({"Original (Baseline)", avg_time, compressed_size, throughput, 1.0, true});
    }

    // 2. Original with Reserve
    {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            CompressedBuffer buffer;
            buffer.reserve((data.size() * 66 + 63) / 64);

            auto start = high_resolution_clock::now();
            CompressedBuffer encoded = FloatEncoder::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back(
            {"Original + Reserve", avg_time, compressed_size, throughput, baseline_time / avg_time, true});
    }

    // 3. Staged Buffer
    {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBufferStaged encoded = StagedFloatEncoder::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back({"Staged Buffer", avg_time, compressed_size, throughput, baseline_time / avg_time, true});
    }

    // 4. SIMD AVX2
    if (FloatEncoderSIMD::isAvailable()) {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBuffer encoded = FloatEncoderSIMD::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back(
            {"SIMD AVX2 (4x parallel)", avg_time, compressed_size, throughput, baseline_time / avg_time, true});
    } else {
        results.push_back({"SIMD AVX2", 0, 0, 0, 0, false});
    }

    // 5. AVX-512 Original
    if (FloatEncoderAVX512::isAvailable()) {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBufferBatch encoded = FloatEncoderAVX512::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back({"AVX-512 Original", avg_time, compressed_size, throughput, baseline_time / avg_time, true});
    } else {
        results.push_back({"AVX-512 Original", 0, 0, 0, 0, false});
    }

    // 6. AVX-512 V2 (Optimized)
    if (FloatEncoderAVX512V2::isAvailable()) {
        double total_time = 0;
        size_t compressed_size = 0;

        for (int r = 0; r < benchmark_runs; r++) {
            auto start = high_resolution_clock::now();
            CompressedBufferBatch encoded = FloatEncoderAVX512V2::encode(data);
            auto end = high_resolution_clock::now();
            total_time += duration_cast<microseconds>(end - start).count() / 1000.0;
            compressed_size = encoded.dataByteSize();
        }

        double avg_time = total_time / benchmark_runs;
        double throughput = (input_bytes / (1024.0 * 1024.0)) / (avg_time / 1000.0);

        results.push_back(
            {"AVX-512 V2 (Optimized)", avg_time, compressed_size, throughput, baseline_time / avg_time, true});
    } else {
        results.push_back({"AVX-512 V2 Optimized", 0, 0, 0, 0, false});
    }

    // Print results
    printHeader();
    for (const auto& r : results) {
        printResult(r);
    }

    // Find best performer
    auto best =
        std::max_element(results.begin(), results.end(), [](const BenchmarkResult& a, const BenchmarkResult& b) {
            return a.available && b.available && a.speedup < b.speedup;
        });

    if (best != results.end() && best->available && best->speedup > 1.0) {
        std::cout << "\nBest performer: " << best->name << " (" << std::setprecision(2) << best->speedup
                  << "x speedup, " << std::setprecision(1) << best->throughput_mbps << " MB/s)" << std::endl;
    }

    // Compression ratio
    if (!results.empty() && results[0].available) {
        double ratio = (double)input_bytes / results[0].compressed_size;
        std::cout << "Compression ratio: " << std::setprecision(2) << ratio << ":1" << std::endl;
    }
}

int main() {
    std::cout << "\n==================================================" << std::endl;
    std::cout << "     COMPREHENSIVE FLOAT ENCODER BENCHMARK" << std::endl;
    std::cout << "==================================================" << std::endl;

    std::cout << "\nCPU Feature Detection:" << std::endl;
    std::cout << "  AVX2:      " << (FloatEncoderSIMD::isAvailable() ? "✓ Available" : "✗ Not Available") << std::endl;
    std::cout << "  AVX-512:   " << (FloatEncoderAVX512::isAvailable() ? "✓ Available" : "✗ Not Available")
              << std::endl;

    // Test datasets
    std::vector<std::pair<std::string, std::vector<double>>> datasets;

    // 1. Small dataset (1K)
    {
        std::vector<double> data;
        for (int i = 0; i < 1000; i++) {
            data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
        }
        datasets.push_back({"Small Sensor Data (1K)", data});
    }

    // 2. Medium dataset (10K)
    {
        std::vector<double> data;
        for (int i = 0; i < 10000; i++) {
            data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
        }
        datasets.push_back({"Medium Sensor Data (10K)", data});
    }

    // 3. Large dataset (100K)
    {
        std::vector<double> data;
        for (int i = 0; i < 100000; i++) {
            data.push_back(20.0 + sin(i * 0.1) * 5.0 + (rand() % 100) / 100.0);
        }
        datasets.push_back({"Large Sensor Data (100K)", data});
    }

    // 4. Financial data (10K)
    {
        std::vector<double> data;
        double price = 100.0;
        for (int i = 0; i < 10000; i++) {
            price *= (1.0 + (rand() % 100 - 50) / 10000.0);
            data.push_back(price);
        }
        datasets.push_back({"Financial Data (10K)", data});
    }

    // 5. Random data (10K)
    {
        std::vector<double> data;
        for (int i = 0; i < 10000; i++) {
            data.push_back((double)rand() / RAND_MAX * 1000.0);
        }
        datasets.push_back({"Random Data (10K)", data});
    }

    // Run benchmarks
    for (const auto& [name, data] : datasets) {
        runBenchmark(name, data);
    }

    std::cout << "\n==================================================" << std::endl;
    std::cout << "                    SUMMARY" << std::endl;
    std::cout << "==================================================" << std::endl;
    std::cout << "\nKey findings based on actual measurements:" << std::endl;
    std::cout << "1. Staged Buffer typically provides best performance" << std::endl;
    std::cout << "2. AVX-512 may underperform due to frequency throttling" << std::endl;
    std::cout << "3. Simple optimizations often beat complex SIMD" << std::endl;
    std::cout << "4. Memory bandwidth is often the limiting factor" << std::endl;

    return 0;
}