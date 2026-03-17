#include "../../lib/encoding/integer_encoder.hpp"
#include "../../lib/storage/aligned_buffer.hpp"
#include "../../lib/storage/slice_buffer.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <vector>

// ANSI color codes for output
#define RESET "\033[0m"
#define BOLD "\033[1m"
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN "\033[36m"

struct BenchmarkResult {
    std::string name;
    double encode_time_ms;
    double decode_time_ms;
    size_t compressed_size;
    size_t original_size;
    double encode_throughput_mbps;
    double decode_throughput_mbps;
    bool correct;
};

// Generate different types of integer datasets
std::vector<uint64_t> generateMonotonicTimestamps(size_t count, uint64_t start = 1000000000000ULL,
                                                  uint64_t interval = 1000) {
    std::vector<uint64_t> data;
    data.reserve(count);
    uint64_t current = start;

    for (size_t i = 0; i < count; i++) {
        data.push_back(current);
        current += interval;
    }

    return data;
}

std::vector<uint64_t> generateJitteredTimestamps(size_t count, uint64_t start = 1000000000000ULL,
                                                 uint64_t interval = 1000, uint64_t jitter = 100) {
    std::vector<uint64_t> data;
    data.reserve(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, jitter);

    uint64_t current = start;

    for (size_t i = 0; i < count; i++) {
        data.push_back(current);
        current += interval + dist(gen) - jitter / 2;
    }

    return data;
}

std::vector<uint64_t> generateRandomIntegers(size_t count, uint64_t min_val = 0, uint64_t max_val = 1000000) {
    std::vector<uint64_t> data;
    data.reserve(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(min_val, max_val);

    for (size_t i = 0; i < count; i++) {
        data.push_back(dist(gen));
    }

    return data;
}

std::vector<uint64_t> generateSmallIntegers(size_t count, uint64_t max_val = 100) {
    std::vector<uint64_t> data;
    data.reserve(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, max_val);

    for (size_t i = 0; i < count; i++) {
        data.push_back(dist(gen));
    }

    return data;
}

std::vector<uint64_t> generateMixedSizeIntegers(size_t count) {
    std::vector<uint64_t> data;
    data.reserve(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());

    for (size_t i = 0; i < count; i++) {
        if (i % 100 < 80) {
            // 80% small values (fit in 1-2 bytes)
            std::uniform_int_distribution<uint64_t> dist(0, 65535);
            data.push_back(dist(gen));
        } else if (i % 100 < 95) {
            // 15% medium values (fit in 4 bytes)
            std::uniform_int_distribution<uint64_t> dist(65536, 4294967295ULL);
            data.push_back(dist(gen));
        } else {
            // 5% large values (need 8 bytes)
            std::uniform_int_distribution<uint64_t> dist(4294967296ULL, UINT64_MAX / 2);
            data.push_back(dist(gen));
        }
    }

    return data;
}

BenchmarkResult benchmarkEncoder(const std::string& name, const std::vector<uint64_t>& data) {
    BenchmarkResult result;
    result.name = name;
    result.original_size = data.size() * sizeof(uint64_t);

    // Warmup
    for (int i = 0; i < 3; i++) {
        auto encoded = IntegerEncoder::encode(data);
    }

    // Encode benchmark
    const int encode_runs = 100;
    auto encode_start = std::chrono::high_resolution_clock::now();

    AlignedBuffer encoded;
    for (int i = 0; i < encode_runs; i++) {
        encoded = IntegerEncoder::encode(data);
    }

    auto encode_end = std::chrono::high_resolution_clock::now();

    // Calculate encode time
    auto encode_duration = std::chrono::duration_cast<std::chrono::microseconds>(encode_end - encode_start).count();
    result.encode_time_ms = encode_duration / 1000.0 / encode_runs;
    result.compressed_size = encoded.size();

    // Decode benchmark
    const int decode_runs = 100;
    std::vector<uint64_t> decoded;
    decoded.reserve(data.size());

    auto decode_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < decode_runs; i++) {
        decoded.clear();
        Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
        auto [skipped, added] = IntegerEncoder::decode(slice, data.size(), decoded);
    }

    auto decode_end = std::chrono::high_resolution_clock::now();

    // Calculate decode time
    auto decode_duration = std::chrono::duration_cast<std::chrono::microseconds>(decode_end - decode_start).count();
    result.decode_time_ms = decode_duration / 1000.0 / decode_runs;

    // Verify correctness
    result.correct = (decoded.size() == data.size());
    if (result.correct) {
        for (size_t i = 0; i < data.size(); i++) {
            if (decoded[i] != data[i]) {
                result.correct = false;
                break;
            }
        }
    }

    // Calculate throughput
    double mb_size = result.original_size / (1024.0 * 1024.0);
    result.encode_throughput_mbps = mb_size / (result.encode_time_ms / 1000.0);
    result.decode_throughput_mbps = mb_size / (result.decode_time_ms / 1000.0);

    return result;
}

void printResults(const std::vector<BenchmarkResult>& results) {
    std::cout << BOLD << CYAN << "\n═══════════════════════════ BENCHMARK RESULTS ═══════════════════════════\n"
              << RESET;

    // Print header
    std::cout << BOLD;
    std::cout << std::setw(25) << std::left << "Dataset" << std::setw(12) << std::right << "Encode(ms)" << std::setw(12)
              << "Decode(ms)" << std::setw(12) << "Enc MB/s" << std::setw(12) << "Dec MB/s" << std::setw(12)
              << "Size(KB)" << std::setw(12) << "Ratio" << std::setw(12) << "Status" << "\n";
    std::cout << std::string(103, '-') << "\n" << RESET;

    // Print results
    for (const auto& result : results) {
        std::cout << std::setw(25) << std::left << result.name;
        std::cout << std::setw(12) << std::right << std::fixed << std::setprecision(3) << result.encode_time_ms;
        std::cout << std::setw(12) << std::fixed << std::setprecision(3) << result.decode_time_ms;
        std::cout << std::setw(12) << std::fixed << std::setprecision(1) << result.encode_throughput_mbps;
        std::cout << std::setw(12) << std::fixed << std::setprecision(1) << result.decode_throughput_mbps;
        std::cout << std::setw(12) << std::fixed << std::setprecision(2) << (result.compressed_size / 1024.0);

        double ratio = (double)result.original_size / result.compressed_size;
        std::cout << std::setw(12) << std::fixed << std::setprecision(2) << ratio << "x";

        if (result.correct) {
            std::cout << GREEN << std::setw(12) << "✓ PASS" << RESET;
        } else {
            std::cout << RED << std::setw(12) << "✗ FAIL" << RESET;
        }
        std::cout << "\n";
    }
}

int main() {
    std::cout << BOLD << MAGENTA;
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║         INTEGER ENCODER/DECODER PERFORMANCE BENCHMARK         ║\n";
    std::cout << "║              FFOR (Frame-of-Reference) Encoding               ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << RESET;

    std::vector<BenchmarkResult> results;

    // Test different datasets
    std::cout << BOLD << "\n[1] Testing with different dataset patterns...\n" << RESET;

    // Small datasets
    results.push_back(benchmarkEncoder("Monotonic TS (10K)", generateMonotonicTimestamps(10000)));
    results.push_back(benchmarkEncoder("Jittered TS (10K)", generateJitteredTimestamps(10000)));
    results.push_back(benchmarkEncoder("Small Ints (10K)", generateSmallIntegers(10000)));
    results.push_back(benchmarkEncoder("Random Ints (10K)", generateRandomIntegers(10000)));
    results.push_back(benchmarkEncoder("Mixed Size (10K)", generateMixedSizeIntegers(10000)));

    // Large datasets
    results.push_back(benchmarkEncoder("Monotonic TS (100K)", generateMonotonicTimestamps(100000)));
    results.push_back(benchmarkEncoder("Jittered TS (100K)", generateJitteredTimestamps(100000)));
    results.push_back(benchmarkEncoder("Small Ints (100K)", generateSmallIntegers(100000)));
    results.push_back(benchmarkEncoder("Random Ints (100K)", generateRandomIntegers(100000)));
    results.push_back(benchmarkEncoder("Mixed Size (100K)", generateMixedSizeIntegers(100000)));

    // Very large dataset
    results.push_back(benchmarkEncoder("Monotonic TS (1M)", generateMonotonicTimestamps(1000000)));

    printResults(results);

    // Summary
    std::cout << BOLD << GREEN << "\n═══════════════════ SUMMARY ═══════════════════\n" << RESET;

    double avg_encode_throughput = 0;
    double avg_decode_throughput = 0;
    double avg_compression_ratio = 0;
    int count = 0;

    for (const auto& result : results) {
        if (result.name.find("100K") != std::string::npos) {
            avg_encode_throughput += result.encode_throughput_mbps;
            avg_decode_throughput += result.decode_throughput_mbps;
            avg_compression_ratio += (double)result.original_size / result.compressed_size;
            count++;
        }
    }

    avg_encode_throughput /= count;
    avg_decode_throughput /= count;
    avg_compression_ratio /= count;

    std::cout << "Average performance (100K datasets):\n";
    std::cout << "  Encode throughput: " << std::fixed << std::setprecision(1) << avg_encode_throughput << " MB/s\n";
    std::cout << "  Decode throughput: " << std::fixed << std::setprecision(1) << avg_decode_throughput << " MB/s\n";
    std::cout << "  Compression ratio: " << std::fixed << std::setprecision(2) << avg_compression_ratio << "x\n";

    return 0;
}