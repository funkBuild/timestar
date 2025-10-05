#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include <algorithm>
#include <cstring>

#include "../../lib/encoding/integer_encoder.hpp"
#include "../../lib/encoding/integer/integer_encoder.hpp"
#include "../../lib/encoding/integer/integer_encoder_simd.hpp"
#include "../../lib/encoding/integer/integer_encoder_avx512.hpp"
#include "../../lib/storage/aligned_buffer.hpp"
#include "../../lib/storage/slice_buffer.hpp"

// ANSI color codes
#define RESET   "\033[0m"
#define BOLD    "\033[1m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"
#define YELLOW  "\033[33m"
#define BLUE    "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN    "\033[36m"

struct BenchmarkResult {
    std::string implementation;
    double encode_time_ms;
    double decode_time_ms;
    double encode_throughput_mbps;
    double decode_throughput_mbps;
    double speedup_encode;
    double speedup_decode;
    bool correct;
    bool available;
};

// Generate test datasets
std::vector<uint64_t> generateMonotonicTimestamps(size_t count, uint64_t start = 1000000000000ULL, uint64_t interval = 1000) {
    std::vector<uint64_t> data;
    data.reserve(count);
    uint64_t current = start;
    for (size_t i = 0; i < count; i++) {
        data.push_back(current);
        current += interval;
    }
    return data;
}

std::vector<uint64_t> generateJitteredTimestamps(size_t count, uint64_t start = 1000000000000ULL, uint64_t interval = 1000, uint64_t jitter = 100) {
    std::vector<uint64_t> data;
    data.reserve(count);
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(0, jitter);

    uint64_t current = start;
    for (size_t i = 0; i < count; i++) {
        data.push_back(current);
        current += interval + dist(gen) - jitter/2;
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

void compareAllVariants(const std::string& dataset_name, const std::vector<uint64_t>& data) {
    std::cout << BOLD << BLUE << "\n═══ Dataset: " << dataset_name << " (" << data.size() << " values) ═══\n" << RESET;

    const int warmup_runs = 10;
    const int benchmark_runs = 100;
    const size_t data_size = data.size() * sizeof(uint64_t);

    std::vector<BenchmarkResult> results;

    // Benchmark Basic Implementation
    {
        BenchmarkResult result;
        result.implementation = "Basic (Non-AVX)";
        result.available = true;

        // Warmup
        for (int i = 0; i < warmup_runs; i++) {
            auto encoded = IntegerEncoderBasic::encode(data);
            std::vector<uint64_t> decoded;
            Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
            IntegerEncoderBasic::decode(slice, data.size(), decoded);
        }

        // Encode benchmark
        auto encode_start = std::chrono::high_resolution_clock::now();
        AlignedBuffer encoded;
        for (int i = 0; i < benchmark_runs; i++) {
            encoded = IntegerEncoderBasic::encode(data);
        }
        auto encode_end = std::chrono::high_resolution_clock::now();

        // Decode benchmark
        std::vector<uint64_t> decoded;
        auto decode_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < benchmark_runs; i++) {
            decoded.clear();
            Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
            IntegerEncoderBasic::decode(slice, data.size(), decoded);
        }
        auto decode_end = std::chrono::high_resolution_clock::now();

        // Calculate times
        result.encode_time_ms = std::chrono::duration<double, std::milli>(encode_end - encode_start).count() / benchmark_runs;
        result.decode_time_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count() / benchmark_runs;

        // Verify correctness
        result.correct = (decoded == data);

        // Calculate throughput
        double mb_size = data_size / (1024.0 * 1024.0);
        result.encode_throughput_mbps = mb_size / (result.encode_time_ms / 1000.0);
        result.decode_throughput_mbps = mb_size / (result.decode_time_ms / 1000.0);

        result.speedup_encode = 1.0;
        result.speedup_decode = 1.0;

        results.push_back(result);
    }

    // Benchmark AVX2/SIMD Implementation
    {
        BenchmarkResult result;
        result.implementation = "AVX2 SIMD";
        result.available = IntegerEncoderSIMD::isAvailable();

        if (result.available) {
            // Warmup
            for (int i = 0; i < warmup_runs; i++) {
                auto encoded = IntegerEncoderSIMD::encode(data);
                std::vector<uint64_t> decoded;
                Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
                IntegerEncoderSIMD::decode(slice, data.size(), decoded);
            }

            // Encode benchmark
            auto encode_start = std::chrono::high_resolution_clock::now();
            AlignedBuffer encoded;
            for (int i = 0; i < benchmark_runs; i++) {
                encoded = IntegerEncoderSIMD::encode(data);
            }
            auto encode_end = std::chrono::high_resolution_clock::now();

            // Decode benchmark
            std::vector<uint64_t> decoded;
            auto decode_start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < benchmark_runs; i++) {
                decoded.clear();
                Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
                IntegerEncoderSIMD::decode(slice, data.size(), decoded);
            }
            auto decode_end = std::chrono::high_resolution_clock::now();

            // Calculate times
            result.encode_time_ms = std::chrono::duration<double, std::milli>(encode_end - encode_start).count() / benchmark_runs;
            result.decode_time_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count() / benchmark_runs;

            // Verify correctness
            result.correct = (decoded == data);

            // Calculate throughput
            double mb_size = data_size / (1024.0 * 1024.0);
            result.encode_throughput_mbps = mb_size / (result.encode_time_ms / 1000.0);
            result.decode_throughput_mbps = mb_size / (result.decode_time_ms / 1000.0);

            result.speedup_encode = results[0].encode_time_ms / result.encode_time_ms;
            result.speedup_decode = results[0].decode_time_ms / result.decode_time_ms;
        }

        results.push_back(result);
    }

    // Benchmark AVX-512 Implementation
    {
        BenchmarkResult result;
        result.implementation = "AVX-512";
        result.available = IntegerEncoderAVX512::isAvailable();

        if (result.available) {
            // Warmup
            for (int i = 0; i < warmup_runs; i++) {
                auto encoded = IntegerEncoderAVX512::encode(data);
                std::vector<uint64_t> decoded;
                Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
                IntegerEncoderAVX512::decode(slice, data.size(), decoded);
            }

            // Encode benchmark
            auto encode_start = std::chrono::high_resolution_clock::now();
            AlignedBuffer encoded;
            for (int i = 0; i < benchmark_runs; i++) {
                encoded = IntegerEncoderAVX512::encode(data);
            }
            auto encode_end = std::chrono::high_resolution_clock::now();

            // Decode benchmark
            std::vector<uint64_t> decoded;
            auto decode_start = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < benchmark_runs; i++) {
                decoded.clear();
                Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
                IntegerEncoderAVX512::decode(slice, data.size(), decoded);
            }
            auto decode_end = std::chrono::high_resolution_clock::now();

            // Calculate times
            result.encode_time_ms = std::chrono::duration<double, std::milli>(encode_end - encode_start).count() / benchmark_runs;
            result.decode_time_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count() / benchmark_runs;

            // Verify correctness
            result.correct = (decoded == data);

            // Calculate throughput
            double mb_size = data_size / (1024.0 * 1024.0);
            result.encode_throughput_mbps = mb_size / (result.encode_time_ms / 1000.0);
            result.decode_throughput_mbps = mb_size / (result.decode_time_ms / 1000.0);

            result.speedup_encode = results[0].encode_time_ms / result.encode_time_ms;
            result.speedup_decode = results[0].decode_time_ms / result.decode_time_ms;
        }

        results.push_back(result);
    }

    // Test automatic selection
    {
        BenchmarkResult result;
        result.implementation = "Auto-selected";
        result.available = true;

        std::string impl_name = IntegerEncoder::getImplementationName();
        result.implementation += " (" + impl_name + ")";

        // Warmup
        for (int i = 0; i < warmup_runs; i++) {
            auto encoded = IntegerEncoder::encode(data);
            std::vector<uint64_t> decoded;
            Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
            IntegerEncoder::decode(slice, data.size(), decoded);
        }

        // Encode benchmark
        auto encode_start = std::chrono::high_resolution_clock::now();
        AlignedBuffer encoded;
        for (int i = 0; i < benchmark_runs; i++) {
            encoded = IntegerEncoder::encode(data);
        }
        auto encode_end = std::chrono::high_resolution_clock::now();

        // Decode benchmark
        std::vector<uint64_t> decoded;
        auto decode_start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < benchmark_runs; i++) {
            decoded.clear();
            Slice slice((const uint8_t*)encoded.data.data(), encoded.size());
            IntegerEncoder::decode(slice, data.size(), decoded);
        }
        auto decode_end = std::chrono::high_resolution_clock::now();

        // Calculate times
        result.encode_time_ms = std::chrono::duration<double, std::milli>(encode_end - encode_start).count() / benchmark_runs;
        result.decode_time_ms = std::chrono::duration<double, std::milli>(decode_end - decode_start).count() / benchmark_runs;

        // Verify correctness
        result.correct = (decoded == data);

        // Calculate throughput
        double mb_size = data_size / (1024.0 * 1024.0);
        result.encode_throughput_mbps = mb_size / (result.encode_time_ms / 1000.0);
        result.decode_throughput_mbps = mb_size / (result.decode_time_ms / 1000.0);

        result.speedup_encode = results[0].encode_time_ms / result.encode_time_ms;
        result.speedup_decode = results[0].decode_time_ms / result.decode_time_ms;

        results.push_back(result);
    }

    // Print results table
    std::cout << BOLD;
    std::cout << std::setw(25) << std::left << "Implementation"
              << std::setw(12) << std::right << "Encode(ms)"
              << std::setw(12) << "Decode(ms)"
              << std::setw(15) << "Enc MB/s"
              << std::setw(15) << "Dec MB/s"
              << std::setw(12) << "Enc Speedup"
              << std::setw(12) << "Dec Speedup"
              << std::setw(12) << "Status" << "\n";
    std::cout << std::string(118, '-') << "\n" << RESET;

    for (const auto& result : results) {
        if (!result.available) {
            std::cout << std::setw(25) << std::left << result.implementation;
            std::cout << YELLOW << std::setw(70) << std::right << "Not available on this CPU" << RESET << "\n";
            continue;
        }

        std::cout << std::setw(25) << std::left << result.implementation;
        std::cout << std::setw(12) << std::right << std::fixed << std::setprecision(3) << result.encode_time_ms;
        std::cout << std::setw(12) << std::fixed << std::setprecision(3) << result.decode_time_ms;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << result.encode_throughput_mbps;
        std::cout << std::setw(15) << std::fixed << std::setprecision(1) << result.decode_throughput_mbps;

        // Color code speedups
        if (result.speedup_encode > 1.1) {
            std::cout << GREEN;
        } else if (result.speedup_encode < 0.9) {
            std::cout << RED;
        }
        std::cout << std::setw(12) << std::fixed << std::setprecision(2) << result.speedup_encode << "x" << RESET;

        if (result.speedup_decode > 1.1) {
            std::cout << GREEN;
        } else if (result.speedup_decode < 0.9) {
            std::cout << RED;
        }
        std::cout << std::setw(12) << std::fixed << std::setprecision(2) << result.speedup_decode << "x" << RESET;

        if (result.correct) {
            std::cout << GREEN << std::setw(12) << "✓ PASS" << RESET;
        } else {
            std::cout << RED << std::setw(12) << "✗ FAIL" << RESET;
        }
        std::cout << "\n";
    }
}

void testForceImplementation() {
    std::cout << BOLD << YELLOW << "\n═══════════════════ TESTING FORCED IMPLEMENTATIONS ═══════════════════\n" << RESET;

    auto data = generateMonotonicTimestamps(10000);

    // Test each forced implementation
    std::cout << "\nForcing BASIC implementation:\n";
    IntegerEncoder::setImplementation(IntegerEncoder::BASIC);
    std::cout << "  Implementation name: " << IntegerEncoder::getImplementationName() << "\n";

    if (IntegerEncoder::hasAVX2()) {
        std::cout << "\nForcing SIMD implementation:\n";
        IntegerEncoder::setImplementation(IntegerEncoder::SIMD);
        std::cout << "  Implementation name: " << IntegerEncoder::getImplementationName() << "\n";
    }

    if (IntegerEncoder::hasAVX512()) {
        std::cout << "\nForcing AVX512 implementation:\n";
        IntegerEncoder::setImplementation(IntegerEncoder::AVX512);
        std::cout << "  Implementation name: " << IntegerEncoder::getImplementationName() << "\n";
    }

    // Reset to auto
    IntegerEncoder::setImplementation(IntegerEncoder::AUTO);
    std::cout << "\nReset to AUTO:\n";
    std::cout << "  Implementation name: " << IntegerEncoder::getImplementationName() << "\n";
}

int main() {
    std::cout << BOLD << MAGENTA;
    std::cout << "\n╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║     INTEGER ENCODER VARIANTS PERFORMANCE BENCHMARK            ║\n";
    std::cout << "║     Comparing: Basic vs AVX2 vs AVX-512 Implementations       ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n" << RESET;

    // Check CPU features
    std::cout << BOLD << "\nCPU Features:\n" << RESET;
    std::cout << "  AVX2:    " << (IntegerEncoder::hasAVX2() ? GREEN "✓ Available" : RED "✗ Not Available") << RESET << "\n";
    std::cout << "  AVX-512: " << (IntegerEncoder::hasAVX512() ? GREEN "✓ Available" : RED "✗ Not Available") << RESET << "\n";

    // Test different datasets
    compareAllVariants("Monotonic Timestamps (10K)", generateMonotonicTimestamps(10000));
    compareAllVariants("Monotonic Timestamps (100K)", generateMonotonicTimestamps(100000));
    compareAllVariants("Monotonic Timestamps (1M)", generateMonotonicTimestamps(1000000));

    compareAllVariants("Jittered Timestamps (100K)", generateJitteredTimestamps(100000));
    compareAllVariants("Small Integers (100K)", generateSmallIntegers(100000));
    compareAllVariants("Random Integers (100K)", generateRandomIntegers(100000));

    // Test forced implementations
    testForceImplementation();

    // Summary
    std::cout << BOLD << GREEN << "\n═══════════════════ SUMMARY ═══════════════════\n" << RESET;
    std::cout << "Integer encoder variants successfully implemented:\n";
    std::cout << "  1. Basic (Non-AVX): Loop unrolling optimizations\n";
    std::cout << "  2. AVX2 SIMD: 8-value parallel processing where possible\n";
    std::cout << "  3. AVX-512: 16-value parallel processing where possible\n";
    std::cout << "  4. Auto-selection: Automatically picks best available\n";
    std::cout << "\nNote: Delta-of-delta reconstruction has inherent sequential\n";
    std::cout << "dependencies that limit SIMD parallelization benefits.\n";

    return 0;
}