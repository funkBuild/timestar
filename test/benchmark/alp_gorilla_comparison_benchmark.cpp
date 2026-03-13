#include "alp/alp_decoder.hpp"
#include "alp/alp_encoder.hpp"
#include "compressed_buffer.hpp"
#include "float_encoder.hpp"
#include "slice_buffer.hpp"

#include <chrono>
#include <cmath>
#include <functional>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

using Clock = std::chrono::high_resolution_clock;

// ============================================================
// Data generators
// ============================================================

std::vector<double> genSensorTemp(size_t n) {
    std::mt19937 gen(55);
    std::normal_distribution<double> noise(0.0, 0.05);
    std::vector<double> data(n);
    for (size_t i = 0; i < n; ++i) {
        double t = static_cast<double>(i);
        data[i] = std::round((20.0 + std::sin(t * 0.001) * 5.0 + noise(gen)) * 10.0) / 10.0;
    }
    return data;
}

std::vector<double> genIntegerCounters(size_t n) {
    std::vector<double> data(n);
    for (size_t i = 0; i < n; ++i) {
        data[i] = static_cast<double>(1000000 + i);
    }
    return data;
}

std::vector<double> genCpuPercentages(size_t n) {
    std::mt19937 gen(42);
    std::uniform_real_distribution<double> dist(0.0, 100.0);
    std::vector<double> data(n);
    for (auto& v : data) {
        v = std::round(dist(gen) * 10.0) / 10.0;
    }
    return data;
}

std::vector<double> genFinancialTicks(size_t n) {
    std::mt19937 gen(77);
    std::normal_distribution<double> dist(0.0, 0.01);
    std::vector<double> data(n);
    data[0] = 150.25;
    for (size_t i = 1; i < n; ++i) {
        data[i] = std::round((data[i - 1] + dist(gen)) * 100.0) / 100.0;
    }
    return data;
}

std::vector<double> genRandomDoubles(size_t n) {
    std::mt19937_64 gen(123);
    std::uniform_real_distribution<double> dist(-1e15, 1e15);
    std::vector<double> data(n);
    for (auto& v : data) {
        v = dist(gen);
    }
    return data;
}

std::vector<double> genSparseWithZeros(size_t n) {
    std::vector<double> data(n, 0.0);
    std::mt19937 gen(99);
    std::uniform_int_distribution<size_t> idx_dist(0, n - 1);
    std::uniform_real_distribution<double> val_dist(1.0, 100.0);
    for (size_t i = 0; i < n / 10; ++i) {
        data[idx_dist(gen)] = std::round(val_dist(gen) * 10.0) / 10.0;
    }
    return data;
}

std::vector<double> genConstant(size_t n) {
    return std::vector<double>(n, 42.5);
}

// ============================================================
// Benchmark helpers
// ============================================================

struct BenchResult {
    double encode_mbs;
    double decode_mbs;
    double compression_ratio;
    size_t compressed_bytes;
};

static constexpr int WARMUP_ITERS = 2;
static constexpr int BENCH_ITERS = 5;

BenchResult benchmarkGorilla(const std::vector<double>& data) {
    const size_t raw_bytes = data.size() * sizeof(double);

    // Warmup
    for (int i = 0; i < WARMUP_ITERS; ++i) {
        auto buf = FloatEncoder::encode(data);
    }

    // Encode benchmark
    auto enc_start = Clock::now();
    CompressedBuffer encoded;
    for (int i = 0; i < BENCH_ITERS; ++i) {
        encoded = FloatEncoder::encode(data);
    }
    auto enc_end = Clock::now();
    double enc_secs = std::chrono::duration<double>(enc_end - enc_start).count() / BENCH_ITERS;

    size_t compressed_bytes = encoded.data.size() * sizeof(uint64_t);

    // Decode benchmark
    for (int i = 0; i < WARMUP_ITERS; ++i) {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(encoded.data.data()), compressed_bytes);
        std::vector<double> out;
        FloatDecoder::decode(slice, 0, data.size(), out);
    }

    auto dec_start = Clock::now();
    for (int i = 0; i < BENCH_ITERS; ++i) {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(encoded.data.data()), compressed_bytes);
        std::vector<double> out;
        FloatDecoder::decode(slice, 0, data.size(), out);
    }
    auto dec_end = Clock::now();
    double dec_secs = std::chrono::duration<double>(dec_end - dec_start).count() / BENCH_ITERS;

    return {(raw_bytes / (1024.0 * 1024.0)) / enc_secs, (raw_bytes / (1024.0 * 1024.0)) / dec_secs,
            static_cast<double>(raw_bytes) / compressed_bytes, compressed_bytes};
}

BenchResult benchmarkALP(const std::vector<double>& data) {
    const size_t raw_bytes = data.size() * sizeof(double);

    // Warmup
    for (int i = 0; i < WARMUP_ITERS; ++i) {
        auto buf = ALPEncoder::encode(data);
    }

    // Encode benchmark
    auto enc_start = Clock::now();
    CompressedBuffer encoded;
    for (int i = 0; i < BENCH_ITERS; ++i) {
        encoded = ALPEncoder::encode(data);
    }
    auto enc_end = Clock::now();
    double enc_secs = std::chrono::duration<double>(enc_end - enc_start).count() / BENCH_ITERS;

    size_t compressed_bytes = encoded.data.size() * sizeof(uint64_t);

    // Decode benchmark
    for (int i = 0; i < WARMUP_ITERS; ++i) {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(encoded.data.data()), compressed_bytes);
        std::vector<double> out;
        ALPDecoder::decode(slice, 0, data.size(), out);
    }

    auto dec_start = Clock::now();
    for (int i = 0; i < BENCH_ITERS; ++i) {
        CompressedSlice slice(reinterpret_cast<const uint8_t*>(encoded.data.data()), compressed_bytes);
        std::vector<double> out;
        ALPDecoder::decode(slice, 0, data.size(), out);
    }
    auto dec_end = Clock::now();
    double dec_secs = std::chrono::duration<double>(dec_end - dec_start).count() / BENCH_ITERS;

    return {(raw_bytes / (1024.0 * 1024.0)) / enc_secs, (raw_bytes / (1024.0 * 1024.0)) / dec_secs,
            static_cast<double>(raw_bytes) / compressed_bytes, compressed_bytes};
}

// ============================================================
// Main
// ============================================================

int main() {
    struct PatternDef {
        std::string name;
        std::function<std::vector<double>(size_t)> generator;
    };

    std::vector<PatternDef> patterns = {
        {"Sensor Temp", genSensorTemp},
        {"Integer Counters", genIntegerCounters},
        {"CPU Percentages", genCpuPercentages},
        {"Financial Ticks", genFinancialTicks},
        {"Random Doubles", genRandomDoubles},
        {"Sparse + Zeros", genSparseWithZeros},
        {"Constant", genConstant},
    };

    std::vector<size_t> sizes = {10000, 100000, 1000000};

    std::cout << "ALP vs Gorilla (XOR) Float Compression Benchmark\n";
    std::cout << "Gorilla impl: " << FloatEncoder::getImplementationName() << "\n";
    std::cout << std::string(130, '=') << "\n\n";

    for (auto sz : sizes) {
        std::cout << "Dataset size: " << sz << " doubles (" << (sz * 8) / 1024 << " KB raw)\n";
        std::cout << std::string(130, '-') << "\n";

        // Header
        std::cout << std::left << std::setw(20) << "Pattern"
                  << " | " << std::setw(12) << "Gorilla Enc"
                  << " | " << std::setw(12) << "ALP Enc"
                  << " | " << std::setw(12) << "Gorilla Dec"
                  << " | " << std::setw(12) << "ALP Dec"
                  << " | " << std::setw(10) << "Gor Ratio"
                  << " | " << std::setw(10) << "ALP Ratio"
                  << " | " << std::setw(10) << "Gor Size"
                  << " | " << std::setw(10) << "ALP Size"
                  << "\n";

        std::cout << std::setw(20) << ""
                  << " | " << std::setw(12) << "(MB/s)"
                  << " | " << std::setw(12) << "(MB/s)"
                  << " | " << std::setw(12) << "(MB/s)"
                  << " | " << std::setw(12) << "(MB/s)"
                  << " | " << std::setw(10) << ""
                  << " | " << std::setw(10) << ""
                  << " | " << std::setw(10) << "(KB)"
                  << " | " << std::setw(10) << "(KB)"
                  << "\n";

        std::cout << std::string(130, '-') << "\n";

        for (auto& pat : patterns) {
            auto data = pat.generator(sz);

            auto gor = benchmarkGorilla(data);
            auto alp = benchmarkALP(data);

            std::cout << std::left << std::setw(20) << pat.name << " | " << std::right << std::setw(10) << std::fixed
                      << std::setprecision(1) << gor.encode_mbs << "  "
                      << " | " << std::setw(10) << alp.encode_mbs << "  "
                      << " | " << std::setw(10) << gor.decode_mbs << "  "
                      << " | " << std::setw(10) << alp.decode_mbs << "  "
                      << " | " << std::setw(8) << std::setprecision(2) << gor.compression_ratio << "x "
                      << " | " << std::setw(8) << alp.compression_ratio << "x "
                      << " | " << std::setw(8) << std::setprecision(1) << gor.compressed_bytes / 1024.0 << "  "
                      << " | " << std::setw(8) << alp.compressed_bytes / 1024.0 << "  "
                      << "\n";
        }

        std::cout << "\n";
    }

    return 0;
}
