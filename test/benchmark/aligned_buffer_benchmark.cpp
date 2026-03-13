#include "storage/aligned_buffer.hpp"

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <vector>

using namespace std::chrono;

class BenchmarkTimer {
    high_resolution_clock::time_point start;
    std::string name;

public:
    BenchmarkTimer(const std::string& n) : name(n) { start = high_resolution_clock::now(); }

    double elapsed_ms() {
        auto end = high_resolution_clock::now();
        return duration_cast<microseconds>(end - start).count() / 1000.0;
    }

    void report(size_t operations, size_t bytes) {
        double ms = elapsed_ms();
        double ops_per_sec = (operations / ms) * 1000.0;
        double mb_per_sec = (bytes / (1024.0 * 1024.0)) / (ms / 1000.0);

        std::cout << std::setw(30) << std::left << name << ": " << std::fixed << std::setprecision(2) << std::setw(8)
                  << ms << " ms, " << std::setw(12) << ops_per_sec << " ops/sec, " << std::setw(10) << mb_per_sec
                  << " MB/s" << std::endl;
    }
};

void benchmark_sequential_writes() {
    const size_t iterations = 1000000;
    const size_t warmup = 10000;

    std::cout << "\n=== Sequential Write Performance ===" << std::endl;
    std::cout << "Iterations: " << iterations << std::endl;

    // Warmup
    {
        AlignedBuffer buffer;
        for (size_t i = 0; i < warmup; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
    }

    // Benchmark uint64_t writes
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer("uint64_t writes");
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
        timer.report(iterations, iterations * sizeof(uint64_t));
    }

    // Benchmark uint32_t writes
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer("uint32_t writes");
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint32_t>(i));
        }
        timer.report(iterations, iterations * sizeof(uint32_t));
    }

    // Benchmark uint8_t writes
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer("uint8_t writes");
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint8_t>(i));
        }
        timer.report(iterations, iterations * sizeof(uint8_t));
    }

    // Benchmark double writes
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer("double writes");
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<double>(i * 1.1));
        }
        timer.report(iterations, iterations * sizeof(double));
    }
}

void benchmark_mixed_workload() {
    const size_t iterations = 100000;

    std::cout << "\n=== Mixed Workload Performance ===" << std::endl;
    std::cout << "Iterations: " << iterations << std::endl;

    AlignedBuffer buffer;
    BenchmarkTimer timer("Mixed writes");

    for (size_t i = 0; i < iterations; ++i) {
        buffer.write(static_cast<uint64_t>(i));
        buffer.write(static_cast<uint32_t>(i));
        buffer.write(static_cast<uint16_t>(i));
        buffer.write(static_cast<uint8_t>(i));
        buffer.write(static_cast<double>(i * 1.1));
    }

    size_t total_bytes =
        iterations * (sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(double));
    timer.report(iterations * 5, total_bytes);
}

void benchmark_string_writes() {
    const size_t iterations = 100000;

    std::cout << "\n=== String Write Performance ===" << std::endl;
    std::cout << "Iterations: " << iterations << std::endl;

    std::vector<std::string> test_strings;
    for (size_t i = 0; i < 100; ++i) {
        test_strings.push_back("Test string " + std::to_string(i));
    }

    AlignedBuffer buffer;
    BenchmarkTimer timer("String writes");

    size_t total_bytes = 0;
    for (size_t i = 0; i < iterations; ++i) {
        std::string& str = test_strings[i % test_strings.size()];
        buffer.write(str);
        total_bytes += str.length();
    }

    timer.report(iterations, total_bytes);
}

void benchmark_buffer_copies() {
    const size_t iterations = 10000;
    const size_t buffer_size = 1024;

    std::cout << "\n=== Buffer Copy Performance ===" << std::endl;
    std::cout << "Iterations: " << iterations << ", Buffer size: " << buffer_size << " bytes" << std::endl;

    // Prepare source buffers
    std::vector<AlignedBuffer> source_buffers;
    for (size_t i = 0; i < 100; ++i) {
        AlignedBuffer src;
        for (size_t j = 0; j < buffer_size / sizeof(uint64_t); ++j) {
            src.write(static_cast<uint64_t>(j));
        }
        source_buffers.push_back(src);
    }

    AlignedBuffer dest;
    BenchmarkTimer timer("Buffer copies");

    for (size_t i = 0; i < iterations; ++i) {
        AlignedBuffer& src = source_buffers[i % source_buffers.size()];
        dest.write(src);
    }

    timer.report(iterations, iterations * buffer_size);
}

int main() {
    std::cout << "AlignedBuffer Performance Benchmark" << std::endl;
    std::cout << "===================================" << std::endl;

    benchmark_sequential_writes();
    benchmark_mixed_workload();
    benchmark_string_writes();
    benchmark_buffer_copies();

    std::cout << "\n=== Benchmark Complete ===" << std::endl;

    return 0;
}