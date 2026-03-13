#include "storage/aligned_buffer.hpp"
#include "storage/aligned_buffer_optimized.hpp"

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

using namespace std::chrono;

struct BenchmarkResult {
    std::string name;
    double original_ms;
    double optimized_ms;
    double speedup;
    double original_throughput;
    double optimized_throughput;
};

class BenchmarkTimer {
    high_resolution_clock::time_point start;

public:
    BenchmarkTimer() { start = high_resolution_clock::now(); }

    double elapsed_ms() {
        auto end = high_resolution_clock::now();
        return duration_cast<microseconds>(end - start).count() / 1000.0;
    }
};

BenchmarkResult benchmark_sequential_writes() {
    const size_t iterations = 1000000;
    const size_t warmup = 10000;

    // Warmup
    {
        AlignedBuffer buffer1;
        AlignedBufferOptimized buffer2;
        for (size_t i = 0; i < warmup; ++i) {
            buffer1.write(static_cast<uint64_t>(i));
            buffer2.write(static_cast<uint64_t>(i));
        }
    }

    // Original version
    double original_ms;
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version
    double optimized_ms;
    {
        AlignedBufferOptimized buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = iterations * sizeof(uint64_t);
    return {"Sequential uint64_t writes",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

BenchmarkResult benchmark_mixed_writes() {
    const size_t iterations = 100000;

    // Original version
    double original_ms;
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
            buffer.write(static_cast<uint32_t>(i));
            buffer.write(static_cast<uint16_t>(i));
            buffer.write(static_cast<uint8_t>(i));
            buffer.write(static_cast<double>(i * 1.1));
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version
    double optimized_ms;
    {
        AlignedBufferOptimized buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
            buffer.write(static_cast<uint32_t>(i));
            buffer.write(static_cast<uint16_t>(i));
            buffer.write(static_cast<uint8_t>(i));
            buffer.write(static_cast<double>(i * 1.1));
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes =
        iterations * (sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(double));
    return {"Mixed type writes",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

BenchmarkResult benchmark_string_writes() {
    const size_t iterations = 100000;

    std::vector<std::string> test_strings;
    for (size_t i = 0; i < 100; ++i) {
        test_strings.push_back("Test string " + std::to_string(i));
    }

    size_t total_bytes = 0;
    for (auto& s : test_strings) {
        total_bytes += s.length() * (iterations / test_strings.size());
    }

    // Original version
    double original_ms;
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(test_strings[i % test_strings.size()]);
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version
    double optimized_ms;
    {
        AlignedBufferOptimized buffer;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(test_strings[i % test_strings.size()]);
        }
        optimized_ms = timer.elapsed_ms();
    }

    return {"String writes",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

BenchmarkResult benchmark_large_buffer_writes() {
    const size_t iterations = 10000;       // Increased iterations for better timing
    const size_t buffer_size = 1024 * 10;  // 10KB buffers

    // Prepare source buffers
    std::vector<AlignedBuffer> source_buffers_orig;
    std::vector<AlignedBufferOptimized> source_buffers_opt;

    for (size_t i = 0; i < 10; ++i) {
        AlignedBuffer src1;
        AlignedBufferOptimized src2;
        for (size_t j = 0; j < buffer_size / sizeof(uint64_t); ++j) {
            src1.write(static_cast<uint64_t>(j));
            src2.write(static_cast<uint64_t>(j));
        }
        source_buffers_orig.push_back(src1);
        source_buffers_opt.push_back(src2);
    }

    // Original version
    double original_ms;
    {
        AlignedBuffer dest;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            dest.write(source_buffers_orig[i % source_buffers_orig.size()]);
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version
    double optimized_ms;
    {
        AlignedBufferOptimized dest;
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            dest.write(source_buffers_opt[i % source_buffers_opt.size()]);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = iterations * buffer_size;
    return {"Large buffer copies (10KB)",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

BenchmarkResult benchmark_bulk_writes() {
    const size_t iterations = 1000;  // Increased for better timing
    const size_t array_size = 10000;

    std::vector<uint64_t> data(array_size);
    for (size_t i = 0; i < array_size; ++i) {
        data[i] = i;
    }

    // Original version - has to write one by one
    double original_ms;
    {
        AlignedBuffer buffer;
        BenchmarkTimer timer;
        for (size_t iter = 0; iter < iterations; ++iter) {
            for (size_t i = 0; i < array_size; ++i) {
                buffer.write(data[i]);
            }
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version - can use bulk write
    double optimized_ms;
    {
        AlignedBufferOptimized buffer;
        BenchmarkTimer timer;
        for (size_t iter = 0; iter < iterations; ++iter) {
            buffer.write_array(data.data(), array_size);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = iterations * array_size * sizeof(uint64_t);
    return {"Bulk array writes (10K elements)",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

BenchmarkResult benchmark_preallocated() {
    const size_t iterations = 1000000;
    const size_t expected_size = iterations * sizeof(uint64_t);

    // Original version - with initial size for fair comparison
    double original_ms;
    {
        AlignedBuffer buffer(expected_size);
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized version with preallocation
    double optimized_ms;
    {
        AlignedBufferOptimized buffer;
        buffer.reserve(expected_size);
        BenchmarkTimer timer;
        for (size_t i = 0; i < iterations; ++i) {
            buffer.write(static_cast<uint64_t>(i));
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = iterations * sizeof(uint64_t);
    return {"Pre-allocated writes",
            original_ms,
            optimized_ms,
            original_ms / optimized_ms,
            (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
            (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)};
}

void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n╔════════════════════════════════════════════════════════════════════════════════════════╗"
              << std::endl;
    std::cout << "║                         AlignedBuffer Performance Comparison                              ║"
              << std::endl;
    std::cout << "╠════════════════════════════════════════════════════════════════════════════════════════╣"
              << std::endl;
    std::cout << "║ Benchmark                      │ Original (ms) │ Optimized (ms) │ Speedup │ Throughput  ║"
              << std::endl;
    std::cout << "╟────────────────────────────────┼───────────────┼────────────────┼─────────┼──────────────╢"
              << std::endl;

    for (const auto& r : results) {
        std::cout << "║ " << std::setw(30) << std::left << r.name << " │ " << std::setw(13) << std::right << std::fixed
                  << std::setprecision(2) << r.original_ms << " │ " << std::setw(14) << std::right << r.optimized_ms
                  << " │ " << std::setw(6) << std::right << std::setprecision(1) << r.speedup << "x"
                  << " │ " << std::setw(6) << std::right << std::setprecision(0) << r.optimized_throughput << " MB/s ║"
                  << std::endl;
    }

    std::cout << "╚════════════════════════════════════════════════════════════════════════════════════════╝"
              << std::endl;

    // Calculate average speedup
    double avg_speedup = 0;
    for (const auto& r : results) {
        avg_speedup += r.speedup;
    }
    avg_speedup /= results.size();

    std::cout << "\n📊 Summary:" << std::endl;
    std::cout << "   Average Speedup: " << std::fixed << std::setprecision(2) << avg_speedup << "x" << std::endl;

    // Find best and worst improvements
    auto best = std::max_element(results.begin(), results.end(),
                                 [](const auto& a, const auto& b) { return a.speedup < b.speedup; });
    auto worst = std::min_element(results.begin(), results.end(),
                                  [](const auto& a, const auto& b) { return a.speedup < b.speedup; });

    std::cout << "   Best Improvement: " << best->name << " (" << std::setprecision(2) << best->speedup << "x)"
              << std::endl;
    std::cout << "   Least Improvement: " << worst->name << " (" << std::setprecision(2) << worst->speedup << "x)"
              << std::endl;
}

int main() {
    std::cout << "\n🔬 Running AlignedBuffer Performance Comparison..." << std::endl;
    std::cout << "   Comparing original vs optimized implementation" << std::endl;

    std::vector<BenchmarkResult> results;

    std::cout << "\n⏱️  Running benchmarks..." << std::endl;

    std::cout << "   1/6 Sequential writes..." << std::flush;
    results.push_back(benchmark_sequential_writes());
    std::cout << " ✓" << std::endl;

    std::cout << "   2/6 Mixed type writes..." << std::flush;
    results.push_back(benchmark_mixed_writes());
    std::cout << " ✓" << std::endl;

    std::cout << "   3/6 String writes..." << std::flush;
    results.push_back(benchmark_string_writes());
    std::cout << " ✓" << std::endl;

    std::cout << "   4/6 Large buffer copies..." << std::flush;
    results.push_back(benchmark_large_buffer_writes());
    std::cout << " ✓" << std::endl;

    std::cout << "   5/6 Bulk array writes..." << std::flush;
    results.push_back(benchmark_bulk_writes());
    std::cout << " ✓" << std::endl;

    std::cout << "   6/6 Pre-allocated writes..." << std::flush;
    results.push_back(benchmark_preallocated());
    std::cout << " ✓" << std::endl;

    print_results(results);

    return 0;
}