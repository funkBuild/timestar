#include <iostream>
#include <chrono>
#include <vector>
#include <random>
#include <iomanip>
#include <numeric>
#include "storage/compressed_buffer.hpp"
#include "storage/compressed_buffer_optimized.hpp"
#include "encoding/float_encoder.hpp"

using namespace std::chrono;

struct BenchmarkResult {
    std::string name;
    double original_ms;
    double optimized_ms;
    double speedup;
    double original_throughput;
    double optimized_throughput;
};

class Timer {
    high_resolution_clock::time_point start;
public:
    Timer() : start(high_resolution_clock::now()) {}
    double elapsed_ms() {
        auto end = high_resolution_clock::now();
        return duration_cast<microseconds>(end - start).count() / 1000.0;
    }
};

BenchmarkResult benchmark_sequential_writes(int bits) {
    const size_t iterations = 1000000;
    uint64_t mask = (bits == 64) ? 0xFFFFFFFFFFFFFFFF : ((1ULL << bits) - 1);

    // Original
    double original_ms;
    {
        CompressedBuffer buffer;
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            buffer.write(i & mask, bits);
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized
    double optimized_ms;
    {
        CompressedBufferOptimized buffer;
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            buffer.write(i & mask, bits);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = (iterations * bits) / 8;
    return {
        std::to_string(bits) + "-bit writes",
        original_ms,
        optimized_ms,
        original_ms / optimized_ms,
        (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
        (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)
    };
}

BenchmarkResult benchmark_float_encoder_pattern() {
    const size_t iterations = 100000;

    // Original
    double original_ms;
    {
        CompressedBuffer buffer;
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            if(i % 3 == 0) {
                buffer.write(0b11, 2);
                buffer.write(i & 0x1F, 5);
                buffer.write(i & 0x3F, 6);
            } else {
                buffer.write(0b01, 2);
            }
            buffer.write(i * 123456789, 48);
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized
    double optimized_ms;
    {
        CompressedBufferOptimized buffer;
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            if(i % 3 == 0) {
                buffer.writeFixed<0b11, 2>();
                buffer.write<5>(i & 0x1F);
                buffer.write<6>(i & 0x3F);
            } else {
                buffer.writeFixed<0b01, 2>();
            }
            buffer.write(i * 123456789, 48);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t avg_bits = (2 + 48) + (5 + 6) / 3;
    size_t total_bytes = (iterations * avg_bits) / 8;
    return {
        "Float pattern",
        original_ms,
        optimized_ms,
        original_ms / optimized_ms,
        (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
        (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)
    };
}

BenchmarkResult benchmark_with_preallocation() {
    const size_t iterations = 1000000;
    const size_t expected_words = (iterations * 32) / 64;

    // Original - no preallocation available
    double original_ms;
    {
        CompressedBuffer buffer;
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            buffer.write(i, 32);
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized with preallocation
    double optimized_ms;
    {
        CompressedBufferOptimized buffer;
        buffer.reserve(expected_words);
        Timer timer;
        for(size_t i = 0; i < iterations; ++i) {
            buffer.write(i, 32);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = (iterations * 32) / 8;
    return {
        "Pre-allocated 32-bit",
        original_ms,
        optimized_ms,
        original_ms / optimized_ms,
        (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
        (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)
    };
}

BenchmarkResult benchmark_bulk_writes() {
    const size_t array_size = 10000;
    const size_t iterations = 100;

    std::vector<uint64_t> data(array_size);
    for(size_t i = 0; i < array_size; ++i) {
        data[i] = i * 123456789;
    }

    // Original - one by one
    double original_ms;
    {
        CompressedBuffer buffer;
        Timer timer;
        for(size_t iter = 0; iter < iterations; ++iter) {
            for(size_t i = 0; i < array_size; ++i) {
                buffer.write(data[i], 48);
            }
        }
        original_ms = timer.elapsed_ms();
    }

    // Optimized - bulk write
    double optimized_ms;
    {
        CompressedBufferOptimized buffer;
        Timer timer;
        for(size_t iter = 0; iter < iterations; ++iter) {
            buffer.write_bulk(data.data(), array_size, 48);
        }
        optimized_ms = timer.elapsed_ms();
    }

    size_t total_bytes = (iterations * array_size * 48) / 8;
    return {
        "Bulk 48-bit writes",
        original_ms,
        optimized_ms,
        original_ms / optimized_ms,
        (total_bytes / (1024.0 * 1024.0)) / (original_ms / 1000.0),
        (total_bytes / (1024.0 * 1024.0)) / (optimized_ms / 1000.0)
    };
}

void memory_efficiency_test() {
    std::cout << "\n═══════════════════════════════════════════════════════" << std::endl;
    std::cout << "              MEMORY EFFICIENCY COMPARISON              " << std::endl;
    std::cout << "═══════════════════════════════════════════════════════" << std::endl;

    const size_t num_writes = 10000;

    // Original
    {
        CompressedBuffer buffer;
        for(size_t i = 0; i < num_writes; ++i) {
            buffer.write(i, 32);
        }
        std::cout << "\nOriginal CompressedBuffer:" << std::endl;
        std::cout << "  Data size:     " << buffer.dataByteSize() << " bytes" << std::endl;
        std::cout << "  Vector size:   " << buffer.data.size() << " words" << std::endl;
        std::cout << "  Capacity:      " << buffer.data.capacity() * 8 << " bytes" << std::endl;
        std::cout << "  Efficiency:    " << std::fixed << std::setprecision(1)
                  << (100.0 * buffer.data.size() / buffer.data.capacity()) << "%" << std::endl;
    }

    // Optimized without reserve
    {
        CompressedBufferOptimized buffer;
        for(size_t i = 0; i < num_writes; ++i) {
            buffer.write(i, 32);
        }
        std::cout << "\nOptimized (no reserve):" << std::endl;
        std::cout << "  Data size:     " << buffer.dataByteSize() << " bytes" << std::endl;
        std::cout << "  Vector size:   " << buffer.data.size() << " words" << std::endl;
        std::cout << "  Capacity:      " << buffer.capacity() << " bytes" << std::endl;
        std::cout << "  Efficiency:    " << std::fixed << std::setprecision(1)
                  << (100.0 * buffer.data.size() / buffer.data.capacity()) << "%" << std::endl;
    }

    // Optimized with reserve
    {
        CompressedBufferOptimized buffer;
        size_t expected_words = (num_writes * 32 + 63) / 64;
        buffer.reserve(expected_words);
        for(size_t i = 0; i < num_writes; ++i) {
            buffer.write(i, 32);
        }
        buffer.shrink_to_fit();
        std::cout << "\nOptimized (with reserve + shrink):" << std::endl;
        std::cout << "  Data size:     " << buffer.dataByteSize() << " bytes" << std::endl;
        std::cout << "  Vector size:   " << buffer.data.size() << " words" << std::endl;
        std::cout << "  Capacity:      " << buffer.capacity() << " bytes" << std::endl;
        std::cout << "  Efficiency:    " << std::fixed << std::setprecision(1)
                  << (100.0 * buffer.dataByteSize() / buffer.capacity()) << "%" << std::endl;
    }
}

void print_results(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n╔════════════════════════════════════════════════════════════════════════════════╗" << std::endl;
    std::cout << "║              CompressedBuffer Performance Comparison Results                   ║" << std::endl;
    std::cout << "╠════════════════════════════════════════════════════════════════════════════════╣" << std::endl;
    std::cout << "║ Test                │ Original (ms) │ Optimized (ms) │ Speedup │ Throughput     ║" << std::endl;
    std::cout << "╟─────────────────────┼───────────────┼────────────────┼─────────┼─────────────────╢" << std::endl;

    for(const auto& r : results) {
        std::cout << "║ " << std::setw(19) << std::left << r.name
                  << " │ " << std::setw(13) << std::right << std::fixed << std::setprecision(2) << r.original_ms
                  << " │ " << std::setw(14) << std::right << r.optimized_ms
                  << " │ " << std::setw(6) << std::right << std::setprecision(1) << r.speedup << "x"
                  << " │ " << std::setw(8) << std::right << std::setprecision(0) << r.optimized_throughput
                  << " MB/s ║" << std::endl;
    }

    std::cout << "╚════════════════════════════════════════════════════════════════════════════════╝" << std::endl;

    // Calculate average speedup
    double avg_speedup = 0;
    for(const auto& r : results) {
        avg_speedup += r.speedup;
    }
    avg_speedup /= results.size();

    std::cout << "\n📊 Performance Summary:" << std::endl;
    std::cout << "   Average Speedup: " << std::fixed << std::setprecision(2) << avg_speedup << "x" << std::endl;
}

int main() {
    std::cout << "\n🔬 CompressedBuffer Optimization Benchmark" << std::endl;
    std::cout << "   Comparing original vs optimized implementation" << std::endl;

    std::vector<BenchmarkResult> results;

    std::cout << "\n⏱️  Running benchmarks..." << std::endl;

    // Test different bit widths
    std::vector<int> bit_widths = {1, 8, 32, 48, 64};
    for(int bits : bit_widths) {
        std::cout << "   Testing " << bits << "-bit writes..." << std::flush;
        results.push_back(benchmark_sequential_writes(bits));
        std::cout << " ✓" << std::endl;
    }

    std::cout << "   Testing float encoder pattern..." << std::flush;
    results.push_back(benchmark_float_encoder_pattern());
    std::cout << " ✓" << std::endl;

    std::cout << "   Testing pre-allocated writes..." << std::flush;
    results.push_back(benchmark_with_preallocation());
    std::cout << " ✓" << std::endl;

    std::cout << "   Testing bulk writes..." << std::flush;
    results.push_back(benchmark_bulk_writes());
    std::cout << " ✓" << std::endl;

    print_results(results);
    memory_efficiency_test();

    std::cout << "\n✅ Benchmark Complete" << std::endl;
    return 0;
}