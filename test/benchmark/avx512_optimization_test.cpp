#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <cmath>
#include "../../lib/encoding/float_encoder_avx512.hpp"
#include "../../lib/encoding/float_encoder_avx512_optimized.hpp"

using namespace std::chrono;

int main() {
    std::cout << "\n=== AVX-512 Optimization Analysis ===\n" << std::endl;
    
    // Check CPU support
    std::cout << "CPU Feature Support:" << std::endl;
    std::cout << "  AVX-512F:  " << (FloatEncoderAVX512::isAvailable() ? "✓" : "✗") << std::endl;
    std::cout << "  AVX-512CD: " << (FloatEncoderAVX512Optimized::hasAVX512CD() ? "✓" : "✗") << std::endl;
    std::cout << "  AVX-512BW: " << (FloatEncoderAVX512Optimized::hasAVX512BW() ? "✓" : "✗") << std::endl;
    std::cout << "  AVX-512VL: " << (FloatEncoderAVX512Optimized::hasAVX512VL() ? "✓" : "✗") << std::endl;
    
    if (!FloatEncoderAVX512::isAvailable()) {
        std::cout << "\nAVX-512 not available on this CPU" << std::endl;
        return 0;
    }
    
    // Generate test data
    std::vector<double> data;
    for (int i = 0; i < 100000; i++) {
        data.push_back(20.0 + sin(i * 0.1) * 5.0);
    }
    
    std::cout << "\nBenchmarking with " << data.size() << " values..." << std::endl;
    
    // Warmup
    for (int i = 0; i < 3; i++) {
        auto r1 = FloatEncoderAVX512::encode(data);
        auto r2 = FloatEncoderAVX512Optimized::encode(data);
    }
    
    const int runs = 10;
    
    // Original AVX-512
    double original_time = 0;
    for (int r = 0; r < runs; r++) {
        auto start = high_resolution_clock::now();
        auto result = FloatEncoderAVX512::encode(data);
        auto end = high_resolution_clock::now();
        original_time += duration_cast<microseconds>(end - start).count() / 1000.0;
    }
    original_time /= runs;
    
    // Optimized AVX-512
    double optimized_time = 0;
    for (int r = 0; r < runs; r++) {
        auto start = high_resolution_clock::now();
        auto result = FloatEncoderAVX512Optimized::encode(data);
        auto end = high_resolution_clock::now();
        optimized_time += duration_cast<microseconds>(end - start).count() / 1000.0;
    }
    optimized_time /= runs;
    
    // Ultra-optimized (if AVX-512CD available)
    double ultra_time = 0;
    if (FloatEncoderAVX512Optimized::hasAVX512CD()) {
        for (int r = 0; r < runs; r++) {
            auto start = high_resolution_clock::now();
            auto result = FloatEncoderAVX512Optimized::encodeUltra(data);
            auto end = high_resolution_clock::now();
            ultra_time += duration_cast<microseconds>(end - start).count() / 1000.0;
        }
        ultra_time /= runs;
    }
    
    // Results
    std::cout << "\nResults:" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    std::cout << std::setw(20) << "Implementation" << std::setw(15) << "Time (ms)" 
              << std::setw(15) << "Throughput" << std::setw(10) << "Speedup" << std::endl;
    std::cout << std::string(60, '-') << std::endl;
    
    size_t bytes = data.size() * sizeof(double);
    double original_throughput = (bytes / (1024.0 * 1024.0)) / (original_time / 1000.0);
    double optimized_throughput = (bytes / (1024.0 * 1024.0)) / (optimized_time / 1000.0);
    
    std::cout << std::fixed << std::setprecision(3);
    std::cout << std::setw(20) << "Original AVX-512" << std::setw(15) << original_time
              << std::setw(12) << original_throughput << " MB/s"
              << std::setw(10) << "1.00x" << std::endl;
    
    std::cout << std::setw(20) << "Optimized AVX-512" << std::setw(15) << optimized_time
              << std::setw(12) << optimized_throughput << " MB/s"
              << std::setw(8) << std::setprecision(2) << (original_time / optimized_time) << "x" << std::endl;
    
    if (FloatEncoderAVX512Optimized::hasAVX512CD() && ultra_time > 0) {
        double ultra_throughput = (bytes / (1024.0 * 1024.0)) / (ultra_time / 1000.0);
        std::cout << std::setw(20) << "Ultra AVX-512" << std::setw(15) << ultra_time
                  << std::setw(12) << ultra_throughput << " MB/s"
                  << std::setw(8) << std::setprecision(2) << (original_time / ultra_time) << "x" << std::endl;
    }
    
    std::cout << "\nOptimization Impact: " 
              << std::setprecision(1) << ((original_time / optimized_time - 1) * 100) 
              << "% improvement" << std::endl;
    
    return 0;
}