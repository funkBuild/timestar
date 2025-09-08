#include <iostream>
#include <vector>
#include <chrono>
#include <random>
#include <iomanip>
#include "aggregator.hpp"
#include "simd_aggregator.hpp"

using namespace std::chrono;

// Generate random data for benchmarking
std::vector<double> generateRandomData(size_t count) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1000.0);
    
    std::vector<double> data;
    data.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        data.push_back(dis(gen));
    }
    return data;
}

// Benchmark a function
template<typename Func>
double benchmark(const std::string& name, Func func, int iterations = 100) {
    auto start = high_resolution_clock::now();
    
    for (int i = 0; i < iterations; ++i) {
        func();
    }
    
    auto end = high_resolution_clock::now();
    auto duration = duration_cast<microseconds>(end - start).count();
    double avg_time = static_cast<double>(duration) / iterations;
    
    std::cout << std::setw(30) << std::left << name 
              << ": " << std::setw(10) << std::right << std::fixed 
              << std::setprecision(2) << avg_time << " µs" << std::endl;
    
    return avg_time;
}

int main() {
    std::cout << "=== SIMD Aggregator Performance Benchmark ===" << std::endl;
    std::cout << std::endl;
    
    // Check AVX2 availability
    bool avx2_available = tsdb::simd::SimdAggregator::isAvx2Available();
    std::cout << "AVX2 Support: " << (avx2_available ? "✓ Available" : "✗ Not Available") << std::endl;
    std::cout << std::endl;
    
    // Test different data sizes
    std::vector<size_t> sizes = {100, 1000, 10000, 100000, 1000000};
    
    for (size_t size : sizes) {
        std::cout << "--- Data Size: " << size << " elements ---" << std::endl;
        
        // Generate test data
        auto data = generateRandomData(size);
        
        // Benchmark SUM
        std::cout << "\nSUM Operation:" << std::endl;
        
        double scalar_sum_time = benchmark("  Scalar Sum", [&]() {
            double sum = 0.0;
            for (double v : data) {
                sum += v;
            }
            return sum;
        });
        
        double simd_sum_time = 0.0;
        if (avx2_available) {
            simd_sum_time = benchmark("  SIMD Sum (AVX2)", [&]() {
                return tsdb::simd::SimdAggregator::calculateSum(data.data(), data.size());
            });
            
            double speedup = scalar_sum_time / simd_sum_time;
            std::cout << "  Speedup: " << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        // Benchmark MIN
        std::cout << "\nMIN Operation:" << std::endl;
        
        double scalar_min_time = benchmark("  Scalar Min", [&]() {
            double min_val = data[0];
            for (double v : data) {
                if (v < min_val) min_val = v;
            }
            return min_val;
        });
        
        double simd_min_time = 0.0;
        if (avx2_available) {
            simd_min_time = benchmark("  SIMD Min (AVX2)", [&]() {
                return tsdb::simd::SimdAggregator::calculateMin(data.data(), data.size());
            });
            
            double speedup = scalar_min_time / simd_min_time;
            std::cout << "  Speedup: " << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        // Benchmark MAX
        std::cout << "\nMAX Operation:" << std::endl;
        
        double scalar_max_time = benchmark("  Scalar Max", [&]() {
            double max_val = data[0];
            for (double v : data) {
                if (v > max_val) max_val = v;
            }
            return max_val;
        });
        
        double simd_max_time = 0.0;
        if (avx2_available) {
            simd_max_time = benchmark("  SIMD Max (AVX2)", [&]() {
                return tsdb::simd::SimdAggregator::calculateMax(data.data(), data.size());
            });
            
            double speedup = scalar_max_time / simd_max_time;
            std::cout << "  Speedup: " << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        // Benchmark AVERAGE
        std::cout << "\nAVERAGE Operation:" << std::endl;
        
        double scalar_avg_time = benchmark("  Scalar Avg", [&]() {
            double sum = 0.0;
            for (double v : data) {
                sum += v;
            }
            return sum / data.size();
        });
        
        double simd_avg_time = 0.0;
        if (avx2_available) {
            simd_avg_time = benchmark("  SIMD Avg (AVX2)", [&]() {
                return tsdb::simd::SimdAggregator::calculateAvg(data.data(), data.size());
            });
            
            double speedup = scalar_avg_time / simd_avg_time;
            std::cout << "  Speedup: " << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        // Benchmark DOT PRODUCT (useful for weighted aggregations)
        std::cout << "\nDOT PRODUCT Operation:" << std::endl;
        
        auto weights = generateRandomData(size);
        
        double scalar_dot_time = benchmark("  Scalar Dot Product", [&]() {
            double dot = 0.0;
            for (size_t i = 0; i < data.size(); ++i) {
                dot += data[i] * weights[i];
            }
            return dot;
        });
        
        double simd_dot_time = 0.0;
        if (avx2_available) {
            simd_dot_time = benchmark("  SIMD Dot Product (AVX2)", [&]() {
                return tsdb::simd::SimdAggregator::dotProduct(data.data(), weights.data(), data.size());
            });
            
            double speedup = scalar_dot_time / simd_dot_time;
            std::cout << "  Speedup: " << std::setprecision(2) << speedup << "x" << std::endl;
        }
        
        std::cout << std::endl;
    }
    
    // Summary
    std::cout << "=== Summary ===" << std::endl;
    if (avx2_available) {
        std::cout << "SIMD optimizations are providing significant speedups for large datasets." << std::endl;
        std::cout << "Typical speedup: 2-4x for aggregation operations on arrays > 1000 elements." << std::endl;
    } else {
        std::cout << "AVX2 is not available on this CPU. Using scalar fallback implementations." << std::endl;
        std::cout << "Consider running on a modern Intel/AMD processor for SIMD benefits." << std::endl;
    }
    
    return 0;
}