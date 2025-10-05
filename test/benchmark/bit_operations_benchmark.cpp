#include <benchmark/benchmark.h>
#include <vector>
#include <random>
#include <immintrin.h>
#include <x86intrin.h>

#include "../../lib/utils/util.hpp"
#include "../../lib/storage/compressed_buffer.hpp"
#include "../../lib/encoding/float_encoder.hpp"

// Test data generators
std::vector<double> generate_random_floats(size_t count, double min_val = -1000.0, double max_val = 1000.0) {
    std::mt19937 gen(42);  // Fixed seed for reproducibility
    std::uniform_real_distribution<double> dist(min_val, max_val);

    std::vector<double> values;
    values.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        values.push_back(dist(gen));
    }
    return values;
}

std::vector<double> generate_compressible_floats(size_t count) {
    std::vector<double> values;
    values.reserve(count);

    double base = 100.0;
    for (size_t i = 0; i < count; ++i) {
        // Generate values with small deltas (more compressible)
        base += (i % 10 == 0) ? 0.1 : 0.001;
        values.push_back(base);
    }
    return values;
}

std::vector<uint64_t> generate_xor_values(size_t count) {
    auto floats = generate_random_floats(count);
    std::vector<uint64_t> xor_values;
    xor_values.reserve(count - 1);

    uint64_t last = *reinterpret_cast<uint64_t*>(&floats[0]);
    for (size_t i = 1; i < count; ++i) {
        uint64_t current = *reinterpret_cast<uint64_t*>(&floats[i]);
        xor_values.push_back(current ^ last);
        last = current;
    }
    return xor_values;
}

// ==================== LZB/TZB Calculation Benchmarks ====================

// Current implementation (using intrinsics)
static void BM_LZB_Current_Intrinsics(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    size_t sum = 0;

    for (auto _ : state) {
        for (const auto& val : xor_values) {
            if (val != 0) {
                sum += getLeadingZeroBitsUnsafe(val);
            } else {
                sum += 64;
            }
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_LZB_Current_Intrinsics);

static void BM_TZB_Current_Intrinsics(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    size_t sum = 0;

    for (auto _ : state) {
        for (const auto& val : xor_values) {
            if (val != 0) {
                sum += getTrailingZeroBitsUnsafe(val);
            } else {
                sum += 64;
            }
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_TZB_Current_Intrinsics);

// Manual loop implementation for comparison
template<typename T>
inline size_t getLeadingZeroBits_Manual(T x) {
    if (x == 0) return sizeof(x) * 8;

    size_t count = 0;
    T mask = T(1) << (sizeof(T) * 8 - 1);

    while ((x & mask) == 0) {
        count++;
        x <<= 1;
    }
    return count;
}

template<typename T>
inline size_t getTrailingZeroBits_Manual(T x) {
    if (x == 0) return sizeof(x) * 8;

    size_t count = 0;
    while ((x & 1) == 0) {
        count++;
        x >>= 1;
    }
    return count;
}

static void BM_LZB_Manual_Loop(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    size_t sum = 0;

    for (auto _ : state) {
        for (const auto& val : xor_values) {
            sum += getLeadingZeroBits_Manual(val);
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_LZB_Manual_Loop);

static void BM_TZB_Manual_Loop(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    size_t sum = 0;

    for (auto _ : state) {
        for (const auto& val : xor_values) {
            sum += getTrailingZeroBits_Manual(val);
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_TZB_Manual_Loop);

// ==================== XOR Operation Benchmarks ====================

// Standard scalar XOR
static void BM_XOR_Scalar(benchmark::State& state) {
    auto floats = generate_random_floats(10000);
    std::vector<uint64_t> results;
    results.reserve(floats.size() - 1);

    for (auto _ : state) {
        results.clear();
        uint64_t last = *reinterpret_cast<uint64_t*>(&floats[0]);

        for (size_t i = 1; i < floats.size(); ++i) {
            uint64_t current = *reinterpret_cast<uint64_t*>(&floats[i]);
            results.push_back(current ^ last);
            last = current;
        }
    }
    benchmark::DoNotOptimize(results);
}
BENCHMARK(BM_XOR_Scalar);

// SIMD XOR using AVX2 (process 4 uint64_t at once)
static void BM_XOR_SIMD_AVX2(benchmark::State& state) {
    auto floats = generate_random_floats(10000);
    // Ensure size is multiple of 4 for SIMD
    while (floats.size() % 4 != 1) floats.pop_back();

    std::vector<uint64_t> results;
    results.resize(floats.size() - 1);

    for (auto _ : state) {
        const uint64_t* input = reinterpret_cast<const uint64_t*>(floats.data());
        uint64_t* output = results.data();

        // Process first element separately
        uint64_t last = input[0];

        // Process in chunks of 4
        size_t simd_count = (floats.size() - 1) / 4;
        for (size_t i = 0; i < simd_count; ++i) {
            __m256i current = _mm256_loadu_si256((__m256i*)(input + 1 + i * 4));
            __m256i prev = _mm256_set_epi64x(
                (i * 4 + 3 < floats.size() - 1) ? input[i * 4 + 3] : input[i * 4 + 2],
                (i * 4 + 2 < floats.size() - 1) ? input[i * 4 + 2] : input[i * 4 + 1],
                (i * 4 + 1 < floats.size() - 1) ? input[i * 4 + 1] : input[i * 4],
                (i == 0) ? last : input[i * 4]
            );

            __m256i xor_result = _mm256_xor_si256(current, prev);
            _mm256_storeu_si256((__m256i*)(output + i * 4), xor_result);
        }

        // Handle remaining elements
        for (size_t i = simd_count * 4; i < floats.size() - 1; ++i) {
            results[i] = input[i + 1] ^ input[i];
        }
    }
    benchmark::DoNotOptimize(results);
}
BENCHMARK(BM_XOR_SIMD_AVX2);

// ==================== Combined Bit Operations Benchmarks ====================

// Current approach: XOR + LZB + TZB in sequence
static void BM_Combined_Current(benchmark::State& state) {
    auto floats = generate_random_floats(10000);
    struct BitStats {
        size_t lzb, tzb;
        uint64_t xor_val;
    };

    std::vector<BitStats> results;
    results.reserve(floats.size() - 1);

    for (auto _ : state) {
        results.clear();
        uint64_t last = *reinterpret_cast<uint64_t*>(&floats[0]);

        for (size_t i = 1; i < floats.size(); ++i) {
            uint64_t current = *reinterpret_cast<uint64_t*>(&floats[i]);
            uint64_t xor_val = current ^ last;

            BitStats stats;
            stats.xor_val = xor_val;
            if (xor_val != 0) {
                stats.lzb = getLeadingZeroBitsUnsafe(xor_val);
                stats.tzb = getTrailingZeroBitsUnsafe(xor_val);
            } else {
                stats.lzb = stats.tzb = 64;
            }

            results.push_back(stats);
            last = current;
        }
    }
    benchmark::DoNotOptimize(results);
}
BENCHMARK(BM_Combined_Current);

// Optimized: batch XOR then batch bit counting
static void BM_Combined_Batched(benchmark::State& state) {
    auto floats = generate_random_floats(10000);
    struct BitStats {
        size_t lzb, tzb;
        uint64_t xor_val;
    };

    std::vector<BitStats> results;
    results.reserve(floats.size() - 1);

    for (auto _ : state) {
        results.clear();
        results.resize(floats.size() - 1);

        const uint64_t* input = reinterpret_cast<const uint64_t*>(floats.data());

        // Batch XOR calculations
        uint64_t last = input[0];
        for (size_t i = 0; i < floats.size() - 1; ++i) {
            uint64_t current = input[i + 1];
            results[i].xor_val = current ^ last;
            last = current;
        }

        // Batch bit counting
        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].xor_val != 0) {
                results[i].lzb = getLeadingZeroBitsUnsafe(results[i].xor_val);
                results[i].tzb = getTrailingZeroBitsUnsafe(results[i].xor_val);
            } else {
                results[i].lzb = results[i].tzb = 64;
            }
        }
    }
    benchmark::DoNotOptimize(results);
}
BENCHMARK(BM_Combined_Batched);

// ==================== Buffer Write Operation Benchmarks ====================

// Test the compressed buffer write operations
static void BM_CompressedBuffer_Write_Pattern(benchmark::State& state) {
    const size_t count = 1000;
    std::vector<uint64_t> test_data = {
        0,           // 1-bit write (common case)
        0b01,        // 2-bit write
        0b11,        // 2-bit write
        31,          // 5-bit write (LZB)
        63,          // 6-bit write (data bits)
        0xFFFFFFFFFFFFFFFFULL  // 64-bit write (full value)
    };

    for (auto _ : state) {
        CompressedBuffer buffer;

        for (size_t i = 0; i < count; ++i) {
            // Simulate the float encoder write pattern
            buffer.writeFixed<0b0, 1>();  // Most values are zero deltas

            if (i % 10 == 0) {  // 10% need metadata
                buffer.writeFixed<0b11, 2>();
                buffer.write<5>(test_data[4]);
                buffer.write<6>(test_data[5]);
                buffer.write(test_data[0], 32);  // Some data bits
            }
        }
    }
}
BENCHMARK(BM_CompressedBuffer_Write_Pattern);

// Test different write sizes separately
static void BM_CompressedBuffer_Write_1Bit(benchmark::State& state) {
    CompressedBuffer buffer;

    for (auto _ : state) {
        state.PauseTiming();
        buffer.rewind();
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i) {
            buffer.writeFixed<0b0, 1>();
        }
    }
}
BENCHMARK(BM_CompressedBuffer_Write_1Bit)->Range(1000, 10000);

static void BM_CompressedBuffer_Write_2Bit(benchmark::State& state) {
    CompressedBuffer buffer;

    for (auto _ : state) {
        state.PauseTiming();
        buffer.rewind();
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i) {
            buffer.writeFixed<0b01, 2>();
        }
    }
}
BENCHMARK(BM_CompressedBuffer_Write_2Bit)->Range(1000, 10000);

static void BM_CompressedBuffer_Write_Variable(benchmark::State& state) {
    CompressedBuffer buffer;
    const std::vector<int> bit_sizes = {1, 5, 6, 32, 64};

    for (auto _ : state) {
        state.PauseTiming();
        buffer.rewind();
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i) {
            int bits = bit_sizes[i % bit_sizes.size()];
            buffer.write(0xAAAAAAAAAAAAAAAAULL, bits);
        }
    }
}
BENCHMARK(BM_CompressedBuffer_Write_Variable)->Range(1000, 10000);

// ==================== End-to-End Encoder Benchmarks ====================

// Full float encoder benchmark with different data patterns
static void BM_FloatEncoder_Random_Data(benchmark::State& state) {
    auto values = generate_random_floats(state.range(0));

    for (auto _ : state) {
        auto result = FloatEncoder::encode(values);
        benchmark::DoNotOptimize(result);
    }

    state.SetComplexityN(state.range(0));
    state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(double));
}
BENCHMARK(BM_FloatEncoder_Random_Data)->Range(1000, 50000)->Complexity();

static void BM_FloatEncoder_Compressible_Data(benchmark::State& state) {
    auto values = generate_compressible_floats(state.range(0));

    for (auto _ : state) {
        auto result = FloatEncoder::encode(values);
        benchmark::DoNotOptimize(result);
    }

    state.SetComplexityN(state.range(0));
    state.SetBytesProcessed(state.iterations() * state.range(0) * sizeof(double));
}
BENCHMARK(BM_FloatEncoder_Compressible_Data)->Range(1000, 50000)->Complexity();

// ==================== Specialized Optimization Tests ====================

// Test if manual loop unrolling helps bit operations
static void BM_LZB_Unrolled(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    // Ensure size is multiple of 4
    while (xor_values.size() % 4 != 0) xor_values.pop_back();

    size_t sum = 0;

    for (auto _ : state) {
        for (size_t i = 0; i < xor_values.size(); i += 4) {
            // Unroll loop for 4 operations
            sum += (xor_values[i] != 0) ? getLeadingZeroBitsUnsafe(xor_values[i]) : 64;
            sum += (xor_values[i+1] != 0) ? getLeadingZeroBitsUnsafe(xor_values[i+1]) : 64;
            sum += (xor_values[i+2] != 0) ? getLeadingZeroBitsUnsafe(xor_values[i+2]) : 64;
            sum += (xor_values[i+3] != 0) ? getLeadingZeroBitsUnsafe(xor_values[i+3]) : 64;
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_LZB_Unrolled);

// Test branch-free bit counting
inline size_t getLeadingZeroBits_BranchFree(uint64_t x) {
    // Use conditional move instead of branch
    return (x == 0) ? 64 : __builtin_clzll(x);
}

static void BM_LZB_BranchFree(benchmark::State& state) {
    auto xor_values = generate_xor_values(10000);
    size_t sum = 0;

    for (auto _ : state) {
        for (const auto& val : xor_values) {
            sum += getLeadingZeroBits_BranchFree(val);
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_LZB_BranchFree);

// ==================== Memory Access Pattern Tests ====================

// Test cache-friendly vs cache-unfriendly patterns
static void BM_MemoryPattern_Sequential(benchmark::State& state) {
    auto floats = generate_random_floats(state.range(0));
    size_t sum = 0;

    for (auto _ : state) {
        // Sequential access pattern
        for (size_t i = 0; i < floats.size(); ++i) {
            sum += *reinterpret_cast<uint64_t*>(&floats[i]);
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_MemoryPattern_Sequential)->Range(1000, 50000);

static void BM_MemoryPattern_Strided(benchmark::State& state) {
    auto floats = generate_random_floats(state.range(0));
    size_t sum = 0;

    for (auto _ : state) {
        // Strided access pattern (every 8th element)
        for (size_t i = 0; i < floats.size(); i += 8) {
            sum += *reinterpret_cast<uint64_t*>(&floats[i]);
        }
    }
    benchmark::DoNotOptimize(sum);
}
BENCHMARK(BM_MemoryPattern_Strided)->Range(1000, 50000);

BENCHMARK_MAIN();