/*
 * Encoding & Infrastructure Micro-Benchmarks
 *
 * Targeted benchmarks for:
 *   E1 — String encoder round-trip (zstd context reuse)
 *   E2 — CRC32 throughput (slicing-by-8)
 *
 * Run with: --gtest_filter='EncodingBenchmark*'
 */

#include "../seastar_gtest.hpp"
#include "../test_helpers.hpp"

#include "crc32.hpp"
#include "string_encoder.hpp"
#include "aligned_buffer.hpp"

#include <gtest/gtest.h>
#include <seastar/core/coroutine.hh>

#include <chrono>
#include <fmt/core.h>
#include <random>
#include <string>
#include <vector>

using clk = std::chrono::high_resolution_clock;

class EncodingBenchmark : public ::testing::Test {};

// ═══════════════════════════════════════════════════════════════════════════
//  E1: String Encoder Round-Trip
//  Measures encode + decode of string batches. Zstd context reuse should
//  eliminate ~200KB alloc per encode and ~130KB per decode.
// ═══════════════════════════════════════════════════════════════════════════

TEST_F(EncodingBenchmark, E1_StringEncoderRoundTrip) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  E1: String Encoder Round-Trip                                 ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    // Generate test strings
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> lenDist(10, 100);

    std::vector<std::string> strings;
    strings.reserve(1000);
    for (int i = 0; i < 1000; i++) {
        int len = lenDist(rng);
        std::string s(len, ' ');
        for (int j = 0; j < len; j++) {
            s[j] = 'a' + (rng() % 26);
        }
        strings.push_back(std::move(s));
    }

    // Warm up
    for (int i = 0; i < 5; i++) {
        auto encoded = StringEncoder::encode(strings);
        std::vector<std::string> decoded;
        StringEncoder::decode(encoded, strings.size(), decoded);
    }

    // Benchmark encode
    constexpr int ITERS = 200;
    auto t0 = clk::now();
    for (int i = 0; i < ITERS; i++) {
        auto encoded = StringEncoder::encode(strings);
    }
    double encodeUs = std::chrono::duration<double, std::micro>(clk::now() - t0).count() / ITERS;

    // Benchmark decode
    auto encoded = StringEncoder::encode(strings);
    auto t1 = clk::now();
    for (int i = 0; i < ITERS; i++) {
        std::vector<std::string> decoded;
        StringEncoder::decode(encoded, strings.size(), decoded);
    }
    double decodeUs = std::chrono::duration<double, std::micro>(clk::now() - t1).count() / ITERS;

    fmt::print("  Encode 1000 strings ({} iters):  {:.1f} µs/iter\n", ITERS, encodeUs);
    fmt::print("  Decode 1000 strings ({} iters):  {:.1f} µs/iter\n", ITERS, decodeUs);
    fmt::print("  Round-trip:                      {:.1f} µs/iter\n", encodeUs + decodeUs);
}

// ═══════════════════════════════════════════════════════════════════════════
//  E2: CRC32 Throughput
//  Measures CRC32 computation on various buffer sizes.
// ═══════════════════════════════════════════════════════════════════════════

TEST_F(EncodingBenchmark, E2_CRC32Throughput) {
    fmt::print("\n╔══════════════════════════════════════════════════════════════════╗\n");
    fmt::print("║  E2: CRC32 Throughput                                          ║\n");
    fmt::print("╚══════════════════════════════════════════════════════════════════╝\n");

    std::mt19937 rng(42);

    for (size_t bufSize : {4096UL, 65536UL, 1048576UL}) {
        std::vector<uint8_t> data(bufSize);
        for (size_t i = 0; i < bufSize; i++) {
            data[i] = static_cast<uint8_t>(rng());
        }

        // Warm up
        uint32_t sink = 0;
        for (int i = 0; i < 10; i++) {
            sink ^= CRC32::compute(data.data(), data.size());
        }

        int iters = (bufSize <= 65536) ? 10000 : 1000;
        auto t0 = clk::now();
        for (int i = 0; i < iters; i++) {
            sink ^= CRC32::compute(data.data(), data.size());
        }
        double totalUs = std::chrono::duration<double, std::micro>(clk::now() - t0).count();
        double perIterUs = totalUs / iters;
        double mbPerSec = (static_cast<double>(bufSize) / (1024.0 * 1024.0)) / (perIterUs / 1e6);

        fmt::print("  CRC32 {:>7} bytes:  {:.2f} µs/iter  ({:.0f} MB/s)\n",
                   bufSize, perIterUs, mbPerSec);

        // Prevent optimization (use volatile to ensure compiler doesn't eliminate the loop)
        volatile uint32_t vsink = sink;
        (void)vsink;
    }
}
