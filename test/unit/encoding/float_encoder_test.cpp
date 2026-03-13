#include "encoding/float_encoder.hpp"

#include "encoding/float/float_decoder.hpp"
#include "encoding/float/float_encoder.hpp"
#include "encoding/float/float_encoder_avx512.hpp"
#include "encoding/float/float_encoder_simd.hpp"
#include "storage/compressed_buffer.hpp"
#include "storage/slice_buffer.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <random>
#include <sstream>
#include <stdexcept>
#include <vector>

class FloatEncoderTest : public ::testing::Test {
protected:
    // Test data generators
    std::vector<double> generateConstantData(size_t size, double value) { return std::vector<double>(size, value); }

    std::vector<double> generateLinearData(size_t size, double start, double step) {
        std::vector<double> data;
        data.reserve(size);
        for (size_t i = 0; i < size; i++) {
            data.push_back(start + i * step);
        }
        return data;
    }

    std::vector<double> generateSinusoidalData(size_t size, double amplitude, double frequency) {
        std::vector<double> data;
        data.reserve(size);
        for (size_t i = 0; i < size; i++) {
            data.push_back(amplitude * sin(i * frequency));
        }
        return data;
    }

    std::vector<double> generateRandomData(size_t size, double min, double max) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(min, max);

        std::vector<double> data;
        data.reserve(size);
        for (size_t i = 0; i < size; i++) {
            data.push_back(dis(gen));
        }
        return data;
    }

    // Helper to test encode/decode roundtrip
    void testRoundtrip(const std::vector<double>& original) {
        // Encode
        CompressedBuffer encoded = FloatEncoder::encode(original);

        // Decode
        encoded.rewind();
        CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
        std::vector<double> decoded;
        FloatDecoder::decode(slice, 0, original.size(), decoded);

        // Verify
        ASSERT_EQ(decoded.size(), original.size());
        for (size_t i = 0; i < original.size(); i++) {
            EXPECT_DOUBLE_EQ(decoded[i], original[i]) << "Mismatch at index " << i;
        }
    }

    // Helper to test specific encoder implementation
    template <typename EncoderType>
    void testEncoderRoundtrip(const std::vector<double>& original) {
        // Encode with specific encoder
        CompressedBuffer encoded = EncoderType::encode(original);

        // Decode (all encoders use same decoder)
        encoded.rewind();
        CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
        std::vector<double> decoded;
        FloatEncoderBasic::decode(slice, 0, original.size(), decoded);

        // Verify
        ASSERT_EQ(decoded.size(), original.size());
        for (size_t i = 0; i < original.size(); i++) {
            EXPECT_DOUBLE_EQ(decoded[i], original[i]) << "Mismatch at index " << i;
        }
    }
};

// Test the main selector
TEST_F(FloatEncoderTest, AutoSelection) {
    std::string impl = FloatEncoder::getImplementationName();
    std::cout << "Selected implementation: " << impl << std::endl;

    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        EXPECT_TRUE(impl.find("ALP") != std::string::npos)
            << "ALP compression is active, expected 'ALP' in name but got: " << impl;
    } else {
        if (FloatEncoder::hasAVX512()) {
            EXPECT_TRUE(impl.find("AVX-512") != std::string::npos);
        } else if (FloatEncoder::hasAVX2()) {
            EXPECT_TRUE(impl.find("AVX2") != std::string::npos);
        } else {
            EXPECT_TRUE(impl.find("Basic") != std::string::npos);
        }
    }
}

// Test constant values
TEST_F(FloatEncoderTest, ConstantValues) {
    auto data = generateConstantData(1000, 42.5);
    testRoundtrip(data);
}

// Test linear progression
TEST_F(FloatEncoderTest, LinearProgression) {
    auto data = generateLinearData(1000, 0.0, 0.1);
    testRoundtrip(data);
}

// Test sinusoidal data
TEST_F(FloatEncoderTest, SinusoidalData) {
    auto data = generateSinusoidalData(1000, 100.0, 0.01);
    testRoundtrip(data);
}

// Test random data
TEST_F(FloatEncoderTest, RandomData) {
    auto data = generateRandomData(1000, -1000.0, 1000.0);
    testRoundtrip(data);
}

// Test edge cases
TEST_F(FloatEncoderTest, EdgeCases) {
    // Test empty vector
    std::vector<double> empty;
    testRoundtrip(empty);

    // Test single value
    std::vector<double> single = {3.14159};
    testRoundtrip(single);

    // Test special values
    std::vector<double> special = {0.0,
                                   -0.0,
                                   1.0,
                                   -1.0,
                                   std::numeric_limits<double>::min(),
                                   std::numeric_limits<double>::max(),
                                   std::numeric_limits<double>::epsilon(),
                                   std::numeric_limits<double>::infinity(),
                                   -std::numeric_limits<double>::infinity(),
                                   std::numeric_limits<double>::quiet_NaN()};

    CompressedBuffer encoded = FloatEncoder::encode(special);
    encoded.rewind();
    CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, special.size(), decoded);

    ASSERT_EQ(decoded.size(), special.size());
    for (size_t i = 0; i < special.size(); i++) {
        if (std::isnan(special[i])) {
            EXPECT_TRUE(std::isnan(decoded[i]));
        } else {
            EXPECT_EQ(decoded[i], special[i]);
        }
    }
}

// Test large datasets
TEST_F(FloatEncoderTest, LargeDataset) {
    auto data = generateSinusoidalData(100000, 50.0, 0.001);
    testRoundtrip(data);
}

// Test compression ratio
TEST_F(FloatEncoderTest, CompressionRatio) {
    // Highly compressible data (constant)
    auto constant = generateConstantData(1000, 123.456);
    CompressedBuffer encodedConstant = FloatEncoder::encode(constant);
    double ratioConstant = (constant.size() * sizeof(double)) / (double)encodedConstant.dataByteSize();
    EXPECT_GT(ratioConstant, 10.0) << "Constant data should compress well";

    // Moderately compressible (sinusoidal)
    auto sinusoidal = generateSinusoidalData(1000, 100.0, 0.01);
    CompressedBuffer encodedSin = FloatEncoder::encode(sinusoidal);
    double ratioSin = (sinusoidal.size() * sizeof(double)) / (double)encodedSin.dataByteSize();
    EXPECT_GT(ratioSin, 1.0) << "Sinusoidal data should have some compression";

    // Poorly compressible (random)
    auto random = generateRandomData(1000, -1000.0, 1000.0);
    CompressedBuffer encodedRandom = FloatEncoder::encode(random);
    double ratioRandom = (random.size() * sizeof(double)) / (double)encodedRandom.dataByteSize();
    EXPECT_LT(ratioRandom, 1.5) << "Random data should compress poorly";
}

// Test basic encoder specifically
TEST_F(FloatEncoderTest, BasicEncoderRoundtrip) {
    auto data = generateSinusoidalData(1000, 20.0, 0.1);
    testEncoderRoundtrip<FloatEncoderBasic>(data);
}

// Test SIMD encoder if available
TEST_F(FloatEncoderTest, SIMDEncoderRoundtrip) {
    if (!FloatEncoderSIMD::isAvailable()) {
        GTEST_SKIP() << "AVX2 not available";
    }

    auto data = generateSinusoidalData(1000, 20.0, 0.1);
    testEncoderRoundtrip<FloatEncoderSIMD>(data);
}

// Test AVX-512 encoder if available
TEST_F(FloatEncoderTest, AVX512EncoderRoundtrip) {
    if (!FloatEncoderAVX512::isAvailable()) {
        GTEST_SKIP() << "AVX-512 not available";
    }

    auto data = generateSinusoidalData(1000, 20.0, 0.1);
    testEncoderRoundtrip<FloatEncoderAVX512>(data);
}

// Test that all implementations produce identical output
TEST_F(FloatEncoderTest, ImplementationConsistency) {
    auto data = generateSinusoidalData(100, 20.0, 0.1);

    // Encode with basic encoder
    CompressedBuffer basicEncoded = FloatEncoderBasic::encode(data);

    // Compare with SIMD if available
    if (FloatEncoderSIMD::isAvailable()) {
        CompressedBuffer simdEncoded = FloatEncoderSIMD::encode(data);
        EXPECT_EQ(simdEncoded.dataByteSize(), basicEncoded.dataByteSize())
            << "SIMD encoder should produce same size output";
    }

    // Compare with AVX-512 if available
    if (FloatEncoderAVX512::isAvailable()) {
        CompressedBuffer avx512Encoded = FloatEncoderAVX512::encode(data);
        EXPECT_EQ(avx512Encoded.dataByteSize(), basicEncoded.dataByteSize())
            << "AVX-512 encoder should produce same size output";
    }
}

// Test force implementation selection
TEST_F(FloatEncoderTest, ForceImplementation) {
    auto data = generateConstantData(100, 42.0);

    if constexpr (FLOAT_COMPRESSION == FloatCompression::ALP) {
        // ALP is selected at compile time, so setImplementation has no effect
        // on the algorithm used. Verify that the name stays "ALP" regardless
        // of what implementation is forced, and that roundtrip still works.
        FloatEncoder::setImplementation(FloatEncoder::BASIC);
        EXPECT_TRUE(FloatEncoder::getImplementationName().find("ALP") != std::string::npos)
            << "With ALP active, forcing BASIC should still report ALP";
        testRoundtrip(data);

        FloatEncoder::setImplementation(FloatEncoder::SIMD);
        EXPECT_TRUE(FloatEncoder::getImplementationName().find("ALP") != std::string::npos)
            << "With ALP active, forcing SIMD should still report ALP";
        testRoundtrip(data);

        FloatEncoder::setImplementation(FloatEncoder::AVX512);
        EXPECT_TRUE(FloatEncoder::getImplementationName().find("ALP") != std::string::npos)
            << "With ALP active, forcing AVX512 should still report ALP";
        testRoundtrip(data);

        // Reset to auto
        FloatEncoder::setImplementation(FloatEncoder::AUTO);
    } else {
        // Gorilla XOR mode: setImplementation selects between SIMD variants
        FloatEncoder::setImplementation(FloatEncoder::BASIC);
        EXPECT_TRUE(FloatEncoder::getImplementationName().find("Basic") != std::string::npos);
        testRoundtrip(data);

        if (FloatEncoderSIMD::isAvailable()) {
            FloatEncoder::setImplementation(FloatEncoder::SIMD);
            EXPECT_TRUE(FloatEncoder::getImplementationName().find("AVX2") != std::string::npos);
            testRoundtrip(data);
        }

        if (FloatEncoderAVX512::isAvailable()) {
            FloatEncoder::setImplementation(FloatEncoder::AVX512);
            EXPECT_TRUE(FloatEncoder::getImplementationName().find("AVX-512") != std::string::npos);
            testRoundtrip(data);
        }

        // Reset to auto
        FloatEncoder::setImplementation(FloatEncoder::AUTO);
    }
}

// Encoding performance benchmark
// NOTE: Performance data should always be presented in clear, aligned tables with:
//   - Column headers with units (MB/s, ms, etc.)
//   - Consistent decimal precision
//   - Comparison ratios when applicable (vs baseline)
//   - Separated sections for different test conditions
//   - Summary statistics at the end
// This format makes it easy to compare implementations and identify performance patterns.
TEST_F(FloatEncoderTest, EncodingBenchmark) {
    const size_t sizes[] = {100, 1000, 10000, 100000, 1000000};

    std::cout << "\n===== ENCODING PERFORMANCE BENCHMARK =====\n" << std::endl;
    std::cout << std::left << std::setw(15) << "Encoder" << std::setw(12) << "Size" << std::setw(12) << "Time (us)"
              << std::setw(15) << "Throughput" << std::setw(15) << "Compression" << std::endl;
    std::cout << std::string(70, '-') << std::endl;

    for (size_t size : sizes) {
        auto data = generateSinusoidalData(size, 100.0, 0.001);
        size_t inputBytes = data.size() * sizeof(double);

        // Benchmark Basic encoder
        {
            const int iterations = 10;
            auto start = std::chrono::high_resolution_clock::now();
            CompressedBuffer encoded;
            for (int i = 0; i < iterations; i++) {
                encoded = FloatEncoderBasic::encode(data);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto totalTime = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            auto avgTime = std::max(1L, totalTime / iterations);

            double throughputMBps = (inputBytes / 1024.0 / 1024.0) / (avgTime / 1000000.0);
            double compressionRatio = (double)inputBytes / encoded.dataByteSize();

            std::cout << std::left << std::setw(15) << "Basic" << std::setw(12) << size << std::setw(12) << avgTime
                      << std::setw(15) << (std::to_string(static_cast<int>(throughputMBps)) + " MB/s") << std::setw(15)
                      << (std::to_string(compressionRatio).substr(0, 4) + "x") << std::endl;
        }

        // Benchmark SIMD encoder if available
        if (FloatEncoderSIMD::isAvailable()) {
            const int iterations = 10;
            auto start = std::chrono::high_resolution_clock::now();
            CompressedBuffer encoded;
            for (int i = 0; i < iterations; i++) {
                encoded = FloatEncoderSIMD::encode(data);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto totalTime = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            auto avgTime = std::max(1L, totalTime / iterations);

            double throughputMBps = (inputBytes / 1024.0 / 1024.0) / (avgTime / 1000000.0);
            double compressionRatio = (double)inputBytes / encoded.dataByteSize();

            std::cout << std::left << std::setw(15) << "AVX2 SIMD" << std::setw(12) << size << std::setw(12) << avgTime
                      << std::setw(15) << (std::to_string(static_cast<int>(throughputMBps)) + " MB/s") << std::setw(15)
                      << (std::to_string(compressionRatio).substr(0, 4) + "x") << std::endl;
        }

        // Benchmark AVX-512 encoder if available
        if (FloatEncoderAVX512::isAvailable()) {
            const int iterations = 10;
            auto start = std::chrono::high_resolution_clock::now();
            CompressedBuffer encoded;
            for (int i = 0; i < iterations; i++) {
                encoded = FloatEncoderAVX512::encode(data);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto totalTime = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            auto avgTime = std::max(1L, totalTime / iterations);

            double throughputMBps = (inputBytes / 1024.0 / 1024.0) / (avgTime / 1000000.0);
            double compressionRatio = (double)inputBytes / encoded.dataByteSize();

            std::cout << std::left << std::setw(15) << "AVX-512" << std::setw(12) << size << std::setw(12) << avgTime
                      << std::setw(15) << (std::to_string(static_cast<int>(throughputMBps)) + " MB/s") << std::setw(15)
                      << (std::to_string(compressionRatio).substr(0, 4) + "x") << std::endl;
        }

        if (size < 1000000) {  // Add separator between sizes except last
            std::cout << std::endl;
        }
    }
    std::cout << std::string(70, '=') << std::endl;
}

// Decoding performance benchmark
TEST_F(FloatEncoderTest, DecodingBenchmark) {
    const size_t sizes[] = {100, 1000, 10000, 100000, 1000000};

    std::cout << "\n===== DECODING PERFORMANCE BENCHMARK =====\n" << std::endl;
    std::cout << std::left << std::setw(15) << "Decoder" << std::setw(12) << "Size" << std::setw(12) << "Time (us)"
              << std::setw(15) << "Throughput" << std::endl;
    std::cout << std::string(55, '-') << std::endl;

    for (size_t size : sizes) {
        auto data = generateSinusoidalData(size, 100.0, 0.001);
        size_t outputBytes = data.size() * sizeof(double);

        // Prepare encoded data
        CompressedBuffer encoded = FloatEncoderBasic::encode(data);

        // Benchmark decoding
        const int iterations = 10;
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < iterations; i++) {
            encoded.rewind();
            CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
            std::vector<double> decoded;
            FloatEncoderBasic::decode(slice, 0, data.size(), decoded);
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto totalTime = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        auto avgTime = std::max(1L, totalTime / iterations);

        double throughputMBps = (outputBytes / 1024.0 / 1024.0) / (avgTime / 1000000.0);

        std::cout << std::left << std::setw(15) << "Basic" << std::setw(12) << size << std::setw(12) << avgTime
                  << std::setw(15) << (std::to_string(static_cast<int>(throughputMBps)) + " MB/s") << std::endl;

        if (size < 1000000) {
            std::cout << std::endl;
        }
    }
    std::cout << std::string(55, '=') << std::endl;
}

// Comprehensive end-to-end benchmark for all encoder/decoder pairs
// NOTE: This test provides the most complete performance picture by:
//   - Testing different data patterns (constant, linear, sinusoidal, random)
//   - Measuring both encode and decode performance
//   - Showing compression ratios for each pattern
//   - Comparing all available implementations (Basic, AVX2, AVX-512)
// Data is presented in a table format with consistent units and precision for easy comparison.
// The "vs Basic" column shows speedup ratios to identify which optimizations are effective.
TEST_F(FloatEncoderTest, ComprehensiveEndToEndBenchmark) {
    std::cout << "\n===== COMPREHENSIVE END-TO-END PERFORMANCE BENCHMARK =====\n" << std::endl;

    // Test different data patterns
    struct TestCase {
        std::string name;
        std::vector<double> data;
        std::string description;
    };

    std::vector<TestCase> testCases = {
        {"Constant", generateConstantData(1000000, 42.5), "1M constant values"},
        {"Linear", generateLinearData(1000000, 0.0, 0.1), "1M linear progression"},
        {"Sinusoidal", generateSinusoidalData(1000000, 100.0, 0.001), "1M sinusoidal pattern"},
        {"Random", generateRandomData(1000000, -1000.0, 1000.0), "1M random values"}};

    std::cout << std::left << std::setw(20) << "Pattern" << std::setw(15) << "Encoder" << std::setw(12) << "Encode(ms)"
              << std::setw(15) << "Encode(MB/s)" << std::setw(10) << "vs Basic" << std::setw(12) << "Decode(ms)"
              << std::setw(15) << "Decode(MB/s)" << std::setw(12) << "Compress" << std::setw(12) << "Total(ms)"
              << std::endl;
    std::cout << std::string(125, '-') << std::endl;

    for (const auto& testCase : testCases) {
        size_t dataBytes = testCase.data.size() * sizeof(double);
        double basicEncodeTime = 0.0;

        // Test each encoder implementation
        std::vector<std::pair<std::string, std::function<CompressedBuffer(const std::vector<double>&)>>> encoders;

        encoders.push_back({"Basic", [](const auto& d) { return FloatEncoderBasic::encode(d); }});

        if (FloatEncoderSIMD::isAvailable()) {
            encoders.push_back({"AVX2", [](const auto& d) { return FloatEncoderSIMD::encode(d); }});
        }

        if (FloatEncoderAVX512::isAvailable()) {
            encoders.push_back({"AVX-512", [](const auto& d) { return FloatEncoderAVX512::encode(d); }});
        }

        for (size_t idx = 0; idx < encoders.size(); idx++) {
            const auto& [encoderName, encoder] = encoders[idx];

            // Measure encoding
            auto encodeStart = std::chrono::high_resolution_clock::now();
            CompressedBuffer encoded = encoder(testCase.data);
            auto encodeEnd = std::chrono::high_resolution_clock::now();

            auto encodeMs =
                std::chrono::duration_cast<std::chrono::microseconds>(encodeEnd - encodeStart).count() / 1000.0;
            double encodeMBps = (dataBytes / 1024.0 / 1024.0) / (encodeMs / 1000.0);

            // Store basic encoder time for comparison
            if (idx == 0) {
                basicEncodeTime = encodeMs;
            }

            // Measure decoding
            auto decodeStart = std::chrono::high_resolution_clock::now();
            encoded.rewind();
            CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
            std::vector<double> decoded;
            FloatEncoderBasic::decode(slice, 0, testCase.data.size(), decoded);
            auto decodeEnd = std::chrono::high_resolution_clock::now();

            auto decodeMs =
                std::chrono::duration_cast<std::chrono::microseconds>(decodeEnd - decodeStart).count() / 1000.0;
            double decodeMBps = (dataBytes / 1024.0 / 1024.0) / (decodeMs / 1000.0);

            double compressionRatio = (double)dataBytes / encoded.dataByteSize();
            double totalMs = encodeMs + decodeMs;
            double speedupRatio = basicEncodeTime / encodeMs;

            std::cout << std::left << std::setw(20) << testCase.name << std::setw(15) << encoderName << std::fixed
                      << std::setprecision(2) << std::setw(12) << encodeMs << std::setw(15)
                      << (std::to_string(static_cast<int>(encodeMBps)) + " MB/s");

            // Show speedup ratio (or baseline for Basic)
            if (idx == 0) {
                std::cout << std::setw(10) << "1.00x";
            } else {
                std::stringstream ss;
                ss << std::fixed << std::setprecision(2) << speedupRatio << "x";
                std::cout << std::setw(10) << ss.str();
            }

            std::cout << std::setw(12) << decodeMs << std::setw(15)
                      << (std::to_string(static_cast<int>(decodeMBps)) + " MB/s") << std::setw(12)
                      << (std::to_string(compressionRatio).substr(0, 4) + "x") << std::setw(12) << totalMs << std::endl;
        }

        std::cout << std::endl;
    }
    std::cout << std::string(125, '=') << std::endl;

    // Summary statistics
    std::cout << "\n===== PERFORMANCE SUMMARY =====\n" << std::endl;
    std::cout << "Test Configuration:" << std::endl;
    std::cout << "  Data size: 1,000,000 double values (7.63 MB)" << std::endl;
    std::cout << "  CPU: " << FloatEncoder::getImplementationName() << " capable" << std::endl;
    std::cout << std::endl;
}

// Performance test (informational only)
TEST_F(FloatEncoderTest, PerformanceComparison) {
    auto data = generateSinusoidalData(10000, 100.0, 0.001);
    size_t dataBytes = data.size() * sizeof(double);

    auto measureTime = [](auto encoder_func, const std::vector<double>& d) {
        auto start = std::chrono::high_resolution_clock::now();
        auto encoded = encoder_func(d);
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    };

    // Measure basic encoder
    auto basicTime = measureTime([](const auto& d) { return FloatEncoderBasic::encode(d); }, data);
    double basicMBps = (dataBytes / 1024.0 / 1024.0) / (basicTime / 1000000.0);
    std::cout << "Basic encoder: " << basicTime << " us (" << std::fixed << std::setprecision(2) << basicMBps
              << " MB/s)" << std::endl;

    // Measure SIMD if available
    if (FloatEncoderSIMD::isAvailable()) {
        auto simdTime = measureTime([](const auto& d) { return FloatEncoderSIMD::encode(d); }, data);
        double simdMBps = (dataBytes / 1024.0 / 1024.0) / (simdTime / 1000000.0);
        std::cout << "SIMD encoder: " << simdTime << " us (" << std::fixed << std::setprecision(2) << simdMBps
                  << " MB/s, " << (double)basicTime / simdTime << "x speedup)" << std::endl;
    }

    // Measure AVX-512 if available
    if (FloatEncoderAVX512::isAvailable()) {
        auto avx512Time = measureTime([](const auto& d) { return FloatEncoderAVX512::encode(d); }, data);
        double avx512MBps = (dataBytes / 1024.0 / 1024.0) / (avx512Time / 1000000.0);
        std::cout << "AVX-512 encoder: " << avx512Time << " us (" << std::fixed << std::setprecision(2) << avx512MBps
                  << " MB/s, " << (double)basicTime / avx512Time << "x speedup)" << std::endl;
    }
}

// ==========================================================================
// Edge case coverage: subnormals, negative zero, infinity, extreme values
// ==========================================================================

TEST_F(FloatEncoderTest, NegativeZeroRoundtrip) {
    std::vector<double> data = {-0.0};
    CompressedBuffer encoded = FloatEncoder::encode(data);
    encoded.rewind();
    CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, data.size(), decoded);
    ASSERT_EQ(decoded.size(), 1u);
    // -0.0 == 0.0 by IEEE 754, so check the sign bit directly
    EXPECT_TRUE(std::signbit(decoded[0])) << "Expected negative zero";
    EXPECT_EQ(decoded[0], -0.0);
}

TEST_F(FloatEncoderTest, SubnormalValues) {
    std::vector<double> data = {
        std::numeric_limits<double>::denorm_min(),
        -std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::denorm_min() * 2,
        std::numeric_limits<double>::denorm_min() * 1000,
    };
    testRoundtrip(data);
}

TEST_F(FloatEncoderTest, InfinityRoundtrip) {
    std::vector<double> data = {
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
    };
    testRoundtrip(data);
}

TEST_F(FloatEncoderTest, NaNRoundtrip) {
    std::vector<double> data = {
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::signaling_NaN(),
    };
    CompressedBuffer encoded = FloatEncoder::encode(data);
    encoded.rewind();
    CompressedSlice slice((const uint8_t*)encoded.data.data(), encoded.data.size() * sizeof(uint64_t));
    std::vector<double> decoded;
    FloatDecoder::decode(slice, 0, data.size(), decoded);
    ASSERT_EQ(decoded.size(), 2u);
    EXPECT_TRUE(std::isnan(decoded[0]));
    EXPECT_TRUE(std::isnan(decoded[1]));
}

TEST_F(FloatEncoderTest, ExtremeValues) {
    std::vector<double> data = {
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::lowest(),  // -DBL_MAX
        std::numeric_limits<double>::min(),     // smallest positive normal
        -std::numeric_limits<double>::min(),   std::numeric_limits<double>::epsilon(),
    };
    testRoundtrip(data);
}

TEST_F(FloatEncoderTest, MixedSpecialAndNormal) {
    // Interleave special values with normal values to stress XOR delta encoding
    std::vector<double> data = {
        1.0,
        std::numeric_limits<double>::infinity(),
        -1.0,
        -0.0,
        std::numeric_limits<double>::denorm_min(),
        42.5,
        std::numeric_limits<double>::max(),
        0.0,
        std::numeric_limits<double>::lowest(),
    };
    testRoundtrip(data);
}

// Test that a corrupted float block with lzb + data_bits > 64 is rejected
// rather than silently underflowing tzb to a huge unsigned value.
//
// Bit layout of a valid Gorilla-encoded float stream:
//   [64 bits: first value as raw uint64_t]
//   [2 bits: 0b11 = new-bounds prefix]
//   [5 bits: lzb]
//   [6 bits: data_bits]
//   [data_bits bits: XOR mantissa]
//
// The decoder stores tzb = 64 - lzb - data_bits (all uint64_t).
// With corrupted lzb=31, data_bits=63 the subtraction wraps to a huge
// unsigned value (0xFFFFFFFFFFFFFFE2) and the subsequent left-shift by
// that huge tzb is undefined behaviour.  The fix must detect this and
// throw before the shift is reached.
TEST_F(FloatEncoderTest, CorruptBlockLzbPlusDataBitsOverflow) {
    // Build the bit-stream manually using CompressedBuffer so the bit-packing
    // is handled correctly, then read it back via CompressedSlice / FloatDecoderBasic.
    //
    // We declare that the block contains 2 values so the decoder enters the
    // delta-decoding loop at least once.

    CompressedBuffer buf;

    // First value: a harmless 1.0
    const uint64_t first_raw = std::bit_cast<uint64_t>(1.0);
    buf.write<64>(first_raw);

    // Second value: 0b11 prefix (new bounds), lzb=31, data_bits=63
    // 31 + 63 = 94 > 64  =>  tzb would underflow without the guard.
    buf.writeFixed<0b11, 2>();  // new-bounds prefix
    buf.write<5>(31u);          // lzb = 31
    buf.write<6>(63u);          // data_bits = 63
    // Write 63 bits of payload so the stream is long enough that the bounds
    // check inside read() does not fire before we reach the tzb guard.
    buf.write<64>(0xDEADBEEFDEADBEEFull);  // provides more than 63 bits

    // Re-read the buffer through a CompressedSlice (as the real decode path does).
    buf.rewind();
    CompressedSlice slice(reinterpret_cast<const uint8_t*>(buf.data.data()), buf.data.size() * sizeof(uint64_t));

    std::vector<double> out;
    // The decoder must throw rather than silently corrupt tzb.
    EXPECT_THROW(FloatDecoderBasic::decode(slice, 0, 2, out), std::runtime_error);
}