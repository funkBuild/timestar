#include "series_id.hpp"
#include "tsm.hpp"
#include "tsm_writer.hpp"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>

namespace fs = std::filesystem;

struct BenchmarkResult {
    std::string scenario;
    size_t numBlocks;
    size_t totalPoints;
    double oldMethodMs;
    double newMethodMs;
    double speedup;
    size_t ioOperationsOld;
    size_t ioOperationsNew;

    void print() const {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "\n┌─────────────────────────────────────────────────────────────┐\n";
        std::cout << "│ " << std::left << std::setw(59) << scenario << " │\n";
        std::cout << "├─────────────────────────────────────────────────────────────┤\n";
        std::cout << "│ Blocks:           " << std::right << std::setw(38) << numBlocks << " │\n";
        std::cout << "│ Total Points:     " << std::right << std::setw(38) << totalPoints << " │\n";
        std::cout << "│ Old Method (ms):  " << std::right << std::setw(38) << oldMethodMs << " │\n";
        std::cout << "│ New Method (ms):  " << std::right << std::setw(38) << newMethodMs << " │\n";
        std::cout << "│ Speedup:          " << std::right << std::setw(37) << speedup << "x │\n";
        std::cout << "│ I/O Ops (Old):    " << std::right << std::setw(38) << ioOperationsOld << " │\n";
        std::cout << "│ I/O Ops (New):    " << std::right << std::setw(38) << ioOperationsNew << " │\n";
        std::cout << "│ I/O Reduction:    " << std::right << std::setw(37)
                  << (100.0 * (1.0 - (double)ioOperationsNew / ioOperationsOld)) << "% │\n";
        std::cout << "└─────────────────────────────────────────────────────────────┘\n";
    }
};

// Create TSM file with specified number of blocks
seastar::future<std::string> createTestFile(const std::string& filename, size_t numBlocks,
                                            size_t pointsPerBlock = 10000) {
    TSMWriter writer(filename);

    std::vector<uint64_t> allTimestamps;
    std::vector<double> allValues;

    size_t totalPoints = numBlocks * pointsPerBlock;
    allTimestamps.reserve(totalPoints);
    allValues.reserve(totalPoints);

    uint64_t baseTime = 1600000000000ULL;
    for (size_t i = 0; i < totalPoints; i++) {
        allTimestamps.push_back(baseTime + i * 1000);
        allValues.push_back(i * 1.5);
    }

    SeriesId128 seriesId = SeriesId128::fromSeriesKey("benchmark.series");
    writer.writeSeries(TSMValueType::Float, seriesId, allTimestamps, allValues);
    writer.writeIndex();
    writer.close();

    std::cout << "Created test file: " << filename << " with " << numBlocks << " blocks (" << totalPoints
              << " points)\n";

    co_return filename;
}

// Benchmark old method (individual block reads)
seastar::future<double> benchmarkOldMethod(const std::string& filename, size_t iterations = 10) {
    std::vector<double> timings;
    timings.reserve(iterations);

    for (size_t i = 0; i < iterations; i++) {
        TSM tsm(filename);
        co_await tsm.open();

        auto start = std::chrono::high_resolution_clock::now();

        SeriesId128 seriesId = SeriesId128::fromSeriesKey("benchmark.series");
        TSMResult<double> results(0);
        co_await tsm.readSeries(seriesId, 0, UINT64_MAX, results);

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
        timings.push_back(elapsed);

        co_await tsm.close();
    }

    // Return median time
    std::sort(timings.begin(), timings.end());
    co_return timings[timings.size() / 2];
}

// Benchmark new method (batched block reads)
seastar::future<double> benchmarkNewMethod(const std::string& filename, size_t iterations = 10) {
    std::vector<double> timings;
    timings.reserve(iterations);

    for (size_t i = 0; i < iterations; i++) {
        TSM tsm(filename);
        co_await tsm.open();

        auto start = std::chrono::high_resolution_clock::now();

        SeriesId128 seriesId = SeriesId128::fromSeriesKey("benchmark.series");
        TSMResult<double> results(0);
        co_await tsm.readSeriesBatched(seriesId, 0, UINT64_MAX, results);

        auto end = std::chrono::high_resolution_clock::now();
        double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
        timings.push_back(elapsed);

        co_await tsm.close();
    }

    // Return median time
    std::sort(timings.begin(), timings.end());
    co_return timings[timings.size() / 2];
}

seastar::future<BenchmarkResult> runBenchmark(const std::string& scenario, size_t numBlocks,
                                              size_t pointsPerBlock = 10000, int fileNum = 1) {
    // TSM expects filenames in format: tier_sequence.tsm
    std::string filename = "./benchmark_tsm/0_" + std::to_string(fileNum) + ".tsm";

    // Create test file
    co_await createTestFile(filename, numBlocks, pointsPerBlock);

    // Warm up
    {
        TSM tsm(filename);
        co_await tsm.open();
        SeriesId128 seriesId = SeriesId128::fromSeriesKey("benchmark.series");
        TSMResult<double> warmup(0);
        co_await tsm.readSeriesBatched(seriesId, 0, UINT64_MAX, warmup);
        co_await tsm.close();
    }

    std::cout << "\nBenchmarking: " << scenario << "\n";
    std::cout << "Running old method (10 iterations)...\n";
    double oldTime = co_await benchmarkOldMethod(filename, 10);

    std::cout << "Running new method (10 iterations)...\n";
    double newTime = co_await benchmarkNewMethod(filename, 10);

    BenchmarkResult result;
    result.scenario = scenario;
    result.numBlocks = numBlocks;
    result.totalPoints = numBlocks * pointsPerBlock;
    result.oldMethodMs = oldTime;
    result.newMethodMs = newTime;
    result.speedup = oldTime / newTime;
    result.ioOperationsOld = numBlocks;  // Old method: 1 I/O per block
    result.ioOperationsNew = 1;          // New method: 1 I/O for all contiguous blocks

    // Clean up
    fs::remove(filename);

    co_return result;
}

seastar::future<> runAllBenchmarks() {
    std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║         TSM Batch Read Performance Benchmark           ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n";

    fs::create_directories("./benchmark_tsm");

    std::vector<BenchmarkResult> results;

    // Test 1: Single block (baseline - no benefit expected)
    results.push_back(co_await runBenchmark("Single block (10K points)", 1, 10000, 1));

    // Test 2: 3 contiguous blocks (typical case)
    results.push_back(co_await runBenchmark("3 blocks (30K points)", 3, 10000, 2));

    // Test 3: 5 contiguous blocks
    results.push_back(co_await runBenchmark("5 blocks (50K points)", 5, 10000, 3));

    // Test 4: 10 contiguous blocks (larger range query)
    results.push_back(co_await runBenchmark("10 blocks (100K points)", 10, 10000, 4));

    // Test 5: 20 contiguous blocks (very large range query)
    results.push_back(co_await runBenchmark("20 blocks (200K points)", 20, 10000, 5));

    // Test 6: Many small blocks
    results.push_back(co_await runBenchmark("10 small blocks (10K points)", 10, 1000, 6));

    // Print all results
    std::cout << "\n\n╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║                    BENCHMARK RESULTS                      ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n";

    for (const auto& result : results) {
        result.print();
    }

    // Summary statistics
    std::cout << "\n╔═══════════════════════════════════════════════════════════╗\n";
    std::cout << "║                       SUMMARY                             ║\n";
    std::cout << "╚═══════════════════════════════════════════════════════════╝\n\n";

    double avgSpeedup = 0.0;
    double maxSpeedup = 0.0;
    double avgIoReduction = 0.0;

    for (const auto& result : results) {
        if (result.numBlocks > 1) {  // Exclude single block from averages
            avgSpeedup += result.speedup;
            maxSpeedup = std::max(maxSpeedup, result.speedup);
            double ioReduction = 100.0 * (1.0 - (double)result.ioOperationsNew / result.ioOperationsOld);
            avgIoReduction += ioReduction;
        }
    }

    size_t multiBlockTests = results.size() - 1;
    avgSpeedup /= multiBlockTests;
    avgIoReduction /= multiBlockTests;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  Average Speedup (multi-block):  " << avgSpeedup << "x\n";
    std::cout << "  Maximum Speedup:                " << maxSpeedup << "x\n";
    std::cout << "  Average I/O Reduction:          " << avgIoReduction << "%\n";
    std::cout << "\n";

    // Clean up
    fs::remove_all("./benchmark_tsm");

    co_return;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    return app.run(argc, argv, [] { return runAllBenchmarks().then([] { std::cout << "\nBenchmark complete!\n"; }); });
}
