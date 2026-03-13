#include "../test_helpers/aggregator_test_helper.hpp"
#include "aggregator.hpp"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>

using namespace timestar;
using namespace std::chrono;

// Configuration matching the benchmark.js scenario
constexpr size_t NUM_HOSTS = 10;
constexpr size_t NUM_FIELDS = 10;
constexpr size_t MINUTES_PER_YEAR = 365 * 24 * 60;  // 525,600 data points per series
constexpr uint64_t MINUTE_IN_NS = 60ULL * 1000000000ULL;
constexpr uint64_t HOUR_IN_NS = 3600ULL * 1000000000ULL;

struct BenchmarkResult {
    std::string testName;
    size_t inputSeries;
    size_t inputPoints;
    double durationMs;
    size_t outputPoints;
    double pointsPerSecond;

    void print() const {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "\n" << std::string(70, '=') << "\n";
        std::cout << "Test: " << testName << "\n";
        std::cout << std::string(70, '-') << "\n";
        std::cout << "Input Series:     " << std::setw(12) << inputSeries << "\n";
        std::cout << "Input Points:     " << std::setw(12) << inputPoints << "\n";
        std::cout << "Output Points:    " << std::setw(12) << outputPoints << "\n";
        std::cout << "Duration:         " << std::setw(12) << durationMs << " ms\n";
        std::cout << "Throughput:       " << std::setw(12) << static_cast<uint64_t>(pointsPerSecond) << " points/sec\n";
        std::cout << std::string(70, '=') << "\n";
    }
};

// Generate realistic time series data
std::pair<std::vector<uint64_t>, std::vector<double>> generateTimeSeries(uint64_t startTime, size_t numPoints,
                                                                         uint64_t interval) {
    std::vector<uint64_t> timestamps;
    std::vector<double> values;
    timestamps.reserve(numPoints);
    values.reserve(numPoints);

    std::mt19937 rng(42);  // Fixed seed for reproducibility
    std::uniform_real_distribution<double> dist(20.0, 80.0);

    for (size_t i = 0; i < numPoints; ++i) {
        timestamps.push_back(startTime + i * interval);
        values.push_back(dist(rng));
    }

    return {timestamps, values};
}

// Benchmark 1: No grouping, no time bucketing (worst case for map operations)
BenchmarkResult benchmarkNoGroupNoInterval() {
    std::cout << "\n[BENCHMARK 1] No grouping, no time interval...\n";

    uint64_t startTime = 1704067200000000000ULL;  // Jan 1, 2024

    // Generate data for multiple series (simulating different hosts)
    std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> series;
    size_t totalPoints = 0;

    for (size_t host = 0; host < NUM_HOSTS; ++host) {
        auto [timestamps, values] = generateTimeSeries(startTime, MINUTES_PER_YEAR, MINUTE_IN_NS);
        totalPoints += timestamps.size();
        series.push_back({timestamps, values});
    }

    std::cout << "Generated " << series.size() << " series with " << totalPoints << " total points\n";
    std::cout << "Aggregating across all series (this will be slow)...\n";

    // Benchmark aggregation
    auto start = high_resolution_clock::now();
    auto result = timestar::test::AggregatorTestHelper::aggregateMultiple(series, AggregationMethod::AVG, 0);
    auto end = high_resolution_clock::now();

    double durationMs = duration<double, std::milli>(end - start).count();
    double pointsPerSec = (totalPoints * 1000.0) / durationMs;

    BenchmarkResult bench;
    bench.testName = "No Grouping, No Time Interval (AVG across all series)";
    bench.inputSeries = series.size();
    bench.inputPoints = totalPoints;
    bench.outputPoints = result.size();
    bench.durationMs = durationMs;
    bench.pointsPerSecond = pointsPerSec;

    return bench;
}

// Benchmark 2: With time bucketing (1 hour intervals)
BenchmarkResult benchmarkWithTimeInterval() {
    std::cout << "\n[BENCHMARK 2] No grouping, with 1-hour time intervals...\n";

    uint64_t startTime = 1704067200000000000ULL;

    std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> series;
    size_t totalPoints = 0;

    for (size_t host = 0; host < NUM_HOSTS; ++host) {
        auto [timestamps, values] = generateTimeSeries(startTime, MINUTES_PER_YEAR, MINUTE_IN_NS);
        totalPoints += timestamps.size();
        series.push_back({timestamps, values});
    }

    std::cout << "Generated " << series.size() << " series with " << totalPoints << " total points\n";
    std::cout << "Aggregating with 1-hour buckets...\n";

    // Benchmark aggregation with 1-hour intervals
    auto start = high_resolution_clock::now();
    auto result = timestar::test::AggregatorTestHelper::aggregateMultiple(series, AggregationMethod::AVG, HOUR_IN_NS);
    auto end = high_resolution_clock::now();

    double durationMs = duration<double, std::milli>(end - start).count();
    double pointsPerSec = (totalPoints * 1000.0) / durationMs;

    BenchmarkResult bench;
    bench.testName = "No Grouping, 1-Hour Time Intervals (AVG)";
    bench.inputSeries = series.size();
    bench.inputPoints = totalPoints;
    bench.outputPoints = result.size();
    bench.durationMs = durationMs;
    bench.pointsPerSecond = pointsPerSec;

    return bench;
}

// Benchmark 3: Single series aggregation (baseline for comparison)
BenchmarkResult benchmarkSingleSeries() {
    std::cout << "\n[BENCHMARK 3] Single series aggregation (baseline)...\n";

    uint64_t startTime = 1704067200000000000ULL;

    auto [timestamps, values] = generateTimeSeries(startTime, MINUTES_PER_YEAR, MINUTE_IN_NS);

    std::cout << "Generated 1 series with " << timestamps.size() << " points\n";
    std::cout << "Aggregating to 1-hour buckets...\n";

    // Benchmark single series aggregation
    auto start = high_resolution_clock::now();
    auto result =
        timestar::test::AggregatorTestHelper::aggregate(timestamps, values, AggregationMethod::AVG, HOUR_IN_NS);
    auto end = high_resolution_clock::now();

    double durationMs = duration<double, std::milli>(end - start).count();
    double pointsPerSec = (timestamps.size() * 1000.0) / durationMs;

    BenchmarkResult bench;
    bench.testName = "Single Series, 1-Hour Time Intervals (baseline)";
    bench.inputSeries = 1;
    bench.inputPoints = timestamps.size();
    bench.outputPoints = result.size();
    bench.durationMs = durationMs;
    bench.pointsPerSecond = pointsPerSec;

    return bench;
}

// Benchmark 4: Large multi-series worst case (like group-by-host with all fields)
BenchmarkResult benchmarkWorstCase() {
    std::cout << "\n[BENCHMARK 4] Worst case: 100 series (10 hosts x 10 fields)...\n";

    uint64_t startTime = 1704067200000000000ULL;

    std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>> series;
    size_t totalPoints = 0;

    // Simulate 10 hosts x 10 fields = 100 series
    for (size_t host = 0; host < NUM_HOSTS; ++host) {
        for (size_t field = 0; field < NUM_FIELDS; ++field) {
            auto [timestamps, values] = generateTimeSeries(startTime, MINUTES_PER_YEAR, MINUTE_IN_NS);
            totalPoints += timestamps.size();
            series.push_back({timestamps, values});
        }
    }

    std::cout << "Generated " << series.size() << " series with " << totalPoints << " total points\n";
    std::cout << "This simulates a group-by query with all fields...\n";
    std::cout << "Aggregating (this will take a while)...\n";

    // Benchmark aggregation
    auto start = high_resolution_clock::now();
    auto result = timestar::test::AggregatorTestHelper::aggregateMultiple(series, AggregationMethod::AVG, HOUR_IN_NS);
    auto end = high_resolution_clock::now();

    double durationMs = duration<double, std::milli>(end - start).count();
    double pointsPerSec = (totalPoints * 1000.0) / durationMs;

    BenchmarkResult bench;
    bench.testName = "Worst Case: 100 series, 1-Hour Intervals";
    bench.inputSeries = series.size();
    bench.inputPoints = totalPoints;
    bench.outputPoints = result.size();
    bench.durationMs = durationMs;
    bench.pointsPerSecond = pointsPerSec;

    return bench;
}

int main() {
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "TimeStar Aggregator Performance Benchmark\n";
    std::cout << std::string(70, '=') << "\n";
    std::cout << "\nConfiguration:\n";
    std::cout << "  - Data points per series: " << MINUTES_PER_YEAR << " (1 year at 1-minute intervals)\n";
    std::cout << "  - Number of hosts: " << NUM_HOSTS << "\n";
    std::cout << "  - Number of fields: " << NUM_FIELDS << "\n";
    std::cout << "  - Time bucket size: 1 hour\n";
    std::cout << "\n";

    std::vector<BenchmarkResult> results;

    // Run benchmarks in order of increasing complexity
    results.push_back(benchmarkSingleSeries());
    results[0].print();

    results.push_back(benchmarkNoGroupNoInterval());
    results[1].print();

    results.push_back(benchmarkWithTimeInterval());
    results[2].print();

    results.push_back(benchmarkWorstCase());
    results[3].print();

    // Summary
    std::cout << "\n" << std::string(70, '=') << "\n";
    std::cout << "PERFORMANCE SUMMARY\n";
    std::cout << std::string(70, '=') << "\n\n";
    std::cout << std::left << std::setw(45) << "Test" << std::right << std::setw(12) << "Duration (ms)" << std::setw(13)
              << "Throughput\n";
    std::cout << std::string(70, '-') << "\n";

    for (const auto& result : results) {
        std::cout << std::left << std::setw(45) << result.testName.substr(0, 44) << std::right << std::setw(12)
                  << std::fixed << std::setprecision(2) << result.durationMs << std::setw(10)
                  << static_cast<uint64_t>(result.pointsPerSecond) << " p/s\n";
    }

    std::cout << std::string(70, '=') << "\n";

    // Calculate overhead
    if (results.size() >= 2) {
        double singleSeriesTime = results[0].durationMs;
        double multiSeriesTime = results[1].durationMs;
        double overhead = ((multiSeriesTime / singleSeriesTime) - 1.0) * 100.0;

        std::cout << "\nMulti-series overhead vs single series: " << std::fixed << std::setprecision(1) << overhead
                  << "%\n";
    }

    if (results.size() >= 4) {
        double estimatedLinearTime = results[0].durationMs * 100;  // 100 series
        double actualTime = results[3].durationMs;
        double overhead = ((actualTime / estimatedLinearTime) - 1.0) * 100.0;

        std::cout << "Worst case overhead vs linear scaling: " << std::fixed << std::setprecision(1) << overhead
                  << "%\n";
        std::cout << "  (Expected: " << estimatedLinearTime << " ms, Actual: " << actualTime << " ms)\n";
    }

    std::cout << "\n";

    return 0;
}
