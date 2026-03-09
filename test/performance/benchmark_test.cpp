#include <gtest/gtest.h>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>
#include <chrono>
#include <filesystem>
#include <random>
#include <iomanip>
#include <sstream>

#include "engine.hpp"
#include "timestar_value.hpp"
#include "query_result.hpp"
#include "../test_helpers.hpp"

namespace fs = std::filesystem;

// Helper to format duration in human-readable format
std::string formatDuration(std::chrono::nanoseconds ns) {
    if (ns.count() < 1000) {
        return std::to_string(ns.count()) + " ns";
    } else if (ns.count() < 1000000) {
        return std::to_string(ns.count() / 1000.0) + " µs";
    } else if (ns.count() < 1000000000) {
        return std::to_string(ns.count() / 1000000.0) + " ms";
    } else {
        return std::to_string(ns.count() / 1000000000.0) + " s";
    }
}

// Helper to format throughput
std::string formatThroughput(size_t operations, std::chrono::nanoseconds totalTime) {
    double seconds = totalTime.count() / 1000000000.0;
    double opsPerSec = operations / seconds;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2);

    if (opsPerSec > 1000000) {
        ss << (opsPerSec / 1000000.0) << " M ops/sec";
    } else if (opsPerSec > 1000) {
        ss << (opsPerSec / 1000.0) << " K ops/sec";
    } else {
        ss << opsPerSec << " ops/sec";
    }

    return ss.str();
}

// Helper to calculate statistics
struct Statistics {
    double mean;
    double median;
    double p95;
    double p99;
    double min;
    double max;
    double stddev;

    static Statistics calculate(std::vector<double>& values) {
        Statistics stats;

        if (values.empty()) {
            return stats;
        }

        std::sort(values.begin(), values.end());

        // Min and Max
        stats.min = values.front();
        stats.max = values.back();

        // Mean
        double sum = 0;
        for (double v : values) {
            sum += v;
        }
        stats.mean = sum / values.size();

        // Median
        size_t midIdx = values.size() / 2;
        if (values.size() % 2 == 0) {
            stats.median = (values[midIdx - 1] + values[midIdx]) / 2.0;
        } else {
            stats.median = values[midIdx];
        }

        // Percentiles
        size_t p95Idx = (size_t)(values.size() * 0.95);
        size_t p99Idx = (size_t)(values.size() * 0.99);
        stats.p95 = values[std::min(p95Idx, values.size() - 1)];
        stats.p99 = values[std::min(p99Idx, values.size() - 1)];

        // Standard deviation
        double variance = 0;
        for (double v : values) {
            variance += std::pow(v - stats.mean, 2);
        }
        stats.stddev = std::sqrt(variance / values.size());

        return stats;
    }

    void print(const std::string& metric) const {
        std::cout << "\n" << metric << " Statistics (ms):" << std::endl;
        std::cout << "  Mean:   " << std::fixed << std::setprecision(3) << mean << " ms" << std::endl;
        std::cout << "  Median: " << std::fixed << std::setprecision(3) << median << " ms" << std::endl;
        std::cout << "  StdDev: " << std::fixed << std::setprecision(3) << stddev << " ms" << std::endl;
        std::cout << "  Min:    " << std::fixed << std::setprecision(3) << min << " ms" << std::endl;
        std::cout << "  Max:    " << std::fixed << std::setprecision(3) << max << " ms" << std::endl;
        std::cout << "  P95:    " << std::fixed << std::setprecision(3) << p95 << " ms" << std::endl;
        std::cout << "  P99:    " << std::fixed << std::setprecision(3) << p99 << " ms" << std::endl;
    }
};

class BenchmarkTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Clean up any existing test directories
        cleanupTestDirectories();
    }

    void TearDown() override {
        cleanupTestDirectories();
    }

    void cleanupTestDirectories() {
        cleanTestShardDirectories();
    }

    // Generate test data
    struct TestData {
        std::vector<std::string> measurements = {"cpu", "memory", "disk", "network", "temperature"};
        std::vector<std::string> hosts = {"server01", "server02", "server03", "server04", "server05"};
        std::vector<std::string> regions = {"us-east", "us-west", "eu-central", "ap-south"};
        std::vector<std::string> fields = {"usage", "load", "latency", "throughput"};
        std::mt19937 rng;
        std::uniform_real_distribution<double> valueDist{0.0, 100.0};
        std::uniform_int_distribution<size_t> measurementDist;
        std::uniform_int_distribution<size_t> hostDist;
        std::uniform_int_distribution<size_t> regionDist;
        std::uniform_int_distribution<size_t> fieldDist;

        TestData() : rng(42),  // Fixed seed for reproducibility
                     measurementDist(0, measurements.size() - 1),
                     hostDist(0, hosts.size() - 1),
                     regionDist(0, regions.size() - 1),
                     fieldDist(0, fields.size() - 1) {}

        TimeStarInsert<double> generateInsert(uint64_t timestamp, int batchSize = 10) {
            std::string measurement = measurements[measurementDist(rng)];
            std::string field = fields[fieldDist(rng)];

            TimeStarInsert<double> insert(measurement, field);
            insert.tags = {
                {"host", hosts[hostDist(rng)]},
                {"region", regions[regionDist(rng)]}
            };

            // Generate batch of data points
            for (int i = 0; i < batchSize; ++i) {
                insert.timestamps.push_back(timestamp + i * 1000);
                insert.values.push_back(valueDist(rng));
            }

            return insert;
        }

        std::string generateSeriesKey() {
            std::string measurement = measurements[measurementDist(rng)];
            std::string host = hosts[hostDist(rng)];
            std::string region = regions[regionDist(rng)];
            std::string field = fields[fieldDist(rng)];

            return measurement + ",host=" + host + ",region=" + region + " " + field;
        }
    };
};

TEST_F(BenchmarkTest, InsertAndQueryPerformance) {
    seastar::thread([this] {
        // Initialize engine
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        const size_t NUM_INSERTS = 1000;
        const size_t NUM_QUERIES = 1000;
        const int BATCH_SIZE = 10;  // Points per insert

        TestData testData;
        std::vector<std::string> insertedSeries;

        std::cout << "\n========================================" << std::endl;
        std::cout << "TimeStar Performance Benchmark" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Configuration:" << std::endl;
        std::cout << "  Insert operations: " << NUM_INSERTS << std::endl;
        std::cout << "  Points per insert: " << BATCH_SIZE << std::endl;
        std::cout << "  Total data points: " << NUM_INSERTS * BATCH_SIZE << std::endl;
        std::cout << "  Query operations:  " << NUM_QUERIES << std::endl;
        std::cout << "========================================\n" << std::endl;

        // Benchmark inserts
        std::vector<double> insertLatencies;
        insertLatencies.reserve(NUM_INSERTS);

        auto insertStartTime = std::chrono::high_resolution_clock::now();
        uint64_t baseTimestamp = 1000000000;

        for (size_t i = 0; i < NUM_INSERTS; ++i) {
            auto insert = testData.generateInsert(baseTimestamp + i * 10000, BATCH_SIZE);

            // Track series for queries using seriesKey() for correct format
            std::string seriesKey = insert.seriesKey();
            insertedSeries.push_back(seriesKey);

            auto opStart = std::chrono::high_resolution_clock::now();
            engine->insert(insert).get();
            auto opEnd = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(opEnd - opStart);
            insertLatencies.push_back(duration.count() / 1000000.0);  // Convert to ms

            // Progress indicator
            if ((i + 1) % 100 == 0) {
                std::cout << "Inserts completed: " << (i + 1) << "/" << NUM_INSERTS << "\r" << std::flush;
            }
        }

        auto insertEndTime = std::chrono::high_resolution_clock::now();
        auto totalInsertTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
            insertEndTime - insertStartTime);

        std::cout << "Inserts completed: " << NUM_INSERTS << "/" << NUM_INSERTS << std::endl;

        // Calculate insert statistics
        auto insertStats = Statistics::calculate(insertLatencies);

        // Print insert results
        std::cout << "\n### INSERT PERFORMANCE ###" << std::endl;
        std::cout << "Total time:      " << formatDuration(totalInsertTime) << std::endl;
        std::cout << "Total points:    " << NUM_INSERTS * BATCH_SIZE << std::endl;
        std::cout << "Throughput:      " << formatThroughput(NUM_INSERTS, totalInsertTime) << std::endl;
        std::cout << "Points/sec:      " << formatThroughput(NUM_INSERTS * BATCH_SIZE, totalInsertTime) << std::endl;
        insertStats.print("Insert Latency");

        // Small delay to ensure all data is written
        seastar::sleep(std::chrono::milliseconds(100)).get();

        // Benchmark queries
        std::vector<double> queryLatencies;
        queryLatencies.reserve(NUM_QUERIES);
        size_t totalPointsReturned = 0;
        size_t emptyQueries = 0;

        auto queryStartTime = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < NUM_QUERIES; ++i) {
            // Query random series from those we inserted
            std::string seriesKey;
            if (!insertedSeries.empty()) {
                seriesKey = insertedSeries[i % insertedSeries.size()];
            } else {
                seriesKey = testData.generateSeriesKey();
            }

            // Query with time range
            uint64_t startTime = baseTimestamp;
            uint64_t endTime = baseTimestamp + NUM_INSERTS * 10000;

            auto opStart = std::chrono::high_resolution_clock::now();
            auto result = engine->query(seriesKey, startTime, endTime).get();
            auto opEnd = std::chrono::high_resolution_clock::now();

            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(opEnd - opStart);
            queryLatencies.push_back(duration.count() / 1000000.0);  // Convert to ms

            // Count returned points
            if (result.has_value() && std::holds_alternative<QueryResult<double>>(*result)) {
                const auto& doubleResult = std::get<QueryResult<double>>(*result);
                totalPointsReturned += doubleResult.timestamps.size();
                if (doubleResult.timestamps.empty()) {
                    emptyQueries++;
                }
            } else {
                emptyQueries++;
            }

            // Progress indicator
            if ((i + 1) % 100 == 0) {
                std::cout << "Queries completed: " << (i + 1) << "/" << NUM_QUERIES << "\r" << std::flush;
            }
        }

        auto queryEndTime = std::chrono::high_resolution_clock::now();
        auto totalQueryTime = std::chrono::duration_cast<std::chrono::nanoseconds>(
            queryEndTime - queryStartTime);

        std::cout << "Queries completed: " << NUM_QUERIES << "/" << NUM_QUERIES << std::endl;

        // Calculate query statistics
        auto queryStats = Statistics::calculate(queryLatencies);

        // Print query results
        std::cout << "\n### QUERY PERFORMANCE ###" << std::endl;
        std::cout << "Total time:      " << formatDuration(totalQueryTime) << std::endl;
        std::cout << "Throughput:      " << formatThroughput(NUM_QUERIES, totalQueryTime) << std::endl;
        std::cout << "Points returned: " << totalPointsReturned << std::endl;
        std::cout << "Avg points/query: " << (totalPointsReturned / (double)NUM_QUERIES) << std::endl;
        std::cout << "Empty queries:   " << emptyQueries << " ("
                  << (emptyQueries * 100.0 / NUM_QUERIES) << "%)" << std::endl;
        queryStats.print("Query Latency");

        // Combined statistics
        std::cout << "\n### COMBINED PERFORMANCE ###" << std::endl;
        auto totalTime = totalInsertTime + totalQueryTime;
        std::cout << "Total operations: " << (NUM_INSERTS + NUM_QUERIES) << std::endl;
        std::cout << "Total time:       " << formatDuration(totalTime) << std::endl;
        std::cout << "Overall throughput: " << formatThroughput(NUM_INSERTS + NUM_QUERIES, totalTime) << std::endl;

        // Memory usage estimate (if available)
        std::cout << "\n### RESOURCE USAGE ###" << std::endl;
        std::cout << "Shards used:     " << seastar::smp::count << std::endl;

        // Check disk usage
        size_t totalDiskUsage = 0;
        for (int i = 0; i < seastar::smp::count; ++i) {
            std::string shardPath = "shard_" + std::to_string(i);
            if (fs::exists(shardPath)) {
                for (const auto& entry : fs::recursive_directory_iterator(shardPath)) {
                    if (fs::is_regular_file(entry)) {
                        totalDiskUsage += fs::file_size(entry);
                    }
                }
            }
        }

        std::cout << "Disk usage:      " << (totalDiskUsage / 1024.0 / 1024.0)
                  << " MB" << std::endl;
        std::cout << "Bytes per point: " << (totalDiskUsage / (double)(NUM_INSERTS * BATCH_SIZE))
                  << std::endl;

        std::cout << "\n========================================" << std::endl;
    }).join().get();
}

TEST_F(BenchmarkTest, BurstPerformance) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        TestData testData;
        const size_t BURST_SIZE = 100;
        const size_t NUM_BURSTS = 10;
        const int POINTS_PER_INSERT = 100;

        std::cout << "\n========================================" << std::endl;
        std::cout << "BURST PERFORMANCE TEST" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Configuration:" << std::endl;
        std::cout << "  Burst size:       " << BURST_SIZE << " inserts" << std::endl;
        std::cout << "  Number of bursts: " << NUM_BURSTS << std::endl;
        std::cout << "  Points per insert: " << POINTS_PER_INSERT << std::endl;
        std::cout << "========================================\n" << std::endl;

        std::vector<double> burstTimes;
        uint64_t baseTimestamp = 1000000000;

        for (size_t burst = 0; burst < NUM_BURSTS; ++burst) {
            auto burstStart = std::chrono::high_resolution_clock::now();

            // Send burst of inserts
            for (size_t i = 0; i < BURST_SIZE; ++i) {
                auto insert = testData.generateInsert(
                    baseTimestamp + burst * 1000000 + i * 1000,
                    POINTS_PER_INSERT
                );
                engine->insert(insert).get();
            }

            auto burstEnd = std::chrono::high_resolution_clock::now();
            auto burstDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
                burstEnd - burstStart);
            burstTimes.push_back(burstDuration.count());

            std::cout << "Burst " << (burst + 1) << "/" << NUM_BURSTS
                      << " completed in " << burstDuration.count() << " ms"
                      << " (" << formatThroughput(BURST_SIZE * POINTS_PER_INSERT,
                                                  std::chrono::duration_cast<std::chrono::nanoseconds>(burstDuration))
                      << " points)" << std::endl;

            // Small delay between bursts
            seastar::sleep(std::chrono::milliseconds(100)).get();
        }

        auto burstStats = Statistics::calculate(burstTimes);
        burstStats.print("Burst Time");

        std::cout << "\nAverage burst throughput: "
                  << formatThroughput(BURST_SIZE * POINTS_PER_INSERT,
                                     std::chrono::milliseconds(static_cast<long>(burstStats.mean)))
                  << " points" << std::endl;
    }).join().get();
}

TEST_F(BenchmarkTest, ConcurrentQueries) {
    seastar::thread([this] {
        ScopedEngine eng;
        eng.initWithBackground();
        auto* engine = eng.get();

        // First, insert some data
        TestData testData;
        const size_t NUM_SERIES = 100;
        uint64_t baseTimestamp = 1000000000;
        std::vector<std::string> seriesKeys;

        std::cout << "\n========================================" << std::endl;
        std::cout << "CONCURRENT QUERY PERFORMANCE TEST" << std::endl;
        std::cout << "========================================" << std::endl;
        std::cout << "Inserting test data..." << std::endl;

        for (size_t i = 0; i < NUM_SERIES; ++i) {
            auto insert = testData.generateInsert(baseTimestamp + i * 10000, 100);
            std::string seriesKey = insert.seriesKey();
            seriesKeys.push_back(seriesKey);
            engine->insert(insert).get();
        }

        seastar::sleep(std::chrono::milliseconds(100)).get();

        // Test concurrent queries
        const size_t CONCURRENT_QUERIES = 10;
        const size_t ITERATIONS = 10;

        std::cout << "Configuration:" << std::endl;
        std::cout << "  Concurrent queries: " << CONCURRENT_QUERIES << std::endl;
        std::cout << "  Iterations:         " << ITERATIONS << std::endl;
        std::cout << "========================================\n" << std::endl;

        for (size_t iter = 0; iter < ITERATIONS; ++iter) {
            auto start = std::chrono::high_resolution_clock::now();

            std::vector<seastar::future<std::optional<VariantQueryResult>>> futures;
            for (size_t q = 0; q < CONCURRENT_QUERIES; ++q) {
                std::string seriesKey = seriesKeys[q % seriesKeys.size()];
                futures.push_back(engine->query(seriesKey, baseTimestamp, baseTimestamp + 1000000));
            }

            // Wait for all queries to complete
            for (auto& f : futures) {
                f.get();
            }

            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

            std::cout << "Iteration " << (iter + 1) << ": "
                      << CONCURRENT_QUERIES << " queries in " << duration.count() << " ms"
                      << " (" << formatThroughput(CONCURRENT_QUERIES,
                                                 std::chrono::duration_cast<std::chrono::nanoseconds>(duration))
                      << ")" << std::endl;
        }

    }).join().get();
}
