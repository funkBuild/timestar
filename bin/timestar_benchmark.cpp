#include "engine.hpp"
#include "logger.hpp"
#include "query_result.hpp"
#include "timestar_value.hpp"

#include <chrono>
#include <filesystem>
#include <iomanip>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/thread.hh>
#include <sstream>

namespace fs = std::filesystem;

// Helper to format duration in human-readable format
std::string formatDuration(std::chrono::nanoseconds ns) {
    if (ns.count() < 1000) {
        return std::to_string(ns.count()) + " ns";
    } else if (ns.count() < 1000000) {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << (ns.count() / 1000.0) << " µs";
        return ss.str();
    } else if (ns.count() < 1000000000) {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << (ns.count() / 1000000.0) << " ms";
        return ss.str();
    } else {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2) << (ns.count() / 1000000000.0) << " s";
        return ss.str();
    }
}

// Helper to format throughput
std::string formatThroughput(size_t operations, std::chrono::nanoseconds totalTime) {
    double seconds = totalTime.count() / 1000000000.0;
    if (seconds == 0)
        return "∞ ops/sec";

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
    double mean = 0;
    double median = 0;
    double p95 = 0;
    double p99 = 0;
    double min = 0;
    double max = 0;
    double stddev = 0;

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

// Test data generator
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

    TestData()
        : rng(42),  // Fixed seed for reproducibility
          measurementDist(0, measurements.size() - 1),
          hostDist(0, hosts.size() - 1),
          regionDist(0, regions.size() - 1),
          fieldDist(0, fields.size() - 1) {}

    TimeStarInsert<double> generateInsert(uint64_t timestamp, int batchSize = 10) {
        std::string measurement = measurements[measurementDist(rng)];
        std::string field = fields[fieldDist(rng)];

        TimeStarInsert<double> insert(measurement, field);
        insert.tags = {{"host", hosts[hostDist(rng)]}, {"region", regions[regionDist(rng)]}};

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

        return measurement + ",host=" + host + ",region=" + region + "." + field;
    }
};

seastar::future<> run_benchmark(seastar::sharded<Engine>& engine) {
    const size_t NUM_INSERTS = 1000;
    const size_t NUM_QUERIES = 1000;
    const int BATCH_SIZE = 10;  // Points per insert

    TestData testData;
    std::vector<std::string> insertedSeries;

    std::cout << "\n========================================" << std::endl;
    std::cout << "TimeStar Performance Benchmark" << std::endl;
    std::cout << "========================================" << std::endl;
    std::cout << "Configuration:" << std::endl;
    std::cout << "  CPU cores/shards:  " << seastar::smp::count << std::endl;
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

        // Track series for queries
        std::string seriesKey = insert.measurement + ",host=" + insert.tags["host"] +
                                ",region=" + insert.tags["region"] + "." + insert.field;
        insertedSeries.push_back(seriesKey);

        // NOTE: This benchmark uses std::hash for shard routing which differs from the
        // engine's PlacementTable::coreForHash(). Results are approximate for multi-shard
        // scenarios. For accurate benchmarks, use timestar_insert_bench / timestar_query_bench.
        std::hash<std::string> hasher;
        size_t hash = hasher(seriesKey);
        unsigned targetShard = hash % seastar::smp::count;

        auto opStart = std::chrono::high_resolution_clock::now();
        co_await engine.invoke_on(targetShard, [insert](Engine& e) { return e.insert(insert); });
        auto opEnd = std::chrono::high_resolution_clock::now();

        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(opEnd - opStart);
        insertLatencies.push_back(duration.count() / 1000000.0);  // Convert to ms

        // Progress indicator
        if ((i + 1) % 100 == 0) {
            std::cout << "Inserts completed: " << (i + 1) << "/" << NUM_INSERTS << "\r" << std::flush;
        }
    }

    auto insertEndTime = std::chrono::high_resolution_clock::now();
    auto totalInsertTime = std::chrono::duration_cast<std::chrono::nanoseconds>(insertEndTime - insertStartTime);

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
    co_await seastar::sleep(std::chrono::milliseconds(100));

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

        // Determine shard
        std::hash<std::string> hasher;
        size_t hash = hasher(seriesKey);
        unsigned targetShard = hash % seastar::smp::count;

        auto opStart = std::chrono::high_resolution_clock::now();
        auto result = co_await engine.invoke_on(
            targetShard, [seriesKey, startTime, endTime](Engine& e) { return e.query(seriesKey, startTime, endTime); });
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
    auto totalQueryTime = std::chrono::duration_cast<std::chrono::nanoseconds>(queryEndTime - queryStartTime);

    std::cout << "Queries completed: " << NUM_QUERIES << "/" << NUM_QUERIES << std::endl;

    // Calculate query statistics
    auto queryStats = Statistics::calculate(queryLatencies);

    // Print query results
    std::cout << "\n### QUERY PERFORMANCE ###" << std::endl;
    std::cout << "Total time:      " << formatDuration(totalQueryTime) << std::endl;
    std::cout << "Throughput:      " << formatThroughput(NUM_QUERIES, totalQueryTime) << std::endl;
    std::cout << "Points returned: " << totalPointsReturned << std::endl;
    std::cout << "Avg points/query: " << std::fixed << std::setprecision(2)
              << (totalPointsReturned / (double)NUM_QUERIES) << std::endl;
    std::cout << "Empty queries:   " << emptyQueries << " (" << std::fixed << std::setprecision(1)
              << (emptyQueries * 100.0 / NUM_QUERIES) << "%)" << std::endl;
    queryStats.print("Query Latency");

    // Combined statistics
    std::cout << "\n### COMBINED PERFORMANCE ###" << std::endl;
    auto totalTime = totalInsertTime + totalQueryTime;
    std::cout << "Total operations: " << (NUM_INSERTS + NUM_QUERIES) << std::endl;
    std::cout << "Total time:       " << formatDuration(totalTime) << std::endl;
    std::cout << "Overall throughput: " << formatThroughput(NUM_INSERTS + NUM_QUERIES, totalTime) << std::endl;

    // Memory and disk usage
    std::cout << "\n### RESOURCE USAGE ###" << std::endl;
    std::cout << "Shards used:     " << seastar::smp::count << std::endl;

    // Check disk usage
    size_t totalDiskUsage = 0;
    size_t tsmFiles = 0;
    size_t walFiles = 0;

    for (int i = 0; i < seastar::smp::count; ++i) {
        std::string shardPath = "shard_" + std::to_string(i);
        if (fs::exists(shardPath)) {
            for (const auto& entry : fs::recursive_directory_iterator(shardPath)) {
                if (fs::is_regular_file(entry)) {
                    size_t fileSize = fs::file_size(entry);
                    totalDiskUsage += fileSize;

                    std::string filename = entry.path().filename().string();
                    if (filename.find(".tsm") != std::string::npos) {
                        tsmFiles++;
                    } else if (filename.find(".wal") != std::string::npos) {
                        walFiles++;
                    }
                }
            }
        }
    }

    std::cout << "Disk usage:      " << std::fixed << std::setprecision(2) << (totalDiskUsage / 1024.0 / 1024.0)
              << " MB" << std::endl;
    std::cout << "TSM files:       " << tsmFiles << std::endl;
    std::cout << "WAL files:       " << walFiles << std::endl;
    std::cout << "Bytes per point: " << std::fixed << std::setprecision(2)
              << (totalDiskUsage / (double)(NUM_INSERTS * BATCH_SIZE)) << std::endl;

    // Compression ratio estimate (assuming 8 bytes timestamp + 8 bytes value uncompressed)
    double uncompressedSize = NUM_INSERTS * BATCH_SIZE * 16.0;
    double compressionRatio = uncompressedSize / totalDiskUsage;
    std::cout << "Compression ratio: " << std::fixed << std::setprecision(2) << compressionRatio << "x" << std::endl;

    std::cout << "\n========================================" << std::endl;
    std::cout << "Benchmark completed successfully!" << std::endl;
    std::cout << "========================================" << std::endl;

    co_return;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    return app.run(argc, argv, []() -> seastar::future<> {
        seastar::sharded<Engine> engine;

        // Initialize engine on all shards
        co_await engine.start();
        co_await engine.invoke_on_all([](Engine& e) { return e.init(); });

        // Set back-reference for cross-shard metadata indexing
        co_await engine.invoke_on_all([&engine](Engine& e) {
            e.setShardedRef(&engine);
            return seastar::make_ready_future<>();
        });

        co_await engine.invoke_on_all([](Engine& e) { return e.startBackgroundTasks(); });

        // Run benchmark
        co_await run_benchmark(engine);

        // Cleanup
        co_await engine.invoke_on_all([](Engine& e) { return e.stop(); });

        co_await engine.stop();
    });
}