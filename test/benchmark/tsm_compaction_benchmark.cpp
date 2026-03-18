#include "../../lib/core/timestar_value.hpp"
#include "../../lib/storage/tsm.hpp"
#include "../../lib/storage/tsm_compactor.hpp"
#include "../../lib/storage/tsm_writer.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/seastar.hh>

namespace fs = std::filesystem;

// Configuration for benchmark
constexpr size_t NUM_FILES = 4;                        // Match TIER0_MIN_FILES threshold
constexpr size_t TARGET_FILE_SIZE = 50 * 1024 * 1024;  // 50MB per file
constexpr size_t NUM_SERIES = 500;                     // Number of unique time series (5x more realistic)
constexpr size_t POINTS_PER_SERIES_PER_FILE = 20000;   // 20K points = 2 blocks per series (multi-block!)
constexpr size_t POINTS_PER_BATCH = 10000;             // Points written per batch

// Global state for benchmark
struct BenchmarkState {
    std::vector<std::string> tsmFiles;
    std::string testDir = "shard_0";
    std::string tsmDir = "shard_0/tsm";
    std::vector<seastar::shared_ptr<TSM>> loadedFiles;
    size_t totalBytesGenerated = 0;
    size_t totalPointsGenerated = 0;
};

static BenchmarkState benchState;

// Generate realistic time series data with non-overlapping time ranges
seastar::future<> generateTSMFile(const std::string& filename, size_t fileIndex, size_t targetSize) {
    TSMWriter writer(filename);

    std::mt19937_64 rng(fileIndex * 12345);  // Deterministic random for reproducibility
    std::uniform_real_distribution<double> valueDist(0.0, 100.0);

    size_t pointsWritten = 0;

    // Each file gets a distinct time range - NO OVERLAP between files
    // File 0: timestamps 0-40K
    // File 1: timestamps 40K-80K
    // File 2: timestamps 80K-120K
    // File 3: timestamps 120K-160K
    uint64_t baseTimestamp = 1700000000000000000ULL;                                   // Nov 2023
    uint64_t fileTimeOffset = fileIndex * POINTS_PER_SERIES_PER_FILE * 1000000000ULL;  // 1 second intervals
    uint64_t currentTimestamp = baseTimestamp + fileTimeOffset;

    std::cout << "Generating TSM file " << (fileIndex + 1) << "/" << NUM_FILES << ": " << filename << std::endl;
    std::cout << "  Time range: " << currentTimestamp << " - "
              << (currentTimestamp + POINTS_PER_SERIES_PER_FILE * 1000000000ULL) << std::endl;
    std::cout << "  Series: " << NUM_SERIES << ", Points per series: " << POINTS_PER_SERIES_PER_FILE << std::endl;

    // Write all series once with their full time range for this file
    for (size_t seriesIdx = 0; seriesIdx < NUM_SERIES; seriesIdx++) {
        std::vector<uint64_t> timestamps;
        std::vector<double> values;
        timestamps.reserve(POINTS_PER_SERIES_PER_FILE);
        values.reserve(POINTS_PER_SERIES_PER_FILE);

        // Generate all points for this series in this file's time range
        for (size_t i = 0; i < POINTS_PER_SERIES_PER_FILE; i++) {
            uint64_t ts = currentTimestamp + (i * 1000000000ULL);  // 1 second intervals
            double value = valueDist(rng);

            timestamps.push_back(ts);
            values.push_back(value);
        }

        // Create unique series ID based on measurement + tags + field
        std::string measurement = "metrics";
        std::string field = "value";
        std::string location = "datacenter_" + std::to_string(seriesIdx / 100);
        std::string host = "server_" + std::to_string(seriesIdx % 100);

        TimeStarInsert<double> insert(measurement, field);
        insert.addTag("location", location);
        insert.addTag("host", host);
        SeriesId128 seriesId = insert.seriesId128();

        writer.writeSeries(TSMValueType::Float, seriesId, timestamps, values);
        pointsWritten += timestamps.size();

        if ((seriesIdx + 1) % 100 == 0) {
            std::cout << "  Progress: " << (seriesIdx + 1) << " / " << NUM_SERIES << " series" << std::endl;
        }
    }

    writer.writeIndex();
    writer.close();

    // Get actual file size
    size_t actualSize = fs::file_size(filename);
    std::cout << "  Generated: " << (actualSize / (1024.0 * 1024.0)) << " MB, " << pointsWritten << " total points"
              << std::endl;

    benchState.totalBytesGenerated += actualSize;
    benchState.totalPointsGenerated += pointsWritten;

    co_return;
}

// Setup: Generate 4 TSM files
seastar::future<> setupBenchmark() {
    std::cout << "\n=== TSM Compaction Benchmark Setup ===" << std::endl;
    std::cout << "Files: " << NUM_FILES << std::endl;
    std::cout << "Series per file: " << NUM_SERIES << std::endl;
    std::cout << "Points per series per file: " << POINTS_PER_SERIES_PER_FILE
              << " (multi-block: " << (POINTS_PER_SERIES_PER_FILE / 10000) << " blocks)" << std::endl;
    std::cout << "Total points per file: " << (NUM_SERIES * POINTS_PER_SERIES_PER_FILE) << std::endl;
    std::cout << "Total points across all files: " << (NUM_FILES * NUM_SERIES * POINTS_PER_SERIES_PER_FILE)
              << std::endl;
    std::cout << "Non-overlapping time ranges: Each file has distinct timestamps (no duplicates)" << std::endl;

    // Clean and create test directory
    fs::remove_all(benchState.testDir);
    fs::create_directories(benchState.tsmDir);

    auto startTime = std::chrono::steady_clock::now();

    // Generate files sequentially to avoid memory issues
    for (size_t i = 0; i < NUM_FILES; i++) {
        std::string filename = benchState.tsmDir + "/0_" + std::to_string(i) + ".tsm";
        benchState.tsmFiles.push_back(filename);
        co_await generateTSMFile(filename, i, TARGET_FILE_SIZE);
    }

    auto endTime = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    std::cout << "\nSetup complete in " << (duration.count() / 1000.0) << " seconds" << std::endl;
    std::cout << "Total data: " << (benchState.totalBytesGenerated / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "Total points: " << benchState.totalPointsGenerated << std::endl;
    std::cout << "Average file size: " << (benchState.totalBytesGenerated / NUM_FILES / (1024.0 * 1024.0)) << " MB"
              << std::endl;
    std::cout << "Write throughput: "
              << (benchState.totalBytesGenerated / (1024.0 * 1024.0)) / (duration.count() / 1000.0) << " MB/s"
              << std::endl;
}

// Benchmark: Compact 4 files into 1
seastar::future<CompactionStats> runCompactionBenchmark() {
    std::cout << "\n=== Running Compaction Benchmark ===" << std::endl;

    // Load all TSM files
    benchState.loadedFiles.clear();
    for (size_t i = 0; i < benchState.tsmFiles.size(); i++) {
        const auto& filename = benchState.tsmFiles[i];
        std::cout << "Loading file " << (i + 1) << "/" << benchState.tsmFiles.size() << ": " << filename << std::endl;

        try {
            auto tsm = seastar::make_shared<TSM>(filename);
            std::cout << "  Created TSM object, opening..." << std::endl;

            co_await tsm->open();  // Read index and tombstones
            std::cout << "  Opened successfully" << std::endl;

            benchState.loadedFiles.push_back(tsm);

            std::cout << "  Loaded: (Tier: " << tsm->tierNum << ", Seq: " << tsm->seqNum
                      << ", Series: " << tsm->getSeriesIds().size() << ")" << std::endl;
        } catch (const std::exception& e) {
            std::cerr << "  ERROR loading " << filename << ": " << e.what() << std::endl;
            throw;
        }
    }

    // Create compactor (without file manager for standalone benchmark)
    std::cout << "\nCreating compactor..." << std::endl;
    TSMCompactor compactor(nullptr);

    std::cout << "\nCompaction input:" << std::endl;
    std::cout << "  Source files: " << benchState.loadedFiles.size() << std::endl;
    std::cout << "  Total size: " << (benchState.totalBytesGenerated / (1024.0 * 1024.0)) << " MB" << std::endl;

    auto startTime = std::chrono::steady_clock::now();

    // Execute compaction using compact() instead of executeCompaction()
    // (executeCompaction() requires a fileManager which we don't have in the benchmark)
    std::cout << "\nExecuting compaction..." << std::endl;
    std::string outputFile;
    try {
        outputFile = (co_await compactor.compact(benchState.loadedFiles)).outputPath;
        std::cout << "Compaction completed successfully" << std::endl;
        std::cout << "Output file: " << outputFile << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "ERROR during compaction: " << e.what() << std::endl;
        throw;
    }

    auto endTime = std::chrono::steady_clock::now();

    // Build stats manually since we used compact() instead of executeCompaction()
    CompactionStats stats;
    stats.filesCompacted = benchState.loadedFiles.size();
    stats.duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Get output file size
    if (fs::exists(outputFile)) {
        stats.bytesWritten = fs::file_size(outputFile);
    }
    stats.bytesRead = benchState.totalBytesGenerated;

    co_return stats;
}

// Print detailed benchmark results
void printResults(const CompactionStats& stats) {
    std::cout << "\n=== Compaction Results ===" << std::endl;
    std::cout << "Duration: " << stats.duration.count() << " ms (" << (stats.duration.count() / 1000.0) << " seconds)"
              << std::endl;
    std::cout << "Files compacted: " << stats.filesCompacted << std::endl;
    std::cout << "Bytes read: " << (stats.bytesRead / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "Bytes written: " << (stats.bytesWritten / (1024.0 * 1024.0)) << " MB" << std::endl;
    std::cout << "Points read: " << stats.pointsRead << std::endl;
    std::cout << "Points written: " << stats.pointsWritten << std::endl;
    std::cout << "Duplicates removed: " << stats.duplicatesRemoved << std::endl;

    std::cout << "\n=== Performance Metrics ===" << std::endl;
    double durationSec = stats.duration.count() / 1000.0;
    std::cout << "Read throughput: " << (stats.bytesRead / (1024.0 * 1024.0)) / durationSec << " MB/s" << std::endl;
    std::cout << "Write throughput: " << (stats.bytesWritten / (1024.0 * 1024.0)) / durationSec << " MB/s" << std::endl;
    std::cout << "Point processing rate: " << (stats.pointsRead / durationSec) << " points/sec" << std::endl;

    if (stats.bytesRead > 0) {
        double compressionRatio = (double)stats.bytesWritten / stats.bytesRead;
        std::cout << "Compression ratio: " << (compressionRatio * 100.0) << "% of input" << std::endl;
    }

    // Check output file
    std::string outputFile = benchState.tsmDir + "/1_100.tsm";
    if (fs::exists(outputFile)) {
        size_t outputSize = fs::file_size(outputFile);
        std::cout << "\nOutput file size: " << (outputSize / (1024.0 * 1024.0)) << " MB" << std::endl;
    }
}

// Cleanup
void cleanupBenchmark() {
    std::cout << "\nCleaning up..." << std::endl;
    benchState.loadedFiles.clear();
    // Optionally keep files for inspection: fs::remove_all(benchState.testDir);
    std::cout << "Test files kept in: " << benchState.tsmDir << std::endl;
}

// Main benchmark runner
seastar::future<int> runBenchmark() {
    try {
        // Setup phase
        co_await setupBenchmark();

        // Run benchmark 3 times for consistency
        std::cout << "\n=== Running Benchmark (3 iterations) ===" << std::endl;

        std::vector<CompactionStats> results;
        for (int i = 0; i < 3; i++) {
            std::cout << "\n--- Iteration " << (i + 1) << " ---" << std::endl;

            auto stats = co_await runCompactionBenchmark();
            results.push_back(stats);

            printResults(stats);

            // Clean up output file between runs
            std::string outputFile = benchState.tsmDir + "/1_100.tsm";
            if (fs::exists(outputFile)) {
                fs::remove(outputFile);
            }

            if (i < 2) {
                std::cout << "\nWaiting 2 seconds before next iteration..." << std::endl;
                co_await seastar::sleep(std::chrono::seconds(2));
            }
        }

        // Calculate averages
        std::cout << "\n=== Average Results (3 iterations) ===" << std::endl;
        CompactionStats avg;
        for (const auto& s : results) {
            avg.duration += s.duration;
            avg.bytesRead += s.bytesRead;
            avg.bytesWritten += s.bytesWritten;
            avg.pointsRead += s.pointsRead;
            avg.pointsWritten += s.pointsWritten;
            avg.duplicatesRemoved += s.duplicatesRemoved;
        }
        avg.duration /= 3;
        avg.bytesRead /= 3;
        avg.bytesWritten /= 3;
        avg.pointsRead /= 3;
        avg.pointsWritten /= 3;
        avg.duplicatesRemoved /= 3;

        printResults(avg);

        cleanupBenchmark();

        co_return 0;

    } catch (const std::exception& e) {
        std::cerr << "Benchmark failed: " << e.what() << std::endl;
        co_return 1;
    }
}

int main(int argc, char** argv) {
    seastar::app_template app;

    std::cout << "TSM Compaction Performance Benchmark" << std::endl;
    std::cout << "=====================================" << std::endl;

    try {
        return app.run(argc, argv, [] {
            return runBenchmark().then([](int exitCode) { return seastar::make_ready_future<int>(exitCode); });
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
