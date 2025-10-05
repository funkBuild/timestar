#include <filesystem>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

// Forward declarations of test functions from leveldb_index_test.cpp
seastar::future<> runIndexTest();
seastar::future<> testSeriesIdGeneration();
seastar::future<> testMetadataIndexing();
seastar::future<> testFindSeries();
seastar::future<> testFindSeriesByTag();
seastar::future<> testGetSeriesGroupedByTag();
seastar::future<> testFieldTypes();
seastar::future<> testFieldStatistics();
seastar::future<> testSeriesMetadata();
seastar::future<> testGetAllMeasurements();

seastar::future<int> run_all_tests() {
    int failedTests = 0;
    int passedTests = 0;

    std::cout << "\n=== LevelDB Index Async Test Suite ===\n" << std::endl;

    // Test 1: Basic Index Operations
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.BasicIndexOperations" << std::endl;
        fs::remove_all("shard_999");
        co_await runIndexTest();
        fs::remove_all("shard_999");
        std::cout << "[       OK ] LevelDBAsyncTest.BasicIndexOperations" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.BasicIndexOperations - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_999");
    }

    // Test 2: Series ID Generation
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.SeriesIdGeneration" << std::endl;
        fs::remove_all("shard_998");
        co_await testSeriesIdGeneration();
        fs::remove_all("shard_998");
        std::cout << "[       OK ] LevelDBAsyncTest.SeriesIdGeneration" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.SeriesIdGeneration - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_998");
    }

    // Test 3: Metadata Indexing
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.MetadataIndexing" << std::endl;
        fs::remove_all("shard_997");
        co_await testMetadataIndexing();
        fs::remove_all("shard_997");
        std::cout << "[       OK ] LevelDBAsyncTest.MetadataIndexing" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.MetadataIndexing - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_997");
    }

    // Test 4: Find Series
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.FindSeries" << std::endl;
        fs::remove_all("shard_996");
        co_await testFindSeries();
        fs::remove_all("shard_996");
        std::cout << "[       OK ] LevelDBAsyncTest.FindSeries" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.FindSeries - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_996");
    }

    // Test 5: Find Series By Tag
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.FindSeriesByTag" << std::endl;
        fs::remove_all("shard_995");
        co_await testFindSeriesByTag();
        fs::remove_all("shard_995");
        std::cout << "[       OK ] LevelDBAsyncTest.FindSeriesByTag" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.FindSeriesByTag - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_995");
    }

    // Test 6: Get Series Grouped By Tag
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.GetSeriesGroupedByTag" << std::endl;
        fs::remove_all("shard_994");
        co_await testGetSeriesGroupedByTag();
        fs::remove_all("shard_994");
        std::cout << "[       OK ] LevelDBAsyncTest.GetSeriesGroupedByTag" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.GetSeriesGroupedByTag - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_994");
    }

    // Test 7: Field Types
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.FieldTypes" << std::endl;
        fs::remove_all("shard_993");
        co_await testFieldTypes();
        fs::remove_all("shard_993");
        std::cout << "[       OK ] LevelDBAsyncTest.FieldTypes" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.FieldTypes - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_993");
    }

    // Test 8: Field Statistics
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.FieldStatistics" << std::endl;
        fs::remove_all("shard_992");
        co_await testFieldStatistics();
        fs::remove_all("shard_992");
        std::cout << "[       OK ] LevelDBAsyncTest.FieldStatistics" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.FieldStatistics - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_992");
    }

    // Test 9: Series Metadata
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.SeriesMetadata" << std::endl;
        fs::remove_all("shard_991");
        co_await testSeriesMetadata();
        fs::remove_all("shard_991");
        std::cout << "[       OK ] LevelDBAsyncTest.SeriesMetadata" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.SeriesMetadata - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_991");
    }

    // Test 10: Get All Measurements
    try {
        std::cout << "[ RUN      ] LevelDBAsyncTest.GetAllMeasurements" << std::endl;
        fs::remove_all("shard_990");
        co_await testGetAllMeasurements();
        fs::remove_all("shard_990");
        std::cout << "[       OK ] LevelDBAsyncTest.GetAllMeasurements" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] LevelDBAsyncTest.GetAllMeasurements - " << e.what() << std::endl;
        failedTests++;
        fs::remove_all("shard_990");
    }

    // Summary
    std::cout << "\n=== Test Summary ===" << std::endl;
    std::cout << "Passed: " << passedTests << std::endl;
    std::cout << "Failed: " << failedTests << std::endl;

    co_return failedTests;
}

int main(int argc, char** argv) {
    seastar::app_template app;

    try {
        return app.run(argc, argv, [] {
            return run_all_tests().then([](int failed) {
                return seastar::make_ready_future<int>(failed);
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
