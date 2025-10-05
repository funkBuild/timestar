#include <filesystem>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

// Forward declarations of test functions from tsm_test.cpp
seastar::future<> testTSMReadFloat(std::string filename);
seastar::future<> testTSMReadBoolean(std::string filename);
seastar::future<> testTSMReadString(std::string filename);
seastar::future<> testTSMReadTimeRange(std::string filename);
seastar::future<> testTSMReadMultipleSeries(std::string filename);
seastar::future<> testTSMDeleteRange(std::string filename);
seastar::future<> testTSMQueryWithTombstones(std::string filename);
seastar::future<> testTSMLoadTombstones(std::string filename);

std::string testDir = "./test_tsm_async";

std::string getTestFilePath(const std::string& filename) {
    return testDir + "/" + filename;
}

seastar::future<int> run_all_tests() {
    int failedTests = 0;
    int passedTests = 0;

    // Setup
    fs::create_directories(testDir);

    std::cout << "\n=== TSM Async Test Suite ===\n" << std::endl;

    // Test 1: Read Float Data
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.ReadFloatData" << std::endl;
        co_await testTSMReadFloat("test_tsm_async/0_1.tsm");
        std::cout << "[       OK ] TSMAsyncTest.ReadFloatData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.ReadFloatData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 2: Read Boolean Data
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.ReadBooleanData" << std::endl;
        co_await testTSMReadBoolean("test_tsm_async/0_2.tsm");
        std::cout << "[       OK ] TSMAsyncTest.ReadBooleanData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.ReadBooleanData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 3: Read String Data
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.ReadStringData" << std::endl;
        co_await testTSMReadString("test_tsm_async/0_3.tsm");
        std::cout << "[       OK ] TSMAsyncTest.ReadStringData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.ReadStringData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 4: Read Time Range
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.ReadTimeRange" << std::endl;
        co_await testTSMReadTimeRange("test_tsm_async/0_4.tsm");
        std::cout << "[       OK ] TSMAsyncTest.ReadTimeRange" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.ReadTimeRange - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 5: Read Multiple Series
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.ReadMultipleSeries" << std::endl;
        co_await testTSMReadMultipleSeries("test_tsm_async/0_5.tsm");
        std::cout << "[       OK ] TSMAsyncTest.ReadMultipleSeries" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.ReadMultipleSeries - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 6: Delete Range
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.DeleteRange" << std::endl;
        co_await testTSMDeleteRange("test_tsm_async/0_6.tsm");
        std::cout << "[       OK ] TSMAsyncTest.DeleteRange" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.DeleteRange - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 7: Query With Tombstones
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.QueryWithTombstones" << std::endl;
        co_await testTSMQueryWithTombstones("test_tsm_async/0_7.tsm");
        std::cout << "[       OK ] TSMAsyncTest.QueryWithTombstones" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.QueryWithTombstones - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 8: Load Tombstones
    try {
        std::cout << "[ RUN      ] TSMAsyncTest.LoadTombstones" << std::endl;
        co_await testTSMLoadTombstones("test_tsm_async/0_8.tsm");
        std::cout << "[       OK ] TSMAsyncTest.LoadTombstones" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] TSMAsyncTest.LoadTombstones - " << e.what() << std::endl;
        failedTests++;
    }

    // Teardown
    fs::remove_all(testDir);

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
