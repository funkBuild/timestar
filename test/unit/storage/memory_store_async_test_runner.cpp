#include <filesystem>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

// Forward declarations of test functions from memory_store_test.cpp
seastar::future<> testMemoryStoreInitWAL();
seastar::future<> testMemoryStoreInitFromWAL();
seastar::future<> testMemoryStoreBatchInsert();
seastar::future<> testMemoryStoreThresholdChecking();

std::string testDir = "./test_memory_store_async";

seastar::future<int> run_all_tests() {
    int failedTests = 0;
    int passedTests = 0;

    // Setup
    fs::create_directories(testDir);
    fs::create_directories(testDir + "/shard_0");

    std::cout << "\n=== MemoryStore Async Test Suite ===\n" << std::endl;

    // Test 1: Init WAL
    try {
        std::cout << "[ RUN      ] MemoryStoreAsyncTest.InitWAL" << std::endl;
        co_await testMemoryStoreInitWAL();
        std::cout << "[       OK ] MemoryStoreAsyncTest.InitWAL" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] MemoryStoreAsyncTest.InitWAL - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 2: Init From WAL
    try {
        std::cout << "[ RUN      ] MemoryStoreAsyncTest.InitFromWAL" << std::endl;
        co_await testMemoryStoreInitFromWAL();
        std::cout << "[       OK ] MemoryStoreAsyncTest.InitFromWAL" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] MemoryStoreAsyncTest.InitFromWAL - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 3: Batch Insert
    try {
        std::cout << "[ RUN      ] MemoryStoreAsyncTest.BatchInsert" << std::endl;
        co_await testMemoryStoreBatchInsert();
        std::cout << "[       OK ] MemoryStoreAsyncTest.BatchInsert" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] MemoryStoreAsyncTest.BatchInsert - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 4: Threshold Checking
    try {
        std::cout << "[ RUN      ] MemoryStoreAsyncTest.ThresholdChecking" << std::endl;
        co_await testMemoryStoreThresholdChecking();
        std::cout << "[       OK ] MemoryStoreAsyncTest.ThresholdChecking" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] MemoryStoreAsyncTest.ThresholdChecking - " << e.what() << std::endl;
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
