#include <filesystem>
#include <iostream>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>

namespace fs = std::filesystem;

// Forward declarations of test functions from wal_test.cpp
seastar::future<> testWALWriteAndRecoverFloat();
seastar::future<> testWALWriteAndRecoverBoolean();
seastar::future<> testWALWriteAndRecoverString();
seastar::future<> testWALBatchInsert();
seastar::future<> testWALMultipleSeries();
seastar::future<> testWALDeleteRange();

std::string testDir = "./test_wal_async";

seastar::future<int> run_all_tests() {
    int failedTests = 0;
    int passedTests = 0;

    // Setup
    fs::create_directories(testDir);
    fs::current_path(testDir);

    std::cout << "\n=== WAL Async Test Suite ===\n" << std::endl;

    // Test 1: Write And Recover Float Data
    try {
        std::cout << "[ RUN      ] WALAsyncTest.WriteAndRecoverFloatData" << std::endl;
        co_await testWALWriteAndRecoverFloat();
        std::cout << "[       OK ] WALAsyncTest.WriteAndRecoverFloatData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.WriteAndRecoverFloatData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 2: Write And Recover Boolean Data
    try {
        std::cout << "[ RUN      ] WALAsyncTest.WriteAndRecoverBooleanData" << std::endl;
        co_await testWALWriteAndRecoverBoolean();
        std::cout << "[       OK ] WALAsyncTest.WriteAndRecoverBooleanData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.WriteAndRecoverBooleanData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 3: Write And Recover String Data
    try {
        std::cout << "[ RUN      ] WALAsyncTest.WriteAndRecoverStringData" << std::endl;
        co_await testWALWriteAndRecoverString();
        std::cout << "[       OK ] WALAsyncTest.WriteAndRecoverStringData" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.WriteAndRecoverStringData - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 4: Batch Insert
    try {
        std::cout << "[ RUN      ] WALAsyncTest.BatchInsert" << std::endl;
        co_await testWALBatchInsert();
        std::cout << "[       OK ] WALAsyncTest.BatchInsert" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.BatchInsert - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 5: Multiple Series
    try {
        std::cout << "[ RUN      ] WALAsyncTest.MultipleSeries" << std::endl;
        co_await testWALMultipleSeries();
        std::cout << "[       OK ] WALAsyncTest.MultipleSeries" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.MultipleSeries - " << e.what() << std::endl;
        failedTests++;
    }

    // Test 6: Delete Range
    try {
        std::cout << "[ RUN      ] WALAsyncTest.DeleteRange" << std::endl;
        co_await testWALDeleteRange();
        std::cout << "[       OK ] WALAsyncTest.DeleteRange" << std::endl;
        passedTests++;
    } catch (const std::exception& e) {
        std::cerr << "[  FAILED  ] WALAsyncTest.DeleteRange - " << e.what() << std::endl;
        failedTests++;
    }

    // Teardown
    fs::current_path("..");
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
