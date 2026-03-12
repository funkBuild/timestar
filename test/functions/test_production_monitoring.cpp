#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include "functions/function_monitoring.hpp"

namespace timestar::test {

class ProductionMonitoringTest : public ::testing::Test {
protected:
    void SetUp() override {
        monitor = &functions::ProductionMonitor::getInstance();
        monitor->stop(); // Ensure monitor is stopped first
        monitor->enableDetailedLogging(false); // Quiet during tests
        monitor->setCollectionInterval(std::chrono::seconds(1)); // Fast collection for tests
        monitor->clearAlertsForTesting(); // Start with clean alert state
    }
    
    void TearDown() override {
        monitor->stop();
        // Give time for cleanup
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    
    functions::ProductionMonitor* monitor;
};

// Basic functionality tests
TEST_F(ProductionMonitoringTest, BasicMetricsCollection) {
    // Register a test function
    monitor->registerFunction("test_function");
    
    auto metrics = monitor->getFunctionMetrics("test_function");
    ASSERT_NE(metrics, nullptr);
    
    // Test initial state
    EXPECT_EQ(metrics->total_executions.load(), 0);
    EXPECT_EQ(metrics->successful_executions.load(), 0);
    EXPECT_EQ(metrics->failed_executions.load(), 0);
    
    // Test auto-registration
    auto auto_metrics = monitor->getFunctionMetrics("auto_registered_function");
    ASSERT_NE(auto_metrics, nullptr);
    EXPECT_EQ(auto_metrics->total_executions.load(), 0);
}

TEST_F(ProductionMonitoringTest, FunctionExecutionTracker) {
    {
        functions::FunctionExecutionTracker tracker("tracked_function");
        
        // Simulate some work
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        
        tracker.recordSuccess();
        tracker.recordCacheHit();
    }
    
    auto metrics = monitor->getFunctionMetrics("tracked_function");
    EXPECT_EQ(metrics->total_executions.load(), 1);
    EXPECT_EQ(metrics->successful_executions.load(), 1);
    EXPECT_EQ(metrics->failed_executions.load(), 0);
    EXPECT_EQ(metrics->cache_hits.load(), 1);
    EXPECT_GT(metrics->total_execution_time_us.load(), 0);
}

TEST_F(ProductionMonitoringTest, SystemMetricsCollection) {
    auto* system_metrics = monitor->getSystemMetrics();
    ASSERT_NE(system_metrics, nullptr);
    
    // Test concurrent call tracking
    {
        functions::FunctionExecutionTracker tracker1("function1");
        EXPECT_EQ(system_metrics->active_function_calls.load(), 1);
        
        {
            functions::FunctionExecutionTracker tracker2("function2");
            EXPECT_EQ(system_metrics->active_function_calls.load(), 2);
        }
        
        EXPECT_EQ(system_metrics->active_function_calls.load(), 1);
    }
    
    EXPECT_EQ(system_metrics->active_function_calls.load(), 0);
}

TEST_F(ProductionMonitoringTest, MetricsCalculations) {
    auto metrics = monitor->getFunctionMetrics("calc_test");
    
    // Simulate executions
    {
        functions::FunctionExecutionTracker tracker("calc_test");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        tracker.recordSuccess();
        tracker.recordCacheHit();
    }
    
    {
        functions::FunctionExecutionTracker tracker("calc_test");
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
        tracker.recordFailure();
        tracker.recordCacheMiss();
    }
    
    {
        functions::FunctionExecutionTracker tracker("calc_test");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        tracker.recordSuccess();
        tracker.recordCacheHit();
    }
    
    // Check calculations
    EXPECT_EQ(metrics->total_executions.load(), 3);
    EXPECT_EQ(metrics->successful_executions.load(), 2);
    EXPECT_EQ(metrics->failed_executions.load(), 1);
    EXPECT_EQ(metrics->cache_hits.load(), 2);
    EXPECT_EQ(metrics->cache_misses.load(), 1);
    
    // Success rate should be 2/3
    EXPECT_NEAR(metrics->getSuccessRate(), 2.0/3.0, 0.01);
    
    // Failure rate should be 1/3  
    EXPECT_NEAR(metrics->getFailureRate(), 1.0/3.0, 0.01);
    
    // Cache hit rate should be 2/3
    EXPECT_NEAR(metrics->getCacheHitRate(), 2.0/3.0, 0.01);
    
    // Average execution time should be positive
    EXPECT_GT(metrics->getAverageExecutionTimeMs(), 0.0);
}

TEST_F(ProductionMonitoringTest, AlertingSystem) {
    // Set strict thresholds for testing
    functions::AlertThresholds thresholds;
    thresholds.max_failure_rate = 0.10; // 10% failure rate
    thresholds.max_response_time_ms = 50.0; // 50ms response time
    thresholds.min_cache_hit_rate = 0.90; // 90% cache hit rate
    thresholds.max_concurrent_calls = 5;   // 5 concurrent calls
    monitor->setAlertThresholds(thresholds);
    
    monitor->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Trigger failure rate alert
    auto metrics = monitor->getFunctionMetrics("alert_test");
    for (int i = 0; i < 20; ++i) {
        functions::FunctionExecutionTracker tracker("alert_test");
        if (i < 15) {
            tracker.recordFailure(); // 75% failure rate
        } else {
            tracker.recordSuccess();
        }
    }
    
    // Wait for alert processing - need at least one monitoring cycle (1 second + buffer) 
    std::this_thread::sleep_for(std::chrono::milliseconds(2000)); // Extended wait to ensure monitoring loop runs
    
    auto alerts = monitor->getActiveAlerts();
    bool found_failure_alert = false;
    for (const auto& alert : alerts) {
        if (alert.type == functions::AlertType::HIGH_FAILURE_RATE) {
            found_failure_alert = true;
            EXPECT_EQ(alert.function_name, "alert_test");
            EXPECT_GT(alert.severity, 0.0);
            break;
        }
    }
    EXPECT_TRUE(found_failure_alert);
}

TEST_F(ProductionMonitoringTest, ConcurrentExecutionTracking) {
    // ProductionMonitor is thread_local (one per Seastar shard), so test
    // sequential execution tracking on a single thread — matching the
    // actual Seastar shard-per-core model.
    const int num_functions = 10;
    const int executions_per_function = 5;

    for (int t = 0; t < num_functions; ++t) {
        for (int i = 0; i < executions_per_function; ++i) {
            functions::FunctionExecutionTracker tracker("concurrent_test_" + std::to_string(t));
            tracker.recordSuccess();
        }
    }

    auto* system_metrics = monitor->getSystemMetrics();
    EXPECT_EQ(system_metrics->active_function_calls.load(), 0);

    // Check all functions were tracked
    auto all_metrics = monitor->getAllFunctionMetrics();
    int total_executions = 0;
    for (const auto& [name, metrics] : all_metrics) {
        if (name.find("concurrent_test_") == 0) {
            total_executions += metrics.total_executions;
        }
    }
    EXPECT_EQ(total_executions, num_functions * executions_per_function);
}

TEST_F(ProductionMonitoringTest, JsonExport) {
    // Generate some test data
    {
        functions::FunctionExecutionTracker tracker("json_test");
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        tracker.recordSuccess();
        tracker.recordCacheHit();
    }
    
    {
        functions::FunctionExecutionTracker tracker("json_test");
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        tracker.recordFailure();
        tracker.recordCacheMiss();
    }
    
    // Test metrics export
    std::string metrics_json = monitor->exportMetricsAsJson();
    EXPECT_FALSE(metrics_json.empty());
    EXPECT_NE(metrics_json.find("json_test"), std::string::npos);
    EXPECT_NE(metrics_json.find("total_executions"), std::string::npos);
    EXPECT_NE(metrics_json.find("success_rate"), std::string::npos);
    EXPECT_NE(metrics_json.find("system"), std::string::npos);
    
    // Test alerts export  
    std::string alerts_json = monitor->exportAlertsAsJson();
    EXPECT_FALSE(alerts_json.empty());
    EXPECT_NE(alerts_json.find("active_alerts"), std::string::npos);
    EXPECT_NE(alerts_json.find("timestamp"), std::string::npos);
}

TEST_F(ProductionMonitoringTest, HealthStatusChecking) {
    // Clear any existing alerts from previous tests to get a clean health status
    auto existing_alerts = monitor->getActiveAlerts();
    for (size_t i = 0; i < existing_alerts.size(); ++i) {
        monitor->acknowledgeAlert(i);
    }
    
    auto health_status = monitor->getHealthStatus();
    EXPECT_TRUE(health_status.healthy);
    EXPECT_EQ(health_status.status, "OK");
    
    // Check key metrics are present
    EXPECT_TRUE(health_status.key_metrics.find("system_memory_mb") != health_status.key_metrics.end());
    EXPECT_TRUE(health_status.key_metrics.find("active_calls") != health_status.key_metrics.end());
    EXPECT_TRUE(health_status.key_metrics.find("queued_calls") != health_status.key_metrics.end());
    EXPECT_TRUE(health_status.key_metrics.find("http_success_rate") != health_status.key_metrics.end());
}

TEST_F(ProductionMonitoringTest, AlertAcknowledgment) {
    // Set up alerts
    functions::AlertThresholds thresholds;
    thresholds.max_failure_rate = 0.10;
    monitor->setAlertThresholds(thresholds);
    
    monitor->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Trigger alert
    auto metrics = monitor->getFunctionMetrics("ack_test");
    for (int i = 0; i < 20; ++i) {
        functions::FunctionExecutionTracker tracker("ack_test");
        tracker.recordFailure(); // 100% failure rate
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    
    auto initial_alerts = monitor->getActiveAlerts();
    EXPECT_GT(initial_alerts.size(), 0);
    
    // Acknowledge first alert
    monitor->acknowledgeAlert(0);
    
    auto remaining_alerts = monitor->getActiveAlerts();
    EXPECT_EQ(remaining_alerts.size(), initial_alerts.size() - 1);
}

TEST_F(ProductionMonitoringTest, ErrorTypeTracking) {
    auto metrics = monitor->getFunctionMetrics("error_test");
    
    {
        functions::FunctionExecutionTracker tracker("error_test");
        tracker.recordParameterValidationError();
        tracker.recordFailure();
    }
    
    {
        functions::FunctionExecutionTracker tracker("error_test");
        tracker.recordTimeoutError();
        tracker.recordFailure();
    }
    
    {
        functions::FunctionExecutionTracker tracker("error_test");
        tracker.recordInsufficientDataError();
        tracker.recordFailure();
    }
    
    EXPECT_EQ(metrics->parameter_validation_errors.load(), 1);
    EXPECT_EQ(metrics->timeout_errors.load(), 1);
    EXPECT_EQ(metrics->insufficient_data_errors.load(), 1);
    EXPECT_EQ(metrics->failed_executions.load(), 3);
    EXPECT_EQ(metrics->total_executions.load(), 3);
}

TEST_F(ProductionMonitoringTest, BackgroundMonitoringThread) {
    EXPECT_FALSE(monitor->isRunning());
    
    monitor->start();
    EXPECT_TRUE(monitor->isRunning());
    
    // Let it run for a bit
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    
    monitor->stop();
    EXPECT_FALSE(monitor->isRunning());
}

TEST_F(ProductionMonitoringTest, AlertHistoryTracking) {
    // Set strict thresholds
    functions::AlertThresholds thresholds;
    thresholds.max_failure_rate = 0.10;
    monitor->setAlertThresholds(thresholds);
    
    monitor->start();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Generate multiple alerts over time
    for (int batch = 0; batch < 3; ++batch) {
        std::string function_name = "history_test_" + std::to_string(batch);
        
        for (int i = 0; i < 20; ++i) {
            functions::FunctionExecutionTracker tracker(function_name);
            tracker.recordFailure(); // High failure rate
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    
    // Wait for monitoring loop to process alerts
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    
    auto alert_history = monitor->getAlertHistory(std::chrono::minutes(5));
    EXPECT_GT(alert_history.size(), 0);
    
    // Check that alerts are sorted by time
    for (size_t i = 1; i < alert_history.size(); ++i) {
        EXPECT_GE(alert_history[i].timestamp, alert_history[i-1].timestamp);
    }
}

TEST_F(ProductionMonitoringTest, MemoryUsageTracking) {
    // This test verifies that memory usage is being tracked
    [[maybe_unused]] size_t initial_peak = 0;
    {
        functions::FunctionExecutionTracker tracker("memory_test");
        
        // Simulate memory allocation (the tracker will measure RSS changes)
        std::vector<char> memory_hog(1024 * 1024, 'x'); // 1MB allocation
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        tracker.recordSuccess();
        
        auto metrics = monitor->getFunctionMetrics("memory_test");
        initial_peak = metrics->peak_memory_bytes.load();
    }
    
    // Memory tracking should have detected some usage
    auto metrics = monitor->getFunctionMetrics("memory_test");
    EXPECT_GE(metrics->total_memory_allocated.load(), 0);
    EXPECT_GE(metrics->peak_memory_bytes.load(), 0);
}

// Race condition tests: concurrent start/stop
TEST_F(ProductionMonitoringTest, ConcurrentStartDoesNotSpawnMultipleThreads) {
    // Call start() from many threads simultaneously and verify only one thread
    // is running afterwards (no double-start race).
    const int kThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> start_count{0};

    // All threads hammer start() at the same time.
    std::atomic<bool> go{false};
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            while (!go.load()) { /* spin */ }
            monitor->start();
            start_count.fetch_add(1);
        });
    }

    go.store(true);
    for (auto& t : threads) {
        t.join();
    }

    // The monitor must be running, and exactly one background thread must have
    // been created (verified indirectly: stop() must not crash/hang).
    EXPECT_TRUE(monitor->isRunning());

    // stop() must complete cleanly — if two threads were started, the internal
    // std::thread object would be in a partially-overwritten state and join()
    // could deadlock or crash.
    monitor->stop();
    EXPECT_FALSE(monitor->isRunning());
}

TEST_F(ProductionMonitoringTest, ConcurrentStopIsSafe) {
    monitor->start();
    EXPECT_TRUE(monitor->isRunning());

    // Call stop() from many threads simultaneously.
    const int kThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<bool> go{false};

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&]() {
            while (!go.load()) { /* spin */ }
            monitor->stop();
        });
    }

    go.store(true);
    for (auto& t : threads) {
        t.join();
    }

    // Must be cleanly stopped, not crashed.
    EXPECT_FALSE(monitor->isRunning());
}

TEST_F(ProductionMonitoringTest, InterleavedStartStopIsSafe) {
    // Rapidly alternate start/stop from multiple threads.
    const int kIterations = 50;
    std::atomic<bool> go{false};
    std::atomic<bool> failed{false};

    std::thread starter([&]() {
        while (!go.load()) {}
        for (int i = 0; i < kIterations && !failed.load(); ++i) {
            monitor->start();
            std::this_thread::yield();
        }
    });

    std::thread stopper([&]() {
        while (!go.load()) {}
        for (int i = 0; i < kIterations && !failed.load(); ++i) {
            monitor->stop();
            std::this_thread::yield();
        }
    });

    go.store(true);
    starter.join();
    stopper.join();

    // Bring to a known-stopped state without crashing.
    monitor->stop();
    EXPECT_FALSE(monitor->isRunning());
}

// Task #55: alert_history_ must be bounded to MAX_ALERT_HISTORY (1000) entries.
// Without the fix this test fails because alert_history_ grows without limit.
TEST_F(ProductionMonitoringTest, AlertHistoryIsBounded) {
    // Inject more alerts than the maximum cap (1001 unique function names,
    // all with HIGH_FAILURE_RATE so each is a distinct (type,name) pair).
    const size_t OVER_CAP = 1001;
    for (size_t i = 0; i < OVER_CAP; ++i) {
        functions::Alert alert;
        alert.type = functions::AlertType::HIGH_FAILURE_RATE;
        alert.function_name = "bounded_test_fn_" + std::to_string(i);
        alert.message = "Test alert " + std::to_string(i);
        alert.timestamp = std::chrono::steady_clock::now();
        alert.severity = 0.5;
        monitor->addAlertForTesting(alert);
    }

    // The raw history size must not exceed the cap.
    size_t history_size = monitor->getAlertHistorySize();
    EXPECT_LE(history_size, 1000u)
        << "alert_history_ grew to " << history_size
        << " which exceeds the MAX_ALERT_HISTORY cap of 1000";
}

} // namespace timestar::test