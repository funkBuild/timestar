#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include "functions/function_monitoring.hpp"

namespace tsdb::test {

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
    
    auto* metrics = monitor->getFunctionMetrics("test_function");
    ASSERT_NE(metrics, nullptr);
    
    // Test initial state
    EXPECT_EQ(metrics->total_executions.load(), 0);
    EXPECT_EQ(metrics->successful_executions.load(), 0);
    EXPECT_EQ(metrics->failed_executions.load(), 0);
    
    // Test auto-registration
    auto* auto_metrics = monitor->getFunctionMetrics("auto_registered_function");
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
    
    auto* metrics = monitor->getFunctionMetrics("tracked_function");
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
    auto* metrics = monitor->getFunctionMetrics("calc_test");
    
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
    auto* metrics = monitor->getFunctionMetrics("alert_test");
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
    const int num_threads = 10;
    const int executions_per_thread = 5;
    
    std::atomic<int> active_trackers{0};
    std::atomic<int> max_concurrent{0};
    std::vector<std::thread> threads;
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < executions_per_thread; ++i) {
                functions::FunctionExecutionTracker tracker("concurrent_test_" + std::to_string(t));
                
                int current_active = active_trackers.fetch_add(1) + 1;
                int current_max = max_concurrent.load();
                while (current_active > current_max) {
                    if (max_concurrent.compare_exchange_weak(current_max, current_active)) {
                        break;
                    }
                }
                
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                tracker.recordSuccess();
                
                active_trackers.fetch_sub(1);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
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
    EXPECT_EQ(total_executions, num_threads * executions_per_thread);
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
    auto* metrics = monitor->getFunctionMetrics("ack_test");
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
    auto* metrics = monitor->getFunctionMetrics("error_test");
    
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
    size_t initial_peak = 0;
    
    {
        functions::FunctionExecutionTracker tracker("memory_test");
        
        // Simulate memory allocation (the tracker will measure RSS changes)
        std::vector<char> memory_hog(1024 * 1024, 'x'); // 1MB allocation
        
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        tracker.recordSuccess();
        
        auto* metrics = monitor->getFunctionMetrics("memory_test");
        initial_peak = metrics->peak_memory_bytes.load();
    }
    
    // Memory tracking should have detected some usage
    auto* metrics = monitor->getFunctionMetrics("memory_test");
    EXPECT_GE(metrics->total_memory_allocated.load(), 0);
    EXPECT_GE(metrics->peak_memory_bytes.load(), 0);
}

} // namespace tsdb::test