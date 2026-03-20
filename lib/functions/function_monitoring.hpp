#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>
#include <string>
#include <unordered_set>
#include <vector>

namespace timestar::functions {

// Forward declarations
class FunctionPerformanceTracker;

// Snapshot structure for returning non-atomic metrics
struct FunctionMetricsSnapshot {
    uint64_t total_executions{0};
    uint64_t successful_executions{0};
    uint64_t failed_executions{0};
    uint64_t total_execution_time_us{0};
    uint64_t peak_memory_bytes{0};
    uint64_t total_memory_allocated{0};
    uint64_t total_cpu_time_us{0};
    uint64_t cache_hits{0};
    uint64_t cache_misses{0};
    uint64_t cache_evictions{0};
    uint64_t timeout_errors{0};
    uint64_t parameter_validation_errors{0};
    uint64_t insufficient_data_errors{0};

    // Calculate derived metrics
    double getSuccessRate() const {
        return total_executions > 0 ? static_cast<double>(successful_executions) / total_executions : 0.0;
    }

    double getFailureRate() const {
        return total_executions > 0 ? static_cast<double>(failed_executions) / total_executions : 0.0;
    }

    double getAverageExecutionTimeMs() const {
        return successful_executions > 0 ? static_cast<double>(total_execution_time_us) / successful_executions / 1000.0
                                         : 0.0;
    }

    double getCacheHitRate() const {
        uint64_t total = cache_hits + cache_misses;
        return total > 0 ? static_cast<double>(cache_hits) / total : 0.0;
    }

    double getAverageMemoryUsageMB() const {
        return successful_executions > 0
                   ? static_cast<double>(total_memory_allocated) / successful_executions / 1024 / 1024
                   : 0.0;
    }
};

// Monitoring metrics structure
struct FunctionMetrics {
    // Execution metrics
    std::atomic<uint64_t> total_executions{0};
    std::atomic<uint64_t> successful_executions{0};
    std::atomic<uint64_t> failed_executions{0};
    std::atomic<uint64_t> total_execution_time_us{0};

    // Resource metrics
    // Note (FUNC-H7): peak_memory_bytes was previously updated with a
    // non-atomic load-compare-store (`if (cur > peak) peak = cur`).  That
    // pattern was removed when memory tracking was taken out of the hot path.
    // The field is now only written by background collection.  Additionally,
    // ProductionMonitor is `static thread_local` (see getInstance()), so all
    // access is single-threaded per shard, making the atomic sufficient as-is.
    std::atomic<uint64_t> peak_memory_bytes{0};
    std::atomic<uint64_t> total_memory_allocated{0};
    std::atomic<uint64_t> total_cpu_time_us{0};

    // Cache metrics
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> cache_evictions{0};

    // Quality metrics
    std::atomic<uint64_t> timeout_errors{0};
    std::atomic<uint64_t> parameter_validation_errors{0};
    std::atomic<uint64_t> insufficient_data_errors{0};

    // Calculate derived metrics
    // Note (FUNC-H6): These methods load multiple atomics non-atomically, which
    // would be a TOCTOU race under concurrent access.  This is safe because the
    // owning ProductionMonitor is `static thread_local` (see getInstance()),
    // so all reads and writes to these atomics happen on a single Seastar shard
    // thread -- no cross-thread visibility concern.
    double getSuccessRate() const {
        uint64_t total = total_executions.load();
        return total > 0 ? static_cast<double>(successful_executions.load()) / total : 0.0;
    }

    double getFailureRate() const {
        uint64_t total = total_executions.load();
        return total > 0 ? static_cast<double>(failed_executions.load()) / total : 0.0;
    }

    double getAverageExecutionTimeMs() const {
        uint64_t executions = successful_executions.load();
        return executions > 0 ? static_cast<double>(total_execution_time_us.load()) / executions / 1000.0 : 0.0;
    }

    double getCacheHitRate() const {
        uint64_t total = cache_hits.load() + cache_misses.load();
        return total > 0 ? static_cast<double>(cache_hits.load()) / total : 0.0;
    }

    double getAverageMemoryUsageMB() const {
        uint64_t executions = successful_executions.load();
        return executions > 0 ? static_cast<double>(total_memory_allocated.load()) / executions / 1024 / 1024 : 0.0;
    }
};

// System metrics snapshot for returning non-atomic metrics
struct SystemMetricsSnapshot {
    uint64_t active_function_calls{0};
    uint64_t queued_function_calls{0};
    uint64_t total_registry_lookups{0};
    uint64_t registry_cache_hits{0};
    uint64_t concurrent_execution_peak{0};
    uint64_t system_memory_bytes{0};
    uint64_t system_cpu_usage_percent{0};
    uint64_t open_file_descriptors{0};
    uint64_t http_requests_total{0};
    uint64_t http_requests_failed{0};
    uint64_t http_response_time_us{0};

    double getRegistryCacheHitRate() const {
        return total_registry_lookups > 0 ? static_cast<double>(registry_cache_hits) / total_registry_lookups : 0.0;
    }

    double getHttpSuccessRate() const {
        return http_requests_total > 0
                   ? static_cast<double>(http_requests_total - http_requests_failed) / http_requests_total
                   : 0.0;
    }

    double getAverageHttpResponseTimeMs() const {
        return http_requests_total > 0 ? static_cast<double>(http_response_time_us) / http_requests_total / 1000.0
                                       : 0.0;
    }
};

// System-wide monitoring metrics
struct SystemMetrics {
    std::atomic<uint64_t> active_function_calls{0};
    std::atomic<uint64_t> queued_function_calls{0};
    std::atomic<uint64_t> total_registry_lookups{0};
    std::atomic<uint64_t> registry_cache_hits{0};
    std::atomic<uint64_t> concurrent_execution_peak{0};

    // Resource monitoring
    std::atomic<uint64_t> system_memory_bytes{0};
    std::atomic<uint64_t> system_cpu_usage_percent{0};
    std::atomic<uint64_t> open_file_descriptors{0};

    // Network/HTTP metrics
    std::atomic<uint64_t> http_requests_total{0};
    std::atomic<uint64_t> http_requests_failed{0};
    std::atomic<uint64_t> http_response_time_us{0};

    double getRegistryCacheHitRate() const {
        uint64_t total = total_registry_lookups.load();
        return total > 0 ? static_cast<double>(registry_cache_hits.load()) / total : 0.0;
    }

    double getHttpSuccessRate() const {
        uint64_t total = http_requests_total.load();
        return total > 0 ? static_cast<double>(total - http_requests_failed.load()) / total : 0.0;
    }

    double getAverageHttpResponseTimeMs() const {
        uint64_t requests = http_requests_total.load();
        return requests > 0 ? static_cast<double>(http_response_time_us.load()) / requests / 1000.0 : 0.0;
    }
};

// Alert thresholds configuration
struct AlertThresholds {
    double max_failure_rate = 0.05;        // 5% failure rate threshold
    double max_response_time_ms = 1000.0;  // 1 second response time
    double min_cache_hit_rate = 0.80;      // 80% cache hit rate minimum
    double max_memory_usage_mb = 1000.0;   // 1GB memory usage per function
    uint64_t max_concurrent_calls = 1000;  // Maximum concurrent function calls
    uint64_t max_queue_size = 5000;        // Maximum queue size
};

// Alert types
enum class AlertType {
    HIGH_FAILURE_RATE,
    SLOW_RESPONSE_TIME,
    LOW_CACHE_HIT_RATE,
    HIGH_MEMORY_USAGE,
    HIGH_CONCURRENCY,
    QUEUE_OVERLOAD,
    SYSTEM_RESOURCE_EXHAUSTION
};

// Alert information
struct Alert {
    AlertType type;
    std::string function_name;
    std::string message;
    std::chrono::steady_clock::time_point timestamp;
    double severity;  // 0.0 to 1.0
    std::map<std::string, double> metrics;
};

// Production monitoring manager
//
// All access (reads and writes) occurs on a single Seastar reactor thread
// because getInstance() returns a thread_local instance. No mutexes are
// needed. Periodic metric collection uses seastar::timer instead of a
// background std::thread, so the callback executes directly on the reactor.
class ProductionMonitor {
private:
    std::map<std::string, std::shared_ptr<FunctionMetrics>> function_metrics_;
    std::unique_ptr<SystemMetrics> system_metrics_;
    AlertThresholds thresholds_;

    static constexpr size_t MAX_ALERT_HISTORY = 1000;

    std::vector<Alert> active_alerts_;
    // O(1) duplicate detection for active alerts keyed by (type, function_name).
    struct AlertKey {
        AlertType type;
        std::string function_name;
        bool operator==(const AlertKey& o) const { return type == o.type && function_name == o.function_name; }
    };
    struct AlertKeyHash {
        size_t operator()(const AlertKey& k) const {
            size_t h1 = std::hash<int>{}(static_cast<int>(k.type));
            size_t h2 = std::hash<std::string>{}(k.function_name);
            return h1 ^ (h2 << 1);
        }
    };
    std::unordered_set<AlertKey, AlertKeyHash> active_alert_keys_;
    std::deque<Alert> alert_history_;

    // Seastar timer for periodic metric collection (replaces std::thread).
    // Fires on the reactor thread -- no cross-thread synchronization needed.
    seastar::timer<seastar::lowres_clock> monitoring_timer_;
    bool monitoring_active_{false};

    // Metrics collection interval
    std::chrono::seconds collection_interval_{30};  // 30 seconds

    // System resource monitoring
    void collectSystemMetrics();
    void checkAlertConditions();
    void processAlert(const Alert& alert);
    void cleanupOldAlerts();

    // Timer callback: runs one collection cycle.
    void onTimerTick();

public:
    ProductionMonitor();
    ~ProductionMonitor();

    // Singleton access
    static ProductionMonitor& getInstance();

    // Lifecycle management
    void start();
    void stop();
    bool isRunning() const { return monitoring_active_; }

    // Metrics registration and access
    void registerFunction(const std::string& function_name);
    std::shared_ptr<FunctionMetrics> getFunctionMetrics(const std::string& function_name);
    SystemMetrics* getSystemMetrics() { return system_metrics_.get(); }

    // Alert management
    void setAlertThresholds(const AlertThresholds& thresholds);
    std::vector<Alert> getActiveAlerts() const;
    std::vector<Alert> getAlertHistory(std::chrono::minutes lookback = std::chrono::minutes(60)) const;
    void acknowledgeAlert(size_t alert_id);
    void clearAlertsForTesting();                 // Clear all alerts and history for testing
    void addAlertForTesting(const Alert& alert);  // Directly inject an alert for testing
    size_t getAlertHistorySize() const;           // Return raw alert_history_ size for testing

    // Run one collection cycle synchronously (for unit tests without a reactor).
    void runCollectionCycle();

    // Metrics reporting
    std::map<std::string, FunctionMetricsSnapshot> getAllFunctionMetrics() const;
    SystemMetricsSnapshot getSystemMetricsSnapshot() const;

    // JSON export for external monitoring systems
    std::string exportMetricsAsJson() const;
    std::string exportAlertsAsJson() const;

    // Health check endpoint data
    struct HealthStatus {
        bool healthy;
        std::string status;
        std::map<std::string, double> key_metrics;
        std::vector<std::string> active_alerts;
    };
    HealthStatus getHealthStatus() const;

    // Configuration
    void setCollectionInterval(std::chrono::seconds interval);
    void enableDetailedLogging(bool enabled);

    // Resource measurement helpers (needed by FunctionExecutionTracker)
    uint64_t getCurrentMemoryUsage();
    uint64_t getCpuUsagePercent();
    uint64_t getOpenFileDescriptors();

private:
    bool detailed_logging_{false};
};

// RAII helper for automatic metric collection
class FunctionExecutionTracker {
private:
    std::string function_name_;
    std::chrono::steady_clock::time_point start_time_;
    std::shared_ptr<FunctionMetrics> metrics_;
    ProductionMonitor* monitor_;
    uint64_t initial_memory_;

public:
    FunctionExecutionTracker(const std::string& function_name);
    ~FunctionExecutionTracker();

    void recordSuccess();
    void recordFailure();
    void recordCacheHit();
    void recordCacheMiss();
    void recordParameterValidationError();
    void recordTimeoutError();
    void recordInsufficientDataError();

    // Disable copy/move
    FunctionExecutionTracker(const FunctionExecutionTracker&) = delete;
    FunctionExecutionTracker& operator=(const FunctionExecutionTracker&) = delete;
};

// Utility macros for easy integration
#define FUNCTION_MONITOR_START(function_name) \
    timestar::functions::FunctionExecutionTracker ts_fn_tracker_(function_name)

#define FUNCTION_MONITOR_SUCCESS() ts_fn_tracker_.recordSuccess()

#define FUNCTION_MONITOR_FAILURE() ts_fn_tracker_.recordFailure()

#define FUNCTION_MONITOR_CACHE_HIT() ts_fn_tracker_.recordCacheHit()

#define FUNCTION_MONITOR_CACHE_MISS() ts_fn_tracker_.recordCacheMiss()

}  // namespace timestar::functions
