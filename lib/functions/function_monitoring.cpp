#include "function_monitoring.hpp"

#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace timestar::functions {

ProductionMonitor::ProductionMonitor() : system_metrics_(std::make_unique<SystemMetrics>()) {}

ProductionMonitor::~ProductionMonitor() {
    stop();
}

ProductionMonitor& ProductionMonitor::getInstance() {
    // Per-shard instance: each Seastar shard gets its own monitor,
    // eliminating cross-shard mutex contention on the reactor thread.
    static thread_local ProductionMonitor instance;
    return instance;
}

void ProductionMonitor::start() {
    // Serialize with stop() via lifecycle_mutex_.  This prevents the scenario
    // where stop() sees monitoring_active_==true and clears the flag while
    // start() has already passed the CAS but has not yet launched the thread,
    // which would leave a zombie std::thread object in monitoring_thread_.
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);

    // Use compare_exchange_strong so that only one thread wins when start() is
    // called concurrently.  If monitoring_active_ is already true the exchange
    // fails and we return immediately without touching monitoring_thread_.
    bool expected = false;
    if (!monitoring_active_.compare_exchange_strong(expected, true)) {
        return;  // Already running (or another thread just won the race)
    }

    monitoring_thread_ = std::thread(&ProductionMonitor::monitoringLoop, this);

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Started background monitoring thread\n";
    }
}

void ProductionMonitor::stop() {
    // Serialize start()/stop() lifecycle transitions so that only one thread
    // performs the join.  Without this:
    //  - Two concurrent stop() calls can both see monitoring_active_==true,
    //    both set it to false, and then both try to join the same std::thread
    //    (undefined behaviour, may deadlock).
    //  - A concurrent start() can create the thread after stop() has already
    //    cleared monitoring_active_, leaving an unjoinable thread.
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);

    if (!monitoring_active_.load()) {
        return;  // Not running
    }

    monitoring_active_.store(false);
    monitoring_cv_.notify_all();

    if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
    }

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Stopped background monitoring thread\n";
    }
}

void ProductionMonitor::registerFunction(const std::string& function_name) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    if (function_metrics_.find(function_name) == function_metrics_.end()) {
        function_metrics_[function_name] = std::make_shared<FunctionMetrics>();

        if (detailed_logging_) {
            std::cout << "[ProductionMonitor] Registered function: " << function_name << "\n";
        }
    }
}

std::shared_ptr<FunctionMetrics> ProductionMonitor::getFunctionMetrics(const std::string& function_name) {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    auto it = function_metrics_.find(function_name);
    if (it != function_metrics_.end()) {
        return it->second;
    }

    // Auto-register if not found
    auto metrics = std::make_shared<FunctionMetrics>();
    function_metrics_[function_name] = metrics;

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Auto-registered function: " << function_name << "\n";
    }

    return metrics;
}

void ProductionMonitor::setAlertThresholds(const AlertThresholds& thresholds) {
    // thresholds_ is read by checkAlertConditions() (under metrics_mutex_) and
    // getHealthStatus(), both of which may run concurrently on other threads.
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    thresholds_ = thresholds;

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Updated alert thresholds\n";
    }
}

std::vector<Alert> ProductionMonitor::getActiveAlerts() const {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    return active_alerts_;
}

std::vector<Alert> ProductionMonitor::getAlertHistory(std::chrono::minutes lookback) const {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    std::vector<Alert> recent_alerts;

    auto cutoff_time = std::chrono::steady_clock::now() - lookback;

    for (const auto& alert : alert_history_) {
        if (alert.timestamp >= cutoff_time) {
            recent_alerts.push_back(alert);
        }
    }

    return recent_alerts;
}

void ProductionMonitor::acknowledgeAlert(size_t alert_id) {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    if (alert_id < active_alerts_.size()) {
        // Remove key from the lookup set before erasing from vector
        const auto& alert = active_alerts_[alert_id];
        active_alert_keys_.erase(AlertKey{alert.type, alert.function_name});
        active_alerts_.erase(active_alerts_.begin() + alert_id);

        if (detailed_logging_) {
            std::cout << "[ProductionMonitor] Acknowledged alert ID: " << alert_id << "\n";
        }
    }
}

void ProductionMonitor::clearAlertsForTesting() {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    active_alerts_.clear();
    active_alert_keys_.clear();
    alert_history_.clear();

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Cleared all alerts and history for testing\n";
    }
}

void ProductionMonitor::addAlertForTesting(const Alert& alert) {
    processAlert(alert);
}

size_t ProductionMonitor::getAlertHistorySize() const {
    std::lock_guard<std::mutex> lock(alerts_mutex_);
    return alert_history_.size();
}

std::map<std::string, FunctionMetricsSnapshot> ProductionMonitor::getAllFunctionMetrics() const {
    std::lock_guard<std::mutex> lock(metrics_mutex_);
    std::map<std::string, FunctionMetricsSnapshot> snapshot;

    for (const auto& [name, metrics] : function_metrics_) {
        if (metrics) {
            FunctionMetricsSnapshot copy;

            // Copy atomic values to regular values
            copy.total_executions = metrics->total_executions.load();
            copy.successful_executions = metrics->successful_executions.load();
            copy.failed_executions = metrics->failed_executions.load();
            copy.total_execution_time_us = metrics->total_execution_time_us.load();
            copy.peak_memory_bytes = metrics->peak_memory_bytes.load();
            copy.total_memory_allocated = metrics->total_memory_allocated.load();
            copy.total_cpu_time_us = metrics->total_cpu_time_us.load();
            copy.cache_hits = metrics->cache_hits.load();
            copy.cache_misses = metrics->cache_misses.load();
            copy.cache_evictions = metrics->cache_evictions.load();
            copy.timeout_errors = metrics->timeout_errors.load();
            copy.parameter_validation_errors = metrics->parameter_validation_errors.load();
            copy.insufficient_data_errors = metrics->insufficient_data_errors.load();

            snapshot[name] = copy;
        }
    }

    return snapshot;
}

SystemMetricsSnapshot ProductionMonitor::getSystemMetricsSnapshot() const {
    SystemMetricsSnapshot copy;

    // Copy atomic values to regular values
    copy.active_function_calls = system_metrics_->active_function_calls.load();
    copy.queued_function_calls = system_metrics_->queued_function_calls.load();
    copy.total_registry_lookups = system_metrics_->total_registry_lookups.load();
    copy.registry_cache_hits = system_metrics_->registry_cache_hits.load();
    copy.concurrent_execution_peak = system_metrics_->concurrent_execution_peak.load();
    copy.system_memory_bytes = system_metrics_->system_memory_bytes.load();
    copy.system_cpu_usage_percent = system_metrics_->system_cpu_usage_percent.load();
    copy.open_file_descriptors = system_metrics_->open_file_descriptors.load();
    copy.http_requests_total = system_metrics_->http_requests_total.load();
    copy.http_requests_failed = system_metrics_->http_requests_failed.load();
    copy.http_response_time_us = system_metrics_->http_response_time_us.load();

    return copy;
}

std::string ProductionMonitor::exportMetricsAsJson() const {
    std::ostringstream json;
    json << "{\n";
    json
        << "  \"timestamp\": "
        << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()
        << ",\n";

    // Function metrics
    json << "  \"functions\": {\n";
    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        bool first = true;
        for (const auto& [name, metrics] : function_metrics_) {
            if (!first)
                json << ",\n";
            first = false;

            json << "    \"" << name << "\": {\n";
            json << "      \"total_executions\": " << metrics->total_executions.load() << ",\n";
            json << "      \"successful_executions\": " << metrics->successful_executions.load() << ",\n";
            json << "      \"failed_executions\": " << metrics->failed_executions.load() << ",\n";
            json << "      \"success_rate\": " << std::fixed << std::setprecision(4) << metrics->getSuccessRate()
                 << ",\n";
            json << "      \"failure_rate\": " << std::fixed << std::setprecision(4) << metrics->getFailureRate()
                 << ",\n";
            json << "      \"avg_execution_time_ms\": " << std::fixed << std::setprecision(2)
                 << metrics->getAverageExecutionTimeMs() << ",\n";
            json << "      \"cache_hit_rate\": " << std::fixed << std::setprecision(4) << metrics->getCacheHitRate()
                 << ",\n";
            json << "      \"avg_memory_usage_mb\": " << std::fixed << std::setprecision(2)
                 << metrics->getAverageMemoryUsageMB() << ",\n";
            json << "      \"timeout_errors\": " << metrics->timeout_errors.load() << ",\n";
            json << "      \"parameter_validation_errors\": " << metrics->parameter_validation_errors.load() << ",\n";
            json << "      \"insufficient_data_errors\": " << metrics->insufficient_data_errors.load() << "\n";
            json << "    }";
        }
    }
    json << "\n  },\n";

    // System metrics
    json << "  \"system\": {\n";
    json << "    \"active_function_calls\": " << system_metrics_->active_function_calls.load() << ",\n";
    json << "    \"queued_function_calls\": " << system_metrics_->queued_function_calls.load() << ",\n";
    json << "    \"concurrent_execution_peak\": " << system_metrics_->concurrent_execution_peak.load() << ",\n";
    json << "    \"registry_cache_hit_rate\": " << std::fixed << std::setprecision(4)
         << system_metrics_->getRegistryCacheHitRate() << ",\n";
    json << "    \"http_success_rate\": " << std::fixed << std::setprecision(4) << system_metrics_->getHttpSuccessRate()
         << ",\n";
    json << "    \"avg_http_response_time_ms\": " << std::fixed << std::setprecision(2)
         << system_metrics_->getAverageHttpResponseTimeMs() << ",\n";
    json << "    \"system_memory_bytes\": " << system_metrics_->system_memory_bytes.load() << ",\n";
    json << "    \"system_cpu_usage_percent\": " << system_metrics_->system_cpu_usage_percent.load() << ",\n";
    json << "    \"open_file_descriptors\": " << system_metrics_->open_file_descriptors.load() << "\n";
    json << "  }\n";

    json << "}";
    return json.str();
}

std::string ProductionMonitor::exportAlertsAsJson() const {
    std::ostringstream json;
    json << "{\n";
    json
        << "  \"timestamp\": "
        << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count()
        << ",\n";
    json << "  \"active_alerts\": [\n";

    {
        std::lock_guard<std::mutex> lock(alerts_mutex_);
        for (size_t i = 0; i < active_alerts_.size(); ++i) {
            const auto& alert = active_alerts_[i];
            if (i > 0)
                json << ",\n";

            json << "    {\n";
            json << "      \"id\": " << i << ",\n";
            json << "      \"type\": " << static_cast<int>(alert.type) << ",\n";
            json << "      \"function_name\": \"" << alert.function_name << "\",\n";
            json << "      \"message\": \"" << alert.message << "\",\n";
            json << "      \"severity\": " << std::fixed << std::setprecision(2) << alert.severity << ",\n";
            json << "      \"timestamp\": "
                 << std::chrono::duration_cast<std::chrono::seconds>(alert.timestamp.time_since_epoch()).count()
                 << "\n";
            json << "    }";
        }
    }

    json << "\n  ]\n";
    json << "}";
    return json.str();
}

ProductionMonitor::HealthStatus ProductionMonitor::getHealthStatus() const {
    HealthStatus status;
    status.healthy = true;
    status.status = "OK";

    // Check active alerts
    {
        std::lock_guard<std::mutex> lock(alerts_mutex_);
        for (const auto& alert : active_alerts_) {
            status.active_alerts.push_back(alert.message);
            if (alert.severity > 0.7) {  // High severity alerts affect health
                status.healthy = false;
                status.status = "DEGRADED";
            }
        }
    }

    // Key metrics
    status.key_metrics["system_memory_mb"] = system_metrics_->system_memory_bytes.load() / 1024.0 / 1024.0;
    status.key_metrics["active_calls"] = system_metrics_->active_function_calls.load();
    status.key_metrics["queued_calls"] = system_metrics_->queued_function_calls.load();
    status.key_metrics["http_success_rate"] = system_metrics_->getHttpSuccessRate();

    // Check critical thresholds (thresholds_ guarded by metrics_mutex_)
    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);
        if (system_metrics_->queued_function_calls.load() > thresholds_.max_queue_size) {
            status.healthy = false;
            status.status = "CRITICAL";
        }

        if (system_metrics_->active_function_calls.load() > thresholds_.max_concurrent_calls) {
            status.healthy = false;
            status.status = "OVERLOADED";
        }
    }

    return status;
}

void ProductionMonitor::setCollectionInterval(std::chrono::seconds interval) {
    // collection_interval_ is read by monitoringLoop() under monitoring_mutex_.
    // Wake the loop so it picks up the new interval immediately.
    {
        std::lock_guard<std::mutex> lock(monitoring_mutex_);
        collection_interval_ = interval;
    }
    monitoring_cv_.notify_one();

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] Updated collection interval to " << interval.count() << "s\n";
    }
}

void ProductionMonitor::enableDetailedLogging(bool enabled) {
    detailed_logging_ = enabled;

    if (enabled) {
        std::cout << "[ProductionMonitor] Detailed logging enabled\n";
    }
}

void ProductionMonitor::monitoringLoop() {
    while (monitoring_active_.load()) {
        try {
            // Collect system metrics
            collectSystemMetrics();

            // Check alert conditions
            checkAlertConditions();

            // Cleanup old alerts
            cleanupOldAlerts();

            if (detailed_logging_) {
                auto system_snapshot = getSystemMetricsSnapshot();
                std::cout << "[ProductionMonitor] Active calls: " << system_snapshot.active_function_calls
                          << ", Queued: " << system_snapshot.queued_function_calls
                          << ", Memory: " << system_snapshot.system_memory_bytes / 1024 / 1024 << "MB\n";
            }

        } catch (const std::exception& e) {
            if (detailed_logging_) {
                std::cout << "[ProductionMonitor] Error in monitoring loop: " << e.what() << "\n";
            }
        }

        // Wait for next collection interval or shutdown signal
        std::unique_lock<std::mutex> lock(monitoring_mutex_);
        monitoring_cv_.wait_for(lock, collection_interval_, [this] { return !monitoring_active_.load(); });
    }
}

void ProductionMonitor::collectSystemMetrics() {
    // Update system memory
    system_metrics_->system_memory_bytes.store(getCurrentMemoryUsage());

    // Update CPU usage
    system_metrics_->system_cpu_usage_percent.store(getCpuUsagePercent());

    // Update file descriptors
    system_metrics_->open_file_descriptors.store(getOpenFileDescriptors());

    // Update concurrent execution peak (CAS loop to avoid TOCTOU race)
    uint64_t current_active = system_metrics_->active_function_calls.load();
    uint64_t current_peak = system_metrics_->concurrent_execution_peak.load();
    while (current_active > current_peak) {
        if (system_metrics_->concurrent_execution_peak.compare_exchange_weak(current_peak, current_active)) {
            break;
        }
    }
}

void ProductionMonitor::checkAlertConditions() {
    // Collect alerts while holding metrics_mutex_, then release it before
    // calling processAlert (which acquires alerts_mutex_). This avoids
    // nested lock acquisition and eliminates any deadlock risk.
    std::vector<Alert> pendingAlerts;

    {
        std::lock_guard<std::mutex> lock(metrics_mutex_);

        for (const auto& [function_name, metrics] : function_metrics_) {
            if (!metrics)
                continue;

            // Check failure rate
            double failure_rate = metrics->getFailureRate();
            if (failure_rate > thresholds_.max_failure_rate && metrics->total_executions.load() > 10) {
                Alert alert;
                alert.type = AlertType::HIGH_FAILURE_RATE;
                alert.function_name = function_name;
                alert.message = "High failure rate: " + std::to_string(failure_rate * 100) + "%";
                alert.timestamp = std::chrono::steady_clock::now();
                alert.severity = std::min(1.0, failure_rate / thresholds_.max_failure_rate);
                alert.metrics["failure_rate"] = failure_rate;
                pendingAlerts.push_back(std::move(alert));
            }

            // Check response time
            double avg_time = metrics->getAverageExecutionTimeMs();
            if (avg_time > thresholds_.max_response_time_ms && metrics->successful_executions.load() > 5) {
                Alert alert;
                alert.type = AlertType::SLOW_RESPONSE_TIME;
                alert.function_name = function_name;
                alert.message = "Slow response time: " + std::to_string(avg_time) + "ms";
                alert.timestamp = std::chrono::steady_clock::now();
                alert.severity = std::min(1.0, avg_time / thresholds_.max_response_time_ms);
                alert.metrics["avg_response_time_ms"] = avg_time;
                pendingAlerts.push_back(std::move(alert));
            }

            // Check cache hit rate
            double cache_rate = metrics->getCacheHitRate();
            uint64_t total_cache_ops = metrics->cache_hits.load() + metrics->cache_misses.load();
            if (cache_rate < thresholds_.min_cache_hit_rate && total_cache_ops > 20) {
                Alert alert;
                alert.type = AlertType::LOW_CACHE_HIT_RATE;
                alert.function_name = function_name;
                alert.message = "Low cache hit rate: " + std::to_string(cache_rate * 100) + "%";
                alert.timestamp = std::chrono::steady_clock::now();
                alert.severity =
                    std::min(1.0, (thresholds_.min_cache_hit_rate - cache_rate) / thresholds_.min_cache_hit_rate);
                alert.metrics["cache_hit_rate"] = cache_rate;
                pendingAlerts.push_back(std::move(alert));
            }

            // Check memory usage
            double memory_mb = metrics->getAverageMemoryUsageMB();
            if (memory_mb > thresholds_.max_memory_usage_mb && metrics->successful_executions.load() > 5) {
                Alert alert;
                alert.type = AlertType::HIGH_MEMORY_USAGE;
                alert.function_name = function_name;
                alert.message = "High memory usage: " + std::to_string(memory_mb) + "MB";
                alert.timestamp = std::chrono::steady_clock::now();
                alert.severity = std::min(1.0, memory_mb / thresholds_.max_memory_usage_mb);
                alert.metrics["memory_usage_mb"] = memory_mb;
                pendingAlerts.push_back(std::move(alert));
            }
        }

        // System-level alerts
        uint64_t active_calls = system_metrics_->active_function_calls.load();
        if (active_calls > thresholds_.max_concurrent_calls) {
            Alert alert;
            alert.type = AlertType::HIGH_CONCURRENCY;
            alert.function_name = "system";
            alert.message = "High concurrency: " + std::to_string(active_calls) + " active calls";
            alert.timestamp = std::chrono::steady_clock::now();
            alert.severity = std::min(1.0, (double)active_calls / thresholds_.max_concurrent_calls);
            alert.metrics["active_calls"] = active_calls;
            pendingAlerts.push_back(std::move(alert));
        }

        uint64_t queued_calls = system_metrics_->queued_function_calls.load();
        if (queued_calls > thresholds_.max_queue_size) {
            Alert alert;
            alert.type = AlertType::QUEUE_OVERLOAD;
            alert.function_name = "system";
            alert.message = "Queue overload: " + std::to_string(queued_calls) + " queued calls";
            alert.timestamp = std::chrono::steady_clock::now();
            alert.severity = std::min(1.0, (double)queued_calls / thresholds_.max_queue_size);
            alert.metrics["queued_calls"] = queued_calls;
            pendingAlerts.push_back(std::move(alert));
        }
    }  // metrics_mutex_ released

    // Now process alerts with only alerts_mutex_ held
    for (const auto& alert : pendingAlerts) {
        processAlert(alert);
    }
}

void ProductionMonitor::processAlert(const Alert& alert) {
    std::lock_guard<std::mutex> lock(alerts_mutex_);

    // O(1) duplicate check using the unordered_set keyed by (type, function_name)
    AlertKey key{alert.type, alert.function_name};
    if (active_alert_keys_.contains(key)) {
        return;  // Similar alert already active, avoid spam
    }

    active_alerts_.push_back(alert);
    active_alert_keys_.insert(std::move(key));

    // Enforce bounded alert history: evict the oldest entry when at capacity.
    if (alert_history_.size() >= MAX_ALERT_HISTORY) {
        alert_history_.pop_front();
    }
    alert_history_.push_back(alert);

    if (detailed_logging_) {
        std::cout << "[ProductionMonitor] ALERT: " << alert.message << " (severity: " << alert.severity << ")\n";
    }
}

void ProductionMonitor::cleanupOldAlerts() {
    std::lock_guard<std::mutex> lock(alerts_mutex_);

    auto cutoff_time = std::chrono::steady_clock::now() - std::chrono::hours(24);

    // Remove old alerts from history
    alert_history_.erase(std::remove_if(alert_history_.begin(), alert_history_.end(),
                                        [cutoff_time](const Alert& alert) { return alert.timestamp < cutoff_time; }),
                         alert_history_.end());

    // Remove old active alerts (auto-acknowledge after 1 hour)
    auto active_cutoff = std::chrono::steady_clock::now() - std::chrono::hours(1);
    auto new_end = std::remove_if(active_alerts_.begin(), active_alerts_.end(),
                                  [active_cutoff](const Alert& alert) { return alert.timestamp < active_cutoff; });
    if (new_end != active_alerts_.end()) {
        active_alerts_.erase(new_end, active_alerts_.end());
        // Rebuild the key set to match the remaining active alerts
        active_alert_keys_.clear();
        for (const auto& alert : active_alerts_) {
            active_alert_keys_.insert(AlertKey{alert.type, alert.function_name});
        }
    }
}

uint64_t ProductionMonitor::getCurrentMemoryUsage() {
    std::ifstream status_file("/proc/self/status");
    std::string line;
    while (std::getline(status_file, line)) {
        if (line.find("VmRSS:") == 0) {
            std::istringstream iss(line);
            std::string label, size_str, unit;
            iss >> label >> size_str >> unit;
            return std::stoull(size_str) * 1024;  // Convert KB to bytes
        }
    }
    return 0;
}

uint64_t ProductionMonitor::getCpuUsagePercent() {
    static thread_local uint64_t last_cpu_time = 0;
    static thread_local auto last_measurement = std::chrono::steady_clock::now();

    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return 0;
    }

    uint64_t current_cpu_time = (usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000ULL +
                                (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);

    auto now = std::chrono::steady_clock::now();
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - last_measurement).count();

    uint64_t cpu_percent = 0;
    if (last_cpu_time > 0 && elapsed_us > 0) {
        uint64_t cpu_delta = current_cpu_time - last_cpu_time;
        cpu_percent = (cpu_delta * 100) / elapsed_us;
    }

    last_cpu_time = current_cpu_time;
    last_measurement = now;

    return cpu_percent;
}

uint64_t ProductionMonitor::getOpenFileDescriptors() {
    uint64_t count = 0;
    try {
        for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator("/proc/self/fd")) {
            ++count;
        }
    } catch (const std::filesystem::filesystem_error&) {
        // /proc/self/fd may not exist on non-Linux platforms
    }
    return count;
}

// FunctionExecutionTracker implementation
FunctionExecutionTracker::FunctionExecutionTracker(const std::string& function_name)
    : function_name_(function_name), start_time_(std::chrono::steady_clock::now()), initial_memory_(0) {
    monitor_ = &ProductionMonitor::getInstance();
    metrics_ = monitor_->getFunctionMetrics(function_name_);
    // Skip blocking /proc read — memory tracking done only in background collection

    // Update active calls
    monitor_->getSystemMetrics()->active_function_calls.fetch_add(1);

    // Record execution attempt
    metrics_->total_executions.fetch_add(1);
}

FunctionExecutionTracker::~FunctionExecutionTracker() {
    auto end_time = std::chrono::steady_clock::now();
    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time_).count();

    // Update execution time
    metrics_->total_execution_time_us.fetch_add(duration_us);

    // Memory tracking removed from hot path — collected periodically by background monitor

    // Update active calls
    monitor_->getSystemMetrics()->active_function_calls.fetch_sub(1);
}

void FunctionExecutionTracker::recordSuccess() {
    metrics_->successful_executions.fetch_add(1);
}

void FunctionExecutionTracker::recordFailure() {
    metrics_->failed_executions.fetch_add(1);
}

void FunctionExecutionTracker::recordCacheHit() {
    metrics_->cache_hits.fetch_add(1);
}

void FunctionExecutionTracker::recordCacheMiss() {
    metrics_->cache_misses.fetch_add(1);
}

void FunctionExecutionTracker::recordParameterValidationError() {
    metrics_->parameter_validation_errors.fetch_add(1);
}

void FunctionExecutionTracker::recordTimeoutError() {
    metrics_->timeout_errors.fetch_add(1);
}

void FunctionExecutionTracker::recordInsufficientDataError() {
    metrics_->insufficient_data_errors.fetch_add(1);
}

}  // namespace timestar::functions