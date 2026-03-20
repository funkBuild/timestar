#pragma once

#include <cstdint>
#include <seastar/core/metrics.hh>
#include <seastar/core/metrics_registration.hh>

// Forward declaration — avoid circular include with engine.hpp
class Engine;

namespace timestar {

// Per-shard metrics counters for Prometheus export.
// Accessed only from the owning shard thread (Seastar shard-per-core model),
// so plain uint64_t is safe — no atomics needed.
struct EngineMetrics {
    // Counters (monotonically increasing)
    uint64_t inserts_total = 0;
    uint64_t insert_points_total = 0;
    uint64_t insert_errors_total = 0;
    uint64_t queries_total = 0;
    uint64_t query_errors_total = 0;
    uint64_t slow_queries_total = 0;
    uint64_t deletes_total = 0;
    uint64_t wal_rollovers_total = 0;

    // Seastar metrics registration (automatically deregisters on destruction)
    seastar::metrics::metric_groups _metrics;

    // Register all metrics with the Seastar metrics framework.
    // Must be called after Engine is fully initialized (inside the reactor).
    void setup(Engine& engine);
};

}  // namespace timestar
