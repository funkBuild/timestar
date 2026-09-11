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
    // Writes shed because the unconverted-store backlog hit its ceiling.
    // Non-zero means ingest is sustainably above what this shard can convert.
    uint64_t inserts_rejected_backlog_total = 0;

    // Series/day memberships restored by the startup day-bitmap repair. Any
    // non-zero value means the previous shutdown was unclean AND its lost
    // membership would otherwise have made those series silently undiscoverable
    // for queries starting in the affected range — the 2026-09-01 incident. A
    // repair that keeps finding work every restart is a signal in itself.
    uint64_t day_bitmap_memberships_repaired_total = 0;

    // Seastar metrics registration (automatically deregisters on destruction)
    seastar::metrics::metric_groups _metrics;

    // Register all metrics with the Seastar metrics framework.
    // Must be called after Engine is fully initialized (inside the reactor).
    void setup(Engine& engine);
};

}  // namespace timestar
