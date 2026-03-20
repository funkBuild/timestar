#include "engine_metrics.hpp"

#include "engine.hpp"

namespace sm = seastar::metrics;

namespace timestar {

void EngineMetrics::setup(Engine& engine) {
    _metrics.add_group(
        "timestar",
        {
            // Counters
            sm::make_counter("inserts_total", inserts_total, sm::description("Total insert operations")),
            sm::make_counter("insert_points_total", insert_points_total, sm::description("Total data points inserted")),
            sm::make_counter("insert_errors_total", insert_errors_total,
                             sm::description("Total failed insert operations")),
            sm::make_counter("queries_total", queries_total, sm::description("Total query operations")),
            sm::make_counter("query_errors_total", query_errors_total,
                             sm::description("Total failed query operations")),
            sm::make_counter("slow_queries_total", slow_queries_total,
                             sm::description("Total queries exceeding slow_query_threshold_ms")),
            sm::make_counter("deletes_total", deletes_total, sm::description("Total delete operations")),
            sm::make_counter("wal_rollovers_total", wal_rollovers_total, sm::description("Total WAL rollovers to TSM")),
            sm::make_gauge(
                "compactions_total", [&engine] { return static_cast<int64_t>(engine.getCompletedCompactions()); },
                sm::description("Total compaction operations completed")),

            // Gauges (live state via lambda, read at scrape time)
            sm::make_gauge(
                "tsm_file_count", [&engine] { return static_cast<int64_t>(engine.getTSMFileCount()); },
                sm::description("Number of TSM files on this shard")),
            sm::make_gauge(
                "index_series_count",
                [&engine] { return static_cast<int64_t>(engine.getIndex().getSeriesCountSync()); },
                sm::description("Number of indexed series on this shard")),
        });
}

}  // namespace timestar
