#pragma once

// Shared helpers for the TimeStar benchmark drivers (timestar_insert_bench,
// timestar_query_bench). Pure (no Seastar dependency) so it's cheap to include
// from either tool.

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <numeric>
#include <vector>

namespace timestar::bench {

// Wire format selected by the --format flag.
// Json      — single array-format write point per request (fast path).
// JsonBatch — {"writes":[...]} batch of scalar write points (DOM batch path).
// Protobuf  — serialized WriteRequest proto.
enum class WireFormat { Json, JsonBatch, Protobuf };

// Monotonic clock used for all latency timing.
using clk = std::chrono::steady_clock;

// Nearest-rank percentile of an already-sorted ascending sample vector.
// p in [0,1]. Returns 0 for an empty sample.
inline double percentile(const std::vector<double>& sortedAsc, double p) {
    if (sortedAsc.empty())
        return 0.0;
    size_t idx = static_cast<size_t>(p * (sortedAsc.size() - 1));
    return sortedAsc[idx];
}

// Latency sample accumulator with merge + min/avg/p50/p95/p99/max reporting.
struct LatencyStats {
    std::vector<double> samples_ms;

    void add(clk::duration d) { samples_ms.push_back(std::chrono::duration<double, std::milli>(d).count()); }

    void merge(LatencyStats&& other) {
        samples_ms.insert(samples_ms.end(), std::make_move_iterator(other.samples_ms.begin()),
                          std::make_move_iterator(other.samples_ms.end()));
    }

    void print(const char* label) const {
        if (samples_ms.empty())
            return;
        auto sorted = samples_ms;  // copy for sorting
        std::sort(sorted.begin(), sorted.end());
        double sum = std::accumulate(sorted.begin(), sorted.end(), 0.0);
        fmt::print(
            "  {:<22s}  min={:>8.2f}  avg={:>8.2f}  med={:>8.2f}"
            "  p95={:>8.2f}  p99={:>8.2f}  max={:>8.2f}  (ms, n={})\n",
            label, sorted.front(), sum / sorted.size(), percentile(sorted, 0.50), percentile(sorted, 0.95),
            percentile(sorted, 0.99), sorted.back(), sorted.size());
    }
};

}  // namespace timestar::bench
