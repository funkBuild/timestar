#include "memory_store.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <vector>

using Clock = std::chrono::steady_clock;

// Compile the same driver against the preserved baseline headers/library and
// the optimized headers/library. No reactor or disk I/O is involved.
int main() {
    for (const bool overwrite : {true, false}) {
        std::vector<double> elapsed;
        for (int rep = 0; rep < 7; ++rep) {
            InMemorySeries<double> series;
            TimeStarInsert<double> initial("cpu_bench", "v");
            for (uint64_t i = 0; i < 1'000'000; ++i)
                initial.addValue(i * 2, static_cast<double>(i % 1000));
            series.insert(std::move(initial));
            std::vector<TimeStarInsert<double>> requests;
            for (uint64_t round = 0; round < 50; ++round) {
                TimeStarInsert<double> request("cpu_bench", "v");
                for (uint64_t j = 0; j < 128; ++j) {
                    // Corrections are scattered; backfills are distinct odd
                    // timestamps near the tail of the original series.
                    const uint64_t ts =
                        overwrite ? ((j * 7919 + round * 103) % 1'000'000) * 2 : (990'000 + round * 128 + j) * 2 + 1;
                    request.addValue(ts, static_cast<double>(j) + 0.25);
                }
                requests.push_back(std::move(request));
            }
            const auto start = Clock::now();
            for (auto& request : requests)
                series.insert(std::move(request));
            elapsed.push_back(std::chrono::duration<double, std::milli>(Clock::now() - start).count());
            const size_t expected = overwrite ? 1'000'000 : 1'006'400;
            if (series.timestamps.size() != expected || series.stats.count != expected)
                return 1;
        }
        std::sort(elapsed.begin(), elapsed.end());
        std::printf("%s: median_ms=%.6f min_ms=%.6f max_ms=%.6f (50 requests, 7 runs)\n",
                    overwrite ? "small_overwrites" : "tail_backfills", elapsed[3], elapsed.front(), elapsed.back());
    }
}
