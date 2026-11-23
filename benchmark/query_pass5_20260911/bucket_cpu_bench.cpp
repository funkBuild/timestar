#include "block_aggregator.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <stdexcept>

using namespace timestar;
using Clock = std::chrono::steady_clock;

int main() {
    constexpr size_t n = 262144;
    std::vector<uint64_t> timestamps(n);
    std::vector<double> values(n);
    double expectedSum = 0;
    for (size_t i = 0; i < n; ++i) {
        timestamps[i] = i;
        values[i] = (i % 127) + .25;
        expectedSum += values[i];
    }
    for (size_t width : {1u, 3u, 8u, 9u, 16u, 32u, 300u, 4096u, 262144u}) {
        for (bool countOnly : {false, true}) {
            std::vector<double> samples;
            for (int rep = 0; rep < 7; ++rep) {
                const auto start = Clock::now();
                BlockAggregator agg(width, 0, n - 1, countOnly ? AggregationMethod::COUNT : AggregationMethod::AVG,
                                    true);
                if (countOnly)
                    agg.addTimestampsOnly(timestamps);
                else
                    agg.addPointsRange(timestamps, values, 0, n);
                auto states = agg.takeBucketStates();
                const double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
                size_t count = 0;
                double sum = 0;
                for (const auto& [key, state] : states) {
                    const size_t expectedCount = std::min(width, n - key);
                    if (state.count != expectedCount)
                        throw std::runtime_error("bucket count mismatch");
                    count += state.count;
                    sum += state.sum;
                }
                if (count != n || (!countOnly && sum != expectedSum))
                    throw std::runtime_error("result mismatch");
                if (rep)
                    samples.push_back(ms);
            }
            std::sort(samples.begin(), samples.end());
            std::printf("%s width=%zu points=%zu median_ms=%.6f min_ms=%.6f max_ms=%.6f\n", countOnly ? "count" : "avg",
                        width, n, (samples[2] + samples[3]) / 2, samples.front(), samples.back());
        }
    }
}
