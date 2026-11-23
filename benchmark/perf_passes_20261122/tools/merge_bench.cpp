#include "query/aggregator.hpp"
#include <chrono>
#include <cstdio>
#include <algorithm>
#include <vector>
#include <string>
using namespace timestar;
using Clock = std::chrono::steady_clock;
static std::vector<PartialAggregationResult> make(const std::string& layout, size_t K, size_t per, bool states) {
    std::vector<PartialAggregationResult> ps(K);
    const size_t CH = 10000;
    for (size_t h = 0; h < K; ++h) {
        auto& p = ps[h]; p.measurement = "m"; p.fieldName = "v"; p.groupKey = "g"; p.groupKeyHash = 1;
        for (size_t i = 0; i < per; ++i) {
            uint64_t t;
            if (layout == "blocky") t = ((i / CH) * K + h) * CH * 60 + (i % CH) * 60;
            else if (layout == "interleaved") t = i * 60 + h;
            else if (layout == "pairs") t = (i / 2) * 120 + (i % 2) * 7 + h * 14;   // runs of 2
            else if (layout == "short") t = ((i / 8) * K + h) * 8 * 60 + (i % 8) * 60;  // runs of 8
            else t = i * 60 + (h % 2);  // halfaligned: ties between half the streams
            double v = double((i * 31 + h * 7) % 1000) * 0.25;
            p.sortedTimestamps.push_back(t);
            if (states) { AggregationState s; s.addValue(v, t); p.sortedStates.push_back(std::move(s)); } else p.sortedValues.push_back(v);
        }
        p.totalPoints = per;
    }
    return ps;
}
int main(int argc, char** argv) {
    int reps = argc > 1 ? atoi(argv[1]) : 7;
    // Optional filters: argv[2] = layout, argv[3] = method name (substring).
    const std::string onlyLayout = argc > 2 ? argv[2] : "";
    const std::string onlyMethod = argc > 3 ? argv[3] : "";
    for (const char* layout : {"blocky", "short", "pairs", "interleaved", "halfaligned"})
        for (auto [name, method, states] : {std::tuple{"avg", AggregationMethod::AVG, false}, std::tuple{"latest", AggregationMethod::LATEST, false}, std::tuple{"stddev(raw)", AggregationMethod::STDDEV, false}, std::tuple{"stddev(states)", AggregationMethod::STDDEV, true}}) {
            if ((!onlyLayout.empty() && onlyLayout != layout) || (!onlyMethod.empty() && std::string(name).find(onlyMethod) == std::string::npos)) continue;
            auto base = make(layout, 10, 100000, states);
            std::vector<double> times; double check = 0; size_t n = 0;
            for (int r = 0; r < reps + 1; ++r) {
                auto in = base;
                auto t0 = Clock::now();
                auto g = Aggregator::mergePartialAggregationsGrouped(in, method, true).get();
                auto t1 = Clock::now();
                if (r) times.push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
                n = g[0].rawTimestamps.size(); check = 0; for (size_t i = 0; i < n; i += 97) check += g[0].rawValues[i] + double(g[0].rawTimestamps[i] % 1000);
            }
            std::sort(times.begin(), times.end());
            printf("%-12s %-15s out=%zu ms=%8.3f check=%.6f\n", layout, name, n, times[times.size() / 2], check);
        }
}
