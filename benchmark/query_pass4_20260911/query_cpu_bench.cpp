#include "aggregator.hpp"
#include "response_formatter.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/util/later.hh>
#include <stdexcept>

using namespace timestar;
using Clock = std::chrono::steady_clock;

seastar::future<int> benchmark() {
    constexpr size_t n = 1'000'000;
    for (const auto method : {AggregationMethod::AVG, AggregationMethod::STDDEV}) {
        std::vector<double> samples;
        for (int rep = 0; rep < 7; ++rep) {
            std::vector<PartialAggregationResult> partials;
            for (size_t k = 0; k < 64; ++k) {
                PartialAggregationResult p;
                p.measurement = "cpu";
                p.fieldName = "v";
                p.groupKey = "cpu.v";
                p.groupKeyHash = std::hash<std::string>{}(p.groupKey);
                for (size_t i = 0; i < n / 64; ++i) {
                    p.sortedTimestamps.push_back(i * 64 + k);
                    p.sortedValues.push_back(static_cast<double>(i % 1000) + k * .25);
                }
                p.totalPoints = p.sortedValues.size();
                partials.push_back(std::move(p));
            }
            const auto start = Clock::now();
#ifdef BENCH_COMPACT_OUTPUT
            auto groups = co_await Aggregator::mergePartialAggregationsGrouped(partials, method, true);
#else
            auto groups = co_await Aggregator::mergePartialAggregationsGrouped(partials, method);
#endif
            // Include the conversion the HTTP finalizer performs when arrays
            // are unavailable. Both binaries deliver the same final shape.
            auto& g = groups.at(0);
            if (g.rawTimestamps.empty()) {
                g.rawTimestamps.reserve(g.points.size());
                g.rawValues.reserve(g.points.size());
                for (const auto& p : g.points) {
                    g.rawTimestamps.push_back(p.timestamp);
                    g.rawValues.push_back(p.value);
                }
            }
            double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
            if (g.rawTimestamps.size() != n || g.rawValues.size() != n || g.rawTimestamps.back() != n - 1)
                throw std::runtime_error("merge output shape mismatch");
            for (size_t j : {size_t{0}, n / 2, n - 1}) {
                double expected = method == AggregationMethod::STDDEV ? 0.0 : (j / 64) % 1000 + (j % 64) * .25;
                if (g.rawValues[j] != expected)
                    throw std::runtime_error("merge value mismatch");
            }
            if (rep)
                samples.push_back(ms);
            co_await seastar::yield();
        }
        std::sort(samples.begin(), samples.end());
        std::printf("merge_%s median_ms=%.6f min_ms=%.6f max_ms=%.6f\n",
                    method == AggregationMethod::AVG ? "avg" : "stddev", (samples[2] + samples[3]) / 2, samples.front(),
                    samples.back());
    }
    http::QueryResponse response;
    response.success = true;
    response.statistics.pointCount = n;
    response.statistics.seriesCount = 1;
    http::SeriesResult series;
    series.measurement = "cpu";
    std::vector<uint64_t> ts;
    std::vector<double> vals;
    for (size_t i = 0; i < n; ++i) {
        ts.push_back(1'700'000'000'000'000'000ULL + i * 1'000'000'000ULL);
        vals.push_back((i % 1000) * .125);
    }
    series.fields["v"] = {std::move(ts), FieldValues(std::move(vals))};
    response.series.push_back(std::move(series));
    std::vector<double> samples;
    size_t bytes = 0;
    uint64_t hash = 0;
    for (int rep = 0; rep < 7; ++rep) {
        const auto start = Clock::now();
        auto json = co_await ResponseFormatter::format(response);
        double ms = std::chrono::duration<double, std::milli>(Clock::now() - start).count();
        if (rep)
            samples.push_back(ms);
        bytes = json.size();
        // Hash outside timing: byte identity also covers the serializer output.
        hash = 14695981039346656037ULL;
        for (unsigned char c : json) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        co_await seastar::yield();
    }
    std::sort(samples.begin(), samples.end());
    std::printf("json median_ms=%.6f min_ms=%.6f max_ms=%.6f bytes=%zu hash=%llu\n", (samples[2] + samples[3]) / 2,
                samples.front(), samples.back(), bytes, static_cast<unsigned long long>(hash));
    co_return 0;
}
int main(int argc, char** argv) {
    seastar::app_template app;
    return app.run(argc, argv, benchmark);
}
