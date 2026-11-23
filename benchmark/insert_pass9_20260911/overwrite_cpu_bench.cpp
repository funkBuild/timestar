#include "memory_store.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <limits>
#include <stdexcept>

using Clock = std::chrono::steady_clock;
int main() {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    for (size_t n : {4096, 16384, 65536, 1048576}) {
        for (std::string shape : {"correction", "duplicate", "extrema", "nan", "high_offset", "append"}) {
            std::vector<double> timings;
            double sumError = 0, m2Error = 0;
            size_t capacityBytes = 0;
            for (int rep = 0; rep < 7; ++rep) {
                InMemorySeries<double> series;
                TimeStarInsert<double> initial("m", "v");
                const double offset = shape == "high_offset" ? 1e12 : 0;
                for (size_t i = 0; i < n; ++i)
                    initial.addValue(i * 2, offset + static_cast<double>(i % 1000));
                series.insert(std::move(initial));
                std::vector<TimeStarInsert<double>> requests;
                for (size_t round = 0; round < 50; ++round) {
                    TimeStarInsert<double> request("m", "v");
                    for (size_t j = 0; j < 128; ++j) {
                        size_t point = (j * 7919 + round * 103) % n;
                        // Interior values exercise the fast path without replacing extrema.
                        if (shape == "correction" || shape == "duplicate")
                            point = (point / 1000) * 1000 + 1 + point % 998;
                        point = std::min(point, n - 1);
                        if (shape == "extrema" && j == 0) point = 0;
                        const uint64_t ts = shape == "append" ? (n + round * 128 + j) * 2 : point * 2;
                        double value = offset + 100 + j + .25;
                        if (shape == "extrema" && j == 0) value = round % 2 == 0 ? -1.0 : 100.0;
                        if (shape == "nan" && j == 0) value = std::numeric_limits<double>::quiet_NaN();
                        request.addValue(ts, value);
                        if (shape == "duplicate") request.addValue(ts, value + .5);
                    }
                    requests.push_back(std::move(request));
                }
                auto start = Clock::now();
                for (auto& request : requests) series.insert(std::move(request));
                double ms = std::chrono::duration<double, std::milli>(Clock::now()-start).count();
                if (rep) timings.push_back(ms);
                capacityBytes = std::max(capacityBytes, (series.timestamps.capacity() + series.values.capacity()) * sizeof(uint64_t));
                long double sum = 0, m2 = 0;
                size_t count = 0;
                for (double v : series.values) if (!std::isnan(v)) {sum += v; ++count;}
                long double mean = sum / count;
                for (double v : series.values) if (!std::isnan(v)) m2 += (v-mean)*(v-mean);
                sumError = std::max(sumError, std::abs(double(series.stats.sum - sum)) / std::max(1.0, std::abs(double(sum))));
                m2Error = std::max(m2Error, std::abs(double(series.stats.m2 - m2)) / std::max(1.0, std::abs(double(m2))));
                if (shape == "high_offset") {
                    // This input exposes pre-existing SIMD summation error. The
                    // candidate must use the identical fresh-summary fallback;
                    // report its error against the independent long-double oracle.
                    InMemorySeriesStats fresh;
                    fresh.update(series.values.data(), series.timestamps.data(), series.values.size(), true);
                    if (series.stats.count != count || series.stats.sum != fresh.sum || series.stats.m2 != fresh.m2)
                        throw std::runtime_error("high-offset fallback changed");
                } else if (series.stats.count != count || std::abs(series.stats.sum - static_cast<double>(sum)) > std::max(1e-6, std::abs(static_cast<double>(sum))*1e-14) ||
                    std::abs(series.stats.m2 - static_cast<double>(m2)) > std::max(1e-6, static_cast<double>(m2)*1e-8))
                {
                    std::fprintf(stderr, "n=%zu shape=%s count=%zu/%zu sum=%.17g expected=%.17g m2=%.17g expected=%.17g\n", n, shape.c_str(), size_t(series.stats.count), count, series.stats.sum, double(sum), series.stats.m2, double(m2));
                    throw std::runtime_error("stats mismatch: " + shape);
                }
            }
            std::sort(timings.begin(),timings.end());
            std::printf("%s n=%zu median_ms=%.6f min_ms=%.6f max_ms=%.6f sum_relative_error=%.9g m2_relative_error=%.9g series_object_bytes=%zu stats_object_bytes=%zu point_capacity_bytes=%zu\n",shape.c_str(),n,(timings[2]+timings[3])/2,timings.front(),timings.back(),sumError,m2Error,sizeof(InMemorySeries<double>),sizeof(InMemorySeriesStats),capacityBytes);
        }
    }
}
