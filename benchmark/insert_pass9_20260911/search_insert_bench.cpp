#include "memory_store.hpp"
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <map>
#include <random>
#include <stdexcept>

int main() {
    std::setvbuf(stdout, nullptr, _IOLBF, 0);
    for (size_t n : {4097, 65537}) {
        for (size_t count : {1, 7, 8, 9, 128}) {
            for (std::string shape : {"random", "sorted", "early_miss", "late_miss"}) {
                std::vector<double> times;
                for (int rep = 0; rep < 7; ++rep) {
                    InMemorySeries<int64_t> series;
                    TimeStarInsert<int64_t> initial("m", "v");
                    for (size_t i = 0; i < n; ++i) initial.addValue(i * 2 + 2, i);
                    series.insert(std::move(initial));
                    std::mt19937 rng(42);
                    std::vector<TimeStarInsert<int64_t>> requests;
                    std::map<uint64_t, int64_t> updates;
                    for (size_t round = 0; round < 50; ++round) {
                        std::vector<uint64_t> keys(count);
                        for (auto& key : keys) key = (rng() % n) * 2 + 2;
                        if (shape == "sorted") std::sort(keys.begin(), keys.end());
                        if (shape == "early_miss") keys.front() = round * 2 + 1;
                        if (shape == "late_miss") keys.back() = round * 2 + 1;
                        TimeStarInsert<int64_t> request("m", "v");
                        for (size_t j = 0; j < count; ++j) {
                            const int64_t value = n + round * count + j;
                            request.addValue(keys[j], value);
                            updates[keys[j]] = value;
                        }
                        requests.push_back(std::move(request));
                    }
                    const auto start = std::chrono::steady_clock::now();
                    for (auto& request : requests) series.insert(std::move(request));
                    const double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start).count();
                    if (rep) times.push_back(ms);
                    const bool backfill = shape == "early_miss" || shape == "late_miss";
                    if (series.timestamps.size() != n + (backfill ? 50 : 0)) throw std::runtime_error("count mismatch");
                    for (size_t i = 0; i < series.timestamps.size(); ++i) {
                        const auto ts = series.timestamps[i];
                        if (i && series.timestamps[i-1] >= ts) throw std::runtime_error("order mismatch");
                        const auto it = updates.find(ts);
                        const auto expected = it == updates.end() ? int64_t(ts / 2 - 1) : it->second;
                        if (series.values[i] != expected) throw std::runtime_error("value mismatch");
                    }
                }
                std::sort(times.begin(), times.end());
                std::printf("%s n=%zu count=%zu median_ms=%.6f min_ms=%.6f max_ms=%.6f\n", shape.c_str(), n, count,
                            (times[2] + times[3]) / 2, times.front(), times.back());
            }
        }
    }
}
