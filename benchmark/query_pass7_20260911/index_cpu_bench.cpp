#include "native_index.hpp"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <seastar/core/app-template.hh>
#include <seastar/core/memory.hh>
#include <seastar/util/later.hh>
#include <stdexcept>

using namespace timestar::index;
using Clock = std::chrono::steady_clock;

seastar::future<int> benchmark() {
    NativeIndex index(0);
    co_await index.open();
    constexpr size_t hosts = 8192;
    constexpr uint64_t day = 20000ULL * keys::NS_PER_DAY;
    std::vector<std::string> names;
    std::vector<SeriesId128> expected;
    for (size_t h = 0; h < hosts; ++h) {
        names.push_back("host-" + std::to_string(h));
        std::map<std::string, std::string> tags{{"a_cohort", h % 2 ? "odd" : "even"},
                                                {"b_region", "r" + std::to_string(h % 4)}, {"host", names.back()}};
        auto id = co_await index.getOrCreateSeriesId("cpu", tags, "v");
        auto timestamp = day + (h % 256 == 0 ? 0 : keys::NS_PER_DAY);
        co_await index.recordDaySpan("cpu", id, timestamp, timestamp);
        if (h % 256 == 0)
            expected.push_back(id);
    }
    co_await index.sync();
    for (std::string shape : {"cardinality_host", "cardinality_region", "cardinality_missing", "time_tags_nonempty", "time_tags_empty",
                              "single_tag_control", "multi_tag_control", "day_membership_existing"}) {
        std::vector<double> samples, allocations;
        size_t repetitions = (shape.starts_with("cardinality") || shape == "day_membership_existing") ? 100000 : 1000;
        for (int rep = 0; rep < 7; ++rep) {
            const auto allocStart = seastar::memory::stats().mallocs();
            const auto start = Clock::now();
            for (size_t i = 0; i < repetitions; ++i) {
                if (shape == "day_membership_existing") {
                    co_await index.recordDaySpan("cpu", expected[i % expected.size()], day, day);
                } else if (shape.starts_with("cardinality")) {
                    bool missing = shape == "cardinality_missing";
                    bool host = shape != "cardinality_region";
                    const auto& name = names[i % hosts];
                    std::string value = missing ? name + "-missing" : host ? name : "r0";
                    double actual = co_await index.estimateTagCardinality("cpu", host ? "host" : "b_region",
                                                                         value);
                    if (actual != (missing ? 0.0 : host ? 1.0 : 2048.0))
                        throw std::runtime_error("cardinality mismatch");
                } else if (shape.starts_with("time")) {
                    const bool empty = shape == "time_tags_empty";
                    std::map<std::string, std::string> filters{{"a_cohort", empty ? "odd" : "even"}, {"b_region", "r0"}};
                    auto actual = co_await index.findSeriesWithMetadataTimeScoped("cpu", filters, {}, day,
                                                                                  day + keys::NS_PER_DAY - 1);
                    if (!actual || actual->size() != (empty ? 0 : expected.size()))
                        throw std::runtime_error("time query mismatch");
                    if (!empty) {
                        for (const auto& item : *actual)
                            if (std::find(expected.begin(), expected.end(), item.seriesId) == expected.end())
                                throw std::runtime_error("unexpected series");
                    }
                } else {
                    std::map<std::string, std::string> filters{{"host", names[i % hosts]}};
                    if (shape == "multi_tag_control")
                        filters["a_cohort"] = i % 2 ? "odd" : "even";
                    auto actual = co_await index.findSeries("cpu", filters);
                    if (!actual || actual->size() != 1)
                        throw std::runtime_error("tag query mismatch");
                }
            }
            double ns = std::chrono::duration<double, std::nano>(Clock::now() - start).count() / repetitions;
            const double allocs = double(seastar::memory::stats().mallocs() - allocStart) / repetitions;
            if (rep) {
                samples.push_back(ns);
                allocations.push_back(allocs);
            }
            co_await seastar::yield();
        }
        std::sort(samples.begin(), samples.end());
        std::sort(allocations.begin(), allocations.end());
        std::printf("%s median_ns=%.3f min_ns=%.3f max_ns=%.3f allocs_per_op=%.3f\n", shape.c_str(), (samples[2] + samples[3]) / 2,
                    samples.front(), samples.back(), (allocations[2] + allocations[3]) / 2);
    }
    co_await index.close();
    co_return 0;
}

int main(int argc, char** argv) {
    seastar::app_template app;
    return app.run(argc, argv, benchmark);
}
