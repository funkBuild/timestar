#pragma once

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

// Immutable batch properties travel with the timestamps across fields/shards.
// Owned, publicly mutable TimeStarInsert::timestamps are never cached here.
struct SharedTimestampBatch {
    std::shared_ptr<const std::vector<uint64_t>> timestamps;
    bool sorted = true;
    bool strictlyIncreasing = true;
    uint64_t min = 0;
    uint64_t max = 0;
    std::vector<uint32_t> days;

    explicit SharedTimestampBatch(std::shared_ptr<const std::vector<uint64_t>> ts) : timestamps(std::move(ts)) {
        if (!timestamps || timestamps->empty())
            return;
        min = max = timestamps->front();
        uint64_t previous = timestamps->front();
        for (size_t i = 0; i < timestamps->size(); ++i) {
            const uint64_t t = (*timestamps)[i];
            if (i != 0) {
                sorted &= previous <= t;
                strictlyIncreasing &= previous < t;
            }
            min = std::min(min, t);
            max = std::max(max, t);
            const auto day = static_cast<uint32_t>(t / (86400ULL * 1'000'000'000ULL));
            if (days.empty() || days.back() != day)
                days.push_back(day);
            previous = t;
        }
        if (!sorted) {
            std::sort(days.begin(), days.end());
            days.erase(std::unique(days.begin(), days.end()), days.end());
        }
    }
};
