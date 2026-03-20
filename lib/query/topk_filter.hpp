#pragma once

#include "expression_evaluator.hpp"

#include <algorithm>
#include <cmath>
#include <map>
#include <string>
#include <vector>

namespace timestar {

// A named group from a multi-group query result (e.g. avg:cpu{} by {host}).
// Each group has a set of tags that identify it and the aligned series data.
struct GroupedSeries {
    // Tags that identify this group (e.g. {"host": "server1"}).
    std::map<std::string, std::string> group_tags;

    // The time series data for this group, already aligned to common timestamps.
    AlignedSeries series;
};

// Compute the mean of an AlignedSeries, ignoring NaN values.
// Returns -infinity if the series is empty or all-NaN (so the group sorts last
// for topk and first for bottomk — i.e. all-NaN groups are always the lowest).
inline double seriesMeanIgnoreNaN(const AlignedSeries& s) {
    double sum = 0.0;
    size_t count = 0;
    for (double v : s.values) {
        if (!std::isnan(v)) {
            sum += v;
            ++count;
        }
    }
    if (count == 0) {
        // All-NaN group — treat as -infinity so it ranks lowest
        return -std::numeric_limits<double>::infinity();
    }
    return sum / static_cast<double>(count);
}

// topk: return the N groups with the highest mean value.
// Groups are ranked by mean (ignoring NaN); NaN-only groups rank lowest.
// If N >= groups.size(), all groups are returned unchanged (in original order).
// The returned groups preserve their original order among themselves (stable).
inline std::vector<GroupedSeries> topk(int N, std::vector<GroupedSeries> groups) {
    if (N < 0) {
        throw EvaluationException("topk() N must be non-negative");
    }
    size_t k = static_cast<size_t>(N);
    if (k >= groups.size()) {
        return groups;  // Nothing to drop
    }

    // Precompute means to avoid O(G log G) calls to seriesMeanIgnoreNaN
    std::vector<double> means(groups.size());
    for (size_t i = 0; i < groups.size(); ++i)
        means[i] = seriesMeanIgnoreNaN(groups[i].series);

    // Build index array, sort by descending mean
    std::vector<size_t> indices(groups.size());
    for (size_t i = 0; i < indices.size(); ++i)
        indices[i] = i;

    std::stable_sort(indices.begin(), indices.end(), [&means](size_t a, size_t b) { return means[a] > means[b]; });

    // Keep the first k indices (highest means), re-sort by original position
    // to preserve the caller's ordering within the selected subset
    indices.resize(k);
    std::sort(indices.begin(), indices.end());

    std::vector<GroupedSeries> result;
    result.reserve(k);
    for (size_t idx : indices) {
        result.push_back(std::move(groups[idx]));
    }
    return result;
}

// bottomk: return the N groups with the lowest mean value.
// Groups are ranked by mean (ignoring NaN); NaN-only groups rank lowest
// (so they are included first when N allows).
// If N >= groups.size(), all groups are returned unchanged (in original order).
// The returned groups preserve their original order among themselves (stable).
inline std::vector<GroupedSeries> bottomk(int N, std::vector<GroupedSeries> groups) {
    if (N < 0) {
        throw EvaluationException("bottomk() N must be non-negative");
    }
    size_t k = static_cast<size_t>(N);
    if (k >= groups.size()) {
        return groups;  // Nothing to drop
    }

    // Precompute means to avoid O(G log G) calls to seriesMeanIgnoreNaN
    std::vector<double> means(groups.size());
    for (size_t i = 0; i < groups.size(); ++i)
        means[i] = seriesMeanIgnoreNaN(groups[i].series);

    // Build index array, sort by ascending mean (lowest first)
    std::vector<size_t> indices(groups.size());
    for (size_t i = 0; i < indices.size(); ++i)
        indices[i] = i;

    std::stable_sort(indices.begin(), indices.end(), [&means](size_t a, size_t b) { return means[a] < means[b]; });

    // Keep the first k indices (lowest means), re-sort by original position
    indices.resize(k);
    std::sort(indices.begin(), indices.end());

    std::vector<GroupedSeries> result;
    result.reserve(k);
    for (size_t idx : indices) {
        result.push_back(std::move(groups[idx]));
    }
    return result;
}

// ==================== Cross-series aggregation functions ====================
//
// Each of these collapses a vector of GroupedSeries into a single GroupedSeries
// by aggregating across all series at each timestamp.
//
// Algorithm:
//   1. Collect all unique timestamps across all input series.
//   2. For each timestamp T, gather the non-NaN value from every series that
//      has a data point at T.
//   3. Apply the aggregation function to the gathered values.
//   4. If no series has a value at T (or all values are NaN), output NaN.
//   5. Return a single GroupedSeries with empty group_tags.
//
// Series with mismatched timestamps are fully supported: a series simply does
// not contribute to a timestamp bucket if it has no data point there.

namespace detail {

// Build the sorted union of all timestamps appearing in any of the groups.
// Uses iterative set_union on already-sorted per-group timestamps: O(G*N)
// instead of O(G*N * log(G*N)) from concat+sort.
inline std::vector<uint64_t> unionTimestamps(const std::vector<GroupedSeries>& groups) {
    if (groups.empty())
        return {};
    std::vector<uint64_t> result = *groups[0].series.timestamps;
    std::vector<uint64_t> temp;
    for (size_t i = 1; i < groups.size(); ++i) {
        const auto& ts = *groups[i].series.timestamps;
        temp.clear();
        temp.reserve(result.size() + ts.size());
        std::set_union(result.begin(), result.end(), ts.begin(), ts.end(), std::back_inserter(temp));
        std::swap(result, temp);
    }
    return result;
}

// Compute the p-th percentile of a sorted non-empty vector using linear
// interpolation (same convention as numpy percentile with linear method).
// p must be in [0, 100].
inline double percentileOfSorted(const std::vector<double>& sorted, double p) {
    if (sorted.empty()) {
        return std::numeric_limits<double>::quiet_NaN();
    }
    if (sorted.size() == 1) {
        return sorted[0];
    }
    double idx = (p / 100.0) * static_cast<double>(sorted.size() - 1);
    size_t lo = static_cast<size_t>(std::floor(idx));
    size_t hi = lo + 1;
    if (hi >= sorted.size()) {
        return sorted.back();
    }
    double frac = idx - static_cast<double>(lo);
    return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
}

// Calls callback(value) for each non-NaN value at timestamp t,
// advancing cursors in place. No heap allocation.
template <typename Callback>
inline void forEachValueAt(const std::vector<GroupedSeries>& groups, std::vector<size_t>& cursors, uint64_t t,
                           Callback&& cb) {
    for (size_t g = 0; g < groups.size(); ++g) {
        const auto& s = groups[g].series;
        const auto& ts = *s.timestamps;
        if (cursors[g] < ts.size() && ts[cursors[g]] == t) {
            double v = s.values[cursors[g]++];
            if (!std::isnan(v))
                cb(v);
        }
    }
}

}  // namespace detail

// avg_of_series: at each timestamp, compute the mean of all non-NaN values
// across all groups. Returns a single GroupedSeries with empty group_tags.
inline GroupedSeries avg_of_series(std::vector<GroupedSeries> groups) {
    if (groups.empty()) {
        return GroupedSeries{};
    }

    auto allTs = detail::unionTimestamps(groups);
    std::vector<size_t> cursors(groups.size(), 0);

    std::vector<uint64_t> resultTs;
    std::vector<double> resultVals;
    resultTs.reserve(allTs.size());
    resultVals.reserve(allTs.size());

    const double nan = std::numeric_limits<double>::quiet_NaN();

    for (uint64_t t : allTs) {
        double sum = 0.0;
        size_t cnt = 0;
        detail::forEachValueAt(groups, cursors, t, [&](double v) {
            sum += v;
            ++cnt;
        });
        resultTs.push_back(t);
        resultVals.push_back(cnt > 0 ? sum / static_cast<double>(cnt) : nan);
    }

    GroupedSeries result;
    result.series = AlignedSeries(std::move(resultTs), std::move(resultVals));
    return result;
}

// sum_of_series: at each timestamp, sum all non-NaN values across all groups.
// If no non-NaN values exist at a timestamp, outputs NaN.
inline GroupedSeries sum_of_series(std::vector<GroupedSeries> groups) {
    if (groups.empty()) {
        return GroupedSeries{};
    }

    auto allTs = detail::unionTimestamps(groups);
    std::vector<size_t> cursors(groups.size(), 0);

    std::vector<uint64_t> resultTs;
    std::vector<double> resultVals;
    resultTs.reserve(allTs.size());
    resultVals.reserve(allTs.size());

    const double nan = std::numeric_limits<double>::quiet_NaN();

    for (uint64_t t : allTs) {
        double sum = 0.0;
        size_t cnt = 0;
        detail::forEachValueAt(groups, cursors, t, [&](double v) {
            sum += v;
            ++cnt;
        });
        resultTs.push_back(t);
        resultVals.push_back(cnt > 0 ? sum : nan);
    }

    GroupedSeries result;
    result.series = AlignedSeries(std::move(resultTs), std::move(resultVals));
    return result;
}

// min_of_series: at each timestamp, the minimum non-NaN value across all groups.
// If no non-NaN values exist at a timestamp, outputs NaN.
inline GroupedSeries min_of_series(std::vector<GroupedSeries> groups) {
    if (groups.empty()) {
        return GroupedSeries{};
    }

    auto allTs = detail::unionTimestamps(groups);
    std::vector<size_t> cursors(groups.size(), 0);

    std::vector<uint64_t> resultTs;
    std::vector<double> resultVals;
    resultTs.reserve(allTs.size());
    resultVals.reserve(allTs.size());

    const double nan = std::numeric_limits<double>::quiet_NaN();

    for (uint64_t t : allTs) {
        double m = std::numeric_limits<double>::infinity();
        size_t cnt = 0;
        detail::forEachValueAt(groups, cursors, t, [&](double v) {
            if (v < m)
                m = v;
            ++cnt;
        });
        resultTs.push_back(t);
        resultVals.push_back(cnt > 0 ? m : nan);
    }

    GroupedSeries result;
    result.series = AlignedSeries(std::move(resultTs), std::move(resultVals));
    return result;
}

// max_of_series: at each timestamp, the maximum non-NaN value across all groups.
// If no non-NaN values exist at a timestamp, outputs NaN.
inline GroupedSeries max_of_series(std::vector<GroupedSeries> groups) {
    if (groups.empty()) {
        return GroupedSeries{};
    }

    auto allTs = detail::unionTimestamps(groups);
    std::vector<size_t> cursors(groups.size(), 0);

    std::vector<uint64_t> resultTs;
    std::vector<double> resultVals;
    resultTs.reserve(allTs.size());
    resultVals.reserve(allTs.size());

    const double nan = std::numeric_limits<double>::quiet_NaN();

    for (uint64_t t : allTs) {
        double m = -std::numeric_limits<double>::infinity();
        size_t cnt = 0;
        detail::forEachValueAt(groups, cursors, t, [&](double v) {
            if (v > m)
                m = v;
            ++cnt;
        });
        resultTs.push_back(t);
        resultVals.push_back(cnt > 0 ? m : nan);
    }

    GroupedSeries result;
    result.series = AlignedSeries(std::move(resultTs), std::move(resultVals));
    return result;
}

// percentile_of_series: at each timestamp, the p-th percentile of all
// non-NaN values across all groups.
// p must be in [0, 100]. Linear interpolation is used.
// If no non-NaN values exist at a timestamp, outputs NaN.
inline GroupedSeries percentile_of_series(double p, std::vector<GroupedSeries> groups) {
    if (p < 0.0 || p > 100.0) {
        throw EvaluationException("percentile_of_series() p must be in [0, 100], got " + std::to_string(p));
    }
    if (groups.empty()) {
        return GroupedSeries{};
    }

    auto allTs = detail::unionTimestamps(groups);
    std::vector<size_t> cursors(groups.size(), 0);

    std::vector<uint64_t> resultTs;
    std::vector<double> resultVals;
    resultTs.reserve(allTs.size());
    resultVals.reserve(allTs.size());

    const double nan = std::numeric_limits<double>::quiet_NaN();

    // Single pre-allocated scratch buffer — zero heap allocations in the hot loop.
    std::vector<double> tmp;
    tmp.reserve(groups.size());

    for (uint64_t t : allTs) {
        tmp.clear();
        detail::forEachValueAt(groups, cursors, t, [&](double v) { tmp.push_back(v); });
        resultTs.push_back(t);
        if (tmp.empty()) {
            resultVals.push_back(nan);
        } else {
            std::sort(tmp.begin(), tmp.end());
            resultVals.push_back(detail::percentileOfSorted(tmp, p));
        }
    }

    GroupedSeries result;
    result.series = AlignedSeries(std::move(resultTs), std::move(resultVals));
    return result;
}

}  // namespace timestar
