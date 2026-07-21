#ifndef AGGREGATOR_TEST_HELPER_HPP
#define AGGREGATOR_TEST_HELPER_HPP

#include "../../lib/http/http_query_handler.hpp"
#include "../../lib/query/aggregator.hpp"

#include <map>
#include <vector>

namespace timestar {
namespace test {

// Helper class for testing - provides legacy-style interface using distributed aggregation
class AggregatorTestHelper {
public:
    // Convert a GroupedAggregationResult to a flat point list.  The merge
    // returns raw (timestamps, values) vectors instead of AggregatedPoints on
    // its single-partial fast path (exactly like production, where the HTTP
    // handler checks rawTimestamps first) — mirror that here so helper-based
    // tests see the same shape regardless of which merge path ran.
    static std::vector<AggregatedPoint> toPoints(const GroupedAggregationResult& g) {
        std::vector<AggregatedPoint> points;
        if (!g.rawTimestamps.empty()) {
            points.reserve(g.rawTimestamps.size());
            for (size_t i = 0; i < g.rawTimestamps.size(); ++i) {
                points.push_back({g.rawTimestamps[i], g.rawValues[i], 1});
            }
        } else {
            points = g.points;
        }
        return points;
    }

    static SeriesResult createSeries(const std::string& measurement, const std::map<std::string, std::string>& tags,
                                     const std::string& fieldName, const std::vector<uint64_t>& timestamps,
                                     const std::vector<double>& values) {
        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        sr.fields[fieldName] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }

    // Legacy-compatible aggregate() - single series aggregation
    static std::vector<AggregatedPoint> aggregate(const std::vector<uint64_t>& timestamps,
                                                  const std::vector<double>& values, AggregationMethod method,
                                                  uint64_t interval = 0) {
        auto series = createSeries("test", {}, "value", timestamps, values);
        auto partials = Aggregator::createPartialAggregations({series}, method, interval, {}).get();
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method).get();

        std::vector<AggregatedPoint> result;
        if (!grouped.empty()) {
            result = toPoints(grouped[0]);
        }

        return result;
    }

    // Legacy-compatible aggregateMultiple() - multiple series aggregation
    static std::vector<AggregatedPoint> aggregateMultiple(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series, AggregationMethod method,
        uint64_t interval = 0) {
        std::vector<SeriesResult> seriesResults;
        for (size_t i = 0; i < series.size(); ++i) {
            auto sr = createSeries("test", {}, "value", series[i].first, series[i].second);
            seriesResults.push_back(sr);
        }

        auto partials = Aggregator::createPartialAggregations(seriesResults, method, interval, {}).get();
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method).get();

        std::vector<AggregatedPoint> result;
        if (!grouped.empty()) {
            result = toPoints(grouped[0]);
        }

        return result;
    }

    // Legacy-compatible aggregateGroupBy() - group-by aggregation
    static std::map<std::string, std::vector<AggregatedPoint>> aggregateGroupBy(
        const std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>>& groups,
        AggregationMethod method, uint64_t interval = 0) {
        std::vector<SeriesResult> allSeries;
        for (const auto& [groupKey, groupSeries] : groups) {
            for (const auto& [timestamps, values] : groupSeries) {
                auto sr = createSeries("test", {{"group", groupKey}}, "value", timestamps, values);
                allSeries.push_back(sr);
            }
        }

        auto partials = Aggregator::createPartialAggregations(allSeries, method, interval, {"group"}).get();
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method).get();

        std::map<std::string, std::vector<AggregatedPoint>> result;
        for (const auto& g : grouped) {
            std::string groupKey = g.tags.at("group");
            result[groupKey] = toPoints(g);
        }

        return result;
    }
};

}  // namespace test
}  // namespace timestar

#endif  // AGGREGATOR_TEST_HELPER_HPP
