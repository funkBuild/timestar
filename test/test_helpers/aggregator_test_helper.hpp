#ifndef AGGREGATOR_TEST_HELPER_HPP
#define AGGREGATOR_TEST_HELPER_HPP

#include "../../lib/query/aggregator.hpp"
#include "../../lib/http/http_query_handler.hpp"
#include <vector>
#include <map>

namespace timestar {
namespace test {

// Helper class for testing - provides legacy-style interface using distributed aggregation
class AggregatorTestHelper {
public:
    static SeriesResult createSeries(
        const std::string& measurement,
        const std::map<std::string, std::string>& tags,
        const std::string& fieldName,
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values) {

        SeriesResult sr;
        sr.measurement = measurement;
        sr.tags = tags;
        sr.fields[fieldName] = std::make_pair(timestamps, FieldValues(values));
        return sr;
    }

    // Legacy-compatible aggregate() - single series aggregation
    static std::vector<AggregatedPoint> aggregate(
        const std::vector<uint64_t>& timestamps,
        const std::vector<double>& values,
        AggregationMethod method,
        uint64_t interval = 0) {

        auto series = createSeries("test", {}, "value", timestamps, values);
        auto partials = Aggregator::createPartialAggregations({series}, method, interval, {});
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method);

        std::vector<AggregatedPoint> result;
        if (!grouped.empty()) {
            for (const auto& point : grouped[0].points) {
                result.push_back(point);
            }
        }

        return result;
    }

    // Legacy-compatible aggregateMultiple() - multiple series aggregation
    static std::vector<AggregatedPoint> aggregateMultiple(
        const std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>& series,
        AggregationMethod method,
        uint64_t interval = 0) {

        std::vector<SeriesResult> seriesResults;
        for (size_t i = 0; i < series.size(); ++i) {
            auto sr = createSeries("test", {}, "value", series[i].first, series[i].second);
            seriesResults.push_back(sr);
        }

        auto partials = Aggregator::createPartialAggregations(seriesResults, method, interval, {});
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method);

        std::vector<AggregatedPoint> result;
        if (!grouped.empty()) {
            for (const auto& point : grouped[0].points) {
                result.push_back(point);
            }
        }

        return result;
    }

    // Legacy-compatible aggregateGroupBy() - group-by aggregation
    static std::map<std::string, std::vector<AggregatedPoint>> aggregateGroupBy(
        const std::map<std::string, std::vector<std::pair<std::vector<uint64_t>, std::vector<double>>>>& groups,
        AggregationMethod method,
        uint64_t interval = 0) {

        std::vector<SeriesResult> allSeries;
        for (const auto& [groupKey, groupSeries] : groups) {
            for (const auto& [timestamps, values] : groupSeries) {
                auto sr = createSeries("test", {{"group", groupKey}}, "value", timestamps, values);
                allSeries.push_back(sr);
            }
        }

        auto partials = Aggregator::createPartialAggregations(allSeries, method, interval, {"group"});
        auto grouped = Aggregator::mergePartialAggregationsGrouped(partials, method);

        std::map<std::string, std::vector<AggregatedPoint>> result;
        for (const auto& g : grouped) {
            std::string groupKey = g.tags.at("group");
            std::vector<AggregatedPoint> points;
            for (const auto& point : g.points) {
                points.push_back(point);
            }
            result[groupKey] = points;
        }

        return result;
    }
};

} // namespace test
} // namespace timestar

#endif // AGGREGATOR_TEST_HELPER_HPP
