#include "streaming_derived_evaluator.hpp"
#include "../utils/logger.hpp"

#include <algorithm>
#include <limits>
#include <set>
#include <cmath>

namespace timestar {

StreamingDerivedEvaluator::StreamingDerivedEvaluator(
    uint64_t intervalNs,
    const std::map<std::string, AggregationMethod>& queryMethods,
    std::shared_ptr<ExpressionNode> formula)
    : _intervalNs(intervalNs), _formula(std::move(formula)) {

    for (const auto& [label, method] : queryMethods) {
        _aggregators[label] = std::make_unique<StreamingAggregator>(
            intervalNs, method);
    }
}

void StreamingDerivedEvaluator::addPoint(
    const std::string& label, const StreamingDataPoint& pt) {

    auto it = _aggregators.find(label);
    if (it != _aggregators.end()) {
        it->second->addPoint(pt);
    }
}

bool StreamingDerivedEvaluator::hasData() const {
    for (const auto& [_, agg] : _aggregators) {
        if (agg->hasData()) return true;
    }
    return false;
}

StreamingBatch StreamingDerivedEvaluator::closeBuckets(uint64_t nowNs) {
    StreamingBatch result;

    // Close completed aggregator buckets and collect their batches
    std::map<std::string, StreamingBatch> perQueryBatches;
    for (auto& [label, agg] : _aggregators) {
        if (agg->hasData()) {
            perQueryBatches[label] = agg->closeBuckets(nowNs);
        }
    }

    if (perQueryBatches.empty()) {
        return result;
    }

    // Build per-query AlignedSeries from aggregated batches.
    // Each aggregator's batch has one point per (bucket, series+field).
    // For cross-query evaluation we collapse each query to a single series
    // (keyed by timestamp). If a query has multiple fields/series, we use
    // the first field's values.
    //
    // Collect all timestamps across all queries (union), then for each
    // query build values at those timestamps (carry-forward for missing).
    std::set<uint64_t> allTimestamps;
    std::map<std::string, std::map<uint64_t, double>> queryTimestampValues;

    for (const auto& [label, batch] : perQueryBatches) {
        auto& tsMap = queryTimestampValues[label];
        for (const auto& pt : batch.points) {
            double val = std::visit([](const auto& v) -> double {
                using VT = std::decay_t<decltype(v)>;
                if constexpr (std::is_same_v<VT, double>) return v;
                else if constexpr (std::is_same_v<VT, int64_t>) return static_cast<double>(v);
                else if constexpr (std::is_same_v<VT, bool>) return v ? 1.0 : 0.0;
                else return std::numeric_limits<double>::quiet_NaN();  // string: not numeric
            }, pt.value);
            // If multiple points at same timestamp (different series/fields),
            // keep the last one seen
            tsMap[pt.timestamp] = val;
            allTimestamps.insert(pt.timestamp);
        }
    }

    if (allTimestamps.empty()) {
        return result;
    }

    // The latest timestamp across all queries serves as the "current time"
    // reference for staleness checks.
    uint64_t latestTs = *allTimestamps.rbegin();

    // Build aligned series for each query using timestamp union.
    // For timestamps where a query has no data, use carry-forward.
    std::vector<uint64_t> sortedTimestamps(allTimestamps.begin(), allTimestamps.end());

    // Share a single timestamp vector across all query series (avoids N copies)
    auto sharedTimestamps = std::make_shared<const std::vector<uint64_t>>(sortedTimestamps);

    ExpressionEvaluator::QueryResultMap queryResults;
    for (const auto& [label, _] : _aggregators) {
        std::vector<double> values;
        values.reserve(sortedTimestamps.size());

        // Determine the carry-forward value, subject to staleness check.
        // If the last real data point is more than kCarryForwardMaxIntervals
        // intervals old relative to the latest timestamp seen, treat the
        // carry-forward as expired and use NaN instead.
        double lastVal = std::numeric_limits<double>::quiet_NaN();
        if (_lastValues.count(label)) {
            uint64_t lastRealTs = _lastValueTimestamps.count(label)
                                      ? _lastValueTimestamps[label]
                                      : 0;
            // Saturating multiply: if _intervalNs > UINT64_MAX / kCarryForwardMaxIntervals
            // then kCarryForwardMaxIntervals * _intervalNs would overflow uint64_t,
            // wrapping to a small value and making the staleness cutoff nonsensical.
            // When overflow would occur, clamp the product to UINT64_MAX so the
            // condition (latestTs > UINT64_MAX) is always false and staleCutoff = 0,
            // correctly treating every carry-forward as fresh.
            uint64_t maxIntervalProduct =
                (_intervalNs > UINT64_MAX / kCarryForwardMaxIntervals)
                    ? UINT64_MAX
                    : kCarryForwardMaxIntervals * _intervalNs;
            uint64_t staleCutoff =
                (latestTs > maxIntervalProduct)
                    ? latestTs - maxIntervalProduct
                    : 0;
            if (lastRealTs >= staleCutoff) {
                lastVal = _lastValues[label];  // still fresh
            }
            // else: too stale — leave as NaN so formula produces NaN for this bucket
        }

        auto& tsMap = queryTimestampValues[label];

        for (uint64_t ts : sortedTimestamps) {
            auto it = tsMap.find(ts);
            if (it != tsMap.end()) {
                lastVal = it->second;
                values.push_back(lastVal);
                _lastValueTimestamps[label] = ts;  // record real data timestamp
            } else {
                values.push_back(lastVal);  // carry-forward (or NaN if stale)
            }
        }

        // Update carry-forward state for next emission only when real data was seen
        if (!values.empty() && !std::isnan(values.back())) {
            _lastValues[label] = values.back();
            // _lastValueTimestamps[label] already updated in the loop above
        }

        queryResults[label] = AlignedSeries(sharedTimestamps, std::move(values));
    }

    // Evaluate formula
    try {
        ExpressionEvaluator evaluator;
        auto evaluated = evaluator.evaluate(*_formula, queryResults);

        for (size_t i = 0; i < evaluated.size(); ++i) {
            StreamingDataPoint pt;
            pt.measurement = "derived";
            pt.field = "value";
            pt.timestamp = (*evaluated.timestamps)[i];
            pt.value = evaluated.values[i];
            result.points.push_back(std::move(pt));
        }
    } catch (const EvaluationException& e) {
        // Formula evaluation failed — log the error and emit NaN for every
        // timestamp that was computed, so callers can distinguish evaluation
        // failure from "no data" and propagate the error downstream.
        query_log.warn("StreamingDerivedEvaluator: formula evaluation failed: {}", e.what());
        for (uint64_t ts : sortedTimestamps) {
            StreamingDataPoint pt;
            pt.measurement = "derived";
            pt.field = "value";
            pt.timestamp = ts;
            pt.value = std::numeric_limits<double>::quiet_NaN();
            result.points.push_back(std::move(pt));
        }
    }

    return result;
}

} // namespace timestar
