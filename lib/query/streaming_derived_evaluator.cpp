#include "streaming_derived_evaluator.hpp"

#include "../utils/logger.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>

namespace timestar {

StreamingDerivedEvaluator::StreamingDerivedEvaluator(uint64_t intervalNs,
                                                     const std::map<std::string, AggregationMethod>& queryMethods,
                                                     std::shared_ptr<ExpressionNode> formula)
    : _intervalNs(intervalNs), _formula(std::move(formula)) {
    for (const auto& [label, method] : queryMethods) {
        _aggregators[label] = std::make_unique<StreamingAggregator>(intervalNs, method);
    }
}

void StreamingDerivedEvaluator::addPoint(const std::string& label, const StreamingDataPoint& pt) {
    auto it = _aggregators.find(label);
    if (it != _aggregators.end()) {
        it->second->addPoint(pt);
    }
}

bool StreamingDerivedEvaluator::hasData() const {
    for (const auto& [_, agg] : _aggregators) {
        if (agg->hasData())
            return true;
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
    // Buckets arrive (near-)sorted, so flat vectors + sort/unique + a
    // two-pointer merge replace the per-point std::set/std::map node churn.
    std::vector<uint64_t> sortedTimestamps;
    std::map<std::string, std::vector<std::pair<uint64_t, double>>> queryPoints;

    for (const auto& [label, batch] : perQueryBatches) {
        auto& pts = queryPoints[label];
        pts.reserve(batch.points.size());
        for (const auto& pt : batch.points) {
            double val = std::visit(
                [](const auto& v) -> double {
                    using VT = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<VT, double>)
                        return v;
                    else if constexpr (std::is_same_v<VT, int64_t>)
                        return static_cast<double>(v);
                    else if constexpr (std::is_same_v<VT, bool>)
                        return v ? 1.0 : 0.0;
                    else
                        return std::numeric_limits<double>::quiet_NaN();  // string: not numeric
                },
                pt.value);
            pts.emplace_back(pt.timestamp, val);
        }
        // Stable sort preserves batch order among equal timestamps so the
        // last-seen value wins (multiple series/fields at the same bucket),
        // matching the previous map-overwrite semantics.
        std::stable_sort(pts.begin(), pts.end(),
                         [](const auto& a, const auto& b) { return a.first < b.first; });
        // In-place dedupe keeping the last value per timestamp.
        size_t w = 0;
        for (size_t r = 0; r < pts.size(); ++r) {
            if (w > 0 && pts[w - 1].first == pts[r].first) {
                pts[w - 1].second = pts[r].second;
            } else {
                pts[w++] = pts[r];
            }
        }
        pts.resize(w);
        for (const auto& [ts, _v] : pts) {
            sortedTimestamps.push_back(ts);
        }
    }

    if (sortedTimestamps.empty()) {
        return result;
    }

    // Timestamp union across all queries.
    std::sort(sortedTimestamps.begin(), sortedTimestamps.end());
    sortedTimestamps.erase(std::unique(sortedTimestamps.begin(), sortedTimestamps.end()), sortedTimestamps.end());

    // The latest timestamp across all queries serves as the "current time"
    // reference for staleness checks.
    uint64_t latestTs = sortedTimestamps.back();

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
            uint64_t lastRealTs = _lastValueTimestamps.count(label) ? _lastValueTimestamps[label] : 0;
            // Saturating multiply: if _intervalNs > UINT64_MAX / kCarryForwardMaxIntervals
            // then kCarryForwardMaxIntervals * _intervalNs would overflow uint64_t,
            // wrapping to a small value and making the staleness cutoff nonsensical.
            // When overflow would occur, clamp the product to UINT64_MAX so the
            // condition (latestTs > UINT64_MAX) is always false and staleCutoff = 0,
            // correctly treating every carry-forward as fresh.
            uint64_t maxIntervalProduct = (_intervalNs > UINT64_MAX / kCarryForwardMaxIntervals)
                                              ? UINT64_MAX
                                              : kCarryForwardMaxIntervals * _intervalNs;
            uint64_t staleCutoff = (latestTs > maxIntervalProduct) ? latestTs - maxIntervalProduct : 0;
            if (lastRealTs >= staleCutoff) {
                lastVal = _lastValues[label];  // still fresh
            }
            // else: too stale — leave as NaN so formula produces NaN for this bucket
        }

        static const std::vector<std::pair<uint64_t, double>> kNoPoints;
        auto qIt = queryPoints.find(label);
        const auto& pts = (qIt != queryPoints.end()) ? qIt->second : kNoPoints;

        // Two-pointer merge: pts is a sorted, deduped subset of the sorted
        // timestamp union, so a single cursor suffices.
        size_t pi = 0;
        for (uint64_t ts : sortedTimestamps) {
            if (pi < pts.size() && pts[pi].first == ts) {
                lastVal = pts[pi].second;
                ++pi;
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

}  // namespace timestar
