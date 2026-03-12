#pragma once

#include "expression_ast.hpp"
#include "expression_evaluator.hpp"
#include "streaming_aggregator.hpp"

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace timestar {

// StreamingDerivedEvaluator handles cross-query formula evaluation on live
// streams. It holds one StreamingAggregator per named query label and, on
// closeBuckets(), aligns their outputs by timestamp, evaluates the formula,
// and returns a single StreamingBatch of computed results.
class StreamingDerivedEvaluator {
public:
    StreamingDerivedEvaluator(uint64_t intervalNs, const std::map<std::string, AggregationMethod>& queryMethods,
                              std::shared_ptr<ExpressionNode> formula);

    // Route a data point to the correct query's aggregator.
    void addPoint(const std::string& label, const StreamingDataPoint& pt);

    // Close completed aggregator buckets, align by timestamp union, evaluate
    // the formula, and return the computed result as a StreamingBatch.
    // Pass nowNs=0 to close all buckets unconditionally.
    StreamingBatch closeBuckets(uint64_t nowNs = 0);

    // Check if any aggregator has accumulated data.
    bool hasData() const;

private:
    uint64_t _intervalNs;
    std::shared_ptr<ExpressionNode> _formula;
    std::map<std::string, std::unique_ptr<StreamingAggregator>> _aggregators;

    // Carry-forward: last known value per query label, for buckets where
    // a query has no data. Keyed by label.
    std::map<std::string, double> _lastValues;
    std::map<std::string, uint64_t> _lastValueTimestamps;  // timestamp of last real data point

    static constexpr uint64_t kCarryForwardMaxIntervals = 10;
};

}  // namespace timestar
