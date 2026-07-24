#pragma once

#include "../../core/series_id.hpp"  // SeriesId128
#include "../raft/raft_types.hpp"    // NodeId

#include <algorithm>
#include <cstdint>
#include <map>
#include <seastar/core/future.hh>
#include <stdexcept>
#include <vector>

namespace timestar::data {

// A query could not be answered completely -- some VShard that could hold
// matching data is unassigned or unreachable. The plan forbids serving this as an
// empty SUCCESS ("could-not-read" must be distinguishable from "genuinely empty"),
// so the coordinator throws this instead, mapped to QUERY_INCOMPLETE at the edge.
class QueryIncomplete : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

using ::SeriesId128;  // SeriesId128 lives in the global namespace
using timestar::raft::NodeId;

// One data point routed by its series (series -> VShard -> owner node).
struct DataPoint {
    SeriesId128 series{};
    uint64_t timestamp = 0;
    double value = 0;
};

enum class AggMethod : uint8_t { Raw = 0, Count = 1, Sum = 2, Avg = 3, Min = 4, Max = 5 };

struct QuerySpec {
    uint64_t startTime = 0;
    uint64_t endTime = 0;  // inclusive
    AggMethod method = AggMethod::Raw;
    // Empty = all series; otherwise restrict to these series.
    std::vector<SeriesId128> series;
};

// A per-series aggregation accumulator that MERGES associatively/commutatively --
// the same partial-merge the intra-node cross-core path uses, now reused across
// nodes so a scatter-gather result equals the single-node result.
struct AggState {
    uint64_t count = 0;
    double sum = 0;
    double min = 0;  // meaningful only when count > 0
    double max = 0;

    void add(double v) {
        if (count == 0) {
            min = max = v;
        } else {
            min = std::min(min, v);
            max = std::max(max, v);
        }
        sum += v;
        ++count;
    }
    void merge(const AggState& o) {
        if (o.count == 0)
            return;
        if (count == 0) {
            *this = o;
            return;
        }
        min = std::min(min, o.min);
        max = std::max(max, o.max);
        sum += o.sum;
        count += o.count;
    }
    double value(AggMethod m) const {
        switch (m) {
            case AggMethod::Count:
                return static_cast<double>(count);
            case AggMethod::Sum:
                return sum;
            case AggMethod::Avg:
                return count ? sum / static_cast<double>(count) : 0.0;
            case AggMethod::Min:
                return min;
            case AggMethod::Max:
                return max;
            default:
                return 0.0;
        }
    }
    friend bool operator==(const AggState&, const AggState&) = default;
};

// A node's partial answer to a query. Raw queries fill `raw`; aggregated queries
// fill `perSeries`. The coordinator merges partials from every owner node.
struct QueryPartial {
    std::vector<DataPoint> raw;
    std::map<SeriesId128, AggState> perSeries;
};

// This node's local storage (the Engine in production). The data plane routes
// only the writes/reads it OWNS to here.
class LocalStore {
public:
    virtual ~LocalStore() = default;
    virtual seastar::future<> applyWrites(std::vector<DataPoint> points) = 0;
    virtual seastar::future<QueryPartial> queryLocal(QuerySpec spec) = 0;
};

// Sends writes/queries to a peer node (seastar RPC in production). Fire-and-block
// per RPC; the router/coordinator handle fan-out concurrency.
class DataPlaneClient {
public:
    virtual ~DataPlaneClient() = default;
    virtual seastar::future<> forwardWrites(NodeId to, std::vector<DataPoint> points) = 0;
    virtual seastar::future<QueryPartial> queryRemote(NodeId to, QuerySpec spec) = 0;
};

}  // namespace timestar::data
