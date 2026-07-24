#pragma once

#include "../data/data_plane.hpp"  // QueryPartial, AggState, DataPoint

#include <algorithm>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace timestar::features {

// A CONSERVATIVE measurement -> VShard-set routing summary for pruning broad
// query fan-out (docs/clustering.md §"Query scaling": "Such summaries must be
// conservative: false positives are allowed; false negatives are not.").
//
// A false positive (a listed VShard that turns out to hold no matching series) is
// harmless -- just wasted fan-out. A false NEGATIVE (a VShard omitted that DOES
// hold matching series) is FORBIDDEN -- it silently loses data.
//
// The safety mechanism is a placement-EPOCH binding. A summary is built for one
// placement/ownership epoch and SEALED once it reflects a COMPLETE scan of that
// epoch's placement. A query pins a placement epoch; pruning is allowed ONLY when
// the summary is sealed AND its epoch matches the query's. Any staleness (a
// placement change bumps the epoch, un-sealing the summary) forces the safe
// fallback to ALL VShards -- so a summary that lags placement can never cause a
// false negative.
//
// WRITE-PATH CONTRACT (within one epoch): every new (measurement, vshard) pairing
// must be observe()'d BEFORE the corresponding series becomes query-visible, so a
// sealed summary is always a SUPERSET of the vshards holding data for a
// measurement. A placement change (not an ordinary write) is what bumps the epoch.
class RoutingSummary {
public:
    // Begin (re)building the summary for placement epoch `epoch`. Un-seals it:
    // until seal() is called, candidates() conservatively fans out to all.
    void beginEpoch(uint64_t epoch) {
        epoch_ = epoch;
        sealed_ = false;
        map_.clear();
    }
    // Record that `measurement` has series on `vshard`. Idempotent.
    void observe(const std::string& measurement, uint16_t vshard) { map_[measurement].insert(vshard); }
    // Declare the summary COMPLETE for its epoch (every series scanned). Only a
    // sealed, current summary may prune.
    void seal() { sealed_ = true; }

    uint64_t epoch() const { return epoch_; }
    bool sealed() const { return sealed_; }

    // The VShards a query on `measurement` (pinned at `queryEpoch`) MUST target.
    // Prune to the observed set ONLY when the summary is sealed AND current for the
    // query's epoch; otherwise -- stale, still building, or an unknown measurement
    // whose series may not yet be reflected -- fan out to ALL VShards. Never prune
    // to a subset we cannot prove is complete.
    std::vector<uint16_t> candidates(const std::string& measurement, const std::vector<uint16_t>& allVShards,
                                     uint64_t queryEpoch) const {
        if (!sealed_ || epoch_ != queryEpoch)
            return allVShards;  // stale/incomplete summary: conservative fan-out
        auto it = map_.find(measurement);
        if (it == map_.end())
            return allVShards;  // unknown even in a sealed epoch: stay conservative
        return std::vector<uint16_t>(it->second.begin(), it->second.end());
    }

private:
    uint64_t epoch_ = 0;
    bool sealed_ = false;
    std::map<std::string, std::set<uint16_t>> map_;
};

// Hierarchical query merge (docs/clustering.md §"Query scaling": "hierarchical
// merge when a single coordinator becomes saturated"). The RESULT must be a pure
// function of the query, never of the merge topology or coordinator saturation
// (§"Aggregation Result Shape": placement-independent). min/max/count are exactly
// associative, but floating-point sum/avg are NOT -- a balanced tree of pairwise
// merges reorders additions and yields a DIFFERENT rounding than a flat fold. So
// the arithmetic reduction is a CANONICAL SEQUENTIAL left-fold over the partials
// in the caller's canonical order (the same order the flat coordinator folds), and
// intermediate mergers must preserve that order. This makes the merged result
// identical whether one coordinator or a delegated hierarchy performed it; the
// hierarchy's benefit is bounded per-level fan-in and memory, not reordering.
class HierarchicalMerge {
public:
    // Merge partials by a canonical sequential left-fold, then total-order the raw
    // points -- byte-for-byte identical to the flat coordinator merge for every
    // aggregation method, on any merge topology, given the same input order.
    static data::QueryPartial merge(const std::vector<data::QueryPartial>& parts) {
        data::QueryPartial acc;
        for (const auto& p : parts)
            mergeInto(acc, p);
        std::sort(acc.raw.begin(), acc.raw.end(), [](const data::DataPoint& a, const data::DataPoint& b) {
            if (a.timestamp != b.timestamp)
                return a.timestamp < b.timestamp;
            if (!(a.series == b.series))
                return a.series < b.series;
            return a.value < b.value;
        });
        return acc;
    }

private:
    static void mergeInto(data::QueryPartial& into, const data::QueryPartial& part) {
        into.raw.insert(into.raw.end(), part.raw.begin(), part.raw.end());
        for (const auto& [series, st] : part.perSeries)
            into.perSeries[series].merge(st);
    }
};

}  // namespace timestar::features
