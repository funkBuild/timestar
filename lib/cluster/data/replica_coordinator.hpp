#pragma once

#include "data_plane.hpp"  // QueryPartial, QueryIncomplete, AggState
#include "replica_read.hpp"

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <vector>

namespace timestar::data {

// The replicas of one VShard available to a query, in preference order (best
// first: locality, lowest lag, lowest queue depth -- the selector's job). Captured
// (pinned) at query start so a placement change mid-query cannot add or drop a
// VShard's contribution.
struct VShardReplicas {
    uint16_t vshard = 0;
    std::vector<ReplicaReader*> replicas;  // preference order; front is tried first
};

// Per-replica routing signals for per-query selection.
struct ReplicaHealth {
    ReplicaReader* replica = nullptr;
    bool reachable = true;  // recently in contact / not known-down
    // Applied-index distance behind the leader commit, in RAFT-LOG-INDEX space
    // (leaderCommit - group.appliedIndex), matching the read-time bounded-staleness
    // check -- NOT data-only sm.appliedIndex, or no-op/config entries misreport lag.
    uint64_t lagIndex = 0;
    uint32_t queueDepth = 0;  // outstanding reads (load)
    bool local = false;       // same failure domain / zone as the coordinator
    double errorRate = 0.0;   // recent error rate in [0,1]
};

// Per-query replica selection (plan §"Query scaling": eligibility, latency,
// locality, error-rate). Drops ineligible replicas -- unreachable always, and
// for BoundedStaleness those lagging beyond the bound (Linearizable/Session
// enforce freshness in the read itself, so any reachable replica is eligible) --
// then orders the rest by locality, then lag, then queue depth, then error rate.
// Returns the preference-ordered replica list for a VShardReplicas.
class ReplicaSelector {
public:
    static std::vector<ReplicaReader*> select(std::vector<ReplicaHealth> candidates, ReadConsistency mode,
                                              uint64_t maxLagIndex) {
        std::vector<ReplicaHealth> eligible;
        for (const auto& c : candidates) {
            if (!c.reachable || c.replica == nullptr)
                continue;
            if (mode == ReadConsistency::BoundedStaleness && c.lagIndex > maxLagIndex)
                continue;  // too stale to satisfy the bound
            eligible.push_back(c);
        }
        std::stable_sort(eligible.begin(), eligible.end(), [](const ReplicaHealth& a, const ReplicaHealth& b) {
            if (a.local != b.local)
                return a.local;  // local first
            if (a.lagIndex != b.lagIndex)
                return a.lagIndex < b.lagIndex;  // freshest next
            if (a.queueDepth != b.queueDepth)
                return a.queueDepth < b.queueDepth;  // least loaded
            // Most reliable last; treat NaN as worst so the comparator is a valid
            // strict weak ordering (a NaN < / > comparison is always false, which
            // would break stable_sort's ordering contract -> UB).
            double ae = std::isnan(a.errorRate) ? std::numeric_limits<double>::infinity() : a.errorRate;
            double be = std::isnan(b.errorRate) ? std::numeric_limits<double>::infinity() : b.errorRate;
            return ae < be;
        });
        std::vector<ReplicaReader*> out;
        out.reserve(eligible.size());
        for (const auto& c : eligible)
            out.push_back(c.replica);
        return out;
    }
};

// The result of a replica query: the merged data plus the VShards that could not
// be served (non-empty only under allow_partial). Returning `missing` in the
// result -- rather than storing it on the coordinator -- keeps query() from
// touching `this` after a suspension.
struct ReplicaQueryResult {
    QueryPartial partial;
    std::vector<uint16_t> missing;
};

// Coordinates a replica read across VShards with hedging and retry, guaranteeing
// each VShard contributes EXACTLY ONCE. For each VShard it tries replicas in waves
// of `hedgeWidth`: a wave launches that many reads concurrently and takes the
// FIRST success (the others are discarded, never merged -- "no combining two
// attempts"); if a whole wave fails it advances to the next replicas (retry), up
// to the pinned list. A VShard with no surviving replica fails the query
// (QueryIncomplete) unless `allowPartial`, in which case it is named in the
// result's `missing` -- never silently omitted.
class ReplicaQueryCoordinator {
public:
    ReplicaQueryCoordinator(std::vector<VShardReplicas> placement, unsigned hedgeWidth = 1, bool allowPartial = false)
        : placement_(std::move(placement)), hedgeWidth_(hedgeWidth < 1 ? 1 : hedgeWidth), allowPartial_(allowPartial) {}

    seastar::future<ReplicaQueryResult> query(ReplicaReadRequest req) {
        // Bind everything reached through `this` to frame-locals BEFORE any
        // co_await, and accumulate `missing` in the result: this coroutine must
        // not touch `this` after a suspension, so an in-flight query survives the
        // coordinator being destroyed mid-flight (e.g. a stack-local coordinator
        // whose caller wrapped the query in seastar::with_timeout -- which does
        // NOT cancel the inner future). Same rule the read facades follow.
        // `placement_` is the pinned config: iterate the snapshot, never live state.
        const std::vector<VShardReplicas>& placement = placement_;
        const unsigned hedgeWidth = hedgeWidth_;
        const bool allowPartial = allowPartial_;

        ReplicaQueryResult result;
        QueryPartial& merged = result.partial;
        for (const auto& vr : placement) {
            bool served = false;
            // Walk the preference list in waves of hedgeWidth.
            for (size_t base = 0; base < vr.replicas.size() && !served; base += hedgeWidth) {
                size_t end = std::min(base + hedgeWidth, vr.replicas.size());
                std::vector<seastar::future<ReplicaReadResult>> wave;
                wave.reserve(end - base);
                for (size_t i = base; i < end; ++i)
                    wave.push_back(vr.replicas[i]->read(req));
                // Await the whole wave (never abandon a future); take the FIRST
                // success in preference order and DISCARD the rest -- exactly one
                // contribution per VShard, no combining of two attempts.
                std::optional<ReplicaReadResult> first;
                for (auto& f : wave) {
                    try {
                        ReplicaReadResult r = co_await std::move(f);
                        if (!first)
                            first = std::move(r);
                    } catch (...) {
                        // this replica failed; another in the wave (or a later
                        // wave) may still serve the VShard.
                    }
                }
                if (first) {
                    merge(merged, first->partial);
                    served = true;
                }
            }
            if (!served) {
                if (allowPartial)
                    result.missing.push_back(vr.vshard);
                else
                    throw QueryIncomplete("replica query: no replica could serve a VShard");
            }
        }
        sortRaw(merged);
        co_return result;
    }

private:
    static void merge(QueryPartial& into, QueryPartial& part) {
        into.raw.insert(into.raw.end(), std::make_move_iterator(part.raw.begin()),
                        std::make_move_iterator(part.raw.end()));
        for (auto& [series, st] : part.perSeries)
            into.perSeries[series].merge(st);
    }
    static void sortRaw(QueryPartial& p) {
        std::sort(p.raw.begin(), p.raw.end(), [](const DataPoint& a, const DataPoint& b) {
            if (a.timestamp != b.timestamp)
                return a.timestamp < b.timestamp;
            if (!(a.series == b.series))
                return a.series < b.series;
            return a.value < b.value;
        });
    }

    std::vector<VShardReplicas> placement_;
    unsigned hedgeWidth_;
    bool allowPartial_;
};

}  // namespace timestar::data
