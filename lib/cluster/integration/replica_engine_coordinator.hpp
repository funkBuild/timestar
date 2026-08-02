#pragma once

#include "../data/data_plane.hpp"  // QueryIncomplete
#include "../data/replica_read.hpp"  // ReadConsistency, ReadEnvelope
#include "replica_engine_reader.hpp"

#include <algorithm>
#include <iterator>
#include <optional>
#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <vector>

namespace timestar::cluster {

// The replicas of one VShard available to a query, in preference order (best first --
// the selector's job). Captured (pinned) at query start so a
// placement change mid-query cannot add or drop a VShard's contribution.
struct EngineVShardReplicas {
    uint16_t vshard = 0;
    std::vector<EngineReplicaReadFace*> replicas;  // preference order; front tried first
};

struct EngineReplicaQueryResult {
    data::NodeQueryPartial partial;  // union of each VShard's one served partial
    std::vector<uint16_t> missing;   // VShards no replica could serve (allowPartial only)
};

// Coordinates a replica read across VShards over ReplicaEngineReaders,
// merging NodeQueryPartials, guaranteeing each VShard contributes EXACTLY ONCE. For
// each VShard it tries replicas in waves of `hedgeWidth`: a wave launches that many
// reads concurrently and takes the FIRST success in preference order (the rest are
// discarded, never merged); a whole failed wave advances to the next replicas
// (retry). A VShard no replica can serve fails the query (QueryIncomplete) unless
// `allowPartial`, in which case it is named in `missing` -- never silently omitted.
class ReplicaEngineQueryCoordinator {
public:
    ReplicaEngineQueryCoordinator(std::vector<EngineVShardReplicas> placement, unsigned hedgeWidth = 1,
                                  bool allowPartial = false)
        : placement_(std::move(placement)), hedgeWidth_(hedgeWidth < 1 ? 1 : hedgeWidth), allowPartial_(allowPartial) {}

    seastar::future<EngineReplicaQueryResult> query(data::NodeQueryRequest req, data::ReadConsistency mode,
                                                    data::ReadEnvelope token, uint64_t maxLagIndex) {
        // Bind everything reached through `this` to frame-locals BEFORE any co_await and
        // accumulate the result locally: this coroutine must not touch `this` after a
        // suspension (a stack-local coordinator whose caller wrapped query() in
        // seastar::with_timeout is not cancelled -- same rule the read facades follow).
        const std::vector<EngineVShardReplicas>& placement = placement_;
        const unsigned hedgeWidth = hedgeWidth_;
        const bool allowPartial = allowPartial_;

        EngineReplicaQueryResult result;
        data::NodeQueryPartial& merged = result.partial;
        for (const auto& vr : placement) {
            bool served = false;
            for (size_t base = 0; base < vr.replicas.size() && !served; base += hedgeWidth) {
                size_t end = std::min(base + hedgeWidth, vr.replicas.size());
                std::vector<seastar::future<ReplicaReadOutcome>> wave;
                wave.reserve(end - base);
                for (size_t i = base; i < end; ++i)
                    wave.push_back(vr.replicas[i]->read(req, mode, token, maxLagIndex));
                // Await the WHOLE wave (never abandon a future); take the FIRST success in
                // preference order and discard the rest -- exactly one contribution/VShard.
                std::optional<ReplicaReadOutcome> first;
                for (auto& f : wave) {
                    try {
                        ReplicaReadOutcome r = co_await std::move(f);
                        if (!first)
                            first = std::move(r);
                    } catch (...) {
                        // this replica failed; another in the wave or a later wave may serve.
                    }
                }
                if (first) {
                    mergePartial(merged, first->partial);
                    served = true;
                }
            }
            if (!served) {
                if (allowPartial)
                    result.missing.push_back(vr.vshard);
                else
                    throw data::QueryIncomplete("replica query: no replica could serve VShard " +
                                                std::to_string(vr.vshard));
            }
        }
        co_return result;
    }

private:
    // Union one VShard's served partial into the accumulated result. Each VShard is
    // disjoint (a series lives in exactly one VShard), so this is a plain concatenation
    // -- the coordinator's downstream finalize merges numeric partials across the union.
    static void mergePartial(data::NodeQueryPartial& into, data::NodeQueryPartial& part) {
        into.partials.insert(into.partials.end(), std::make_move_iterator(part.partials.begin()),
                             std::make_move_iterator(part.partials.end()));
        into.nonNumeric.insert(into.nonNumeric.end(), std::make_move_iterator(part.nonNumeric.begin()),
                               std::make_move_iterator(part.nonNumeric.end()));
        into.seriesFound += part.seriesFound;
        into.incompleteReasons.insert(into.incompleteReasons.end(),
                                      std::make_move_iterator(part.incompleteReasons.begin()),
                                      std::make_move_iterator(part.incompleteReasons.end()));
    }

    std::vector<EngineVShardReplicas> placement_;
    unsigned hedgeWidth_;
    bool allowPartial_;
};

}  // namespace timestar::cluster
