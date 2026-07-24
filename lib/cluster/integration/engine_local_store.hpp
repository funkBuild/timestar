#pragma once

#include "../../core/engine.hpp"
#include "../data/write_record.hpp"

#include <cstdint>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>

namespace timestar::cluster {

// Node-level adapter over sharded<Engine> (integration plan F.3): turns the
// lossless inter-node write/delete commands into real Engine operations. It lives
// outside lib/core so the Engine stays cluster-unaware. One instance per node
// (constructed against the shared sharded<Engine>); its methods dispatch to the
// owning reactor core via invoke_on.
class EngineLocalStore {
public:
    // `vshardCohesiveRouting`: in cluster mode, route a series to
    // assignCore(virtualShard(id), cores) so each VShard is single-core (the
    // precondition for per-VShard Raft groups/snapshots/moves in M3). Off = the
    // legacy hash % cores single-node routing.
    EngineLocalStore(seastar::sharded<Engine>& engines, bool vshardCohesiveRouting);

    // Apply a WriteBatch: group each WriteSeries by owning core, build
    // TimeStarInsert<T> per (series, type), dispatch to insertBatch<T>, then
    // indexMetadataSync the schema. Engine apply failures propagate (never
    // swallowed). Revisions in the batch are passed through -- the Engine must not
    // re-stamp non-empty revision vectors.
    seastar::future<> applyWrites(data::WriteBatch batch);

    seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end);
    seastar::future<> applyRetention(std::string measurement, uint64_t cutoff);

    // Routing helper (public for tests): the core that owns `id`.
    unsigned coreFor(const SeriesId128& id) const;

private:
    seastar::sharded<Engine>& engines_;
    bool vshardCohesive_;
    unsigned cores_;
};

}  // namespace timestar::cluster
