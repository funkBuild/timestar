#pragma once

#include "../../core/engine.hpp"
#include "../data/node_query.hpp"
#include "../data/node_store.hpp"
#include "../data/snapshot_payload.hpp"
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
class EngineLocalStore : public data::NodeStore {
public:
    // Routes every series by routeToCore(id) -- the SAME authority the metadata
    // index (Engine::indexMetadataSync) and per-core query discovery use, so a
    // series' data and its index entry always co-locate. VShard-cohesive
    // single-core placement (the M3 per-VShard-Raft precondition) must be a global
    // routeToCore change in cluster mode, not an adapter-local one, precisely so it
    // stays consistent with those two paths; see coreFor.
    explicit EngineLocalStore(seastar::sharded<Engine>& engines);

    // Apply a WriteBatch: group each WriteSeries by owning core, build
    // TimeStarInsert<T> per (series, type), dispatch to insertBatch<T>, then
    // indexMetadataSync the schema. Engine apply failures propagate (never
    // swallowed). Revisions in the batch are passed through -- the Engine must not
    // re-stamp non-empty revision vectors.
    seastar::future<> applyWrites(data::WriteBatch batch) override;

    seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end) override;
    seastar::future<> applyRetention(std::string measurement, uint64_t cutoff);

    // Build a self-contained InstallSnapshot payload for one VShard (M3). Precondition:
    // vshardsCohesiveOnCores(smp::count) -- a VShard's data must live entirely on
    // assignCore(vshard); this throws otherwise rather than ship a partial snapshot.
    // Dispatches to that core. catalogHash is a data-only sentinel (catalog reconciles
    // via schema broadcast, not the snapshot).
    seastar::future<data::SnapshotPayload> buildVShardSnapshot(VShardId vshard);

    // Install a received VShard snapshot (consumer side of the above). Same cohesion
    // precondition; dispatches to assignCore(vshard). Returns true iff installed.
    seastar::future<bool> installVShardSnapshot(VShardId vshard, data::SnapshotPayload payload);

    // Run the node's local query (the real HTTP query pipeline over this node's
    // engines) and return the per-series results as a NodeQueryPartial. In a
    // partitioned cluster the node only stores its owned VShards' series, so a
    // local query naturally returns only this node's contribution; the coordinator
    // merges partials across nodes. A failed local query surfaces as an
    // incompleteReason (fail-closed), never a silent empty success.
    seastar::future<data::NodeQueryPartial> queryLocal(data::NodeQueryRequest req) override;

    // This node's schema/cardinality for its owned series (M2 metadata scatter): the
    // coordinator unions items and sums cardinality across owners.
    seastar::future<data::MetadataResult> queryMetadata(data::MetadataRequest req) override;

    // Routing helper (public for tests): the core that owns `id`.
    unsigned coreFor(const SeriesId128& id) const;

private:
    seastar::sharded<Engine>& engines_;
};

}  // namespace timestar::cluster
