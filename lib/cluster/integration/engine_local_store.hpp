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

    // Are there rolled-over memory stores on this VShard's core that have NOT yet been
    // converted to TSM? (debt D-6.)
    //
    // THE SNAPSHOT PRODUCER MUST NOT COMPACT WHILE THIS IS TRUE, and the reason is
    // measured rather than theoretical -- it cost the snapshot-durability gate 7 of 200
    // acknowledged points on the first run.
    //
    // A snapshot's truncation boundary is derived from the highest revision present in
    // TSM. That is only a safe boundary if every revision BELOW it is also in TSM, and
    // WAL->TSM conversions run `conversion_concurrency` at a time (6) and therefore
    // COMPLETE OUT OF ORDER. Store A (revisions 1..10) and store B (11..20) can both have
    // rolled with only B converted: TSM then holds 11..20, the max flushed revision is 20,
    // and truncating anywhere near it discards the log entries for 1..10 -- whose data
    // exists ONLY in retained store A, in RAM, and is gone on a kill -9. The nodes then
    // disagree about how much they lost, because which conversions had finished differs
    // per node.
    //
    // With no unconverted store, TSM is exactly the set of ROLLED stores and the only
    // unflushed data is the ACTIVE store, which holds a contiguous SUFFIX of each VShard's
    // revisions -- so the max flushed revision has every revision below it in TSM, which is
    // what makes it a boundary at all.
    //
    // Conservative in the safe direction: under sustained write load a shard may rarely
    // have zero pending conversions, and a group then keeps its log until it does. A larger
    // log is a cost; a truncated log is data loss. Residual D-35 wants the precise
    // per-VShard flushed watermark so compaction need not wait for a quiet moment.
    seastar::future<bool> hasUnconvertedStores(VShardId vshard);

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
