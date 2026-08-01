#pragma once

#include "../../core/engine.hpp"
#include "../data/node_query.hpp"
#include "../data/node_store.hpp"
#include "../data/snapshot_payload.hpp"
#include "../data/write_record.hpp"

#include <cstdint>
#include <functional>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <utility>

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
    // Apply an already-committed/replayed command. This deliberately bypasses
    // ingest admission: rejecting after quorum commit stalls every retry of the
    // same Ready and can never make the durable work disappear.
    seastar::future<> applyCommittedWrites(data::WriteBatch batch);
    // Admission probes used by the leader before it proposes any slice.
    seastar::future<> checkWriteAdmission(const data::WriteBatch& batch);
    seastar::future<> checkWriteAdmission(data::VShardBatchView view);

    seastar::future<bool> applyDelete(std::string seriesKey, uint64_t start, uint64_t end) override;
    seastar::future<> applyRetention(std::string measurement, uint64_t cutoff);

    // Build a self-contained InstallSnapshot payload for one VShard (M3). Precondition:
    // vshardsCohesiveOnCores(smp::count) -- a VShard's data must live entirely on
    // assignCore(vshard); this throws otherwise rather than ship a partial snapshot.
    // Dispatches to that core and includes the exact catalog/index reconstruction
    // records bound by manifest.catalogHash.
    seastar::future<data::SnapshotPayload> buildVShardSnapshot(VShardId vshard);

    // Are there rolled-over memory stores holding THIS VSHARD's data that have NOT yet
    // been converted to TSM? (debt D-6, narrowed from per-shard to per-VShard by D-35.)
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
    // PER-VSHARD SINCE D-35, and the narrowing is what makes this usable under load. The
    // predicate was originally per-SHARD ("does ANY rolled store still await conversion?"),
    // which gave all ~1365 groups on a core the same answer: a shard under sustained ingest
    // is never at zero pending conversions, so no group on it ever compacted and every one
    // of them kept its whole log. Each MemoryStore now carries a 512-byte presence bitmap
    // over the 4096 VShards (MemoryStore::noteVShard), so the question narrows to "does an
    // unconverted rolled store hold data for THIS VShard?".
    //
    // The safety argument is unchanged, not weakened: if no unconverted rolled store holds
    // VShard V, every rolled point of V is already in TSM and the only unflushed remainder
    // of V lives in the ACTIVE store -- a contiguous suffix of V's revisions. That is
    // exactly the property the max-flushed-revision boundary needs, and it never depended
    // on the OTHER VShards' stores.
    //
    // Still conservative in the safe direction: a VShard whose own data is converting
    // continuously keeps its log until a conversion lands. A larger log is a cost; a
    // truncated log is data loss.
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

    // THE READ FENCE (debt D-36). Returns false if this node cannot prove it has applied
    // everything it had already committed, within its budget.
    //
    // APPLIED TO EVERY READ KIND THIS STORE SERVES, not just `queryLocal`: `queryMetadata`
    // answers /measurements, /fields, /tags, /tag-values and /cardinality out of the same
    // applied state, so a series that exists only in unapplied entries is missing from a
    // successful 200 there exactly as a data point would be. The two differ only in how
    // they report it -- queryLocal has an incompleteReason, metadata throws -- because
    // `MetadataResult` has no failure channel and its coordinator already fails the whole
    // request on any node's exception.
    //
    // Injected rather than reached for: EngineLocalStore is deliberately Raft-unaware
    // (it is the adapter that keeps the Engine cluster-unaware), and the groups live in
    // a per-shard ReplicatedVShardHost the node-level store has no handle on. Unset in
    // single-node and RF=1 modes, where there is no log above the Engine and therefore
    // nothing to fence -- and the fast path of the replicated implementation is an
    // integer compare per hosted group, so wiring it costs a caught-up node nothing.
    using ApplyFenceFn = std::function<seastar::future<bool>()>;
    void setApplyFence(ApplyFenceFn fn) { applyFence_ = std::move(fn); }

    // THE LEADER READ FENCE. In replicated mode every VShard named by a query
    // must complete its current leader's quorum-confirmed ReadIndex before any
    // Engine state is read. A partitioned former leader therefore rejects instead
    // of serving its locally-applied but stale state. The callback receives the
    // exact filter because an unrestricted replicated read cannot be fenced safely.
    using LeaderReadFenceFn = std::function<seastar::future<bool>(const std::vector<uint16_t>&)>;
    void setLeaderReadFence(LeaderReadFenceFn fn) { leaderReadFence_ = std::move(fn); }

    // Routing helper (public for tests): the core that owns `id`.
    unsigned coreFor(const SeriesId128& id) const;

private:
    seastar::future<> applyWritesImpl(data::WriteBatch batch, bool enforceAdmission);
    seastar::sharded<Engine>& engines_;
    ApplyFenceFn applyFence_;
    LeaderReadFenceFn leaderReadFence_;
};

}  // namespace timestar::cluster
