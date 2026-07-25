#include "replicated_vshard_host.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"
#include "../../utils/logger.hpp"  // timestar::http_log
#include "../raft/raft_node.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <stdexcept>

namespace timestar::cluster {

namespace fs = std::filesystem;

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, std::chrono::milliseconds tick)
    : store_(store), self_(self), journalRoot_(std::move(journalRoot)), registry_(transport, tick) {}

ReplicatedVShardHost::~ReplicatedVShardHost() {
    // stop() (async: drains ticks, closes journal writers) MUST be called before
    // destruction. Destruction alone tears down the registry (groups) before
    // vshards_ (per the declaration order), which is UAF-safe, but skips writer
    // close() -> fd leak. Warn loudly rather than fail silently.
    if (!stopped_ && !vshards_.empty())
        timestar::http_log.warn(
            "ReplicatedVShardHost destroyed without stop(): {} VShard journal(s) not closed (fd leak)",
            vshards_.size());
}

seastar::future<> ReplicatedVShardHost::addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts) {
    // A VShard is hosted at most once: re-adding would open a second JournalWriter on
    // the same dir (two recoverers over one journal) and leak the old writer's fd.
    if (registry_.group(vshard))
        throw std::runtime_error("ReplicatedVShardHost::addVShard: VShard already hosted");
    VShardState vs;
    fs::path dir = journalRoot_ / ("vshard_" + std::to_string(vshard));
    fs::create_directories(dir);

    JournalSegmentHeader hdr;
    hdr.clusterUuid.fill(0x11);  // TODO(M3 group-0): the real cluster UUID from node.json
    hdr.coreNumber = static_cast<uint16_t>(seastar::this_shard_id());
    hdr.bootId.fill(0x44);
    vs.writer = std::make_unique<JournalWriter>(dir, hdr, 1u << 20);
    auto recovered = co_await vs.writer->open();
    raft::RecoveredRaftState st = raft::recoverRaftState(recovered, VShardId{vshard});

    std::vector<NodeId> baseVoters = voters;
    std::vector<NodeId> baseLearners;
    if (st.snapshot) {
        // Snapshot recovery installs the manifest into the Engine
        // (EngineDataStateMachine::applySnapshot) -- not wired in v1, so a compacted
        // journal cannot be recovered yet. Fail-closed rather than start with a hole.
        throw std::runtime_error("ReplicatedVShardHost: snapshot recovery not yet wired (M3)");
    }

    vs.persistence = std::make_unique<raft::JournalRaftPersistence>(*vs.writer, VShardId{vshard}, st.nextSeq);
    vs.sm = std::make_unique<EngineDataStateMachine>(store_, VShardId{vshard});
    raft::RaftNode node(self_, baseVoters, std::move(st.log), st.hardState, opts, baseLearners);
    registry_.addGroup(vshard, std::move(node), *vs.persistence, *vs.sm);
    vshards_[vshard] = std::move(vs);
    co_return;
}

seastar::future<bool> ReplicatedVShardHost::propose(uint16_t vshard, const data::ReplicatedCommand& cmd) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::propose: VShard not hosted here");
    return g->proposeAndAwaitApplied(data::encodeReplicatedCommand(cmd));
}

seastar::future<uint64_t> ReplicatedVShardHost::snapshotVShard(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        co_return 0;  // not hosted here
    // Capture only FLUSHED (TSM) data. manifest.snapshotRevision is the highest
    // revision the snapshot reproduces; since revisions are stamped from the log index
    // (ADR 0003), it is a safe log-truncation boundary. Entries after it may hold data
    // that lives only in the memory store, so they MUST stay in the log -- compacting to
    // appliedIndex would truncate them and lose unflushed data on restart.
    auto payload = co_await store_.buildVShardSnapshot(VShardId{vshard});
    const uint64_t upto = payload.manifest.snapshotRevision;
    if (upto == 0)
        co_return 0;  // no flushed data yet -> nothing to compact
    co_await g->compact(upto, data::encodeSnapshotPayload(std::move(payload)));
    co_return upto;
}

seastar::future<bool> ReplicatedVShardHost::proposeBatch(data::WriteBatch batch) {
    // Group the batch's series by VShard (a series routes to its VShard by hash,
    // same authority every replica uses). schemaVersion is carried per group.
    std::map<uint16_t, data::WriteBatch> byVShard;
    for (auto& s : batch.series) {
        const uint16_t vs = timestar::virtualShard(SeriesId128::fromSeriesKey(s.seriesKey));
        if (!registry_.group(vs))
            throw std::runtime_error("ReplicatedVShardHost::proposeBatch: VShard not led here");
        data::WriteBatch& dest = byVShard[vs];
        dest.schemaVersion = batch.schemaVersion;
        dest.series.push_back(std::move(s));
    }
    // Replicate every VShard group CONCURRENTLY; every group must commit for the
    // batch to ack.
    //
    // These are independent Raft groups with no ordering between them (this was never
    // an atomic multi-group commit -- see the header contract), but they used to be
    // proposed ONE AT A TIME, so a batch spanning N VShards serialised N full quorum
    // round trips (durable append + replicate + commit + apply) end to end. That was
    // the dominant write latency: measured at ~187ms average per 10k-point batch,
    // matching ~20 VShards x ~9ms. Issuing them together overlaps the network and
    // fsync waits instead of stacking them.
    std::vector<seastar::future<bool>> pending;
    pending.reserve(byVShard.size());
    for (auto& [vs, b] : byVShard)
        pending.push_back(propose(vs, data::ReplicatedCommand{std::move(b)}));

    // Await EVERY proposal even after one fails -- abandoning an in-flight future
    // would let its group's commit land on a destroyed continuation.
    bool allOk = true;
    std::exception_ptr firstErr;
    for (auto& f : pending) {
        try {
            const bool ok = co_await std::move(f);
            allOk = allOk && ok;
        } catch (...) {
            if (!firstErr)
                firstErr = std::current_exception();
        }
    }
    if (firstErr)
        std::rethrow_exception(firstErr);
    co_return allOk;
}

raft::RaftGroup* ReplicatedVShardHost::group(uint16_t vshard) {
    return registry_.group(vshard);
}

NodeId ReplicatedVShardHost::leaderOf(uint16_t vshard) const {
    raft::RaftGroup* g = const_cast<raft::RaftGroupRegistry&>(registry_).group(vshard);
    return g ? g->leader() : raft::kNoNode;
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderReadIndex(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderReadIndex: VShard not hosted here");
    // readBarrier() runs a quorum-confirmed ReadIndex round and REJECTS (throws) if this
    // node is not the current-term leader -- exactly the partition/redirect signal the
    // reaching replica needs, so no forwarding of stale barriers.
    return g->readBarrier();
}

seastar::future<raft::LogIndex> ReplicatedVShardHost::leaderCommitIndex(uint16_t vshard) {
    raft::RaftGroup* g = registry_.group(vshard);
    if (!g)
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: VShard not hosted here");
    if (!g->isLeader())
        // Bounded-staleness freshness must come from the leader's commit index; a
        // follower's is itself possibly stale, so reject rather than mislead.
        throw std::runtime_error("ReplicatedVShardHost::leaderCommitIndex: not the leader for this VShard");
    return seastar::make_ready_future<raft::LogIndex>(g->commitIndex());
}

seastar::future<> ReplicatedVShardHost::stop() {
    if (stopped_)
        co_return;
    stopped_ = true;
    co_await registry_.stop();  // stops the tick loop + drains
    for (auto& [vs, state] : vshards_)
        if (state.writer)
            co_await state.writer->close();
    vshards_.clear();
}

}  // namespace timestar::cluster
