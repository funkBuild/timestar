#include "replicated_vshard_host.hpp"

#include "../../core/placement_table.hpp"  // virtualShard
#include "../../core/vshard.hpp"
#include "../raft/raft_node.hpp"

#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <stdexcept>

namespace timestar::cluster {

namespace fs = std::filesystem;

ReplicatedVShardHost::ReplicatedVShardHost(EngineLocalStore& store, raft::RaftTransport& transport, NodeId self,
                                           std::filesystem::path journalRoot, std::chrono::milliseconds tick)
    : store_(store), self_(self), journalRoot_(std::move(journalRoot)), registry_(transport, tick) {}

ReplicatedVShardHost::~ReplicatedVShardHost() = default;

seastar::future<> ReplicatedVShardHost::addVShard(uint16_t vshard, std::vector<NodeId> voters, raft::RaftOptions opts) {
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
    vs.sm = std::make_unique<EngineDataStateMachine>(store_);
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
    // Replicate each VShard group; every group must commit for the batch to ack.
    bool allOk = true;
    for (auto& [vs, b] : byVShard) {
        const bool ok = co_await propose(vs, data::ReplicatedCommand{std::move(b)});
        allOk = allOk && ok;
    }
    co_return allOk;
}

raft::RaftGroup* ReplicatedVShardHost::group(uint16_t vshard) {
    return registry_.group(vshard);
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
