#include "group0_host.hpp"

#include "replicated_vshard_host.hpp"  // JournalIdentity
#include "../../storage/journal_segment.hpp"
#include "../../utils/logger.hpp"
#include "../raft/raft_node.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <stdexcept>
#include <utility>

namespace timestar::cluster {

namespace fs = std::filesystem;

namespace {

// The journal is private to group 0, so its record-level VShard id is merely a
// storage discriminator. Keeping it valid avoids a journal format change; the
// private directory is what separates it from the real data VShard 0.
inline constexpr VShardId kControlJournalStorageId{0};

}  // namespace

Group0Host::Group0Host(raft::RaftTransport& transport, raft::NodeId self, fs::path journalRoot,
                       const JournalIdentity& identity, std::chrono::milliseconds tick)
    : self_(self),
      journalRoot_(std::move(journalRoot)),
      clusterUuid_(identity.clusterUuid),
      bootId_(identity.bootId),
      registry_(transport, tick) {}

Group0Host::~Group0Host() {
    if (started_ && !stopped_)
        timestar::http_log.warn("Group0Host destroyed without stop(): control journal was not closed");
}

seastar::future<> Group0Host::start(std::vector<raft::NodeId> voters, raft::RaftOptions opts) {
    if (started_ || writer_)
        throw std::logic_error("Group0Host::start called more than once");
    if (voters.empty())
        throw std::invalid_argument("Group0Host: control voter set must not be empty");
    if (self_ == raft::kNoNode || std::find(voters.begin(), voters.end(), self_) == voters.end())
        throw std::invalid_argument("Group0Host: this node must be a non-zero control voter");

    const fs::path dir = journalRoot_ / "group0";
    fs::create_directories(dir);
    JournalSegmentHeader header;
    header.clusterUuid = clusterUuid_;
    header.coreNumber = 0;  // group 0 is always hosted on reactor shard 0
    header.bootId = bootId_;

    writer_ = std::make_unique<JournalWriter>(dir, header, 1u << 20);
    std::exception_ptr failure;
    try {
        auto recovered = co_await writer_->open();
        freshJournal_ = recovered.empty();
        raft::RecoveredRaftState st = raft::recoverRaftState(recovered, kControlJournalStorageId);

        std::vector<raft::NodeId> baseVoters = std::move(voters);
        std::vector<raft::NodeId> baseLearners;
        if (st.snapshot && !st.snapshot->config.voters.empty()) {
            baseVoters = st.snapshot->config.voters;
            baseLearners = st.snapshot->config.learners;
        }

        persistence_ =
            std::make_unique<raft::JournalRaftPersistence>(*writer_, kControlJournalStorageId, st.nextSeq);
        persistence_->seedRetention(std::move(st.retention));
        sm_ = std::make_unique<control::Group0StateMachine>();

        raft::RaftNode node(self_, std::move(baseVoters), std::move(st.log), st.hardState, opts,
                            std::move(baseLearners));
        if (st.snapshot) {
            // Unlike a data VShard's locally-produced snapshot, every byte of
            // control state lives in this payload. It must therefore be applied
            // on every recovery, independent of provenance, before the group can
            // receive traffic or expose its last committed map.
            co_await sm_->applySnapshot(*st.snapshot);
            node.seedRecoveredSnapshot(std::move(*st.snapshot));
        }
        registry_.addGroup(kControlRaftGroupId, std::move(node), *persistence_, *sm_);
        started_ = true;
        if (freshJournal_)
            timestar::http_log.info("cluster: group 0 opened a fresh dedicated journal at {}",
                                    dir.string());
        else
            timestar::http_log.info("cluster: group 0 recovered its dedicated journal at {}",
                                    dir.string());
    } catch (...) {
        failure = std::current_exception();
    }
    if (failure) {
        // start() is allowed to fail closed (foreign/corrupt journal, malformed
        // snapshot). Close the fd before propagating so a rejected startup does
        // not leak resources or leave a second opener behind.
        if (writer_)
            co_await writer_->close().handle_exception([](std::exception_ptr) {
                // Preserve the recovery/startup failure: it is the reason this
                // node cannot safely serve. The writer is fenced either way.
                return seastar::make_ready_future<>();
            });
        sm_.reset();
        persistence_.reset();
        writer_.reset();
        std::rethrow_exception(failure);
    }
}

void Group0Host::startTicking() {
    if (!started_)
        throw std::logic_error("Group0Host::startTicking before start");
    registry_.startTicking();
}

seastar::future<> Group0Host::deliver(raft::Envelope env) {
    if (!started_)
        co_return;
    co_await registry_.deliver(std::move(env));
}

seastar::future<bool> Group0Host::propose(control::ControlCommand command) {
    raft::RaftGroup* g = group();
    if (!g)
        throw std::logic_error("Group0Host::propose before start");
    co_return co_await g->proposeAndAwaitApplied(control::encodeCommand(command));
}

seastar::future<> Group0Host::compact() {
    raft::RaftGroup* g = group();
    if (!g || !sm_)
        throw std::logic_error("Group0Host::compact before start");
    if (g->appliedIndex() == raft::kNoIndex || g->appliedIndex() <= g->node().log().snapshotIndex())
        co_return;
    co_await g->compact(g->appliedIndex(), sm_->snapshot());
}

seastar::future<> Group0Host::stop() {
    if (stopped_)
        co_return;
    stopped_ = true;
    if (started_)
        co_await registry_.stop();
    if (writer_)
        co_await writer_->close();
    started_ = false;
}

}  // namespace timestar::cluster
