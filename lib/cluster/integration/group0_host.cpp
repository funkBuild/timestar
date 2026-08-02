#include "group0_host.hpp"

#include "replicated_vshard_host.hpp"  // JournalIdentity
#include "../../storage/journal_segment.hpp"
#include "../../utils/logger.hpp"
#include "../raft/raft_node.hpp"

#include <algorithm>
#include <seastar/core/coroutine.hh>
#include <set>
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

seastar::future<> Group0Host::start(std::vector<raft::NodeId> voters, raft::RaftOptions opts,
                                    std::string expectedClusterUuid,
                                    std::optional<control::NodeRecord> localRecord,
                                    std::optional<control::ControlMap> expectedInitialServingMap,
                                    control::Group0StateMachine::ServingMapObserver servingMapObserver,
                                    control::Group0StateMachine::ActiveFormatObserver activeFormatObserver) {
    if (started_ || writer_)
        throw std::logic_error("Group0Host::start called more than once");
    if (voters.empty())
        throw std::invalid_argument("Group0Host: control voter set must not be empty");
    if (self_ == raft::kNoNode)
        throw std::invalid_argument("Group0Host: this node must have a non-zero id");
    std::set<raft::NodeId> uniqueVoters;
    for (raft::NodeId voter : voters)
        if (voter == raft::kNoNode || !uniqueVoters.insert(voter).second)
            throw std::invalid_argument("Group0Host: control voters must be non-zero and unique");

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
        if (localRecord)
            sm_->expectLocalIdentity(std::move(expectedClusterUuid), std::move(*localRecord));
        if (expectedInitialServingMap)
            sm_->expectInitialServingMap(std::move(*expectedInitialServingMap));
        if (servingMapObserver)
            sm_->setServingMapObserver(std::move(servingMapObserver));
        if (activeFormatObserver)
            sm_->setActiveFormatObserver(std::move(activeFormatObserver));

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
    if (ticking_)
        throw std::logic_error("Group0Host::startTicking called more than once");
    registry_.startTicking();
    ticking_ = true;
    maintenanceTimer_.set_callback([this] {
        if (stopped_ || maintenanceRunning_ || backgroundGate_.is_closed())
            return;
        maintenanceRunning_ = true;
        // A named member owns the coroutine frame; the callback itself never
        // suspends or outlives its captures.
        (void)seastar::with_gate(backgroundGate_, [this] {
            return maintenanceSweep().finally([this] { maintenanceRunning_ = false; });
        });
    });
    maintenanceTimer_.arm(seastar::lowres_clock::now() + kMaintenanceInterval, {kMaintenanceInterval});
    controllerTimer_.set_callback([this] {
        if (stopped_ || controllerRunning_ || backgroundGate_.is_closed())
            return;
        controllerRunning_ = true;
        (void)seastar::with_gate(backgroundGate_, [this] {
            return controllerSweep().finally([this] { controllerRunning_ = false; });
        });
    });
    controllerTimer_.arm_periodic(kControllerActuationInterval);
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
    co_return co_await g->proposeAndAwaitApplied(control::encodeCommand(command),
                                                 seastar::lowres_clock::now() + kProposalTimeout);
}

seastar::future<bool> Group0Host::compactAppliedState() {
    raft::RaftGroup* g = group();
    if (!g || !sm_)
        throw std::logic_error("Group0Host::compact before start");
    if (g->appliedIndex() == raft::kNoIndex || g->appliedIndex() <= g->node().log().snapshotIndex())
        co_return false;
    std::string snapshot = sm_->snapshot();
    // RaftNode will have discarded the only log prefix a lagging replica could
    // use once compact() returns. Never cross that point with a payload the
    // production transport refuses to stage or send.
    if (snapshot.size() > raft::kMaxVShardSnapshotBytes) {
        ++compactionsRefusedTooLarge_;
        timestar::http_log.error(
            "cluster: NOT compacting group 0: its snapshot is {} bytes, over the {} byte total-snapshot bound; "
            "the control log is kept so a follower can still catch up",
            snapshot.size(), raft::kMaxVShardSnapshotBytes);
        co_return false;
    }
    co_await g->compact(g->appliedIndex(), std::move(snapshot));
    ++compactionsTaken_;
    co_return true;
}

seastar::future<size_t> Group0Host::reclaimJournalSegments() {
    if (!writer_ || !persistence_)
        throw std::logic_error("Group0Host::reclaimJournalSegments before start");
    const uint64_t floor = persistence_->releasedSeq();
    if (floor == 0)
        co_return 0;
    retention_.setReleased(kControlJournalStorageId, floor);
    auto result = co_await JournalGc::collect(journalRoot_ / "group0", writer_->currentSegmentNumber(), *writer_,
                                              retention_, JournalGc::Options{.copyForward = false});
    journalSegmentsDeleted_ += result.deletedSegments.size();
    co_return result.deletedSegments.size();
}

seastar::future<> Group0Host::compact() {
    if (stopped_ || backgroundGate_.is_closed())
        throw std::logic_error("Group0Host::compact after stop");
    auto held = backgroundGate_.hold();
    (void)co_await compactAppliedState();
    (void)co_await reclaimJournalSegments();
}

seastar::future<bool> Group0Host::maybeCompactOnce() {
    if (stopped_ || backgroundGate_.is_closed())
        co_return false;
    auto held = backgroundGate_.hold();
    ++maintenancePasses_;
    raft::RaftGroup* g = group();
    if (!g)
        throw std::logic_error("Group0Host::maybeCompactOnce before start");
    const raft::LogIndex applied = g->appliedIndex();
    const raft::LogIndex snapshot = g->node().log().snapshotIndex();
    const bool due = compactionEntryThreshold_ != 0 && applied > snapshot &&
                     applied - snapshot >= compactionEntryThreshold_;
    const bool compacted = due ? co_await compactAppliedState() : false;
    (void)co_await reclaimJournalSegments();
    co_return compacted;
}

seastar::future<> Group0Host::maintenanceSweep() {
    try {
        // This body already runs under maintenanceGate_ from the timer callback,
        // so perform the same pass without taking a nested hold.
        ++maintenancePasses_;
        raft::RaftGroup* g = group();
        if (!g)
            throw std::logic_error("Group0Host::maintenanceSweep before start");
        const raft::LogIndex applied = g->appliedIndex();
        const raft::LogIndex snapshot = g->node().log().snapshotIndex();
        if (compactionEntryThreshold_ != 0 && applied > snapshot &&
            applied - snapshot >= compactionEntryThreshold_)
            (void)co_await compactAppliedState();
        const size_t deleted = co_await reclaimJournalSegments();
        if (deleted > 0)
            timestar::http_log.info("cluster: group 0 reclaimed {} sealed journal segment(s) ({} total)", deleted,
                                    journalSegmentsDeleted_);
    } catch (const std::exception& e) {
        ++maintenanceFailures_;
        timestar::http_log.warn(
            "cluster: group-0 snapshot/journal maintenance failed: {} (the log is kept; will retry)", e.what());
    }
}

seastar::future<bool> Group0Host::stampControllerTermProposal() {
    raft::RaftGroup* g = group();
    if (!g || !sm_)
        throw std::logic_error("Group0Host::stampControllerTermProposal before start");
    if (!g->isLeader() || sm_->state().clusterUuid.empty())
        co_return false;
    const raft::Term term = g->currentTerm();
    if (term == raft::kNoTerm || sm_->state().controllerTerm >= term || lastControllerProposalTerm_ >= term)
        co_return false;

    // Do not use proposeAndAwaitApplied here. With CheckQuorum disabled, an
    // isolated leader can retain its role indefinitely and a quorum waiter would
    // leak until a later term, as well as keeping Group0Host::stop() open. A
    // successful propose() has already durably appended and sent the entry; Raft
    // will commit it when a quorum is available. The state machine derives the
    // controller epoch from the entry's real term, closing a lock-wait election
    // race around the sampled term in this payload.
    if (!co_await g->propose(control::encodeCommand(control::SetControllerTerm{term, self_})))
        co_return false;
    lastControllerProposalTerm_ = std::max(lastControllerProposalTerm_, g->currentTerm());
    ++controllerStampProposals_;
    co_return true;
}

seastar::future<bool> Group0Host::maybeStampControllerTermOnce() {
    if (stopped_ || backgroundGate_.is_closed())
        co_return false;
    auto held = backgroundGate_.hold();
    co_return co_await stampControllerTermProposal();
}

seastar::future<> Group0Host::controllerSweep() {
    try {
        (void)co_await stampControllerTermProposal();
    } catch (const std::exception& e) {
        ++controllerActuationFailures_;
        if (controllerActuationFailures_ == 1 || controllerActuationFailures_ % 1024 == 0)
            timestar::http_log.warn(
                "cluster: group-0 controller-term actuation failed: {} (will retry; occurrence {})", e.what(),
                controllerActuationFailures_);
    } catch (...) {
        ++controllerActuationFailures_;
        if (controllerActuationFailures_ == 1 || controllerActuationFailures_ % 1024 == 0)
            timestar::http_log.warn(
                "cluster: group-0 controller-term actuation failed with an unknown error (will retry; occurrence {})",
                controllerActuationFailures_);
    }
}

seastar::future<> Group0Host::stop() {
    if (stopped_)
        co_return;
    stopped_ = true;
    maintenanceTimer_.cancel();
    controllerTimer_.cancel();
    if (!backgroundGate_.is_closed())
        co_await backgroundGate_.close();
    if (started_)
        co_await registry_.stop();
    if (writer_)
        co_await writer_->close();
    started_ = false;
    ticking_ = false;
}

}  // namespace timestar::cluster
