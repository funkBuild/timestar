#pragma once

#include "../../storage/journal_gc.hpp"
#include "../../storage/journal_retention.hpp"
#include "../control/control_command.hpp"
#include "../control/group0_state_machine.hpp"
#include "../raft/raft_group_registry.hpp"
#include "../raft/raft_journal_persistence.hpp"
#include "../../core/vshard.hpp"
#include "../../storage/journal_writer.hpp"

#include <array>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>
#include <vector>

namespace timestar::cluster {

struct JournalIdentity;

// VShard Raft groups occupy the complete [0, 4096) range. Group 0 therefore
// cannot use wire id 0 in this implementation: that id is already the durable
// data group for VShard 0. The first id outside the VShard namespace is stable,
// fits the existing uint16_t Raft envelope, and is routed explicitly to core 0.
inline constexpr uint16_t kControlRaftGroupId = timestar::VIRTUAL_SHARD_COUNT;
static_assert(kControlRaftGroupId < UINT16_MAX);

// The durable production substrate for the cluster control group. It shares
// shard 0's Raft transport, but deliberately has its own registry/tick loop and
// journal directory: control records use storage VShard id 0 inside that private
// file, so no journal or wire format needs to be widened and data VShard 0 cannot
// collide with it.
class Group0Host {
public:
    // Group 0 is low volume compared with a data VShard, but it is also expected
    // to live for the lifetime of the cluster. Snapshot often enough to bound
    // restart replay, then reclaim sealed private-journal segments below the
    // durable snapshot boundary.
    static constexpr uint64_t kCompactionEntryThreshold = 1024;
    static constexpr std::chrono::seconds kMaintenanceInterval{60};
    static constexpr std::chrono::milliseconds kControllerActuationInterval{250};
    static constexpr std::chrono::seconds kProposalTimeout{6};

    Group0Host(raft::RaftTransport& transport, raft::NodeId self, std::filesystem::path journalRoot,
               const JournalIdentity& identity,
               std::chrono::milliseconds tick = std::chrono::milliseconds(20));
    ~Group0Host();
    Group0Host(const Group0Host&) = delete;
    Group0Host& operator=(const Group0Host&) = delete;

    // Open/recover the dedicated journal and register the control group. `self`
    // need not be a voter: a fresh joining node starts as a non-campaigning
    // observer that knows the seed voter set, then receives the membership entry
    // that admits/promotes it. This never initializes cluster state and never
    // campaigns; bootstrap is an explicit operation at the composition layer.
    // Production also supplies node-local identity/map recovery fences and an
    // observer that durably publishes an applied serving map before Raft's
    // applied boundary advances.
    seastar::future<> start(std::vector<raft::NodeId> voters, raft::RaftOptions opts = {},
                            std::string expectedClusterUuid = {},
                            std::optional<control::NodeRecord> localRecord = std::nullopt,
                            std::optional<control::ControlMap> expectedServingMap = std::nullopt,
                            control::Group0StateMachine::ServingMapObserver servingMapObserver = {});
    void startTicking();
    seastar::future<> stop();

    seastar::future<> deliver(raft::Envelope env);
    seastar::future<bool> propose(control::ControlCommand command);
    seastar::future<> compact();
    // One production maintenance pass, exposed so focused tests and a future
    // operator action do not need to wait for the timer. Returns true only when
    // this pass produced a new snapshot; segment GC is attempted either way so a
    // prior failed reclamation is retried even with no new commands.
    seastar::future<bool> maybeCompactOnce();
    // Propose this leader's current controller term once, without waiting for a
    // quorum. The locally durable entry commits when quorum is available; using
    // the non-waiting proposal path keeps an isolated leader and shutdown bounded.
    seastar::future<bool> maybeStampControllerTermOnce();
    void setCompactionEntryThreshold(uint64_t entries) { compactionEntryThreshold_ = entries; }

    bool started() const { return started_; }
    bool freshJournal() const { return freshJournal_; }
    raft::RaftGroup* group() { return registry_.group(kControlRaftGroupId); }
    control::Group0StateMachine* stateMachine() { return sm_.get(); }
    const control::Group0State& state() const { return sm_->state(); }
    uint64_t maintenancePasses() const { return maintenancePasses_; }
    uint64_t compactionsTaken() const { return compactionsTaken_; }
    uint64_t compactionsRefusedTooLarge() const { return compactionsRefusedTooLarge_; }
    uint64_t maintenanceFailures() const { return maintenanceFailures_; }
    uint64_t journalSegmentsDeleted() const { return journalSegmentsDeleted_; }
    uint64_t controllerStampProposals() const { return controllerStampProposals_; }
    uint64_t controllerActuationFailures() const { return controllerActuationFailures_; }
    uint64_t tickErrors() const { return registry_.tickErrors(); }

private:
    seastar::future<bool> compactAppliedState();
    seastar::future<size_t> reclaimJournalSegments();
    seastar::future<> maintenanceSweep();
    seastar::future<bool> stampControllerTermProposal();
    seastar::future<> controllerSweep();

    raft::NodeId self_;
    std::filesystem::path journalRoot_;
    std::array<uint8_t, 16> clusterUuid_{};
    std::array<uint8_t, 16> bootId_{};

    // Declaration order is load-bearing. The registry owns a RaftGroup that
    // borrows persistence_ and sm_; persistence_ borrows writer_. Destruction is
    // reverse declaration order, so the registry/group dies first.
    std::unique_ptr<JournalWriter> writer_;
    std::unique_ptr<raft::JournalRaftPersistence> persistence_;
    std::unique_ptr<control::Group0StateMachine> sm_;
    raft::RaftGroupRegistry registry_;
    JournalRetention retention_;
    seastar::timer<seastar::lowres_clock> maintenanceTimer_;
    seastar::timer<seastar::lowres_clock> controllerTimer_;
    seastar::gate backgroundGate_;
    uint64_t compactionEntryThreshold_ = kCompactionEntryThreshold;
    uint64_t maintenancePasses_ = 0;
    uint64_t compactionsTaken_ = 0;
    uint64_t compactionsRefusedTooLarge_ = 0;
    uint64_t maintenanceFailures_ = 0;
    uint64_t journalSegmentsDeleted_ = 0;
    uint64_t controllerStampProposals_ = 0;
    uint64_t controllerActuationFailures_ = 0;
    raft::Term lastControllerProposalTerm_ = raft::kNoTerm;
    bool maintenanceRunning_ = false;
    bool controllerRunning_ = false;
    bool freshJournal_ = false;
    bool started_ = false;
    bool ticking_ = false;
    bool stopped_ = false;
};

}  // namespace timestar::cluster
