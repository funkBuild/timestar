#pragma once

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
#include <seastar/core/future.hh>
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
    seastar::future<> start(std::vector<raft::NodeId> voters, raft::RaftOptions opts = {});
    void startTicking();
    seastar::future<> stop();

    seastar::future<> deliver(raft::Envelope env);
    seastar::future<bool> propose(control::ControlCommand command);
    seastar::future<> compact();

    bool started() const { return started_; }
    bool freshJournal() const { return freshJournal_; }
    raft::RaftGroup* group() { return registry_.group(kControlRaftGroupId); }
    control::Group0StateMachine* stateMachine() { return sm_.get(); }
    const control::Group0State& state() const { return sm_->state(); }

private:
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
    bool freshJournal_ = false;
    bool started_ = false;
    bool ticking_ = false;
    bool stopped_ = false;
};

}  // namespace timestar::cluster
