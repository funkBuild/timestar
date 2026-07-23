#pragma once

#include "raft_driver.hpp"
#include "raft_group.hpp"
#include "raft_node.hpp"

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

namespace timestar::raft {

// All Raft groups (VShards) hosted on ONE core, sharing ONE transport and ONE
// periodic timer -- no per-group sockets, threads, or timer services. This is
// what lets a single core run thousands of lightweight groups. Incoming
// envelopes are routed to the addressed group by id; the timer ticks every
// group each interval.
class RaftGroupRegistry {
public:
    RaftGroupRegistry(RaftTransport& transport, std::chrono::milliseconds tickInterval);

    // Add a group. Its RaftGroup shares this registry's transport (so its sends
    // multiplex over the same per-host connections). persistence/sm are borrowed
    // and must outlive the registry.
    RaftGroup& addGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence,
                        RaftStateMachine& sm);
    RaftGroup* group(uint16_t groupId);
    size_t size() const { return groups_.size(); }

    // Route an incoming envelope to its group (wire this as the transport's
    // onDeliver). Unknown groups are dropped.
    seastar::future<> deliver(Envelope env);

    // Start / stop the shared periodic tick loop.
    void startTicking();
    seastar::future<> stop();

private:
    seastar::future<> tickAll();

    RaftTransport& transport_;
    std::chrono::milliseconds tickInterval_;
    std::map<uint16_t, std::unique_ptr<RaftGroup>> groups_;
    seastar::timer<> timer_;
    seastar::gate gate_;
    bool ticking_ = false;  // guards against overlapping tick passes
    bool stopping_ = false;
};

}  // namespace timestar::raft
