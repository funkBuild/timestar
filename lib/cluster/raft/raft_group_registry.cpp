#include "raft_group_registry.hpp"

#include <seastar/core/coroutine.hh>

namespace timestar::raft {

RaftGroupRegistry::RaftGroupRegistry(RaftTransport& transport, std::chrono::milliseconds tickInterval)
    : transport_(transport), tickInterval_(tickInterval) {}

RaftGroup& RaftGroupRegistry::addGroup(uint16_t groupId, RaftNode node, RaftPersistence& persistence,
                                       RaftStateMachine& sm) {
    auto g = std::make_unique<RaftGroup>(groupId, std::move(node), persistence, transport_, sm);
    RaftGroup& ref = *g;
    groups_[groupId] = std::move(g);
    return ref;
}

RaftGroup* RaftGroupRegistry::group(uint16_t groupId) {
    auto it = groups_.find(groupId);
    return it == groups_.end() ? nullptr : it->second.get();
}

seastar::future<> RaftGroupRegistry::deliver(Envelope env) {
    if (stopping_ || gate_.is_closed())
        return seastar::make_ready_future<>();
    auto it = groups_.find(env.groupId);
    if (it == groups_.end())
        return seastar::make_ready_future<>();  // no such group here; drop
    // Run the step under the gate so stop()'s gate.close() waits for it -- the
    // group must not be destroyed while a delivery is mid-flight in its step().
    RaftGroup* g = it->second.get();
    return seastar::with_gate(gate_, [g, m = std::move(env.message)]() mutable {
        return g->step(std::move(m));
    });
}

void RaftGroupRegistry::startTicking() {
    timer_.set_callback([this] {
        // Skip if a previous tick pass is still running (thousands of groups can
        // take longer than one interval) or we are shutting down.
        if (ticking_ || stopping_ || gate_.is_closed())
            return;
        ticking_ = true;
        (void)seastar::with_gate(gate_, [this]() -> seastar::future<> {
            return tickAll().finally([this] { ticking_ = false; });
        });
    });
    timer_.arm_periodic(tickInterval_);
}

seastar::future<> RaftGroupRegistry::tickAll() {
    for (auto& [gid, g] : groups_) {
        if (stopping_)
            co_return;
        co_await g->tick();
    }
}

seastar::future<> RaftGroupRegistry::stop() {
    stopping_ = true;
    timer_.cancel();
    co_await gate_.close();
}

}  // namespace timestar::raft
