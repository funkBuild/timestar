#pragma once

#include "raft_driver.hpp"  // RaftTransport, Envelope

#include <functional>
#include <memory>
#include <seastar/core/future.hh>
#include <seastar/net/socket_defs.hh>

namespace timestar::raft {

// Multiplexed Raft transport over seastar::rpc. ONE connection per peer HOST
// carries EVERY group's traffic -- the group id travels inside each envelope --
// so thousands of Raft groups share a handful of sockets rather than one socket
// per group. send() is fire-and-forget: it returns immediately and never blocks
// the Ready loop on network round-trips (Raft tolerates loss and retries via
// heartbeats).
class RaftRpcTransport : public RaftTransport {
public:
    using DeliverFn = std::function<seastar::future<>(Envelope)>;

    RaftRpcTransport();
    ~RaftRpcTransport() override;
    RaftRpcTransport(const RaftRpcTransport&) = delete;
    RaftRpcTransport& operator=(const RaftRpcTransport&) = delete;

    // Begin serving on `local`; each decoded incoming envelope is handed to
    // `onDeliver` (which routes it to the addressed group on this core).
    seastar::future<> start(seastar::socket_address local, DeliverFn onDeliver);
    // Drain in-flight sends, then close the server and all peer clients.
    seastar::future<> stop();
    // Register how to reach a peer node (its host RPC address).
    void addPeer(NodeId id, seastar::socket_address addr);

    seastar::future<> send(Envelope env) override;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace timestar::raft
