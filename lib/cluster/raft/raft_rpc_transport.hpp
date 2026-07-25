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
    // Routes an incoming envelope by group id WITHOUT decoding it, so the decode can
    // happen on the shard that owns the group instead of on the listening shard.
    // `bytes` stays alive for the duration of the returned future.
    using DeliverRawFn = std::function<seastar::future<>(uint16_t groupId, const char* bytes, size_t len)>;

    RaftRpcTransport();
    ~RaftRpcTransport() override;
    RaftRpcTransport(const RaftRpcTransport&) = delete;
    RaftRpcTransport& operator=(const RaftRpcTransport&) = delete;

    // Begin serving on `local`; each decoded incoming envelope is handed to
    // `onDeliver` (which routes it to the addressed group on this core).
    //
    // `perShardListener` declares that EVERY shard starts an instance on this same
    // address, exactly as DataPlaneRpc::start does. It selects where inbound
    // connections are ACCEPTED: pinned to the starting shard (false -- a single
    // instance must answer every connection itself) versus spread across shards
    // (true). It is NOT SO_REUSEPORT: this seastar disables reuseport outright, so
    // shard 0 owns the only socket and hands each accepted fd to the shard the listen
    // options name. Passing false while every shard listens quietly keeps the node's
    // whole inbound Raft ingress -- accept, read, and the peek/route hop -- on one
    // core. See listenServer() in the .cpp.
    seastar::future<> start(seastar::socket_address local, DeliverFn onDeliver, bool perShardListener = false);
    // Optional: when set, takes precedence over the DeliverFn given to start().
    // The node's single Raft listener lives on one shard, so decoding every inbound
    // AppendEntries there made that shard the bottleneck (a follower's shard 0 showed
    // ~10x the journal-sync latency of its peers). Peeking the group id costs 2 bytes.
    void setRawDeliver(DeliverRawFn onDeliverRaw);
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
