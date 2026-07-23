#pragma once

#include "raft_codec.hpp"  // Envelope
#include "raft_types.hpp"

#include <seastar/core/future.hh>
#include <vector>

namespace timestar::raft {

// Durable Raft state persistence (journal-backed in production). Each method must
// be durable -- fsync'd -- before its returned future resolves: the driver sends
// messages and advances commit only after persistence completes, so a premature
// resolution would break the Raft safety contract.
class RaftPersistence {
public:
    virtual ~RaftPersistence() = default;
    virtual seastar::future<> persistHardState(HardState hs) = 0;
    virtual seastar::future<> persistEntries(std::vector<LogEntry> entries) = 0;
    virtual seastar::future<> persistSnapshot(Snapshot snap) = 0;
};

// Sends an envelope toward its addressed peer. Fire-and-forget and best-effort:
// Raft tolerates loss and retries via heartbeats, so send() must swallow
// transport errors and may resolve before (or without) delivery.
class RaftTransport {
public:
    virtual ~RaftTransport() = default;
    virtual seastar::future<> send(Envelope env) = 0;
};

// The replicated state machine that consumes committed output.
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    virtual seastar::future<> apply(LogEntry entry) = 0;
    virtual seastar::future<> applySnapshot(Snapshot snap) = 0;
};

}  // namespace timestar::raft
