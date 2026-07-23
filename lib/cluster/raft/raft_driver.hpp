#pragma once

#include "raft_codec.hpp"  // Envelope
#include "raft_types.hpp"

#include <seastar/core/future.hh>
#include <vector>

namespace timestar::raft {

// Durable Raft state persistence (journal-backed in production). The persist*
// methods APPEND (buffer) their record; nothing is guaranteed durable until
// sync() resolves. The driver appends a Ready's snapshot/hardState/entries then
// calls sync() ONCE before sending any message -- so a whole Ready costs a
// single fsync (bounded fsync count, as the Phase 2 gate requires) while still
// honouring "durable before send".
class RaftPersistence {
public:
    virtual ~RaftPersistence() = default;
    virtual seastar::future<> persistHardState(HardState hs) = 0;
    virtual seastar::future<> persistEntries(std::vector<LogEntry> entries) = 0;
    virtual seastar::future<> persistSnapshot(Snapshot snap) = 0;
    // Make everything appended since the last sync() durable (fsync).
    virtual seastar::future<> sync() = 0;
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
