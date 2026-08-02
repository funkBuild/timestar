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
    // `receivedFromPeer` records PROVENANCE, and it is a durability contract rather than
    // bookkeeping (review F2). A snapshot this node PRODUCED was built from its own on-disk
    // state, so that state survives a crash and recovery need only adopt the boundary. A
    // snapshot RECEIVED from a peer is durable in this record and NOWHERE ELSE until the
    // state machine has finished installing it -- and the record is fsync'd BEFORE the
    // install by design (the Ready contract). A crash in that window leaves a replica whose
    // log has been truncated to the boundary and whose Engine holds only whichever files
    // landed, so recovery MUST re-install a received snapshot and MUST NOT re-install a
    // produced one. The two are byte-identical in shape, so the flag is the only thing that
    // can tell them apart.
    virtual seastar::future<> persistSnapshot(Snapshot snap, bool receivedFromPeer) = 0;
    // Production persistence overrides these two hooks to keep snapshot bytes
    // on disk. The deterministic/in-memory implementations need no staging and
    // retain the original string path used by core tests.
    virtual seastar::future<> hydrateSnapshotChunk(InstallSnapshot& chunk) {
        if (chunk.sourceFile)
            throw std::runtime_error("RaftPersistence cannot hydrate a file-backed snapshot chunk");
        return seastar::make_ready_future<>();
    }
    virtual seastar::future<> stageSnapshotChunk(InstallSnapshot&) { return seastar::make_ready_future<>(); }
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
