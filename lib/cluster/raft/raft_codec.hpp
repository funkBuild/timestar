#pragma once

#include "raft_messages.hpp"
#include "raft_types.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace timestar::raft {

// A Raft message plus the group (VShard) it belongs to. The multiplexed
// transport carries one of these per RPC; the receiving registry routes it to
// the addressed group's RaftNode. groupId 0 is the control-plane group.
struct Envelope {
    uint16_t groupId = 0;
    Message message;
};

// Compact little-endian wire serialization for an Envelope. Pure and
// transport-agnostic: the Seastar-RPC layer ships the resulting bytes and the
// receiver decodes them. Length-prefixed throughout so it is self-delimiting.
std::string encodeEnvelope(const Envelope& e);

// Decode bytes produced by encodeEnvelope. Returns nullopt on any malformed or
// truncated input (a corrupt/hostile frame must never fabricate a valid message).
std::optional<Envelope> decodeEnvelope(const std::string& bytes);

}  // namespace timestar::raft
