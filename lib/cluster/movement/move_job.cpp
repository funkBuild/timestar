#include "move_job.hpp"

#include "../../core/vshard.hpp"

#include <cstring>
#include <stdexcept>

namespace timestar::movement {

namespace {

constexpr char kMoveJobMagic[] = {'T', 'S', 'M', 'J', '1'};
constexpr size_t kMoveJobHeaderBytes = sizeof(kMoveJobMagic) + sizeof(uint8_t) + sizeof(uint16_t) +
                                       4 * sizeof(uint64_t);  // epoch, dest, victim, voter count
constexpr uint64_t kMaxMoveVoters = 1024;

void putU16(std::string& o, uint16_t v) {
    o.push_back(static_cast<char>(v & 0xff));
    o.push_back(static_cast<char>((v >> 8) & 0xff));
}

void putU64(std::string& o, uint64_t v) {
    for (unsigned i = 0; i < 8; ++i)
        o.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

uint16_t getU16(const std::string& bytes, size_t& offset) {
    const uint16_t value = static_cast<uint16_t>(static_cast<uint8_t>(bytes[offset])) |
                           (static_cast<uint16_t>(static_cast<uint8_t>(bytes[offset + 1])) << 8);
    offset += 2;
    return value;
}

uint64_t getU64(const std::string& bytes, size_t& offset) {
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i)
        value |= static_cast<uint64_t>(static_cast<uint8_t>(bytes[offset++])) << (8 * i);
    return value;
}

}  // namespace

// Persisted group-0 job payload. The explicit marker is retained while the
// greenfield layout is updated in place as v1. NodeId is a uint64_t everywhere
// else in Raft and must stay full-width here: narrowing it would make a resumed
// move target a different node once ids exceed 65535.
//
// ["TSMJ1":5][step:u8][vshard:u16][map-epoch:u64][dest:u64]
// [victim:u64][source-voter-count:u64][source-voters:u64...]
std::string MoveJob::encode() const {
    if (!valid() || plan_.sourceVoters.size() > kMaxMoveVoters)
        throw std::invalid_argument("cannot encode an invalid VShard move job");

    std::string o;
    o.reserve(kMoveJobHeaderBytes + plan_.sourceVoters.size() * sizeof(uint64_t));
    o.append(kMoveJobMagic, sizeof(kMoveJobMagic));
    o.push_back(static_cast<char>(step_));
    putU16(o, plan_.vshard);
    putU64(o, plan_.mapEpoch);
    putU64(o, plan_.dest);
    putU64(o, plan_.victim);
    putU64(o, plan_.sourceVoters.size());
    for (NodeId voter : plan_.sourceVoters)
        putU64(o, voter);
    return o;
}

std::optional<MoveJob> MoveJob::decode(const std::string& bytes) {
    if (bytes.size() < kMoveJobHeaderBytes || std::memcmp(bytes.data(), kMoveJobMagic, sizeof(kMoveJobMagic)) != 0)
        return std::nullopt;
    size_t offset = sizeof(kMoveJobMagic);
    const uint8_t s = static_cast<uint8_t>(bytes[offset++]);
    if (s > static_cast<uint8_t>(MoveStep::Done))
        return std::nullopt;
    MovePlan plan;
    plan.vshard = getU16(bytes, offset);
    plan.mapEpoch = getU64(bytes, offset);
    plan.dest = getU64(bytes, offset);
    plan.victim = getU64(bytes, offset);
    const uint64_t voters = getU64(bytes, offset);
    if (voters > kMaxMoveVoters || voters != static_cast<uint64_t>(bytes.size() - offset) / sizeof(uint64_t) ||
        (bytes.size() - offset) % sizeof(uint64_t) != 0)
        return std::nullopt;
    plan.sourceVoters.reserve(voters);
    for (uint64_t i = 0; i < voters; ++i)
        plan.sourceVoters.push_back(getU64(bytes, offset));
    MoveJob job(std::move(plan), static_cast<MoveStep>(s));
    if (!job.valid())
        return std::nullopt;
    return job;
}

}  // namespace timestar::movement
