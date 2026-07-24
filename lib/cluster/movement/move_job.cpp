#include "move_job.hpp"

namespace timestar::movement {

namespace {
void putU16(std::string& o, uint16_t v) {
    o.push_back(static_cast<char>(v & 0xff));
    o.push_back(static_cast<char>((v >> 8) & 0xff));
}
uint16_t getU16(const std::string& b, size_t i) {
    return static_cast<uint16_t>(static_cast<uint8_t>(b[i])) |
           (static_cast<uint16_t>(static_cast<uint8_t>(b[i + 1])) << 8);
}
}  // namespace

// Fixed 7-byte record: [step:u8][vshard:u16][dest:u16][victim:u16]. NodeId fits in
// u16 for the cluster sizes in scope; decode rejects anything malformed.
std::string MoveJob::encode() const {
    std::string o;
    o.push_back(static_cast<char>(step_));
    putU16(o, plan_.vshard);
    putU16(o, static_cast<uint16_t>(plan_.dest));
    putU16(o, static_cast<uint16_t>(plan_.victim));
    return o;
}

std::optional<MoveJob> MoveJob::decode(const std::string& bytes) {
    if (bytes.size() != 7)
        return std::nullopt;
    uint8_t s = static_cast<uint8_t>(bytes[0]);
    if (s > static_cast<uint8_t>(MoveStep::Done))
        return std::nullopt;
    MovePlan plan;
    plan.vshard = getU16(bytes, 1);
    plan.dest = getU16(bytes, 3);
    plan.victim = getU16(bytes, 5);
    // Reject a nonsensical plan from a corrupt payload: a move must have a real
    // destination, and cannot make a node replace itself.
    if (plan.dest == 0 || plan.dest == plan.victim)
        return std::nullopt;
    return MoveJob(plan, static_cast<MoveStep>(s));
}

}  // namespace timestar::movement
