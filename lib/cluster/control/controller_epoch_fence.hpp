#pragma once

#include <cstdint>
#include <map>
#include <string>

namespace timestar::control {

// Data-node side of controller term fencing (the plan: "every job-step RPC to a
// data node carries that epoch; data nodes persist the highest epoch seen per
// group and reject lower ones"). The controller epoch is the group-0 term under
// which its leader was elected; a deposed-but-stalled controller carries a lower
// epoch, so its late job-step RPCs fail the >= check here instead of
// interleaving with the current controller's.
//
// Per-group (per-VShard) because different data groups are actuated independently
// and a controller step names the group it targets.
class ControllerEpochFence {
public:
    // Accept a job step for `groupId` at controller `epoch`. Advances the
    // per-group high-water mark and returns true iff epoch >= the highest epoch
    // already seen for that group; a lower epoch (a deposed controller) is
    // rejected with no state change.
    bool accept(uint16_t groupId, uint64_t epoch) {
        uint64_t& hi = highest_[groupId];
        if (epoch < hi)
            return false;  // deposed controller: fenced out
        hi = epoch;
        return true;
    }

    uint64_t highestSeen(uint16_t groupId) const {
        auto it = highest_.find(groupId);
        return it == highest_.end() ? 0 : it->second;
    }

    // Durable serialization (each entry: groupId u16, epoch u64) so the fence
    // survives restart -- a stale controller must stay fenced across a crash.
    std::string serialize() const {
        std::string out;
        auto put16 = [&](uint16_t v) {
            out.push_back(static_cast<char>(v & 0xff));
            out.push_back(static_cast<char>((v >> 8) & 0xff));
        };
        auto put64 = [&](uint64_t v) {
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
        };
        put64(highest_.size());
        for (const auto& [g, e] : highest_) {  // std::map: canonical order
            put16(g);
            put64(e);
        }
        return out;
    }

    bool load(const std::string& s) {
        const char* p = s.data();
        const char* end = p + s.size();
        auto avail = [&](size_t n) { return static_cast<size_t>(end - p) >= n; };
        auto get16 = [&]() -> uint16_t {
            uint16_t v = static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8);
            p += 2;
            return v;
        };
        auto get64 = [&]() -> uint64_t {
            uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (8 * i);
            p += 8;
            return v;
        };
        if (!avail(8))
            return false;
        uint64_t n = get64();
        if (n > static_cast<uint64_t>(end - p) / 10)  // each entry is 10 bytes
            return false;
        std::map<uint16_t, uint64_t> loaded;
        for (uint64_t i = 0; i < n; ++i) {
            if (!avail(10))
                return false;
            uint16_t g = get16();
            uint64_t e = get64();
            loaded[g] = e;
        }
        highest_ = std::move(loaded);
        return true;
    }

private:
    std::map<uint16_t, uint64_t> highest_;
};

}  // namespace timestar::control
