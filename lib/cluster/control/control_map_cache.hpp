#pragma once

#include "../raft/raft_types.hpp"  // NodeId

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace timestar::control {

using timestar::raft::NodeId;

// The cluster-wide desired placement at a given map epoch: which nodes hold each
// VShard's replicas. This is the routing-relevant slice of group-0 state.
struct ControlMap {
    uint64_t epoch = 0;
    std::map<uint16_t, std::vector<NodeId>> placement;

    friend bool operator==(const ControlMap&, const ControlMap&) = default;
};

// Every node caches the last valid control map (the plan) so it can keep routing
// from a committed view during a group-0 quorum outage. Updates NEVER regress the
// epoch -- a stale/restored notification with a lower epoch is rejected, which is
// the cheap first line of defence behind the epoch-regression guard.
class ControlMapCache {
public:
    // Adopt a fresh map iff its epoch is >= the cached epoch. Returns true if the
    // cache changed.
    bool update(ControlMap fresh) {
        if (fresh.epoch < map_.epoch)
            return false;  // never regress
        if (fresh.epoch == map_.epoch && fresh.placement == map_.placement)
            return false;  // no change
        map_ = std::move(fresh);
        return true;
    }

    const ControlMap& current() const { return map_; }
    uint64_t epoch() const { return map_.epoch; }

    // Durable serialization so the cache survives restart (route during a cold
    // start before group 0 is reachable).
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
        put64(map_.epoch);
        put64(map_.placement.size());
        for (const auto& [vs, reps] : map_.placement) {  // std::map: canonical order
            put16(vs);
            put64(reps.size());
            for (NodeId n : reps)
                put64(n);
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
        if (!avail(16))
            return false;
        ControlMap m;
        m.epoch = get64();
        uint64_t nvs = get64();
        for (uint64_t i = 0; i < nvs; ++i) {
            if (!avail(10))
                return false;  // vs(2) + count(8)
            uint16_t vs = get16();
            uint64_t nr = get64();
            if (nr > static_cast<uint64_t>(end - p) / 8)
                return false;  // non-wrapping bound
            std::vector<NodeId> reps;
            reps.reserve(nr);
            for (uint64_t j = 0; j < nr; ++j)
                reps.push_back(get64());
            m.placement[vs] = std::move(reps);
        }
        map_ = std::move(m);
        return true;
    }

private:
    ControlMap map_;
};

}  // namespace timestar::control
